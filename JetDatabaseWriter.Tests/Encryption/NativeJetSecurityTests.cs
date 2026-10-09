namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native Jet password changes remask system security identities.</summary>
public sealed class NativeJetSecurityTests
{
    [Fact]
    public async Task NativeWorkgroupFixture_HasCustomOwnerIdentity()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Workgroup.mdb"), new AccessReaderOptions("Native123") { UseLockFile = false }, TestContext.Current.CancellationToken);
        using System.Data.DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: TestContext.Current.CancellationToken);
        byte[] stored = Assert.IsType<byte[]>(Assert.Single(objects.Select("Name = 'T'"))["Owner"]);
        byte[] header = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Workgroup.mdb"), TestContext.Current.CancellationToken);
        byte[] identity = JetDatabaseWriter.Catalog.Jet4SecuritySid.Encode(JetFormat.FromHeader(header), header, stored);
        Assert.True(identity.Length > 2, $"Expected a custom workgroup SID; got {Convert.ToHexString(identity)}.");
    }

    [Theory]
    [InlineData("Bad\0Password")]
    [InlineData("123456789012345678901")]
    public async Task InvalidPassword_LeavesHeaderUnchanged(string password)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        byte[] header = original.AsSpan(0, 4096).ToArray();
        byte[] before = (byte[])header.Clone();
        Assert.ThrowsAny<Exception>(() => EncryptionManager.WriteNativeJet4EncryptionHeader(header, 1, password));
        Assert.Equal(before, header);
    }

    [Theory]
    [InlineData(false, "MSysObjects", "Owner")]
    [InlineData(true, "MSysObjects", "Owner")]
    [InlineData(true, "MSysACEs", "SID")]
    [InlineData(false, "MSysACEs", "SID")]
    public async Task IndexedOrUnresolvedSecurityColumn_RebuildsOrRefusesBeforeCatalogMutation(bool indexedOwner, string table, string columnName)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(original.AsSpan().ToArray());
        byte[] oldHeader = original.AsSpan(0, 4096).ToArray();
        byte[] newHeader = (byte[])oldHeader.Clone();
        EncryptionManager.WriteNativeJet4EncryptionHeader(newHeader, 0, "Changed123");
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken);
        long tablePage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(table, TestContext.Current.CancellationToken);
        JetDatabaseWriter.Catalog.Models.TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(tablePage, table, TestContext.Current.CancellationToken);
        byte[] tdef = Assert.IsType<byte[]>(await harness.Database.TableDefs.ReadTDefBytesAsync(tablePage, TestContext.Current.CancellationToken));
        TDefCounts counts = TDefCodec.ReadCounts(harness.Database.Format, tdef);
        int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(harness.Database.Format, tdef, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(harness.Database.Format.Index.TryReadRealIdxSlot(tdef, descriptorStart, 0, out RealIdxSlot slot));
        int column = indexedOwner ? Assert.IsType<JetDatabaseWriter.Schema.Models.ColumnInfo>(definition.FindColumn(columnName)).ColNum : 60_000;
        byte[] changed = await harness.Pager.ReadPageCopyAsync(tablePage, TestContext.Current.CancellationToken);
        Assert.True(harness.Database.Format.Index.TryReadRealIdxSlotWithKeyColumns(tdef, descriptorStart, 0, out _, out List<KeyColumn>? originalKeys));
        int[] keyColumns = indexedOwner ? originalKeys.Select(key => key.ColNum).Append(column).Distinct().ToArray() : [column];
        harness.Database.Format.Index.WriteRealIdxDescriptor(changed, slot.PhysStart, keyColumns, slot.Flags, BinaryPrimitives.ReadInt32LittleEndian(changed.AsSpan(slot.FirstDpOffset, 4)));
        await harness.Pager.WritePageAsync(tablePage, changed, TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        int originalEntryCount = indexedOwner ? (await IndexLeafChain.ReadEntriesAsync(harness.Database, tablePage, BinaryPrimitives.ReadInt32LittleEndian(changed.AsSpan(slot.FirstDpOffset, 4)), TestContext.Current.CancellationToken)).Count : 0;
        if (indexedOwner)
        {
            await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, harness.Services.Indexes, oldHeader, newHeader, TestContext.Current.CancellationToken);
            List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, tablePage, TestContext.Current.CancellationToken);
            List<IndexEntry> entries = await IndexLeafChain.ReadEntriesAsync(harness.Database, tablePage, roots[0], TestContext.Current.CancellationToken);
            Assert.Equal(originalEntryCount, entries.Count);
            Assert.NotEqual(before, stream.ToArray());
        }
        else
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(() => NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, harness.Services.Indexes, oldHeader, newHeader, TestContext.Current.CancellationToken).AsTask());
        }

        if (!indexedOwner)
        {
            Assert.Equal(before, stream.ToArray());
        }
    }

    [Theory]
    [InlineData("NativeJet4Rc4.mdb", "NativeJet4PasswordChanged.mdb")]
    [InlineData("NativeJet4Workgroup.mdb", "NativeJet4WorkgroupChanged.mdb")]
    public async Task PasswordChange_MatchesDaoNewPasswordSecurityPages(string sourceFixture, string changedFixture)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, sourceFixture), TestContext.Current.CancellationToken);
        byte[] oracle = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, changedFixture), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(original.AsSpan().ToArray());
        byte[] oldHeader = original.AsSpan(0, 4096).ToArray();
        byte[] newHeader = oracle.AsSpan(0, 4096).ToArray();
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken))
        {
            await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, harness.Services.Indexes, oldHeader, newHeader, TestContext.Current.CancellationToken);
        }

        byte[] actual = stream.ToArray();
        using IPageCodec sourceCodec = PageCodecFactory.Open(oldHeader, DatabaseFormat.Jet4Mdb, "Native123".AsMemory(), "password", cancellationToken: TestContext.Current.CancellationToken);
        using IPageCodec oracleCodec = PageCodecFactory.Open(newHeader, DatabaseFormat.Jet4Mdb, "Changed123".AsMemory(), "password", cancellationToken: TestContext.Current.CancellationToken);
        for (int page = 1; page < original.Length / 4096; page++)
        {
            sourceCodec.Decode(actual, page * 4096, page, 4096);
            oracleCodec.Decode(oracle, page * 4096, page, 4096);
            Assert.True(actual.AsSpan(page * 4096, 4096).SequenceEqual(oracle.AsSpan(page * 4096, 4096)), $"Native password change differs on page {page}.");
        }
    }

    [Fact]
    public async Task PasswordChange_RemasksEveryStoredIdentity_AndRoundTrips()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(original.AsSpan().ToArray());
        byte[] oldHeader = original.AsSpan(0, 4096).ToArray();
        byte[] newHeader = (byte[])oldHeader.Clone();
        EncryptionManager.WriteNativeJet4EncryptionHeader(newHeader, 0, "Changed123");
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken);
        await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, harness.Services.Indexes, oldHeader, newHeader, TestContext.Current.CancellationToken);
        await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, harness.Services.Indexes, newHeader, oldHeader, TestContext.Current.CancellationToken);
        Assert.Equal(original, stream.ToArray());
    }
}
