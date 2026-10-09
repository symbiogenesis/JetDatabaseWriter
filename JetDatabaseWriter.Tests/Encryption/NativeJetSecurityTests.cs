namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.IO;
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
    [InlineData(false)]
    [InlineData(true)]
    public async Task IndexedOrUnresolvedSecurityColumn_RefusesBeforeCatalogMutation(bool indexedOwner)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(original.AsSpan().ToArray());
        byte[] oldHeader = original.AsSpan(0, 4096).ToArray();
        byte[] newHeader = (byte[])oldHeader.Clone();
        EncryptionManager.WriteNativeJet4EncryptionHeader(newHeader, 0, "Changed123");
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken);
        JetDatabaseWriter.Catalog.Models.TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", TestContext.Current.CancellationToken);
        byte[] tdef = Assert.IsType<byte[]>(await harness.Database.TableDefs.ReadTDefBytesAsync(2, TestContext.Current.CancellationToken));
        TDefCounts counts = TDefCodec.ReadCounts(harness.Database.Format, tdef);
        int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(harness.Database.Format, tdef, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(harness.Database.Format.Index.TryReadRealIdxSlot(tdef, descriptorStart, 0, out RealIdxSlot slot));
        int column = indexedOwner ? Assert.IsType<JetDatabaseWriter.Schema.Models.ColumnInfo>(definition.FindColumn("Owner")).ColNum : 60_000;
        byte[] changed = await harness.Pager.ReadPageCopyAsync(2, TestContext.Current.CancellationToken);
        harness.Database.Format.Index.WriteRealIdxDescriptor(changed, slot.PhysStart, [column], slot.Flags, BinaryPrimitives.ReadInt32LittleEndian(changed.AsSpan(slot.FirstDpOffset, 4)));
        await harness.Pager.WritePageAsync(2, changed, TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        if (indexedOwner)
        {
            await Assert.ThrowsAsync<NotSupportedException>(() => NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, oldHeader, newHeader, TestContext.Current.CancellationToken).AsTask());
        }
        else
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(() => NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, oldHeader, newHeader, TestContext.Current.CancellationToken).AsTask());
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task PasswordChange_MatchesDaoNewPasswordSecurityPages()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        byte[] oracle = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4PasswordChanged.mdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(original.AsSpan().ToArray());
        byte[] oldHeader = original.AsSpan(0, 4096).ToArray();
        byte[] newHeader = oracle.AsSpan(0, 4096).ToArray();
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken))
        {
            await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, oldHeader, newHeader, TestContext.Current.CancellationToken);
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
        await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, oldHeader, newHeader, TestContext.Current.CancellationToken);
        await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, newHeader, oldHeader, TestContext.Current.CancellationToken);
        Assert.Equal(original, stream.ToArray());
    }
}
