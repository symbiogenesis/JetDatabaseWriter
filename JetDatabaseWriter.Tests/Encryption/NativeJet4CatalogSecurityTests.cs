namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Native Jet4 catalog ownership and permissions remain consistent during table creation.</summary>
public sealed class NativeJet4CatalogSecurityTests
{
    [Fact]
    public async Task NativeHeader_EncodesInheritedOwnerPlaceholderWithoutMutation()
    {
        byte[] bytes = await File.ReadAllBytesAsync(
            Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Schema.mdb"),
            TestContext.Current.CancellationToken);
        byte[] header = bytes.AsSpan(0, 4096).ToArray();
        byte[] before = (byte[])header.Clone();
        Assert.Equal(new byte[] { 0xFB, 0x7E }, Jet4SecuritySid.GetOwnerPlaceholder(JetFormat.FromHeader(header), header));
        JetFormat format = JetFormat.FromHeader(header);
        Assert.Equal(new byte[] { 0xFA, 0x7B }, Jet4SecuritySid.Encode(format, header, [0x03, 0x01]));
        Assert.Equal(new byte[] { 0xFB, 0x7B }, Jet4SecuritySid.Encode(format, header, [0x02, 0x01]));
        Assert.Equal(new byte[] { 0xFB, 0x79 }, Jet4SecuritySid.Encode(format, header, [0x02, 0x03]));
        Assert.Equal(before, header);
    }

    [Fact]
    public async Task CreatedTable_OwnerAndPermissions_MatchDaoCreatedEncryptedTable()
    {
        string[] expected = await ReadSecurityAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Schema.mdb"));
        Assert.Contains(expected, entry => entry.StartsWith("ACE:", StringComparison.Ordinal));
        byte[] original = await File.ReadAllBytesAsync(
            Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"),
            TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions("Native123") { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Added",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }],
                TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions("Native123") { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
        string[] actual = await ReadSecurityAsync(reader);
        string nativeCatalog = await ReadNativeSecurityCatalogAsync();
        Assert.True(
            expected.SequenceEqual(actual),
            $"Native permissions:\n{string.Join("\n", expected)}\nWriter permissions:\n{string.Join("\n", actual)}\nNative catalog:\n{nativeCatalog}");
    }

    [Fact]
    public void ShortHeader_RefusesSidDerivation()
        => Assert.Throws<JetCorruptDataException>(() => Jet4SecuritySid.GetOwnerPlaceholder(
            JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), new byte[0x91]));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InvalidInheritedPermissions_DirectDdl_LeavesOriginalImage(bool allInheritable)
    {
        await using var stream = new MemoryStream(await File.ReadAllBytesAsync(
            Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Schema.mdb"),
            TestContext.Current.CancellationToken));
        await using (WriterHarness harness = await WriterHarness.OpenAsync(
            stream, new AccessWriterOptions("Native123") { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken))
        {
            long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysACEs", TestContext.Current.CancellationToken);
            TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(page, "MSysACEs", TestContext.Current.CancellationToken);
            ColumnInfo inherit = Assert.IsType<ColumnInfo>(definition.FindColumn("FInheritable"));
            var changed = new Dictionary<long, byte[]>();
            await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
                page,
                (row, _) =>
                {
                    Assert.True(RowDecodePlan.TryParseRowLayout(
                        harness.Database.Format.RowFields,
                        row.Page,
                        row.Location.RowStart,
                        row.Location.RowSize,
                        hasVarColumns: true,
                        out RowLayout layout));
                    if (!changed.TryGetValue(row.Location.PageNumber, out byte[]? bytes))
                    {
                        bytes = row.Page.AsSpan(0, harness.Database.Format.PageSize).ToArray();
                        changed.Add(row.Location.PageNumber, bytes);
                    }

                    int offset = row.Location.RowStart + layout.NullMaskPos + inherit.ColNum / 8;
                    int mask = 1 << (inherit.ColNum % 8);
                    bytes[offset] = (byte)(allInheritable ? bytes[offset] | mask : bytes[offset] & ~mask);
                    return new ValueTask<bool>(true);
                },
                TestContext.Current.CancellationToken);
            foreach (KeyValuePair<long, byte[]> pair in changed)
            {
                await harness.Pager.WritePageAsync(pair.Key, pair.Value, TestContext.Current.CancellationToken);
            }
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions("Native123") { UseLockFile = false, UseTransactionalWrites = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            if (allInheritable)
            {
                await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.CreateTableAsync(
                    "Rejected", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken));
            }
            else
            {
                await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.CreateTableAsync(
                    "Rejected", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken));
            }
        }

        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<string> ReadNativeSecurityCatalogAsync()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Schema.mdb"),
            new AccessReaderOptions("Native123") { UseLockFile = false },
            TestContext.Current.CancellationToken);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: TestContext.Current.CancellationToken);
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: TestContext.Current.CancellationToken);
        string[] owners = objects.AsEnumerable().Select(row => string.Format(
            CultureInfo.InvariantCulture,
            "OBJECT:{0}:{1}:{2}",
            row["Id"],
            row["Name"],
            row["Owner"] is byte[] owner ? Convert.ToHexString(owner) : "NULL")).ToArray();
        string[] aces = permissions.AsEnumerable().Select(row => string.Format(
            CultureInfo.InvariantCulture,
            "ACE:{0}:{1}:{2}:{3}",
            row["ObjectId"],
            Convert.ToHexString(Assert.IsType<byte[]>(row["SID"])),
            row["ACM"],
            row["FInheritable"])).ToArray();
        return string.Join("\n", owners.Concat(aces));
    }

    private static async Task<string[]> ReadSecurityAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions("Native123") { UseLockFile = false },
            TestContext.Current.CancellationToken);
        return await ReadSecurityAsync(reader);
    }

    private static async Task<string[]> ReadSecurityAsync(AccessReader reader)
    {
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: TestContext.Current.CancellationToken);
        DataRow table = Assert.Single(objects.AsEnumerable(), row => string.Equals(
            Convert.ToString(row["Name"], CultureInfo.InvariantCulture), "Added", StringComparison.Ordinal));
        long objectId = Convert.ToInt64(table["Id"], CultureInfo.InvariantCulture);
        string owner = Convert.ToHexString(Assert.IsType<byte[]>(table["Owner"]));
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: TestContext.Current.CancellationToken);
        return permissions.AsEnumerable()
            .Where(row => Convert.ToInt64(row["ObjectId"], CultureInfo.InvariantCulture) == objectId)
            .Select(row => string.Concat(
                "ACE:",
                Convert.ToHexString(Assert.IsType<byte[]>(row["SID"])),
                ":",
                Convert.ToInt64(row["ACM"], CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
                ":",
                Convert.ToBoolean(row["FInheritable"], CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture)))
            .Append("OWNER:" + owner)
            .OrderBy(entry => entry, StringComparer.Ordinal)
            .ToArray();
    }
}
