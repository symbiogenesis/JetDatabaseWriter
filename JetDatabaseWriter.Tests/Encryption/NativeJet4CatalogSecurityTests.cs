namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
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
