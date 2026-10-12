namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>ACE catalog mutations retain the database's native security identities and permissions.</summary>
public sealed class NativeAceCatalogSecurityTests
{
    [Fact]
    public async Task CreatedTable_UsesNativeOwnerAndInheritedPermissions()
    {
        await using MemoryStream stream = await CopyFixtureAsync();
        SecuritySnapshot native = await ReadSecurityAsync(stream, "Table4");
        Assert.Equal(native.DatabaseOwner, native.TableOwner);
        Assert.NotEmpty(native.ExpectedPermissions);
        Assert.Equal(native.ExpectedPermissions, native.TablePermissions);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        SecuritySnapshot created = await ReadSecurityAsync(stream, "Added");
        Assert.Equal(native.DatabaseOwner, created.DatabaseOwner);
        Assert.Equal(native.DatabaseOwner, created.TableOwner);
        Assert.Equal(native.ExpectedPermissions, created.TablePermissions);
    }

    [Theory]
    [InlineData("Add")]
    [InlineData("Rename")]
    [InlineData("Drop")]
    public async Task SchemaEdit_PreservesNativeOwnerAndPermissions(string operation)
    {
        await using MemoryStream stream = await CopyFixtureAsync();
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Note", typeof(string), maxLength: 50), new ColumnDefinition("Spare", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Added", new RowValues { ["Id"] = 1, ["Note"] = "Owned row", ["Spare"] = 7 }, TestContext.Current.CancellationToken);
        }

        SecuritySnapshot before = await ReadSecurityAsync(stream, "Added");
        Assert.Equal(before.DatabaseOwner, before.TableOwner);
        Assert.Equal(before.ExpectedPermissions, before.TablePermissions);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            switch (operation)
            {
                case "Add":
                    await writer.AddColumnAsync("Added", new ColumnDefinition("Another", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "Rename":
                    await writer.RenameColumnAsync("Added", "Note", "Label", TestContext.Current.CancellationToken);
                    break;
                case "Drop":
                    await writer.DropColumnAsync("Added", "Spare", TestContext.Current.CancellationToken);
                    break;
                default:
                    throw new ArgumentException("Unknown schema edit.", nameof(operation));
            }
        }

        SecuritySnapshot after = await ReadSecurityAsync(stream, "Added");
        Assert.Equal(before.DatabaseOwner, after.DatabaseOwner);
        Assert.Equal(before.TableOwner, after.TableOwner);
        Assert.Equal(before.TablePermissions, after.TablePermissions);
    }

    [Fact]
    public async Task FreshAceBootstrap_HasOwnersAndInheritedTablePermissions()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        SecuritySnapshot created = await ReadSecurityAsync(stream, "Added", checkBootstrapOwners: true);
        Assert.Equal(created.DatabaseOwner, created.TableOwner);
        Assert.NotEmpty(created.ExpectedPermissions);
        Assert.Equal(created.ExpectedPermissions, created.TablePermissions);
    }

    private static async Task<MemoryStream> CopyFixtureAsync()
    {
        byte[] fixture = await File.ReadAllBytesAsync(TestDatabases.TestV2010, TestContext.Current.CancellationToken);
        var stream = new MemoryStream();
        await stream.WriteAsync(fixture, TestContext.Current.CancellationToken);
        stream.Position = 0;
        return stream;
    }

    private static async Task<SecuritySnapshot> ReadSecurityAsync(MemoryStream stream, string tableName, bool checkBootstrapOwners = false)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: TestContext.Current.CancellationToken);
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: TestContext.Current.CancellationToken);
        string owner = ReadOwner(objects, "MSysDb");
        DataRow table = Assert.Single(objects.AsEnumerable(), row => string.Equals(Assert.IsType<string>(row["Name"]), tableName, StringComparison.Ordinal));
        int objectId = Assert.IsType<int>(table["Id"]);
        string tableOwner = Convert.ToHexString(Assert.IsType<byte[]>(table["Owner"]));
        if (checkBootstrapOwners)
        {
            foreach (string objectName in new[] { "Tables", "MSysObjects", "MSysACEs", "MSysQueries", "MSysRelationships" })
            {
                Assert.NotEmpty(ReadOwner(objects, objectName));
            }
        }

        byte[] header = stream.ToArray().AsSpan(0, 4096).ToArray();
        string placeholder = Convert.ToHexString(Jet4SecuritySid.GetOwnerPlaceholder(JetFormat.FromHeader(header), header));
        Permission[] inherited = permissions.AsEnumerable()
            .Where(row => Assert.IsType<int>(row["ObjectId"]) == Constants.SystemObjects.TablesParentId && Assert.IsType<bool>(row["FInheritable"]))
            .Select(ReadPermission)
            .ToArray();
        bool explicitOwner = inherited.Any(permission => string.Equals(permission.Sid, owner, StringComparison.Ordinal));
        Permission[] expected = inherited
            .Where(permission => !string.Equals(permission.Sid, placeholder, StringComparison.Ordinal) || !explicitOwner)
            .Select(permission => permission with { Sid = string.Equals(permission.Sid, placeholder, StringComparison.Ordinal) ? owner : permission.Sid, Inheritable = false })
            .OrderBy(permission => permission.Sid, StringComparer.Ordinal)
            .ToArray();
        Permission[] actual = permissions.AsEnumerable()
            .Where(row => Assert.IsType<int>(row["ObjectId"]) == objectId)
            .Select(ReadPermission)
            .OrderBy(permission => permission.Sid, StringComparer.Ordinal)
            .ToArray();
        return new SecuritySnapshot(owner, tableOwner, expected, actual);
    }

    private static string ReadOwner(DataTable objects, string objectName)
    {
        DataRow row = Assert.Single(objects.AsEnumerable(), row => string.Equals(Assert.IsType<string>(row["Name"]), objectName, StringComparison.Ordinal));
        return Convert.ToHexString(Assert.IsType<byte[]>(row["Owner"]));
    }

    private static Permission ReadPermission(DataRow row)
        => new(Convert.ToHexString(Assert.IsType<byte[]>(row["SID"])), Assert.IsType<int>(row["ACM"]), Assert.IsType<bool>(row["FInheritable"]));

    private sealed record SecuritySnapshot(string DatabaseOwner, string TableOwner, Permission[] ExpectedPermissions, Permission[] TablePermissions);

    private sealed record Permission(string Sid, int Acm, bool Inheritable);
}
