namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Native Jet3 catalog creation inherits existing security without synthesizing grants.</summary>
public sealed class NativeJet3CatalogSecurityTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets malformed security cases in direct and caller-owned transactions.</summary>
    public static TheoryData<string, WriteMode> Corruptions()
    {
        var cases = new TheoryData<string, WriteMode>();
        foreach (string corruption in new[] { "database-owner", "container-owner", "duplicate-sid", "no-inheritance" })
        {
            cases.Add(corruption, WriteMode.Direct);
            cases.Add(corruption, WriteMode.ExplicitCommit);
        }

        return cases;
    }

    [Fact]
    public async Task CreatedTable_UsesExactNativeOwnerAndInheritedPermissions()
    {
        await using MemoryStream stream = await CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        SecuritySnapshot security = await ReadSecurityAsync(stream);
        Assert.Equal(security.DatabaseOwner, security.TableOwner);
        Assert.NotEmpty(security.Expected);
        Assert.Equal(security.Expected, security.Actual);
        Assert.DoesNotContain(security.Actual, permission => permission.Sid is "7110" or "7010");
    }

    [Fact]
    public async Task RestrictedInheritance_ExcludesNoninheritableAndPrefersExplicitOwner()
    {
        await using MemoryStream stream = await CopyFixtureAsync();
        await SeedRestrictedInheritanceAsync(stream);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        SecuritySnapshot security = await ReadSecurityAsync(stream);
        Assert.Equal(security.DatabaseOwner, security.TableOwner);
        Assert.Equal(security.Expected, security.Actual);
        Permission owner = Assert.Single(security.Actual, permission => permission.Sid == security.DatabaseOwner);
        Assert.Equal(8, owner.Acm);
        Assert.DoesNotContain(security.Actual, permission => permission.Sid == "027F" || permission.Acm == 0xFFFFF);
        Assert.All(security.Actual, permission => Assert.False(permission.Inheritable));
    }

    [Theory]
    [MemberData(nameof(Corruptions))]
    public async Task MalformedNativeSecurity_RefusesWithoutChangingBytesOrCallerWork(string corruption, WriteMode mode)
    {
        await using MemoryStream stream = await CopyFixtureAsync();
        await CorruptAsync(stream, corruption);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(Ct) : null;
            if (transaction is not null)
            {
                await writer.InsertRowAsync("Native", new RowValues { ["Id"] = 4 }, Ct);
            }

            async Task CreateAsync() => await writer.CreateTableAsync("Rejected", [new ColumnDefinition("Id", typeof(int))], Ct);
            if (corruption == "no-inheritance")
            {
                await Assert.ThrowsAsync<NotSupportedException>(CreateAsync);
            }
            else
            {
                await Assert.ThrowsAsync<JetCorruptDataException>(CreateAsync);
            }

            Assert.Equal(before, stream.ToArray());
            if (transaction is not null)
            {
                Assert.False(transaction.IsCommitted);
                Assert.False(transaction.IsRolledBack);
                Assert.Equal(1, await writer.DeleteRowsAsync("Native", "Id", 4, Ct));
                await writer.InsertRowAsync("Native", new RowValues { ["Id"] = 5 }, Ct);
                await transaction.RollbackAsync(Ct);
            }
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task FreshJet3Bootstrap_InheritsNativePermissions()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        Assert.Contains(objects.AsEnumerable(), row => Equals(row["Name"], "MSysACEs"));
        Assert.Equal(new byte[] { 3, 1 }, Assert.IsType<byte[]>(Assert.Single(objects.AsEnumerable(), row => Equals(row["Name"], "Added"))["Owner"]));
        SecuritySnapshot security = await ReadSecurityAsync(stream);
        Assert.Equal(security.Expected, security.Actual);
    }

    private static async Task<MemoryStream> CopyFixtureAsync()
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb");
        var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, Ct), Ct);
        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
    }

    private static async Task<SecuritySnapshot> ReadSecurityAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        string owner = Convert.ToHexString(Assert.IsType<byte[]>(Assert.Single(objects.AsEnumerable(), row => Equals(row["Name"], "MSysDb"))["Owner"]));
        DataRow table = Assert.Single(objects.AsEnumerable(), row => Equals(row["Name"], "Added"));
        int tableId = Assert.IsType<int>(table["Id"]);
        Permission[] inherited = permissions.AsEnumerable()
            .Where(row => Equals(row["ObjectId"], Constants.SystemObjects.TablesParentId) && Assert.IsType<bool>(row["FInheritable"]))
            .Select(ReadPermission).ToArray();
        bool explicitOwner = inherited.Any(permission => permission.Sid == owner);
        Permission[] expected = inherited
            .Where(permission => permission.Sid != "0204" || !explicitOwner)
            .Select(permission => permission with { Sid = permission.Sid == "0204" ? owner : permission.Sid, Inheritable = false })
            .OrderBy(permission => permission.Sid, StringComparer.Ordinal).ToArray();
        Permission[] actual = permissions.AsEnumerable().Where(row => Equals(row["ObjectId"], tableId))
            .Select(ReadPermission).OrderBy(permission => permission.Sid, StringComparer.Ordinal).ToArray();
        return new SecuritySnapshot(owner, Convert.ToHexString(Assert.IsType<byte[]>(table["Owner"])), expected, actual);
    }

    private static Permission ReadPermission(DataRow row)
        => new(Convert.ToHexString(Assert.IsType<byte[]>(row["SID"])), Assert.IsType<int>(row["ACM"]), Assert.IsType<bool>(row["FInheritable"]));

    private static async Task SeedRestrictedInheritanceAsync(MemoryStream stream)
    {
        await using AccessWriter writer = await OpenWriterAsync(stream);
        TableDef objects = await writer.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
        ColumnInfo name = Assert.IsType<ColumnInfo>(objects.FindColumn("Name"));
        ColumnInfo ownerColumn = Assert.IsType<ColumnInfo>(objects.FindColumn("Owner"));
        byte[]? owner = null;
        await writer.Database.OwnedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                if (Decode(writer, row.Page, row.Location.RowStart, row.Location.RowSize, name) == "MSysDb")
                {
                    owner = Convert.FromHexString(Decode(writer, row.Page, row.Location.RowStart, row.Location.RowSize, ownerColumn));
                }

                return new ValueTask<bool>(true);
            },
            Ct);
        Assert.NotNull(owner);
        Assert.False(owner.AsSpan().SequenceEqual(new byte[] { 2, 4 }));
        long acesPage = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysACEs", Ct);
        TableDef aces = await writer.Database.TableDefs.ReadRequiredTableDefAsync(acesPage, "MSysACEs", Ct);
        ColumnInfo objectId = Assert.IsType<ColumnInfo>(aces.FindColumn("ObjectId"));
        ColumnInfo sidColumn = Assert.IsType<ColumnInfo>(aces.FindColumn("SID"));
        ColumnInfo acm = Assert.IsType<ColumnInfo>(aces.FindColumn("ACM"));
        ColumnInfo inherit = Assert.IsType<ColumnInfo>(aces.FindColumn("FInheritable"));
        bool explicitOwner = false;
        bool ownerPlaceholder = false;
        var pages = new Dictionary<long, byte[]>();
        await writer.Database.OwnedPages.ForEachLiveTableRowAsync(
            acesPage,
            (row, _) =>
            {
                if (Decode(writer, row.Page, row.Location.RowStart, row.Location.RowSize, objectId) != "251658241")
                {
                    return new ValueTask<bool>(true);
                }

                byte[] bytes = GetPage(pages, row.Location.PageNumber, row.Page, writer.Database.Format.PageSize);
                string sid = Decode(writer, row.Page, row.Location.RowStart, row.Location.RowSize, sidColumn);
                bool isOwner = sid == Convert.ToHexString(owner);
                ownerPlaceholder |= sid == "0204";
                explicitOwner |= isOwner;
                Assert.True(RowDecodePlan.TryParseRowLayout(writer.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, true, out RowLayout layout));
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(writer.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, layout, acm);
                int mask = sid == "0204" ? 0xFFFFF : 16;
                if (isOwner)
                {
                    mask = 8;
                    int inheritOffset = row.Location.RowStart + layout.NullMaskPos + (inherit.ColNum / 8);
                    bytes[inheritOffset] = (byte)(bytes[inheritOffset] | (1 << (inherit.ColNum % 8)));
                }

                JetDatabaseWriter.Schema.JetTypeInfo.Wi32(bytes, row.Location.RowStart + slice.DataStart, mask);
                return new ValueTask<bool>(true);
            },
            Ct);
        foreach (KeyValuePair<long, byte[]> page in pages)
        {
            await writer.Pager.WritePageAsync(page.Key, page.Value, Ct);
        }

        Assert.True(ownerPlaceholder, "The seed must exercise an inherited owner placeholder alongside an explicit owner.");
        if (!explicitOwner)
        {
            await InsertAceAsync(writer, acesPage, aces, owner, 8, inheritable: true);
        }

        await InsertAceAsync(writer, acesPage, aces, [2, 127], int.MaxValue, inheritable: false);
    }

    private static async Task InsertAceAsync(AccessWriter writer, long page, TableDef definition, byte[] sid, int mask, bool inheritable)
    {
        object[] values = definition.CreateNullValueRow();
        definition.SetValueByName(values, "ObjectId", Constants.SystemObjects.TablesParentId);
        definition.SetValueByName(values, "SID", sid);
        definition.SetValueByName(values, "ACM", mask);
        definition.SetValueByName(values, "FInheritable", inheritable);
        await writer.Services.Indexes.InsertSystemRowAndMaintainAsync(page, definition, "MSysACEs", values, cancellationToken: Ct);
    }

    private static async Task CorruptAsync(MemoryStream stream, string corruption)
    {
        await using AccessWriter writer = await OpenWriterAsync(stream);
        bool ownerFault = corruption is "database-owner" or "container-owner";
        string table = ownerFault ? "MSysObjects" : "MSysACEs";
        long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(table, Ct);
        TableDef definition = await writer.Database.TableDefs.ReadRequiredTableDefAsync(page, table, Ct);
        ColumnInfo target = Assert.IsType<ColumnInfo>(definition.FindColumn(ownerFault ? "Owner" : "FInheritable"));
        ColumnInfo selector = Assert.IsType<ColumnInfo>(definition.FindColumn(ownerFault ? "Name" : "ObjectId"));
        ColumnInfo? sidColumn = definition.FindColumn("SID");
        var pages = new Dictionary<long, byte[]>();
        byte[]? firstSid = null;
        int changed = 0;
        string selectedObject = corruption switch
        {
            "database-owner" => "MSysDb",
            "container-owner" => "Tables",
            _ => "251658241",
        };
        await writer.Database.OwnedPages.ForEachLiveTableRowAsync(
            page,
            (row, _) =>
            {
                string selected = Decode(writer, row.Page, row.Location.RowStart, row.Location.RowSize, selector);
                if (selected != selectedObject)
                {
                    return new ValueTask<bool>(true);
                }

                Assert.True(RowDecodePlan.TryParseRowLayout(writer.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, true, out RowLayout layout));
                byte[] bytes = GetPage(pages, row.Location.PageNumber, row.Page, writer.Database.Format.PageSize);
                if (corruption == "duplicate-sid")
                {
                    if (Decode(writer, row.Page, row.Location.RowStart, row.Location.RowSize, target) != "True")
                    {
                        return new ValueTask<bool>(true);
                    }

                    ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(writer.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, layout, Assert.IsType<ColumnInfo>(sidColumn));
                    Assert.Equal(2, slice.DataLen);
                    if (firstSid is null)
                    {
                        firstSid = row.Page.AsSpan(row.Location.RowStart + slice.DataStart, slice.DataLen).ToArray();
                    }
                    else
                    {
                        firstSid.CopyTo(bytes, row.Location.RowStart + slice.DataStart);
                        changed++;
                    }
                }
                else
                {
                    int maskOffset = row.Location.RowStart + layout.NullMaskPos + (target.ColNum / 8);
                    bytes[maskOffset] = (byte)(bytes[maskOffset] & ~(1 << (target.ColNum % 8)));
                    changed++;
                }

                return new ValueTask<bool>(true);
            },
            Ct);
        Assert.True(changed > 0);
        foreach (KeyValuePair<long, byte[]> entry in pages)
        {
            await writer.Pager.WritePageAsync(entry.Key, entry.Value, Ct);
        }
    }

    private static string Decode(AccessWriter writer, byte[] page, int start, int length, ColumnInfo column)
        => ScalarColumnReader.DecodeSimpleColumnValue(writer.Database.Format, page, start, length, column);

    private static byte[] GetPage(Dictionary<long, byte[]> pages, long number, byte[] source, int size)
    {
        if (!pages.TryGetValue(number, out byte[]? bytes))
        {
            bytes = source.AsSpan(0, size).ToArray();
            pages.Add(number, bytes);
        }

        return bytes;
    }

    private sealed record Permission(string Sid, int Acm, bool Inheritable);

    private sealed record SecuritySnapshot(string DatabaseOwner, string TableOwner, Permission[] Expected, Permission[] Actual);
}
