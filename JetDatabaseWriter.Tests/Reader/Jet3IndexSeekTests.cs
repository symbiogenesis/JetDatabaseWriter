namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Indexes.Collation;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Jet3 public seeks, inferred queries and relationship operations agree with row scans.</summary>
/// <param name="cache">Caches fixture bytes.</param>
public sealed class Jet3IndexSeekTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    public static TheoryData<string> Fixtures => [TestDatabases.MdbtoolsNwind, TestDatabases.TestV1997];

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task AccessAuthoredSingleColumnIndexes_SeekMatchesScan(string fixture)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        JetFormat format;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            format = harness.Database.Format;
        }

        Assert.True(format.SupportsIndexSeeks);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        int checkedKeys = 0;
        foreach (string table in await reader.ListTablesAsync(ct))
        {
            IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(table, ct);
            foreach (IndexMetadata index in await reader.ListIndexesAsync(table, ct))
            {
                if (index.Columns.Count != 1 || index.IsForeignKey || index.FirstDp <= 0)
                {
                    continue;
                }

                string name = index.Columns[0].Name;
                ColumnMetadata metadata = columns.Single(value => value.Name == name);
                if (metadata.ClrType != typeof(string) && metadata.ClrType != typeof(int))
                {
                    continue;
                }

                ColumnInfo column = await CollationTestSupport.ReadColumnAsync(stream, table, name, ct);
                using DataTable scanned = await reader.ReadDataTableAsync(table, cancellationToken: ct);
                foreach (object key in scanned.Rows.Cast<DataRow>().Select(row => row[name]).Where(value => value is not DBNull).Distinct().Take(4))
                {
                    byte[] sought = IndexKeyEncoder.EncodeColumnEntry(format, column, key, index.Columns[0].IsAscending);
                    int expected = scanned.Rows.Cast<DataRow>().Count(row =>
                        IndexKeyEncoder.EncodeColumnEntry(format, column, row[name], index.Columns[0].IsAscending).SequenceEqual(sought));
                    var actual = new List<object[]>();
                    await foreach (object[] row in reader.SeekRowsAsync(table, index.Name, [key], ct))
                    {
                        actual.Add(row);
                    }

                    Assert.Equal(expected, actual.Count);
                    int ordinal = scanned.Columns[name]!.Ordinal;
                    Assert.All(actual, row => Assert.Equal(sought, IndexKeyEncoder.EncodeColumnEntry(format, column, row[ordinal], index.Columns[0].IsAscending)));
                    checkedKeys++;
                }
            }
        }

        Assert.True(checkedKeys > 0);
    }

    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task TextForeignKeys_CascadeAndIncludeUseGeneral97Keys(WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.TestV1997, ct);
        stream.Position = 0;
        await using (AccessWriter seed = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await seed.CreateTableAsync("SeekParent", [new ColumnDefinition("Code", typeof(string), 40) { IsPrimaryKey = true }], ct);
            await seed.CreateTableAsync("SeekChild", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentCode", typeof(string), 40)], ct);
            await seed.CreateRelationshipAsync(new RelationshipDefinition("SeekRel", "SeekParent", "Code", "SeekChild", "ParentCode") { CascadeUpdates = true, CascadeDeletes = true }, ct);
            await seed.InsertRowsAsync("SeekParent", [["\u0152uvre"], ["r\u00e9sum\u00e9"]], ct);
            await seed.InsertRowsAsync("SeekChild", [[1, "\u0152uvre"], [2, "r\u00e9sum\u00e9"]], ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync("SeekChild", [9, "missing"], ct));
                await writer.InsertRowAsync("SeekChild", [3, "\u0152uvre"], ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("SeekParent", "Code", "\u0152uvre", new RowValues { ["Code"] = "caf\u00e9" }, ct));
                Assert.Equal(1, await writer.DeleteRowsAsync("SeekParent", "Code", "r\u00e9sum\u00e9", ct));
            }, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        List<SeekParent> parents = await reader.Query<SeekParent>("SeekParent").Where(row => row.Code == "caf\u00e9").Include(row => row.Children).ToListAsync(ct);
        SeekParent parent = Assert.Single(parents);
        Assert.Equal([1, 3], parent.Children.Select(row => row.Id).Order());
        Assert.All(parent.Children, child => Assert.Equal("caf\u00e9", child.ParentCode));
        List<SeekChild> children = await reader.Query<SeekChild>("SeekChild").Include(row => row.Parent).ToListAsync(ct);
        Assert.Equal([1, 3], children.Select(row => row.Id).Order());
        Assert.All(children, child => Assert.Equal("caf\u00e9", Assert.IsType<SeekParent>(child.Parent).Code));
        using DataTable scan = await reader.ReadDataTableAsync("SeekChild", cancellationToken: ct);
        Assert.Equal(scan.Rows.Count, children.Count);
    }

    [Fact]
    public async Task NorthwindQueryAndInclude_MatchShipperOrderScan()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.MdbtoolsNwind, ct);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        using DataTable shippers = await reader.ReadDataTableAsync("Shippers", cancellationToken: ct);
        using DataTable orders = await reader.ReadDataTableAsync("Orders", cancellationToken: ct);
        DataRow shipper = shippers.Rows[0];
        int id = (int)shipper["ShipperID"];
        List<Shippers> result = await reader.Query<Shippers>("Shippers").Where(row => row.ShipperID == id).Include(row => row.Orders).ToListAsync(ct);
        Shippers found = Assert.Single(result);
        Assert.Equal(shipper["CompanyName"], found.CompanyName);
        Assert.Equal(orders.Rows.Cast<DataRow>().Where(row => row["ShipVia"] is int value && value == id).Select(row => (int)row["OrderID"]).Order(),
            found.Orders.Select(row => row.OrderID).Order());
    }

#pragma warning disable CA1812 // These entity types are constructed by the query materializer.
    internal sealed class SeekParent
    {
        public string Code { get; set; } = string.Empty;

        public List<SeekChild> Children { get; set; } = [];
    }

    internal sealed class SeekChild
    {
        public int Id { get; set; }

        public string ParentCode { get; set; } = string.Empty;

        public SeekParent? Parent { get; set; }
    }

    internal sealed class Shippers
    {
        public int ShipperID { get; set; }

        public string CompanyName { get; set; } = string.Empty;

        public List<Orders> Orders { get; set; } = [];
    }

    internal sealed class Orders
    {
        public int OrderID { get; set; }

        public int ShipVia { get; set; }
    }
#pragma warning restore CA1812
}
