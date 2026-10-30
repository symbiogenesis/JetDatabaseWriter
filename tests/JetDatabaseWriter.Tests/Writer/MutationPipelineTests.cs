namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Composed row mutations keep ordinary, cascade and complex storage atomic.</summary>
public sealed class MutationPipelineTests
{
    [Fact]
    public async Task PreparedRows_AllTablesAreMeasuredBeforeTheFirstWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("First", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], ct);
        await writer.CreateTableAsync("Last", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], ct);
        ResolvedTable first = await writer.Services.Catalog.ResolveRequiredTableAsync("First", ct);
        ResolvedTable last = await writer.Services.Catalog.ResolveRequiredTableAsync("Last", ct);
        var plan = new MutationPlan();
        plan.ForTable(first.Entry.TDefPage, first.Definition, "First").Insert([7]);
        plan.ForTable(last.Entry.TDefPage, last.Definition, "Last").Insert([1, 2]);
        byte[] before = stream.ToArray();

        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.Services.Mutations.ApplyAsync(plan, ct));

        Assert.Equal(before, stream.ToArray());
        Assert.Empty(await writer.Services.Snapshots.ReadRowsAsync(first.Entry.TDefPage, ct));
        await AssertIndexesAsync(writer, first.Entry.TDefPage, ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SelfCascade_UpdateAndDeleteKeepSharedTableIndexesAndCountsCurrent(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("Nodes", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("Body", typeof(string))], ct);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("Tree", "Nodes", "Id", "Nodes", "ParentId") { CascadeUpdates = true, CascadeDeletes = true }, ct);
        await writer.InsertRowsAsync("Nodes", [[1, DBNull.Value, "root"], [2, 1, "child"], [3, 2, "leaf"]], ct);
        Assert.Equal(1, await writer.UpdateRowsAsync("Nodes", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 10 }, ct));
        ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("Nodes", ct);
        List<LocatedRow> rows = await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct);
        Assert.Equal(10, Assert.Single(rows, row => Equals(row.Values[0], 2)).Values[1]);
        Assert.Equal(2, Assert.Single(rows, row => Equals(row.Values[0], 3)).Values[1]);
        await AssertIndexesAsync(writer, table.Entry.TDefPage, ct);

        Assert.Equal(1, await writer.DeleteRowsAsync("Nodes", "Id", 10, ct));
        Assert.Empty(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
        await AssertIndexesAsync(writer, table.Entry.TDefPage, ct);
    }

    [Fact]
    public async Task CascadeAndComplexChanges_RollbackRestoresBytesAndGeneratedItemIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("Nodes", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], ct);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("Tree", "Nodes", "Id", "Nodes", "ParentId") { CascadeUpdates = true, CascadeDeletes = true }, ct);
        await writer.InsertRowsAsync("Nodes", [[1, DBNull.Value, DBNull.Value], [2, 1, DBNull.Value], [3, 2, DBNull.Value]], ct);
        foreach (int id in new[] { 1, 2, 3 })
        {
            await writer.AddMultiValueItemAsync("Nodes", "Tags", new Dictionary<string, object?> { ["Id"] = id }, $"tag{id}", ct);
        }

        byte[] before = stream.ToArray();
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Nodes", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 10 }, ct));
            await writer.ReplaceMultiValueItemAsync("Nodes", "Tags", new Dictionary<string, object?> { ["Id"] = 10 }, 1, "changed", ct);
            await writer.AddMultiValueItemAsync("Nodes", "Tags", new Dictionary<string, object?> { ["Id"] = 10 }, "temporary", ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("Nodes", "Id", 10, ct));
            await transaction.RollbackAsync(ct);
        }

        Assert.Equal(before, stream.ToArray());
        await writer.AddMultiValueItemAsync("Nodes", "Tags", new Dictionary<string, object?> { ["Id"] = 1 }, "retained", ct);
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            IReadOnlyList<MultiValueItem> items = await reader.GetMultiValueItemsAsync("Nodes", "Tags", ct);
            Assert.Equal(4, Assert.Single(items, item => Equals(item.Value, "retained")).Id);
            Assert.Equal(["retained", "tag1", "tag2", "tag3"], items.Select(item => Assert.IsType<string>(item.Value)).Order(StringComparer.Ordinal));
        }

        Assert.Equal(1, await writer.DeleteRowsAsync("Nodes", "Id", 1, ct));
        await using AccessReader empty = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        Assert.Empty(await empty.GetMultiValueItemsAsync("Nodes", "Tags", ct));
        ComplexColumnInfo column = Assert.Single(await empty.GetComplexColumnsAsync("Nodes", ct));
        ResolvedTable? flat = await writer.Services.Snapshots.ResolveTableAsync(column.FlatTableName, ct);
        Assert.NotNull(flat);
        await AssertIndexesAsync(writer, flat.Entry.TDefPage, ct);
    }

    private static async Task AssertIndexesAsync(AccessWriter writer, long page, CancellationToken ct)
    {
        foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, page, ct))
        {
            await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, page, root, ct);
        }
    }
}
