namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for the named-column (<see cref="RowValues"/>) insert API and the
/// multi-column predicate (<see cref="RowCriteria"/>) update/delete API added to
/// replace the positional/single-column primitive surface (audit finding #3).
/// Each test builds a fresh in-memory ACE database, writes via the new API, then
/// reads back to verify.
/// </summary>
public sealed class NamedRowAndCriteriaTests
{
    private const string TableName = "People";

    [Fact]
    public async Task InsertRow_RowValues_NamedColumnsAreOrderIndependent()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            // Columns deliberately out of schema order: Score, Name, Id.
            await writer.InsertRowAsync(
                TableName,
                new RowValues { ["Score"] = 95.5m, ["Name"] = "Alice", ["Id"] = 1 },
                TestContext.Current.CancellationToken);
        }

        DataRow row = await SingleRowAsync(ms, "Id = 1");
        Assert.Equal("Alice", row["Name"]);
        Assert.Equal(95.5m, row["Score"]);
    }

    [Fact]
    public async Task InsertRow_RowValues_OmittedColumnsBecomeNull()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            await writer.InsertRowAsync(
                TableName,
                new RowValues { ["Id"] = 7, ["Name"] = "Grace" },
                TestContext.Current.CancellationToken);
        }

        DataRow row = await SingleRowAsync(ms, "Id = 7");
        Assert.Equal("Grace", row["Name"]);
        Assert.Equal(DBNull.Value, row["Score"]);
    }

    [Fact]
    public async Task InsertRow_RowValues_UnknownColumnThrows()
    {
        await using var ms = new MemoryStream();
        await using AccessWriter writer = await CreateSeededAsync(ms, leaveOpen: false);

        ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(
                TableName,
                new RowValues { ["Id"] = 1, ["Nonexistent"] = "x" },
                TestContext.Current.CancellationToken));

        Assert.Contains("Nonexistent", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InsertRows_RowValues_FluentBuilderInsertsAll()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            int inserted = await writer.InsertRowsAsync(
                TableName,
                [
                    RowValues.Create().Set("Id", 1).Set("Name", "Alice").Set("Score", 10m),
                    RowValues.Create().Set("Id", 2).Set("Name", "Bob").Set("Score", 20m),
                ],
                TestContext.Current.CancellationToken);

            Assert.Equal(2, inserted);
        }

        Assert.Equal(2, await RowCountAsync(ms));
    }

    [Fact]
    public async Task UpdateRows_RowCriteria_MultiColumnAndFilter()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            await SeedPeopleAsync(writer);

            // WHERE Region = 'West' AND Score > 80
            int updated = await writer.UpdateRowsAsync(
                TableName,
                RowCriteria.Where("Region", "West").And(ColumnPredicate.GreaterThan("Score", 80m)),
                new RowValues { ["Name"] = "PROMOTED" },
                TestContext.Current.CancellationToken);

            Assert.Equal(1, updated);
        }

        DataTable dt = await ReadAllAsync(ms);

        // Only the West row with Score 90 should be renamed.
        DataRow promoted = dt.AsEnumerable().Single(r => (string)r["Name"] == "PROMOTED");
        Assert.Equal("West", promoted["Region"]);
        Assert.Equal(90m, promoted["Score"]);
    }

    [Fact]
    public async Task DeleteRows_RowCriteria_BetweenRange()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            await SeedPeopleAsync(writer);

            // Delete Score between 50 and 90 inclusive (rows: 60, 70, 90).
            int deleted = await writer.DeleteRowsAsync(
                TableName,
                RowCriteria.Where(ColumnPredicate.Between("Score", 50m, 90m)),
                TestContext.Current.CancellationToken);

            Assert.Equal(3, deleted);
        }

        DataTable dt = await ReadAllAsync(ms);
        decimal[] remaining = [.. dt.AsEnumerable().Select(r => (decimal)r["Score"]).OrderBy(s => s)];
        Assert.Equal([40m, 100m], remaining);
    }

    [Fact]
    public async Task DeleteRows_RowCriteria_InSet()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            await SeedPeopleAsync(writer);

            int deleted = await writer.DeleteRowsAsync(
                TableName,
                RowCriteria.Where(ColumnPredicate.In("Id", 1, 3, 5)),
                TestContext.Current.CancellationToken);

            Assert.Equal(3, deleted);
        }

        Assert.Equal(2, await RowCountAsync(ms));
    }

    [Fact]
    public async Task UpdateRows_RowCriteria_IsNullMatchesNullColumn()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            await writer.InsertRowAsync(TableName, new RowValues { ["Id"] = 1, ["Name"] = "HasRegion", ["Region"] = "East", ["Score"] = 1m }, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(TableName, new RowValues { ["Id"] = 2, ["Name"] = "NoRegion", ["Score"] = 2m }, TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                TableName,
                RowCriteria.Where(ColumnPredicate.IsNull("Region")),
                new RowValues { ["Region"] = "FILLED" },
                TestContext.Current.CancellationToken);

            Assert.Equal(1, updated);
        }

        DataRow row = await SingleRowAsync(ms, "Id = 2");
        Assert.Equal("FILLED", row["Region"]);
    }

    [Fact]
    public async Task DeleteRows_SingleColumnConvenience_StillWorks()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            await SeedPeopleAsync(writer);

            int deleted = await writer.DeleteRowsAsync(TableName, "Region", "East", TestContext.Current.CancellationToken);
            Assert.True(deleted > 0);
        }

        DataTable dt = await ReadAllAsync(ms);
        Assert.DoesNotContain(dt.AsEnumerable(), r => (string)r["Region"] == "East");
    }

    [Fact]
    public async Task UpdateRows_RowCriteria_NumericOperandTypeTolerant()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await CreateSeededAsync(ms))
        {
            await SeedPeopleAsync(writer);

            // Operand is int 80 while the column decodes to decimal; comparison
            // must still coerce and match Score >= 80 (rows 90 and 100).
            int updated = await writer.UpdateRowsAsync(
                TableName,
                RowCriteria.Where(ColumnPredicate.GreaterThanOrEqual("Score", 80)),
                new RowValues { ["Name"] = "HIGH" },
                TestContext.Current.CancellationToken);

            Assert.Equal(2, updated);
        }

        DataTable dt = await ReadAllAsync(ms);
        Assert.Equal(2, dt.AsEnumerable().Count(r => (string)r["Name"] == "HIGH"));
    }

    private static async Task<AccessWriter> CreateSeededAsync(MemoryStream ms, bool leaveOpen = true)
    {
        AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: leaveOpen,
            cancellationToken: TestContext.Current.CancellationToken);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 100),
            new("Region", typeof(string), maxLength: 50),
            new("Score", typeof(decimal)) { NumericScale = 2 },
        };

        await writer.CreateTableAsync(TableName, columns, TestContext.Current.CancellationToken);
        return writer;
    }

    private static async Task SeedPeopleAsync(AccessWriter writer)
    {
        (int Id, string Name, string Region, decimal Score)[] seed =
        [
            (1, "A", "West", 40m),
            (2, "B", "West", 90m),
            (3, "C", "East", 60m),
            (4, "D", "East", 70m),
            (5, "E", "North", 100m),
        ];

        foreach ((int id, string name, string region, decimal score) in seed)
        {
            await writer.InsertRowAsync(
                TableName,
                new RowValues { ["Id"] = id, ["Name"] = name, ["Region"] = region, ["Score"] = score },
                TestContext.Current.CancellationToken);
        }
    }

    private static async Task<DataTable> ReadAllAsync(MemoryStream ms)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);
        return await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
    }

    private static async Task<DataRow> SingleRowAsync(MemoryStream ms, string filter)
    {
        DataTable dt = await ReadAllAsync(ms);
        return dt.Select(filter).Single();
    }

    private static async Task<long> RowCountAsync(MemoryStream ms)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);
        return await reader.GetRealRowCountAsync(TableName, TestContext.Current.CancellationToken);
    }
}
