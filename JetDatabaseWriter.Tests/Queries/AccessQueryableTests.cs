namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for the <see cref="IQueryable{T}"/> entity query returned by
/// <c>reader.Query&lt;T&gt;(...)</c>: filtering (with index inference), ordering, paging,
/// async terminal operators, async enumeration, and the unsupported-operator behavior.
/// </summary>
/// <param name="db">The <see cref="DatabaseCache"/> instance used to provide cached database connections for the tests.</param>
public sealed class AccessQueryableTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task Where_FiltersRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem").Where(i => i.Score >= 20).ToListAsync(ct);

        Assert.Equal(5, result.Count);
        Assert.All(result, i => Assert.True(i.Score >= 20));
    }

    [Fact]
    public async Task OrderBy_SortsAscending()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Score).ToListAsync(ct);

        int[] expected = [2, 4, 1, 6, 5, 3];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task OrderByDescending_SortsDescending()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem").OrderByDescending(i => i.Score).ToListAsync(ct);

        Assert.Equal(50, result[0].Score);
        Assert.Equal(10, result[^1].Score);
    }

    [Fact]
    public async Task OrderBy_ThenBy_BreaksTiesByName()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem")
            .OrderBy(i => i.Score)
            .ThenBy(i => i.Name)
            .ToListAsync(ct);

        int[] expected = [2, 4, 6, 1, 5, 3];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task Skip_Take_PageInOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem")
            .OrderBy(i => i.Id)
            .Skip(2)
            .Take(2)
            .ToListAsync(ct);

        int[] expected = [3, 4];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task CountAsync_CountsMatches()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        int count = await reader.Query<JdwItem>("JdwItem").Where(i => i.Score >= 30).CountAsync(ct);

        Assert.Equal(4, count);
    }

    [Fact]
    public async Task AnyAsync_ReflectsExistence()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.True(await reader.Query<JdwItem>("JdwItem").AnyAsync(ct));
        Assert.False(await reader.Query<JdwItem>("JdwItem").Where(i => i.Score > 1000).AnyAsync(ct));
    }

    [Fact]
    public async Task FirstOrDefaultAsync_ReturnsTopOfOrdering()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem? top = await reader.Query<JdwItem>("JdwItem").OrderByDescending(i => i.Score).FirstOrDefaultAsync(ct);

        Assert.NotNull(top);
        Assert.Equal(3, top.Id);
    }

    [Fact]
    public async Task SingleOrDefaultAsync_ReturnsNullWhenNoMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem? match = await reader.Query<JdwItem>("JdwItem").Where(i => i.Id == 999).SingleOrDefaultAsync(ct);

        Assert.Null(match);
    }

    [Fact]
    public async Task AwaitForeach_EnumeratesAllRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        var ids = new List<int>();
        await foreach (JdwItem item in reader.Query<JdwItem>("JdwItem").AsAsyncEnumerable().WithCancellation(ct))
        {
            ids.Add(item.Id);
        }

        Assert.Equal(6, ids.Count);
    }

    [Fact]
    public async Task UnsupportedOperator_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await reader.Query<JdwItem>("JdwItem").Select(i => i.Name).ToListAsync(ct));
    }

    [Fact]
    public async Task Take_BeforeWhere_TakesThenFilters()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // OrderBy fixes the row order to 1..6; Take(3) keeps {1,2,3}; the later Where then
        // filters only those three (ids 1 and 3 score >= 30), not the whole table.
        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem")
            .OrderBy(i => i.Id)
            .Take(3)
            .Where(i => i.Score >= 30)
            .ToListAsync(ct);

        int[] expected = [1, 3];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task Take_BeforeSkip_PagesWithinTakenWindow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // Order is 1..6; Take(4) => {1,2,3,4}; Skip(2) within that window => {3,4}.
        // A fixed filter->page collapse would instead yield {3,4,5,6}.
        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem")
            .OrderBy(i => i.Id)
            .Take(4)
            .Skip(2)
            .ToListAsync(ct);

        int[] expected = [3, 4];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task Skip_BeforeOrderBy_OrdersTheRemainder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // Skip applies to the unordered scan, then OrderBy sorts what remains. Compare
        // against the same operators run over the engine's scan order in memory.
        List<JdwItem> scan = await reader.Query<JdwItem>("JdwItem").ToListAsync(ct);
        int[] expected = scan.Skip(2).OrderBy(i => i.Score).ThenBy(i => i.Id).Select(i => i.Id).ToArray();

        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem")
            .Skip(2)
            .OrderBy(i => i.Score)
            .ThenBy(i => i.Id)
            .ToListAsync(ct);

        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task LeadingFilter_OrderThenTake_KeepsIndexFastPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // A leading Where still pushes into index inference; ordering and Take then run in
        // sequence over the filtered rows {1,3,5,6} -> ordered by Id -> first two {1,3}.
        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem")
            .Where(i => i.Score >= 30)
            .OrderBy(i => i.Id)
            .Take(2)
            .ToListAsync(ct);

        int[] expected = [1, 3];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task Count_CountsAllRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(6, reader.Query<JdwItem>("JdwItem").Count());
    }

    [Fact]
    public async Task Count_WithPredicate_CountsMatches()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(4, reader.Query<JdwItem>("JdwItem").Count(i => i.Score >= 30));
    }

    [Fact]
    public async Task Where_Count_AppliesLeadingFilter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(2, reader.Query<JdwItem>("JdwItem").Where(i => i.Score == 30).Count());
    }

    [Fact]
    public async Task Any_ReflectsExistence()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.True(reader.Query<JdwItem>("JdwItem").Any());
        Assert.True(reader.Query<JdwItem>("JdwItem").Any(i => i.Score == 50));
        Assert.False(reader.Query<JdwItem>("JdwItem").Any(i => i.Score > 1000));
    }

    [Fact]
    public async Task First_AfterOrdering_ReturnsTop()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem top = reader.Query<JdwItem>("JdwItem").OrderByDescending(i => i.Score).First();

        Assert.Equal(3, top.Id);
    }

    [Fact]
    public async Task First_WithPredicate_ReturnsMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(5, reader.Query<JdwItem>("JdwItem").First(i => i.Score == 40).Id);
    }

    [Fact]
    public async Task First_OnEmptyResult_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Throws<InvalidOperationException>(() =>
            reader.Query<JdwItem>("JdwItem").First(i => i.Score > 1000));
    }

    [Fact]
    public async Task Single_WithPredicate_ReturnsMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(3, reader.Query<JdwItem>("JdwItem").Single(i => i.Id == 3).Id);
    }

    [Fact]
    public async Task SingleOrDefault_WhenMultipleMatch_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // Ids 1 and 6 both score 30, so SingleOrDefault must throw rather than pick one.
        Assert.Throws<InvalidOperationException>(() =>
            reader.Query<JdwItem>("JdwItem").SingleOrDefault(i => i.Score == 30));
    }

    [Fact]
    public async Task Sum_Selector_AddsScores()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(180, reader.Query<JdwItem>("JdwItem").Sum(i => i.Score));
    }

    [Fact]
    public async Task MinMax_Selectors_ReturnExtremes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(10, reader.Query<JdwItem>("JdwItem").Min(i => i.Score));
        Assert.Equal(50, reader.Query<JdwItem>("JdwItem").Max(i => i.Score));
    }

    [Fact]
    public async Task Average_Selector_ReturnsMean()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(30.0, reader.Query<JdwItem>("JdwItem").Average(i => i.Score));
    }

    [Fact]
    public async Task ToList_SyncMaterializesAllRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwItem> rows = reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Id).ToList();

        Assert.Equal([1, 2, 3, 4, 5, 6], rows.Select(i => i.Id).ToArray());
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
    }

    private async Task<MemoryStream> BuildAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "JdwItem",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50), new("Score", typeof(int))],
            [new IndexDefinition("IX_JdwItem_Score", "Score")],
            ct);
        await writer.InsertRowsAsync(
            "JdwItem",
            [
                [1, "alice", 30],
                [2, "bob", 10],
                [3, "carol", 50],
                [4, "dave", 20],
                [5, "eve", 40],
                [6, "adam", 30],
            ],
            ct);

        return temp;
    }

    internal sealed class JdwItem
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Score { get; set; }
    }
}
