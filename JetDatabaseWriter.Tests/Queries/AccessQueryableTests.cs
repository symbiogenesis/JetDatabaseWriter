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
    public async Task Query_BeforeOrdering_IsNotOrderedQueryable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        IQueryable<JdwItem> root = reader.Query<JdwItem>("JdwItem");
        IQueryable<JdwItem> filtered = root.Where(i => i.Score >= 20);

        // The query is not ordered until an ordering operator runs, so ThenBy/
        // ThenByDescending (which require IOrderedQueryable) are not reachable.
        Assert.IsNotAssignableFrom<IOrderedQueryable<JdwItem>>(root);
        Assert.IsNotAssignableFrom<IOrderedQueryable<JdwItem>>(filtered);
    }

    [Fact]
    public async Task OrderBy_ProducesOrderedQueryable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        IOrderedQueryable<JdwItem> ordered = reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Score);

        // OrderBy establishes an ordering, so the result is an IOrderedQueryable and a
        // further ThenBy keeps that contract.
        Assert.IsAssignableFrom<IOrderedQueryable<JdwItem>>(ordered);
        Assert.IsAssignableFrom<IOrderedQueryable<JdwItem>>(ordered.ThenBy(i => i.Name));
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
    public async Task Select_ProjectsColumn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // OrderBy stays in the engine; the trailing Select projects each row in memory.
        List<string> names = await reader.Query<JdwItem>("JdwItem")
            .OrderBy(i => i.Id)
            .Select(i => i.Name)
            .ToListAsync(ct);

        Assert.Equal(["alice", "bob", "carol", "dave", "eve", "adam"], names);
    }

    [Fact]
    public async Task Where_OrderBy_Select_FiltersOrdersThenProjects()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // The leading Where pushes into index inference; the projection shapes a DTO.
        List<(int Id, string Name)> rows = await reader.Query<JdwItem>("JdwItem")
            .Where(i => i.Score >= 30)
            .OrderBy(i => i.Id)
            .Select(i => new ValueTuple<int, string>(i.Id, i.Name))
            .ToListAsync(ct);

        Assert.Equal([1, 3, 5, 6], rows.Select(r => r.Id).ToArray());
        Assert.Equal("carol", rows[1].Name);
    }

    [Fact]
    public async Task Select_Where_ProjectsThenFiltersInMemory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // Select changes the element type, so the following Where runs in memory over the
        // projected scores rather than as an engine filter.
        List<int> scores = await reader.Query<JdwItem>("JdwItem")
            .Select(i => i.Score)
            .Where(s => s >= 30)
            .ToListAsync(ct);

        Assert.Equal([30, 30, 40, 50], scores.OrderBy(s => s).ToArray());
    }

    [Fact]
    public async Task Select_Count_ReducesProjection()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(6, reader.Query<JdwItem>("JdwItem").Select(i => i.Name).Count());
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

    [Fact]
    public async Task FirstAsync_ReturnsFirstInOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem first = await reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Id).FirstAsync(ct);

        Assert.Equal(1, first.Id);
    }

    [Fact]
    public async Task FirstAsync_WithPredicate_ReturnsMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem match = await reader.Query<JdwItem>("JdwItem").FirstAsync(i => i.Score == 40, ct);

        Assert.Equal(5, match.Id);
    }

    [Fact]
    public async Task FirstAsync_OnEmptyResult_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await reader.Query<JdwItem>("JdwItem").Where(i => i.Score > 1000).FirstAsync(ct));
    }

    [Fact]
    public async Task FirstOrDefaultAsync_WithPredicate_ReturnsNullWhenNoMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem? match = await reader.Query<JdwItem>("JdwItem").FirstOrDefaultAsync(i => i.Id == 999, ct);

        Assert.Null(match);
    }

    [Fact]
    public async Task SingleAsync_WithPredicate_ReturnsMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem match = await reader.Query<JdwItem>("JdwItem").SingleAsync(i => i.Id == 3, ct);

        Assert.Equal("carol", match.Name);
    }

    [Fact]
    public async Task SingleAsync_WhenMultipleMatch_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // Ids 1 and 6 both score 30, so SingleAsync must throw.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await reader.Query<JdwItem>("JdwItem").Where(i => i.Score == 30).SingleAsync(ct));
    }

    [Fact]
    public async Task SingleOrDefaultAsync_WithPredicate_ReturnsMatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem? match = await reader.Query<JdwItem>("JdwItem").SingleOrDefaultAsync(i => i.Id == 3, ct);

        Assert.NotNull(match);
        Assert.Equal("carol", match.Name);
    }

    [Fact]
    public async Task CountAsync_WithPredicate_CountsMatches()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(4, await reader.Query<JdwItem>("JdwItem").CountAsync(i => i.Score >= 30, ct));
    }

    [Fact]
    public async Task LongCountAsync_CountsAllRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(6L, await reader.Query<JdwItem>("JdwItem").LongCountAsync(ct));
        Assert.Equal(2L, await reader.Query<JdwItem>("JdwItem").LongCountAsync(i => i.Score == 30, ct));
    }

    [Fact]
    public async Task AnyAsync_WithPredicate_ReflectsExistence()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.True(await reader.Query<JdwItem>("JdwItem").AnyAsync(i => i.Score == 50, ct));
        Assert.False(await reader.Query<JdwItem>("JdwItem").AnyAsync(i => i.Score > 1000, ct));
    }

    [Fact]
    public async Task ToArrayAsync_MaterializesInOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem[] array = await reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Id).ToArrayAsync(ct);

        Assert.Equal([1, 2, 3, 4, 5, 6], array.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task ToDictionaryAsync_KeyedById()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Dictionary<int, JdwItem> byId = await reader.Query<JdwItem>("JdwItem").ToDictionaryAsync(i => i.Id, ct);

        Assert.Equal(6, byId.Count);
        Assert.Equal("carol", byId[3].Name);
    }

    [Fact]
    public async Task ToDictionaryAsync_KeyAndElement()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Dictionary<int, string> byId = await reader.Query<JdwItem>("JdwItem").ToDictionaryAsync(i => i.Id, i => i.Name, ct);

        Assert.Equal("eve", byId[5]);
    }

    [Fact]
    public async Task SumAsync_AddsScores()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(180, await reader.Query<JdwItem>("JdwItem").SumAsync(i => i.Score, ct));
    }

    [Fact]
    public async Task AverageAsync_ReturnsMean()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(30.0, await reader.Query<JdwItem>("JdwItem").AverageAsync(i => i.Score, ct));
    }

    [Fact]
    public async Task MinMaxAsync_ReturnExtremes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        Assert.Equal(10, await reader.Query<JdwItem>("JdwItem").MinAsync(i => i.Score, ct));
        Assert.Equal(50, await reader.Query<JdwItem>("JdwItem").MaxAsync(i => i.Score, ct));
    }

    [Fact]
    public async Task CountAsync_NoPredicate_CountsAllRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // Whole-table count takes the metadata fast path (live row-slot tally), not a scan
        // that maps every row to a POCO.
        Assert.Equal(6, await reader.Query<JdwItem>("JdwItem").CountAsync(ct));
        Assert.Equal(6L, await reader.Query<JdwItem>("JdwItem").LongCountAsync(ct));
    }

    [Fact]
    public async Task CountAsync_AfterDelete_CountsLiveRowsNotDeclared()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);

        await using (AccessWriter writer = await OpenWriterAsync(temp, ct))
        {
            await writer.DeleteRowsAsync("JdwItem", "Id", 2, ct);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // The whole-table fast path must report the live row count (5). The declared TDEF
        // row count is not decremented on delete, so a naive metadata read would wrongly
        // return 6.
        Assert.Equal(5, await reader.Query<JdwItem>("JdwItem").CountAsync(ct));
        Assert.Equal(5L, await reader.Query<JdwItem>("JdwItem").LongCountAsync(ct));
    }

    [Fact]
    public async Task OrderByPrimaryKey_Take_PagesInKeyOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // OrderBy(Id) is served straight from the unique primary-key index in key order, so
        // Take(3) yields the first three rows by Id without sorting the whole table.
        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem")
            .OrderBy(i => i.Id)
            .Take(3)
            .ToListAsync(ct);

        int[] expected = [1, 2, 3];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task OrderByPrimaryKey_ReturnsAllRowsInKeyOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // The index-served ordering must yield every live row exactly once, in key order.
        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Id).ToListAsync(ct);

        Assert.Equal([1, 2, 3, 4, 5, 6], result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task FirstAsync_OrderByPrimaryKey_ReturnsLeadingRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        JdwItem first = await reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Id).FirstAsync(ct);

        Assert.Equal(1, first.Id);
    }

    [Fact]
    public async Task OrderByPrimaryKeyDescending_FallsBackToMemory_StillCorrect()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // The primary-key index is ascending, so a descending order cannot be walked from it
        // and falls back to the in-memory sort, which must still produce the right order.
        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem").OrderByDescending(i => i.Id).ToListAsync(ct);

        Assert.Equal([6, 5, 4, 3, 2, 1], result.Select(i => i.Id).ToArray());
    }

    [Fact]
    public async Task OrderByNonUniqueColumn_PreservesStableTieOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // Score has a non-unique index, so it must NOT serve the ordering: the index would
        // break Score ties (ids 1 and 6 both score 30) in physical order, whereas LINQ keeps
        // them in source order. The in-memory stable sort must be preserved.
        List<JdwItem> result = await reader.Query<JdwItem>("JdwItem").OrderBy(i => i.Score).ToListAsync(ct);

        int[] expected = [2, 4, 1, 6, 5, 3];
        Assert.Equal(expected, result.Select(i => i.Id).ToArray());
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
