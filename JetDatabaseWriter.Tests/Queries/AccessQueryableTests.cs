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
