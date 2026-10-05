namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// <c>Query&lt;T&gt;</c> results are async-only. Enumerating one synchronously (<c>foreach</c>,
/// <c>ToList()</c>) or running a synchronous LINQ terminal on one (<c>Count()</c>,
/// <c>First()</c>, <c>Sum(...)</c>, ...) throws <see cref="NotSupportedException"/> naming
/// <c>ToListAsync</c>, and the matching async terminal where there is one, before a page is
/// read, so nothing blocks a thread on the async read stack and no option brings that back.
/// </summary>
/// <param name="db">The shared database cache.</param>
public sealed class AsyncOnlyQueryTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string ItemTable = "AoItem";
    private const string TagTable = "AoTag";

    [Theory]
    [InlineData("Root", 3)]
    [InlineData("Where", 2)]
    [InlineData("OrderBy", 3)]
    [InlineData("SkipTake", 1)]
    [InlineData("Select", 3)]
    [InlineData("Include", 3)]
    public async Task SyncEnumeration_Throws_NamingToListAsync(string shape, int expectedRows)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using var counting = new CountingStream(temp);
        await using AccessReader reader = await OpenReaderAsync(counting, ct);
        counting.Reset();

        NotSupportedException viaForeach = Assert.Throws<NotSupportedException>(() => Enumerate(Compose(reader, shape)));
        NotSupportedException viaToList = Assert.Throws<NotSupportedException>(() => Compose(reader, shape).ToList());
        NotSupportedException viaToArray = Assert.Throws<NotSupportedException>(() => Compose(reader, shape).ToArray());

        Assert.Contains("ToListAsync", viaForeach.Message, StringComparison.Ordinal);
        Assert.Contains("ToListAsync", viaToList.Message, StringComparison.Ordinal);
        Assert.Contains("ToListAsync", viaToArray.Message, StringComparison.Ordinal);
        Assert.Equal(0L, counting.BytesRead);

        // The same query still runs through the async terminal.
        Assert.Equal(expectedRows, (await Compose(reader, shape).ToListAsync(ct)).Count);
    }

    [Theory]
    [InlineData("Count", "CountAsync")]
    [InlineData("LongCount", "LongCountAsync")]
    [InlineData("Any", "AnyAsync")]
    [InlineData("First", "FirstAsync")]
    [InlineData("FirstOrDefault", "FirstOrDefaultAsync")]
    [InlineData("Single", "SingleAsync")]
    [InlineData("SingleOrDefault", "SingleOrDefaultAsync")]
    [InlineData("Min", "MinAsync")]
    [InlineData("Max", "MaxAsync")]
    [InlineData("Sum", "SumAsync")]
    [InlineData("Average", "AverageAsync")]
    [InlineData("All", null)]
    [InlineData("Contains", null)]
    [InlineData("Last", null)]
    [InlineData("ElementAt", null)]
    [InlineData("Aggregate", null)]
    public async Task SyncTerminal_Throws_NamingTheAsyncTerminal(string terminal, string? asyncTerminal)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using var counting = new CountingStream(temp);
        await using AccessReader reader = await OpenReaderAsync(counting, ct);
        IQueryable<AoItem> query = reader.Query<AoItem>(ItemTable).Where(i => i.Score >= 10);
        counting.Reset();

        NotSupportedException error = Assert.Throws<NotSupportedException>(() => RunSyncTerminal(query, terminal));

        Assert.Contains($"'{terminal}'", error.Message, StringComparison.Ordinal);
        Assert.Contains("ToListAsync", error.Message, StringComparison.Ordinal);
        if (asyncTerminal is not null)
        {
            Assert.Contains(asyncTerminal, error.Message, StringComparison.Ordinal);
        }

        Assert.Equal(0L, counting.BytesRead);
    }

    [Fact]
    public async Task SyncTerminal_AfterProjectionOrInclude_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // A projection used to replay a sync terminal in memory over the read rows, and an
        // include to load its navigation first; both are refused like any other query.
        NotSupportedException projected = Assert.Throws<NotSupportedException>(
            () => reader.Query<AoItem>(ItemTable).Select(i => i.Name).Count());
        NotSupportedException included = Assert.Throws<NotSupportedException>(
            () => reader.Query<AoItem>(ItemTable).Include(i => i.Tags).First());

        Assert.Contains("CountAsync", projected.Message, StringComparison.Ordinal);
        Assert.Contains("FirstAsync", included.Message, StringComparison.Ordinal);
        Assert.Equal(3, await reader.Query<AoItem>(ItemTable).Select(i => i.Name).CountAsync(ct));
        AoItem first = await reader.Query<AoItem>(ItemTable).OrderBy(i => i.Id).Include(i => i.Tags).FirstAsync(ct);
        Assert.Equal(2, first.Tags.Count);
    }

    [Fact]
    public async Task ProviderExecute_Throws_NamingToListAsync()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);
        IQueryable<AoItem> query = reader.Query<AoItem>(ItemTable).OrderBy(i => i.Id);

        NotSupportedException untyped = Assert.Throws<NotSupportedException>(() => query.Provider.Execute(query.Expression));
        NotSupportedException typed = Assert.Throws<NotSupportedException>(() => query.Provider.Execute<IEnumerable<AoItem>>(query.Expression));

        Assert.Contains("ToListAsync", untyped.Message, StringComparison.Ordinal);
        Assert.Contains("ToListAsync", typed.Message, StringComparison.Ordinal);
    }

    private static IQueryable<object> Compose(AccessReader reader, string shape)
    {
        IQueryable<AoItem> items = reader.Query<AoItem>(ItemTable);
        return shape switch
        {
            "Root" => items,
            "Where" => items.Where(i => i.Score >= 20),
            "OrderBy" => items.OrderBy(i => i.Name),
            "SkipTake" => items.OrderBy(i => i.Id).Skip(1).Take(1),
            "Select" => items.Select(i => i.Name),
            "Include" => items.Include(i => i.Tags),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "Unknown query shape."),
        };
    }

    private static object? RunSyncTerminal(IQueryable<AoItem> query, string terminal) => terminal switch
    {
        "Count" => query.Count(),
        "LongCount" => query.LongCount(),
        "Any" => query.Any(),
        "First" => query.First(),
        "FirstOrDefault" => query.FirstOrDefault(),
        "Single" => query.Single(i => i.Id == 1),
        "SingleOrDefault" => query.SingleOrDefault(i => i.Id == 1),
        "Min" => query.Min(i => i.Score),
        "Max" => query.Max(i => i.Score),
        "Sum" => query.Sum(i => i.Score),
        "Average" => query.Average(i => i.Score),
        "All" => query.All(i => i.Score > 0),
        "Contains" => query.Contains(new AoItem()),
        "Last" => query.Last(),
        "ElementAt" => query.ElementAt(0),
        "Aggregate" => query.Aggregate((a, _) => a),
        _ => throw new ArgumentOutOfRangeException(nameof(terminal), terminal, "Unknown terminal."),
    };

    private static void Enumerate(IEnumerable<object> rows)
    {
        foreach (object row in rows)
        {
            _ = row;
        }
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(Stream stream, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(Stream stream, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
    }

    private async Task<MemoryStream> BuildAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            ItemTable,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50), new("Score", typeof(int))],
            ct);
        await writer.CreateTableAsync(
            TagTable,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ItemId", typeof(int)), new("Label", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_AoTag_AoItem", ItemTable, "Id", TagTable, "ItemId"),
            ct);

        await writer.InsertRowsAsync(ItemTable, [[1, "alpha", 10], [2, "beta", 20], [3, "gamma", 30]], ct);
        await writer.InsertRowsAsync(TagTable, [[10, 1, "red"], [11, 1, "blue"], [12, 2, "green"]], ct);
        return temp;
    }

    internal sealed class AoItem
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Score { get; set; }

        public List<AoTag> Tags { get; set; } = [];
    }

#pragma warning disable CA1812 // Only the include loader creates it, through reflection.
    internal sealed class AoTag
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public int ItemId { get; set; }

        public string Label { get; set; } = string.Empty;
    }
}
