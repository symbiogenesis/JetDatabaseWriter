namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// The operators of a <c>Query&lt;T&gt;</c> above the engine boundary (a <c>Select</c>
/// projection and the operators after it, or an operator the engine does not translate, such
/// as an ordering with a comparer) run natively over the rows the read streams, not through
/// <c>EnumerableQuery</c>. In every format each supported shape returns what LINQ to Objects
/// returns over the same rows in scan order, a second sequence that is itself a query
/// included; a <c>Take</c> after a projection stops the read; and <c>GroupBy</c>,
/// <c>SelectMany</c>, the joins, the indexed overloads and an <c>Include</c> above the
/// boundary throw before a byte is read, naming the operator.
/// </summary>
/// <param name="db">The shared database cache.</param>
public sealed class QueryTailTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Table = "TailItem";
    private const string ParentTable = "TailParent";
    private const string TagTable = "TailTag";

    private static readonly DatabaseFormat[] Formats = [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];
    private static readonly int[] ExtraIds = [3, 99, 3, 7];
    private static readonly int?[] ScoreProbe = [10, 30, null, 99];
    private static readonly string[] RedOnly = ["RED"];

    /// <summary>
    /// The supported tail shapes. Each takes the root query and a second query over the same
    /// table, and boxes value-type results with <c>Cast&lt;object?&gt;()</c>, itself a tail
    /// operator. Leading filters use columns no index covers, so both sides see scan order.
    /// </summary>
    private static readonly Dictionary<string, Func<IQueryable<TailItem>, IQueryable<TailItem>, IQueryable<object?>>> Shapes = new(StringComparer.Ordinal)
    {
        ["Select"] = static (q, _) => q.Select(i => i.Name),
        ["SelectValue"] = static (q, _) => q.Select(i => i.Id * 10).Cast<object?>(),
        ["SelectAnonymous"] = static (q, _) => q.Select(i => new { i.Id, i.Category }),
        ["SelectWhere"] = static (q, _) => q.Select(i => i.Score).Where(s => s > 20).Cast<object?>(),
        ["WhereSelect"] = static (q, _) => q.Where(i => i.Category == "Red").Select(i => i.Name),
        ["SelectOrderBy"] = static (q, _) => q.Select(i => i.Category).OrderBy(c => c.Length),
        ["OrderByComparer"] = static (q, _) => q.OrderBy(i => i.Name, StringComparer.Ordinal),
        ["OrderByDescendingComparerThenBy"] = static (q, _) => q.OrderByDescending(i => i.Category, StringComparer.OrdinalIgnoreCase).ThenBy(i => i.Id),
        ["EngineOrderByThenByComparer"] = static (q, _) => q.OrderBy(i => i.Score).ThenBy(i => i.Name, StringComparer.Ordinal),
        ["EngineOrderingThenByDescendingComparer"] = static (q, _) => q.OrderBy(i => i.Category).ThenBy(i => i.Score).ThenByDescending(i => i.Name, StringComparer.Ordinal),
        ["SelectOrderByDescendingThenByDescending"] = static (q, _) => q.Select(i => new { i.Score, i.Id }).OrderByDescending(x => x.Score).ThenByDescending(x => x.Id),
        ["Order"] = static (q, _) => q.Select(i => i.Score).Order().Cast<object?>(),
        ["OrderDescendingComparer"] = static (q, _) => q.Select(i => i.Category).OrderDescending(StringComparer.Ordinal),
        ["SelectSkipTake"] = static (q, _) => q.Select(i => i.Id).Skip(2).Take(5).Cast<object?>(),
        ["SkipNegativeTakeZero"] = static (q, _) => q.Select(i => i.Id).Skip(-1).Take(0).Cast<object?>(),
        ["SkipWhileTakeWhile"] = static (q, _) => q.Select(i => i.Id).SkipWhile(id => id != 1).TakeWhile(id => id != 4).Cast<object?>(),
        ["SkipLastTakeLast"] = static (q, _) => q.Select(i => i.Id).SkipLast(2).TakeLast(4).Cast<object?>(),
        ["Distinct"] = static (q, _) => q.Select(i => i.Category).Distinct(),
        ["DistinctComparer"] = static (q, _) => q.Select(i => i.Category).Distinct(StringComparer.OrdinalIgnoreCase),
        ["DistinctNullable"] = static (q, _) => q.Select(i => i.Score).Distinct().Cast<object?>(),
        ["Reverse"] = static (q, _) => q.Select(i => i.Id).Reverse().Cast<object?>(),
        ["DefaultIfEmpty"] = static (q, _) => q.Where(i => i.Score > 1000).Select(i => i.Id).DefaultIfEmpty().Cast<object?>(),
        ["DefaultIfEmptyWithValue"] = static (q, _) => q.Where(i => i.Score > 1000).Select(i => i.Name).DefaultIfEmpty("none"),
        ["DefaultIfEmptyNotEmpty"] = static (q, _) => q.Select(i => i.Id).DefaultIfEmpty(-1).Cast<object?>(),
        ["DefaultIfEmptyEntity"] = static (q, _) => q.Where(i => i.Score > 1000).DefaultIfEmpty(),
        ["OfType"] = static (q, _) => q.Select(i => (object?)i.Name).OfType<string>(),
        ["OfTypeValue"] = static (q, _) => q.Select(i => (object?)i.Score).OfType<int>().Cast<object?>(),
        ["Cast"] = static (q, _) => q.Select(i => (object)i.Id).Cast<int>().Cast<object?>(),
        ["AppendPrepend"] = static (q, _) => q.Select(i => i.Id).Append(100).Prepend(-100).Cast<object?>(),
        ["ConcatInMemory"] = static (q, _) => q.Select(i => i.Id).Concat(ExtraIds).Cast<object?>(),
        ["ConcatQuery"] = static (q, other) => q.Where(i => i.Score > 20).Concat(other.Where(i => i.Score <= 20)),
        ["UnionQuery"] = static (q, other) => q.Select(i => i.Category).Union(other.Select(i => i.Category.ToUpperInvariant())),
        ["UnionComparer"] = static (q, _) => q.Select(i => i.Category).Union(RedOnly, StringComparer.OrdinalIgnoreCase),
        ["UnionEntitiesById"] = static (q, other) => q.Where(i => i.Score >= 30).Union(other.Where(i => i.Category == "blue"), TailItemIdComparer.Instance),
        ["IntersectInMemory"] = static (q, _) => q.Select(i => i.Score).Intersect(ScoreProbe).Cast<object?>(),
        ["IntersectQuery"] = static (q, other) => q.Select(i => i.Id).Intersect(other.Where(i => i.Category == "blue").Select(i => i.Id)).Cast<object?>(),
        ["ExceptComparer"] = static (q, _) => q.Select(i => i.Category).Except(RedOnly, StringComparer.OrdinalIgnoreCase),
        ["ExceptQuery"] = static (q, other) => q.Select(i => i.Score).Except(other.Where(i => i.Category == "Red").Select(i => i.Score)).Cast<object?>(),
        ["Chain"] = static (q, _) => q.Select(i => new { i.Id, i.Score }).Where(x => x.Score != null).OrderBy(x => x.Score).ThenByDescending(x => x.Id).Skip(1).Take(4).Select(x => x.Id).Cast<object?>(),
        ["EngineTakeThenSelect"] = static (q, _) => q.OrderBy(i => i.Id).Take(5).Select(i => i.Name),
    };

    /// <summary>Gets every pair of database format and supported tail shape.</summary>
    public static TheoryData<DatabaseFormat, string> FormatsAndShapes
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, string>();
            foreach (DatabaseFormat format in Formats)
            {
                foreach (string shape in Shapes.Keys)
                {
                    data.Add(format, shape);
                }
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(FormatsAndShapes))]
    public async Task TailShape_MatchesLinqToObjects(DatabaseFormat format, string shape)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(format, fillerRows: 0, ct);
        await using AccessReader reader = await OpenReaderAsync(stream, ct);
        Func<IQueryable<TailItem>, IQueryable<TailItem>, IQueryable<object?>> compose = Shapes[shape];

        // The reference runs the same operators with LINQ to Objects over the rows the scan
        // returns. The second source is a separate list, as the second query reads separate
        // entities, so reference equality never relates the two sides.
        List<TailItem> rows = await reader.Query<TailItem>(Table).ToListAsync(ct);
        List<TailItem> otherRows = await reader.Query<TailItem>(Table).ToListAsync(ct);
        List<object?> expected = [.. compose(rows.AsQueryable(), otherRows.AsQueryable())];

        List<object?> actual = await compose(reader.Query<TailItem>(Table), reader.Query<TailItem>(Table)).ToListAsync(ct);

        Assert.Equal(expected.Select(Describe), actual.Select(Describe));
    }

    [Theory]
    [InlineData("GroupBy", "GroupBy")]
    [InlineData("SelectMany", "SelectMany")]
    [InlineData("Join", "Join")]
    [InlineData("GroupJoin", "GroupJoin")]
    [InlineData("Zip", "Zip")]
    [InlineData("Chunk", "Chunk")]
    [InlineData("DistinctBy", "DistinctBy")]
    [InlineData("IndexedSelect", "Select")]
    [InlineData("IndexedWhere", "Where")]
    [InlineData("IndexedTakeWhile", "TakeWhile")]
    [InlineData("IndexedSkipWhile", "SkipWhile")]
    [InlineData("TakeRangeEngine", "Take")]
    [InlineData("TakeRangeTail", "Take")]
    public async Task UnsupportedOperator_Throws_NamingItAndAsAsyncEnumerable(string shape, string operatorName)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(DatabaseFormat.AceAccdb, fillerRows: 0, ct);
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await OpenReaderAsync(counting, ct);
        IQueryable<object?> query = ComposeUnsupported(reader.Query<TailItem>(Table), reader.Query<TailItem>(Table), shape);
        counting.Reset();

        NotSupportedException error = await Assert.ThrowsAsync<NotSupportedException>(async () => await query.ToListAsync(ct));

        Assert.Contains($"'{operatorName}'", error.Message, StringComparison.Ordinal);
        Assert.Contains("AsAsyncEnumerable()", error.Message, StringComparison.Ordinal);
        Assert.Equal(0L, counting.BytesRead);
    }

    [Theory]
    [InlineData("Concat", false)]
    [InlineData("Concat", true)]
    [InlineData("Union", false)]
    [InlineData("Union", true)]
    [InlineData("Intersect", false)]
    [InlineData("Intersect", true)]
    [InlineData("Except", false)]
    [InlineData("Except", true)]
    public async Task UnsupportedNestedSecondSource_ThrowsBeforeEitherSourceIsRead(string operation, bool takeFirst)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(DatabaseFormat.AceAccdb, fillerRows: 0, ct);
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await OpenReaderAsync(counting, ct);
        IQueryable<TailItem> source = reader.Query<TailItem>(Table);
        IQueryable<TailItem> second = source.Concat(source.Where((_, index) => index > 0));
        IQueryable<TailItem> query = operation switch
        {
            "Concat" => source.Concat(second),
            "Union" => source.Union(second),
            "Intersect" => source.Intersect(second),
            "Except" => source.Except(second),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unknown set operator."),
        };
        counting.Reset();

        NotSupportedException error = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await (takeFirst ? query.Take(1) : query).ToListAsync(ct));

        Assert.Contains("'Where'", error.Message, StringComparison.Ordinal);
        Assert.Contains("AsAsyncEnumerable()", error.Message, StringComparison.Ordinal);
        Assert.Equal(0L, counting.BytesRead);
    }

    [Fact]
    public async Task UnsupportedOperator_RunsOverAsAsyncEnumerable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(DatabaseFormat.AceAccdb, fillerRows: 0, ct);
        await using AccessReader reader = await OpenReaderAsync(stream, ct);

        // The remedy the message names: the rows stream to the async LINQ operators.
        int groups = await reader.Query<TailItem>(Table)
            .AsAsyncEnumerable()
            .GroupBy(i => i.Category, StringComparer.OrdinalIgnoreCase)
            .CountAsync(ct);

        Assert.Equal(3, groups);
    }

    [Fact]
    public async Task Take_AfterProjection_StopsTheTableRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(DatabaseFormat.AceAccdb, fillerRows: 2000, ct);

        long engineFirst = await BytesReadAsync(stream, static q => q.Take(1).Select(i => i.Id), ct);
        long tailFirst = await BytesReadAsync(stream, static q => q.Select(i => i.Id).Take(1), ct);
        long everyRow = await BytesReadAsync(stream, static q => q.Select(i => i.Id), ct);

        // The projection streams, so a Take after it stops the read where an engine Take
        // stops it, instead of reading the whole table into a list first.
        Assert.Equal(engineFirst, tailFirst);
        Assert.True(tailFirst < everyRow, $"Take(1) after Select read {tailFirst} bytes; the whole projection read {everyRow}.");
    }

    [Fact]
    public async Task Select_AfterInclude_ProjectsTheLoadedNavigations()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildRelatedAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        // The include loads onto the rows the read returns, and the projection runs over them.
        List<int> tagCounts = await reader.Query<TailParent>(ParentTable)
            .OrderBy(p => p.Id)
            .Include(p => p.Tags)
            .Select(p => p.Tags.Count)
            .ToListAsync(ct);

        Assert.Equal([2, 1, 0], tagCounts);
    }

    [Fact]
    public async Task Include_AboveTheBoundary_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildRelatedAsync(ct);
        await using var counting = new CountingStream(temp);
        await using AccessReader reader = await OpenReaderAsync(counting, ct);
        IQueryable<TailParent> query = reader.Query<TailParent>(ParentTable).Distinct().Include(p => p.Tags);
        counting.Reset();

        // Distinct runs after the read, so the include would have no entities of its own to load onto.
        NotSupportedException error = await Assert.ThrowsAsync<NotSupportedException>(async () => await query.ToListAsync(ct));

        Assert.Contains("'Include'", error.Message, StringComparison.Ordinal);
        Assert.Equal(0L, counting.BytesRead);
    }

    private static IQueryable<object?> ComposeUnsupported(IQueryable<TailItem> q, IQueryable<TailItem> other, string shape) => shape switch
    {
        "GroupBy" => q.GroupBy(i => i.Category),
        "SelectMany" => q.SelectMany(i => i.Category).Cast<object?>(),
        "Join" => q.Join(other, i => i.Id, o => o.Id, (i, _) => i.Name),
        "GroupJoin" => q.GroupJoin(other, i => i.Id, o => o.Id, (i, _) => i.Name),
        "Zip" => q.Zip(other).Cast<object?>(),
        "Chunk" => q.Chunk(3),
        "DistinctBy" => q.DistinctBy(i => i.Category),
        "IndexedSelect" => q.Select((i, index) => i.Id + index).Cast<object?>(),
        "IndexedWhere" => q.Where((_, index) => index > 1),
        "IndexedTakeWhile" => q.Select(i => i.Id).TakeWhile((_, index) => index < 3).Cast<object?>(),
        "IndexedSkipWhile" => q.SkipWhile((_, index) => index < 3),
        "TakeRangeEngine" => q.Take(..3),
        "TakeRangeTail" => q.Select(i => i.Id).Take(..3).Cast<object?>(),
        _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "Unknown query shape."),
    };

    private static string Describe(object? value) => value switch
    {
        null => "<null>",
        TailItem item => string.Create(
            CultureInfo.InvariantCulture,
            $"item {item.Id} {item.Name ?? "<null>"} {item.Score?.ToString(CultureInfo.InvariantCulture) ?? "<null>"} {item.Category}"),
        IFormattable formattable => formattable.ToString(format: null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };

    private static async Task<long> BytesReadAsync(MemoryStream stream, Func<IQueryable<TailItem>, IQueryable<int>> compose, CancellationToken ct)
    {
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await OpenReaderAsync(counting, ct);
        counting.Reset();
        _ = await compose(reader.Query<TailItem>(Table)).ToListAsync(ct);
        return counting.BytesRead;
    }

    private static async Task<MemoryStream> CreateItemsAsync(DatabaseFormat format, int fillerRows, CancellationToken ct)
    {
        var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync(
            Table,
            [
                new("Id", typeof(int)) { IsPrimaryKey = true },
                new("Name", typeof(string), maxLength: 50),
                new("Score", typeof(int)),
                new("Category", typeof(string), maxLength: 20),
            ],
            ct);

        // Inserted out of key order, with duplicate scores, null names and scores, and
        // categories that differ only in case.
        List<object?[]> rows =
        [
            [7, "grace", 30, "Red"],
            [3, "carol", 10, "blue"],
            [11, DBNull.Value, 50, "red"],
            [1, "alice", 30, "Blue"],
            [9, "ivan", DBNull.Value, "green"],
            [5, "eve", 20, "Red"],
            [12, "Zed", 10, "green"],
            [2, "bob", 40, "blue"],
            [8, "heidi", DBNull.Value, "Red"],
            [4, "dave", 20, "Green"],
            [10, "judy", 30, "red"],
            [6, "Frank", 50, "blue"],
        ];
        for (int i = 0; i < fillerRows; i++)
        {
            rows.Add([1000 + i, string.Create(CultureInfo.InvariantCulture, $"filler row {i:D5} with some padding text"), i % 50, "filler"]);
        }

        await writer.InsertRowsAsync(Table, rows, ct);
        return stream;
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

    private async Task<MemoryStream> BuildRelatedAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            ParentTable,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            TagTable,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("TailParentId", typeof(int)), new("Label", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_TailTag_TailParent", ParentTable, "Id", TagTable, "TailParentId"),
            ct);

        await writer.InsertRowsAsync(ParentTable, [[1, "alpha"], [2, "beta"], [3, "gamma"]], ct);
        await writer.InsertRowsAsync(TagTable, [[10, 1, "red"], [11, 1, "blue"], [12, 2, "green"]], ct);
        return temp;
    }

    internal sealed class TailItem
    {
        public int Id { get; set; }

        public string? Name { get; set; }

        public int? Score { get; set; }

        public string Category { get; set; } = string.Empty;
    }

    internal sealed class TailParent
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<TailTag> Tags { get; set; } = [];
    }

#pragma warning disable CA1812 // Only the include loader creates it, through reflection.
    internal sealed class TailTag
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public int TailParentId { get; set; }

        public string Label { get; set; } = string.Empty;
    }

    /// <summary>Equates items by key, so a union of entities from two reads dedupes them.</summary>
    private sealed class TailItemIdComparer : IEqualityComparer<TailItem>
    {
        public static readonly TailItemIdComparer Instance = new();

        public bool Equals(TailItem? x, TailItem? y) => x?.Id == y?.Id;

        public int GetHashCode(TailItem obj) => obj.Id;
    }
}
