namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// The async aggregates of a <c>Query&lt;T&gt;</c> (<c>SumAsync</c>, <c>AverageAsync</c>,
/// <c>MinAsync</c> and <c>MaxAsync</c>) take expression selectors only and fold the rows as the
/// read returns them, with LINQ to Objects' rules: checked integer and decimal sums, float sums
/// accumulated in double, nulls skipped, and a query with no values giving 0, null or an
/// <see cref="InvalidOperationException"/>. <c>AllAsync</c>, <c>LastAsync</c>,
/// <c>LastOrDefaultAsync</c> and <c>ContainsAsync</c> complete the async terminals. In every
/// format each one returns, or throws, what LINQ to Objects does over the same rows.
/// </summary>
public sealed class QueryAggregateTests
{
    private const string Table = "AggItem";
    private const string SelectorFault = "The selector ran on row";

    private static readonly DatabaseFormat[] Formats = [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];
    private static readonly string[] AggregateNames = ["Sum", "Average", "Min", "Max"];
    private static readonly AggItem Probe = new() { Id = 1 };

    /// <summary>
    /// The queries each aggregate runs over: every row, no row, and the rows whose Score is
    /// null, so the empty-sequence and all-null rules are checked against LINQ to Objects too.
    /// </summary>
    private static readonly Dictionary<string, Func<IQueryable<AggItem>, IQueryable<AggItem>>> Filters = new(StringComparer.Ordinal)
    {
        ["AllRows"] = static q => q,
        ["NoRows"] = static q => q.Where(i => i.Id < 0),
        ["NullScores"] = static q => q.Where(i => i.Score == null),
    };

    /// <summary>
    /// Each aggregate as an async terminal and as the LINQ to Objects call it must agree with.
    /// Value-type results box to <see cref="object"/>, so the comparison also checks the result type.
    /// </summary>
    private static readonly Dictionary<string, AggregateCase> Cases = new(StringComparer.Ordinal)
    {
        ["SumInt"] = new(static async (q, ct) => await q.SumAsync(i => i.Id, ct), static r => r.Sum(i => i.Id)),
        ["SumNullableInt"] = new(static async (q, ct) => await q.SumAsync(i => i.Score, ct), static r => r.Sum(i => i.Score)),
        ["SumLong"] = new(static async (q, ct) => await q.SumAsync(i => i.Id * 3_000_000_000L, ct), static r => r.Sum(i => i.Id * 3_000_000_000L)),
        ["SumNullableLong"] = new(static async (q, ct) => await q.SumAsync(i => i.Score * 3_000_000_000L, ct), static r => r.Sum(i => i.Score * 3_000_000_000L)),
        ["SumFloat"] = new(static async (q, ct) => await q.SumAsync(i => i.Ratio, ct), static r => r.Sum(i => i.Ratio)),
        ["SumNullableFloat"] = new(static async (q, ct) => await q.SumAsync(i => (float?)i.Weight, ct), static r => r.Sum(i => (float?)i.Weight)),
        ["SumDouble"] = new(static async (q, ct) => await q.SumAsync(i => i.Id / 3.0, ct), static r => r.Sum(i => i.Id / 3.0)),
        ["SumNullableDouble"] = new(static async (q, ct) => await q.SumAsync(i => i.Weight, ct), static r => r.Sum(i => i.Weight)),
        ["SumDecimal"] = new(static async (q, ct) => await q.SumAsync(i => i.Id * 1.25m, ct), static r => r.Sum(i => i.Id * 1.25m)),
        ["SumNullableDecimal"] = new(static async (q, ct) => await q.SumAsync(i => i.Price, ct), static r => r.Sum(i => i.Price)),
        ["SumFloatAccumulatesInDouble"] = new(static async (q, ct) => await q.SumAsync(i => i.Id == 7 ? 16_777_216f : 1f, ct), static r => r.Sum(i => i.Id == 7 ? 16_777_216f : 1f)),
        ["SumIntOverflow"] = new(static async (q, ct) => await q.SumAsync(_ => int.MaxValue, ct), static r => r.Sum(_ => int.MaxValue)),
        ["SumNullableIntOverflow"] = new(static async (q, ct) => await q.SumAsync(_ => (int?)int.MaxValue, ct), static r => r.Sum(_ => (int?)int.MaxValue)),
        ["SumLongOverflow"] = new(static async (q, ct) => await q.SumAsync(_ => long.MaxValue, ct), static r => r.Sum(_ => long.MaxValue)),
        ["SumDecimalOverflow"] = new(static async (q, ct) => await q.SumAsync(_ => decimal.MaxValue, ct), static r => r.Sum(_ => decimal.MaxValue)),
        ["AverageInt"] = new(static async (q, ct) => await q.AverageAsync(i => i.Id, ct), static r => r.Average(i => i.Id)),
        ["AverageNullableInt"] = new(static async (q, ct) => await q.AverageAsync(i => i.Score, ct), static r => r.Average(i => i.Score)),
        ["AverageLong"] = new(static async (q, ct) => await q.AverageAsync(i => i.Id * 3_000_000_000L, ct), static r => r.Average(i => i.Id * 3_000_000_000L)),
        ["AverageNullableLong"] = new(static async (q, ct) => await q.AverageAsync(i => i.Score * 3_000_000_000L, ct), static r => r.Average(i => i.Score * 3_000_000_000L)),
        ["AverageFloat"] = new(static async (q, ct) => await q.AverageAsync(i => i.Ratio, ct), static r => r.Average(i => i.Ratio)),
        ["AverageNullableFloat"] = new(static async (q, ct) => await q.AverageAsync(i => (float?)i.Weight, ct), static r => r.Average(i => (float?)i.Weight)),
        ["AverageDouble"] = new(static async (q, ct) => await q.AverageAsync(i => i.Id / 3.0, ct), static r => r.Average(i => i.Id / 3.0)),
        ["AverageNullableDouble"] = new(static async (q, ct) => await q.AverageAsync(i => i.Weight, ct), static r => r.Average(i => i.Weight)),
        ["AverageDecimal"] = new(static async (q, ct) => await q.AverageAsync(i => i.Id * 1.25m, ct), static r => r.Average(i => i.Id * 1.25m)),
        ["AverageNullableDecimal"] = new(static async (q, ct) => await q.AverageAsync(i => i.Price, ct), static r => r.Average(i => i.Price)),
        ["AverageIntAccumulatesInLong"] = new(static async (q, ct) => await q.AverageAsync(_ => int.MaxValue, ct), static r => r.Average(_ => int.MaxValue)),
        ["AverageLongOverflow"] = new(static async (q, ct) => await q.AverageAsync(_ => long.MaxValue, ct), static r => r.Average(_ => long.MaxValue)),
        ["AverageNullableLongOverflow"] = new(static async (q, ct) => await q.AverageAsync(i => i.Score == null ? null : (long?)long.MaxValue, ct), static r => r.Average(i => i.Score == null ? null : (long?)long.MaxValue)),
        ["AverageDecimalOverflow"] = new(static async (q, ct) => await q.AverageAsync(_ => decimal.MaxValue, ct), static r => r.Average(_ => decimal.MaxValue)),
        ["AverageNullableDecimalOverflow"] = new(static async (q, ct) => await q.AverageAsync(i => i.Price == null ? null : (decimal?)decimal.MaxValue, ct), static r => r.Average(i => i.Price == null ? null : (decimal?)decimal.MaxValue)),
        ["MinInt"] = new(static async (q, ct) => await q.MinAsync(i => i.Id, ct), static r => r.Min(i => i.Id)),
        ["MaxInt"] = new(static async (q, ct) => await q.MaxAsync(i => i.Id, ct), static r => r.Max(i => i.Id)),
        ["MinNullableInt"] = new(static async (q, ct) => await q.MinAsync(i => i.Score, ct), static r => r.Min(i => i.Score)),
        ["MaxNullableInt"] = new(static async (q, ct) => await q.MaxAsync(i => i.Score, ct), static r => r.Max(i => i.Score)),
        ["MinString"] = new(static async (q, ct) => await q.MinAsync(i => i.Name, ct), static r => r.Min(i => i.Name)),
        ["MaxString"] = new(static async (q, ct) => await q.MaxAsync(i => i.Name, ct), static r => r.Max(i => i.Name)),
        ["MinNullableDecimal"] = new(static async (q, ct) => await q.MinAsync(i => i.Price, ct), static r => r.Min(i => i.Price)),
        ["MaxNullableDouble"] = new(static async (q, ct) => await q.MaxAsync(i => i.Weight, ct), static r => r.Max(i => i.Weight)),
        ["MinFloat"] = new(static async (q, ct) => await q.MinAsync(i => i.Ratio, ct), static r => r.Min(i => i.Ratio)),
        ["MinDoubleWithNaN"] = new(static async (q, ct) => await q.MinAsync(i => i.Id == 5 ? double.NaN : i.Id / 3.0, ct), static r => r.Min(i => i.Id == 5 ? double.NaN : i.Id / 3.0)),
        ["MaxDoubleWithNaN"] = new(static async (q, ct) => await q.MaxAsync(i => i.Id == 5 ? double.NaN : i.Id / 3.0, ct), static r => r.Max(i => i.Id == 5 ? double.NaN : i.Id / 3.0)),
        ["MinNullableDoubleWithNaN"] = new(static async (q, ct) => await q.MinAsync(i => i.Id == 5 ? double.NaN : i.Weight, ct), static r => r.Min(i => i.Id == 5 ? double.NaN : i.Weight)),
        ["MaxNullableDoubleWithNaN"] = new(static async (q, ct) => await q.MaxAsync(i => i.Id == 5 ? double.NaN : i.Weight, ct), static r => r.Max(i => i.Id == 5 ? double.NaN : i.Weight)),
        ["All"] = new(static async (q, ct) => await q.AllAsync(i => i.Score > 15, ct), static r => r.All(i => i.Score > 15)),
        ["AllHold"] = new(static async (q, ct) => await q.AllAsync(i => i.Id > 0, ct), static r => r.All(i => i.Id > 0)),
        ["Last"] = new(static async (q, ct) => await q.LastAsync(ct), static r => r.Last()),
        ["LastWithPredicate"] = new(static async (q, ct) => await q.LastAsync(i => i.Score > 15, ct), static r => r.Last(i => i.Score > 15)),
        ["LastAfterOrderBy"] = new(static async (q, ct) => await q.OrderBy(i => i.Id).LastAsync(ct), static r => r.OrderBy(i => i.Id).Last()),
        ["LastOrDefault"] = new(static async (q, ct) => await q.LastOrDefaultAsync(ct), static r => r.LastOrDefault()),
        ["LastOrDefaultWithPredicate"] = new(static async (q, ct) => await q.LastOrDefaultAsync(i => i.Score > 15, ct), static r => r.LastOrDefault(i => i.Score > 15)),
        ["LastOrDefaultValue"] = new(static async (q, ct) => await q.Select(i => i.Id).LastOrDefaultAsync(ct), static r => r.Select(i => i.Id).LastOrDefault()),
        ["ContainsValue"] = new(static async (q, ct) => await q.Select(i => i.Id).ContainsAsync(5, ct), static r => r.Select(i => i.Id).Contains(5)),
        ["ContainsMissingValue"] = new(static async (q, ct) => await q.Select(i => i.Id).ContainsAsync(-1, ct), static r => r.Select(i => i.Id).Contains(-1)),
        ["ContainsNull"] = new(static async (q, ct) => await q.Select(i => i.Name).ContainsAsync(null, ct), static r => r.Select(i => i.Name).Contains(null)),
        ["ContainsWithComparer"] = new(static async (q, ct) => await q.Select(i => i.Name).ContainsAsync("ALICE", StringComparer.OrdinalIgnoreCase, ct), static r => r.Select(i => i.Name).Contains("ALICE", StringComparer.OrdinalIgnoreCase)),
        ["ContainsEntity"] = new(static async (q, ct) => await q.ContainsAsync(Probe, ct), static r => r.Contains(Probe)),
        ["ContainsEntityWithComparer"] = new(static async (q, ct) => await q.ContainsAsync(Probe, AggItemIdComparer.Instance, ct), static r => r.Contains(Probe, AggItemIdComparer.Instance)),
    };

    /// <summary>Gets every pair of database format and aggregate.</summary>
    public static TheoryData<DatabaseFormat, string> FormatsAndCases
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, string>();
            foreach (DatabaseFormat format in Formats)
            {
                foreach (string aggregate in Cases.Keys)
                {
                    data.Add(format, aggregate);
                }
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(FormatsAndCases))]
    public async Task Aggregate_MatchesLinqToObjects(DatabaseFormat format, string aggregate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(format, fillerRows: 0, ct);
        await using AccessReader reader = await OpenReaderAsync(stream, ct);
        AggregateCase run = Cases[aggregate];

        var expected = new List<string>();
        var actual = new List<string>();
        foreach ((string filterName, Func<IQueryable<AggItem>, IQueryable<AggItem>> filter) in Filters)
        {
            // The reference runs the aggregate with LINQ to Objects over the rows the filtered
            // query returns, in the order the read returns them.
            List<AggItem> rows = await filter(reader.Query<AggItem>(Table)).ToListAsync(ct);
            string expectedOutcome = await OutcomeAsync(() => new ValueTask<object?>(run.Expected(rows)));
            string actualOutcome = await OutcomeAsync(() => run.Actual(filter(reader.Query<AggItem>(Table)), ct));
            expected.Add($"{filterName}: {expectedOutcome}");
            actual.Add($"{filterName}: {actualOutcome}");
        }

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task AverageFloatingPoint_PreservesNegativeZeroLikeLinqToObjects()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(DatabaseFormat.AceAccdb, fillerRows: 0, ct);
        await using AccessReader reader = await OpenReaderAsync(stream, ct);
        IQueryable<AggItem> query = reader.Query<AggItem>(Table).OrderBy(i => i.Id).Take(3);
        List<AggItem> rows = await query.ToListAsync(ct);

        Assert.Equal(
            BitConverter.DoubleToInt64Bits(rows.Average(_ => -0.0)),
            BitConverter.DoubleToInt64Bits(await query.AverageAsync(_ => -0.0, ct)));
        Assert.Equal(
            BitConverter.SingleToInt32Bits(rows.Average(_ => -0.0f)),
            BitConverter.SingleToInt32Bits(await query.AverageAsync(_ => -0.0f, ct)));

        // A leading null must not seed the sum with positive zero either.
        double? nullableDouble = await query.AverageAsync(i => i.Id == 1 ? null : (double?)-0.0, ct);
        float? nullableFloat = await query.AverageAsync(i => i.Id == 1 ? null : (float?)-0.0f, ct);
        Assert.Equal(
            BitConverter.DoubleToInt64Bits(rows.Average(i => i.Id == 1 ? null : (double?)-0.0)!.Value),
            BitConverter.DoubleToInt64Bits(nullableDouble!.Value));
        Assert.Equal(
            BitConverter.SingleToInt32Bits(rows.Average(i => i.Id == 1 ? null : (float?)-0.0f)!.Value),
            BitConverter.SingleToInt32Bits(nullableFloat!.Value));
    }

    [Theory]
    [InlineData("Sum")]
    [InlineData("Average")]
    [InlineData("Min")]
    [InlineData("Max")]
    public async Task Aggregate_RunsItsSelectorAsTheRowsStream(string aggregate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(DatabaseFormat.AceAccdb, fillerRows: 2000, ct);

        long everyRow = await BytesReadAsync(stream, q => AggregateAsync(q, aggregate, i => i.Id, ct), ct);
        long untilFault = await BytesReadAsync(
            stream,
            async q =>
            {
                InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => AggregateAsync(q, aggregate, i => Fault(i), ct));
                Assert.StartsWith(SelectorFault, error.Message, StringComparison.Ordinal);
            },
            ct);

        // The selector runs on each row as the read returns it, so its fault on the first row
        // stops the read; aggregating a materialized list would have read every row first.
        Assert.True(untilFault < everyRow, $"A selector fault on the first row read {untilFault} bytes; the whole aggregate read {everyRow}.");
    }

    [Fact]
    public async Task AllAsync_AndContainsAsync_StopAtTheDecidingRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateItemsAsync(DatabaseFormat.AceAccdb, fillerRows: 2000, ct);
        int firstId;
        await using (AccessReader reader = await OpenReaderAsync(stream, ct))
        {
            firstId = await reader.Query<AggItem>(Table).Select(i => i.Id).FirstAsync(ct);
        }

        long allHold = await BytesReadAsync(stream, async q => Assert.True(await q.AllAsync(i => i.Id > 0, ct)), ct);
        long firstFails = await BytesReadAsync(stream, async q => Assert.False(await q.AllAsync(i => i.Id < 0, ct)), ct);
        long noneMatch = await BytesReadAsync(stream, async q => Assert.False(await q.Select(i => i.Id).ContainsAsync(-1, ct)), ct);
        long firstMatches = await BytesReadAsync(stream, async q => Assert.True(await q.Select(i => i.Id).ContainsAsync(firstId, ct)), ct);

        // Like LINQ to Objects' All and Contains, each stops at the first row that decides it.
        Assert.True(firstFails < allHold, $"AllAsync false on the first row read {firstFails} bytes; true over every row read {allHold}.");
        Assert.True(firstMatches < noneMatch, $"ContainsAsync matching the first row read {firstMatches} bytes; matching none read {noneMatch}.");
    }

    [Fact]
    public void Aggregates_TakeExpressionSelectorsOnly_MirroringQueryable()
    {
        // Every Queryable aggregate that takes a selector has an async counterpart with the same
        // selector and result types, and no other overload exists: a Func selector would make
        // each lambda call ambiguous (CS0121) and would run outside the query.
        string[] expected = [.. SelectorOverloads(typeof(Queryable), suffix: string.Empty, unwrapResult: false).Order(StringComparer.Ordinal)];
        string[] actual = [.. SelectorOverloads(typeof(AccessQueryExtensions), suffix: "Async", unwrapResult: true).Order(StringComparer.Ordinal)];

        Assert.Equal(22, expected.Length);
        Assert.Equal(expected, actual);
    }

    private static IEnumerable<string> SelectorOverloads(Type type, string suffix, bool unwrapResult) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name.EndsWith(suffix, StringComparison.Ordinal) && AggregateNames.Contains(m.Name[..^suffix.Length]))
            .Select(m => (Method: m, Parameters: m.GetParameters()))
            .Where(m => unwrapResult || (m.Parameters.Length == 2 && m.Parameters[1].ParameterType.IsGenericType && m.Parameters[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>)))
            .Select(m => $"{m.Method.Name[..^suffix.Length]} {Display(m.Parameters[1].ParameterType)} -> {Display(unwrapResult ? m.Method.ReturnType.GetGenericArguments()[0] : m.Method.ReturnType)}");

    private static string Display(Type type) => type switch
    {
        { IsGenericParameter: true } => string.Create(CultureInfo.InvariantCulture, $"T{type.GenericParameterPosition}"),
        { IsGenericType: true } => $"{type.Name}[{string.Join(',', type.GetGenericArguments().Select(Display))}]",
        _ => type.Name,
    };

    private static Task AggregateAsync(IQueryable<AggItem> query, string aggregate, Expression<Func<AggItem, int>> selector, CancellationToken ct) => aggregate switch
    {
        "Sum" => query.SumAsync(selector, ct).AsTask(),
        "Average" => query.AverageAsync(selector, ct).AsTask(),
        "Min" => query.MinAsync(selector, ct).AsTask(),
        "Max" => query.MaxAsync(selector, ct).AsTask(),
        _ => throw new ArgumentOutOfRangeException(nameof(aggregate), aggregate, "Unknown aggregate."),
    };

    private static int Fault(AggItem item) =>
        throw new InvalidOperationException(string.Create(CultureInfo.InvariantCulture, $"{SelectorFault} {item.Id}."));

    private static async Task<string> OutcomeAsync(Func<ValueTask<object?>> run)
    {
        try
        {
            return Describe(await run());
        }
        catch (InvalidOperationException)
        {
            return $"throws {nameof(InvalidOperationException)}";
        }
        catch (OverflowException)
        {
            return $"throws {nameof(OverflowException)}";
        }
    }

    private static string Describe(object? value) => value switch
    {
        null => "<null>",
        AggItem item => string.Create(CultureInfo.InvariantCulture, $"item {item.Id}"),
        IFormattable formattable => $"{value.GetType().Name} {formattable.ToString(format: null, CultureInfo.InvariantCulture)}",
        _ => string.Create(CultureInfo.InvariantCulture, $"{value.GetType().Name} {value}"),
    };

    private static async Task<long> BytesReadAsync(MemoryStream stream, Func<IQueryable<AggItem>, Task> run, CancellationToken ct)
    {
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await OpenReaderAsync(counting, ct);
        counting.Reset();
        await run(reader.Query<AggItem>(Table));
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
                new("Weight", typeof(double)),
                new("Price", typeof(decimal)) { IsCurrency = true },
                new("Ratio", typeof(float)),
            ],
            ct);

        // Inserted out of key order, with nulls in every nullable column. Weight and Ratio hold
        // binary fractions, so float conversions and their sums are exact.
        List<object?[]> rows =
        [
            [7, "grace", 30, 2.5, 12.5m, 0.5f],
            [3, "carol", 10, DBNull.Value, 3.25m, 1.25f],
            [11, DBNull.Value, 50, -1.75, DBNull.Value, 2f],
            [1, "alice", 30, 0.5, 7m, 0.125f],
            [9, "ivan", DBNull.Value, 4.0, 1.1m, 3.5f],
            [5, "eve", 20, DBNull.Value, 99.99m, 0.375f],
            [12, "Zed", 10, 1024.25, DBNull.Value, 7.25f],
            [2, "bob", 40, -3.5, 0.0001m, 0.75f],
            [8, "heidi", DBNull.Value, DBNull.Value, DBNull.Value, 1.5f],
            [4, "dave", 20, 6.25, 5.5m, 0.25f],
            [10, "judy", 30, 0.375, 2.75m, 4f],
            [6, "Frank", 50, 9.75, 10m, 0.875f],
        ];
        for (int i = 0; i < fillerRows; i++)
        {
            rows.Add([1000 + i, string.Create(CultureInfo.InvariantCulture, $"filler row {i:D5} with some padding text"), i % 50, 1.5, 1m, 1f]);
        }

        await writer.InsertRowsAsync(Table, rows, ct);
        return stream;
    }

    private static ValueTask<AccessReader> OpenReaderAsync(Stream stream, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
    }

    internal sealed class AggItem
    {
        public int Id { get; set; }

        public string? Name { get; set; }

        public int? Score { get; set; }

        public double? Weight { get; set; }

        public decimal? Price { get; set; }

        public float Ratio { get; set; }
    }

    /// <summary>One aggregate: the async terminal over a query, and the LINQ to Objects call it must agree with.</summary>
    /// <param name="Actual">Runs the async terminal over the query.</param>
    /// <param name="Expected">Runs the same aggregate with LINQ to Objects over the rows the query returns.</param>
    private sealed record AggregateCase(Func<IQueryable<AggItem>, CancellationToken, ValueTask<object?>> Actual, Func<IEnumerable<AggItem>, object?> Expected);

    /// <summary>Equates items by key, so <c>ContainsAsync</c> with a comparer finds an entity it did not read.</summary>
    private sealed class AggItemIdComparer : IEqualityComparer<AggItem>
    {
        public static readonly AggItemIdComparer Instance = new();

        public bool Equals(AggItem? x, AggItem? y) => x?.Id == y?.Id;

        public int GetHashCode(AggItem obj) => obj.Id;
    }
}
