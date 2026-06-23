namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// End-to-end tests for the index-inferring read overload
/// <see cref="AccessReader.Rows{T}(string, Expression{Func{T, bool}}, IProgress{long}?, System.Threading.CancellationToken)"/>.
/// Every inferred read must return exactly the same rows a brute-force scan would,
/// whether an index is used (Jet4 / ACE) or the read falls back to a scan (Jet3, or
/// predicates with no covering index).
/// </summary>
public sealed class InferredIndexReadTests
{
    private static readonly (int Id, string Name, int Score)[] SeedRows =
    [
        (1, "Alice", 50),
        (2, "Bob", 30),
        (3, "Carol", 70),
        (4, "Bob", 65),
        (5, "Dave", 30),
        (6, "Eve", 90),
        (7, "Frank", 55),
        (8, "Bob", 45),
        (9, "Grace", 30),
        (10, "Heidi", 80),
    ];

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task InferredReads_MatchScan_OnSeekCapableFormats(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildPeopleDatabaseAsync(format);
        await using AccessReader reader = await OpenReaderAsync(stream);

        IReadOnlyList<Person> all = await ScanAllAsync(reader);

        await AssertInferredMatchesScanAsync(reader, all, p => p.Score >= 30 && p.Score < 70);
        await AssertInferredMatchesScanAsync(reader, all, p => p.Name == "Bob");
        await AssertInferredMatchesScanAsync(reader, all, p => p.Id == 3);
        await AssertInferredMatchesScanAsync(reader, all, p => p.Score > 1000);
        await AssertInferredMatchesScanAsync(reader, all, p => p.Score >= 30 && p.Name == "Bob");
        await AssertInferredMatchesScanAsync(reader, all, p => p.Name.StartsWith("B", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task ScoreRangePredicate_PlansTheScoreIndex(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildPeopleDatabaseAsync(format);
        await using AccessReader reader = await OpenReaderAsync(stream);

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("People", TestContext.Current.CancellationToken);
        Expression<Func<Person, bool>> predicate = p => p.Score >= 30 && p.Score < 70;
        RowCriteria pushable = IndexPredicateTranslator.ExtractPushableCriteria(predicate);

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, pushable);

        Assert.NotNull(plan);
        Assert.Equal("IX_Score", plan.Index.Name);
        Assert.Equal(IndexQueryKind.Range, plan.Criteria.Kind);
    }

    [Fact]
    public async Task InferredReads_MatchScan_OnJet3ViaScanFallback()
    {
        await using MemoryStream stream = await BuildPeopleDatabaseAsync(DatabaseFormat.Jet3Mdb);
        await using AccessReader reader = await OpenReaderAsync(stream);

        IReadOnlyList<Person> all = await ScanAllAsync(reader);

        // Jet3 has no index-seek support, so these are scan-and-filter reads — but the
        // result set must be identical to the seek-capable formats.
        await AssertInferredMatchesScanAsync(reader, all, p => p.Score >= 30 && p.Score < 70);
        await AssertInferredMatchesScanAsync(reader, all, p => p.Name == "Bob");
    }

    private static async Task AssertInferredMatchesScanAsync(
        AccessReader reader,
        IReadOnlyList<Person> all,
        Expression<Func<Person, bool>> predicate)
    {
        Func<Person, bool> compiled = predicate.Compile();
        int[] expected = all.Where(compiled).Select(p => p.Id).OrderBy(id => id).ToArray();

        var matched = new List<int>();
        await foreach (Person person in reader.Rows<Person>("People", predicate, cancellationToken: TestContext.Current.CancellationToken))
        {
            matched.Add(person.Id);
        }

        matched.Sort();
        Assert.Equal(expected, matched);
    }

    private static async Task<IReadOnlyList<Person>> ScanAllAsync(AccessReader reader)
    {
        var all = new List<Person>();
        await foreach (Person person in reader.Rows<Person>("People", progress: null, TestContext.Current.CancellationToken))
        {
            all.Add(person);
        }

        return all;
    }

    private static async ValueTask<MemoryStream> BuildPeopleDatabaseAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "People",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                    new ColumnDefinition("Score", typeof(int)),
                ],
                [
                    new IndexDefinition("IX_Score", "Score"),
                    new IndexDefinition("IX_Name", "Name"),
                ],
                TestContext.Current.CancellationToken);

            foreach ((int id, string name, int score) in SeedRows)
            {
                await writer.InsertRowAsync("People", [id, name, score], TestContext.Current.CancellationToken);
            }
        }

        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    public sealed class Person
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Score { get; set; }
    }
}
