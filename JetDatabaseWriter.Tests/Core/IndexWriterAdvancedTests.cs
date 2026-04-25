namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// W11 (2026-04-25) round-trip tests for the lifted single-column /
/// non-unique / ascending-only restrictions on <see cref="IndexDefinition"/>:
/// <list type="bullet">
///   <item><description>Multi-column non-PK indexes round-trip through <see cref="IAccessReader.ListIndexesAsync"/>.</description></item>
///   <item><description><see cref="IndexDefinition.IsUnique"/> emits the real-idx <c>flags</c> bit <c>0x01</c> (§3.1) and is surfaced as <see cref="IndexMetadata.IsUnique"/>.</description></item>
///   <item><description><see cref="IndexDefinition.DescendingColumns"/> emits <c>col_order = 0x02</c> in the matching col_map slots and is surfaced as <see cref="IndexColumnReference.IsAscending"/> = <see langword="false"/>.</description></item>
///   <item><description>The W5 bulk-rebuild path now concatenates per-column encoded keys (and respects per-column direction) for multi-column indexes.</description></item>
///   <item><description>Inserting a duplicate row into a unique index throws <see cref="InvalidOperationException"/>.</description></item>
/// </list>
/// </summary>
public sealed class IndexWriterAdvancedTests
{
    private const int PageSize = 4096;
    private const int LeafBitmaskOffset = 0x1B;
    private const int LeafFirstEntryOffset = 0x1E0;

    private static readonly string[] CompositeAB = { "A", "B" };
    private static readonly string[] DescendingB = { "B" };
    private static readonly string[] DescendingScore = { "Score" };
    private static readonly string[] DescendingMissing = { "B" };

    [Fact]
    public async Task CreateTable_WithUniqueSingleColumnIndex_RoundTripsIsUnique()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Unique";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                new[] { new ColumnDefinition("Id", typeof(int)) },
                new[] { new IndexDefinition("UQ_Id", "Id") { IsUnique = true } },
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
        IndexMetadata ix = Assert.Single(indexes);
        Assert.Equal(IndexKind.Normal, ix.Kind);
        Assert.True(ix.IsUnique);
    }

    [Fact]
    public async Task CreateTable_WithMultiColumnNonPkIndex_RoundTripsAllColumnsInOrder()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Multi";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                new[]
                {
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                },
                new[] { new IndexDefinition("IX_AB", CompositeAB) },
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
        IndexMetadata ix = Assert.Single(indexes);
        Assert.Equal(IndexKind.Normal, ix.Kind);
        Assert.False(ix.IsUnique);
        Assert.Equal(CompositeAB, ix.Columns.Select(c => c.Name).ToArray());
        Assert.All(ix.Columns, c => Assert.True(c.IsAscending));
    }

    [Fact]
    public async Task CreateTable_WithDescendingSingleColumnIndex_EmitsColOrder0x02()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Desc";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                new[] { new ColumnDefinition("Score", typeof(int)) },
                new[] { new IndexDefinition("IX_ScoreDesc", "Score") { DescendingColumns = DescendingScore } },
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
        IndexColumnReference col = Assert.Single(Assert.Single(indexes).Columns);
        Assert.Equal("Score", col.Name);
        Assert.False(col.IsAscending);
    }

    [Fact]
    public async Task CreateTable_WithMixedAscDescMultiColumn_RoundTripsPerColumnDirection()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Mixed";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                new[]
                {
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                },
                new[]
                {
                    new IndexDefinition("IX_AB_Mixed", CompositeAB)
                    {
                        DescendingColumns = DescendingB,
                    },
                },
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IndexMetadata ix = Assert.Single(await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken));
        Assert.Equal("A", ix.Columns[0].Name);
        Assert.True(ix.Columns[0].IsAscending);
        Assert.Equal("B", ix.Columns[1].Name);
        Assert.False(ix.Columns[1].IsAscending);
    }

    [Fact]
    public async Task CreateTable_DescendingColumnNotInColumns_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("A", typeof(int)) },
                new[] { new IndexDefinition("IX_A", "A") { DescendingColumns = DescendingMissing } },
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UniqueIndex_DuplicateInsert_ThrowsInvalidOperationException()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);

        await writer.CreateTableAsync(
            "T",
            new[] { new ColumnDefinition("Id", typeof(int)) },
            new[] { new IndexDefinition("UQ_Id", "Id") { IsUnique = true } },
            ct);

        await writer.InsertRowAsync("T", new object[] { 1 }, ct);
        await writer.InsertRowAsync("T", new object[] { 2 }, ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", new object[] { 1 }, ct));
    }

    [Fact]
    public async Task UniqueIndex_NonDuplicateInserts_Succeed()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Id", typeof(int)) },
                new[] { new IndexDefinition("UQ_Id", "Id") { IsUnique = true } },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { 5 },
                    new object[] { 1 },
                    new object[] { 3 },
                },
                ct);
        }

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task MultiColumnIndex_BulkInsert_RebuildsLeafWithExpectedEntryCount()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[]
                {
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                },
                new[] { new IndexDefinition("IX_AB", CompositeAB) },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { 1, 100 },
                    new object[] { 1, 50 },
                    new object[] { 2, 25 },
                    new object[] { 1, 75 },
                },
                ct);
        }

        // Multi-column composite key concatenation through the W5 maintenance
        // path should rebuild a single leaf with 4 entries.
        Assert.Equal(4, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task UniqueMultiColumnIndex_DuplicateCompositeKey_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);

        await writer.CreateTableAsync(
            "T",
            new[]
            {
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int)),
            },
            new[] { new IndexDefinition("UQ_AB", CompositeAB) { IsUnique = true } },
            ct);

        await writer.InsertRowAsync("T", new object[] { 1, 10 }, ct);
        await writer.InsertRowAsync("T", new object[] { 1, 20 }, ct); // (1,10) ≠ (1,20) — fine

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", new object[] { 1, 10 }, ct));
    }

    [Fact]
    public async Task MultiColumnIndex_SurvivesAddColumn()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[]
                {
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                },
                new[] { new IndexDefinition("IX_AB", CompositeAB) { IsUnique = true } },
                ct);

            await writer.InsertRowsAsync("T", new[] { new object[] { 1, 1 }, new object[] { 2, 2 } }, ct);
            await writer.AddColumnAsync("T", new ColumnDefinition("Note", typeof(string), maxLength: 50), ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        IndexMetadata ix = Assert.Single(await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken));
        Assert.True(ix.IsUnique);
        Assert.Equal(CompositeAB, ix.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task DescendingIndex_SurvivesRenameColumn()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Score", typeof(int)) },
                new[] { new IndexDefinition("IX_Score", "Score") { DescendingColumns = DescendingScore } },
                ct);

            await writer.RenameColumnAsync("T", "Score", "Points", ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        IndexMetadata ix = Assert.Single(await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken));
        IndexColumnReference col = Assert.Single(ix.Columns);
        Assert.Equal("Points", col.Name);
        Assert.False(col.IsAscending);
    }

    // --- W12: GUID-keyed index live B-tree maintenance ------------------------

    [Fact]
    public async Task GuidIndex_BulkInsert_RebuildsLeafWithExpectedEntryCount()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Id", typeof(Guid)) },
                new[] { new IndexDefinition("IX_Id", "Id") },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { Guid.Parse("00000000-0000-0000-0000-000000000001") },
                    new object[] { Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF") },
                    new object[] { Guid.Parse("11111111-2222-3333-4444-555555555555") },
                },
                ct);
        }

        // W12: GUID-keyed index now participates in the W5 bulk-rebuild path.
        // Before W12 this would have stayed at the empty W3 placeholder leaf
        // (entry count = 1, the implicit first entry).
        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task UniqueGuidIndex_DuplicateInsert_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);

        await writer.CreateTableAsync(
            "T",
            new[] { new ColumnDefinition("Id", typeof(Guid)) },
            new[] { new IndexDefinition("UQ_Id", "Id") { IsUnique = true } },
            ct);

        var dup = Guid.Parse("11111111-2222-3333-4444-555555555555");
        await writer.InsertRowAsync("T", new object[] { dup }, ct);
        await writer.InsertRowAsync("T", new object[] { Guid.NewGuid() }, ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", new object[] { dup }, ct));
    }

    [Fact]
    public async Task DecimalIndex_BulkInsert_RebuildsLeafWithExpectedEntryCount()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Amount", typeof(decimal)) },
                new[] { new IndexDefinition("IX_Amount", "Amount") },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { -1000.50m },
                    new object[] { 0m },
                    new object[] { 1m },
                    new object[] { 1.50m },
                    new object[] { 1000m },
                },
                ct);
        }

        // W13: Decimal-keyed index now participates in the W5 bulk-rebuild path.
        // Before W13 this would have stayed at the empty W3 placeholder leaf
        // (entry count = 1, the implicit first entry).
        Assert.Equal(5, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task UniqueDecimalIndex_DuplicateInsert_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);

        await writer.CreateTableAsync(
            "T",
            new[] { new ColumnDefinition("Amount", typeof(decimal)) },
            new[] { new IndexDefinition("UQ_Amount", "Amount") { IsUnique = true } },
            ct);

        // 1.50 and 1.5 normalise to the same numeric value; they must collide
        // under the W13 target-scale normalisation.
        await writer.InsertRowAsync("T", new object[] { 1.50m }, ct);
        await writer.InsertRowAsync("T", new object[] { 2m }, ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", new object[] { 1.5m }, ct));
    }

    // --- helpers (page scanning) ---------------------------------------------

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        int count = 1;
        for (int i = LeafBitmaskOffset; i < LeafFirstEntryOffset; i++)
        {
            byte b = fileBytes[leafOffset + i];
            for (int bit = 0; bit < 8; bit++)
            {
                if ((b & (1 << bit)) != 0)
                {
                    count++;
                }
            }
        }

        return count;
    }

    private static int FindMaxLeafEntryCount(byte[] fileBytes)
    {
        int max = 0;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                int n = CountLeafEntries(fileBytes, o);
                if (n > max)
                {
                    max = n;
                }
            }
        }

        return max;
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
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
}
