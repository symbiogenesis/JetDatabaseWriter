namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Round-trip tests for multi-column / unique / descending
/// <see cref="IndexDefinition"/> emission:
/// <list type="bullet">
///   <item><description>Multi-column non-PK indexes round-trip through <see cref="IAccessReader.ListIndexesAsync"/>.</description></item>
///   <item><description><see cref="IndexDefinition.IsUnique"/> emits the real-idx <c>flags</c> bit <c>0x01</c> (§3.1), surfaced as <see cref="IndexMetadata.HasUniqueFlag"/>, and contributes to <see cref="IndexMetadata.EnforcesUniqueness"/>.</description></item>
///   <item><description><see cref="IndexDefinition.DescendingColumns"/> clears the col_map ascending flag and is surfaced as <see cref="IndexColumnReference.IsAscending"/> = <see langword="false"/>.</description></item>
///   <item><description>The bulk-rebuild path concatenates per-column encoded keys (and respects per-column direction) for multi-column indexes.</description></item>
///   <item><description>Inserting a duplicate row into a unique index throws <see cref="InvalidOperationException"/>.</description></item>
/// </list>
/// </summary>
public sealed class IndexWriterAdvancedTests
{
    private static readonly string[] CompositeAB = ["A", "B"];
    private static readonly string[] DescendingB = ["B"];
    private static readonly string[] DescendingScore = ["Score"];
    private static readonly string[] DescendingMissing = ["B"];
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateTable_WithUniqueSingleColumnIndex_RoundTripsUniquenessMetadata()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);
        const string tableName = "Idx_Unique";

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                tableName,
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, TestContext.Current.CancellationToken);
        IndexMetadata ix = Assert.Single(indexes);
        Assert.Equal(IndexKind.Normal, ix.Kind);
        Assert.True(ix.EnforcesUniqueness);
        Assert.True(ix.HasUniqueFlag);
    }

    [Fact]
    public async Task CreateTable_WithMultiColumnNonPkIndex_RoundTripsAllColumnsInOrder()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);
        const string tableName = "Idx_Multi";

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                ],
                [new IndexDefinition("IX_AB", CompositeAB)],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, TestContext.Current.CancellationToken);
        IndexMetadata ix = Assert.Single(indexes);
        Assert.Equal(IndexKind.Normal, ix.Kind);
        Assert.False(ix.EnforcesUniqueness);
        Assert.False(ix.HasUniqueFlag);
        Assert.Equal(CompositeAB, ix.Columns.Select(c => c.Name).ToArray());
        Assert.All(ix.Columns, c => Assert.True(c.IsAscending));
    }

    [Fact]
    public async Task CreateTable_WithDescendingSingleColumnIndex_RoundTripsDescendingFlag()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);
        const string tableName = "Idx_Desc";

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                tableName,
                [new ColumnDefinition("Score", typeof(int))],
                [new IndexDefinition("IX_ScoreDesc", "Score") { DescendingColumns = DescendingScore }],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, TestContext.Current.CancellationToken);
        IndexColumnReference col = Assert.Single(Assert.Single(indexes).Columns);
        Assert.Equal("Score", col.Name);
        Assert.False(col.IsAscending);
    }

    [Fact]
    public async Task CreateTable_WithMixedAscDescMultiColumn_RoundTripsPerColumnDirection()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);
        const string tableName = "Idx_Mixed";

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                ],
                [
                    new IndexDefinition("IX_AB_Mixed", CompositeAB)
                    {
                        DescendingColumns = DescendingB,
                    },
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        IndexMetadata ix = Assert.Single(await reader.ListIndexesAsync(tableName, TestContext.Current.CancellationToken));
        Assert.Equal("A", ix.Columns[0].Name);
        Assert.True(ix.Columns[0].IsAscending);
        Assert.Equal("B", ix.Columns[1].Name);
        Assert.False(ix.Columns[1].IsAscending);
    }

    [Fact]
    public async Task CreateTable_DescendingColumnNotInColumns_Throws()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);
        await using AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("A", typeof(int))],
                [new IndexDefinition("IX_A", "A") { DescendingColumns = DescendingMissing }],
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UniqueIndex_DuplicateInsert_ThrowsInvalidOperationException()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct);

        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int))],
            [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
            this.ct);

        await writer.InsertRowAsync("T", [1], this.ct);
        await writer.InsertRowAsync("T", [2], this.ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1], this.ct));
    }

    [Fact]
    public async Task UniqueIndex_NonDuplicateInserts_Succeed()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
                this.ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [5],
                    [1],
                    [3],
                ],
                this.ct);
        }

        await AssertLeafEntryCountAsync(stream, "T", "UQ_Id", expectedCount: 3);
    }

    [Fact]
    public async Task MultiColumnIndex_BulkInsert_RebuildsLeafWithExpectedEntryCount()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                ],
                [new IndexDefinition("IX_AB", CompositeAB)],
                this.ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 100],
                    [1, 50],
                    [2, 25],
                    [1, 75],
                ],
                this.ct);
        }

        // Multi-column composite key concatenation through the maintenance
        // path should rebuild a single leaf with 4 entries.
        await AssertLeafEntryCountAsync(stream, "T", "IX_AB", expectedCount: 4);
    }

    [Fact]
    public async Task UniqueMultiColumnIndex_DuplicateCompositeKey_Throws()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct);

        await writer.CreateTableAsync(
            "T",
            [
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int)),
            ],
            [new IndexDefinition("UQ_AB", CompositeAB) { IsUnique = true }],
            this.ct);

        await writer.InsertRowAsync("T", [1, 10], this.ct);
        await writer.InsertRowAsync("T", [1, 20], this.ct); // (1,10) ≠ (1,20) — fine

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1, 10], this.ct));
    }

    [Fact]
    public async Task MultiColumnIndex_SurvivesAddColumn()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                ],
                [new IndexDefinition("IX_AB", CompositeAB) { IsUnique = true }],
                this.ct);

            await writer.InsertRowsAsync("T", [[1, 1], [2, 2]], this.ct);
            await writer.AddColumnAsync("T", new ColumnDefinition("Note", typeof(string), maxLength: 50), this.ct);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        IndexMetadata ix = Assert.Single(await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken));
        Assert.True(ix.EnforcesUniqueness);
        Assert.True(ix.HasUniqueFlag);
        Assert.Equal(CompositeAB, ix.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task DescendingIndex_SurvivesRenameColumn()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Score", typeof(int))],
                [new IndexDefinition("IX_Score", "Score") { DescendingColumns = DescendingScore }],
                this.ct);

            await writer.RenameColumnAsync("T", "Score", "Points", this.ct);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        IndexMetadata ix = Assert.Single(await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken));
        IndexColumnReference col = Assert.Single(ix.Columns);
        Assert.Equal("Points", col.Name);
        Assert.False(col.IsAscending);
    }

    // ── GUID-keyed index live B-tree maintenance ─────────────────────────────

    [Fact]
    public async Task GuidIndex_BulkInsert_RebuildsLeafWithExpectedEntryCount()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(Guid))],
                [new IndexDefinition("IX_Id", "Id")],
                this.ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [Guid.Parse("00000000-0000-0000-0000-000000000001")],
                    [Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")],
                    [Guid.Parse("11111111-2222-3333-4444-555555555555")],
                ],
                this.ct);
        }

        // GUID-keyed index participates in the bulk-rebuild path.
        await AssertLeafEntryCountAsync(stream, "T", "IX_Id", expectedCount: 3);
    }

    [Fact]
    public async Task UniqueGuidIndex_DuplicateInsert_Throws()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct);

        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(Guid))],
            [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
            this.ct);

        var dup = Guid.Parse("11111111-2222-3333-4444-555555555555");
        await writer.InsertRowAsync("T", [dup], this.ct);
        await writer.InsertRowAsync("T", [Guid.NewGuid()], this.ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [dup], this.ct));
    }

    [Fact]
    public async Task DecimalIndex_BulkInsert_RebuildsLeafWithExpectedEntryCount()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Amount", typeof(decimal))],
                [new IndexDefinition("IX_Amount", "Amount")],
                this.ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [-1000.50m],
                    [0m],
                    [1m],
                    [1.50m],
                    [1000m],
                ],
                this.ct);
        }

        // Decimal-keyed index participates in the bulk-rebuild path.
        await AssertLeafEntryCountAsync(stream, "T", "IX_Amount", expectedCount: 5);
    }

    [Fact]
    public async Task UniqueDecimalIndex_DuplicateInsert_Throws()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct);

        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Amount", typeof(decimal)) { NumericPrecision = 18, NumericScale = 2 }],
            [new IndexDefinition("UQ_Amount", "Amount") { IsUnique = true }],
            this.ct);

        // 1.50 and 1.5 normalise to the same numeric value at the column's
        // declared scale (2); they must collide under the canonical-scale
        // index encoding. (The intermediate 2m insert is at a different
        // canonical value and must succeed.)
        await writer.InsertRowAsync("T", [1.50m], this.ct);
        await writer.InsertRowAsync("T", [2m], this.ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1.5m], this.ct));
    }

    // ── helpers (page scanning) ─────────────────────────────────

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        // Subtract 1 for the sentinel bit at the position one past the last entry.
        int count = 1;
        for (int i = Constants.IndexLeafPage.Jet4.BitmaskOffset; i < Constants.IndexLeafPage.Jet4.FirstEntryOffset; i++)
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

        return count < 1 ? 0 : count - 1;
    }

    private static async Task AssertLeafEntryCountAsync(MemoryStream stream, string tableName, string indexName, int expectedCount)
    {
        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, TestContext.Current.CancellationToken);
        IndexMetadata index = Assert.Single(
            await reader.ListIndexesAsync(tableName, TestContext.Current.CancellationToken),
            candidate => candidate.Name == indexName);

        byte[] fileBytes = stream.ToArray();
        int leafOffset = checked(index.FirstDp * Constants.PageSizes.Jet4);
        Assert.Equal(0x04, fileBytes[leafOffset]);
        Assert.Equal(0x01, fileBytes[leafOffset + 1]);
        Assert.Equal(expectedCount, CountLeafEntries(fileBytes, leafOffset));
    }

}
