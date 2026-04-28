namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for <see cref="IAccessWriter.CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, System.Threading.CancellationToken)"/>:
/// emit single-column non-unique ascending logical indexes into the new table's
/// TDEF page chain, and confirm they are surfaced by
/// <see cref="IAccessReader.ListIndexesAsync"/>. The build also appends
/// one empty B-tree leaf page (<c>page_type = 0x04</c>) per index and patches
/// the leaf's page number into the matching real-index <c>first_dp</c> field;
/// the last two tests in this class scan the on-disk byte stream to confirm
/// that wiring.
/// <para>
/// These tests do <em>not</em> assert any seek / lookup behaviour — the leaf is
/// empty at table-creation time. Index maintenance on subsequent inserts is
/// covered by <c>IndexMaintenanceTests</c> and <c>IndexWriterAdvancedTests</c>.
/// See <c>docs/design/index-and-relationship-format-notes.md</c>.
/// </para>
/// </summary>
public sealed class IndexWriterTests
{
    private static readonly string[] ExpectedIndexNames = ["IX_Name", "IX_Score", "IX_Id"];

    [Fact]
    public async Task CreateTable_WithSingleIndex_RoundTripsThroughListIndexes()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Single";
        const string IndexName = "IX_Idx_Single_Name";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                [new IndexDefinition(IndexName, "Name")],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        IndexMetadata idx = Assert.Single(indexes);
        Assert.Equal(IndexName, idx.Name);
        Assert.Equal(IndexKind.Normal, idx.Kind);
        Assert.False(idx.IsUnique);
        Assert.False(idx.IsForeignKey);
        Assert.False(idx.CascadeUpdates);
        Assert.False(idx.CascadeDeletes);
        Assert.Equal(0, idx.RealIndexNumber);

        IndexColumnReference col = Assert.Single(idx.Columns);
        Assert.Equal("Name", col.Name);
        Assert.True(col.IsAscending);
    }

    [Fact]
    public async Task CreateTable_WithMultipleIndexes_RoundTripsAll()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Multi";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                    new ColumnDefinition("Score", typeof(int)),
                ],
                [
                    new IndexDefinition("IX_Name", "Name"),
                    new IndexDefinition("IX_Score", "Score"),
                    new IndexDefinition("IX_Id", "Id"),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        Assert.Equal(3, indexes.Count);
        Assert.Equal(ExpectedIndexNames, indexes.Select(i => i.Name).ToArray());
        Assert.All(indexes, i => Assert.Equal(IndexKind.Normal, i.Kind));
        Assert.All(indexes, i => Assert.Single(i.Columns));

        // One real-idx is emitted per logical-idx (no sharing).
        var realIdxNumbers = indexes.Select(i => i.RealIndexNumber).ToHashSet();
        Assert.Equal(indexes.Count, realIdxNumbers.Count);

        Assert.Equal("Name", indexes[0].Columns[0].Name);
        Assert.Equal("Score", indexes[1].Columns[0].Name);
        Assert.Equal("Id", indexes[2].Columns[0].Name);
    }

    [Fact]
    public async Task CreateTable_NoIndexes_StillSucceedsAndExposesNoIndexes()
    {
        // The new overload with an empty index list must produce byte-identical output
        // to the original column-only overload, and the reader must report no indexes.
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Empty";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int))],
                [],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Empty(indexes);
    }

    [Fact]
    public async Task CreateTable_IndexReferencesUnknownColumn_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "Idx_Bad",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Missing", "NoSuchColumn")],
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateTable_DuplicateIndexNames_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "Idx_Dupe",
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                ],
                [
                    new IndexDefinition("IX_Dup", "A"),
                    new IndexDefinition("IX_Dup", "B"),
                ],
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateTable_WithIndex_DataInsertsAndReadsBack()
    {
        // The heap data path must not be perturbed by the TDEF index sections.
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Data";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Label", typeof(string), maxLength: 32),
                ],
                [new IndexDefinition("IX_Label", "Label")],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                TableName,
                [
                    [1, "alpha"],
                    [2, "beta"],
                    [3, "gamma"],
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows(TableName, cancellationToken: TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            rows.Add(row);
        }

        Assert.Equal(3, rows.Count);
        Assert.Equal([1, "alpha"], rows[0]);
        Assert.Equal([2, "beta"], rows[1]);
        Assert.Equal([3, "gamma"], rows[2]);
    }

    [Fact]
    public async Task CreateTable_WithIndex_ColumnMetadataUnchanged()
    {
        // Index emission must not alter the column descriptors / names that the
        // reader extracts from the same TDEF buffer.
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Idx_Cols";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 40),
                    new ColumnDefinition("Score", typeof(double)),
                ],
                [new IndexDefinition("IX_Name", "Name")],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        var meta = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);

        Assert.Equal(3, meta.Count);
        Assert.Equal("Id", meta[0].Name);
        Assert.Equal(typeof(int), meta[0].ClrType);
        Assert.Equal("Name", meta[1].Name);
        Assert.Equal(typeof(string), meta[1].ClrType);
        Assert.Equal("Score", meta[2].Name);
        Assert.Equal(typeof(double), meta[2].ClrType);
    }

    [Fact]
    public async Task CreateTable_WithIndex_EmitsLeafPageWithMatchingParent()
    {
        // A single empty leaf page (page_type=0x04) is appended per index,
        // and its page number is patched into the real-idx physical descriptor's
        // first_dp field. We don't have a public API to read first_dp directly,
        // but we can verify by scanning the file for leaf pages and checking
        // their parent_page is non-zero — for a single-table single-index
        // database, exactly one such page must exist.
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Idx_Leaf_Single",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);
        }

        const int PageSize = 4096; // ACE
        byte[] bytes = stream.ToArray();
        int totalPages = bytes.Length / PageSize;

        int leafCount = 0;
        int observedParent = -1;
        int observedFreeSpace = -1;
        for (int p = 0; p < totalPages; p++)
        {
            int o = p * PageSize;
            if (bytes[o] == 0x04 && bytes[o + 1] == 0x01)
            {
                leafCount++;
                observedFreeSpace = bytes[o + 2] | (bytes[o + 3] << 8);
                observedParent = bytes[o + 4] | (bytes[o + 5] << 8) | (bytes[o + 6] << 16) | (bytes[o + 7] << 24);
            }
        }

        Assert.Equal(1, leafCount);
        Assert.True(observedParent > 0, "Index leaf parent_page must reference a TDEF page.");
        Assert.Equal(PageSize - 0x1E0, observedFreeSpace);
    }

    [Fact]
    public async Task CreateTable_WithMultipleIndexes_EmitsOneLeafPagePerIndex()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Idx_Leaf_Multi",
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                    new ColumnDefinition("C", typeof(int)),
                ],
                [
                    new IndexDefinition("IX_A", "A"),
                    new IndexDefinition("IX_B", "B"),
                    new IndexDefinition("IX_C", "C"),
                ],
                TestContext.Current.CancellationToken);
        }

        const int PageSize = 4096;
        byte[] bytes = stream.ToArray();
        int leafCount = 0;
        for (int p = 0; p < bytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            if (bytes[o] == 0x04 && bytes[o + 1] == 0x01)
            {
                leafCount++;
            }
        }

        Assert.Equal(3, leafCount);
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
            // Empty database; tests reopen and add tables.
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
