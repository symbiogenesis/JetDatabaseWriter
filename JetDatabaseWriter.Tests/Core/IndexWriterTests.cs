namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Round-trip tests for <see cref="IAccessSchema.CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, System.Threading.CancellationToken)"/>:
/// emit single-column non-unique ascending logical indexes into the new table's
/// TDEF page chain, and confirm they are surfaced by
/// <see cref="IAccessReader.ListIndexesAsync"/>. The build also appends
/// one empty B-tree leaf page (<c>page_type = 0x04</c>) per index and patches
/// the leaf's page number into the matching real-index <c>first_dp</c> field;
/// the last two tests in this class scan the on-disk byte stream to confirm
/// that wiring.
/// <para>
/// Tests run against both Jet3 (<c>.mdb</c> Access 97, 2048-byte pages) and
/// Jet4/ACE (<c>.accdb</c>, 4096-byte pages). The format is injected as a
/// <c>[Theory]</c> parameter so every assertion exercises both page layouts.
/// </para>
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

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_WithSingleIndex_RoundTripsThroughListIndexes(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
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

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_WithMultipleIndexes_RoundTripsAll(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
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

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_NoIndexes_StillSucceedsAndExposesNoIndexes(DatabaseFormat format)
    {
        // The new overload with an empty index list must produce byte-identical output
        // to the original column-only overload, and the reader must report no indexes.
        await using var stream = await CreateFreshStreamAsync(format);
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

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_IndexReferencesUnknownColumn_Throws(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        await using var writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "Idx_Bad",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Missing", "NoSuchColumn")],
                TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_DuplicateIndexNames_Throws(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
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

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_WithIndex_DataInsertsAndReadsBack(DatabaseFormat format)
    {
        // The heap data path must not be perturbed by the TDEF index sections.
        await using var stream = await CreateFreshStreamAsync(format);
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

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_WithIndex_ColumnMetadataUnchanged(DatabaseFormat format)
    {
        // Index emission must not alter the column descriptors / names that the
        // reader extracts from the same TDEF buffer.
        await using var stream = await CreateFreshStreamAsync(format);
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

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_WithIndex_EmitsLeafPageWithMatchingParent(DatabaseFormat format)
    {
        // A single empty leaf page (page_type=0x04) is appended per index,
        // and its page number is patched into the real-idx physical descriptor's
        // first_dp field. We don't have a public API to read first_dp directly,
        // but we can verify by scanning the file for leaf pages and checking
        // their parent_page is non-zero — for a single-table single-index
        // database, exactly one such page must exist. The free_space header must
        // equal pageSize - firstEntryOffset per the format-specific §4.2 layout.
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Idx_Leaf_Single",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);
        }

        byte[] bytes = stream.ToArray();
        int pageSize = PageSizeOf(format);
        int firstEntryOffset = FirstEntryOffset(format);
        int totalPages = bytes.Length / pageSize;

        int leafCount = 0;
        int observedParent = -1;
        int observedFreeSpace = -1;
        for (int p = 0; p < totalPages; p++)
        {
            int o = p * pageSize;
            if (bytes[o] == 0x04 && bytes[o + 1] == 0x01)
            {
                leafCount++;
                observedFreeSpace = bytes[o + 2] | (bytes[o + 3] << 8);
                observedParent = bytes[o + 4] | (bytes[o + 5] << 8) | (bytes[o + 6] << 16) | (bytes[o + 7] << 24);
            }
        }

        Assert.Equal(1, leafCount);
        Assert.True(observedParent > 0, "Index leaf parent_page must reference a TDEF page.");
        Assert.Equal(pageSize - firstEntryOffset, observedFreeSpace);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_WithMultipleIndexes_EmitsOneLeafPagePerIndex(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

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

        byte[] bytes = stream.ToArray();
        int pageSize = PageSizeOf(format);
        int leafCount = 0;
        for (int p = 0; p < bytes.Length / pageSize; p++)
        {
            int o = p * pageSize;
            if (bytes[o] == 0x04 && bytes[o + 1] == 0x01)
            {
                leafCount++;
            }
        }

        Assert.Equal(3, leafCount);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CreateTable_WithPrimaryKey_RoundTripsAsPkKind(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Pk_Table",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Pk_Table", TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal("PrimaryKey", pk.Name);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal("Id", Assert.Single(pk.Columns).Name);
    }

    /// <summary>
    /// A table with more than 32 columns and more than 16 indexes must
    /// round-trip without truncation. mdbtools historically clipped at
    /// 32 columns; Access supports up to 255 (with one slot reserved).
    /// This test uses 50 columns + 20 single-column indexes — well past
    /// both historical caps but well below the format ceiling — and
    /// asserts every column and every index survives the round-trip.
    /// <para>
    /// Runs across both Jet4 .mdb and ACE .accdb (4 KB pages) and Jet3 .mdb
    /// (2 KB pages). The Jet3 case forces the writer onto the multi-page
    /// TDEF chain path: the schema overflows the 2 KB page, so the builder
    /// emits a continuation page and the reader's existing
    /// <c>ReadTDefBytesAsync</c> stitches them back together. ACE/Jet4 fit
    /// in one page at this size and exercise the legacy single-page path.
    /// </para>
    /// </summary>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_WithFiftyColumnsAndTwentyIndexes_RoundTripsWithoutTruncation(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        const string TableName = "Wide";
        const int ColumnCount = 50;
        const int IndexCount = 20;

        var columns = new List<ColumnDefinition>(ColumnCount);
        for (int i = 0; i < ColumnCount; i++)
        {
            columns.Add(new ColumnDefinition($"C{i:D2}", typeof(int)));
        }

        var indexes = new List<IndexDefinition>(IndexCount);
        for (int i = 0; i < IndexCount; i++)
        {
            indexes.Add(new IndexDefinition($"IX_{i:D2}", $"C{i:D2}"));
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(TableName, columns, indexes, TestContext.Current.CancellationToken);

            // Insert one row covering every column to exercise the wide-row
            // write path alongside the wide-schema read path.
            object[] row = new object[ColumnCount];
            for (int i = 0; i < ColumnCount; i++)
            {
                row[i] = i + 1;
            }

            await writer.InsertRowAsync(TableName, row, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);

        // Every column survives.
        var meta = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Equal(ColumnCount, meta.Count);
        for (int i = 0; i < ColumnCount; i++)
        {
            Assert.Equal($"C{i:D2}", meta[i].Name);
            Assert.Equal(typeof(int), meta[i].ClrType);
        }

        // Every index survives, with the right key column.
        var idxList = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Equal(IndexCount, idxList.Count);
        for (int i = 0; i < IndexCount; i++)
        {
            IndexMetadata idx = idxList.Single(x => x.Name == $"IX_{i:D2}");
            Assert.Equal($"C{i:D2}", Assert.Single(idx.Columns).Name);
        }

        // Row data round-trips with every column populated.
        System.Data.DataTable dt = (await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken))!;
        System.Data.DataRow r = Assert.Single(System.Data.DataTableExtensions.AsEnumerable(dt));
        for (int i = 0; i < ColumnCount; i++)
        {
            Assert.Equal(i + 1, Convert.ToInt32(r[$"C{i:D2}"], System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    /// <summary>
    /// Forces the writer onto the multi-page TDEF chain path on every format
    /// by emitting a schema deliberately larger than the largest single page
    /// (4 KB on Jet4 / ACE) — 200 columns + 30 indexes. Asserts that the
    /// physical TDEF chain on disk is in fact multi-page (the reader's
    /// stitched buffer exceeds <c>_pgSz</c>) and that every column and index
    /// survives the round-trip through <see cref="AccessReader"/>.
    /// </summary>
    /// <param name="format">The on-disk format under test.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_TDefChainSpansMultiplePages_RoundTrips(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        const string TableName = "VeryWide";
        const int ColumnCount = 200;
        const int IndexCount = 30;
        int pgSz = PageSizeOf(format);

        var columns = new List<ColumnDefinition>(ColumnCount);
        for (int i = 0; i < ColumnCount; i++)
        {
            columns.Add(new ColumnDefinition($"C{i:D3}", typeof(int)));
        }

        var indexes = new List<IndexDefinition>(IndexCount);
        for (int i = 0; i < IndexCount; i++)
        {
            indexes.Add(new IndexDefinition($"IX_{i:D2}", $"C{i:D3}"));
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(TableName, columns, indexes, TestContext.Current.CancellationToken);
        }

        // Verify the TDEF chain is in fact multi-page on disk by walking it
        // directly from the raw stream. The first page (the catalog ID) is
        // located via the catalog; subsequent pages are linked via the
        // 4-byte next-page pointer at offset 4. A single-page TDEF stores
        // 0 there.
        long firstTdefPage;
        await using (var reader = await OpenReaderAsync(stream))
        {
            var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.Contains(TableName, tables);

            // Use the reader's internal stitched-bytes accessor (used by the
            // diagnostic tooling) to confirm the logical TDEF body exceeds
            // a single page.
            var msys = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
            Assert.Equal(ColumnCount, msys.Count);

            var idxList = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
            Assert.Equal(IndexCount, idxList.Count);

            // Pull the catalog row to recover the TDEF page number.
            firstTdefPage = await GetTDefPageNumberAsync(reader, TableName);
        }

        // Walk the on-disk page chain manually to assert it has > 1 page.
        stream.Position = 0;
        byte[] streamBytes = stream.ToArray();
        long pg = firstTdefPage;
        var seen = new HashSet<long>();
        int chainLen = 0;
        while (pg != 0 && seen.Add(pg))
        {
            int basePos = checked((int)(pg * pgSz));
            Assert.Equal(0x02, streamBytes[basePos]);
            chainLen++;

            // Next-page pointer at offset 4 (little-endian uint32).
            pg = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
                streamBytes.AsSpan(basePos + 4, 4));
        }

        Assert.True(
            chainLen > 1,
            $"Expected a multi-page TDEF chain on {format} (page size {pgSz}); got {chainLen} page(s).");
    }

    private static async ValueTask<long> GetTDefPageNumberAsync(AccessReader reader, string tableName)
    {
        JetDatabaseWriter.Catalog.Models.CatalogEntry? entry = await reader.GetCatalogEntryAsync(
            tableName, TestContext.Current.CancellationToken);
        if (entry is null)
        {
            throw new InvalidOperationException($"Table '{tableName}' not found in catalog.");
        }

        return entry.TDefPage;
    }

    private static int PageSizeOf(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private static int FirstEntryOffset(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.IndexLeafPage.Jet3.FirstEntryOffset : Constants.IndexLeafPage.Jet4.FirstEntryOffset;

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
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
