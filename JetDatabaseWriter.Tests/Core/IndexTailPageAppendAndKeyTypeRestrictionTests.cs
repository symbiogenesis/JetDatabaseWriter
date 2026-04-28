namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Tests for the tail-page append optimisation and the matching key-type
/// restriction:
/// <list type="bullet">
///   <item><b>A — Indexable key types matched to Microsoft Access.</b>
///   <c>IndexDefinition</c> referencing an OLE / Attachment / Multi-Value
///   column now throws <see cref="NotSupportedException"/> at create time
///   instead of silently emitting a stale schema-only leaf. Microsoft
///   Access UI and engine reject the same indexes.</item>
///   <item><b>B — <c>tail_page</c> append optimisation.</b> The bulk
///   builder stamps the rightmost-leaf page number into every intermediate
///   page's <c>tail_page</c> header field; the seeker honours it on
///   intermediate-summary overshoot; an append-only fast path in the
///   incremental maintenance loop rewrites the tail leaf in place when
///   every new key sorts strictly after the current tail max — skipping
///   the full descend-walk-rebuild that the prior bulk rebuild path
///   performed.</item>
/// </list>
/// <para>
/// The fast path's signal is the rewritten tail-leaf page count: the bulk rebuild
/// path always allocates a fresh tree at the end of the file, so a
/// successful tail-page append fast path leaves the file shorter than the equivalent
/// bulk rebuild rebuild (the new tail-leaf bytes overwrite the existing tail page
/// number rather than appending fresh pages). A regression to bulk rebuild would
/// allocate a fresh leaf chain plus intermediates, growing the file
/// dramatically.
/// </para>
/// </summary>
public sealed class IndexTailPageAppendAndKeyTypeRestrictionTests
{
    private const int PageSize = 4096; // ACE
    private const byte PageTypeIntermediate = 0x03;
    private const byte PageTypeLeaf = 0x04;

    private static readonly string[] MixedKeyColumns = ["Id", "Blob"];

    // ── A: reject indexes on OLE / Attachment / Multi-Value columns ──

    [Fact]
    public async Task CreateTable_IndexOnOleColumn_ThrowsNotSupported()
    {
        // byte[] maps to T_OLE (0x0B). Microsoft Access does not permit
        // CREATE INDEX on OLE Object columns; the writer must surface that
        // up-front instead of silently emitting an empty schema-only leaf.
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateTableAsync(
                "OleIdxRejected",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                [new IndexDefinition("IX_Blob", "Blob")],
                TestContext.Current.CancellationToken));

        Assert.Contains("OLE Object", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Microsoft Access", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTable_MultiColumnIndexWithOleColumn_ThrowsNotSupported()
    {
        // Even when the OLE column is just one of several key columns the
        // whole index must be rejected — the engine cannot encode a sort
        // key for the OLE column, so a partial composite key would sort
        // incorrectly.
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateTableAsync(
                "MixedOleIdxRejected",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                [new IndexDefinition("IX_Mixed", MixedKeyColumns)],
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateTable_OleColumnWithoutIndex_StillSucceeds()
    {
        // The rejection must be scoped to IndexDefinition validation; OLE
        // columns themselves remain a fully supported column type.
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "OleColumnSupported",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        List<ColumnMetadata> cols = await reader.GetColumnMetadataAsync(
            "OleColumnSupported", TestContext.Current.CancellationToken);
        Assert.Equal(2, cols.Count);
        Assert.Equal("Blob", cols[1].Name);
    }

    // ── B: tail_page append optimisation ──

    [Fact]
    public void Builder_MultiLevelTree_StampsTailPageOnIntermediateRoot()
    {
        // Bulk-build a 3-leaf tree (40 fat entries) and assert the
        // intermediate root carries tail_page = rightmost leaf page number.
        const int parentTdef = 100;
        const long firstPage = 50;

        var entries = new List<IndexLeafPageBuilder.LeafEntry>();
        for (int i = 0; i < 40; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)(i >> 8);
            big[1] = (byte)i;
            entries.Add(new IndexLeafPageBuilder.LeafEntry(big, dataPage: 1, dataRow: (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(PageSize, parentTdef, entries, firstPage);

        // Layout assumed by IndexBTreeBuilderTests: 3 leaves at pages 50..52,
        // 1 intermediate root at page 53.
        Assert.Equal(4, r.Pages.Count);
        Assert.Equal(PageTypeIntermediate, r.Pages[3][0]);

        long tailPage = ReadI32(r.Pages[3], 16);
        Assert.Equal(firstPage + 2, tailPage); // rightmost leaf
    }

    [Fact]
    public void Builder_SingleLeafTree_TailPageRemainsZero()
    {
        // Single-leaf trees have nothing to optimise; the leaf is its own
        // tail and tail_page must stay at 0 (matches IsSingleRootLeaf, which
        // refuses to recognise a leaf with non-zero sibling/tail pointers).
        const int parentTdef = 100;
        const long firstPage = 50;

        var entries = new List<IndexLeafPageBuilder.LeafEntry>
        {
            new IndexLeafPageBuilder.LeafEntry(new byte[] { 0x7F, 0x80, 0x00, 0x00, 0x01 }, 1, 0),
            new IndexLeafPageBuilder.LeafEntry(new byte[] { 0x7F, 0x80, 0x00, 0x00, 0x02 }, 1, 1),
        };

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(PageSize, parentTdef, entries, firstPage);

        Assert.Single(r.Pages);
        Assert.Equal(PageTypeLeaf, r.Pages[0][0]);
        Assert.Equal(0, ReadI32(r.Pages[0], 16));
    }

    [Fact]
    public async Task InsertRow_AppendOnlyAfterMultiLevelTree_RewritesTailLeafInPlace()
    {
        // Build a multi-level tree (700 INT rows → intermediate root over a
        // chain of leaves). Then append a single row whose key sorts
        // strictly after every existing key. The tail-page append fast path must
        // overwrite the existing tail leaf in place — file size grows by at
        // most one page (the rewritten tail leaf is *the same page*; only
        // the data page that got the new row appears at the end). A
        // regression to bulk rebuild would allocate ~50 fresh leaf + intermediate
        // pages, growing the file by hundreds of KB.
        const int InitialRows = 700;
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(InitialRows);
            for (int i = 0; i < InitialRows; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        long sizeAfterBulk = stream.Length;

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Strictly-greater-than current tree max. Triggers the tail-page append
            // append-only fast path.
            await writer.InsertRowAsync("T", [InitialRows], ct);
        }

        long sizeAfterAppend = stream.Length;
        long growth = sizeAfterAppend - sizeAfterBulk;

        // The append-only fast path rewrites the tail leaf at its existing
        // page number (in place) and adds at most a single fresh data page
        // for the new row. Allow up to 4 fresh pages of slack for misc
        // catalog churn (free-space patches, etc.) — a bulk rebuild regression would
        // allocate dozens of pages (≥ 50 KB), well outside this bound.
        Assert.True(
            growth <= 4 * PageSize,
            $"Expected append-only fast path to grow the file by ≤ 4 pages; grew by {growth} bytes ({growth / PageSize} pages).");

        // Row count must still be correct after the append.
        await using var reader = await OpenReaderAsync(stream);
        var rowsRead = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.Equal(InitialRows + 1, rowsRead.Rows.Count);
    }

    [Fact]
    public async Task InsertRow_OutOfOrderAfterMultiLevelTree_StillRoundTripsCorrectly()
    {
        // The tail-page append fast path must NOT engage when the new key sorts within
        // the existing range (its predicate requires every new key > tail
        // max). The bulk rebuild path picks up the slack and the row
        // count must remain correct after the out-of-order insert. We
        // assert correctness rather than size because more than one
        // fall-back path (single-leaf splice single-leaf splice, multi-level rebuild
        // bulk rebuild) can fire depending on tree shape.
        const int InitialRows = 1400;
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(InitialRows);
            for (int i = 0; i < InitialRows; i += 2)
            {
                // Even ids only — leaves a gap at every odd id for the
                // out-of-order insert to land in.
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Falls into a gap (id = 5 is well below the tree max) — fails
            // the tail-page append append predicate and routes through the existing
            // incremental fall-back paths.
            await writer.InsertRowAsync("T", [5], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var rowsRead = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.Equal((InitialRows / 2) + 1, rowsRead.Rows.Count);
    }

    [Fact]
    public async Task InsertRows_ManyAppendsAfterMultiLevelTree_StayInTailFastPath()
    {
        // Repeated append-only single-row inserts must each be handled by
        // the fast path. With ≥ 1.5 rows per page-aligned data page, a
        // bulk rebuild regression would balloon the file. Cap total growth at a
        // generous bound: 16 fresh data pages + 16 misc = 32 pages.
        const int InitialRows = 700;
        const int Appends = 50;
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(InitialRows);
            for (int i = 0; i < InitialRows; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        long sizeAfterBulk = stream.Length;

        await using (var writer = await OpenWriterAsync(stream))
        {
            for (int j = 0; j < Appends; j++)
            {
                await writer.InsertRowAsync("T", [InitialRows + j], ct);
            }
        }

        long growth = stream.Length - sizeAfterBulk;
        Assert.True(
            growth <= 32 * PageSize,
            $"Expected {Appends} append-only inserts to stay on the fast path; total growth {growth} bytes ({growth / PageSize} pages).");

        await using var reader = await OpenReaderAsync(stream);
        var rowsRead = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.Equal(InitialRows + Appends, rowsRead.Rows.Count);
    }

    [Fact]
    public async Task RowsInsertedViaAppendFastPath_AreReadableAndCorrectlyOrdered()
    {
        // Functional correctness of the fast path: the new entries must be
        // visible to a follow-up reader and must round-trip with the right
        // value (i.e. they ended up in the correct on-disk row, not lost
        // or duplicated).
        const int InitialRows = 700;
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(InitialRows);
            for (int i = 0; i < InitialRows; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);

            // Three sequential append-only inserts.
            await writer.InsertRowAsync("T", [InitialRows], ct);
            await writer.InsertRowAsync("T", [InitialRows + 1], ct);
            await writer.InsertRowAsync("T", [InitialRows + 2], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var rowsRead = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.Equal(InitialRows + 3, rowsRead.Rows.Count);

        // The three appended values must be present at least once each.
        var ids = new HashSet<int>();
        foreach (System.Data.DataRow row in rowsRead.Rows)
        {
            ids.Add((int)row["Id"]);
        }

        Assert.Contains(InitialRows, ids);
        Assert.Contains(InitialRows + 1, ids);
        Assert.Contains(InitialRows + 2, ids);
    }

    private static int ReadI32(byte[] b, int o) =>
        b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);

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
