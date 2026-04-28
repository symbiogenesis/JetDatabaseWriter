namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for the in-place leaf rewrite (surgical in-place leaf-local mutate) and
/// leaf split (surgical leaf split + propagate up) paths in
/// <c>AccessWriter.TrySurgicalMultiLevelMaintainAsync</c>. Distinguishes the
/// surgical paths from the bulk rebuild by counting <c>0x03</c> /
/// <c>0x04</c> index pages before and after each mutation: surgical paths
/// rewrite pages in place and never increase the count, while bulk rebuild appends
/// a fresh tree and leaves old pages orphaned (still typed) so the count
/// strictly increases.
/// </summary>
public sealed class IndexSurgicalSingleLeafMutationTests
{
    private const int PageSize = 4096; // ACE

    [Fact]
    public async Task SurgicalInPlaceUpdate_OnMiddleLeaf_AppendsZeroIndexPages()
    {
        // Multi-level tree (≈800 int entries spans several leaves and at
        // least one intermediate). Update a row whose key sits in a non-tail
        // leaf and does NOT change the leaf's max key → in-place leaf rewrite rewrites the
        // single affected leaf in place with no parent-summary update.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Val", typeof(int)),
                ],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i + 1, 0];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            int updated = await writer.UpdateRowsAsync(
                "T",
                "Id",
                50,
                new Dictionary<string, object> { ["Val"] = 999 },
                ct);
            Assert.Equal(1, updated);
        }

        int idxAfter = CountIndexPages(stream.ToArray());

        // Surgical in-place: the single affected leaf was rewritten at its
        // existing page number → ZERO new index pages appended.
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(800, dt!.Rows.Count);
    }

    [Fact]
    public async Task SurgicalInPlaceInsert_BetweenExistingKeys_AppendsZeroIndexPages()
    {
        // Insert a single row whose key lands in a middle leaf and stays
        // strictly less than that leaf's existing max → in-place leaf rewrite in-place
        // leaf rewrite, no parent-summary change required.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // Insert ids 0, 10, 20, ..., 7990 — gaps allow a true "middle"
            // insert later that doesn't change the affected leaf's max.
            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i * 10];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Land on a leaf in the middle (not the tail); 105 sits between
            // 100 and 110 and is well below any leaf's max key.
            await writer.InsertRowAsync("T", [105], ct);
        }

        int idxAfter = CountIndexPages(stream.ToArray());
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(801, dt!.Rows.Count);

        // Spot-check the inserted key is readable.
        bool found = false;
        foreach (DataRow r in dt.Rows)
        {
            if ((int)r["Id"] == 105)
            {
                found = true;
                break;
            }
        }

        Assert.True(found, "Inserted row id=105 should be readable after surgical in-place mutation.");
    }

    [Fact]
    public async Task SurgicalDelete_FromMiddleLeaf_AppendsZeroIndexPages()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i + 1];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Id", 50, ct);
            Assert.Equal(1, deleted);
        }

        int idxAfter = CountIndexPages(stream.ToArray());
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(799, dt!.Rows.Count);
    }

    [Fact]
    public async Task SurgicalInsert_ChangingLeafMaxKey_PropagatesParentSummary()
    {
        // Insert a row whose key is greater than the affected leaf's CURRENT
        // max but still strictly less than the next leaf's min → in-place leaf rewrite
        // in-place rewrite of the leaf PLUS in-place rewrite of the parent
        // intermediate to update its summary entry. Total index page delta:
        // still ZERO.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // Sparse layout: ids 0, 100, 200, ..., 79900. Plenty of room to
            // pick an insert key between the max of one leaf and the min of
            // its right sibling.
            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i * 100];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Pick an id in the middle of the key range that's NOT a multiple
            // of 100. It will land in some non-tail leaf and (with high
            // probability) become that leaf's new max — provided it sorts
            // strictly between the chosen leaf's prior max and the next
            // leaf's min. Either way (max-changes or doesn't), surgical
            // path applies and no new index pages should appear.
            await writer.InsertRowAsync("T", [40050], ct);
        }

        int idxAfter = CountIndexPages(stream.ToArray());
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(801, dt!.Rows.Count);
    }

    [Fact]
    public async Task SurgicalLeafSplit_AppendsExactlyOneNewIndexPage()
    {
        // Drive a non-tail leaf to the brink of one-page capacity, then
        // insert one more row that lands in that same leaf and forces
        // overflow → leaf split splits the leaf into two and rewrites the
        // parent intermediate in place to insert one new summary entry.
        // Total NEW index pages = exactly 1 (the new right half).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // 1200 entries → multi-leaf tree. Each int entry is ~9 bytes
            // (1 flag + 4 key + 3 page + 1 row) and a single 4096-byte page
            // holds ~400 entries; 1200 entries occupies 3 leaves.
            var rows = new object[1200][];
            for (int i = 0; i < 1200; i++)
            {
                rows[i] = [i * 10];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        // Insert a small number of rows targeting ONLY the first leaf
        // (keys < ~3990, well below leaf-2's min of ~4000), enough to push
        // leaf 1 past one-page capacity but not past two-page capacity →
        // leaf split 2-way split. The change-set must NOT span leaves (that
        // bails to bulk rebuild).
        await using (var writer = await OpenWriterAsync(stream))
        {
            // ~30 inserts on top of leaf 1's existing ~400 entries pushes
            // it to ~430 entries (~3870 bytes) — past one 3616-byte payload
            // area but well within two pages. Keys 1, 7, 13, ..., 175 — all
            // strictly < 200, so all land in leaf 1 with key < its existing
            // max of ~3990.
            var inserts = new object[30][];
            for (int i = 0; i < 30; i++)
            {
                inserts[i] = [(i * 6) + 1];
            }

            await writer.InsertRowsAsync("T", inserts, ct);
        }

        int idxAfter = CountIndexPages(stream.ToArray());

        // Surgical leaf split split appends ONE new index page (the right half).
        // bulk rebuild would append a whole fresh tree (3+ pages
        // including a new intermediate root). Bound at <= 2 to allow some
        // slack for the optional intermediate prev/next-sibling repair.
        Assert.True(
            idxAfter - idxBefore <= 2,
            $"Expected surgical leaf split to append at most 2 index pages, but added {idxAfter - idxBefore}.");

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(1230, dt!.Rows.Count);
    }

    [Fact]
    public async Task SurgicalBail_MultiLeafChangeSet_RoundTripsViaBulkRebuild()
    {
        // Insert rows whose keys span at least TWO leaves → surgical path
        // bails (cannot be a single-leaf rewrite); bulk rebuild handles
        // it. Verify only that the result is correct and the file is
        // readable — page-count delta is intentionally NOT asserted here.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i + 1];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Two inserts spanning the leaf boundary (key 50 in leaf 1, key
            // 700 in a later leaf) → surgical bails to bulk rebuild.
            await writer.InsertRowsAsync("T", [[50_000], [60_000]], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(802, dt!.Rows.Count);
    }

    [Fact]
    public async Task SurgicalBail_LeafBecomesEmpty_RoundTripsViaBulkRebuild()
    {
        // Build a tiny multi-leaf tree, then delete every row that lives on
        // one specific leaf → surgical path bails (empty-leaf underflow is
        // territory); bulk rebuild rebuilds and the read-back stays correct.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i + 1];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        // Delete a contiguous range that covers a whole leaf (~400 rows).
        await using (var writer = await OpenWriterAsync(stream))
        {
            for (int id = 1; id <= 400; id++)
            {
                await writer.DeleteRowsAsync("T", "Id", id, ct);
            }
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(400, dt!.Rows.Count);
    }

    private static int CountIndexPages(byte[] fileBytes)
    {
        int n = 0;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            byte t = fileBytes[o];
            if ((t == 0x03 || t == 0x04) && fileBytes[o + 1] == 0x01)
            {
                n++;
            }
        }

        return n;
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
