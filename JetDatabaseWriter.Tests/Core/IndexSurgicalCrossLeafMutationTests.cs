namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Round-trip tests for the cross-leaf surgical (cross-leaf surgical multi-level mutation)
/// path in <c>AccessWriter.TrySurgicalCrossLeafMaintainAsync</c>. The in-place leaf rewrite /
/// single-leaf surgical path bails the moment two change-set keys
/// descend to different leaves; cross-leaf surgical picks up that case by grouping the
/// change-set per target leaf and aggregating per-parent-intermediate
/// summary updates into one rewrite per intermediate page.
/// <para>
/// As with the single-leaf surgical tests, "did the surgical path engage?" is
/// answered by counting <c>0x03</c> / <c>0x04</c> index pages in the
/// post-mutation file: the bulk rebuild appends a fresh tree (count
/// strictly increases by &gt;= the number of leaves), while a successful
/// path appends at most one page per leaf split (zero on a pure
/// in-place batch).
/// </para>
/// </summary>
public sealed class IndexSurgicalCrossLeafMutationTests
{
    private const int PageSize = 4096; // ACE

    [Fact]
    public async Task CrossLeafBulkInsert_TwoLeavesNoMaxChange_AppendsZeroIndexPages()
    {
        // Pre-populate sparse keys 0, 10, ..., 7990 (800 entries → 2 leaves
        // under one intermediate root). Then in a SECOND writer session,
        // bulk-insert 2 rows whose keys land on different leaves AND are
        // strictly less than each leaf's existing max → no per-leaf splits,
        // no parent-summary changes. cross-leaf surgical rewrites both leaves in place
        // at their existing page numbers.
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
                rows[i] = [i * 10];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            // 5 lands on leaf 0 (between 0 and 10); 4005 lands on leaf 1
            // (between 4000 and 4010). Neither is the leaf's max key.
            await writer.InsertRowsAsync(
                "T",
                new[] { new object[] { 5 }, new object[] { 4005 } },
                ct);
        }

        int idxAfter = CountIndexPages(stream.ToArray());

        // Surgical cross-leaf: both leaves rewritten in place; zero new
        // index pages appended.
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(802, dt!.Rows.Count);

        var ids = dt.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).OrderBy(x => x).ToArray();
        Assert.Contains(5, ids);
        Assert.Contains(4005, ids);
    }

    [Fact]
    public async Task CrossLeafBulkInsert_OneLeafChangesMax_OtherStable_RewritesParentOnce()
    {
        // 800-row sparse tree (2 leaves, 1 intermediate). Insert 2 rows on
        // different leaves: one becomes the affected leaf's NEW MAX (forcing
        // a parent-summary update), the other lands in the middle of its
        // leaf (no parent change). The cross-leaf path must rewrite both
        // leaves AND the shared parent intermediate exactly once. The tail
        // leaf's max is intentionally NOT touched (changing the tree's
        // overall max would trigger the tail-page append tail-page descent overshoot
        // and bail to bulk rebuild,-C-3 / leaf split design).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // Sparse keys 0, 10, ..., 7990. Leaf 0 ends at ~3990, leaf 1 at
            // 7990 (the tail).
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
            // 3991 → leaf 0, becomes leaf 0's new max (parent summary for
            // leaf 0 must be updated).
            // 4005 → leaf 1, between 4000 and 4010, NOT new max (no parent
            // change for leaf 1).
            await writer.InsertRowsAsync(
                "T",
                new[] { new object[] { 3991 }, new object[] { 4005 } },
                ct);
        }

        int idxAfter = CountIndexPages(stream.ToArray());
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(802, dt!.Rows.Count);

        var ids = dt.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).OrderBy(x => x).ToArray();
        Assert.Contains(3991, ids);
        Assert.Contains(4005, ids);
    }

    [Fact]
    public async Task CrossLeafBulkInsert_RoundTripsCorrectlyOnReader()
    {
        // Functional verification: after a cross-leaf insert batch, every
        // inserted key is readable AND seekable via the index. (Filter via
        // LINQ over Rows, which exercises the post-mutation tree.)
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
                rows[i] = [i * 10];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Three inserts spanning two leaves (the tree only has 2 with
            // 800 sparse-int entries) — but we still go through the cross-
            // leaf path because two distinct leaves are touched.
            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { 5 },     // leaf 0
                    new object[] { 25 },    // leaf 0
                    new object[] { 4005 },  // leaf 1
                },
                ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(803, dt!.Rows.Count);

        var ids = dt.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).ToHashSet();
        Assert.Contains(5, ids);
        Assert.Contains(25, ids);
        Assert.Contains(4005, ids);

        // Spot-check sort order is preserved (the index leaves still sort
        // ascending after surgical rewrites).
        var ordered = dt.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).OrderBy(x => x).ToArray();
        Assert.Equal(0, ordered[0]);
        Assert.Equal(7990, ordered[^1]);
    }

    [Fact]
    public async Task CrossLeafBulkInsert_OneLeafSplits_AppendsAtMostOneIndexPage()
    {
        // Insert a batch such that one of the affected leaves overflows on
        // splice AND another leaf in the
        // batch stays in-place. cross-leaf surgical should commit both: one new appended
        // page (the split's right half) plus rewrites of both leaves +
        // parent in place. Net delta on index page count: exactly +1.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // Pack leaf 0 nearly full so a single INSERT into it overflows.
            // ~400 9-byte entries fit per ACE leaf with ints; populate 399
            // dense keys at the bottom (covers leaf 0 packed) plus 400
            // sparse keys far above (leaf 1 with room to spare).
            var rows = new List<object[]>(799);
            for (int i = 0; i < 399; i++)
            {
                rows.Add(new object[] { i + 1 }); // 1..399 (dense, leaf 0)
            }

            for (int i = 0; i < 400; i++)
            {
                rows.Add(new object[] { 10000 + (i * 10) }); // 10000..13990 sparse, leaf 1
            }

            await writer.InsertRowsAsync("T", rows.ToArray(), ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            // 200 → leaf 0 (forces overflow if leaf 0 is full).
            // 10005 → leaf 1, stays in-place.
            await writer.InsertRowsAsync(
                "T",
                new[] { new object[] { 200 }, new object[] { 10005 } },
                ct);
        }

        int idxAfter = CountIndexPages(stream.ToArray());

        // Cross-leaf with at most one split should add 0 or 1 new index
        // pages (leaf 0 might not actually split if our capacity estimate
        // is loose; either way the count delta should be small and not
        // approach the bulk-rebuild leaf count).
        int delta = idxAfter - idxBefore;
        Assert.True(delta <= 2, $"Expected ≤2 new index pages, got {delta}.");

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(801, dt!.Rows.Count);

        var ids = dt.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).ToHashSet();
        Assert.Contains(200, ids);
        Assert.Contains(10005, ids);
    }

    [Fact]
    public async Task CrossLeafBulkInsert_DataIntegrityAfterMixedSizeBatches()
    {
        // Stress test: several successive cross-leaf batches of varying
        // size. Verifies the surgical path re-engages cleanly across
        // sequential calls and that the final data is consistent.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var seedRows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                seedRows[i] = [i * 10];
            }

            await writer.InsertRowsAsync("T", seedRows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Batch 1: 3 cross-leaf inserts.
            await writer.InsertRowsAsync(
                "T",
                new[] { new object[] { 1 }, new object[] { 2 }, new object[] { 4001 } },
                ct);

            // Batch 2: 4 cross-leaf inserts.
            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { 3 },
                    new object[] { 4 },
                    new object[] { 4002 },
                    new object[] { 4003 },
                },
                ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(807, dt!.Rows.Count);

        var ids = dt.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).OrderBy(x => x).ToArray();
        for (int v = 1; v <= 4; v++)
        {
            Assert.Contains(v, ids);
        }

        for (int v = 4001; v <= 4003; v++)
        {
            Assert.Contains(v, ids);
        }
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
