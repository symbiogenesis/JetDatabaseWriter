namespace JetDatabaseWriter.Tests;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Round-trip tests for the N-way split (N-way leaf and intermediate split) path.
/// Before N-way split, both <c>TryGreedySplitInTwo</c> (leaf) and
/// <c>TryGreedySplitIntermediateInTwo</c> bailed when a single-leaf splice
/// or single-intermediate update needed three or more pages, falling back
/// to the bulk rebuild bulk-rebuild path. N-way split generalises both helpers to N-way
/// (greedy left-fill, allocate the next page on overflow, repeat) and emits
/// one Replace + N-1 InsertAfter ops on the parent intermediate so the
/// surgical path engages instead. These tests force splices that span 3,
/// 4, or more leaf pages by bulk-inserting many ~255-byte composite TEXT
/// keys into a single target leaf and verify that all rows round-trip
/// across the surgical-only and surgical-plus-fallback paths.
/// </summary>
public sealed class IndexSurgicalNWaySplitTests
{
    private static readonly string[] CompositeKeyColumns = ["K1", "K2"];

    [Fact]
    public async Task BulkInsert_LeafSpliceNeedsThreePages_AllRowsRoundTrip()
    {
        // Fresh table, single bulk-insert of 40 rows whose ~260-byte
        // composite keys all land on the (initially empty) root leaf.
        // 40 × 260 = 10_400 bytes — comfortably exceeds 2 pages of leaf
        // payload (~4_080 bytes each on a 4_096-byte page). With the
        // 2-way splitter this would bail to bulk rebuild; with the N-way splitter
        // the splice succeeds surgically.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;
        const int rowCount = 40;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("V", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), i];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await AssertAllRowsPresentAsync(stream, rowCount, ct);
    }

    [Fact]
    public async Task BulkInsert_LeafSpliceNeedsManyPages_AllRowsRoundTrip()
    {
        // 200 rows in a single batch — splice runs to ~10 pages worth of
        // leaf entries. Exercises the N-way splitter at scale; the cap
        // matches Access default behaviour (no artificial bail floor).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;
        const int rowCount = 200;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("V", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), i];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await AssertAllRowsPresentAsync(stream, rowCount, ct);
    }

    [Fact]
    public async Task BulkInsert_AppendBatchOnExistingLeaf_NWaySplit_DataRoundTrips()
    {
        // Seed the table with a small bulk insert (single leaf, ~5 rows),
        // then in a separate writer session append a large batch whose
        // keys all sort AFTER the seeded keys. The append path's per-leaf
        // grouping routes the whole batch onto the rightmost leaf; the
        // resulting splice spans 3+ pages and engages the N-way split with
        // tail_page cascade up every captured ancestor.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("V", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var seed = new object[5][];
            for (int i = 0; i < 5; i++)
            {
                seed[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), i];
            }

            await writer.InsertRowsAsync("T", seed, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // 60 ascending-key rows whose values all sort AFTER the seed.
            var batch = new object[60][];
            for (int i = 0; i < 60; i++)
            {
                int n = 1_000 + i;
                batch[i] = [BuildKey(n, prefix: 'A'), BuildKey(n, prefix: 'M'), n];
            }

            await writer.InsertRowsAsync("T", batch, ct);
        }

        await AssertAllRowsPresentAsync(stream, expectedRows: 65, ct);
    }

    [Fact]
    public async Task BulkInsert_NWayLeafSplit_ParentNearCapacity_RecursesIntoIntermediateSplit()
    {
        // Step 1: drip-feed inserts across many leaves to fill a
        // parent-of-leaf intermediate to near-capacity (each batch hits
        // a different leaf, so the parent gains exactly one summary entry
        // per batch). Step 2: a single batch into one of those leaves
        // produces a 3-way leaf split → pushes 2 new summaries into the
        // (already-full) parent → parent itself splits N-way.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("V", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            // Seed: 300 rows in one bulk insert, distributed across many
            // leaves (the bulk rebuild bulk-rebuild path is engaged for this
            // initial population on an empty tree).
            var seed = new object[300][];
            for (int i = 0; i < 300; i++)
            {
                int n = i * 10;
                seed[i] = [BuildKey(n, prefix: 'A'), BuildKey(n, prefix: 'M'), n];
            }

            await writer.InsertRowsAsync("T", seed, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Drip feed 30 small inserts whose keys are interleaved across
            // many leaves (each batch hits ~5 leaves). Forces parent-of-
            // leaf intermediates to grow toward capacity.
            for (int b = 0; b < 30; b++)
            {
                var batch = new object[5][];
                for (int i = 0; i < 5; i++)
                {
                    int n = (b * 5 * 10) + i + 1; // odd offsets between seed rows
                    batch[i] = [BuildKey(n, prefix: 'A'), BuildKey(n, prefix: 'M'), n];
                }

                await writer.InsertRowsAsync("T", batch, ct);
            }
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Final BIG batch of 50 rows clustered into one narrow key range
            // → all land on one leaf → splice runs to multiple pages →
            // N-way leaf split → multiple summaries pushed into a
            // potentially-full parent → recursive intermediate split.
            var bigBatch = new object[50][];
            for (int i = 0; i < 50; i++)
            {
                int n = 5_000 + i; // cluster well above seeded range, single leaf target
                bigBatch[i] = [BuildKey(n, prefix: 'A'), BuildKey(n, prefix: 'M'), n];
            }

            await writer.InsertRowsAsync("T", bigBatch, ct);
        }

        // Total: 300 + 30 * 5 + 50 = 500 rows.
        await AssertAllRowsPresentAsync(stream, expectedRows: 500, ct);
    }

    [Fact]
    public async Task BulkInsert_NWayLeafSplit_UniqueIndexHonored()
    {
        // After an N-way leaf split, every key in the batch must remain
        // searchable via the index — including via the new pages. This
        // test stresses the index by enforcing uniqueness; a duplicate
        // insertion against any of the post-split keys must throw.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;
        const int rowCount = 80;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns) { IsUnique = true }],
                ct);

            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M')];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        // Reopen and try to insert a duplicate key from each of the now
        // multi-page leaf groups — must throw.
        await using (var writer = await OpenWriterAsync(stream))
        {
            int[] sampleIndexes = [0, 7, 13, 25, 41, 67, rowCount - 1];
            foreach (int i in sampleIndexes)
            {
                await Assert.ThrowsAsync<System.InvalidOperationException>(async () =>
                    await writer.InsertRowAsync(
                        "T",
                        new object[] { BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M') },
                        ct));
            }
        }

        await AssertAllRowsPresentAsync(stream, expectedRows: rowCount, ct);
    }

    private static async Task AssertAllRowsPresentAsync(MemoryStream stream, int expectedRows, System.Threading.CancellationToken ct)
    {
        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(expectedRows, dt!.Rows.Count);

        // Composite (K1, K2) is unique by construction in every test —
        // duplicate would indicate a corrupted index.
        var keys = new HashSet<(string, string)>();
        foreach (DataRow r in dt.Rows)
        {
            Assert.True(keys.Add(((string)r["K1"], (string)r["K2"])));
        }
    }

    private static string BuildKey(int n, char prefix)
    {
        // 255-byte deterministic key: prefix + 8-digit number + 246 chars
        // of derived padding. Lexicographic order tracks n.
        var sb = new StringBuilder(255);
        sb.Append(prefix);
        sb.Append(n.ToString("D8", System.Globalization.CultureInfo.InvariantCulture));
        for (int i = 0; i < 246; i++)
        {
            sb.Append((char)('A' + ((n + i) % 26)));
        }

        return sb.ToString();
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
