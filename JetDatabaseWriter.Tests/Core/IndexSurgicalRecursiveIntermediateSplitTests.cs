namespace JetDatabaseWriter.Tests;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Round-trip tests for the recursive higher-level intermediate split path.
/// The plain parent-of-leaf intermediate split only handles
/// parent-of-leaf overflow; the recursive path lets ANY captured intermediate level split
/// in place by computing the left half's <c>tail_page</c> from the
/// rightmost child intermediate's effective tail (staged override, staged
/// rewrite, or live page). These tests force a 3+ level tree via composite
/// long-text keys so per-page entry budget is small enough that the
/// non-parent-of-leaf intermediate level can overflow without inflating row
/// counts to the millions, then verify that ALL row data round-trips
/// across the surgical and the bulk rebuild fallback paths interchangeably.
/// </summary>
public sealed class IndexSurgicalRecursiveIntermediateSplitTests
{
    private static readonly string[] CompositeKeyColumns = ["K1", "K2"];

    [Fact]
    public async Task DeepMultiLevelTree_BulkInsertThenCrossLeafDelete_DataRoundTrips()
    {
        // Composite (K1, K2) of 255-byte TEXT columns → ~530 byte/entry.
        // ~7 entries per page → 3-level tree at >49 rows. With 600 rows we
        // get a comfortably 3-level tree (root → mid intermediates →
        // parent-of-leaf → leaves). A subsequent cross-leaf delete by a
        // non-indexed Tag column may trigger leaf-merge plus mid-level
        // intermediate updates that require the recursive split path
        // (which the plain parent-of-leaf split cannot reach).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        const int rowCount = 600;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("Tag", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), i % 5];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Cross-leaf delete of ~120 rows spread across many leaves.
            int deleted = await writer.DeleteRowsAsync("T", "Tag", 2, ct);
            Assert.Equal(rowCount / 5, deleted);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(rowCount - (rowCount / 5), dt!.Rows.Count);
        foreach (DataRow r in dt.Rows)
        {
            Assert.NotEqual(2, (int)r["Tag"]);
        }
    }

    [Fact]
    public async Task DeepMultiLevelTree_DripFeedInsertsCrossingMidIntermediateBoundaries_DataRoundTrips()
    {
        // Build a near-3-level tree, then drip-feed inserts whose keys
        // span many leaves AND many mid-level intermediates. This is the
        // classic shape that engages a leaf split → propagated summary
        // insert into a parent-of-leaf intermediate near capacity → its
        // own split → propagated summary insert into the mid-level
        // intermediate. The plain parent-of-leaf split bails at the
        // mid-level overflow; the recursive path handles it in place.
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

            // Initial bulk: 500 rows on even-numbered slot positions.
            var seed = new object[500][];
            for (int i = 0; i < 500; i++)
            {
                seed[i] = [BuildKey(i * 4, prefix: 'A'), BuildKey(i * 4, prefix: 'M'), i];
            }

            await writer.InsertRowsAsync("T", seed, ct);

            // Drip-feed 60 inserts in 10 small batches, each batch
            // spanning many leaves at odd-numbered slot positions.
            for (int b = 0; b < 10; b++)
            {
                var batch = new object[6][];
                for (int i = 0; i < 6; i++)
                {
                    int idx = (b * 6) + i;
                    int slot = (idx * 4) + 1; // off-by-one from seed grid
                    batch[i] = [BuildKey(slot, prefix: 'A'), BuildKey(slot, prefix: 'M'), idx + 1_000_000];
                }

                await writer.InsertRowsAsync("T", batch, ct);
            }
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(560, dt!.Rows.Count);

        // Verify uniqueness of (K1, K2) composite — no duplicate keys
        // (would indicate a corrupted index or row split).
        var keyPairs = dt.Rows.Cast<DataRow>()
            .Select(r => ((string)r["K1"], (string)r["K2"]))
            .ToHashSet();
        Assert.Equal(560, keyPairs.Count);
    }

    [Fact]
    public async Task DeepMultiLevelTree_DeleteAndReinsertCycle_NoCorruption()
    {
        // Delete-then-insert cycles on a deep multi-level tree. Verifies
        // that any combination of surgical + bulk rebuild fallback leaves
        // the tree in a consistent state across multiple round-trips.
        // Uses composite keys so trees grow past root capacity at
        // manageable row counts.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        const int initialRows = 700;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var rows = new object[initialRows][];
            for (int i = 0; i < initialRows; i++)
            {
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M')];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        // Cycle: delete every 11th key, reinsert different ones.
        await using (var writer = await OpenWriterAsync(stream))
        {
            for (int i = 0; i < initialRows; i += 11)
            {
                await writer.DeleteRowsAsync(
                    "T",
                    "K1",
                    BuildKey(i, prefix: 'A'),
                    ct);
            }

            var fresh = new object[20][];
            for (int i = 0; i < 20; i++)
            {
                fresh[i] = [BuildKey(10_000 + i, prefix: 'A'), BuildKey(10_000 + i, prefix: 'M')];
            }

            await writer.InsertRowsAsync("T", fresh, ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);

        int deletedCount = (initialRows + 10) / 11; // ceil(700/11) = 64
        int expected = initialRows - deletedCount + 20;
        Assert.Equal(expected, dt!.Rows.Count);

        var keyPairs = dt.Rows.Cast<DataRow>()
            .Select(r => ((string)r["K1"], (string)r["K2"]))
            .ToHashSet();
        Assert.Equal(expected, keyPairs.Count);

        // Every reinserted key is present.
        for (int i = 0; i < 20; i++)
        {
            Assert.Contains(
                (BuildKey(10_000 + i, prefix: 'A'), BuildKey(10_000 + i, prefix: 'M')),
                keyPairs);
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
