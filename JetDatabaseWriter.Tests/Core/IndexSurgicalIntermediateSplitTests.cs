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
/// Stress / round-trip tests for the W4-C-7 (intermediate split on
/// overflow) path. Uses large TEXT index keys to force a small
/// per-page entry budget so multi-level trees with parent-of-leaf
/// intermediates near capacity arise from manageable row counts.
/// W4-C-7 v1 only handles parent-of-leaf overflow; higher-level
/// (recursive) intermediate splits bail to W4-D. These tests verify
/// data integrity round-trips through both surgical AND fallback
/// paths so neither path corrupts the index when large multi-level
/// trees experience cross-leaf mutations.
/// </summary>
public sealed class IndexSurgicalIntermediateSplitTests
{
    [Fact]
    public async Task LargeKeyMultiLevelTree_BulkInsertThenCrossLeafDelete_DataRoundTrips()
    {
        // 200-char keys → ~210 bytes/entry → ~18 entries/page. 400 rows
        // → ~22 leaves + 2-level intermediate (root with 2 entries above
        // 2 parent-of-leaf intermediates each holding ~11 leaf summaries,
        // OR root with 22 entries directly above leaves). Either way the
        // tree is multi-level. A subsequent cross-leaf delete may engage
        // W4-C-5 (in-place rewrites) and possibly W4-C-7 if parent-of-leaf
        // splits — or fall back to W4-D. We assert correctness either way.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K", typeof(string), maxLength: 220),
                    new ColumnDefinition("Tag", typeof(int)),
                ],
                [new IndexDefinition("IX_K", "K")],
                ct);

            var rows = new object[400][];
            for (int i = 0; i < 400; i++)
            {
                rows[i] = [BuildKey(i), i % 5];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            // Cross-leaf delete by non-indexed Tag column. With Tag values
            // in 0..4 distributed evenly, deleting Tag=2 removes ~80 rows
            // spread across many leaves. If their leaves' parent-of-leaf
            // intermediates need rewriting AND the rewrite happens to
            // overflow (rare for deletes but possible if max-key
            // propagation widens entries), W4-C-7 fires; otherwise W4-C-5
            // handles it in-place or W4-D rebuilds.
            int deleted = await writer.DeleteRowsAsync("T", "Tag", 2, ct);
            Assert.Equal(80, deleted);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(320, dt!.Rows.Count);
        foreach (DataRow r in dt.Rows)
        {
            Assert.NotEqual(2, (int)r["Tag"]);
        }
    }

    [Fact]
    public async Task LargeKeyMultiLevelTree_IncrementalInsertsTriggerSplits_DataRoundTrips()
    {
        // Drip-feed inserts so each batch potentially triggers a leaf
        // split that propagates up through the parent-of-leaf
        // intermediate. With 200-byte keys and ~18-entry intermediate
        // capacity, after ~18 leaves' worth of bulk content the root
        // intermediate sits near capacity; subsequent splits need
        // W4-C-7 OR fall through to W4-D.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K", typeof(string), maxLength: 220),
                    new ColumnDefinition("V", typeof(int)),
                ],
                [new IndexDefinition("IX_K", "K")],
                ct);

            // Initial bulk to set up multi-level tree.
            var seed = new object[200][];
            for (int i = 0; i < 200; i++)
            {
                seed[i] = [BuildKey(i * 3), i];
            }

            await writer.InsertRowsAsync("T", seed, ct);

            // Now drip-feed 30 more rows in 6 small batches. Each batch
            // is a cross-leaf change-set (keys spread across many leaves)
            // that may trigger leaf splits on the parent-of-leaf level.
            for (int b = 0; b < 6; b++)
            {
                var batch = new object[5][];
                for (int i = 0; i < 5; i++)
                {
                    int idx = (b * 5) + i;
                    batch[i] = [BuildKey((idx * 3) + 1), idx + 1000];
                }

                await writer.InsertRowsAsync("T", batch, ct);
            }
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(230, dt!.Rows.Count);

        // Verify all 230 keys present (no duplicate / missing rows).
        var keys = dt.Rows.Cast<DataRow>().Select(r => (string)r["K"]).ToHashSet();
        Assert.Equal(230, keys.Count);
    }

    [Fact]
    public async Task LargeKeyMultiLevelTree_DeleteAndReinsert_NoCorruption()
    {
        // Delete-then-insert cycle on a multi-level large-key tree.
        // Verifies that ANY combination of W4-C-5/6/7 + W4-D fallback
        // leaves the tree in a consistent state across multiple round-
        // trips.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K", typeof(string), maxLength: 220),
                ],
                [new IndexDefinition("IX_K", "K")],
                ct);

            var rows = new object[300][];
            for (int i = 0; i < 300; i++)
            {
                rows[i] = [BuildKey(i)];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        // Cycle 1: delete every 7th key, reinsert different ones.
        await using (var writer = await OpenWriterAsync(stream))
        {
            for (int i = 0; i < 300; i += 7)
            {
                await writer.DeleteRowsAsync("T", "K", BuildKey(i), ct);
            }

            var fresh = new object[10][];
            for (int i = 0; i < 10; i++)
            {
                fresh[i] = [BuildKey(1000 + i)];
            }

            await writer.InsertRowsAsync("T", fresh, ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);

        // 300 - ceil(300/7) deleted + 10 inserted = 300 - 43 + 10 = 267.
        Assert.Equal(267, dt!.Rows.Count);

        var keys = dt.Rows.Cast<DataRow>().Select(r => (string)r["K"]).ToHashSet();
        Assert.Equal(267, keys.Count);

        // The 10 reinserted keys are present.
        for (int i = 0; i < 10; i++)
        {
            Assert.Contains(BuildKey(1000 + i), keys);
        }

        // Deleted keys are gone.
        for (int i = 0; i < 300; i += 7)
        {
            Assert.DoesNotContain(BuildKey(i), keys);
        }
    }

    private static string BuildKey(int n)
    {
        // 200-byte deterministic key: 8-digit number prefix + 192 chars
        // of derived padding. Produces lexicographically increasing
        // keys when n increases (digits dominate sort).
        var sb = new StringBuilder(200);
        sb.Append(n.ToString("D8", System.Globalization.CultureInfo.InvariantCulture));
        for (int i = 0; i < 192; i++)
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
