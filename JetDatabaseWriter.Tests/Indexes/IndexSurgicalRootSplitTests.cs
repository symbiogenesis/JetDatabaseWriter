namespace JetDatabaseWriter.Tests.Indexes;

using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Regression tests for the surgical <b>root-intermediate split</b> path in
/// <c>IndexBTreeEditor.TryStageIntermediateRewritesAsync</c>. When a cross-leaf
/// batch cascades enough splits to overflow the root intermediate, the editor
/// allocates a fresh root whose summary entries must point at the freshly
/// split pages and patches <c>first_dp</c> to it.
/// <para>
/// Table reads scan data pages, so they survive even a structurally corrupt
/// index. These tests therefore descend the index B-tree from its root and
/// assert that every leaf entry is reachable — a root whose child pointers
/// skip the split level orphans most leaves and is caught here.
/// </para>
/// </summary>
public sealed class IndexSurgicalRootSplitTests
{
    private static readonly string[] CompositeKeyColumns = ["K1", "K2"];
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task DripFeedInsertsGrowingTreeHeight_RootStaysReachable()
    {
        // Build a multi-level tree with large composite keys (~7 entries per
        // page), then drip-feed interleaved cross-leaf insert batches that
        // keep splitting leaves and pushing summaries up. Over many batches
        // the tree grows in height, exercising the root-split path. Validate
        // the full index is reachable from the root after EACH batch so a
        // committed root corruption cannot be masked by a later rebuild.
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("V", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                this.ct);

            // Seed on a coarse grid so later inserts fall strictly between
            // existing keys (mid-leaf → forces splits rather than appends).
            object[][] seed = new object[100][];
            for (int i = 0; i < 100; i++)
            {
                seed[i] = [BuildKey(i * 100, 'A'), BuildKey(i * 100, 'M'), i];
            }

            await writer.InsertRowsAsync("T", seed, this.ct);
        }

        await AssertIndexFullyReachableFromRootAsync(stream, "IX_K", this.ct);

        int expected = 100;
        for (int b = 0; b < 150; b++)
        {
            await using (AccessWriter writer = await OpenWriterAsync(stream))
            {
                // Each batch spreads across 8 dense sub-ranges (cross-leaf)
                // at mid-leaf offsets, so several leaves split at once and the
                // tree keeps growing in height (eventually overflowing the
                // root → root split).
                object[][] batch = new object[8][];
                for (int i = 0; i < 8; i++)
                {
                    int slot = (i * 1200) + (b * 7) + 13;
                    batch[i] = [BuildKey(slot, 'A'), BuildKey(slot, 'M'), 1_000_000 + (b * 8) + i];
                }

                await writer.InsertRowsAsync("T", batch, this.ct);
                expected += 8;
            }

            await AssertIndexFullyReachableFromRootAsync(stream, "IX_K", this.ct, expected);
        }
    }

    [Fact]
    public async Task MultiLevelTree_CrossLeafInsertBatch_IndexFullyReachable()
    {
        // A simpler single-batch counterpart to the drip-feed test: build a
        // multi-level tree, then apply one cross-leaf insert batch that splits
        // several leaves at once, and verify the whole index stays reachable
        // and uniform-depth.
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                this.ct);

            object[][] seed = new object[120][];
            for (int i = 0; i < 120; i++)
            {
                seed[i] = [BuildKey(i * 50, 'A'), BuildKey(i * 50, 'M')];
            }

            await writer.InsertRowsAsync("T", seed, this.ct);
        }

        int expected = 120;
        await AssertIndexFullyReachableFromRootAsync(stream, "IX_K", this.ct, expected);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            // 16 interleaved keys spread across the whole range → splits many
            // leaves in one cross-leaf batch, maximising upward cascade.
            object[][] batch = new object[16][];
            for (int i = 0; i < 16; i++)
            {
                int slot = (i * 375) + 25;
                batch[i] = [BuildKey(slot, 'A'), BuildKey(slot, 'M')];
            }

            await writer.InsertRowsAsync("T", batch, this.ct);
            expected += 16;
        }

        await AssertIndexFullyReachableFromRootAsync(stream, "IX_K", this.ct, expected);
    }

    /// <summary>
    /// Opens a reader on <paramref name="stream"/> and asserts that descending
    /// the named index from its root reaches exactly the table's row count of
    /// leaf entries (every entry reachable, no orphaned subtree, no cycle).
    /// </summary>
    /// <param name="stream">The database stream.</param>
    /// <param name="indexName">The index name.</param>
    /// <param name="ct">A token used to cancel the operation.</param>
    /// <param name="expectedRowCount">Optional expected row count; defaults to the live table count.</param>
    private static async Task AssertIndexFullyReachableFromRootAsync(
        MemoryStream stream,
        string indexName,
        CancellationToken ct,
        int? expectedRowCount = null)
    {
        await using AccessReader reader = await OpenReaderAsync(stream);

        int rowCount = expectedRowCount
            ?? (await reader.ReadDataTableAsync("T", cancellationToken: ct)).Rows.Count;

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("T", ct);
        IndexMetadata index = indexes.Single(i => i.Name == indexName);
        Assert.True(index.FirstDp > 0, "Index root page (first_dp) should be set.");

        var layout = IndexPageLayout.ForFormat(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        var visited = new HashSet<long>();
        int reachableLeafEntries = 0;
        bool sawIntermediate = false;
        var report = new StringBuilder();
        var current = new List<long> { index.FirstDp };
        int depth = 0;
        var leafLevels = new List<int>();

        while (current.Count > 0)
        {
            var next = new List<long>();
            int leaves = 0;
            int inters = 0;
            int childPtrs = 0;
            int levelLeafEntries = 0;
            foreach (long pageNum in current)
            {
                if (!visited.Add(pageNum))
                {
                    report.AppendLine(CultureInfo.InvariantCulture, $"  ** page {pageNum} revisited (cycle/shared subtree)");
                    continue;
                }

                byte[] page = await reader.GetRawPageBytesAsync(pageNum, ct);
                byte pageType = page[0];
                if (pageType == Constants.IndexLeafPage.PageTypeLeaf)
                {
                    leaves++;
                    int c = IndexPageCodec.DecodeLeafEntries(layout, page, pageSize).Count;
                    levelLeafEntries += c;
                    reachableLeafEntries += c;
                }
                else if (pageType == Constants.IndexLeafPage.PageTypeIntermediate)
                {
                    sawIntermediate = true;
                    inters++;
                    foreach (DecodedIntermediateEntry entry in IndexPageCodec.DecodeIntermediateEntries(layout, page, pageSize))
                    {
                        childPtrs++;
                        next.Add(entry.ChildPage);
                    }
                }
                else
                {
                    report.AppendLine(CultureInfo.InvariantCulture, $"  ** page {pageNum} unexpected type 0x{pageType:X2}");
                }
            }

            if (leaves > 0)
            {
                leafLevels.Add(depth);
            }

            report.AppendLine(CultureInfo.InvariantCulture, $"  level {depth}: pages={current.Count} leaves={leaves} inters={inters} childPtrs={childPtrs} leafEntries={levelLeafEntries}");
            current = next;
            depth++;
        }

        Assert.True(sawIntermediate, "Scenario should produce a multi-level tree (root is an intermediate page).");
        Assert.True(
            leafLevels.Count <= 1,
            $"Index '{indexName}' root={index.FirstDp}: leaves appear at multiple depths {string.Join(",", leafLevels)} (non-uniform B-tree).\n{report}");
        Assert.True(
            rowCount == reachableLeafEntries,
            $"Index '{indexName}' root={index.FirstDp}: reachable leaf entries {reachableLeafEntries} != row count {rowCount}.\n{report}");
    }

    private static string BuildKey(int n, char prefix)
    {
        // 255-byte deterministic key, lexicographically ordered by n.
        var sb = new StringBuilder(255);
        sb.Append(prefix)
            .Append(n.ToString("D8", CultureInfo.InvariantCulture));
        for (int i = 0; i < 246; i++)
        {
            sb.Append((char)('A' + ((n + i) % 26)));
        }

        return sb.ToString();
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
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
