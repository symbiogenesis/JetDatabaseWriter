namespace JetDatabaseWriter.Tests.Infrastructure;

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Walks an index B-tree's leaf chain for tests that check an index against
/// the rows of its table.
/// </summary>
internal static class IndexLeafChain
{
    /// <summary>
    /// Descends from <paramref name="rootPage"/> to the leftmost leaf and
    /// returns every leaf entry along the sibling chain, in chain order.
    /// Asserts that every page is an index page owned by
    /// <paramref name="tdefPage"/> and that keys never decrease.
    /// </summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="tdefPage">The owning table's TDEF page.</param>
    /// <param name="rootPage">The index root page (<c>first_dp</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>The leaf entries.</returns>
    public static async Task<List<IndexEntry>> ReadEntriesAsync(DatabaseFile db, long tdefPage, long rootPage, CancellationToken cancellationToken)
    {
        IndexPageLayout layout = db.Profile.IndexPage;
        long current = rootPage;
        for (int depth = 0; ; depth++)
        {
            Assert.True(depth < 16, $"Index descent from root {rootPage} did not reach a leaf.");
            byte[] page = await db.ReadPageCopyAsync(current, cancellationToken);
            Assert.True(Ri32(page, 4) == tdefPage, $"Index page {current} is owned by page {Ri32(page, 4)}, not the table's TDEF {tdefPage}.");
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            Assert.True(page[0] == Constants.IndexLeafPage.PageTypeIntermediate, $"Page {current} has type 0x{page[0]:X2}, not an index page.");
            List<DecodedIntermediateEntry> children = IndexPageCodec.DecodeIntermediateEntries(layout, page, db.PageSizeBytes);
            Assert.NotEmpty(children);
            current = children[0].ChildPage;
        }

        var entries = new List<IndexEntry>();
        byte[]? previousKey = null;
        int leafBudget = 100_000;
        while (current != 0)
        {
            Assert.True(--leafBudget > 0, "Index leaf chain does not end.");
            byte[] leaf = await db.ReadPageCopyAsync(current, cancellationToken);
            Assert.True(leaf[0] == Constants.IndexLeafPage.PageTypeLeaf, $"Leaf chain reached page {current} of type 0x{leaf[0]:X2}.");
            Assert.True(Ri32(leaf, 4) == tdefPage, $"Leaf page {current} is owned by page {Ri32(leaf, 4)}, not the table's TDEF {tdefPage}.");
            foreach (IndexEntry entry in IndexPageCodec.DecodeLeafEntries(layout, leaf, db.PageSizeBytes))
            {
                Assert.True(previousKey is null || IndexPageCodec.CompareKeyBytes(previousKey, entry.Key) <= 0, $"Leaf page {current} holds a key out of order.");
                previousKey = entry.Key;
                entries.Add(entry);
            }

            current = IndexPageCodec.ReadNextPage(layout, leaf);
        }

        return entries;
    }

    /// <summary>
    /// Returns every page of the index B-tree rooted at
    /// <paramref name="rootPage"/>: the root, each intermediate page, each
    /// child an intermediate entry or <c>tail_page</c> names, and each leaf.
    /// Asserts that every page is an index page owned by
    /// <paramref name="tdefPage"/>.
    /// </summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="tdefPage">The owning table's TDEF page.</param>
    /// <param name="rootPage">The index root page (<c>first_dp</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>The tree's page numbers.</returns>
    public static async Task<HashSet<long>> ReadTreePagesAsync(DatabaseFile db, long tdefPage, long rootPage, CancellationToken cancellationToken)
    {
        IndexPageLayout layout = db.Profile.IndexPage;
        var pages = new HashSet<long>();
        var pending = new Stack<long>();
        pending.Push(rootPage);
        while (pending.Count > 0)
        {
            long current = pending.Pop();
            if (current == 0 || !pages.Add(current))
            {
                continue;
            }

            Assert.True(pages.Count < 100_000, $"The index tree rooted at {rootPage} does not end.");
            byte[] page = await db.ReadPageCopyAsync(current, cancellationToken);
            Assert.True(Ri32(page, 4) == tdefPage, $"Index page {current} is owned by page {Ri32(page, 4)}, not the table's TDEF {tdefPage}.");
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                continue;
            }

            Assert.True(page[0] == Constants.IndexLeafPage.PageTypeIntermediate, $"Page {current} has type 0x{page[0]:X2}, not an index page.");
            pending.Push(IndexPageCodec.ReadTailPage(layout, page));
            foreach (DecodedIntermediateEntry child in IndexPageCodec.DecodeIntermediateEntries(layout, page, db.PageSizeBytes))
            {
                pending.Push(child.ChildPage);
            }
        }

        return pages;
    }

    /// <summary>Returns the root page (<c>first_dp</c>) of every real index of the table at <paramref name="tdefPage"/>.</summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>The root pages, by real-index number.</returns>
    public static async Task<List<long>> ReadRealIndexRootsAsync(DatabaseFile db, long tdefPage, CancellationToken cancellationToken)
    {
        byte[]? tdef = await db.ReadTDefBytesAsync(tdefPage, cancellationToken);
        Assert.NotNull(tdef);
        int numCols = Ru16(tdef, db.TDef.NumCols);
        int numRealIdx = Ri32(tdef, db.TDef.NumRealIdx);
        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(db, tdef, numCols, numRealIdx);
        Assert.True(realIdxDescStart >= 0, $"The TDEF at page {tdefPage} could not be walked.");

        var roots = new List<long>(numRealIdx);
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            Assert.True(db.IndexLayoutInfo.TryReadRealIdxSlot(tdef, realIdxDescStart, ri, out RealIdxSlot slot));
            roots.Add((uint)Ri32(tdef, slot.FirstDpOffset));
        }

        return roots;
    }

    /// <summary>
    /// Asserts that the index rooted at <paramref name="rootPage"/> holds
    /// exactly one entry for every live row of the table at
    /// <paramref name="tdefPage"/>, each pointing at its row.
    /// </summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="rootPage">The index root page (<c>first_dp</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>A task that completes when the check has passed.</returns>
    public static async Task AssertCoversLiveRowsAsync(DatabaseFile db, long tdefPage, long rootPage, CancellationToken cancellationToken)
    {
        List<IndexEntry> entries = await ReadEntriesAsync(db, tdefPage, rootPage, cancellationToken);
        List<RowLocation> liveRows = await db.GetLiveRowLocationsAsync(tdefPage, cancellationToken);
        Assert.Equal(
            liveRows.Select(row => (row.PageNumber, row.RowIndex)).Order(),
            entries.Select(entry => (entry.DataPage, (int)entry.DataRow)).Order());
    }
}
