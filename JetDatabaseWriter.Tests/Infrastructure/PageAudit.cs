namespace JetDatabaseWriter.Tests.Infrastructure;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Classifies pages against the global page-allocation map, for tests that
/// check an operation gives back the pages it reserved but never linked.
/// </summary>
internal static class PageAudit
{
    /// <summary>
    /// Returns every page from 3 up to the end of file that the global usage
    /// map does not list as free, except reference usage-map pages: releasing
    /// a page can promote the global map and add one, which is not a leak.
    /// </summary>
    /// <param name="db">The database file to read, a writer's when a transaction is active.</param>
    /// <param name="allocator">The allocator that reads the global usage map.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>The allocated page numbers, ascending.</returns>
    public static async ValueTask<SortedSet<long>> FindAllocatedPagesAsync(DatabaseFile db, PageAllocator allocator, CancellationToken cancellationToken)
    {
        var allocated = new SortedSet<long>();
        long pageCount = db.Pages.PageCount;
        for (long pageNumber = 3; pageNumber < pageCount; pageNumber++)
        {
            if (await allocator.IsPageFreeAsync(pageNumber, cancellationToken))
            {
                continue;
            }

            byte[] page = await db.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);
            if (page[0] != Constants.PageTypes.UsageMap)
            {
                _ = allocated.Add(pageNumber);
            }
        }

        return allocated;
    }

    /// <summary>
    /// Returns every page from 3 up to the end of file that holds nothing (all
    /// zero bytes, or the freed-page stamp) yet is not listed as free by the
    /// global usage map: a reservation that was neither written nor released.
    /// </summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="allocator">The allocator that reads the global usage map.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>The unlinked reserved page numbers, ascending.</returns>
    public static async ValueTask<SortedSet<long>> FindUnlinkedReservedPagesAsync(DatabaseFile db, PageAllocator allocator, CancellationToken cancellationToken)
    {
        var unlinked = new SortedSet<long>();
        long pageCount = db.Pages.PageCount;
        for (long pageNumber = 3; pageNumber < pageCount; pageNumber++)
        {
            byte[] page = await db.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);
            if ((page[0] == Constants.PageTypes.Freed || IsAllZero(page))
                && !await allocator.IsPageFreeAsync(pageNumber, cancellationToken))
            {
                _ = unlinked.Add(pageNumber);
            }
        }

        return unlinked;
    }

    /// <summary>
    /// Returns every index page (<c>0x03</c> or <c>0x04</c>) owned by the table
    /// at <paramref name="tdefPage"/> that the global usage map does not list
    /// as free, yet no real index of the table reaches from its
    /// <c>first_dp</c>: a tree that a rebuild replaced and never gave back.
    /// </summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="allocator">The allocator that reads the global usage map.</param>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>The unreachable index page numbers, ascending.</returns>
    public static async ValueTask<SortedSet<long>> FindUnreachableIndexPagesAsync(DatabaseFile db, PageAllocator allocator, long tdefPage, CancellationToken cancellationToken)
    {
        var reachable = new HashSet<long>();
        foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(db, tdefPage, cancellationToken))
        {
            if (root != 0)
            {
                reachable.UnionWith(await IndexLeafChain.ReadTreePagesAsync(db, tdefPage, root, cancellationToken));
            }
        }

        var unreachable = new SortedSet<long>();
        long pageCount = db.Pages.PageCount;
        for (long pageNumber = 3; pageNumber < pageCount; pageNumber++)
        {
            if (reachable.Contains(pageNumber))
            {
                continue;
            }

            byte[] page = await db.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);
            if (page[0] is Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf
                && Ri32(page, 4) == tdefPage
                && !await allocator.IsPageFreeAsync(pageNumber, cancellationToken))
            {
                _ = unreachable.Add(pageNumber);
            }
        }

        return unreachable;
    }

    private static bool IsAllZero(byte[] page)
    {
        foreach (byte b in page)
        {
            if (b != 0)
            {
                return false;
            }
        }

        return true;
    }
}
