namespace JetDatabaseWriter.Tests.Infrastructure;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages;

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
        long pageCount = db.PageCount;
        for (long pageNumber = 3; pageNumber < pageCount; pageNumber++)
        {
            if (await allocator.IsPageFreeAsync(pageNumber, cancellationToken))
            {
                continue;
            }

            byte[] page = await db.ReadPageCopyAsync(pageNumber, cancellationToken);
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
        long pageCount = db.PageCount;
        for (long pageNumber = 3; pageNumber < pageCount; pageNumber++)
        {
            byte[] page = await db.ReadPageCopyAsync(pageNumber, cancellationToken);
            if ((page[0] == Constants.PageTypes.Freed || IsAllZero(page))
                && !await allocator.IsPageFreeAsync(pageNumber, cancellationToken))
            {
                _ = unlinked.Add(pageNumber);
            }
        }

        return unlinked;
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
