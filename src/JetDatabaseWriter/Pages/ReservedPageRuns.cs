namespace JetDatabaseWriter.Pages;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// The page runs one operation has reserved through
/// <see cref="PageAllocator"/> but not yet linked into the file. A run
/// counts as linked once a write makes it reachable: a TDEF <c>first_dp</c>
/// patch, a usage-map row, or a sibling or parent pointer. Until then the
/// operation can still abandon it, by bailing, throwing, or taking a fallback
/// that reserves its own pages, and <see cref="ReleaseAsync"/> gives it back
/// to the global usage map instead of leaving it marked used and unreachable.
/// </summary>
/// <param name="allocator">The allocator the runs were reserved from.</param>
internal sealed class ReservedPageRuns(PageAllocator allocator)
{
    private List<(long FirstPage, int PageCount)>? runs;

    /// <summary>Gets a value indicating whether no unlinked run is recorded.</summary>
    internal bool IsEmpty => this.runs is not { Count: > 0 };

    /// <summary>Records a run the caller reserved and wrote but has not linked.</summary>
    /// <param name="firstPage">The first page of the run.</param>
    /// <param name="pageCount">The number of pages in the run.</param>
    internal void Add(long firstPage, int pageCount) => (this.runs ??= []).Add((firstPage, pageCount));

    /// <summary>Reserves <paramref name="pageCount"/> contiguous pages and records the run.</summary>
    /// <param name="pageCount">The number of pages to reserve.</param>
    /// <param name="cancellationToken">A token used to cancel the reservation.</param>
    /// <returns>The first page of the reserved run.</returns>
    internal async ValueTask<long> ReserveAsync(int pageCount, CancellationToken cancellationToken)
    {
        long firstPage = await allocator.ReserveContiguousPagesAsync(pageCount, cancellationToken).ConfigureAwait(false);
        this.Add(firstPage, pageCount);
        return firstPage;
    }

    /// <summary>
    /// Forgets every recorded run. Call it immediately before the first write
    /// that can make any of them reachable: once a run is linked, releasing it
    /// would free pages the file still uses.
    /// </summary>
    internal void MarkLinked() => this.runs?.Clear();

    /// <summary>
    /// Gives every recorded run back to the global usage map and forgets it.
    /// Best effort: a release that fails leaves the rest of the run allocated,
    /// as before this type existed, and never throws.
    /// </summary>
    /// <returns>A task that completes when every run has been released.</returns>
    internal async ValueTask ReleaseAsync()
    {
        if (this.runs is null)
        {
            return;
        }

        foreach ((long firstPage, int pageCount) in this.runs)
        {
            await allocator.ReleaseReservedPagesAsync(firstPage, pageCount).ConfigureAwait(false);
        }

        this.runs.Clear();
    }
}
