namespace JetDatabaseWriter.Pages;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>
/// The reader's page cache: an LRU of decrypted page buffers plus a parallel
/// LRU of each data page's parsed row directory. The cache is absent
/// altogether when its capacity is zero or negative, which it must be over
/// the writer's file: those pages change, and every read must see the active
/// transaction's journal, so the constructor refuses a positive capacity over
/// a <see cref="Pager"/>.
/// </summary>
internal sealed class ReaderPageCache : IDisposable
{
    private readonly DatabaseFile db;
    private readonly LruCache<long, byte[]>? pageCache;

    /// <summary>
    /// Memoizes the parsed row directory per data page. Same eviction
    /// profile as <see cref="pageCache"/> (sized 1:1 with it) so a page that's
    /// still hot in the byte-cache also keeps its bounds array. Stale entries
    /// left behind after a page is evicted from the byte-cache simply age out of
    /// this LRU on their own — correctness doesn't depend on the two caches being
    /// kept in lock-step.
    /// </summary>
    private readonly LruCache<long, RowBound[]>? rowBoundsCache;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReaderPageCache"/> class.
    /// </summary>
    /// <param name="db">The database file pages are read from.</param>
    /// <param name="capacity">The number of pages to keep; zero or negative disables caching.</param>
    /// <exception cref="ArgumentException"><paramref name="capacity"/> is positive and <paramref name="db"/> is the writer's file.</exception>
    internal ReaderPageCache(DatabaseFile db, int capacity)
    {
        if (capacity > 0 && db.Pages is Pager)
        {
            throw new ArgumentException(
                "A page cache over the writer's file must have capacity 0: its pages change, and every read must see the active transaction's journal.",
                nameof(capacity));
        }

        this.db = db;
        if (capacity > 0)
        {
            // No eviction callback: callers keep using a cached buffer after it
            // is evicted (a scan holds its data page while decoding a long-value
            // chain that cycles the whole cache), so returning it to the shared
            // pool would let the next rent overwrite it mid-scan. Evicted
            // buffers are left to the GC instead.
            this.pageCache = new LruCache<long, byte[]>(capacity);
            this.rowBoundsCache = new LruCache<long, RowBound[]>(capacity);
        }
    }

    /// <summary>Gets the number of page reads served from the cache.</summary>
    internal long Hits => this.pageCache?.Hits ?? 0;

    /// <summary>Gets the number of page reads that went to the file.</summary>
    internal long Misses => this.pageCache?.Misses ?? 0;

    /// <summary>
    /// Reads a page through the cache when one is configured. Cached buffers
    /// are owned by the cache: callers must not return them to the pool.
    /// </summary>
    /// <param name="n">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<byte[]> ReadPageAsync(long n, CancellationToken cancellationToken)
    {
        this.db.ThrowIfDisposedOrCancelled(cancellationToken);

        if (this.pageCache is null)
        {
            return await this.db.ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        }

        if (this.pageCache.TryGetValue(n, out byte[] cached))
        {
            return cached;
        }

        byte[] page = await this.db.ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        this.pageCache.Add(n, page);
        return page;
    }

    /// <summary>Returns a page that is already cached without touching the file.</summary>
    /// <param name="n">The page number.</param>
    /// <param name="page">Receives the cached page bytes, or an empty array.</param>
    internal bool TryGetCachedPage(long n, out byte[] page)
    {
        if (this.pageCache is not null && this.pageCache.TryGetValue(n, out page))
        {
            return true;
        }

        page = [];
        return false;
    }

    /// <summary>
    /// Returns the row directory for <paramref name="page"/> (see
    /// <see cref="DatabaseFile.ComputeRowDirectory"/>: live rows plus overflow headers
    /// flagged <see cref="RowBound.IsOverflowPointer"/>), computing it on first request
    /// and caching the result keyed by <paramref name="pageNumber"/> when a page cache
    /// is configured. The returned array is owned by the cache — callers must not
    /// mutate it. Used by the typed/untyped scan paths to avoid re-parsing the
    /// row-offset trailer on repeated scans of the same table.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    internal RowBound[] GetRowDirectory(long pageNumber, byte[] page)
    {
        if (this.rowBoundsCache is not null && this.rowBoundsCache.TryGetValue(pageNumber, out RowBound[]? cached))
        {
            return cached;
        }

        RowBound[] bounds = this.db.ComputeRowDirectory(page);
        this.rowBoundsCache?.Add(pageNumber, bounds);
        return bounds;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        this.pageCache?.Clear();
        this.pageCache?.Dispose();
        this.rowBoundsCache?.Clear();
        this.rowBoundsCache?.Dispose();
    }
}
