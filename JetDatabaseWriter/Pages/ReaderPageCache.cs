namespace JetDatabaseWriter.Pages;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// The reader's page cache: an LRU of decrypted page buffers plus a parallel
/// LRU of each data page's parsed live-row directory. Every cached read falls
/// through to the <see cref="DatabaseFile"/> while a transaction journal is
/// attached, and the cache is absent altogether when its capacity is zero or
/// negative, as it is for the writer's reads of its own rows.
/// </summary>
internal sealed class ReaderPageCache : IDisposable
{
    private readonly DatabaseFile db;
    private readonly LruCache<long, byte[]>? pageCache;

    /// <summary>
    /// Memoizes the parsed live-row directory per data page. Same eviction
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
    internal ReaderPageCache(DatabaseFile db, int capacity)
    {
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

    /// <summary>Gets a value indicating whether pages are cached at all.</summary>
    internal bool IsEnabled => this.pageCache is not null;

    /// <summary>Gets the number of page reads served from the cache.</summary>
    internal long Hits => this.pageCache?.Hits ?? 0;

    /// <summary>Gets the number of page reads that went to the file.</summary>
    internal long Misses => this.pageCache?.Misses ?? 0;

    /// <summary>
    /// Reads a page through the cache when one is configured and no transaction
    /// journal is active. Cached buffers are owned by the cache: callers must not
    /// return them to the pool.
    /// </summary>
    /// <param name="n">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<byte[]> ReadPageAsync(long n, CancellationToken cancellationToken)
    {
        this.db.ThrowIfDisposedOrCancelled(cancellationToken);

        if (this.db.ActiveJournal is not null)
        {
            return await this.db.ReadPageAsync(n, cancellationToken).ConfigureAwait(false);
        }

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
    /// Returns the live row-bound directory for <paramref name="page"/>, computing
    /// it on first request and caching the result keyed by <paramref name="pageNumber"/>
    /// when a page cache is configured. The returned array is owned by the cache —
    /// callers must not mutate it. Used by the typed/untyped scan paths to avoid
    /// re-parsing the row-offset trailer on repeated scans of the same table.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    internal RowBound[] GetLiveRowBounds(long pageNumber, byte[] page)
    {
        if (this.db.ActiveJournal is not null)
        {
            return this.db.ComputeLiveRowBoundsArray(page);
        }

        if (this.rowBoundsCache is not null && this.rowBoundsCache.TryGetValue(pageNumber, out RowBound[]? cached))
        {
            return cached;
        }

        RowBound[] bounds = this.db.ComputeLiveRowBoundsArray(page);
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
