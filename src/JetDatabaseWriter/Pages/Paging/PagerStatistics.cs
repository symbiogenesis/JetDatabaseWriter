namespace JetDatabaseWriter.Pages.Paging;

/// <summary>A snapshot of the pager's physical reads, frame cache, active leases, in-flight reads, and cached row directories.</summary>
/// <param name="StoreReads">The number of physical store read attempts.</param>
/// <param name="CacheHits">The number of cached reads.</param>
/// <param name="Evictions">The number of CLOCK replacements.</param>
/// <param name="CachedFrames">The retained frame count.</param>
/// <param name="EvictedPinnedFrames">The number of frames evicted or invalidated from the cache that remain pinned by active leases.</param>
/// <param name="LiveFrames">The total live frame count (retained plus evicted-pinned).</param>
/// <param name="OutstandingLeases">The combined pending and live page leases.</param>
/// <param name="InFlightStoreReads">The number of concurrent store or pending page reads currently in flight.</param>
/// <param name="DirtyBufferedPages">The number of dirty write-scope pages and zero reservations buffered outside a transaction journal.</param>
/// <param name="CachedRowDirectoryEntries">The total row-directory slot entries cached across live frames.</param>
internal readonly record struct PagerStatistics(
    long StoreReads,
    long CacheHits,
    long Evictions,
    int CachedFrames,
    int EvictedPinnedFrames = 0,
    int LiveFrames = 0,
    int OutstandingLeases = 0,
    int InFlightStoreReads = 0,
    int DirtyBufferedPages = 0,
    int CachedRowDirectoryEntries = 0);

/// <summary>A snapshot of the reader page cache's hits, misses, retained/evicted-pinned frames, leases, load gates, and cached row directories.</summary>
/// <param name="Hits">The number of cache hits.</param>
/// <param name="Misses">The number of cache misses.</param>
/// <param name="RetainedFrames">The number of frames currently retained in the cache.</param>
/// <param name="EvictedPinnedFrames">The number of frames evicted from the cache that remain pinned by active leases.</param>
/// <param name="LiveFrames">The total live frame count (retained plus evicted-pinned).</param>
/// <param name="OutstandingLeases">The combined pending and live page leases.</param>
/// <param name="ActiveLoadGates">The number of in-flight coalesced miss load gates.</param>
/// <param name="InFlightStoreReads">The number of concurrent store page reads currently in flight.</param>
/// <param name="CachedRowDirectoryEntries">The total row-directory slot entries cached across live frames.</param>
internal readonly record struct PageCacheStatistics(
    long Hits,
    long Misses,
    int RetainedFrames,
    int EvictedPinnedFrames,
    int LiveFrames,
    int OutstandingLeases,
    int ActiveLoadGates,
    int InFlightStoreReads,
    int CachedRowDirectoryEntries);
