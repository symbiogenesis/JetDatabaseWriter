namespace JetDatabaseWriter.Pages.Paging;

/// <summary>A snapshot of the pager's physical reads and frame cache.</summary>
/// <param name="StoreReads">The number of physical store read attempts.</param>
/// <param name="CacheHits">The number of cached reads.</param>
/// <param name="Evictions">The number of CLOCK replacements.</param>
/// <param name="CachedFrames">The retained frame count.</param>
internal readonly record struct PagerStatistics(long StoreReads, long CacheHits, long Evictions, int CachedFrames);