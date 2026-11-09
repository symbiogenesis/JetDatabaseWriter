namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;

/// <summary>Retains immutable page images and their row directories; each read owns a disposable lease.</summary>
internal sealed class ReaderPageCache : IPageFrameOwner, IDisposable
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock sync = new();
#else
    private readonly object sync = new();
#endif
    private readonly IPageSource pages;
    private readonly PageFile? pageFile;
    private readonly LruCache<long, PageFrame>? cache;
    private readonly Dictionary<long, PageLoad> loads = [];
    private readonly int maxActiveLeases;
    private readonly int maxLiveFrames;
    private readonly int maxCachedRowDirectoryEntries;
    private readonly Action releaseLease;
    private int outstandingLeases;
    private int liveFrames;
    private int cachedRowDirectoryEntries;
    private bool disposed;

    /// <summary>Initializes a new instance of the <see cref="ReaderPageCache"/> class.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="pages">The source of owned page copies.</param>
    /// <param name="capacity">The retained image count; zero or negative disables reader retention.</param>
    /// <param name="maxActiveLeases">The positive combined pending and live reader lease allowance.</param>
    /// <param name="maxLiveFrames">The maximum total live page frames (retained plus evicted-pinned), or 0 for automatic sizing.</param>
    /// <param name="maxCachedRowDirectoryEntries">The positive aggregate cached row-directory entry allowance.</param>
    internal ReaderPageCache(
        JetFormat format,
        IPageSource pages,
        int capacity,
        int maxActiveLeases = 4096,
        int maxLiveFrames = 0,
        int maxCachedRowDirectoryEntries = 262_144)
    {
        if (format.PageSize != pages.PageSize)
        {
            throw new ArgumentException("The page source and database format must have the same page size.", nameof(pages));
        }

        Guard.Positive(maxActiveLeases, nameof(maxActiveLeases));
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(maxLiveFrames);
#else
        if (maxLiveFrames < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLiveFrames));
        }
#endif
        Guard.Positive(maxCachedRowDirectoryEntries, nameof(maxCachedRowDirectoryEntries));

        this.pages = pages;
        this.pageFile = pages as PageFile;
        int normalizedCapacity = Math.Max(0, capacity);
        this.maxActiveLeases = maxActiveLeases;
        this.maxLiveFrames = maxLiveFrames > 0 ? maxLiveFrames : checked(normalizedCapacity + maxActiveLeases);
        this.maxCachedRowDirectoryEntries = maxCachedRowDirectoryEntries;
        if (this.pageFile is not null)
        {
            int effectiveCapacity = pages is Pager pager ? pager.CacheCapacity : normalizedCapacity;
            this.pageFile.ConfigureLimits(effectiveCapacity, maxActiveLeases, maxLiveFrames, maxCachedRowDirectoryEntries);
            this.releaseLease = this.pageFile.ReleaseLeaseSlot;
            if (pages is not Pager)
            {
                this.pageFile.TryEvictUnpinnedCachedFrame = this.TryEvictUnpinnedFrame;
            }
        }
        else
        {
            this.releaseLease = () => Interlocked.Decrement(ref this.outstandingLeases);
        }

        if (capacity > 0 && pages is not Pager)
        {
            this.cache = new LruCache<long, PageFrame>(capacity, static frame => frame.Release());
        }
    }

    /// <summary>Gets the cache-hit count.</summary>
    internal long Hits => this.pages is Pager pager ? pager.Statistics.CacheHits : (this.cache?.Hits ?? 0);

    /// <summary>Gets the cache-miss count.</summary>
    internal long Misses => this.pages is Pager pager ? pager.Statistics.StoreReads : (this.cache?.Misses ?? 0);

    /// <summary>Gets the readable page count, including pending appended pages.</summary>
    internal long PageCount => this.pages.PageCount;

    /// <summary>Gets admitted pending acquisitions and reader leases not yet released.</summary>
    internal int OutstandingLeases => this.pageFile is not null ? this.pageFile.OutstandingLeases : Volatile.Read(ref this.outstandingLeases);

    /// <summary>Gets the number of frames currently retained in the cache.</summary>
    internal int RetainedFrames => this.pages is Pager pager ? pager.Statistics.CachedFrames : (this.cache?.Count ?? 0);

    /// <summary>Gets the total live frame count (retained plus evicted-pinned).</summary>
    internal int LiveFrames => this.pageFile is not null ? this.pageFile.LiveFrames : Volatile.Read(ref this.liveFrames);

    /// <summary>Gets the number of frames evicted from the cache that remain pinned by active leases.</summary>
    internal int EvictedPinnedFrames => this.pages is Pager pager
        ? pager.Statistics.EvictedPinnedFrames
        : Math.Max(0, this.LiveFrames - (this.cache?.Count ?? 0));

    /// <summary>Gets the total row-directory slot entries cached across live frames.</summary>
    internal int CachedRowDirectoryEntries => this.pageFile is not null ? this.pageFile.CachedRowDirectoryEntries : Volatile.Read(ref this.cachedRowDirectoryEntries);

    /// <summary>Gets a snapshot of aggregate cache, frame, lease, load-gate, and row-directory accounting.</summary>
    internal PageCacheStatistics Statistics
    {
        get
        {
            if (this.pages is Pager pager)
            {
                PagerStatistics s = pager.Statistics;
                return new(s.CacheHits, s.StoreReads, s.CachedFrames, s.EvictedPinnedFrames, s.LiveFrames, s.OutstandingLeases, 0, s.InFlightStoreReads, s.CachedRowDirectoryEntries);
            }

            lock (this.sync)
            {
                int retained = this.cache?.Count ?? 0;
                int live = this.LiveFrames;
                int inFlight = this.pageFile?.InFlightReads ?? 0;
                return new(
                    this.cache?.Hits ?? 0,
                    this.cache?.Misses ?? 0,
                    retained,
                    Math.Max(0, live - retained),
                    live,
                    this.OutstandingLeases,
                    this.loads.Count,
                    inFlight,
                    this.CachedRowDirectoryEntries);
            }
        }
    }

    /// <summary>Acquires immutable page bytes, valid until the returned lease is disposed.</summary>
    /// <param name="number">The page number.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A reader-owned lease, including when retention is disabled.</returns>
    internal async ValueTask<PageLease> ReadPageAsync(long number, CancellationToken cancellationToken)
    {
        this.pages.ThrowIfDisposedOrCancelled(cancellationToken);
        if (this.pages is Pager pager)
        {
            lock (this.sync)
            {
                this.ThrowIfDisposed();
            }

            return await pager.ReadPageLeaseAsync(number, cancellationToken).ConfigureAwait(false);
        }

        lock (this.sync)
        {
            this.ThrowIfDisposed();
            this.CountLease();
            try
            {
                if (this.cache is not null && this.cache.TryGetValue(number, out PageFrame frame))
                {
                    return frame.Acquire(this.releaseLease);
                }
            }
            catch
            {
                this.ReleaseLeaseSlot();
                throw;
            }
        }

        PageLease? lease = null;
        try
        {
            return lease = await this.ReadMissAsync(number, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (lease is null)
            {
                this.ReleaseLeaseSlot();
            }
        }
    }

    /// <inheritdoc/>
    public bool TryReserveRowDirectoryEntries(int entryCount)
    {
        if (this.pageFile is not null)
        {
            return this.pageFile.TryReserveRowDirectoryEntries(entryCount);
        }

        if (entryCount <= 0)
        {
            return true;
        }

        int current;
        do
        {
            current = Volatile.Read(ref this.cachedRowDirectoryEntries);
            if ((long)current + entryCount > this.maxCachedRowDirectoryEntries)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref this.cachedRowDirectoryEntries, current + entryCount, current) == current)
            {
                return true;
            }
        }
        while (true);
    }

    /// <inheritdoc/>
    public void OnFrameReleased(byte[] buffer, bool bound, int cachedDirectoryEntries)
    {
        if (this.pageFile is not null)
        {
            this.pageFile.OnFrameReleased(buffer, bound, cachedDirectoryEntries);
            return;
        }

        if (cachedDirectoryEntries > 0)
        {
            _ = Interlocked.Add(ref this.cachedRowDirectoryEntries, -cachedDirectoryEntries);
        }

        if (bound)
        {
            _ = Interlocked.Decrement(ref this.liveFrames);
        }

        PageBuffers.Return(buffer);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (this.sync)
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            if (this.pageFile is not null && this.pages is not Pager)
            {
                this.pageFile.TryEvictUnpinnedCachedFrame = null;
            }

            this.cache?.Dispose();
        }
    }

    private async ValueTask<PageLease> ReadMissAsync(long number, CancellationToken cancellationToken)
    {
        PageLoad load;
        lock (this.sync)
        {
            this.ThrowIfDisposed();
            if (!this.loads.TryGetValue(number, out PageLoad? existing))
            {
                load = new PageLoad();
                this.loads.Add(number, load);
            }
            else
            {
                load = existing;
            }

            load.Users++;
        }

        bool entered = false;
        try
        {
            await load.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            entered = true;
            lock (this.sync)
            {
                this.ThrowIfDisposed();
                cancellationToken.ThrowIfCancellationRequested();
                if (load.Frame is { } shared)
                {
                    return shared.Acquire(this.releaseLease);
                }

                if (this.cache is not null && this.cache.TryGetValue(number, out PageFrame cached))
                {
                    return cached.Acquire(this.releaseLease);
                }
            }

            byte[] bytes = await this.pages.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
            var loaded = PageFrame.Own(bytes, this.pages.PageSize, owner: this);
            PageLease? hold = null;
            bool retained = false;
            try
            {
                lock (this.sync)
                {
                    this.ThrowIfDisposed();
                    cancellationToken.ThrowIfCancellationRequested();
                    this.AdmitAndInstallLoadedFrame(number, loaded, ref retained);
                    hold = loaded.Acquire();
                    load.Frame = loaded;
                    load.Hold = hold;
                    hold = null;
                    return loaded.Acquire(this.releaseLease);
                }
            }
            finally
            {
                hold?.Dispose();
                if (!retained)
                {
                    loaded.Release();
                }
            }
        }
        finally
        {
            if (entered)
            {
                load.Gate.Release();
            }

            lock (this.sync)
            {
                load.Users--;
                if (load.Users == 0)
                {
                    this.loads.Remove(number);
                    load.Dispose();
                }
            }
        }
    }

    private void AdmitAndInstallLoadedFrame(long number, PageFrame loaded, ref bool retained)
    {
        if (this.cache is null)
        {
            this.ReserveLiveFrameOrThrow();
            loaded.MarkBoundToOwner();
            return;
        }

        if (this.LiveFrames >= this.maxLiveFrames && !this.HasUnpinnedFrame())
        {
            throw new JetLimitationException(JetErrorCode.PageLeaseBudgetExceeded, $"The reader permits at most {this.maxLiveFrames} live page frames.");
        }

        this.IncrementLiveFrames();
        loaded.MarkBoundToOwner();
        if (this.LiveFrames > this.maxLiveFrames && !this.cache.ContainsKey(number))
        {
            _ = this.cache.TryEvictOne(static frame => frame.IsExclusivelyOwned);
        }

        this.cache.Add(number, loaded);
        retained = true;
        while (this.LiveFrames > this.maxLiveFrames)
        {
            if (!this.cache.TryEvictOne(frame => !ReferenceEquals(frame, loaded) && frame.IsExclusivelyOwned))
            {
                break;
            }
        }
    }

    private bool HasUnpinnedFrame()
        => this.cache?.HasMatch(static frame => frame.IsExclusivelyOwned) == true;

    private bool TryEvictUnpinnedFrame()
    {
        lock (this.sync)
        {
            if (this.disposed || this.cache is null)
            {
                return false;
            }

            return this.cache.TryEvictOne(static frame => frame.IsExclusivelyOwned);
        }
    }

    private void CountLease()
    {
        if (this.pageFile is not null)
        {
            this.pageFile.CountLease();
            return;
        }

        int current;
        do
        {
            current = Volatile.Read(ref this.outstandingLeases);
            DataPageKernel.TryReserveCount(checked((uint)current), checked((uint)this.maxActiveLeases), out bool admitted, out uint next);
            if (!admitted)
            {
                throw new JetLimitationException(JetErrorCode.PageLeaseBudgetExceeded, $"The reader permits at most {this.maxActiveLeases} pending and live page leases.");
            }

            if (Interlocked.CompareExchange(ref this.outstandingLeases, checked((int)next), current) == current)
            {
                return;
            }
        }
        while (true);
    }

    private void ReleaseLeaseSlot()
    {
        if (this.pageFile is not null)
        {
            this.pageFile.ReleaseLeaseSlot();
            return;
        }

        _ = Interlocked.Decrement(ref this.outstandingLeases);
    }

    private void ReserveLiveFrameOrThrow()
    {
        if (this.pageFile is not null)
        {
            this.pageFile.ReserveLiveFrameOrThrow();
            return;
        }

        int current;
        do
        {
            current = Volatile.Read(ref this.liveFrames);
            DataPageKernel.TryReserveCount(checked((uint)current), checked((uint)this.maxLiveFrames), out bool admitted, out uint next);
            if (!admitted)
            {
                throw new JetLimitationException(JetErrorCode.PageLeaseBudgetExceeded, $"The reader permits at most {this.maxLiveFrames} live page frames.");
            }

            if (Interlocked.CompareExchange(ref this.liveFrames, checked((int)next), current) == current)
            {
                return;
            }
        }
        while (true);
    }

    private void IncrementLiveFrames()
    {
        if (this.pageFile is not null)
        {
            _ = this.pageFile.IncrementLiveFrames();
            return;
        }

        _ = Interlocked.Increment(ref this.liveFrames);
    }

    private void ThrowIfDisposed()
#if NET8_0_OR_GREATER
        => ObjectDisposedException.ThrowIf(this.disposed, this);
#else
    {
        if (this.disposed)
        {
            throw new ObjectDisposedException(nameof(ReaderPageCache));
        }
    }
#endif

    private sealed class PageLoad : IDisposable
    {
        internal SemaphoreSlim Gate { get; } = new(1, 1);

        internal int Users { get; set; }

        internal PageFrame? Frame { get; set; }

        internal PageLease? Hold { get; set; }

        public void Dispose()
        {
            this.Hold?.Dispose();
            this.Gate.Dispose();
        }
    }
}
