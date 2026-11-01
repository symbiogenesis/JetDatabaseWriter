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
internal sealed class ReaderPageCache : IDisposable
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock sync = new();
#else
    private readonly object sync = new();
#endif
    private readonly IPageSource pages;
    private readonly LruCache<long, PageFrame>? cache;
    private readonly Dictionary<long, PageLoad> loads = [];
    private readonly int maxActiveLeases;
    private readonly Action releaseLease;
    private int outstandingLeases;
    private bool disposed;

    /// <summary>Initializes a new instance of the <see cref="ReaderPageCache"/> class.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="pages">The source of owned page copies.</param>
    /// <param name="capacity">The retained image count; zero or negative disables reader retention.</param>
    /// <param name="maxActiveLeases">The positive combined pending and live reader lease allowance.</param>
    internal ReaderPageCache(JetFormat format, IPageSource pages, int capacity, int maxActiveLeases = 4096)
    {
        if (format.PageSize != pages.PageSize)
        {
            throw new ArgumentException("The page source and database format must have the same page size.", nameof(pages));
        }

        Guard.Positive(maxActiveLeases, nameof(maxActiveLeases));

        this.pages = pages;
        this.maxActiveLeases = maxActiveLeases;
        this.releaseLease = () => Interlocked.Decrement(ref this.outstandingLeases);
        if (capacity > 0 && pages is not Pager)
        {
            this.cache = new LruCache<long, PageFrame>(capacity, static frame => frame.Release());
        }
    }

    /// <summary>Gets the cache-hit count.</summary>
    internal long Hits => this.cache?.Hits ?? 0;

    /// <summary>Gets the cache-miss count.</summary>
    internal long Misses => this.cache?.Misses ?? 0;

    /// <summary>Gets the readable page count, including pending appended pages.</summary>
    internal long PageCount => this.pages.PageCount;

    /// <summary>Gets admitted pending acquisitions and reader leases not yet released.</summary>
    internal int OutstandingLeases => Volatile.Read(ref this.outstandingLeases);

    /// <summary>Acquires immutable page bytes, valid until the returned lease is disposed.</summary>
    /// <param name="number">The page number.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A reader-owned lease, including when retention is disabled.</returns>
    internal async ValueTask<PageLease> ReadPageAsync(long number, CancellationToken cancellationToken)
    {
        this.pages.ThrowIfDisposedOrCancelled(cancellationToken);
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
                Interlocked.Decrement(ref this.outstandingLeases);
                throw;
            }
        }

        PageLease? lease = null;
        try
        {
            if (this.pages is Pager pager)
            {
                return lease = await pager.ReadPageLeaseAsync(number, this.releaseLease, cancellationToken).ConfigureAwait(false);
            }

            return lease = await this.ReadMissAsync(number, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (lease is null)
            {
                Interlocked.Decrement(ref this.outstandingLeases);
            }
        }
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
            var loaded = PageFrame.Own(bytes, this.pages.PageSize);
            PageLease? hold = null;
            bool retained = false;
            try
            {
                hold = loaded.Acquire();
                lock (this.sync)
                {
                    this.ThrowIfDisposed();
                    cancellationToken.ThrowIfCancellationRequested();
                    if (this.cache is not null)
                    {
                        this.cache.Add(number, loaded);
                        retained = true;
                    }

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

    private void CountLease()
    {
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
