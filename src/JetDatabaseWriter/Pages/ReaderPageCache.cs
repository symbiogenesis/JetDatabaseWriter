namespace JetDatabaseWriter.Pages;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;

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
    private readonly Action releaseLease;
    private int outstandingLeases;
    private bool disposed;

    /// <summary>Initializes a new instance of the <see cref="ReaderPageCache"/> class.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="pages">The source of owned page copies.</param>
    /// <param name="capacity">The retained image count; zero or negative disables reader retention.</param>
    internal ReaderPageCache(JetFormat format, IPageSource pages, int capacity)
    {
        if (format.PageSize != pages.PageSize)
        {
            throw new ArgumentException("The page source and database format must have the same page size.", nameof(pages));
        }

        this.pages = pages;
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

    /// <summary>Gets the number of reader leases not yet released.</summary>
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
            if (this.cache is not null && this.cache.TryGetValue(number, out PageFrame frame))
            {
                return this.Acquire(frame);
            }
        }

        if (this.pages is Pager pager)
        {
            this.CountLease();
            try
            {
                return await pager.ReadPageLeaseAsync(number, this.releaseLease, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                Interlocked.Decrement(ref this.outstandingLeases);
                throw;
            }
        }

        byte[] bytes = await this.pages.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
        var loaded = PageFrame.Own(bytes, this.pages.PageSize);
        bool retained = false;
        try
        {
            lock (this.sync)
            {
                this.ThrowIfDisposed();
                if (this.cache is not null)
                {
                    this.cache.Add(number, loaded);
                    retained = true;
                }

                return this.Acquire(loaded);
            }
        }
        finally
        {
            if (!retained)
            {
                loaded.Release();
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

    private PageLease Acquire(PageFrame frame)
    {
        this.CountLease();
        try
        {
            return frame.Acquire(this.releaseLease);
        }
        catch
        {
            Interlocked.Decrement(ref this.outstandingLeases);
            throw;
        }
    }

    private void CountLease()
    {
        int current;
        do
        {
            current = Volatile.Read(ref this.outstandingLeases);
        }
        while (Interlocked.CompareExchange(ref this.outstandingLeases, checked(current + 1), current) != current);
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
}
