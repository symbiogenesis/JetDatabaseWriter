namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>A lifetime owner index kept current from plaintext writes.</summary>
/// <param name="pages">The page source.</param>
/// <param name="format">The format.</param>
internal sealed class OwnedPageIndex(IPageSource pages, JetFormat format) : IPageWriteObserver, IDisposable
{
    private readonly SemaphoreSlim gate = new(1, 1);
#if NET9_0_OR_GREATER
    private readonly Lock sync = new();
#else
    private readonly object sync = new();
#endif
    private Dictionary<long, List<long>> owners = [];
    private bool initialized;
    private long epoch;

    /// <summary>Returns the pages owned by a table.</summary>
    /// <param name="owner">The TDEF page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The ascending page numbers.</returns>
    internal async ValueTask<long[]> GetAsync(long owner, CancellationToken cancellationToken)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            while (true)
            {
                long loadedEpoch;
                lock (this.sync)
                {
                    if (this.initialized)
                    {
                        return this.FindOwnedPages(owner);
                    }

                    loadedEpoch = this.epoch;
                }

                var loaded = new Dictionary<long, List<long>>();
                long totalPages = pages.PageCount;
                for (long number = 3; number < totalPages; number++)
                {
                    byte[] page = await pages.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        long pageOwner = this.ReadOwner(page);
                        if (pageOwner > 0)
                        {
                            if (!loaded.TryGetValue(pageOwner, out List<long>? ownedPages))
                            {
                                ownedPages = [];
                                loaded.Add(pageOwner, ownedPages);
                            }

                            ownedPages.Add(number);
                        }
                    }
                    finally
                    {
                        PageBuffers.Return(page);
                    }
                }

                lock (this.sync)
                {
                    if (loadedEpoch == this.epoch)
                    {
                        this.owners = loaded;
                        this.initialized = true;
                        return this.FindOwnedPages(owner);
                    }
                }
            }
        }
        finally
        {
            _ = this.gate.Release();
        }
    }

    /// <inheritdoc/>
    public void Dispose() => this.gate.Dispose();

    /// <inheritdoc/>
    public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
    {
        long oldOwner = this.ReadOwner(before);
        long newOwner = this.ReadOwner(after);
        if (oldOwner == newOwner)
        {
            return;
        }

        lock (this.sync)
        {
            this.epoch++;
            if (!this.initialized)
            {
                return;
            }

            if (oldOwner > 0 && this.owners.TryGetValue(oldOwner, out List<long>? oldPages))
            {
                int oldIndex = oldPages.BinarySearch(pageNumber);
                if (oldIndex >= 0)
                {
                    oldPages.RemoveAt(oldIndex);
                }
            }

            if (newOwner > 0)
            {
                if (!this.owners.TryGetValue(newOwner, out List<long>? newPages))
                {
                    newPages = [];
                    this.owners.Add(newOwner, newPages);
                }

                int newIndex = newPages.BinarySearch(pageNumber);
                if (newIndex < 0)
                {
                    newPages.Insert(~newIndex, pageNumber);
                }
            }
        }
    }

    /// <inheritdoc/>
    public void OnInvalidateAll()
    {
        lock (this.sync)
        {
            this.owners.Clear();
            this.initialized = false;
            this.epoch++;
        }
    }

    private long[] FindOwnedPages(long owner)
        => this.owners.TryGetValue(owner, out List<long>? ownedPages) ? ownedPages.ToArray() : [];

    private long ReadOwner(ReadOnlySpan<byte> page)
        => !page.IsEmpty && page[0] == Constants.PageTypes.Data ? Ri32(page, format.DataPage.TDefOff) : 0;
}
