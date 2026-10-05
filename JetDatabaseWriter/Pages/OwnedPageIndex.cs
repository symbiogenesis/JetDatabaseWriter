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
    private Dictionary<long, long> owners = [];
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

                var loaded = new Dictionary<long, long>();
                for (long number = 3; number < pages.PageCount; number++)
                {
                    byte[] page = await pages.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        this.SetOwner(loaded, number, page);
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
        lock (this.sync)
        {
            this.SetOwner(this.owners, pageNumber, after);
            this.epoch++;
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
    {
        var result = new List<long>();
        foreach (KeyValuePair<long, long> pair in this.owners)
        {
            if (pair.Value == owner)
            {
                result.Add(pair.Key);
            }
        }

        result.Sort();
        return result.ToArray();
    }

    private void SetOwner(Dictionary<long, long> target, long pageNumber, ReadOnlySpan<byte> page)
    {
        _ = target.Remove(pageNumber);
        if (page[0] == Constants.PageTypes.Data)
        {
            long owner = Ri32(page, format.DataPage.TDefOff);
            if (owner > 0)
            {
                target[pageNumber] = owner;
            }
        }
    }
}
