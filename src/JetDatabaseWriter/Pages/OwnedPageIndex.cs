namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
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
    private Dictionary<long, SortedDictionary<long, byte>> unreadableOwners = [];
    private bool initialized;
    private long epoch;

    /// <summary>Returns the pages owned by a table.</summary>
    /// <param name="owner">The TDEF page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The ascending page numbers.</returns>
    internal async ValueTask<long[]> GetAsync(long owner, CancellationToken cancellationToken)
        => (await this.GetDiscoveryAsync(owner, cancellationToken).ConfigureAwait(false)).Pages;

    /// <summary>Returns readable data pages and retained unknown-type ownership evidence.</summary>
    /// <param name="owner">The TDEF page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The readable pages and first unreadable physical candidate, if any.</returns>
    /// <exception cref="JetCorruptDataException">A physical page is shorter than the source format's page size.</exception>
    internal async ValueTask<Discovery> GetDiscoveryAsync(long owner, CancellationToken cancellationToken)
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
                        return this.FindDiscovery(owner);
                    }

                    loadedEpoch = this.epoch;
                }

                var loaded = new Dictionary<long, List<long>>();
                var loadedUnreadable = new Dictionary<long, SortedDictionary<long, byte>>();
                long totalPages = pages.PageCount;
                for (long number = 3; number < totalPages; number++)
                {
                    byte[] page = await pages.ReadUncachedPageAsync(number, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (page.Length < format.PageSize)
                        {
                            throw new JetCorruptDataException(JetErrorCode.MalformedValue, "The physical owner scan received a truncated page.", new JetErrorInfo { PageNumber = number });
                        }

                        AddUnreadable(loadedUnreadable, this.ReadUnreadableOwner(page), number, page[0]);
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
                        this.unreadableOwners = loadedUnreadable;
                        this.initialized = true;
                        return this.FindDiscovery(owner);
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
        long oldUnreadableOwner = this.ReadUnreadableOwner(before);
        long newUnreadableOwner = this.ReadUnreadableOwner(after);
        if (oldOwner == newOwner && oldUnreadableOwner == newUnreadableOwner
            && (newUnreadableOwner == 0 || before[0] == after[0]))
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

            if (oldUnreadableOwner > 0 && this.unreadableOwners.TryGetValue(oldUnreadableOwner, out SortedDictionary<long, byte>? oldUnreadable))
            {
                oldUnreadable.Remove(pageNumber);
                if (oldUnreadable.Count == 0)
                {
                    this.unreadableOwners.Remove(oldUnreadableOwner);
                }
            }

            AddUnreadable(this.unreadableOwners, newUnreadableOwner, pageNumber, after[0]);
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
            this.unreadableOwners.Clear();
            this.initialized = false;
            this.epoch++;
        }
    }

    private static void AddUnreadable(Dictionary<long, SortedDictionary<long, byte>> owners, long owner, long page, byte pageType)
    {
        if (owner <= 0)
        {
            return;
        }

        if (!owners.TryGetValue(owner, out SortedDictionary<long, byte>? ownedPages))
        {
            ownedPages = [];
            owners.Add(owner, ownedPages);
        }

        ownedPages[page] = pageType;
    }

    private Discovery FindDiscovery(long owner)
    {
        if (this.unreadableOwners.TryGetValue(owner, out SortedDictionary<long, byte>? unreadable))
        {
            foreach (KeyValuePair<long, byte> entry in unreadable)
            {
                return new Discovery(this.FindOwnedPages(owner), entry.Key, entry.Value);
            }
        }

        return new Discovery(this.FindOwnedPages(owner), null, 0);
    }

    private long ReadUnreadableOwner(ReadOnlySpan<byte> page)
        => page.Length >= format.DataPage.TDefOff + 4
            && page[0] is not (0 or Constants.PageTypes.Data or Constants.PageTypes.TableDefinition or Constants.PageTypes.IndexIntermediate
                or Constants.PageTypes.IndexLeaf or Constants.PageTypes.UsageMap or Constants.PageTypes.Freed)
            ? Ri32(page, format.DataPage.TDefOff) : 0;

    private long[] FindOwnedPages(long owner)
        => this.owners.TryGetValue(owner, out List<long>? ownedPages) ? ownedPages.ToArray() : [];

    private long ReadOwner(ReadOnlySpan<byte> page)
        => !page.IsEmpty && page[0] == Constants.PageTypes.Data ? Ri32(page, format.DataPage.TDefOff) : 0;

    /// <summary>Readable data pages and the first physical page with unknown type and matching ownership.</summary>
    /// <param name="Pages">The ascending data page numbers.</param>
    /// <param name="UnreadablePage">The first unknown-type physical page, when present.</param>
    /// <param name="UnreadablePageType">The unknown page type.</param>
    internal readonly record struct Discovery(long[] Pages, long? UnreadablePage, byte UnreadablePageType);
}
