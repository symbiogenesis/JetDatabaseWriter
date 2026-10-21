namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;

/// <summary>Caches immutable structural images and observes every page of each chain.</summary>
/// <param name="format">The table header layout.</param>
internal sealed class TDefImageCache(JetFormat format) : IPageWriteObserver
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock cacheLock = new();
#else
    private readonly object cacheLock = new();
#endif
    private readonly Dictionary<long, CacheEntry> entries = [];
    private long epoch;

    /// <summary>Gets the publication epoch, advanced by every write or global invalidation.</summary>
    internal long Epoch
    {
        get
        {
            lock (this.cacheLock)
            {
                return this.epoch;
            }
        }
    }

    /// <summary>Finds an owned immutable image.</summary>
    /// <param name="root">The root page.</param>
    /// <param name="image">The cached image.</param>
    /// <returns>Whether the image is cached.</returns>
    internal bool TryGet(long root, [NotNullWhen(true)] out TDefImage? image)
    {
        lock (this.cacheLock)
        {
            image = this.entries.TryGetValue(root, out CacheEntry? entry) ? entry.Image : null;
            return image is not null;
        }
    }

    /// <summary>Publishes only when no write raced with the chain read.</summary>
    /// <param name="root">The root page.</param>
    /// <param name="image">The parsed image.</param>
    /// <param name="pages">Every physical page read.</param>
    /// <param name="readEpoch">The epoch captured before reading.</param>
    /// <returns>Whether publication succeeded.</returns>
    internal bool Publish(long root, TDefImage image, IReadOnlyList<long> pages, long readEpoch)
    {
        var ownedPages = new HashSet<long>(pages);
        lock (this.cacheLock)
        {
            if (this.epoch != readEpoch)
            {
                return false;
            }

            this.entries[root] = new CacheEntry(image, ownedPages);
            return true;
        }
    }

    /// <inheritdoc/>
    public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
    {
        lock (this.cacheLock)
        {
            unchecked
            {
                this.epoch++;
            }

            List<long>? removed = null;
            foreach (KeyValuePair<long, CacheEntry> entry in this.entries)
            {
                if (entry.Value.Pages.Contains(pageNumber)
                    && (entry.Key != pageNumber || !this.OnlyCountersChanged(before, after)))
                {
                    (removed ??= []).Add(entry.Key);
                }
            }

            if (removed is not null)
            {
                foreach (long root in removed)
                {
                    this.entries.Remove(root);
                }
            }
        }
    }

    /// <inheritdoc/>
    public void OnInvalidateAll()
    {
        lock (this.cacheLock)
        {
            unchecked
            {
                this.epoch++;
            }

            this.entries.Clear();
        }
    }

    /// <summary>Compares all structural bytes while excluding the three live counter fields.</summary>
    /// <param name="before">The previous bytes.</param>
    /// <param name="after">The current bytes.</param>
    /// <returns>Whether the structure is identical.</returns>
    internal bool StructuralBytesEqual(ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
    {
        TDefHeaderLayout header = format.TDefFormat.Header;
        if (before.Length != after.Length || before.Length < header.BlockEnd)
        {
            return false;
        }

        int rowsEnd = header.NumRows + sizeof(uint);
        int autoEnd = header.AutoNumber + sizeof(uint);
        if (!before[..header.NumRows].SequenceEqual(after[..header.NumRows])
            || !before[rowsEnd..header.AutoNumber].SequenceEqual(after[rowsEnd..header.AutoNumber]))
        {
            return false;
        }

        if (header.ComplexAutoNumber >= 0)
        {
            if (!before[autoEnd..header.ComplexAutoNumber].SequenceEqual(after[autoEnd..header.ComplexAutoNumber]))
            {
                return false;
            }

            autoEnd = header.ComplexAutoNumber + sizeof(uint);
        }

        return before[autoEnd..].SequenceEqual(after[autoEnd..]);
    }

    private bool OnlyCountersChanged(ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
        => before.Length == format.PageSize && this.StructuralBytesEqual(before, after);

    private sealed record CacheEntry(TDefImage Image, HashSet<long> Pages);
}
