namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Threading;
using JetDatabaseWriter.Pages.Models;

/// <summary>Tracks aggregate live-frame and cached row-directory budgets for a <see cref="PageFrame"/> owner.</summary>
internal interface IPageFrameOwner
{
    /// <summary>Attempts to reserve cached row-directory entries for a live frame.</summary>
    /// <param name="entryCount">The number of row-directory entries.</param>
    /// <returns><see langword="true"/> when the entries may be retained on the frame; otherwise, <see langword="false"/>.</returns>
    public bool TryReserveRowDirectoryEntries(int entryCount);

    /// <summary>Releases a frame's pooled buffer and any reserved frame and row-directory counts when its final reference drops.</summary>
    /// <param name="buffer">The pooled page buffer.</param>
    /// <param name="bound">Whether the frame was bound to the owner's live-frame count.</param>
    /// <param name="cachedDirectoryEntries">The number of cached row-directory entries retained on the frame.</param>
    public void OnFrameReleased(byte[] buffer, bool bound, int cachedDirectoryEntries);
}

/// <summary>An immutable page image retained by its cache and each active reader.</summary>
/// <param name="bytes">The owned pooled buffer, which must never be mutated after publication.</param>
/// <param name="pageSize">The readable page size.</param>
/// <param name="returnBuffer">Returns the buffer after its final reference is released.</param>
/// <param name="owner">Optional aggregate frame and row-directory accounting owner.</param>
internal sealed class PageFrame(byte[] bytes, int pageSize, Action<byte[]>? returnBuffer = null, IPageFrameOwner? owner = null)
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock directorySync = new();
#else
    private readonly object directorySync = new();
#endif
    private RowBound[]? rowDirectory;
    private int cachedDirectoryEntries;
    private bool boundToOwner;
    private int references = 1;

    /// <summary>Gets the page image for the array-based read-only parsers.</summary>
    internal byte[] Buffer => bytes;

    /// <summary>Gets the bounded immutable page image.</summary>
    internal ReadOnlyMemory<byte> Memory => bytes.AsMemory(0, pageSize);

    /// <summary>Gets or sets a value indicating whether the cache observed this frame under its lock.</summary>
    internal bool Referenced { get; set; } = true;

    /// <summary>Gets a value indicating whether the cache (or sole producer) holds the only reference to this frame.</summary>
    internal bool IsExclusivelyOwned => Volatile.Read(ref this.references) == 1;

    /// <summary>Gets the current reference count for diagnostics and tests.</summary>
    internal int ReferenceCount => Volatile.Read(ref this.references);

    /// <summary>Takes ownership of a pooled image, returning its buffer if construction fails.</summary>
    /// <param name="buffer">The owned pooled image.</param>
    /// <param name="size">The page size.</param>
    /// <param name="release">The buffer return operation, or the shared pool.</param>
    /// <param name="owner">Optional aggregate frame and directory accounting owner.</param>
    /// <returns>The producer's frame reference.</returns>
    internal static PageFrame Own(byte[] buffer, int size, Action<byte[]>? release = null, IPageFrameOwner? owner = null)
    {
        try
        {
            return new PageFrame(buffer, size, release, owner);
        }
        catch
        {
            (release ?? PageBuffers.Return)(buffer);
            throw;
        }
    }

    /// <summary>Copies a mutable page into a separately owned immutable frame.</summary>
    /// <param name="source">The mutable image.</param>
    /// <param name="size">The page size.</param>
    /// <param name="owner">Optional aggregate frame and directory accounting owner.</param>
    /// <returns>The producer's frame reference.</returns>
    internal static PageFrame Copy(byte[] source, int size, IPageFrameOwner? owner = null)
    {
        ReadOnlySpan<byte> image = source.AsSpan(0, size);
        byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(size);
        image.CopyTo(buffer);
        return Own(buffer, size, owner: owner);
    }

    /// <summary>Marks this frame as admitted into its owner's live-frame count.</summary>
    internal void MarkBoundToOwner() => this.boundToOwner = true;

    /// <summary>Acquires a reader reference while the caller still owns a cache or reader reference.</summary>
    /// <param name="released">Optional reader accounting callback.</param>
    /// <returns>The independently disposable reader reference.</returns>
    /// <exception cref="ObjectDisposedException">The final owner has already released the frame.</exception>
    internal PageLease Acquire(Action? released = null)
    {
        int current;
        do
        {
            current = Volatile.Read(ref this.references);
#if NET8_0_OR_GREATER
            ObjectDisposedException.ThrowIf(current <= 0, this);
#else
            if (current <= 0)
            {
                throw new ObjectDisposedException(nameof(PageFrame));
            }
#endif
        }
        while (Interlocked.CompareExchange(ref this.references, checked(current + 1), current) != current);

        try
        {
            return new PageLease(this, released);
        }
        catch
        {
            this.Release();
            throw;
        }
    }

    /// <summary>Releases one cache or reader reference.</summary>
    internal void Release()
    {
        if (Interlocked.Decrement(ref this.references) == 0)
        {
            if (owner is not null)
            {
                owner.OnFrameReleased(bytes, this.boundToOwner, this.cachedDirectoryEntries);
            }
            else
            {
                (returnBuffer ?? PageBuffers.Return)(bytes);
            }
        }
    }

    /// <summary>Parses the row directory once for this exact immutable page image when within the owner's directory budget.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The immutable directory retained with the image.</returns>
    internal RowBound[] GetRowDirectory(JetFormat format)
    {
        lock (this.directorySync)
        {
            if (this.rowDirectory is { } existing)
            {
                return existing;
            }

            RowBound[] computed = DataPageRows.ComputeRowDirectory(format, bytes);
            if (owner?.TryReserveRowDirectoryEntries(computed.Length) != false)
            {
                this.rowDirectory = computed;
                this.cachedDirectoryEntries = computed.Length;
            }

            return computed;
        }
    }
}
