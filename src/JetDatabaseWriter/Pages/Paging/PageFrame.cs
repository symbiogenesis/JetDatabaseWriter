namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Threading;
using JetDatabaseWriter.Pages.Models;

/// <summary>An immutable page image retained by its cache and each active reader.</summary>
/// <param name="bytes">The owned pooled buffer, which must never be mutated after publication.</param>
/// <param name="pageSize">The readable page size.</param>
/// <param name="returnBuffer">Returns the buffer after its final reference is released.</param>
internal sealed class PageFrame(byte[] bytes, int pageSize, Action<byte[]>? returnBuffer = null)
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock directorySync = new();
#else
    private readonly object directorySync = new();
#endif
    private RowBound[]? rowDirectory;
    private int references = 1;

    /// <summary>Gets the page image for the array-based read-only parsers.</summary>
    internal byte[] Buffer => bytes;

    /// <summary>Gets the bounded immutable page image.</summary>
    internal ReadOnlyMemory<byte> Memory => bytes.AsMemory(0, pageSize);

    /// <summary>Gets or sets a value indicating whether the cache observed this frame under its lock.</summary>
    internal bool Referenced { get; set; } = true;

    /// <summary>Takes ownership of a pooled image, returning its buffer if construction fails.</summary>
    /// <param name="buffer">The owned pooled image.</param>
    /// <param name="size">The page size.</param>
    /// <param name="release">The buffer return operation, or the shared pool.</param>
    /// <returns>The producer's frame reference.</returns>
    internal static PageFrame Own(byte[] buffer, int size, Action<byte[]>? release = null)
    {
        try
        {
            return new PageFrame(buffer, size, release);
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
    /// <returns>The producer's frame reference.</returns>
    internal static PageFrame Copy(byte[] source, int size)
    {
        ReadOnlySpan<byte> image = source.AsSpan(0, size);
        byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(size);
        image.CopyTo(buffer);
        return Own(buffer, size);
    }

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
            (returnBuffer ?? PageBuffers.Return)(bytes);
        }
    }

    /// <summary>Parses the row directory once for this exact immutable page image.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The immutable directory retained with the image.</returns>
    internal RowBound[] GetRowDirectory(JetFormat format)
    {
        lock (this.directorySync)
        {
            return this.rowDirectory ??= DataPageRows.ComputeRowDirectory(format, bytes);
        }
    }
}
