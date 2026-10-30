namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Threading;
using JetDatabaseWriter.Pages.Models;

/// <summary>A read-only page reference whose lifetime is independent of cache eviction or invalidation.</summary>
/// <param name="frame">The pinned immutable frame.</param>
/// <param name="released">Optional reader accounting callback.</param>
internal sealed class PageLease(PageFrame frame, Action? released = null) : IDisposable
{
    private PageFrame? frame = frame;

    /// <summary>Gets the bounded read-only bytes until this lease is disposed.</summary>
    internal ReadOnlyMemory<byte> Memory => this.Frame.Memory;

    /// <summary>Gets the image for existing array-based parsers; callers must only read it within this lease.</summary>
    internal byte[] Buffer => this.Frame.Buffer;

    private PageFrame Frame => this.frame ?? throw new ObjectDisposedException(nameof(PageLease));

    /// <summary>Transfers an owned buffer into a lease without retaining a cache reference.</summary>
    /// <param name="buffer">The owned buffer.</param>
    /// <param name="pageSize">The readable page size.</param>
    /// <param name="returnBuffer">The buffer's release operation, or the shared pool.</param>
    /// <returns>The sole owner of the supplied buffer.</returns>
    internal static PageLease Own(byte[] buffer, int pageSize, Action<byte[]>? returnBuffer = null)
    {
        var owned = PageFrame.Own(buffer, pageSize, returnBuffer);
        try
        {
            return new PageLease(owned);
        }
        catch
        {
            owned.Release();
            throw;
        }
    }

    /// <summary>Gets the immutable row directory belonging to this image.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The row directory.</returns>
    internal RowBound[] GetRowDirectory(JetFormat format) => this.Frame.GetRowDirectory(format);

    /// <summary>Releases this reader reference exactly once.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref this.frame, null) is { } owned)
        {
            owned.Release();
            released?.Invoke();
        }
    }
}
