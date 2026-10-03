namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// The ownership rules of the page buffers an <see cref="IPageSource"/> hands
/// out: each read rents a buffer from the shared pool, and the caller gives it
/// back with <see cref="Return"/> or asks for a copy it owns outright with
/// <see cref="ReadPageCopyAsync"/>.
/// </summary>
internal static class PageBuffers
{
    /// <summary>Gives a page buffer that <see cref="IPageSource.ReadPageAsync"/> rented back to the shared pool.</summary>
    /// <param name="page">The pooled page buffer; the caller must not use it afterwards.</param>
    internal static void Return(byte[] page) => ArrayPool<byte>.Shared.Return(page);

    /// <summary>
    /// Returns a heap-allocated copy of the decrypted bytes of
    /// <paramref name="pageNumber"/>, exactly <see cref="IPageSource.PageSize"/>
    /// long. The caller owns the result; nothing is returned to the pool.
    /// </summary>
    /// <param name="pages">The page source.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    /// <returns>The page copy.</returns>
    internal static async ValueTask<byte[]> ReadPageCopyAsync(this IPageSource pages, long pageNumber, CancellationToken cancellationToken = default)
    {
        byte[] pooled = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        byte[] copy = new byte[pages.PageSize];
        Buffer.BlockCopy(pooled, 0, copy, 0, pages.PageSize);
        Return(pooled);
        return copy;
    }
}
