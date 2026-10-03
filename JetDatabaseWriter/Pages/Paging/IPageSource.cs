namespace JetDatabaseWriter.Pages.Paging;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Reads the decrypted pages of one open database file. Implemented by
/// <see cref="PageFile"/>, the read-only page file, and so by
/// <see cref="Pager"/>, the writer's page file, which also returns a pending
/// transaction's pages. Read-side services depend on this interface, never on
/// a type that can write.
/// </summary>
internal interface IPageSource
{
    /// <summary>Gets the page size in bytes.</summary>
    public int PageSize { get; }

    /// <summary>
    /// Gets the end of file in pages: one past the highest page number that
    /// can be read. Inside a writer's transaction this includes the pages the
    /// transaction has appended past the physical end of file, so it is the
    /// next page number an append assigns.
    /// </summary>
    public long PageCount { get; }

    /// <summary>Gets a value indicating whether the file has been disposed.</summary>
    public bool IsDisposed { get; }

    /// <summary>
    /// Reads and decrypts one page into a buffer rented from the shared pool.
    /// The caller owns the buffer and gives it back through
    /// <see cref="PageBuffers.Return"/>; the buffer may be longer than
    /// <see cref="PageSize"/>.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    /// <returns>The pooled page buffer.</returns>
    public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default);

    /// <summary>
    /// Throws <see cref="System.ObjectDisposedException"/>, naming the owning
    /// reader or writer, when the file has been disposed.
    /// </summary>
    public void ThrowIfDisposed();

    /// <summary>
    /// Throws <see cref="System.ObjectDisposedException"/> when the file has
    /// been disposed, then <see cref="System.OperationCanceledException"/> when
    /// <paramref name="cancellationToken"/> is cancelled.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken);
}
