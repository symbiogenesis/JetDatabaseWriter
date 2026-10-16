namespace JetDatabaseWriter.Maintenance;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Bounds every inventory read, including reads made by its private metadata services.</summary>
/// <param name="inner">The caller-owned immutable page source.</param>
/// <param name="maxReads">The aggregate read allowance.</param>
internal sealed class CompactInventoryPageSource(IPageSource inner, long maxReads) : IPageSource
{
    private long reads;

    /// <inheritdoc/>
    public int PageSize { get; } = inner.PageSize;

    /// <inheritdoc/>
    public long PageCount { get; } = inner.PageCount;

    /// <inheritdoc/>
    public bool IsDisposed => inner.IsDisposed;

    /// <inheritdoc/>
    public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
    {
        this.ThrowIfDisposedOrCancelled(cancellationToken);
        if (this.reads >= maxReads)
        {
            const string reason = "The aggregate inventory reads exceed MaxPageReads.";
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = pageNumber, Reason = reason });
        }

        if (pageNumber < 0 || pageNumber >= this.PageCount || inner.PageCount != this.PageCount || inner.PageSize != this.PageSize)
        {
            throw Corrupt(pageNumber, "The inventory page reference or source geometry is invalid.");
        }

        this.reads++;
        byte[] page = await inner.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (page.Length < this.PageSize || inner.PageCount != this.PageCount || inner.PageSize != this.PageSize)
            {
                throw Corrupt(pageNumber, "The inventory received a truncated physical page or changing source geometry.");
            }

            return page;
        }
        catch
        {
            PageBuffers.Return(page);
            throw;
        }
    }

    /// <inheritdoc/>
    public void ThrowIfDisposed() => inner.ThrowIfDisposed();

    /// <inheritdoc/>
    public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
    {
        inner.ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptTableDefinition, reason, new JetErrorInfo { PageNumber = page, Reason = reason });
}
