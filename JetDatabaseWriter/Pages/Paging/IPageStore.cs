namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>The physical byte-addressed database storage.</summary>
internal interface IPageStore : IAsyncDisposable
{
    /// <summary>Gets storage capabilities.</summary>
    public StoreCapabilities Capabilities { get; }

    /// <summary>Gets the physical length.</summary>
    public long Length { get; }

    /// <summary>Reads exactly the requested bytes.</summary>
    /// <param name="offset">The byte offset.</param>
    /// <param name="buffer">The destination.</param>
    /// <param name="inline">Whether synchronous reads are permitted.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The completion.</returns>
    public ValueTask ReadAsync(long offset, Memory<byte> buffer, bool inline, CancellationToken cancellationToken);

    /// <summary>Writes the requested bytes with a cooperative page lock.</summary>
    /// <param name="offset">The byte offset.</param>
    /// <param name="buffer">The source.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The completion.</returns>
    public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken);

    /// <summary>Sets the physical length.</summary>
    /// <param name="length">The new length.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The completion.</returns>
    public ValueTask SetLengthAsync(long length, CancellationToken cancellationToken);

    /// <summary>Flushes the store, durably when supported and requested.</summary>
    /// <param name="toDisk">Whether to flush to disk.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The completion.</returns>
    public ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken);
    /// <summary>Disposes owned synchronization resources without closing the stream.</summary>
    public void DisposeManagedResources();
}