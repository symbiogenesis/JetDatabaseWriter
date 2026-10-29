namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;

/// <summary>Stream storage with its own seek gate and optional positional reads.</summary>
internal class StreamPageStore : IPageStore
{
    private readonly SemaphoreSlim ioGate = new(1, 1);
    private readonly bool leaveOpen;
    private bool resourcesDisposed;
#if NET6_0_OR_GREATER
    private Microsoft.Win32.SafeHandles.SafeFileHandle? handle;
#endif

    /// <summary>Initializes a new instance of the <see cref="StreamPageStore"/> class.</summary>
    /// <param name="stream">The backing stream.</param>
    /// <param name="leaveOpen">Whether the caller owns the stream.</param>
    internal StreamPageStore(Stream stream, bool leaveOpen)
    {
        this.Stream = stream;
        this.leaveOpen = leaveOpen;
    }

    /// <inheritdoc/>
    public StoreCapabilities Capabilities => new(this.Stream is FileStream, this.PositionalReads, this.Stream is FileStream, false, this.Stream is FileStream && this.AcquireWriteLock is not null, true);

    /// <inheritdoc/>
    public long Length => this.Stream.Length;

    /// <summary>Gets the backing stream for container maintenance.</summary>
    internal Stream Stream { get; }

    /// <summary>Gets or sets the crash journal active during physical transaction writes.</summary>
    internal PersistentRollbackJournal? RollbackJournal { get; set; }

    /// <summary>Gets a value indicating whether positional reads are enabled.</summary>
    internal bool PositionalReads { get; private set; }

    /// <summary>Gets or sets the writer's cooperative lock acquisition.</summary>
    internal Func<long, int, CancellationToken, ValueTask<IDisposable>>? AcquireWriteLock { get; set; }

    /// <summary>Enables positional reads on a supported file handle.</summary>
    internal void EnablePositionalReads()
#if NET6_0_OR_GREATER
    {
        if (this.Stream is FileStream file)
        {
            this.handle = file.SafeFileHandle;
            this.PositionalReads = !this.handle.IsInvalid && !this.handle.IsClosed;
        }
    }
#else
        => this.PositionalReads = false;
#endif

    /// <inheritdoc/>
    public async ValueTask ReadAsync(long offset, Memory<byte> buffer, bool inline, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!BinaryBuffer.IsRange(long.MaxValue, offset, buffer.Length))
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "The requested read exceeds the supported offset range.");
        }

#if NET6_0_OR_GREATER
        if (this.handle is { } fileHandle)
        {
            int total = 0;
            while (total < buffer.Length)
            {
                int read = inline
                    ? RandomAccess.Read(fileHandle, buffer.Span[total..], checked(offset + total))
                    : await RandomAccess.ReadAsync(fileHandle, buffer[total..], checked(offset + total), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    throw new EndOfStreamException();
                }

                total = StreamRead.AdvanceRead(buffer.Length, total, read);
            }

            return;
        }
#endif
        await this.ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _ = this.Stream.Seek(offset, SeekOrigin.Begin);
            if (inline)
            {
                StreamRead.ReadExactly(this.Stream, buffer.Span);
            }
            else
            {
                await StreamRead.ReadExactlyAsync(this.Stream, buffer, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = this.ioGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
    {
        IDisposable? pageLock = this.AcquireWriteLock is { } acquire
            ? await acquire(offset / buffer.Length, buffer.Length, cancellationToken).ConfigureAwait(false)
            : null;
        try
        {
            await this.ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                this.RollbackJournal?.RecordBeforeWrite(offset, buffer.Span);
                _ = this.Stream.Seek(offset, SeekOrigin.Begin);
                await this.Stream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = this.ioGate.Release();
            }
        }
        finally
        {
            pageLock?.Dispose();
        }
    }

    /// <inheritdoc/>
    public async ValueTask SetLengthAsync(long length, CancellationToken cancellationToken)
    {
        await this.ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (length < this.Stream.Length)
            {
                this.RollbackJournal?.RecordBeforeTruncate(length);
            }

            this.Stream.SetLength(length);
        }
        finally
        {
            _ = this.ioGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken)
    {
        await this.ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (toDisk && this.Stream is FileStream file)
            {
                file.Flush(true);
            }
            else
            {
                await this.Stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = this.ioGate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!this.leaveOpen)
            {
                await this.Stream.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            this.DisposeManagedResources();
        }
    }

    /// <inheritdoc/>
    public void DisposeManagedResources()
    {
        if (!this.resourcesDisposed)
        {
            this.resourcesDisposed = true;
            this.ioGate.Dispose();
        }
    }
}
