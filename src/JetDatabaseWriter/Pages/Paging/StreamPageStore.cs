namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;

/// <summary>Stream storage with its own seek gate and optional positional reads.</summary>
internal class StreamPageStore : IPageStore
{
    private readonly SemaphoreSlim ioGate = new(1, 1);
    private readonly bool leaveOpen;
    private bool resourcesDisposed;
    private FilePageDigests? pageDigests;
#if NET6_0_OR_GREATER
    private Microsoft.Win32.SafeHandles.SafeFileHandle? handle;
#endif

    /// <summary>Initializes a new instance of the <see cref="StreamPageStore"/> class.</summary>
    /// <param name="stream">The backing stream.</param>
    /// <param name="leaveOpen">Whether the caller owns the stream.</param>
    /// <param name="recoveryLease">An optional persistent caller-stream recovery lease.</param>
    internal StreamPageStore(Stream stream, bool leaveOpen, IPersistentStreamRecoveryLease? recoveryLease = null)
    {
        this.Stream = stream;
        this.leaveOpen = leaveOpen;
        this.RecoveryLease = recoveryLease;
    }

    /// <inheritdoc/>
    public StoreCapabilities Capabilities => new(this.Stream is FileStream, this.PositionalReads, this.HasPersistentRecovery, false, this.Stream is FileStream && this.AcquireWriteLock is not null, true);

    /// <inheritdoc/>
    public long Length => this.Stream.Length;

    /// <summary>Gets the backing stream for container maintenance.</summary>
    internal Stream Stream { get; }

    /// <summary>Gets the active persistent caller-stream recovery lease, if any.</summary>
    internal IPersistentStreamRecoveryLease? RecoveryLease { get; }

    /// <summary>Gets a value indicating whether the store uses persistent recovery journaling.</summary>
    internal bool HasPersistentRecovery => this.Stream is FileStream || this.RecoveryLease is not null;

    /// <summary>Gets or sets the crash journal active during physical transaction writes.</summary>
    internal PersistentRollbackJournal? RollbackJournal { get; set; }

    /// <summary>Acquires raw digest reuse under the writer's exclusive, serialized store ownership.</summary>
    /// <param name="pageSize">The validated physical page size.</param>
    /// <returns>The digest ledger for the current physical image.</returns>
    /// <exception cref="InvalidOperationException">The store does not own persistent recovery.</exception>
    internal FilePageDigests GetPageDigests(int pageSize)
    {
        if (!this.HasPersistentRecovery)
        {
            throw new InvalidOperationException("Only an exclusively owned persistent store can retain raw page digests.");
        }

        if (this.pageDigests is not { IsValid: true } || this.pageDigests.Length != this.Stream.Length || this.pageDigests.PageSize != pageSize)
        {
            this.pageDigests?.Invalidate();
            this.pageDigests = FilePageDigests.Load(this.Stream, pageSize);
        }

        return this.pageDigests;
    }

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
            if (!BinaryBuffer.IsRange(this.Stream.Length, offset, buffer.Length))
            {
                throw new EndOfStreamException();
            }

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
                long oldLength = this.Stream.Length;
                _ = this.Stream.Seek(offset, SeekOrigin.Begin);
                await this.Stream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
                this.pageDigests?.Written(oldLength, offset, buffer.Span);
            }
            catch
            {
                this.pageDigests?.Invalidate();
                throw;
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
            long oldLength = this.Stream.Length;
            if (length < this.Stream.Length)
            {
                this.RollbackJournal?.RecordBeforeTruncate(length);
            }

            this.Stream.SetLength(length);
            this.pageDigests?.Resized(oldLength, length);
        }
        catch
        {
            this.pageDigests?.Invalidate();
            throw;
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
            if (toDisk && this.RecoveryLease is { } recoveryLease)
            {
                await this.Stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                recoveryLease.FenceDatabase(this.Stream);
            }
            else if (toDisk && this.Stream is FileStream file)
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
            this.RecoveryLease?.Dispose();
        }
    }
}
