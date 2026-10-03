namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// The read side of one open database file's page I/O: the backing stream, the
/// I/O gate that serializes seek-and-read access to it, the page cipher that
/// decrypts each page after it is read, and positional <c>RandomAccess</c>
/// reads for path-opened readers. It cannot write; the writer's
/// <see cref="Pager"/> derives from it and adds writes and the transaction
/// journal. A reader's object graph holds a <see cref="PageFile"/> and never a
/// <see cref="Pager"/>.
/// </summary>
internal class PageFile : IPageSource, IAsyncDisposable
{
    private readonly bool leaveOpen;
    private readonly Type ownerType;

#if NET6_0_OR_GREATER
    /// <summary>
    /// The backing <see cref="FileStream"/>'s handle, read once by
    /// <see cref="EnableRandomAccessPageReadsIfSupported"/>; <see langword="null"/>
    /// until then. The stream owns the handle and closes it on dispose.
    /// </summary>
    private Microsoft.Win32.SafeHandles.SafeFileHandle? randomAccessHandle;
#endif

    /// <summary>
    /// Initializes a new instance of the <see cref="PageFile"/> class.
    /// </summary>
    /// <param name="stream">An open, seekable stream for the database file.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    /// <param name="pageKeys">The page cipher; the page file owns it and disposes it.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller keeps ownership of <paramref name="stream"/> and it is not disposed.</param>
    /// <param name="ownerType">The public type that owns the file, named by <see cref="ObjectDisposedException"/>s raised after disposal.</param>
    internal PageFile(Stream stream, int pageSize, PageDecryptionKeys pageKeys, bool leaveOpen, Type ownerType)
    {
        this.Stream = stream;
        this.PageSize = pageSize;
        this.PageKeys = pageKeys;
        this.leaveOpen = leaveOpen;
        this.ownerType = ownerType;
    }

    /// <inheritdoc/>
    public int PageSize { get; }

    /// <inheritdoc/>
    /// <remarks>The read-only page file has no transaction, so this is <see cref="PhysicalPageCount"/>.</remarks>
    public virtual long PageCount => this.PhysicalPageCount;

    /// <inheritdoc/>
    public bool IsDisposed { get; private set; }

    /// <summary>Gets the backing stream.</summary>
    internal Stream Stream { get; }

    /// <summary>
    /// Gets the page count of the backing stream alone, ignoring any pages a
    /// writer's transaction has appended.
    /// </summary>
    internal long PhysicalPageCount => this.Stream.Length / this.PageSize;

    /// <summary>
    /// Gets the length of the backing stream. Inside a writer's transaction
    /// this is the length when the transaction began, because appended pages
    /// stay in the journal until commit; use <see cref="PageCount"/> for page
    /// bounds.
    /// </summary>
    internal long LengthBytes => this.Stream.Length;

    /// <summary>Gets a value indicating whether the backing stream is a <see cref="FileStream"/>.</summary>
    internal bool IsFileBacked => this.Stream is FileStream;

    /// <summary>
    /// Gets a value indicating whether <see cref="EnableRandomAccessPageReadsIfSupported"/>
    /// switched page reads to positional <c>RandomAccess</c> reads.
    /// </summary>
    internal bool UsesRandomAccessPageReads { get; private set; }

    /// <summary>
    /// Gets or sets a value indicating whether a page read that starts on a
    /// thread-pool thread, with no <see cref="SynchronizationContext"/> and the
    /// default <see cref="TaskScheduler"/>, reads the file synchronously on that
    /// thread instead of handing the read to another pool thread. Only
    /// <see cref="AccessReader.OpenAsync(string, AccessReaderOptions?, CancellationToken)"/>
    /// sets it, because its file handle is synchronous: a blocking read on a
    /// caller's stream, which may be overlapped, or on the writer's overlapped
    /// handle would be slower than the offloaded one.
    /// </summary>
    /// <remarks>
    /// An inline read cannot be cancelled once it has started; the token is
    /// checked before it. Other callers, such as a UI thread or a thread with a
    /// synchronization context, keep the offloaded read and never block on the disk.
    /// </remarks>
    internal bool ReadsInlineOnThreadPool { get; set; }

    /// <summary>
    /// Gets the I/O gate that serializes seek-based stream access and, in the
    /// writer, journal attach and detach. Never held across a page read or
    /// write call: those take it themselves, and it is not reentrant.
    /// </summary>
    private protected SemaphoreSlim IoGate { get; } = new(1, 1);

    /// <summary>Gets the page cipher: page reads decrypt with it, and the writer's page writes encrypt with it.</summary>
    private protected PageDecryptionKeys PageKeys { get; }

    /// <summary>
    /// Gets a value indicating whether a page read may bypass <see cref="IoGate"/>
    /// through a positional <c>RandomAccess</c> read. The writer turns it off
    /// while a transaction journal is attached, because a pending page must be
    /// read from the journal under the gate.
    /// </summary>
    private protected virtual bool CanReadPositionally => true;

    /// <summary>
    /// Asynchronously reads the fixed-size JET header (first 0x80 bytes) from page 0.
    /// </summary>
    /// <param name="stream">An open, seekable stream positioned anywhere.</param>
    /// <param name="cancellationToken">Token used to cancel the read operation.</param>
    /// <returns>A 0x80-byte header buffer.</returns>
    internal static async ValueTask<byte[]> ReadHeaderAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        byte[] hdr = new byte[Constants.DatabaseHeader.Length];
        _ = stream.Seek(0, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(hdr.AsMemory(), cancellationToken).ConfigureAwait(false);

        return hdr;
    }

    /// <summary>
    /// Opens a database file with the given access / share / option combination.
    /// Used by both <see cref="AccessReader"/> (a synchronous handle with no
    /// access hint) and <see cref="AccessWriter"/> (an overlapped read-write
    /// handle with the random-access hint).
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="access">The access.</param>
    /// <param name="share">The share.</param>
    /// <param name="options">The options.</param>
    /// <returns>The opened stream.</returns>
    internal static FileStream OpenFileStream(string path, FileAccess access, FileShare share, FileOptions options) => FileStreamFactory.Open(path, FileMode.Open, access, share, options);

    /// <inheritdoc/>
    public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        byte[] buf = ArrayPool<byte>.Shared.Rent(this.PageSize);
        try
        {
#if NET6_0_OR_GREATER
            if (this.randomAccessHandle is { } handle && this.CanReadPositionally)
            {
                if (this.CanReadInline())
                {
                    this.ReadPageRandomAccess(handle, pageNumber, buf);
                }
                else
                {
                    await this.ReadPageRandomAccessAsync(handle, pageNumber, buf, cancellationToken).ConfigureAwait(false);
                }
            }
            else
#endif
            {
                await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    // Inside a writer's transaction, prefer the journal: the page
                    // may be a transaction-local mutation (or an appended page that
                    // has no on-disk slot yet). Journal bytes are plaintext, so a
                    // hit skips the decrypt.
                    if (this.TryCopyPendingPage(pageNumber, buf))
                    {
                        return buf;
                    }

                    _ = this.Stream.Seek(pageNumber * this.PageSize, SeekOrigin.Begin);
                    if (this.CanReadInline())
                    {
                        this.ReadPageFromStream(buf);
                    }
                    else
                    {
                        await this.Stream.ReadExactlyAsync(buf.AsMemory(0, this.PageSize), cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    _ = this.IoGate.Release();
                }
            }

            EncryptionManager.DecryptPageInPlace(buf, pageNumber, this.PageSize, this.PageKeys);

            return buf;
        }
        catch
        {
            PageBuffers.Return(buf);
            throw;
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ThrowIfDisposed() => Guard.ThrowIfDisposed(this.IsDisposed, this.ownerType);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
    {
        Guard.ThrowIfDisposed(this.IsDisposed, this.ownerType);
        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Marks the file disposed, disposes the backing stream unless the caller
    /// kept ownership of it, then the I/O gate and the page cipher.
    /// </summary>
    /// <returns>A task that completes when the stream is disposed.</returns>
    public async ValueTask DisposeAsync()
    {
        if (this.IsDisposed)
        {
            return;
        }

        this.IsDisposed = true;
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

    /// <summary>
    /// Switches page reads to positional <c>RandomAccess</c> reads on the
    /// backing <see cref="FileStream"/>'s handle, which bypass <see cref="IoGate"/>
    /// and the shared stream position. Does nothing for other streams and in
    /// the netstandard2.1 build, which has no <c>RandomAccess</c>.
    /// </summary>
    internal void EnableRandomAccessPageReadsIfSupported()
    {
#if NET6_0_OR_GREATER
        // FileStream.SafeFileHandle is not a field read: every get flushes the
        // stream's buffer and seeks the OS file pointer to the stream's
        // position. Read once per page, it made RandomAccess page reads slower
        // than seek-and-read through the stream, so the handle is kept here.
        if (this.Stream is FileStream fileStream)
        {
            Microsoft.Win32.SafeHandles.SafeFileHandle handle = fileStream.SafeFileHandle;
            if (!handle.IsInvalid && !handle.IsClosed)
            {
                this.randomAccessHandle = handle;
                this.UsesRandomAccessPageReads = true;
            }
        }
#else
        this.UsesRandomAccessPageReads = false;

        _ = this.UsesRandomAccessPageReads;
#endif
    }

    /// <summary>
    /// Disposes the I/O gate and the page cipher, but not the backing stream.
    /// The owning reader or writer calls it when its construction fails before
    /// an instance can be returned to the caller and disposed normally.
    /// </summary>
    internal void DisposeManagedResources()
    {
        this.IoGate.Dispose();
        this.PageKeys.Dispose();
    }

    /// <summary>
    /// Copies the pending (not yet written) contents of <paramref name="pageNumber"/>
    /// into <paramref name="buffer"/> when the writer holds one. Called under
    /// <see cref="IoGate"/>, in the same acquisition as the stream read it
    /// replaces; a hit is plaintext and is not decrypted. The read-only page
    /// file has none.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="buffer">The buffer that receives the page.</param>
    /// <returns><see langword="true"/> when <paramref name="buffer"/> holds the pending page.</returns>
    private protected virtual bool TryCopyPendingPage(long pageNumber, byte[] buffer) => false;

    /// <summary>
    /// Determines whether this page read runs synchronously on the calling
    /// thread: see <see cref="ReadsInlineOnThreadPool"/>. A pool thread with no
    /// synchronization context and the default scheduler would otherwise hand
    /// the read to another pool thread and wait for it.
    /// </summary>
    private bool CanReadInline() =>
        this.ReadsInlineOnThreadPool
        && Thread.CurrentThread.IsThreadPoolThread
        && SynchronizationContext.Current is null
        && TaskScheduler.Current == TaskScheduler.Default;

    /// <summary>
    /// Reads one page from the backing stream's current position, blocking the
    /// calling thread. The caller holds <see cref="IoGate"/>.
    /// </summary>
    /// <param name="page">The buffer that receives the page.</param>
    /// <exception cref="EndOfStreamException">The stream ends before the page does.</exception>
    private void ReadPageFromStream(byte[] page)
    {
        int totalRead = 0;
        while (totalRead < this.PageSize)
        {
            int bytesRead = this.Stream.Read(page.AsSpan(totalRead, this.PageSize - totalRead));
            if (bytesRead == 0)
            {
                throw new EndOfStreamException();
            }

            totalRead += bytesRead;
        }
    }

#if NET6_0_OR_GREATER
    /// <summary>Reads one page at its file offset through <paramref name="handle"/>, blocking the calling thread.</summary>
    /// <param name="handle">The cached file handle.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The buffer that receives the page.</param>
    /// <exception cref="EndOfStreamException">The file ends before the page does.</exception>
    private void ReadPageRandomAccess(Microsoft.Win32.SafeHandles.SafeFileHandle handle, long pageNumber, byte[] page)
    {
        long fileOffset = pageNumber * this.PageSize;
        int totalRead = 0;
        while (totalRead < this.PageSize)
        {
            int bytesRead = RandomAccess.Read(handle, page.AsSpan(totalRead, this.PageSize - totalRead), fileOffset + totalRead);
            if (bytesRead == 0)
            {
                throw new EndOfStreamException();
            }

            totalRead += bytesRead;
        }
    }

    private async ValueTask ReadPageRandomAccessAsync(Microsoft.Win32.SafeHandles.SafeFileHandle handle, long pageNumber, byte[] page, CancellationToken cancellationToken)
    {
        long fileOffset = pageNumber * this.PageSize;
        int totalRead = 0;
        while (totalRead < this.PageSize)
        {
            int bytesRead = await RandomAccess.ReadAsync(
                handle,
                page.AsMemory(totalRead, this.PageSize - totalRead),
                fileOffset + totalRead,
                cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                throw new EndOfStreamException();
            }

            totalRead += bytesRead;
        }
    }
#endif
}
