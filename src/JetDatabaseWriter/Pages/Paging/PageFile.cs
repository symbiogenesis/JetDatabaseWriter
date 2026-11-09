namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Verification;

/// <summary>
/// The read side of one open database file's page I/O: the physical store,
/// its independent I/O synchronization, and the page codec that
/// decrypts each page after it is read, and positional <c>RandomAccess</c>
/// reads for path-opened readers. It cannot write; the writer's
/// <see cref="Pager"/> derives from it and adds writes and the transaction
/// journal. A reader's object graph holds a <see cref="PageFile"/> and never a
/// <see cref="Pager"/>.
/// </summary>
internal class PageFile : IPageSource, IPageFrameOwner, IAsyncDisposable
{
    private readonly Type ownerType;
    private int inFlightReads;
    private int outstandingLeases;
    private int liveFrames;
    private int cachedRowDirectoryEntries;
    private long storeReads;

    /// <summary>
    /// Initializes a new instance of the <see cref="PageFile"/> class.
    /// </summary>
    /// <param name="stream">An open, seekable stream for the database file.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    /// <param name="pageKeys">The page cipher; the page file owns it and disposes it.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller keeps ownership of <paramref name="stream"/> and it is not disposed.</param>
    /// <param name="ownerType">The public type that owns the file, named by <see cref="ObjectDisposedException"/>s raised after disposal.</param>
    /// <param name="maxActiveLeases">The positive combined pending and live page lease allowance.</param>
    /// <param name="maxLiveFrames">The maximum total live page frames, or 0 for automatic sizing.</param>
    /// <param name="maxCachedRowDirectoryEntries">The positive aggregate cached row-directory entry allowance.</param>
    internal PageFile(
        Stream stream,
        int pageSize,
        IPageCodec pageKeys,
        bool leaveOpen,
        Type ownerType,
        int maxActiveLeases = 4096,
        int maxLiveFrames = 0,
        int maxCachedRowDirectoryEntries = 262_144)
        : this(
            stream is MemoryStream memory ? new MemoryPageStore(memory, leaveOpen) : new StreamPageStore(stream, leaveOpen),
            pageSize,
            pageKeys,
            ownerType,
            maxActiveLeases,
            maxLiveFrames,
            maxCachedRowDirectoryEntries)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PageFile"/> class over an owned store.</summary>
    /// <param name="store">The owned physical store.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="pageKeys">The owned page codec.</param>
    /// <param name="ownerType">The disposal exception owner.</param>
    /// <param name="maxActiveLeases">The positive combined pending and live page lease allowance.</param>
    /// <param name="maxLiveFrames">The maximum total live page frames, or 0 for automatic sizing.</param>
    /// <param name="maxCachedRowDirectoryEntries">The positive aggregate cached row-directory entry allowance.</param>
    internal PageFile(
        IPageStore store,
        int pageSize,
        IPageCodec pageKeys,
        Type ownerType,
        int maxActiveLeases = 4096,
        int maxLiveFrames = 0,
        int maxCachedRowDirectoryEntries = 262_144)
    {
        Guard.Positive(maxActiveLeases, nameof(maxActiveLeases));
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(maxLiveFrames);
#else
        if (maxLiveFrames < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLiveFrames));
        }
#endif
        Guard.Positive(maxCachedRowDirectoryEntries, nameof(maxCachedRowDirectoryEntries));

        this.Store = store;
        this.PageSize = pageSize;
        this.PageKeys = pageKeys;
        this.ownerType = ownerType;
        this.MaxActiveLeases = maxActiveLeases;
        this.MaxLiveFrames = maxLiveFrames > 0 ? maxLiveFrames : maxActiveLeases;
        this.MaxCachedDirectoryEntries = maxCachedRowDirectoryEntries;
        this.ReleaseLeaseCallback = () => Interlocked.Decrement(ref this.outstandingLeases);
    }

    /// <inheritdoc/>
    public int PageSize { get; }

    /// <inheritdoc/>
    /// <remarks>The read-only page file has no transaction, so this is <see cref="PhysicalPageCount"/>.</remarks>
    public virtual long PageCount => this.PhysicalPageCount;

    /// <inheritdoc/>
    public bool IsDisposed { get; private set; }

    /// <summary>Gets the backing stream.</summary>
    internal Stream Stream => ((StreamPageStore)this.Store).Stream;

    /// <summary>
    /// Gets the page count of the backing stream alone, ignoring any pages a
    /// writer's transaction has appended.
    /// </summary>
    internal long PhysicalPageCount => this.Store.Length / this.PageSize;

    /// <summary>
    /// Gets the length of the backing stream. Inside a writer's transaction
    /// this is the length when the transaction began, because appended pages
    /// stay in the journal until commit; use <see cref="PageCount"/> for page
    /// bounds.
    /// </summary>
    internal long LengthBytes => this.Store.Length;

    /// <summary>Gets a value indicating whether the backing stream is a <see cref="FileStream"/>.</summary>
    internal bool IsFileBacked => this.Store.Capabilities.IsFileBacked;

    /// <summary>
    /// Gets a value indicating whether <see cref="EnableRandomAccessPageReadsIfSupported"/>
    /// switched page reads to positional <c>RandomAccess</c> reads.
    /// </summary>
    internal bool UsesRandomAccessPageReads => this.Store.Capabilities.PositionalReads;

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

    /// <summary>Gets the number of concurrent store or pending page reads currently in flight.</summary>
    internal int InFlightReads => Volatile.Read(ref this.inFlightReads);

    /// <summary>Gets the combined pending and live page leases.</summary>
    internal int OutstandingLeases => Volatile.Read(ref this.outstandingLeases);

    /// <summary>Gets the total live page frame count (retained plus evicted-pinned).</summary>
    internal int LiveFrames => Volatile.Read(ref this.liveFrames);

    /// <summary>Gets the total row-directory slot entries cached across live frames.</summary>
    internal int CachedRowDirectoryEntries => Volatile.Read(ref this.cachedRowDirectoryEntries);

    /// <summary>Gets the cumulative physical store read count.</summary>
    internal long StoreReads => Interlocked.Read(ref this.storeReads);

    /// <summary>Gets or sets an optional eviction callback invoked when a direct lease read needs to reclaim an unpinned frame from an attached reader cache.</summary>
    internal Func<bool>? TryEvictUnpinnedCachedFrame { get; set; }

    /// <summary>Gets the gate for journal access only. It is never held across a store call.</summary>
    private protected SemaphoreSlim IoGate { get; } = new(1, 1);

    /// <summary>Gets the page cipher: page reads decrypt with it, and the writer's page writes encrypt with it.</summary>
    private protected IPageCodec PageKeys { get; }

    /// <summary>Gets the owned physical page store.</summary>
    private protected IPageStore Store { get; }

    /// <summary>Gets a value indicating whether the writer may have pending journal pages.</summary>
    private protected virtual bool HasPendingPages => false;

    /// <summary>Gets the pre-allocated lease-release callback.</summary>
    private protected Action ReleaseLeaseCallback { get; }

    /// <summary>Gets the configured maximum active page leases.</summary>
    private protected int MaxActiveLeases { get; private set; }

    /// <summary>Gets the configured maximum live page frames.</summary>
    private protected int MaxLiveFrames { get; private set; }

    /// <summary>Gets the configured maximum cached row-directory entries.</summary>
    private protected int MaxCachedDirectoryEntries { get; private set; }

    /// <summary>Synchronizes aggregate resource limits with an owning cache composition.</summary>
    /// <param name="cacheCapacity">The retained cache capacity.</param>
    /// <param name="maxLeases">The maximum active page leases.</param>
    /// <param name="maxFrames">The explicit maximum live frames, or 0 for automatic sizing.</param>
    /// <param name="maxDirectoryEntries">The maximum cached row-directory entries.</param>
    internal void ConfigureLimits(int cacheCapacity, int maxLeases, int maxFrames, int maxDirectoryEntries)
    {
        this.MaxActiveLeases = maxLeases;
        this.MaxLiveFrames = maxFrames > 0 ? maxFrames : checked(Math.Max(0, cacheCapacity) + maxLeases);
        this.MaxCachedDirectoryEntries = maxDirectoryEntries;
    }

    /// <summary>Records a physical store read.</summary>
    private protected virtual void OnStoreRead() => Interlocked.Increment(ref this.storeReads);

    /// <summary>Reserves one pending/live lease slot or throws <see cref="JetLimitationException"/>.</summary>
    /// <exception cref="JetLimitationException">The active page lease budget would be exceeded.</exception>
    internal void CountLease()
    {
        int current;
        do
        {
            current = Volatile.Read(ref this.outstandingLeases);
            DataPageKernel.TryReserveCount(checked((uint)current), checked((uint)this.MaxActiveLeases), out bool admitted, out uint next);
            if (!admitted)
            {
                string owner = this.ownerType == typeof(AccessWriter) ? "writer" : "reader";
                throw new JetLimitationException(JetErrorCode.PageLeaseBudgetExceeded, $"The {owner} permits at most {this.MaxActiveLeases} pending and live page leases.");
            }

            if (Interlocked.CompareExchange(ref this.outstandingLeases, checked((int)next), current) == current)
            {
                return;
            }
        }
        while (true);
    }

    /// <summary>Releases one reserved lease slot when acquisition fails before returning a lease.</summary>
    internal void ReleaseLeaseSlot() => Interlocked.Decrement(ref this.outstandingLeases);

    /// <summary>Reserves one live-frame slot or throws <see cref="JetLimitationException"/>.</summary>
    /// <exception cref="JetLimitationException">The live page frame budget would be exceeded.</exception>
    internal void ReserveLiveFrameOrThrow()
    {
        int current;
        do
        {
            current = Volatile.Read(ref this.liveFrames);
            DataPageKernel.TryReserveCount(checked((uint)current), checked((uint)this.MaxLiveFrames), out bool admitted, out uint next);
            if (!admitted)
            {
                if (this.TryEvictUnpinnedCachedFrame?.Invoke() == true)
                {
                    continue;
                }

                string owner = this.ownerType == typeof(AccessWriter) ? "writer" : "reader";
                throw new JetLimitationException(JetErrorCode.PageLeaseBudgetExceeded, $"The {owner} permits at most {this.MaxLiveFrames} live page frames.");
            }

            if (Interlocked.CompareExchange(ref this.liveFrames, checked((int)next), current) == current)
            {
                return;
            }
        }
        while (true);
    }

    /// <summary>Increments the live-frame counter under caller-controlled cache admission.</summary>
    /// <returns>The updated live-frame count.</returns>
    internal int IncrementLiveFrames() => Interlocked.Increment(ref this.liveFrames);

    /// <inheritdoc/>
    public bool TryReserveRowDirectoryEntries(int entryCount)
    {
        if (entryCount <= 0)
        {
            return true;
        }

        int current;
        do
        {
            current = Volatile.Read(ref this.cachedRowDirectoryEntries);
            if ((long)current + entryCount > this.MaxCachedDirectoryEntries)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref this.cachedRowDirectoryEntries, current + entryCount, current) == current)
            {
                return true;
            }
        }
        while (true);
    }

    /// <inheritdoc/>
    public void OnFrameReleased(byte[] buffer, bool bound, int cachedDirectoryEntries)
    {
        if (cachedDirectoryEntries > 0)
        {
            _ = Interlocked.Add(ref this.cachedRowDirectoryEntries, -cachedDirectoryEntries);
        }

        if (bound)
        {
            _ = Interlocked.Decrement(ref this.liveFrames);
        }

        PageBuffers.Return(buffer);
    }

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
        await StreamRead.ReadExactlyAsync(stream, hdr.AsMemory(), cancellationToken).ConfigureAwait(false);

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

    /// <summary>Acquires a direct page lease accounted against this page file's lease and frame budgets.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The disposable read lease.</returns>
    internal virtual async ValueTask<PageLease> ReadPageLeaseAsync(long pageNumber, CancellationToken cancellationToken = default)
    {
        this.ThrowIfDisposedOrCancelled(cancellationToken);
        this.CountLease();
        PageLease? lease = null;
        try
        {
            byte[] bytes = await this.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            var frame = PageFrame.Own(bytes, this.PageSize, owner: this);
            try
            {
                this.ReserveLiveFrameOrThrow();
                frame.MarkBoundToOwner();
                return lease = frame.Acquire(this.ReleaseLeaseCallback);
            }
            finally
            {
                frame.Release();
            }
        }
        finally
        {
            if (lease is null)
            {
                this.ReleaseLeaseSlot();
            }
        }
    }

    /// <inheritdoc/>
    public virtual async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        this.ReserveInFlightRead();
        try
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(this.PageSize);
            try
            {
                bool pending = false;
                if (this.HasPendingPages)
                {
                    await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        pending = this.TryCopyPendingPage(pageNumber, buffer);
                    }
                    finally
                    {
                        _ = this.IoGate.Release();
                    }
                }

                if (!pending)
                {
                    this.OnStoreRead();
                    await this.Store.ReadAsync(pageNumber * this.PageSize, buffer.AsMemory(0, this.PageSize), this.CanReadInline(), cancellationToken).ConfigureAwait(false);
                    this.PageKeys.Decode(buffer, 0, pageNumber, this.PageSize);
                }

                return buffer;
            }
            catch
            {
                PageBuffers.Return(buffer);
                throw;
            }
        }
        finally
        {
            _ = Interlocked.Decrement(ref this.inFlightReads);
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
            await this.Store.DisposeAsync().ConfigureAwait(false);
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
    internal void EnableRandomAccessPageReadsIfSupported() => ((StreamPageStore)this.Store).EnablePositionalReads();

    /// <summary>
    /// Disposes the I/O gate and the page cipher, but not the backing stream.
    /// The owning reader or writer calls it when its construction fails before
    /// an instance can be returned to the caller and disposed normally.
    /// </summary>
    internal virtual void DisposeManagedResources()
    {
        this.Store.DisposeManagedResources();
        this.IoGate.Dispose();
        this.PageKeys.Dispose();
    }

    /// <summary>
    /// Copies the pending (not yet written) contents of <paramref name="pageNumber"/>
    /// into <paramref name="buffer"/> when the writer holds one. Called under
    /// <see cref="IoGate"/>; the gate is released before any store read.
    /// A hit is plaintext and is not decrypted. The read-only page
    /// file has none.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="buffer">The buffer that receives the page.</param>
    /// <returns><see langword="true"/> when <paramref name="buffer"/> holds the pending page.</returns>
    private protected virtual bool TryCopyPendingPage(long pageNumber, byte[] buffer) => false;

    private void ReserveInFlightRead()
    {
        int current;
        do
        {
            current = Volatile.Read(ref this.inFlightReads);
            DataPageKernel.TryReserveCount(checked((uint)current), checked((uint)this.MaxActiveLeases), out bool admitted, out uint next);
            if (!admitted)
            {
                string owner = this.ownerType == typeof(AccessWriter) ? "writer" : "reader";
                throw new JetLimitationException(JetErrorCode.PageLeaseBudgetExceeded, $"The {owner} permits at most {this.MaxActiveLeases} concurrent in-flight page reads.");
            }

            if (Interlocked.CompareExchange(ref this.inFlightReads, checked((int)next), current) == current)
            {
                return;
            }
        }
        while (true);
    }

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
}
