namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Transactions;

/// <summary>
/// The writer's page file: everything <see cref="PageFile"/> reads, plus page
/// writes, appends, truncation and flushes, the encrypt-on-write copy, the
/// cooperative byte-range page locks, and the in-memory journal of an explicit
/// transaction. While a journal is attached, writes and appends are buffered
/// in it instead of reaching the stream, reads return its pending pages, and
/// <see cref="PageCount"/> includes the pages it appended. Only the writer's
/// object graph holds a <see cref="Pager"/>.
/// </summary>
internal sealed class Pager : PageFile
{
    private readonly SemaphoreSlim frameGate = new(1, 1);
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock frameSync = new();
#else
    private readonly object frameSync = new();
#endif
    private readonly Dictionary<long, Frame> frames = [];
    private readonly List<long> clock = [];
    private readonly int cacheSize;

    /// <summary>
    /// The journal of the active explicit transaction, or <see langword="null"/>.
    /// Attached and detached only through a <see cref="JournalGate"/>, which
    /// holds the I/O gate, or by <see cref="ForceDetachJournal"/> at dispose.
    /// </summary>
    private PageJournal? journal;
    private int clockHand;
    private long generation;
    private long storeReads;
    private long cacheHits;
    private long evictions;

    /// <summary>
    /// Initializes a new instance of the <see cref="Pager"/> class.
    /// </summary>
    /// <param name="stream">An open, readable, writable, seekable stream for the database file.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    /// <param name="pageKeys">The page cipher; the pager owns it and disposes it.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller keeps ownership of <paramref name="stream"/> and it is not disposed.</param>
    /// <param name="ownerType">The public type that owns the file, named by <see cref="ObjectDisposedException"/>s raised after disposal.</param>
    /// <param name="cacheSize">The maximum cached frame count; zero disables caching.</param>
    /// <exception cref="ArgumentOutOfRangeException">The cache size is negative.</exception>
    internal Pager(Stream stream, int pageSize, IPageCodec pageKeys, bool leaveOpen, Type ownerType, int cacheSize = 256)
        : this(stream is MemoryStream memory ? new MemoryPageStore(memory, leaveOpen) : new StreamPageStore(stream, leaveOpen), pageSize, pageKeys, ownerType, cacheSize)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="Pager"/> class over an owned physical store.</summary>
    /// <param name="store">The owned store.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="pageKeys">The owned page codec.</param>
    /// <param name="ownerType">The disposal exception owner.</param>
    /// <param name="cacheSize">The maximum frame count.</param>
    /// <exception cref="ArgumentOutOfRangeException">The cache size is negative.</exception>
    internal Pager(IPageStore store, int pageSize, IPageCodec pageKeys, Type ownerType, int cacheSize = 256)
        : base(store, pageSize, pageKeys, ownerType)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(cacheSize);
#else
        if (cacheSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(cacheSize));
        }
#endif

        this.cacheSize = cacheSize;
    }

    /// <summary>
    /// Gets the end of file in pages: one past the highest page number that
    /// can be read. Inside a transaction this includes the pages the journal
    /// has appended past the physical end of file, so it is the next page
    /// number <see cref="AppendPageAsync"/> assigns. Every page-number bounds
    /// check and every caller that numbers pages before appending them uses
    /// this count.
    /// </summary>
    public override long PageCount => this.journal?.NextAppendPageNumber ?? this.PhysicalPageCount;

    /// <summary>
    /// Gets or sets the cooperative JET byte-range lock helper. Defaults to
    /// <see cref="JetByteRangeLock.Disabled"/> so page writes can dispatch
    /// without a null check; the owning writer replaces it with a stream-bound
    /// instance once its lock-file slot is held.
    /// </summary>
    internal JetByteRangeLock ByteRangeLock
    {
        get;
        set
        {
            field = value;
            ((StreamPageStore)this.Store).AcquireWriteLock = value.IsEnabled ? value.AcquirePageLockAsync : null;
        }
    } = JetByteRangeLock.Disabled;

    /// <summary>Gets a value indicating whether a transaction journal is attached.</summary>
    internal bool IsJournalActive => this.journal is not null;

    /// <summary>Gets physical-read and cache counters.</summary>
    internal PagerStatistics Statistics
    {
        get
        {
            lock (this.frameSync)
            {
                return new(Interlocked.Read(ref this.storeReads), this.cacheHits, this.evictions, this.frames.Count);
            }
        }
    }

    /// <summary>
    /// Writes one page in place. Inside a transaction the page is buffered in
    /// the journal; otherwise it is encrypted (when the file is), written
    /// under its byte-range lock, and flushed.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The plaintext page; only its first <see cref="PageFile.PageSize"/> bytes are written, and the buffer is not changed.</param>
    /// <param name="cancellationToken">A token used to cancel the write.</param>
    /// <returns>A task that completes when the page is written or buffered.</returns>
    internal async ValueTask WritePageAsync(long pageNumber, byte[] page, CancellationToken cancellationToken = default)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.WriteCoreAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = this.frameGate.Release();
        }
    }

    /// <summary>
    /// Appends one page at the end of file and returns its page number. Inside
    /// a transaction the page is buffered in the journal past the physical end
    /// of file; otherwise it is written as <see cref="WritePageAsync"/> writes.
    /// </summary>
    /// <param name="page">The plaintext page.</param>
    /// <param name="cancellationToken">A token used to cancel the append.</param>
    /// <returns>The appended page's number.</returns>
    internal async ValueTask<long> AppendPageAsync(byte[] page, CancellationToken cancellationToken = default)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (this.journal is { } active)
                {
                    long appended = active.Append(page.AsSpan(0, this.PageSize));
                    lock (this.frameSync)
                    {
                        this.RetainFrame(appended, page);
                    }

                    return appended;
                }
            }
            finally
            {
                _ = this.IoGate.Release();
            }

            long pageNumber = this.PhysicalPageCount;
            await this.WriteCoreAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            return pageNumber;
        }
        finally
        {
            _ = this.frameGate.Release();
        }
    }

    /// <summary>Sets the length of the backing stream and flushes it.</summary>
    /// <param name="length">The new length in bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the stream is resized and flushed.</returns>
    internal async ValueTask SetLengthAsync(long length, CancellationToken cancellationToken)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.InvalidateAll();
            await this.Store.SetLengthAsync(length, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.InvalidateAll();
            _ = this.frameGate.Release();
        }
    }

    /// <summary>Truncates to a page count and discards every cached frame.</summary>
    /// <param name="pageCount">The remaining page count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The completion.</returns>
    internal ValueTask TruncateAsync(long pageCount, CancellationToken cancellationToken = default)
        => this.SetLengthAsync(checked(pageCount * this.PageSize), cancellationToken);

    /// <summary>
    /// Flushes the backing stream; with <paramref name="toDisk"/> a
    /// <see cref="FileStream"/> is flushed through the OS cache to the device.
    /// </summary>
    /// <param name="toDisk">Whether to flush a file through to the device.</param>
    /// <param name="cancellationToken">A token used to cancel the flush.</param>
    /// <returns>A task that completes when the stream is flushed.</returns>
    internal ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken) => this.Store.FlushAsync(toDisk, cancellationToken);

    /// <summary>
    /// Takes the frame gate and then the journal gate, waiting for in-flight
    /// page I/O, and returns a lease through which a transaction
    /// attaches or detaches its journal. Dispose the lease to release the
    /// gates. Never hold it across a page read or write call: those take the
    /// gate themselves, and it is not reentrant, so the call would wait on
    /// itself forever.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the wait for the gate.</param>
    /// <returns>The lease, holding the gate.</returns>
    internal async ValueTask<JournalGate> EnterJournalGateAsync(CancellationToken cancellationToken)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new JournalGate(this);
        }
        catch
        {
            _ = this.frameGate.Release();
            throw;
        }
    }

    /// <summary>
    /// Detaches any journal without taking the I/O gate. Only the writer's
    /// dispose path calls it, after the transaction's own rollback, which
    /// detaches under the gate, has run or failed.
    /// </summary>
    internal void ForceDetachJournal()
    {
        this.journal = null;
        this.InvalidateAll();
    }

    /// <inheritdoc/>
    private protected override bool TryCopyPendingPage(long pageNumber, byte[] buffer)
    {
        byte[]? pending = this.journal?.TryGet(pageNumber);
        if (pending is null)
        {
            return false;
        }

        Buffer.BlockCopy(pending, 0, buffer, 0, this.PageSize);
        return true;
    }

    /// <summary>
    /// Returns <paramref name="page"/> unchanged when no page-encryption is
    /// active, or a freshly allocated, encrypted copy otherwise. The caller's
    /// buffer is never mutated so it can be reused safely after writing.
    /// Page 0 (the unencrypted header) is always returned as-is.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    private byte[] PrepareEncryptedPageForWrite(long pageNumber, byte[] page)
    {
        if (pageNumber < 1 || !this.PageKeys.HasEncryption)
        {
            return page;
        }

        byte[] copy = new byte[this.PageSize];
        Buffer.BlockCopy(page, 0, copy, 0, this.PageSize);
        this.PageKeys.Encode(copy, 0, pageNumber, this.PageSize);
        return copy;
    }

    /// <inheritdoc/>
    private protected override void OnStoreRead() => Interlocked.Increment(ref this.storeReads);

    /// <inheritdoc/>
    private protected override bool HasPendingPages => this.journal is not null;

    /// <inheritdoc/>
    internal override void DisposeManagedResources()
    {
        this.InvalidateAll();
        this.frameGate.Dispose();
        base.DisposeManagedResources();
    }

    /// <inheritdoc/>
    public override ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        => this.ReadPageAsync(pageNumber, PageReadHint.Normal, cancellationToken);

    /// <summary>Reads an owned page, optionally bypassing cache retention.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="hint">The cache hint.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The pooled owned page.</returns>
    internal async ValueTask<byte[]> ReadPageAsync(long pageNumber, PageReadHint hint, CancellationToken cancellationToken = default)
    {
        if (this.cacheSize == 0 || hint == PageReadHint.NoCache)
        {
            return await base.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        }

        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long loadedGeneration;
            lock (this.frameSync)
            {
                loadedGeneration = this.generation;
                if (this.frames.TryGetValue(pageNumber, out Frame? frame))
                {
                    frame.Referenced = true;
                    this.cacheHits++;
                    byte[] copy = ArrayPool<byte>.Shared.Rent(this.PageSize);
                    Buffer.BlockCopy(frame.Bytes, 0, copy, 0, this.PageSize);
                    return copy;
                }
            }

            byte[] page = await base.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            lock (this.frameSync)
            {
                if (loadedGeneration == this.generation)
                {
                    this.RetainFrame(pageNumber, page);
                }
            }

            return page;
        }
        finally
        {
            _ = this.frameGate.Release();
        }
    }

    /// <summary>Discards all frames after rollback, replay failure or truncation.</summary>
    internal void InvalidateAll()
    {
        lock (this.frameSync)
        {
            this.frames.Clear();
            this.clock.Clear();
            this.clockHand = 0;
            this.generation++;
        }
    }

    private async ValueTask WriteCoreAsync(long pageNumber, byte[] page, CancellationToken cancellationToken)
    {
        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        bool pending;
        long writeGeneration;
        try
        {
            pending = this.journal is not null;
            this.journal?.Write(pageNumber, page.AsSpan(0, this.PageSize));
            lock (this.frameSync)
            {
                writeGeneration = this.generation;
            }
        }
        finally
        {
            _ = this.IoGate.Release();
        }

        if (!pending)
        {
            byte[] encoded = this.PrepareEncryptedPageForWrite(pageNumber, page);
            try
            {
                await this.Store.WriteAsync(pageNumber * this.PageSize, encoded.AsMemory(0, this.PageSize), cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                this.InvalidateAll();
                throw;
            }
        }

        lock (this.frameSync)
        {
            if (writeGeneration == this.generation)
            {
                this.RetainFrame(pageNumber, page);
            }
        }
    }

    private void RetainFrame(long pageNumber, byte[] page)
    {
        if (this.cacheSize == 0)
        {
            return;
        }

        byte[] bytes = new byte[this.PageSize];
        Buffer.BlockCopy(page, 0, bytes, 0, this.PageSize);
        if (this.frames.ContainsKey(pageNumber))
        {
            this.frames[pageNumber] = new Frame(bytes);
            return;
        }

        if (this.frames.Count == this.cacheSize)
        {
            while (this.frames[this.clock[this.clockHand]].Referenced)
            {
                this.frames[this.clock[this.clockHand]].Referenced = false;
                this.clockHand = (this.clockHand + 1) % this.clock.Count;
            }

            _ = this.frames.Remove(this.clock[this.clockHand]);
            this.clock[this.clockHand] = pageNumber;
            this.clockHand = (this.clockHand + 1) % this.clock.Count;
            this.evictions++;
        }
        else
        {
            this.clock.Add(pageNumber);
        }

        this.frames.Add(pageNumber, new Frame(bytes));
    }

    private sealed class Frame(byte[] bytes)
    {
        internal byte[] Bytes { get; } = bytes;

        internal bool Referenced { get; set; } = true;
    }

    /// <summary>
    /// A lease on the pager's I/O gate, taken by
    /// <see cref="EnterJournalGateAsync"/>, through which the transaction
    /// lifecycle attaches and detaches a journal and captures or restores the
    /// writer's state as one step that no page read or write can interleave
    /// with. Disposing it releases the gate.
    /// </summary>
    internal sealed class JournalGate : IDisposable
    {
        private Pager? owner;

        /// <summary>
        /// Initializes a new instance of the <see cref="JournalGate"/> class
        /// for a pager whose I/O gate the caller has taken.
        /// </summary>
        /// <param name="owner">The pager.</param>
        internal JournalGate(Pager owner) => this.owner = owner;

        /// <summary>Gets the attached journal, or <see langword="null"/>.</summary>
        internal PageJournal? Current => this.Owner.journal;

        /// <summary>
        /// Gets the physical length of the file, which a new journal takes as
        /// its base: its first appended page follows the last physical page.
        /// </summary>
        internal long PhysicalLengthBytes => this.Owner.LengthBytes;

        private Pager Owner => this.owner ?? throw new ObjectDisposedException(nameof(JournalGate));

        /// <summary>Releases the I/O gate. Further calls do nothing.</summary>
        public void Dispose()
        {
            Pager? pager = this.owner;
            if (pager is null)
            {
                return;
            }

            this.owner = null;
            _ = pager.IoGate.Release();
            _ = pager.frameGate.Release();
        }

        /// <summary>Attaches <paramref name="journal"/>, so later writes and appends are buffered in it.</summary>
        /// <param name="journal">The new transaction's journal.</param>
        /// <exception cref="InvalidOperationException">A journal is already attached.</exception>
        internal void Attach(PageJournal journal)
        {
            Pager pager = this.Owner;
            if (pager.journal is not null)
            {
                throw new InvalidOperationException("A transaction journal is already attached to this file.");
            }

            pager.InvalidateAll();
            pager.journal = journal;
        }

        /// <summary>Detaches the journal, so later writes and appends reach the file.</summary>
        internal void Detach()
        {
            this.Owner.journal = null;
            this.Owner.InvalidateAll();
        }
    }
}
