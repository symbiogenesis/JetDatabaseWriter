namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
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
    private readonly SortedDictionary<long, byte[]> dirtyPages = [];
    private readonly SortedSet<long> zeroReservations = [];
    private IPageWriteObserver[] observers = [];
    private PagerTransaction? journal;
    private int scopeDepth;
    private bool scopeHasWrites;
    private long bufferedPageCount;
    private int clockHand;
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
        this.ByteRangeLock = JetByteRangeLock.Disabled;
    }

    /// <summary>
    /// Gets the end of file in pages: one past the highest page number that
    /// can be read. Inside a transaction this includes the pages the journal
    /// has appended past the physical end of file, so it is the next page
    /// number <see cref="AppendPageAsync"/> assigns. Every page-number bounds
    /// check and every caller that numbers pages before appending them uses
    /// this count.
    /// </summary>
    public override long PageCount => this.journal?.NextAppendPageNumber ?? Math.Max(this.PhysicalPageCount, this.bufferedPageCount);

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
            if (this.Store is StreamPageStore streamStore)
            {
                streamStore.AcquireWriteLock = value.IsEnabled ? value.AcquirePageLockAsync : null;
            }
        }
    }

    /// <summary>Gets a value indicating whether commit undo failed and mutations must stop.</summary>
    internal bool IsFaulted { get; private set; }

    /// <summary>Gets a value indicating whether a transaction journal is attached.</summary>
    internal bool IsJournalActive => this.journal is not null;

    /// <summary>Gets the invalidation epoch for derived state.</summary>
    internal long InvalidationGeneration { get; private set; }

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
            PagerTransaction? active;
            long appended = -1;
            await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                active = this.journal;
                if (active is not null)
                {
                    appended = active.Append(page.AsSpan(0, this.PageSize));
                    lock (this.frameSync)
                    {
                        this.RetainFrame(appended, page);
                    }
                }
            }
            finally
            {
                _ = this.IoGate.Release();
            }

            if (active is not null)
            {
                foreach (IPageWriteObserver observer in this.observers)
                {
                    observer.OnPageWritten(appended, default, page.AsSpan(0, this.PageSize));
                }

                await this.SpillStatementAsync(active, cancellationToken).ConfigureAwait(false);
                return appended;
            }

            long pageNumber = this.PageCount;
            await this.WriteCoreAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            return pageNumber;
        }
        finally
        {
            _ = this.frameGate.Release();
        }
    }

    /// <summary>Reserves a readable zero page without writing a provisional image.</summary>
    /// <param name="cancellationToken">Cancellation before reservation.</param>
    /// <param name="existingPageNumber">An existing free page to reserve, or null to append.</param>
    /// <returns>The reserved logical page number.</returns>
    internal async ValueTask<long> ReserveZeroedPageAsync(CancellationToken cancellationToken, long? existingPageNumber = null)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.ThrowIfDisposed();
            long pageNumber = this.journal is { } active
                ? active.ReserveZeroedPage(existingPageNumber) : existingPageNumber ?? this.PageCount;

            lock (this.frameSync)
            {
                if (this.frames.TryGetValue(pageNumber, out Frame? frame))
                {
                    Array.Clear(frame.Bytes, 0, this.PageSize);
                }
            }

            if (this.journal is not null)
            {
                return pageNumber;
            }

            _ = this.zeroReservations.Add(pageNumber);
            this.bufferedPageCount = Math.Max(this.PageCount, checked(pageNumber + 1));
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
            await this.DrainDirtyPagesAsync().ConfigureAwait(false);
            this.InvalidateAll();
            await this.Store.SetLengthAsync(length, cancellationToken).ConfigureAwait(false);
            this.zeroReservations.RemoveWhere(pageNumber => pageNumber >= length / this.PageSize);
            this.scopeHasWrites = true;
            this.bufferedPageCount = this.zeroReservations.Count == 0 ? this.PhysicalPageCount : Math.Max(this.PhysicalPageCount, checked(this.zeroReservations.Max + 1));
        }
        finally
        {
            this.InvalidateAll();
            _ = this.frameGate.Release();
        }
    }

    /// <summary>Preserves raw tail images until secure erase, truncation and flush succeed.</summary>
    /// <param name="length">The retained physical length.</param>
    /// <param name="secure">Whether freed pages must be erased before truncation.</param>
    /// <param name="cancellationToken">Cancellation before and during preparation.</param>
    /// <returns>The asynchronous completion.</returns>
    /// <exception cref="AggregateException">Both shrink and restoration failed.</exception>
    internal async ValueTask ShrinkTailAsync(long length, bool secure, CancellationToken cancellationToken)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        StatementUndoLog? undo = null;
        try
        {
            await this.DrainDirtyPagesAsync().ConfigureAwait(false);
            long originalLength = this.Store.Length;
            undo = new StatementUndoLog(originalLength, fileBacked: true, Math.Max(64, this.cacheSize / 2));
            for (long offset = length; offset < originalLength; offset += this.PageSize)
            {
                await undo.CaptureAsync(this.Store, offset, this.PageSize, cancellationToken).ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            this.InvalidateAll();
            undo.HasWrites = true;
            if (secure)
            {
                byte[] freed = new byte[this.PageSize];
                freed[0] = Constants.PageTypes.Freed;
                freed[1] = 0x01;
                System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(freed.AsSpan(2), checked((ushort)(this.PageSize - 16)));
                for (long offset = length; offset < originalLength; offset += this.PageSize)
                {
                    byte[] encoded = this.PrepareEncryptedPageForWrite(offset / this.PageSize, freed);
                    await this.Store.WriteAsync(offset, encoded, CancellationToken.None).ConfigureAwait(false);
                }

                await this.Store.FlushAsync(true, CancellationToken.None).ConfigureAwait(false);
            }

            await this.Store.SetLengthAsync(length, CancellationToken.None).ConfigureAwait(false);
            await this.Store.FlushAsync(true, CancellationToken.None).ConfigureAwait(false);
            undo.HasWrites = false;
            this.zeroReservations.RemoveWhere(pageNumber => pageNumber >= length / this.PageSize);
            this.bufferedPageCount = this.zeroReservations.Count == 0 ? this.PhysicalPageCount : Math.Max(this.PhysicalPageCount, checked(this.zeroReservations.Max + 1));
        }
        catch (Exception failure)
        {
            if (undo?.HasWrites == true)
            {
                try
                {
                    await undo.RestoreAsync(this.Store, durable: true).ConfigureAwait(false);
                }
                catch (Exception undoFailure) when (undoFailure is IOException or UnauthorizedAccessException or ObjectDisposedException or NotSupportedException)
                {
                    this.IsFaulted = true;
                    throw new AggregateException("Physical shrink failed and its original database image could not be restored.", failure, undoFailure);
                }
            }

            throw;
        }
        finally
        {
            try
            {
                if (undo is not null)
                {
                    await undo.DisposeAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                this.InvalidateAll();
                _ = this.frameGate.Release();
            }
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
    internal async ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.DrainDirtyPagesAsync().ConfigureAwait(false);
            await this.Store.FlushAsync(toDisk, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = this.frameGate.Release();
        }
    }

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
        if (this.zeroReservations.Contains(pageNumber) || this.journal?.IsZeroReservation(pageNumber) == true)
        {
            Array.Clear(buffer, 0, this.PageSize);
            return true;
        }

        byte[]? pending = this.journal?.TryGet(pageNumber);
        if (pending is null)
        {
            _ = this.dirtyPages.TryGetValue(pageNumber, out pending);
        }

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
    private protected override bool HasPendingPages => this.journal is not null || this.dirtyPages.Count != 0 || this.zeroReservations.Count != 0;

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
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.cacheSize == 0 || hint == PageReadHint.NoCache)
            {
                return await base.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }

            long loadedGeneration;
            lock (this.frameSync)
            {
                loadedGeneration = this.InvalidationGeneration;
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
                if (loadedGeneration == this.InvalidationGeneration)
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
            this.InvalidationGeneration++;
            foreach (IPageWriteObserver observer in this.observers)
            {
                observer.OnInvalidateAll();
            }
        }
    }

    private ValueTask<byte[]> ReadBeforeWriteAsync(long pageNumber, CancellationToken cancellationToken)
    {
        lock (this.frameSync)
        {
            if (this.frames.TryGetValue(pageNumber, out Frame? frame))
            {
                byte[] copy = ArrayPool<byte>.Shared.Rent(this.PageSize);
                Buffer.BlockCopy(frame.Bytes, 0, copy, 0, this.PageSize);
                return new ValueTask<byte[]>(copy);
            }
        }

        return base.ReadPageAsync(pageNumber, cancellationToken);
    }

    private async ValueTask WriteCoreAsync(long pageNumber, byte[] page, CancellationToken cancellationToken)
    {
        byte[]? before = this.observers.Length != 0 && pageNumber < this.PageCount
            ? await this.ReadBeforeWriteAsync(pageNumber, cancellationToken).ConfigureAwait(false) : null;
        try
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
                    writeGeneration = this.InvalidationGeneration;
                }
            }
            finally
            {
                _ = this.IoGate.Release();
            }

            if (this.journal is { } statement)
            {
                await this.SpillStatementAsync(statement, cancellationToken).ConfigureAwait(false);
            }

            if (!pending && this.scopeDepth != 0)
            {
                this.scopeHasWrites = true;
                if (this.dirtyPages.TryGetValue(pageNumber, out byte[]? dirty))
                {
                    Buffer.BlockCopy(page, 0, dirty, 0, this.PageSize);
                }
                else
                {
                    this.dirtyPages.Add(pageNumber, page.AsSpan(0, this.PageSize).ToArray());
                }

                this.bufferedPageCount = Math.Max(this.PageCount, pageNumber + 1);
                pending = true;
                if (this.dirtyPages.Count >= Math.Max(64, this.cacheSize / 2))
                {
                    await this.DrainDirtyPagesAsync().ConfigureAwait(false);
                }
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

            _ = this.zeroReservations.Remove(pageNumber);

            lock (this.frameSync)
            {
                if (writeGeneration == this.InvalidationGeneration)
                {
                    this.RetainFrame(pageNumber, page);
                }
            }

            foreach (IPageWriteObserver observer in this.observers)
            {
                observer.OnPageWritten(pageNumber, before is null ? default : before.AsSpan(0, this.PageSize), page.AsSpan(0, this.PageSize));
            }
        }
        finally
        {
            if (before is not null)
            {
                PageBuffers.Return(before);
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

    /// <summary>Flushes outstanding call pages before disposal, with no extra flush after a completed replay.</summary>
    /// <returns>The completion.</returns>
    internal async ValueTask FlushPendingWritesAsync()
    {
        await this.frameGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (this.dirtyPages.Count == 0)
            {
                return;
            }

            await this.DrainDirtyPagesAsync().ConfigureAwait(false);
            await this.Store.FlushAsync(false, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _ = this.frameGate.Release();
        }
    }

    /// <summary>Replays a detached transaction without stealing pending frames.</summary>
    /// <param name="transaction">The detached transaction.</param>
    /// <param name="beforeFirstWrite">The last preparation before replay.</param>
    /// <param name="cancellationToken">Cancellation before the first write.</param>
    /// <param name="durable">Whether successful replay and undo request a device flush.</param>
    /// <returns>The completion.</returns>
    /// <exception cref="AggregateException">Commit replay and restoration both failed.</exception>
    internal async ValueTask CommitAsync(PagerTransaction transaction, Action beforeFirstWrite, CancellationToken cancellationToken, bool durable = true)
    {
        await this.frameGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        StatementUndoLog? image = null;
        bool recordingCommitDecision = false;
        try
        {
            image = transaction.UndoLog ??= new StatementUndoLog(transaction.BaseFileLengthBytes, this.Store.Capabilities.IsFileBacked && this.Store.Capabilities.InPlacePages, Math.Max(64, this.cacheSize / 2));
            foreach (KeyValuePair<long, byte[]> entry in transaction.EnumerateInOrder())
            {
                await image.CaptureAsync(this.Store, checked(entry.Key * this.PageSize), this.PageSize, cancellationToken).ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            beforeFirstWrite();
            this.EnsureRecoveryJournal(transaction);
            image.HasWrites = true;
            foreach (KeyValuePair<long, byte[]> entry in transaction.EnumerateInOrder())
            {
                byte[] encoded = this.PrepareEncryptedPageForWrite(entry.Key, entry.Value);
                await this.Store.WriteAsync(checked(entry.Key * this.PageSize), encoded.AsMemory(0, this.PageSize), CancellationToken.None).ConfigureAwait(false);
            }

            await this.Store.FlushAsync(durable || transaction.RecoveryJournal is not null, CancellationToken.None).ConfigureAwait(false);
            recordingCommitDecision = transaction.RecoveryJournal is not null;
            transaction.RecoveryJournal?.MarkCommitted();
            image.HasWrites = false;
        }
        catch (Exception failure)
        {
            if (recordingCommitDecision)
            {
                // A failed marker flush has an uncertain durable outcome. Preserve
                // the database and sidecar so reopen can resolve that decision.
                this.IsFaulted = true;
                this.InvalidateAll();
                throw;
            }

            try
            {
                if (image?.HasWrites == true)
                {
                    this.DetachRecoveryJournal();
                    await image.RestoreAsync(this.Store, durable || transaction.RecoveryJournal is not null).ConfigureAwait(false);
                    transaction.RecoveryJournal?.CompleteRollback();
                }
            }
            catch (Exception undoFailure) when (undoFailure is IOException or UnauthorizedAccessException or ObjectDisposedException or NotSupportedException)
            {
                this.IsFaulted = true;
                throw new AggregateException("Commit failed and its original database image could not be restored.", failure, undoFailure);
            }
            finally
            {
                this.InvalidateAll();
            }

            throw;
        }
        finally
        {
            try
            {
                if (image is not null)
                {
                    await image.DisposeAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                try
                {
                    this.CloseRecoveryJournal(transaction);
                }
                finally
                {
                    transaction.UndoLog = null;
                    _ = this.frameGate.Release();
                }
            }
        }
    }

    /// <summary>Restores early statement writes before discarding their journal.</summary>
    /// <param name="transaction">The journal being discarded.</param>
    /// <returns>The asynchronous restoration.</returns>
    internal async ValueTask RollbackStatementAsync(PagerTransaction transaction)
    {
        if (transaction.UndoLog is not { } undo)
        {
            return;
        }

        await this.frameGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (undo.HasWrites)
            {
                this.DetachRecoveryJournal();
                await undo.RestoreAsync(this.Store, transaction.RecoveryJournal is not null).ConfigureAwait(false);
                transaction.RecoveryJournal?.CompleteRollback();
            }
        }
        catch
        {
            this.IsFaulted = true;
            throw;
        }
        finally
        {
            try
            {
                await undo.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    this.CloseRecoveryJournal(transaction);
                }
                finally
                {
                    transaction.UndoLog = null;
                    this.InvalidateAll();
                    _ = this.frameGate.Release();
                }
            }
        }
    }

    private async ValueTask SpillStatementAsync(PagerTransaction transaction, CancellationToken cancellationToken)
    {
        if (!transaction.IsStatement || transaction.Count < Math.Max(64, this.cacheSize / 2))
        {
            return;
        }

        StatementUndoLog undo = transaction.UndoLog ??= new StatementUndoLog(transaction.BaseFileLengthBytes, this.Store.Capabilities.IsFileBacked && this.Store.Capabilities.InPlacePages, Math.Max(64, this.cacheSize / 2));
        foreach (KeyValuePair<long, byte[]> entry in transaction.EnumerateInOrder())
        {
            await undo.CaptureAsync(this.Store, checked(entry.Key * this.PageSize), this.PageSize, cancellationToken).ConfigureAwait(false);
        }

        if (!transaction.SpillCommitLockAcquired)
        {
            transaction.SpillCommitLockOffset = await this.ByteRangeLock.AcquireCommitLockOffsetAsync(transaction.CommitLockAddress, cancellationToken).ConfigureAwait(false);
            transaction.SpillCommitLockAcquired = true;
        }

        cancellationToken.ThrowIfCancellationRequested();
        this.EnsureRecoveryJournal(transaction);
        undo.HasWrites = true;
        foreach (KeyValuePair<long, byte[]> entry in transaction.EnumerateInOrder())
        {
            byte[] encoded = this.PrepareEncryptedPageForWrite(entry.Key, entry.Value);
            await this.Store.WriteAsync(checked(entry.Key * this.PageSize), encoded.AsMemory(0, this.PageSize), CancellationToken.None).ConfigureAwait(false);
        }

        transaction.DiscardReplayImages();
    }

    private void EnsureRecoveryJournal(PagerTransaction transaction)
    {
        if (this.Store is StreamPageStore { Stream: FileStream database } store)
        {
            try
            {
                transaction.RecoveryJournal ??= PersistentRollbackJournal.Create(database, this.PageSize);
                store.RollbackJournal = transaction.RecoveryJournal;
            }
            catch
            {
                // Failed creation can leave incomplete recovery evidence. Exclude
                // further writes rather than replace a journal of uncertain state.
                this.IsFaulted = true;
                throw;
            }
        }
    }

    private void DetachRecoveryJournal()
    {
        if (this.Store is StreamPageStore store)
        {
            store.RollbackJournal = null;
        }
    }

    private void CloseRecoveryJournal(PagerTransaction transaction)
    {
        if (this.Store is StreamPageStore store)
        {
            store.RollbackJournal = null;
        }

        transaction.RecoveryJournal?.Dispose();
        transaction.RecoveryJournal = null;
    }

    /// <summary>Rejects mutations after an unsuccessful commit undo.</summary>
    /// <exception cref="JetOperationException">The writer could not restore a failed commit.</exception>
    internal void ThrowIfFaulted()
    {
        if (this.IsFaulted)
        {
            throw JetErrors.Operation(JetErrorCode.WriterFaulted, "The writer is faulted because a failed commit could not restore the original database image. Dispose it before reopening the database.");
        }
    }

    /// <summary>Begins a nested write-back scope.</summary>
    /// <returns>The scope whose final disposal flushes pending writes.</returns>
    internal WriteScope BeginWriteScope()
    {
        this.ThrowIfDisposed();
        if (this.scopeDepth == 0)
        {
            this.scopeHasWrites = false;
        }

        this.scopeDepth++;
        return new WriteScope(this);
    }

    /// <summary>Registers a page image observer.</summary>
    /// <param name="observer">The observer.</param>
    internal void AddWriteObserver(IPageWriteObserver observer)
    {
        lock (this.frameSync)
        {
            this.observers = [.. this.observers, observer];
        }
    }

    /// <summary>Unregisters a page image observer.</summary>
    /// <param name="observer">The observer.</param>
    internal void RemoveWriteObserver(IPageWriteObserver observer)
    {
        lock (this.frameSync)
        {
            this.observers = Array.FindAll(this.observers, item => !ReferenceEquals(item, observer));
        }
    }

    /// <summary>Ends one scope, flushing only the outermost scope.</summary>
    /// <returns>The completion.</returns>
    internal async ValueTask EndWriteScopeAsync()
    {
        if (--this.scopeDepth != 0 || this.journal is not null || !this.scopeHasWrites)
        {
            return;
        }

        await this.frameGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            await this.DrainDirtyPagesAsync().ConfigureAwait(false);
            await this.Store.FlushAsync(false, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            this.InvalidateAll();
            throw;
        }
        finally
        {
            _ = this.frameGate.Release();
        }
    }

    private async ValueTask DrainDirtyPagesAsync()
    {
        try
        {
            foreach (KeyValuePair<long, byte[]> entry in this.dirtyPages)
            {
                byte[] encoded = this.PrepareEncryptedPageForWrite(entry.Key, entry.Value);
                await this.Store.WriteAsync(checked(entry.Key * this.PageSize), encoded.AsMemory(0, this.PageSize), CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch
        {
            this.InvalidateAll();
            throw;
        }
        finally
        {
            this.dirtyPages.Clear();
            this.bufferedPageCount = this.zeroReservations.Count == 0 ? this.PhysicalPageCount : Math.Max(this.PhysicalPageCount, checked(this.zeroReservations.Max + 1));
        }
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
        internal PagerTransaction? Current => this.Owner.journal;

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
        /// <param name="preserveFrames">Whether unchanged physical frames may be retained for a statement.</param>
        /// <exception cref="InvalidOperationException">A journal is already attached.</exception>
        internal void Attach(PagerTransaction journal, bool preserveFrames = false)
        {
            Pager pager = this.Owner;
            if (pager.journal is not null)
            {
                throw new InvalidOperationException("A transaction journal is already attached to this file.");
            }

            if (pager.zeroReservations.Count != 0)
            {
                throw new InvalidOperationException("Initialize or release reserved pages before starting a transaction.");
            }

            if (!preserveFrames)
            {
                pager.InvalidateAll();
            }

            pager.journal = journal;
        }

        /// <summary>Detaches the journal, so later writes and appends reach the file.</summary>
        /// <param name="invalidate">Whether discarded journal images must be evicted. Successful replay retains them.</param>
        internal void Detach(bool invalidate = true)
        {
            this.Owner.journal = null;
            if (invalidate)
            {
                this.Owner.InvalidateAll();
            }
        }
    }
}
