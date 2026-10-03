namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Encryption.Models;
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
    /// <summary>
    /// The journal of the active explicit transaction, or <see langword="null"/>.
    /// Attached and detached only through a <see cref="JournalGate"/>, which
    /// holds the I/O gate, or by <see cref="ForceDetachJournal"/> at dispose.
    /// </summary>
    private PageJournal? journal;

    /// <summary>
    /// Initializes a new instance of the <see cref="Pager"/> class.
    /// </summary>
    /// <param name="stream">An open, readable, writable, seekable stream for the database file.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    /// <param name="pageKeys">The page cipher; the pager owns it and disposes it.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller keeps ownership of <paramref name="stream"/> and it is not disposed.</param>
    /// <param name="ownerType">The public type that owns the file, named by <see cref="ObjectDisposedException"/>s raised after disposal.</param>
    internal Pager(Stream stream, int pageSize, PageDecryptionKeys pageKeys, bool leaveOpen, Type ownerType)
        : base(stream, pageSize, pageKeys, leaveOpen, ownerType)
    {
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
    internal JetByteRangeLock ByteRangeLock { get; set; } = JetByteRangeLock.Disabled;

    /// <summary>Gets a value indicating whether a transaction journal is attached.</summary>
    internal bool IsJournalActive => this.journal is not null;

    /// <inheritdoc/>
    /// <remarks>Positional reads stay off while a journal is attached: a pending page is read from the journal under the I/O gate.</remarks>
    private protected override bool CanReadPositionally => this.journal is null;

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
        cancellationToken.ThrowIfCancellationRequested();

        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.journal is { } active)
            {
                active.Write(pageNumber, page.AsSpan(0, this.PageSize));
                return;
            }

            byte[] toWrite = this.PrepareEncryptedPageForWrite(pageNumber, page);
            IDisposable pageLock = await this.ByteRangeLock.AcquirePageLockAsync(pageNumber, this.PageSize, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = this.Stream.Seek(pageNumber * this.PageSize, SeekOrigin.Begin);
                await this.Stream.WriteAsync(toWrite.AsMemory(0, this.PageSize), cancellationToken).ConfigureAwait(false);
                await this.Stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = this.IoGate.Release();
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
        cancellationToken.ThrowIfCancellationRequested();

        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.journal is { } active)
            {
                return active.Append(page.AsSpan(0, this.PageSize));
            }

            long pageNumber = this.PhysicalPageCount;
            byte[] toWrite = this.PrepareEncryptedPageForWrite(pageNumber, page);
            IDisposable pageLock = await this.ByteRangeLock.AcquirePageLockAsync(pageNumber, this.PageSize, cancellationToken).ConfigureAwait(false);
            try
            {
                _ = this.Stream.Seek(pageNumber * this.PageSize, SeekOrigin.Begin);
                await this.Stream.WriteAsync(toWrite.AsMemory(0, this.PageSize), cancellationToken).ConfigureAwait(false);
                await this.Stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                return pageNumber;
            }
            finally
            {
                pageLock.Dispose();
            }
        }
        finally
        {
            _ = this.IoGate.Release();
        }
    }

    /// <summary>Sets the length of the backing stream and flushes it.</summary>
    /// <param name="length">The new length in bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the stream is resized and flushed.</returns>
    internal async ValueTask SetLengthAsync(long length, CancellationToken cancellationToken)
    {
        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.Stream.SetLength(length);
            await this.Stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = this.IoGate.Release();
        }
    }

    /// <summary>
    /// Flushes the backing stream; with <paramref name="toDisk"/> a
    /// <see cref="FileStream"/> is flushed through the OS cache to the device.
    /// </summary>
    /// <param name="toDisk">Whether to flush a file through to the device.</param>
    /// <param name="cancellationToken">A token used to cancel the flush.</param>
    /// <returns>A task that completes when the stream is flushed.</returns>
    internal async ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken)
    {
        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (toDisk && this.Stream is FileStream fileStream)
            {
#pragma warning disable CA1849
                fileStream.Flush(flushToDisk: true);
#pragma warning restore CA1849
            }
            else
            {
                await this.Stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = this.IoGate.Release();
        }
    }

    /// <summary>
    /// Takes the I/O gate and returns a lease through which a transaction
    /// attaches or detaches its journal. Dispose the lease to release the
    /// gate. Never hold it across a page read or write call: those take the
    /// gate themselves, and it is not reentrant, so the call would wait on
    /// itself forever.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the wait for the gate.</param>
    /// <returns>The lease, holding the gate.</returns>
    internal async ValueTask<JournalGate> EnterJournalGateAsync(CancellationToken cancellationToken)
    {
        await this.IoGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new JournalGate(this);
    }

    /// <summary>
    /// Detaches any journal without taking the I/O gate. Only the writer's
    /// dispose path calls it, after the transaction's own rollback, which
    /// detaches under the gate, has run or failed.
    /// </summary>
    internal void ForceDetachJournal() => this.journal = null;

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
        if (pageNumber < 1 || !EncryptionManager.HasPageEncryption(this.PageKeys))
        {
            return page;
        }

        byte[] copy = new byte[this.PageSize];
        Buffer.BlockCopy(page, 0, copy, 0, this.PageSize);
        EncryptionManager.EncryptPageInPlace(copy, pageNumber, this.PageSize, this.PageKeys);
        return copy;
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

            pager.journal = journal;
        }

        /// <summary>Detaches the journal, so later writes and appends reach the file.</summary>
        internal void Detach() => this.Owner.journal = null;
    }
}
