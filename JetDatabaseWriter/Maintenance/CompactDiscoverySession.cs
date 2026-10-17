namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.ValueDecoding;

/// <summary>Composes bounded raw discovery and staging copies across multiple tables, roots and values.</summary>
/// <remarks>
/// The caller keeps the decrypted source immutable and owns its lifetime. This session owns no cache.
/// Root scans run serially; their awaited visitors may audit or copy one long value at a time.
/// No method authorizes relocation, reclamation, repair or publication of a partial destination.
/// Runtime overhead, shared-pool retention and caller-owned buffers, destinations and retained observations are separate from admission.
/// </remarks>
internal sealed class CompactDiscoverySession
{
    private readonly JetFormat format;
    private readonly CompactDiscoveryBudget budget;
    private readonly DiscoveryPageSource pages;
    private int activeScan;
    private int activeValue;

    /// <summary>Initializes a new instance of the <see cref="CompactDiscoverySession"/> class without reading the source.</summary>
    /// <param name="pages">The caller-owned immutable decrypted page source.</param>
    /// <param name="format">The verified source format.</param>
    /// <param name="budget">The immutable whole-session limits.</param>
    /// <exception cref="JetLimitationException">The source physical size exceeds the session limit.</exception>
    /// <exception cref="JetCorruptDataException">The source geometry disagrees with the verified format.</exception>
    internal CompactDiscoverySession(IPageSource pages, JetFormat format, CompactDiscoveryBudget budget)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        Guard.InRange(budget.MaxPages, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxValueBytes, 0, LongValueDescriptor.MaxLength, nameof(budget));
        Guard.InRange(budget.MaxChainRows, 0, int.MaxValue, nameof(budget));
        this.Accounting = new CompactDiscoveryAccounting(budget);
        this.format = format;
        this.budget = budget;
        pages.ThrowIfDisposed();
        int pageSize = pages.PageSize;
        long pageCount = pages.PageCount;
        if (pageCount > budget.MaxPages)
        {
            throw Limit("The source physical size exceeds MaxPages.");
        }

        if (pageSize != format.PageSize || pageCount < 3)
        {
            throw Corrupt(0, "The discovery source geometry does not match the verified format.");
        }

        this.pages = new DiscoveryPageSource(pages, this.Accounting, pageSize, pageCount);
        this.pages.CheckGeometry(0, default);
    }

    /// <summary>Gets the session's cumulative work and current storage admission.</summary>
    internal CompactDiscoveryAccounting Accounting { get; }

    /// <summary>Streams one table's raw rows using allowances retained across every invocation.</summary>
    /// <param name="ownerTDefPage">The expected table owner.</param>
    /// <param name="visitor">The awaited borrowed-row consumer, which may copy or audit a long value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after this table's raw scan.</returns>
    internal async ValueTask ScanRowsAsync(long ownerTDefPage, Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.EnterScan(cancellationToken);
        try
        {
            this.Accounting.Charge(CompactDiscoveryCharge.Objects, 1, ownerTDefPage);
            await CompactRawRowReader.ScanAsync(
                this.pages,
                this.format,
                ownerTDefPage,
                new()
                {
                    Accounting = this.Accounting,
                    MaxPages = this.budget.MaxPages,
                    MaxPageReads = this.budget.MaxPageReads,
                    MaxRows = this.budget.MaxRows,
                    MaxDirectorySlots = this.budget.MaxDirectorySlots,
                    MaxWorkingBytes = this.budget.MaxWorkingBytes,
                },
                visitor,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Audits one index root using allowances retained across every invocation.</summary>
    /// <param name="ownerTDefPage">The expected table owner.</param>
    /// <param name="rootPage">The native index root.</param>
    /// <param name="visitor">The awaited observation consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after this root's raw audit.</returns>
    internal async ValueTask ScanIndexAsync(long ownerTDefPage, long rootPage, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.EnterScan(cancellationToken);
        try
        {
            this.Accounting.Charge(CompactDiscoveryCharge.Objects, 1, rootPage);
            await CompactIndexAudit.ScanAsync(
                this.pages,
                this.format,
                ownerTDefPage,
                rootPage,
                new()
                {
                    Accounting = this.Accounting,
                    MaxTreePages = checked((int)Math.Min(int.MaxValue, this.budget.MaxIndexPages)),
                    MaxEntries = this.budget.MaxIndexEntries,
                    MaxCanonicalBytes = this.budget.MaxIndexCanonicalBytes,
                    MaxReferences = this.budget.MaxReferences,
                    MaxGraphSteps = this.budget.MaxGraphSteps,
                    MaxRecords = this.budget.MaxRecords,
                    MaxWorkingBytes = this.budget.MaxWorkingBytes,
                },
                visitor,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeScan, 0);
        }
    }

    /// <summary>Audits complete stored long-value regions without interpreting their payload.</summary>
    /// <param name="column">The borrowed original descriptor and column bytes.</param>
    /// <param name="visitor">The awaited borrowed-observation consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after this value's raw audit.</returns>
    internal async ValueTask AuditLongValueAsync(ReadOnlyMemory<byte> column, Func<CompactLongValueRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        this.EnterValue(cancellationToken);
        try
        {
            await CompactLongValueAudit.AuditAsync(
                this.pages,
                this.format,
                column,
                new()
                {
                    MaxPages = this.budget.MaxPages,
                    MaxPageReads = this.budget.MaxPageReads,
                    MaxChainRows = this.budget.MaxChainRows,
                    MaxDirectorySlots = this.budget.MaxDirectorySlots,
                    MaxRawBytes = this.budget.MaxLongValueBytes,
                    MaxWorkingBytes = this.budget.MaxWorkingBytes,
                },
                visitor,
                cancellationToken,
                this.Accounting).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref this.activeValue, 0);
        }
    }

    /// <summary>Streams a declared raw payload into a private destination using cumulative byte, row and read limits.</summary>
    /// <param name="column">The borrowed original descriptor and column bytes.</param>
    /// <param name="destination">The caller-owned staging destination, left open and unflushed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completing after the declared payload is written.</returns>
    /// <remarks>Payload bytes are charged before copying, including bytes of an interrupted copy. Complete row bytes, trailing content and reference ownership require auditing separately.</remarks>
    /// <exception cref="InvalidDataException">The original column has a truncated descriptor or unreadable payload.</exception>
    /// <exception cref="ArgumentException">The destination is not writable.</exception>
    internal async ValueTask CopyLongValueAsync(ReadOnlyMemory<byte> column, Stream destination, CancellationToken cancellationToken)
    {
        Guard.NotNull(destination, nameof(destination));
        this.EnterValue(cancellationToken);
        try
        {
            if (!LongValueDescriptor.TryRead(column.Span, out LongValueDescriptor descriptor))
            {
                throw new InvalidDataException("The raw long-value column is shorter than its descriptor.");
            }

            ReadOnlyMemory<byte> inline = column[Constants.LongValue.HeaderSize..];
            LongValueStore.ValidatePayload(descriptor, inline.Length, this.format.PageSize, this.budget.MaxValueBytes, this.budget.MaxChainRows);
            if (!destination.CanWrite)
            {
                throw new ArgumentException("The raw long-value staging destination must be writable.", nameof(destination));
            }

            this.Accounting.Charge(CompactDiscoveryCharge.LongValues, 1);
            this.Accounting.Charge(CompactDiscoveryCharge.LongValuePayloadBytes, descriptor.Length);
            int capacity = (this.format.PageSize - this.format.DataPage.RowsStart) / 2;
            long storage = 65_536L + (2L * this.format.PageSize) + (32L * capacity) + (descriptor.IsInline ? 0 : 64L * this.budget.MaxChainRows);
            using IDisposable admission = this.Accounting.ReserveWorkingBytes(storage);
            using var cache = new ReaderPageCache(this.format, new CopyPageSource(this.pages, this.format, this.Accounting), 0);
            var decoder = new LongValueDecoder(this.format, cache, this.budget.MaxValueBytes);
            await LongValueStore.CopyPayloadAsync(
                descriptor,
                inline,
                destination,
                this.format.PageSize,
                this.budget.MaxValueBytes,
                this.budget.MaxChainRows,
                LocateAsync,
                cancellationToken,
                cache.ReturnUncachedPage).ConfigureAwait(false);
            this.pages.CheckGeometry(0, cancellationToken);

            ValueTask<LvalRowLocation> LocateAsync(uint pointer, CancellationToken token)
            {
                this.Accounting.Charge(CompactDiscoveryCharge.References, 1, LongValueStore.PageNumber(pointer));
                this.Accounting.Charge(CompactDiscoveryCharge.LongValueRows, 1, LongValueStore.PageNumber(pointer));
                this.Accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, LongValueStore.PageNumber(pointer));
                return decoder.LocateLvalRowAsync(pointer, token);
            }
        }
        finally
        {
            Volatile.Write(ref this.activeValue, 0);
        }
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private void EnterScan(CancellationToken cancellationToken)
    {
        this.pages.CheckGeometry(0, cancellationToken);
        if (Interlocked.CompareExchange(ref this.activeScan, 1, 0) != 0)
        {
            throw new InvalidOperationException("A compact discovery root scan is already active.");
        }
    }

    private void EnterValue(CancellationToken cancellationToken)
    {
        this.pages.CheckGeometry(0, cancellationToken);
        if (Interlocked.CompareExchange(ref this.activeValue, 1, 0) != 0)
        {
            throw new InvalidOperationException("A compact discovery long-value operation is already active.");
        }
    }

    private sealed class DiscoveryPageSource(IPageSource inner, CompactDiscoveryAccounting accounting, int pageSize, long pageCount) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount => pageCount;

        public bool IsDisposed => inner.IsDisposed;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            this.CheckGeometry(pageNumber, cancellationToken);
            if (pageNumber < 0 || pageNumber >= this.PageCount)
            {
                throw Corrupt(pageNumber, "The discovery page reference is outside the source.");
            }

            accounting.Charge(CompactDiscoveryCharge.PageReads, 1, pageNumber);
            byte[] page = await inner.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                this.CheckGeometry(pageNumber, cancellationToken);
                if (page.Length < this.PageSize)
                {
                    throw Corrupt(pageNumber, "The discovery source returned a truncated physical page.");
                }

                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
        {
            inner.ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
        }

        internal void CheckGeometry(long page, CancellationToken cancellationToken)
        {
            this.ThrowIfDisposedOrCancelled(cancellationToken);
            if (inner.PageSize != this.PageSize || inner.PageCount != this.PageCount)
            {
                throw Corrupt(page, "The source geometry changed during discovery.");
            }
        }
    }

    private sealed class CopyPageSource(DiscoveryPageSource pages, JetFormat format, CompactDiscoveryAccounting accounting) : IPageSource
    {
        public int PageSize => pages.PageSize;

        public long PageCount => pages.PageCount;

        public bool IsDisposed => pages.IsDisposed;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (!LongValueStore.IsLvalPage(page))
                {
                    throw Corrupt(pageNumber, "A raw long-value copy reference does not target an LVAL data page.");
                }

                int count = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2));
                if (page[0] == Constants.PageTypes.Data && count <= (format.PageSize - format.DataPage.RowsStart) / 2)
                {
                    accounting.Charge(CompactDiscoveryCharge.DirectorySlots, count, pageNumber);
                }

                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        public void ThrowIfDisposed() => pages.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => pages.ThrowIfDisposedOrCancelled(cancellationToken);
    }
}
