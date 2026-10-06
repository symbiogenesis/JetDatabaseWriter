namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Journal of dirty pages produced inside a private statement or explicit
/// <see cref="JetTransaction"/>. Each entry is the page's new contents (an
/// after-image), buffered in plaintext until commit or a bounded statement spill. At
/// <c>CommitAsync</c> the entries are written over the file in place, page by
/// page; <c>RollbackAsync</c> / dispose discards buffered pages and restores spills.
/// </summary>
/// <remarks>
/// <para>
/// Savepoint prior images rewind the in-memory journal. Commit captures raw
/// before-images separately and restores them after a write or flush failure.
/// File-backed stores spill raw undo records into a DeleteOnClose temporary log.
/// This log is not a crash-recovery journal: a process crash can still leave
/// part of a statement or transaction in the file, with no recovery pass.
/// </para>
/// <para>
/// The journal stores **plaintext** page bytes. Page-level encryption is applied
/// at commit time by <c>Pager.PrepareEncryptedPageForWrite</c>
/// — buffering encrypted bytes would make repeated writes to the same page
/// (a common pattern inside large multi-row inserts) needlessly re-encrypt.
/// </para>
/// <para>
/// Not thread-safe. Callers serialize access via the writer's I/O gate.
/// </para>
/// </remarks>
internal sealed class PagerTransaction
{
    private readonly SortedDictionary<long, byte[]> pages = [];
    private readonly HashSet<long> zeroReservations = [];
    private readonly int pageSize;
    private readonly int maxPages;
    private readonly Stack<SavepointFrame> savepoints = [];
    private long appendedCount;

    public PagerTransaction(long baseFileLengthBytes, int pageSize, int maxPages, bool statement = false)
    {
        Guard.Positive(pageSize, nameof(pageSize));
        Guard.Positive(maxPages, nameof(maxPages));

        this.BaseFileLengthBytes = baseFileLengthBytes;
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.IsStatement = statement;
    }

    /// <summary>Gets the file length captured when the transaction began.</summary>
    public long BaseFileLengthBytes { get; }

    /// <summary>Gets the number of distinct pages currently buffered in the journal.</summary>
    public int Count => this.pages.Count;

    /// <summary>Gets a value indicating whether dirty frames may spill during a private statement.</summary>
    internal bool IsStatement { get; }

    /// <summary>Gets or sets first raw images retained across statement spills.</summary>
    internal StatementUndoLog? UndoLog { get; set; }

    /// <summary>Gets or sets the commit lock address for early statement writes.</summary>
    internal long CommitLockAddress { get; set; }

    /// <summary>Gets or sets the commit sentinel held across early writes.</summary>
    internal long? SpillCommitLockOffset { get; set; }

    /// <summary>Gets or sets a value indicating whether the early write lock was acquired.</summary>
    internal bool SpillCommitLockAcquired { get; set; }

    /// <summary>Discards successfully spilled images while retaining provisional zeros.</summary>
    internal void DiscardReplayImages()
    {
        foreach (long pageNumber in new List<long>(this.pages.Keys))
        {
            if (!this.zeroReservations.Contains(pageNumber))
            {
                _ = this.pages.Remove(pageNumber);
            }
        }
    }

    /// <summary>
    /// Gets the page number that the next <see cref="Append"/> call will assign,
    /// computed as <c>(BaseFileLengthBytes / pageSize) + appendedCount</c>.
    /// </summary>
    public long NextAppendPageNumber => (this.BaseFileLengthBytes / this.pageSize) + this.appendedCount;

    /// <summary>
    /// Buffers a write to <paramref name="pageNumber"/>. The supplied bytes are
    /// copied; the caller's buffer can be reused / returned to a pool immediately.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    /// <exception cref="JetLimitationException">
    /// Thrown when adding this page would exceed the configured page budget.
    /// </exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="page"/> does not match the journal page size.</exception>
    public void Write(long pageNumber, ReadOnlySpan<byte> page)
    {
        if (page.Length != this.pageSize)
        {
            throw new ArgumentException("Page length mismatch.", nameof(page));
        }

        this.CapturePrior(pageNumber);

        if (this.pages.TryGetValue(pageNumber, out byte[]? existing))
        {
            page.CopyTo(existing);
            _ = this.zeroReservations.Remove(pageNumber);
            return;
        }

        if (this.pages.Count >= this.maxPages)
        {
            throw JetErrors.Limitation(JetErrorCode.JournalBudgetExceeded, string.Format(
                CultureInfo.InvariantCulture,
                "Transaction journal exceeded MaxTransactionPageBudget = {0} pages. The operation was rolled back; the transaction is still active.",
                this.maxPages));
        }

        byte[] copy = new byte[this.pageSize];
        page.CopyTo(copy);
        this.pages.Add(pageNumber, copy);
        _ = this.zeroReservations.Remove(pageNumber);
    }

    /// <summary>
    /// Buffers an append of a new page past the (snapshotted) end-of-file and
    /// returns the assigned page number.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <exception cref="JetLimitationException">
    /// Thrown when adding this page would exceed the configured page budget.
    /// </exception>
    public long Append(ReadOnlySpan<byte> page)
    {
        long pageNumber = this.NextAppendPageNumber;

        // Pre-check budget so we don't increment _appendedCount on failure.
        if (!this.pages.ContainsKey(pageNumber) && this.pages.Count >= this.maxPages)
        {
            throw JetErrors.Limitation(JetErrorCode.JournalBudgetExceeded, string.Format(
                CultureInfo.InvariantCulture,
                "Transaction journal exceeded MaxTransactionPageBudget = {0} pages. The operation was rolled back; the transaction is still active.",
                this.maxPages));
        }

        this.Write(pageNumber, page);
        this.appendedCount++;
        return pageNumber;
    }

    /// <summary>Reserves a journal page without adding a replay image.</summary>
    /// <param name="existingPageNumber">An existing free page, or null to append.</param>
    /// <returns>The reserved page number.</returns>
    internal long ReserveZeroedPage(long? existingPageNumber)
    {
        if (this.IsStatement)
        {
            long reserved = existingPageNumber ?? this.NextAppendPageNumber;
            if (existingPageNumber is null)
            {
                this.appendedCount++;
            }

            _ = this.pages.Remove(reserved);
            _ = this.zeroReservations.Add(reserved);
            return reserved;
        }

        byte[] zero = new byte[this.pageSize];
        long pageNumber;
        if (existingPageNumber is { } existing)
        {
            this.Write(existing, zero);
            pageNumber = existing;
        }
        else
        {
            pageNumber = this.Append(zero);
        }

        _ = this.zeroReservations.Add(pageNumber);
        return pageNumber;
    }

    /// <summary>Checks whether the page has only a provisional zero image.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <returns>Whether the page awaits initialization.</returns>
    internal bool IsZeroReservation(long pageNumber) => this.zeroReservations.Contains(pageNumber);

    /// <summary>
    /// Returns the buffered page bytes for <paramref name="pageNumber"/>, or
    /// <see langword="null"/> when the journal does not contain it.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    public byte[]? TryGet(long pageNumber)
        => this.pages.TryGetValue(pageNumber, out byte[]? p) ? p : null;

    /// <summary>
    /// Enumerates every (pageNumber, pageBytes) pair in ascending page-number
    /// order. The enumeration is stable so the commit replay extends the file
    /// monotonically rather than seeking back and forth.
    /// </summary>
    public IEnumerable<KeyValuePair<long, byte[]>> EnumerateInOrder()
    {
        foreach (KeyValuePair<long, byte[]> page in this.pages)
        {
            if (!this.zeroReservations.Contains(page.Key))
            {
                yield return page;
            }
        }
    }

    /// <summary>Starts an internal statement frame.</summary>
    internal void BeginSavepoint() => this.savepoints.Push(new SavepointFrame(this.appendedCount));

    /// <summary>Merges the child frame into its parent.</summary>
    internal void ReleaseSavepoint()
    {
        SavepointFrame frame = this.savepoints.Pop();
        if (this.savepoints.Count == 0)
        {
            return;
        }

        SavepointFrame parent = this.savepoints.Peek();
        foreach (KeyValuePair<long, PriorImage> prior in frame.Priors)
        {
            if (!parent.Priors.ContainsKey(prior.Key))
            {
                parent.Priors.Add(prior.Key, prior.Value);
            }
        }
    }

    /// <summary>Restores the journal and append boundary captured by the frame.</summary>
    internal void RollbackSavepoint()
    {
        SavepointFrame frame = this.savepoints.Pop();
        foreach (KeyValuePair<long, PriorImage> prior in frame.Priors)
        {
            if (prior.Value.Bytes is { } bytes)
            {
                this.pages[prior.Key] = bytes;
            }
            else
            {
                _ = this.pages.Remove(prior.Key);
            }

            if (prior.Value.ZeroReservation)
            {
                _ = this.zeroReservations.Add(prior.Key);
            }
            else
            {
                _ = this.zeroReservations.Remove(prior.Key);
            }
        }

        this.appendedCount = frame.AppendedCount;
    }

    private void CapturePrior(long pageNumber)
    {
        if (this.savepoints.Count == 0)
        {
            return;
        }

        SavepointFrame frame = this.savepoints.Peek();
        if (!frame.Priors.ContainsKey(pageNumber))
        {
            byte[]? prior = this.TryGet(pageNumber);
            frame.Priors.Add(pageNumber, new PriorImage(prior is null ? null : (byte[])prior.Clone(), this.zeroReservations.Contains(pageNumber)));
        }
    }

    private sealed record PriorImage(byte[]? Bytes, bool ZeroReservation);

    private sealed class SavepointFrame(long appendedCount)
    {
        internal long AppendedCount { get; } = appendedCount;

        internal Dictionary<long, PriorImage> Priors { get; } = [];
    }
}
