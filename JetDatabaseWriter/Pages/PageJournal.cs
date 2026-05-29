namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// In-memory journal of dirty pages produced inside an explicit
/// <see cref="JetTransaction"/>. Page mutations are
/// buffered (plaintext) instead of flushed to disk, then atomically replayed
/// by <see cref="AccessWriter"/> at <c>CommitAsync</c>
/// time (or discarded by <c>RollbackAsync</c> / dispose).
/// </summary>
/// <remarks>
/// <para>
/// The journal stores **plaintext** page bytes. Page-level encryption is applied
/// at commit time by <see cref="AccessBase.PrepareEncryptedPageForWrite"/>
/// — buffering encrypted bytes would make repeated writes to the same page
/// (a common pattern inside large multi-row inserts) needlessly re-encrypt.
/// </para>
/// <para>
/// Not thread-safe. Callers serialize access via the writer's I/O gate.
/// </para>
/// </remarks>
internal sealed class PageJournal
{
    private readonly SortedDictionary<long, byte[]> pages = [];
    private readonly int pageSize;
    private readonly int maxPages;
    private long appendedCount;

    public PageJournal(long baseFileLengthBytes, int pageSize, int maxPages)
    {
        Guard.Positive(pageSize, nameof(pageSize));
        Guard.Positive(maxPages, nameof(maxPages));

        this.BaseFileLengthBytes = baseFileLengthBytes;
        this.pageSize = pageSize;
        this.maxPages = maxPages;
    }

    /// <summary>Gets the file length captured when the transaction began.</summary>
    public long BaseFileLengthBytes { get; }

    /// <summary>Gets the number of distinct pages currently buffered in the journal.</summary>
    public int Count => this.pages.Count;

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

        if (this.pages.TryGetValue(pageNumber, out byte[]? existing))
        {
            page.CopyTo(existing);
            return;
        }

        if (this.pages.Count >= this.maxPages)
        {
            throw new JetLimitationException(string.Format(
                CultureInfo.InvariantCulture,
                "Transaction journal exceeded MaxTransactionPageBudget = {0} pages. The transaction has been rolled back.",
                this.maxPages));
        }

        var copy = new byte[this.pageSize];
        page.CopyTo(copy);
        this.pages.Add(pageNumber, copy);
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
            throw new JetLimitationException(string.Format(
                CultureInfo.InvariantCulture,
                "Transaction journal exceeded MaxTransactionPageBudget = {0} pages. The transaction has been rolled back.",
                this.maxPages));
        }

        this.Write(pageNumber, page);
        this.appendedCount++;
        return pageNumber;
    }

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
    public IEnumerable<KeyValuePair<long, byte[]>> EnumerateInOrder() => this.pages;
}
