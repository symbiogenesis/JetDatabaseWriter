namespace JetDatabaseWriter.Internal.Transactions;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Exceptions;

/// <summary>
/// In-memory journal of dirty pages produced inside an explicit
/// <see cref="JetDatabaseWriter.Core.JetTransaction"/>. Page mutations are
/// buffered (plaintext) instead of flushed to disk, then atomically replayed
/// by <see cref="JetDatabaseWriter.Core.AccessWriter"/> at <c>CommitAsync</c>
/// time (or discarded by <c>RollbackAsync</c> / dispose).
/// </summary>
/// <remarks>
/// <para>
/// The journal stores **plaintext** page bytes. Page-level encryption is applied
/// at commit time by <see cref="JetDatabaseWriter.Core.AccessBase.PrepareEncryptedPageForWrite"/>
/// — buffering encrypted bytes would make repeated writes to the same page
/// (a common pattern inside large multi-row inserts) needlessly re-encrypt.
/// </para>
/// <para>
/// Not thread-safe. Callers serialize access via the writer's I/O gate.
/// </para>
/// </remarks>
internal sealed class PageJournal
{
    private readonly Dictionary<long, byte[]> _pages = new();
    private readonly long _baseFileLengthBytes;
    private readonly int _pageSize;
    private readonly int _maxPages;
    private long _appendedCount;

    public PageJournal(long baseFileLengthBytes, int pageSize, int maxPages)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxPages);
#else
        if (pageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize));
        }

        if (maxPages <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPages));
        }
#endif

        _baseFileLengthBytes = baseFileLengthBytes;
        _pageSize = pageSize;
        _maxPages = maxPages;
    }

    /// <summary>Gets the file length captured when the transaction began.</summary>
    public long BaseFileLengthBytes => _baseFileLengthBytes;

    /// <summary>Gets the number of distinct pages currently buffered in the journal.</summary>
    public int Count => _pages.Count;

    /// <summary>
    /// Gets the page number that the next <see cref="Append"/> call will assign,
    /// computed as <c>(BaseFileLengthBytes / pageSize) + appendedCount</c>.
    /// </summary>
    public long NextAppendPageNumber => (_baseFileLengthBytes / _pageSize) + _appendedCount;

    /// <summary>
    /// Buffers a write to <paramref name="pageNumber"/>. The supplied bytes are
    /// copied; the caller's buffer can be reused / returned to a pool immediately.
    /// </summary>
    /// <exception cref="JetLimitationException">
    /// Thrown when adding this page would exceed the configured page budget.
    /// </exception>
    public void Write(long pageNumber, ReadOnlySpan<byte> page)
    {
        if (page.Length != _pageSize)
        {
            throw new ArgumentException("Page length mismatch.", nameof(page));
        }

        if (_pages.TryGetValue(pageNumber, out byte[]? existing))
        {
            page.CopyTo(existing);
            return;
        }

        if (_pages.Count >= _maxPages)
        {
            throw new JetLimitationException(string.Format(
                CultureInfo.InvariantCulture,
                "Transaction journal exceeded MaxTransactionPageBudget = {0} pages. The transaction has been rolled back.",
                _maxPages));
        }

        var copy = new byte[_pageSize];
        page.CopyTo(copy);
        _pages.Add(pageNumber, copy);
    }

    /// <summary>
    /// Buffers an append of a new page past the (snapshotted) end-of-file and
    /// returns the assigned page number.
    /// </summary>
    /// <exception cref="JetLimitationException">
    /// Thrown when adding this page would exceed the configured page budget.
    /// </exception>
    public long Append(ReadOnlySpan<byte> page)
    {
        long pageNumber = NextAppendPageNumber;

        // Pre-check budget so we don't increment _appendedCount on failure.
        if (!_pages.ContainsKey(pageNumber) && _pages.Count >= _maxPages)
        {
            throw new JetLimitationException(string.Format(
                CultureInfo.InvariantCulture,
                "Transaction journal exceeded MaxTransactionPageBudget = {0} pages. The transaction has been rolled back.",
                _maxPages));
        }

        Write(pageNumber, page);
        _appendedCount++;
        return pageNumber;
    }

    /// <summary>
    /// Returns the buffered page bytes for <paramref name="pageNumber"/>, or
    /// <see langword="null"/> when the journal does not contain it.
    /// </summary>
    public byte[]? TryGet(long pageNumber)
        => _pages.TryGetValue(pageNumber, out byte[]? p) ? p : null;

    /// <summary>
    /// Enumerates every (pageNumber, pageBytes) pair in ascending page-number
    /// order. The enumeration is stable so the commit replay extends the file
    /// monotonically rather than seeking back and forth.
    /// </summary>
    public IEnumerable<KeyValuePair<long, byte[]>> EnumerateInOrder()
    {
        var keys = new List<long>(_pages.Keys);
        keys.Sort();
        foreach (long key in keys)
        {
            yield return new KeyValuePair<long, byte[]>(key, _pages[key]);
        }
    }
}
