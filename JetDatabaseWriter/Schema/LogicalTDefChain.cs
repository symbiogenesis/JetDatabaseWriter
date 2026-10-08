namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using static JetDatabaseWriter.Schema.JetTypeInfo;

internal sealed class LogicalTDefChain
{
    private readonly int pageSizeBytes;
    private readonly List<long> pageNumbers;

    private LogicalTDefChain(byte[] bytes, List<long> pageNumbers, int pageSizeBytes)
    {
        this.Bytes = bytes;
        this.pageNumbers = pageNumbers;
        this.pageSizeBytes = pageSizeBytes;
    }

    internal byte[] Bytes { get; private set; }

    internal IReadOnlyList<long> PageNumbers => this.pageNumbers;

    internal static async ValueTask<LogicalTDefChain?> ReadAsync(
        long startPage,
        int pageSizeBytes,
        Func<long, CancellationToken, ValueTask<byte[]>> readPageAsync,
        Action<byte[]> returnPage,
        bool retainPageNumbers,
        CancellationToken cancellationToken,
        int maxLogicalBytes = 16 * 1024 * 1024)
    {
        cancellationToken.ThrowIfCancellationRequested();
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxLogicalBytes);
#else
        if (maxLogicalBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLogicalBytes));
        }
#endif

        await using var output = new MemoryStream();
        List<long> physicalPages = [];
        HashSet<long> seen = [];
        long pageNumber = startPage;
        int pageIndex = 0;

        while (pageNumber != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!seen.Add(pageNumber))
            {
                throw new InvalidDataException($"TDEF chain contains a cycle at page {pageNumber}.");
            }

            int bytesToCopy = pageIndex == 0 ? pageSizeBytes : pageSizeBytes - 8;
            if (output.Length > (long)maxLogicalBytes - bytesToCopy)
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The table definition exceeds the configured byte budget.");
            }

            byte[] page = await readPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page.Length < pageSizeBytes)
                {
                    throw new InvalidDataException($"TDEF page {pageNumber} is truncated.");
                }

                if (page[0] != Constants.PageTypes.TableDefinition)
                {
                    if (pageIndex == 0)
                    {
                        return null;
                    }

                    throw new InvalidDataException($"TDEF continuation page {pageNumber} has an invalid page type.");
                }

                // Geometric growth keeps assembly linear in the chain length;
                // its capacity remains inside the caller's logical-byte budget.
                int requiredLength = checked((int)output.Length + bytesToCopy);
                if (output.Capacity < requiredLength)
                {
                    output.Capacity = (int)Math.Min(
                        maxLogicalBytes,
                        Math.Max(requiredLength, Math.Max(pageSizeBytes, (long)output.Capacity * 2)));
                }

                await output.WriteAsync(page.AsMemory(pageIndex == 0 ? 0 : 8, bytesToCopy), cancellationToken).ConfigureAwait(false);
                if (retainPageNumbers)
                {
                    physicalPages.Add(pageNumber);
                }

                pageNumber = Ru32(page, 4);
                pageIndex++;
            }
            finally
            {
                returnPage(page);
            }
        }

        return pageIndex == 0
            ? null
            : new LogicalTDefChain(output.ToArray(), physicalPages, pageSizeBytes);
    }

    internal static async ValueTask<LogicalTDefChain> ReadRequiredAsync(
        long startPage,
        int pageSizeBytes,
        Func<long, CancellationToken, ValueTask<byte[]>> readPageAsync,
        Action<byte[]> returnPage,
        bool retainPageNumbers,
        CancellationToken cancellationToken,
        int maxLogicalBytes = 16 * 1024 * 1024)
        => await ReadAsync(
            startPage,
            pageSizeBytes,
            readPageAsync,
            returnPage,
            retainPageNumbers,
            cancellationToken,
            maxLogicalBytes).ConfigureAwait(false)
            ?? throw new NotSupportedException($"TDEF at page {startPage} could not be read.");

    internal static int GetLogicalPageCount(int pageSizeBytes, int usedLength)
    {
        if (usedLength <= pageSizeBytes)
        {
            return 1;
        }

        int bodyPerContinuation = pageSizeBytes - 8;
        return 1 + ((usedLength - pageSizeBytes + bodyPerContinuation - 1) / bodyPerContinuation);
    }

    internal static int GetLogicalCapacity(int pageSizeBytes, int usedLength)
        => LogicalLengthForPageCount(pageSizeBytes, GetLogicalPageCount(pageSizeBytes, usedLength));

    internal static (int PageIndex, int PageOffset) LogicalToPhysicalOffset(int pageSizeBytes, int logicalOffset)
    {
        if (logicalOffset < pageSizeBytes)
        {
            return (0, logicalOffset);
        }

        int bodyPerContinuation = pageSizeBytes - 8;
        int rest = logicalOffset - pageSizeBytes;
        return (1 + (rest / bodyPerContinuation), 8 + (rest % bodyPerContinuation));
    }

    internal static byte[][] MaterializePages(
        byte[] logicalBytes,
        int usedLength,
        int pageSizeBytes,
        IReadOnlyList<long>? pageNumbers = null)
    {
        int pageCount = pageNumbers?.Count ?? GetLogicalPageCount(pageSizeBytes, usedLength);
        byte[][] pages = new byte[pageCount][];
        pages[0] = new byte[pageSizeBytes];
        Buffer.BlockCopy(logicalBytes, 0, pages[0], 0, Math.Min(pageSizeBytes, logicalBytes.Length));
        Wi32(pages[0], 4, GetNextPageNumber(pageNumbers, 0));

        int bodyPerContinuation = pageSizeBytes - 8;
        for (int pageIndex = 1; pageIndex < pageCount; pageIndex++)
        {
            byte[] page = new byte[pageSizeBytes];
            page[0] = Constants.PageTypes.TableDefinition;
            page[1] = 0x01;
            Wi32(page, 4, GetNextPageNumber(pageNumbers, pageIndex));

            int sourceOffset = pageSizeBytes + ((pageIndex - 1) * bodyPerContinuation);
            int copyLength = Math.Min(bodyPerContinuation, Math.Max(0, usedLength - sourceOffset));
            if (copyLength > 0)
            {
                Buffer.BlockCopy(logicalBytes, sourceOffset, page, 8, copyLength);
            }

            pages[pageIndex] = page;
        }

        return pages;
    }

    internal byte[] EnsureCapacity(int usedLength)
    {
        int capacity = GetLogicalCapacity(this.pageSizeBytes, usedLength);
        if (this.Bytes.Length >= capacity)
        {
            return this.Bytes;
        }

        byte[] resized = this.Bytes;
        Array.Resize(ref resized, capacity);
        this.Bytes = resized;
        return resized;
    }

    /// <summary>
    /// Writes <paramref name="logicalBytes"/> back over the chain's pages,
    /// allocating or freeing continuation pages as the used length requires,
    /// and stamps the page type and <c>tdef_len</c> header fields.
    /// </summary>
    /// <param name="logicalBytes">The logical TDEF bytes.</param>
    /// <param name="usedLength">The number of used logical bytes, including the 8-byte page header.</param>
    /// <param name="allocatePageAsync">Allocates a continuation page.</param>
    /// <param name="writePageAsync">Writes one physical page.</param>
    /// <param name="deallocatePageAsync">Frees a continuation page that is no longer needed.</param>
    /// <param name="writeFreeSpace">
    /// <see langword="true"/> to stamp the Jet4 / ACE free-space word at
    /// offset 2; <see langword="false"/> for Jet3, whose TDEF keeps the
    /// <c>VC</c> signature there.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when the chain retains no physical page.</exception>
    internal async ValueTask WriteAsync(
        byte[] logicalBytes,
        int usedLength,
        Func<byte[], CancellationToken, ValueTask<long>> allocatePageAsync,
        Func<long, byte[], CancellationToken, ValueTask> writePageAsync,
        Func<long, CancellationToken, ValueTask> deallocatePageAsync,
        bool writeFreeSpace,
        CancellationToken cancellationToken)
    {
        if (this.pageNumbers.Count == 0)
        {
            throw new InvalidOperationException("A logical TDEF chain must retain at least one physical page before it can be written.");
        }

        this.Bytes = logicalBytes;
        logicalBytes = this.EnsureCapacity(usedLength);

        int pageCount = GetLogicalPageCount(this.pageSizeBytes, usedLength);
        long[] physicalPages = new long[pageCount];
        int retainedCount = Math.Min(this.pageNumbers.Count, pageCount);
        for (int pageIndex = 0; pageIndex < retainedCount; pageIndex++)
        {
            physicalPages[pageIndex] = this.pageNumbers[pageIndex];
        }

        for (int pageIndex = retainedCount; pageIndex < pageCount; pageIndex++)
        {
            physicalPages[pageIndex] = await allocatePageAsync(new byte[this.pageSizeBytes], cancellationToken).ConfigureAwait(false);
        }

        logicalBytes[0] = Constants.PageTypes.TableDefinition;
        logicalBytes[1] = 0x01;
        int tdefLength = Math.Max(0, usedLength - 8);
        Wi32(logicalBytes, 8, tdefLength);
        if (writeFreeSpace)
        {
            Wu16(logicalBytes, 2, Math.Max(0, this.pageSizeBytes - tdefLength - 8));
        }

        byte[][] pages = MaterializePages(logicalBytes, usedLength, this.pageSizeBytes, physicalPages);
        for (int pageIndex = 0; pageIndex < pages.Length; pageIndex++)
        {
            await writePageAsync(physicalPages[pageIndex], pages[pageIndex], cancellationToken).ConfigureAwait(false);
        }

        for (int pageIndex = pageCount; pageIndex < this.pageNumbers.Count; pageIndex++)
        {
            await deallocatePageAsync(this.pageNumbers[pageIndex], cancellationToken).ConfigureAwait(false);
        }

        this.pageNumbers.Clear();
        this.pageNumbers.AddRange(physicalPages);
    }

    /// <summary>
    /// Writes <see cref="Bytes"/> back over the chain's retained physical
    /// pages without changing the chain's length or page numbers. Used for
    /// in-place field patches (index <c>first_dp</c> roots, <c>used_pages</c>
    /// pointers) whose logical offsets may fall on any page of the chain. Each
    /// page is re-read, the logical slice it holds is laid over it (page 0
    /// whole, continuation pages after their 8-byte header), and the page is
    /// written only when that changed it.
    /// </summary>
    /// <param name="readPageAsync">Reads one physical page.</param>
    /// <param name="returnPage">Returns a buffer obtained from <paramref name="readPageAsync"/>.</param>
    /// <param name="writePageAsync">Writes one physical page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The chain was read without retaining its page numbers.</exception>
    internal async ValueTask WriteInPlaceAsync(
        Func<long, CancellationToken, ValueTask<byte[]>> readPageAsync,
        Action<byte[]> returnPage,
        Func<long, byte[], CancellationToken, ValueTask> writePageAsync,
        CancellationToken cancellationToken)
    {
        if (this.pageNumbers.Count == 0)
        {
            throw new InvalidOperationException("A logical TDEF chain must retain its physical pages before it can be written in place.");
        }

        int bodyPerContinuation = this.pageSizeBytes - 8;
        for (int pageIndex = 0; pageIndex < this.pageNumbers.Count; pageIndex++)
        {
            int logicalStart = pageIndex == 0 ? 0 : this.pageSizeBytes + ((pageIndex - 1) * bodyPerContinuation);
            int physicalStart = pageIndex == 0 ? 0 : 8;
            int length = this.pageSizeBytes - physicalStart;
            ReadOnlyMemory<byte> logicalSlice = this.Bytes.AsMemory(logicalStart, length);

            byte[] page = await readPageAsync(this.pageNumbers[pageIndex], cancellationToken).ConfigureAwait(false);
            try
            {
                if (logicalSlice.Span.SequenceEqual(page.AsSpan(physicalStart, length)))
                {
                    continue;
                }

                logicalSlice.Span.CopyTo(page.AsSpan(physicalStart, length));
                await writePageAsync(this.pageNumbers[pageIndex], page, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                returnPage(page);
            }
        }
    }

    private static int GetNextPageNumber(IReadOnlyList<long>? pageNumbers, int currentPageIndex)
        => pageNumbers is not null && currentPageIndex + 1 < pageNumbers.Count
            ? checked((int)pageNumbers[currentPageIndex + 1])
            : 0;

    private static int LogicalLengthForPageCount(int pageSizeBytes, int pageCount)
        => pageSizeBytes + ((pageCount - 1) * (pageSizeBytes - 8));
}
