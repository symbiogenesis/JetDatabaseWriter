namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        byte[]? logicalBytes = null;
        List<long> physicalPages = [];
        HashSet<long> seen = [];
        long pageNumber = startPage;
        int pageIndex = 0;

        while (pageNumber != 0 && seen.Add(pageNumber))
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await readPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.TableDefinition)
                {
                    break;
                }

                int requiredLength = LogicalLengthForPageCount(pageSizeBytes, pageIndex + 1);
                if (logicalBytes is null)
                {
                    logicalBytes = new byte[requiredLength];
                }
                else if (logicalBytes.Length < requiredLength)
                {
                    Array.Resize(ref logicalBytes, requiredLength);
                }

                if (pageIndex == 0)
                {
                    Buffer.BlockCopy(page, 0, logicalBytes, 0, pageSizeBytes);
                }
                else
                {
                    int logicalOffset = pageSizeBytes + ((pageIndex - 1) * (pageSizeBytes - 8));
                    Buffer.BlockCopy(page, 8, logicalBytes, logicalOffset, pageSizeBytes - 8);
                }

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

        return logicalBytes is null
            ? null
            : new LogicalTDefChain(logicalBytes, physicalPages, pageSizeBytes);
    }

    internal static async ValueTask<LogicalTDefChain> ReadRequiredAsync(
        long startPage,
        int pageSizeBytes,
        Func<long, CancellationToken, ValueTask<byte[]>> readPageAsync,
        Action<byte[]> returnPage,
        bool retainPageNumbers,
        CancellationToken cancellationToken)
        => await ReadAsync(
            startPage,
            pageSizeBytes,
            readPageAsync,
            returnPage,
            retainPageNumbers,
            cancellationToken).ConfigureAwait(false)
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

    internal async ValueTask WriteAsync(
        byte[] logicalBytes,
        int usedLength,
        Func<byte[], CancellationToken, ValueTask<long>> allocatePageAsync,
        Func<long, byte[], CancellationToken, ValueTask> writePageAsync,
        Func<long, CancellationToken, ValueTask> deallocatePageAsync,
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
        Wu16(logicalBytes, 2, Math.Max(0, this.pageSizeBytes - tdefLength - 8));

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

    private static int GetNextPageNumber(IReadOnlyList<long>? pageNumbers, int currentPageIndex)
        => pageNumbers is not null && currentPageIndex + 1 < pageNumbers.Count
            ? checked((int)pageNumbers[currentPageIndex + 1])
            : 0;

    private static int LogicalLengthForPageCount(int pageSizeBytes, int pageCount)
        => pageSizeBytes + ((pageCount - 1) * (pageSizeBytes - 8));
}
