namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Schema;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class LogicalTDefChainTests
{
    private const int PageSize = 64;

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task WriteAsync_GrowingPastFirstPage_AllocatesContinuationAndPreservesLogicalBytes()
    {
        var pages = new Dictionary<long, byte[]>
        {
            [10] = CreatePage(nextPage: 0),
        };

        var deallocatedPages = new List<long>();
        long nextAllocatedPage = 20;

        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(
            10,
            PageSize,
            (pageNumber, cancellationToken) => ReadPageAsync(pages, pageNumber, cancellationToken),
            ReturnBorrowedPage,
            retainPageNumbers: true,
            this.ct);

        const int usedLength = PageSize + 9;
        byte[] logicalBytes = chain.EnsureCapacity(usedLength);
        for (int offset = PageSize; offset < usedLength; offset++)
        {
            logicalBytes[offset] = checked((byte)(0xA0 + offset - PageSize));
        }

        await chain.WriteAsync(
            logicalBytes,
            usedLength,
            AllocatePageAsync,
            (pageNumber, page, cancellationToken) => WritePageAsync(pages, pageNumber, page, cancellationToken),
            DeallocatePageAsync,
            this.ct);

        Assert.Equal(2, chain.PageNumbers.Count);
        Assert.Equal(10L, chain.PageNumbers[0]);
        Assert.Equal(20L, chain.PageNumbers[1]);
        Assert.Equal(20, Ri32(pages[10], 4));
        Assert.Equal(0, Ri32(pages[20], 4));
        Assert.Equal(usedLength - 8, Ri32(pages[10], 8));
        Assert.Equal((ushort)0, Ru16(pages[10], 2));
        Assert.Empty(deallocatedPages);

        for (int offset = 0; offset < usedLength - PageSize; offset++)
        {
            Assert.Equal(checked((byte)(0xA0 + offset)), pages[20][8 + offset]);
        }

        ValueTask<long> AllocatePageAsync(byte[] page, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long allocatedPage = nextAllocatedPage++;
            pages[allocatedPage] = ClonePage(page);
            return ValueTask.FromResult(allocatedPage);
        }

        ValueTask DeallocatePageAsync(long pageNumber, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            deallocatedPages.Add(pageNumber);
            _ = pages.Remove(pageNumber);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task WriteAsync_ShrinkingToFirstPage_DeallocatesUnusedContinuationPages()
    {
        var pages = new Dictionary<long, byte[]>
        {
            [10] = CreatePage(nextPage: 20),
            [20] = CreatePage(nextPage: 30),
            [30] = CreatePage(nextPage: 0),
        };

        var deallocatedPages = new List<long>();

        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(
            10,
            PageSize,
            (pageNumber, cancellationToken) => ReadPageAsync(pages, pageNumber, cancellationToken),
            ReturnBorrowedPage,
            retainPageNumbers: true,
            this.ct);

        Assert.Equal(3, chain.PageNumbers.Count);

        const int usedLength = PageSize - 7;
        byte[] logicalBytes = chain.Bytes;
        logicalBytes[usedLength - 1] = 0x7E;

        await chain.WriteAsync(
            logicalBytes,
            usedLength,
            AllocateUnexpectedPageAsync,
            (pageNumber, page, cancellationToken) => WritePageAsync(pages, pageNumber, page, cancellationToken),
            DeallocatePageAsync,
            this.ct);

        Assert.Single(chain.PageNumbers);
        Assert.Equal(10L, chain.PageNumbers[0]);
        Assert.Equal(0, Ri32(pages[10], 4));
        Assert.Equal(usedLength - 8, Ri32(pages[10], 8));
        Assert.Equal((ushort)(PageSize - usedLength), Ru16(pages[10], 2));
        Assert.Equal([20L, 30L], deallocatedPages);
        Assert.False(pages.ContainsKey(20));
        Assert.False(pages.ContainsKey(30));

        ValueTask DeallocatePageAsync(long pageNumber, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            deallocatedPages.Add(pageNumber);
            _ = pages.Remove(pageNumber);
            return ValueTask.CompletedTask;
        }
    }

    private static byte[] CreatePage(long nextPage)
    {
        byte[] page = new byte[PageSize];
        page[0] = Constants.PageTypes.TableDefinition;
        page[1] = 0x01;
        Wi32(page, 4, checked((int)nextPage));
        Wi32(page, 8, PageSize - 8);
        return page;
    }

    private static ValueTask<byte[]> ReadPageAsync(
        Dictionary<long, byte[]> pages,
        long pageNumber,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(ClonePage(pages[pageNumber]));
    }

    private static ValueTask WritePageAsync(
        Dictionary<long, byte[]> pages,
        long pageNumber,
        byte[] page,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        pages[pageNumber] = ClonePage(page);
        return ValueTask.CompletedTask;
    }

    private static ValueTask<long> AllocateUnexpectedPageAsync(byte[] page, CancellationToken cancellationToken)
    {
        _ = page;
        cancellationToken.ThrowIfCancellationRequested();
        throw new InvalidOperationException("Shrinking a TDEF chain should not allocate pages.");
    }

    private static void ReturnBorrowedPage(byte[] page) => _ = page;

    private static byte[] ClonePage(byte[] page) => (byte[])page.Clone();
}
