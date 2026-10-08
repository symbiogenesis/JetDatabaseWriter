namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
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
            writeFreeSpace: true,
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
            writeFreeSpace: true,
            this.ct);

        Assert.Equal(10L, Assert.Single(chain.PageNumbers));
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

    [Fact]
    public async Task WriteAsync_WithoutFreeSpace_KeepsJet3SignatureWord()
    {
        byte[] jet3Page = CreatePage(nextPage: 0);
        jet3Page[2] = (byte)'V';
        jet3Page[3] = (byte)'C';
        var pages = new Dictionary<long, byte[]> { [10] = jet3Page };

        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(
            10,
            PageSize,
            (pageNumber, cancellationToken) => ReadPageAsync(pages, pageNumber, cancellationToken),
            ReturnBorrowedPage,
            retainPageNumbers: true,
            this.ct);

        const int usedLength = PageSize - 12;
        await chain.WriteAsync(
            chain.Bytes,
            usedLength,
            AllocateUnexpectedPageAsync,
            (pageNumber, page, cancellationToken) => WritePageAsync(pages, pageNumber, page, cancellationToken),
            (_, _) => ValueTask.CompletedTask,
            writeFreeSpace: false,
            this.ct);

        Assert.Equal((byte)'V', pages[10][2]);
        Assert.Equal((byte)'C', pages[10][3]);
        Assert.Equal(usedLength - 8, Ri32(pages[10], 8));
    }

    /// <summary>Security regression: TDEF-CYCLE refuses cyclic partial schemas.</summary>
    [Fact]
    public async Task ReadAsync_Cycle_RefusesInsteadOfReturningPartialSchema()
    {
        var pages = new Dictionary<long, byte[]> { [10] = CreatePage(20), [20] = CreatePage(10) };
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await LogicalTDefChain.ReadAsync(
                10,
                PageSize,
                (number, token) => ReadPageAsync(pages, number, token),
                ReturnBorrowedPage,
                true,
                this.ct));
    }

    /// <summary>Security regression: TDEF-CONTINUATION refuses invalid continuation pages.</summary>
    /// <param name="truncated">Whether the continuation is shorter than one page.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReadAsync_InvalidContinuation_RefusesInsteadOfReturningPartialSchema(bool truncated)
    {
        byte[] continuation = truncated ? new byte[PageSize - 1] : CreatePage(0);
        if (!truncated)
        {
            continuation[0] = Constants.PageTypes.Data;
        }

        var pages = new Dictionary<long, byte[]> { [10] = CreatePage(20), [20] = continuation };
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await LogicalTDefChain.ReadAsync(
                10,
                PageSize,
                (number, token) => ReadPageAsync(pages, number, token),
                ReturnBorrowedPage,
                true,
                this.ct));
    }

    /// <summary>Security regression: TDEF-BYTE-BUDGET limits reads before excess allocation.</summary>
    /// <param name="budget">The logical table definition byte budget.</param>
    /// <param name="expectedReads">The number of pages that fit inside the budget.</param>
    [Theory]
    [InlineData(119, 1)]
    [InlineData(120, 2)]
    public async Task ReadAsync_ByteBudget_RefusesBeforeReadingExcessPage(int budget, int expectedReads)
    {
        int reads = 0;
        var pages = new Dictionary<long, byte[]> { [10] = CreatePage(20), [20] = CreatePage(30), [30] = CreatePage(0) };
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await LogicalTDefChain.ReadAsync(10, PageSize, ReadCountedAsync, ReturnBorrowedPage, true, this.ct, budget));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
        Assert.Equal(expectedReads, reads);

        ValueTask<byte[]> ReadCountedAsync(long number, CancellationToken token)
        {
            reads++;
            return ReadPageAsync(pages, number, token);
        }
    }

    [Fact]
    public async Task ReadAsync_ExactBudget_PreservesAllContinuationBytes()
    {
        var pages = new Dictionary<long, byte[]> { [10] = CreatePage(20), [20] = CreatePage(30), [30] = CreatePage(0) };
        pages[20][PageSize - 1] = 0xAB;
        pages[30][PageSize - 1] = 0xCD;
        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
            10,
            PageSize,
            (number, token) => ReadPageAsync(pages, number, token),
            ReturnBorrowedPage,
            true,
            this.ct,
            176);
        Assert.NotNull(chain);
        Assert.Equal(176, chain.Bytes.Length);
        Assert.Equal(0xAB, chain.Bytes[119]);
        Assert.Equal(0xCD, chain.Bytes[175]);
        Assert.Equal([10L, 20L, 30L], chain.PageNumbers);
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
