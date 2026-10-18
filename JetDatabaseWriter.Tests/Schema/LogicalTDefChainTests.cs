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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_SinglePageRetainsExactBytesWithoutBorrowingReturnedBuffer(bool retainPageNumbers)
    {
        byte[] borrowed = new byte[PageSize + 17];
        CreatePage(0).CopyTo(borrowed, 0);
        borrowed.AsSpan(12).Fill(0xA7);
        byte[] expected = borrowed.AsSpan(0, PageSize).ToArray();
        int reads = 0;
        int returns = 0;
        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(10, PageSize, ReadAsync, Return, retainPageNumbers, this.ct, PageSize);
        Assert.NotNull(chain);
        Assert.Equal(expected, chain.Bytes);
        Assert.Equal(retainPageNumbers ? new[] { 10L } : [], chain.PageNumbers);
        Assert.Equal(1, reads);
        Assert.Equal(1, returns);

        ValueTask<byte[]> ReadAsync(long number, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Assert.Equal(10, number);
            reads++;
            return ValueTask.FromResult(borrowed);
        }

        void Return(byte[] page)
        {
            Assert.Same(borrowed, page);
            Array.Fill(page, (byte)0xCC);
            returns++;
        }
    }

    [Fact]
    public async Task ReadAsync_CancellationDuringSinglePageReadReturnsTheBuffer()
    {
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(this.ct);
        byte[] borrowed = CreatePage(0);
        int returns = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await LogicalTDefChain.ReadAsync(10, PageSize, ReadAsync, Return, false, canceled.Token));
        Assert.Equal(1, returns);

        ValueTask<byte[]> ReadAsync(long number, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Assert.Equal(10, number);
            canceled.Cancel();
            return ValueTask.FromResult(borrowed);
        }

        void Return(byte[] page)
        {
            Assert.Same(borrowed, page);
            returns++;
        }
    }

    [Theory]
    [InlineData(57, false)]
    [InlineData(76, false)]
    [InlineData(88, false)]
    [InlineData(100, false)]
    [InlineData(130, false)]
    [InlineData(76, true)]
    [InlineData(100, true)]
    public async Task WriteAsync_ResizingPreservesRetainedHeadersAndOpaquePadding(int usedLength, bool replaceBuffer)
    {
        const int originalLength = 88;
        var pages = new Dictionary<long, byte[]>
        {
            [10] = CreatePage(20),
            [20] = CreatePage(0),
        };
        Wi32(pages[10], 8, originalLength - 8);
        pages[10][1] = 0xA1;
        pages[20][1] = 0xB2;
        pages[20][2] = 0xC3;
        pages[20][3] = 0xD4;
        pages[20].AsSpan(8, originalLength - PageSize).Fill(0x6B);
        pages[20].AsSpan(8 + originalLength - PageSize).Fill(0xD7);
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(
            10, PageSize, (number, token) => ReadPageAsync(pages, number, token), ReturnBorrowedPage, true, this.ct);
        byte[] bytes = chain.EnsureCapacity(usedLength);
        if (replaceBuffer)
        {
            bytes = new byte[LogicalTDefChain.GetLogicalCapacity(PageSize, usedLength)];
            chain.Bytes.AsSpan(0, Math.Min(originalLength, usedLength)).CopyTo(bytes);
        }

        if (usedLength > originalLength)
        {
            bytes.AsSpan(originalLength, usedLength - originalLength).Fill(0x8B);
        }

        var deallocated = new List<long>();
        long nextPage = 30;
        await chain.WriteAsync(bytes, usedLength, AllocateAsync, (number, page, token) => WritePageAsync(pages, number, page, token), DeallocateAsync, true, this.ct);

        Assert.Equal(0xA1, pages[10][1]);
        Assert.Equal(usedLength - 8, Ri32(pages[10], 8));
        if (usedLength <= PageSize)
        {
            Assert.Equal([20L], deallocated);
            Assert.Equal(0, Ri32(pages[10], 4));
        }
        else
        {
            Assert.Empty(deallocated);
            Assert.Equal(new byte[] { Constants.PageTypes.TableDefinition, 0xB2, 0xC3, 0xD4 }, pages[20].AsSpan(0, 4).ToArray());
            Assert.Equal(usedLength > 120 ? 30 : 0, Ri32(pages[20], 4));
            if (usedLength < 120)
            {
                Assert.All(pages[20].AsSpan(8 + Math.Max(originalLength, usedLength) - PageSize).ToArray(), value => Assert.Equal(0xD7, value));
            }
        }

        if (usedLength < originalLength)
        {
            LogicalTDefChain rewritten = await LogicalTDefChain.ReadRequiredAsync(
                10, PageSize, (number, token) => ReadPageAsync(pages, number, token), ReturnBorrowedPage, true, this.ct);
            int removedLength = Math.Min(originalLength, rewritten.Bytes.Length) - usedLength;
            Assert.All(rewritten.Bytes.AsSpan(usedLength, removedLength).ToArray(), value => Assert.Equal(0, value));
        }

        if (usedLength > 120)
        {
            Assert.Equal(new byte[] { Constants.PageTypes.TableDefinition, 1, 0, 0, 0, 0, 0, 0 }, pages[30].AsSpan(0, 8).ToArray());
        }

        ValueTask<long> AllocateAsync(byte[] page, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            long number = nextPage++;
            pages[number] = ClonePage(page);
            return ValueTask.FromResult(number);
        }

        ValueTask DeallocateAsync(long number, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            deallocated.Add(number);
            Assert.True(pages.Remove(number));
            return ValueTask.CompletedTask;
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    [InlineData(PageSize - 8)]
    public async Task WriteAsync_AmbiguousOriginalLengthRefusesBeforeChangingPages(int declaredLength)
    {
        var pages = new Dictionary<long, byte[]> { [10] = CreatePage(20), [20] = CreatePage(0) };
        Wi32(pages[10], 8, declaredLength);
        byte[] root = ClonePage(pages[10]);
        byte[] continuation = ClonePage(pages[20]);
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(
            10, PageSize, (number, token) => ReadPageAsync(pages, number, token), ReturnBorrowedPage, true, this.ct);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await chain.WriteAsync(chain.Bytes, PageSize + 24, AllocateUnexpectedPageAsync, (number, page, token) => WritePageAsync(pages, number, page, token), (_, _) => ValueTask.CompletedTask, true, this.ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, error.ErrorCode);
        Assert.Equal(10, error.ErrorInfo.PageNumber);
        Assert.NotNull(error.ErrorInfo.Reason);
        Assert.Equal(root, pages[10]);
        Assert.Equal(continuation, pages[20]);
    }

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

        Wi32(pages[10], 8, 176 - 8);

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
