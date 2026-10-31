namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Verification;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Checks the generated scan against independently expanded bitmap membership.</summary>
public sealed class UsageMapEnumerationKernelTests
{
    /// <summary>Every bit pattern, boundary and signed base preserves exact ordered membership and refusal prefixes.</summary>
    [Fact]
    public void GeneratedScanner_AllPatternsAndBounds_PreserveFirstFailureAndExactMembers()
    {
        for (int pattern = 0; pattern <= byte.MaxValue; pattern++)
        {
            byte[] bitmap = [0xFF, 0, (byte)pattern, 0, 0x81, 0xFF];
            foreach (long basePage in new long[] { long.MinValue, -16, 0, 100, long.MaxValue - 32 })
            {
                foreach (bool strict in new[] { false, true })
                {
                    foreach ((long minimum, long total) in new (long, long)[] { (-32, 200), (basePage + 9, basePage + 25), (0, 0) })
                    {
                        var expected = new List<long>();
                        bool admitted = true;
                        for (int bit = 0; bit < 32; bit++)
                        {
                            if ((bitmap[1 + (bit / 8)] & (1 << (bit % 8))) == 0)
                            {
                                continue;
                            }

                            long page = basePage + bit;
                            if (page < minimum || page >= total)
                            {
                                if (strict)
                                {
                                    admitted = false;
                                    break;
                                }
                            }
                            else
                            {
                                expected.Add(page);
                            }
                        }

                        byte[] original = (byte[])bitmap.Clone();
                        var actual = new List<long>();
                        uint cursor = 0;
                        byte state;
                        do
                        {
                            uint previous = cursor;
                            UsageMapKernel.NextBitmapPage(bitmap, 1, 4, basePage, total, minimum, strict, cursor, out cursor, out byte members, out state);
                            Assert.InRange(cursor, previous, 32U);
                            if (members != 0)
                            {
                                long firstPage = UsageMapKernel.Page(basePage, cursor);
                                for (byte bit = 0; bit < 8; bit++)
                                {
                                    if ((members & (1 << bit)) != 0)
                                    {
                                        actual.Add(UsageMapKernel.PageOffset(firstPage, bit));
                                    }
                                }
                            }

                            if (state == 2)
                            {
                                Assert.True(cursor > previous);
                                Assert.Equal(0U, cursor % 8);
                                Assert.NotEqual((byte)0, members);
                            }
                        }
                        while (state == 2);

                        Assert.Equal(admitted ? (byte)1 : (byte)3, state);
                        Assert.Equal(expected, actual);
                        Assert.Equal(original, bitmap);
                    }
                }
            }
        }
    }

    /// <summary>Invalid native-width ranges and overflowing page windows refuse before the scan starts.</summary>
    [Fact]
    public void GeneratedScanner_InvalidRanges_RefuseWithoutReadingOrProgress()
    {
        byte[] bytes = [0xFF];
        foreach ((uint offset, uint count, long basePage, uint cursor) in new (uint, uint, long, uint)[]
        {
            (2, 0, 0, 0), (0, 2, 0, 0), (uint.MaxValue, uint.MaxValue, 0, 0),
            (0, 1, long.MaxValue, 0), (0, 1, 0, 9),
        })
        {
            Assert.False(UsageMapKernel.HasScanRange((uint)bytes.Length, offset, count, basePage, cursor));
            Assert.Equal((byte)0xFF, bytes[0]);
        }
    }

    /// <summary>Native cursor extremes select the exact byte base and preserve every signed page offset.</summary>
    [Fact]
    public void BatchPageBase_NativeExtremes_PreserveByteAndSignedOffsets()
    {
        foreach (uint cursor in new uint[] { 0, 1, 7, 8, 9, int.MaxValue, uint.MaxValue })
        {
            uint expectedStart = cursor == 0 ? 0 : (uint)(((long)cursor - 1) / 8 * 8);
            Assert.Equal(expectedStart, UsageMapKernel.BatchStart(cursor));
            if (cursor == 0)
            {
                continue;
            }

            foreach (long basePage in new long[] { long.MinValue, 0, long.MaxValue - expectedStart - 7 })
            {
                long firstPage = UsageMapKernel.Page(basePage, cursor);
                Assert.Equal(basePage + expectedStart, firstPage);
                for (byte bit = 0; bit < 8; bit++)
                {
                    Assert.Equal(firstPage + bit, UsageMapKernel.PageOffset(firstPage, bit));
                }
            }
        }
    }

    /// <summary>A batched strict refusal retains every earlier admitted member from that same byte.</summary>
    [Fact]
    public void GeneratedBatch_AllBytePatternsAndBoundaryCuts_PreserveExactMaskAndFirstRefusal()
    {
        for (int pattern = 0; pattern <= byte.MaxValue; pattern++)
        {
            byte[] bitmap = [(byte)pattern];
            for (int minimum = 0; minimum <= 8; minimum++)
            {
                for (int total = 0; total <= 8; total++)
                {
                    foreach (bool strict in new[] { false, true })
                    {
                        int expectedMask = 0;
                        uint expectedCursor = 8;
                        byte expectedState = 1;
                        for (int bit = 0; bit < 8; bit++)
                        {
                            if ((pattern & (1 << bit)) == 0)
                            {
                                continue;
                            }

                            if (bit < minimum || bit >= total)
                            {
                                if (strict)
                                {
                                    expectedState = 3;
                                    expectedCursor = (uint)bit + 1;
                                    break;
                                }
                            }
                            else
                            {
                                expectedMask |= 1 << bit;
                            }
                        }

                        if (expectedState == 1 && expectedMask != 0)
                        {
                            expectedState = 2;
                        }

                        UsageMapKernel.NextBitmapPage(bitmap, 0, 1, 0, total, minimum, strict, 0, out uint cursor, out byte members, out byte state);
                        Assert.Equal((byte)expectedMask, members);
                        Assert.Equal(expectedState, state);
                        Assert.Equal(expectedCursor, cursor);
                        if (expectedMask != 0)
                        {
                            Assert.Equal(0, UsageMapKernel.Page(0, cursor));
                        }
                    }
                }
            }
        }
    }

    /// <summary>Reference scanning retains emitted prefixes and always returns an acquired page after strict refusal.</summary>
    [Fact]
    public async Task ReferenceScanner_StrictFailure_RetainsPrefixAndReturnsBorrowedPage()
    {
        byte[] map = new byte[32];
        var row = new RowBound(0, 8, 5);
        map[8] = Constants.UsageMap.ReferenceMapType;
        Wi32(map, 9, 2);
        byte[] bitmap = new byte[2048];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[4] = 0b1000_0101;
        int returns = 0;
        var pages = new List<long> { 99 };
        Assert.False(await UsageMap.TryEnumeratePagesAsync(map, row, bitmap.Length, 7, 0, true, ReadAsync, Return, pages, TestContext.Current.CancellationToken));
        Assert.Equal(new long[] { 99, 0, 2 }, pages);
        Assert.Equal(1, returns);

        ValueTask<byte[]> ReadAsync(long pageNumber, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(2, pageNumber);
            return new ValueTask<byte[]>(bitmap);
        }

        void Return(byte[] page)
        {
            Assert.Same(bitmap, page);
            returns++;
        }
    }

    /// <summary>Cancellation between reference reads keeps the acquired page returned and its emitted members.</summary>
    [Fact]
    public async Task ReferenceScanner_CancellationAfterReturnedPage_PreservesBorrowedOwnership()
    {
        byte[] map = new byte[32];
        var row = new RowBound(0, 8, 9);
        map[8] = Constants.UsageMap.ReferenceMapType;
        Wi32(map, 9, 2);
        Wi32(map, 13, 3);
        byte[] bitmap = new byte[2048];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[4] = 0b1000_0101;
        using var cancellation = new CancellationTokenSource();
        int reads = 0;
        int returns = 0;
        var pages = new List<long>();
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await UsageMap.TryEnumeratePagesAsync(map, row, bitmap.Length, 100, 0, true, ReadAsync, Return, pages, cancellation.Token));
        Assert.Equal(new long[] { 0, 2, 7 }, pages);
        Assert.Equal(1, reads);
        Assert.Equal(1, returns);

        ValueTask<byte[]> ReadAsync(long pageNumber, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(2, pageNumber);
            reads++;
            return new ValueTask<byte[]>(bitmap);
        }

        void Return(byte[] page)
        {
            Assert.Same(bitmap, page);
            returns++;
            cancellation.Cancel();
        }
    }
}
