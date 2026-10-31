namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks bitmap admission and exact updates through the production usage-map adapters.</summary>
public sealed class UsageMapBitmapKernelTests
{
    /// <summary>The native-width address selector agrees with an independent wide-arithmetic interval oracle.</summary>
    [Fact]
    public void BitmapPositions_NativeWidthBoundaries_MatchWideArithmetic()
    {
        uint[] values = [0, 1, 4, 32, int.MaxValue, uint.MaxValue];
        ulong[] relatives = [0, 1, 7, 8, uint.MaxValue, ulong.MaxValue];
        foreach (uint length in values)
        {
            foreach (uint start in values)
            {
                foreach (uint count in values)
                {
                    foreach (ulong relative in relatives)
                    {
                        bool valid = length <= int.MaxValue && (ulong)start + count <= length && relative < (ulong)count * 8;
                        ulong expected = valid ? ((ulong)start * 8) + relative : ulong.MaxValue;
                        Assert.Equal(expected, RecoveryKernel.UsageBitmapPosition(length, start, count, relative));
                    }
                }
            }
        }

        byte[] bytes = [0xA5];
        foreach (ulong position in new ulong[] { 8, uint.MaxValue, ulong.MaxValue })
        {
            Assert.Equal((byte)0, RecoveryKernel.ReadUsageBit(bytes, position));
            Assert.False(RecoveryKernel.WriteUsageBit(bytes, position, marked: true));
            Assert.Equal((byte)0xA5, bytes[0]);
        }
    }

    /// <summary>Every byte and bit retains its neighbors through repeated set and clear operations.</summary>
    [Fact]
    public void InlineAndReferenceUpdates_ChangeExactlyTheSelectedBit()
    {
        for (int value = 0; value <= byte.MaxValue; value++)
        {
            for (int bit = 0; bit < 8; bit++)
            {
                foreach (bool marked in new[] { false, true })
                {
                    byte[] inline = new byte[32];
                    Array.Fill(inline, (byte)0xA5);
                    BinaryPrimitives.WriteInt32LittleEndian(inline.AsSpan(9, 4), 1000);
                    inline[13] = (byte)value;
                    byte[] expected = (byte[])inline.Clone();
                    expected[13] = marked ? (byte)(value | (1 << bit)) : (byte)(value & ~(1 << bit));
                    Assert.True(UsageMap.TrySetInlinePageState(inline, 8, 7, 1000 + bit, marked));
                    Assert.Equal(expected, inline);
                    Assert.True(UsageMap.TryGetInlinePageState(inline, 8, 7, 1000 + bit, out bool actual));
                    Assert.Equal(marked, actual);

                    byte[] reference = new byte[2048];
                    Array.Fill(reference, (byte)0xA5);
                    reference[4] = (byte)value;
                    byte[] expectedReference = (byte[])reference.Clone();
                    expectedReference[4] = expected[13];
                    Assert.True(UsageMap.TrySetReferencePageState(reference, 2048, (2044L * 8 * 3) + bit, marked));
                    Assert.Equal(expectedReference, reference);
                    Assert.True(UsageMap.TryGetReferencePageState(reference, 2048, bit, out actual));
                    Assert.Equal(marked, actual);
                }
            }
        }
    }

    /// <summary>Malformed signed bounds and incomplete headers refuse before reading or mutating the buffer.</summary>
    /// <param name="start">The row's signed starting offset.</param>
    /// <param name="size">The row's declared size.</param>
    /// <param name="pageNumber">The page whose membership is requested.</param>
    [Theory]
    [InlineData(-1, 7, 0L)]
    [InlineData(0, -1, 0L)]
    [InlineData(0, 5, 0L)]
    [InlineData(30, 7, 0L)]
    [InlineData(int.MaxValue, int.MaxValue, 0L)]
    [InlineData(0, int.MaxValue, 0L)]
    [InlineData(0, 7, -1L)]
    [InlineData(0, 7, long.MinValue)]
    [InlineData(0, 7, long.MaxValue)]
    public void InvalidInlineBounds_RefuseWithoutMutation(int start, int size, long pageNumber)
    {
        byte[] page = new byte[32];
        byte[] original = (byte[])page.Clone();
        Assert.False(UsageMap.TryGetInlinePageState(page, start, size, pageNumber, out bool marked));
        Assert.False(marked);
        Assert.False(UsageMap.TrySetInlinePageState(page, start, size, pageNumber, isMarked: true));
        Assert.Equal(original, page);
    }

    /// <summary>Negative inline bases and either edge outside the bitmap window refuse consistently.</summary>
    /// <param name="basePage">The signed base page stored in the row.</param>
    /// <param name="pageNumber">The page whose membership is requested.</param>
    [Theory]
    [InlineData(-1, 0L)]
    [InlineData(1000, 999L)]
    [InlineData(1000, 1016L)]
    public void InvalidInlineWindow_RefusesWithoutMutation(int basePage, long pageNumber)
    {
        byte[] page = new byte[32];
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(9, 4), basePage);
        byte[] original = (byte[])page.Clone();
        Assert.False(UsageMap.TryGetInlinePageState(page, 8, 7, pageNumber, out bool marked));
        Assert.False(marked);
        Assert.False(UsageMap.TrySetInlinePageState(page, 8, 7, pageNumber, isMarked: true));
        Assert.Equal(original, page);
    }

    /// <summary>Reference addressing rejects invalid layout and missing bitmap bytes without arithmetic failures.</summary>
    /// <param name="pageSize">The declared logical page size.</param>
    /// <param name="pageNumber">The page whose membership is requested.</param>
    /// <param name="length">The acquired buffer length.</param>
    [Theory]
    [InlineData(2048, -1L, 2048)]
    [InlineData(2048, long.MinValue, 2048)]
    [InlineData(0, 0L, 2048)]
    [InlineData(4, 0L, 2048)]
    [InlineData(int.MaxValue, long.MaxValue, 2048)]
    [InlineData(2048, 0L, 4)]
    [InlineData(2048, 16351L, 2047)]
    public void InvalidReferenceBounds_RefuseWithoutMutation(int pageSize, long pageNumber, int length)
    {
        byte[] page = new byte[length];
        Assert.False(UsageMap.TryGetReferencePageState(page, pageSize, pageNumber, out bool marked));
        Assert.False(marked);
        Assert.False(UsageMap.TrySetReferencePageState(page, pageSize, pageNumber, isMarked: true));
        Assert.Equal(new byte[length], page);
    }

    /// <summary>Exact native-size window edges remain addressable without touching pooled trailing bytes.</summary>
    /// <param name="pageSize">The declared logical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void WindowEdges_StayInsideTheDeclaredBitmap(int pageSize)
    {
        byte[] reference = new byte[pageSize + 32];
        Array.Fill(reference, (byte)0xA5, pageSize, 32);
        long capacity = (pageSize - 4L) * 8;
        Assert.True(UsageMap.TrySetReferencePageState(reference, pageSize, capacity - 1, isMarked: true));
        Assert.Equal((byte)0x80, reference[pageSize - 1]);
        Assert.True(UsageMap.TrySetReferencePageState(reference, pageSize, capacity, isMarked: true));
        Assert.Equal((byte)1, reference[4]);
        Assert.All(reference[pageSize..], value => Assert.Equal((byte)0xA5, value));
        Assert.True(UsageMap.TryGetReferencePageState(reference, pageSize, long.MaxValue, out _));

        byte[] inline = new byte[32];
        BinaryPrimitives.WriteInt32LittleEndian(inline.AsSpan(9, 4), int.MaxValue);
        Assert.True(UsageMap.TrySetInlinePageState(inline, 8, 7, int.MaxValue, isMarked: true));
        Assert.True(UsageMap.TrySetInlinePageState(inline, 8, 7, int.MaxValue + 15L, isMarked: true));
        Assert.Equal((byte)1, inline[13]);
        Assert.Equal((byte)0x80, inline[14]);
        Assert.False(UsageMap.TrySetInlinePageState(inline, 8, 7, int.MaxValue + 16L, isMarked: true));
    }
}
