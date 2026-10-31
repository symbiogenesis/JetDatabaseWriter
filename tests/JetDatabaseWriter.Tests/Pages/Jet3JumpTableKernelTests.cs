namespace JetDatabaseWriter.Tests.Pages;

using System;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Jet3 jump controls retain native boundary, prefix and dummy-entry behavior.</summary>
public sealed class Jet3JumpTableKernelTests
{
    [Fact]
    public void SizingUsesTheSmallestSelfConsistentJumpCount()
    {
        for (int length = 1; length <= 4096; length++)
        {
            int expected = 0;
            while ((length + expected - 1) / 256 > expected)
            {
                expected++;
            }

            Assert.Equal(expected, Jet3JumpTable.CountForLength(length));
            Assert.Equal(checked((uint)expected), RecoveryKernel.Jet3JumpCount(checked((uint)length)));
            Assert.Equal(expected, Jet3JumpTable.EntryCount(length + expected));
        }
    }

    [Theory]
    [InlineData("030201", 0, 0)]
    [InlineData("030201", 1, 256)]
    [InlineData("030201", 2, 512)]
    [InlineData("030201", 3, 768)]
    [InlineData("030101", 1, 512)]
    [InlineData("010201", 3, 512)]
    [InlineData("FF0201", 255, 768)]
    public void HighPartStopsAtTheFirstUnreachedOrDecreasingEntry(string hex, int index, int expected)
    {
        ArgumentNullException.ThrowIfNull(hex);
        byte[] bytes = Convert.FromHexString("AABB" + hex + "CCDD");
        byte[] original = (byte[])bytes.Clone();
        int count = hex.Length / 2;
        Assert.Equal(expected, Jet3JumpTable.HighPart(bytes, 1, 1 + count, count, index));
        Assert.Equal(checked((uint)expected), RecoveryKernel.ReadJet3JumpHighPart(bytes, 1, checked((uint)(1 + count)), checked((uint)count), checked((uint)index)));
        Assert.Equal(original, bytes);
    }

    [Fact]
    public void ArrayReadRefusesInvalidRangesBeforeAccess()
    {
        byte[] bytes = [1, 2, 3];
        Assert.Equal(uint.MaxValue, RecoveryKernel.ReadJet3JumpHighPart(bytes, uint.MaxValue, 1, 1, 0));
        Assert.Equal(uint.MaxValue, RecoveryKernel.ReadJet3JumpHighPart(bytes, 0, 4, 1, 0));
        Assert.Equal(uint.MaxValue, RecoveryKernel.ReadJet3JumpHighPart(bytes, 0, 2, 3, 0));
        Assert.Equal(0u, RecoveryKernel.ReadJet3JumpHighPart(bytes, 3, 0, 0, 0));
    }

    [Fact]
    public void EncoderChoosesFirstCrossingOffsetAndRetainsDummyBytes()
    {
        int[] offsets = [5, 260, 260, 530];
        byte[] bytes = [0xAA, 0, 0, 0, 0xBB];
        Jet3JumpTable.Write(bytes.AsSpan(1, 3), offsets, 600);
        Assert.Equal(new byte[] { 0xAA, 0xFF, 3, 1, 0xBB }, bytes);

        Assert.Equal(0xFFu, RecoveryKernel.Jet3JumpEntryStart(4, 768, 600));
        Assert.Equal(256u, RecoveryKernel.Jet3JumpEntryStart(4, 512, 600));
        Assert.Equal(256u, RecoveryKernel.Jet3JumpEntryAdvance(512, 0, 260));
        Assert.Equal(3u, RecoveryKernel.Jet3JumpEntryAdvance(512, 3, 530));
    }

    [Theory]
    [InlineData(256, 247, "", 0)]
    [InlineData(512, 507, "FF01", 512)]
    [InlineData(2028, 2010, "07060504030201", 8960)]
    public void BoundaryDummyAndLongRowControlsHaveIndependentExpectedBytes(int baseLength, int eod, string hex, int highParts)
    {
        int[] offsets = baseLength switch
        {
            256 => [5, 128],
            512 => [5, 260],
            _ => [5, 260, 520, 780, 1040, 1300, 1560, 1820],
        };
        byte[] bytes = new byte[Jet3JumpTable.CountForLength(baseLength)];
        Jet3JumpTable.Write(bytes, offsets, eod);
        Assert.Equal(Convert.FromHexString(hex), bytes);
        int result = 0;
        int usable = Jet3JumpTable.UsableEntryCount(bytes.Length, eod);
        for (int index = 0; index <= offsets.Length; index++)
        {
            result += Jet3JumpTable.HighPart(bytes, 0, bytes.Length, usable, index);
        }

        Assert.Equal(highParts, result);
    }

    [Fact]
    public void DenseMaximumSizeRowRetainsLateCrossingsAndOneDummy()
    {
        int[] offsets = new int[255];
        Array.Fill(offsets, 1);
        int[] tail = [225, 478, 731, 984, 1237, 1490, 1700];
        tail.CopyTo(offsets, 248);
        byte[] bytes = new byte[Jet3JumpTable.CountForLength(2029)];
        Jet3JumpTable.Write(bytes, offsets, 1740);
        Assert.Equal(Convert.FromHexString("FFFEFDFCFBFAF9"), bytes);
        Assert.Equal(2036, 2029 + bytes.Length);
        int result = 0;
        int usable = Jet3JumpTable.UsableEntryCount(bytes.Length, 1740);
        for (int index = 0; index <= offsets.Length; index++)
        {
            result += Jet3JumpTable.HighPart(bytes, 0, bytes.Length, usable, index);
        }

        Assert.Equal(6912, result);
    }

    [Fact]
    public void AmbiguousTwoDummyEncodingRetainsTheLimitationAndWrittenBytes()
    {
        int[] offsets = new int[255];
        byte[] bytes = [0xAA, 0, 0, 0xBB];
        JetLimitationException exception = Assert.Throws<JetLimitationException>(() => Jet3JumpTable.Write(bytes.AsSpan(1, 2), offsets, 255));
        Assert.Equal(JetErrorCode.RowTooLarge, exception.ErrorCode);
        Assert.Equal(new byte[] { 0xAA, 0xFF, 0xFF, 0xBB }, bytes);
        Assert.False(RecoveryKernel.Jet3JumpWritable(255, 2, 255));
        Assert.True(RecoveryKernel.Jet3JumpWritable(254, 2, 255));
        Assert.True(RecoveryKernel.Jet3JumpWritable(255, 1, 255));
    }
}
