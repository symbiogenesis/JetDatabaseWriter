namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks bounded descriptor packing through its production entry points.</summary>
public sealed class LongValueDescriptorKernelTests
{
    /// <summary>Unsupported flags cannot alter the decoded length or mutate the destination.</summary>
    [Fact]
    public void WriteTo_UnsupportedStorageModes_RefusesBeforeWriting()
    {
        for (int mode = 0; mode <= byte.MaxValue; mode++)
        {
            if (mode is 0 or 64 or 128)
            {
                continue;
            }

            byte[] destination = new byte[16];
            destination.AsSpan().Fill(0xA5);
            byte[] expected = (byte[])destination.Clone();
            var descriptor = new LongValueDescriptor(1, checked((byte)mode), uint.MaxValue, uint.MaxValue);
            JetOperationException error = Assert.Throws<JetOperationException>(() => descriptor.WriteTo(destination));
            Assert.Equal(JetErrorCode.InvalidOperationState, error.ErrorCode);
            Assert.Equal(expected, destination);
        }
    }

    /// <summary>Signed and oversized lengths cannot overflow into a native storage flag.</summary>
    /// <param name="length">The unrepresentable length.</param>
    [Theory]
    [InlineData(int.MinValue)]
    [InlineData(-1)]
    [InlineData(0x40000000)]
    [InlineData(int.MaxValue)]
    public void WriteTo_InvalidLength_RefusesBeforeWriting(int length)
    {
        foreach (byte mode in new byte[] { 0, 64, 128 })
        {
            byte[] destination = new byte[16];
            destination.AsSpan().Fill(0xA5);
            byte[] expected = (byte[])destination.Clone();
            var descriptor = new LongValueDescriptor(length, mode, uint.MaxValue, uint.MaxValue);
            JetLimitationException error = Assert.Throws<JetLimitationException>(() => descriptor.WriteTo(destination));
            Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
            Assert.Equal(expected, destination);
        }
    }

    /// <summary>Every byte position decodes its exact unsigned bits and retains reserved raw modes.</summary>
    [Fact]
    public void Decode_AllBytePositionsAndWordBoundaries_MatchesIndependentOracle()
    {
        byte[] bytes = new byte[12];
        for (int position = 0; position < 4; position++)
        {
            for (int value = 0; value <= byte.MaxValue; value++)
            {
                Array.Clear(bytes);
                bytes[position] = checked((byte)value);
                AssertDecodedWord(bytes);
            }
        }

        foreach (uint word in new uint[] { 0, 1, 0x3FFFFFFE, 0x3FFFFFFF, 0x40000000, 0x7FFFFFFF, 0x80000000, 0xBFFFFFFF, 0xC0000000, uint.MaxValue })
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes, word);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(4), 0xFEDCBA98);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), 0x89ABCDEF);
            AssertDecodedWord(bytes);
        }
    }

    /// <summary>Only defined storage classes and the complete 30-bit length domain encode.</summary>
    [Fact]
    public void Encode_AllStorageModesAndLengthBoundaries_RefusesExactlyInvalidFields()
    {
        foreach (uint length in new uint[] { 0, 1, 0xFFFFFF, 0x1000000, 0x3FFFFFFF, 0x40000000, int.MaxValue, uint.MaxValue })
        {
            for (int mode = 0; mode <= byte.MaxValue; mode++)
            {
                bool supportedMode = mode is 0 or 64 or 128;
                Assert.Equal(supportedMode, LongValueKernel.IsSupportedStorageMode(checked((byte)mode)));
                ulong packed = LongValueKernel.EncodeLengthAndMode(length, checked((byte)mode));
                if (length > 0x3FFFFFFF || !supportedMode)
                {
                    Assert.Equal(4294967296ul, packed);
                }
                else
                {
                    uint expected = length | (checked((uint)mode) << 24);
                    Assert.Equal(expected, packed);
                    Assert.Equal(length, LongValueKernel.DecodeLength(checked((uint)packed)));
                    Assert.Equal(checked((byte)mode), LongValueKernel.DecodeStorageMode(checked((uint)packed)));
                }
            }
        }
    }

    /// <summary>Production writes the independent wire representation and preserves surrounding bytes.</summary>
    [Fact]
    public void WriteTo_SupportedBoundaryFields_RoundTripsExactBytes()
    {
        foreach (int length in new int[] { 0, 1, 255, 256, 0xFFFFFF, 0x1000000, 0x3FFFFFFE, 0x3FFFFFFF })
        {
            foreach (byte mode in new byte[] { 0, 64, 128 })
            {
                var descriptor = new LongValueDescriptor(length, mode, 0xFEDCBA98, 0x89ABCDEF);
                byte[] destination = new byte[20];
                destination.AsSpan().Fill(0xA5);
                byte[] expected = (byte[])destination.Clone();
                BinaryPrimitives.WriteUInt32LittleEndian(expected.AsSpan(4), checked((uint)length) | ((uint)mode << 24));
                BinaryPrimitives.WriteUInt32LittleEndian(expected.AsSpan(8), descriptor.FirstDp);
                BinaryPrimitives.WriteUInt32LittleEndian(expected.AsSpan(12), descriptor.Token);
                descriptor.WriteTo(destination.AsSpan(4, 12));
                Assert.Equal(expected, destination);
                Assert.True(LongValueDescriptor.TryRead(destination.AsSpan(4, 12), out LongValueDescriptor decoded));
                Assert.Equal(descriptor, decoded);
            }
        }
    }

    /// <summary>Incomplete input and output are refused before any first-word access or mutation.</summary>
    [Fact]
    public void Descriptor_TruncatedHeader_RefusesWithoutWriting()
    {
        for (int length = 0; length < 12; length++)
        {
            byte[] bytes = new byte[length];
            bytes.AsSpan().Fill(0xA5);
            byte[] expected = (byte[])bytes.Clone();
            Assert.False(LongValueDescriptor.TryRead(bytes, out LongValueDescriptor decoded));
            Assert.Equal(default, decoded);
            Assert.Throws<ArgumentException>(() => LongValueDescriptor.Inline(1).WriteTo(bytes));
            Assert.Equal(expected, bytes);
        }
    }

    /// <summary>Out-of-range row identities cannot wrap onto another native LVAL row.</summary>
    /// <param name="page">The page outside the native pointer field.</param>
    /// <param name="row">The row outside the native pointer field.</param>
    [Theory]
    [InlineData(-1L, 0)]
    [InlineData(0x1000000L, 0)]
    [InlineData(long.MaxValue, 0)]
    [InlineData(1L, -1)]
    [InlineData(1L, 256)]
    [InlineData(1L, int.MaxValue)]
    public void MakeRowPointer_InvalidIdentity_RefusesWithoutAliasing(long page, int row)
    {
        JetLimitationException error = Assert.Throws<JetLimitationException>(() => LongValueStore.MakeRowPointer(page, row));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
    }

    /// <summary>All twelve acquired byte positions retain their complete unsigned fields.</summary>
    [Fact]
    public void ReadDescriptor_EveryFieldBytePositionAndSubslice_MatchesWireOracle()
    {
        byte[] source = new byte[20];
        for (int position = 0; position < 12; position++)
        {
            for (int value = 0; value <= byte.MaxValue; value++)
            {
                Array.Clear(source);
                source[position + 4] = checked((byte)value);
                Assert.True(LongValueDescriptor.TryRead(source, 4, 12, out LongValueDescriptor acquired));
                Assert.True(LongValueDescriptor.TryRead(source.AsSpan(4, 12), out LongValueDescriptor spanAcquired));
                Assert.Equal(spanAcquired, acquired);
                uint word = BinaryPrimitives.ReadUInt32LittleEndian(source.AsSpan(4, 4));
                Assert.Equal(checked((int)(word & 0x3FFFFFFF)), acquired.Length);
                Assert.Equal(checked((byte)(source[7] & 0xC0)), acquired.StorageMode);
                Assert.Equal(BinaryPrimitives.ReadUInt32LittleEndian(source.AsSpan(8, 4)), acquired.FirstDp);
                Assert.Equal(BinaryPrimitives.ReadUInt32LittleEndian(source.AsSpan(12, 4)), acquired.Token);
            }
        }
    }

    /// <summary>Invalid signed, overflowing and truncated slices refuse before acquisition.</summary>
    /// <param name="start">The candidate slice start.</param>
    /// <param name="count">The candidate slice count.</param>
    [Theory]
    [InlineData(-1, 12)]
    [InlineData(0, -1)]
    [InlineData(0, 11)]
    [InlineData(1, 12)]
    [InlineData(int.MaxValue, 12)]
    [InlineData(0, int.MaxValue)]
    [InlineData(int.MaxValue, int.MaxValue)]
    public void ReadDescriptor_InvalidSlice_RefusesWithoutAcquisition(int start, int count)
    {
        Assert.False(LongValueDescriptor.TryRead(new byte[12], start, count, out LongValueDescriptor descriptor));
        Assert.Equal(default, descriptor);
    }

    /// <summary>Array writes preserve every byte outside the complete header and round-trip exact fields.</summary>
    [Fact]
    public void ArrayKernel_CompleteDescriptor_RoundTripsAndPreservesOutsideBytes()
    {
        foreach (uint pointer in new uint[] { 0, 1, 0xFF, 0x100, 0xFFFFFF, 0x80000000, uint.MaxValue })
        {
            foreach (byte mode in new byte[] { 0, 64, 128 })
            {
                byte[] bytes = new byte[20];
                bytes.AsSpan().Fill(0xA5);
                byte[] expected = (byte[])bytes.Clone();
                const uint length = 0x12345678;
                uint token = ~pointer;
                BinaryPrimitives.WriteUInt32LittleEndian(expected.AsSpan(4), length | ((uint)mode << 24));
                BinaryPrimitives.WriteUInt32LittleEndian(expected.AsSpan(8), pointer);
                BinaryPrimitives.WriteUInt32LittleEndian(expected.AsSpan(12), token);
                Assert.True(LongValueKernel.WriteDescriptor(bytes, 4, 12, length, mode, pointer, token));
                Assert.Equal(expected, bytes);
                Assert.True(LongValueDescriptor.TryRead(bytes, 4, 12, out LongValueDescriptor descriptor));
                Assert.Equal(new LongValueDescriptor(checked((int)length), mode, pointer, token), descriptor);
            }
        }
    }

    /// <summary>Every refusal class leaves even the first destination byte untouched.</summary>
    /// <param name="start">The output slice offset.</param>
    /// <param name="count">The output slice length.</param>
    /// <param name="length">The declared value length.</param>
    /// <param name="mode">The storage class.</param>
    [Theory]
    [InlineData(0u, 11u, 1u, (byte)128)]
    [InlineData(1u, 12u, 1u, (byte)128)]
    [InlineData(uint.MaxValue, uint.MaxValue, 1u, (byte)128)]
    [InlineData(0u, 12u, 0x40000000u, (byte)128)]
    [InlineData(0u, 12u, 1u, (byte)192)]
    [InlineData(0u, 12u, 1u, (byte)1)]
    public void ArrayKernel_InvalidEncodingOrRange_RefusesBeforeMutation(uint start, uint count, uint length, byte mode)
    {
        byte[] bytes = new byte[12];
        bytes.AsSpan().Fill(0xA5);
        byte[] original = (byte[])bytes.Clone();
        Assert.False(LongValueKernel.WriteDescriptor(bytes, start, count, length, mode, uint.MaxValue, uint.MaxValue));
        Assert.Equal(original, bytes);
    }

    /// <summary>All row bits and page boundaries invert without signed narrowing or aliasing.</summary>
    [Fact]
    public void RowPointer_AllRowsAndPageBoundaries_MatchesIndependentOracle()
    {
        foreach (long page in new long[] { 0, 1, 0x7FFF, 0x8000, 0x7FFFFF, 0x800000, 0xFFFFFF })
        {
            for (int row = 0; row <= byte.MaxValue; row++)
            {
                uint pointer = LongValueStore.MakeRowPointer(page, row);
                Assert.Equal(checked((uint)((page * 256) + row)), pointer);
                Assert.Equal(page, LongValueStore.PageNumber(pointer));
                Assert.Equal(row, LongValueStore.RowIndex(pointer));
            }
        }
    }

    /// <summary>Production admission retains malformed-data and budget refusal priority.</summary>
    [Fact]
    public void ValidatePayload_EncodingAndBudgetFailures_KeepExactRefusalPriority()
    {
        JetCorruptDataException reserved = Assert.Throws<JetCorruptDataException>(() =>
            LongValueStore.ValidatePayload(new LongValueDescriptor(1, 192, 0, 0), 0, 4096, 0, 0));
        Assert.Equal(JetErrorCode.UnreadableLongValue, reserved.ErrorCode);
        JetLimitationException budget = Assert.Throws<JetLimitationException>(() =>
            LongValueStore.ValidatePayload(LongValueDescriptor.Inline(1), 0, 4096, 0, 0));
        Assert.Equal(JetErrorCode.ValueTooLarge, budget.ErrorCode);
        JetCorruptDataException truncated = Assert.Throws<JetCorruptDataException>(() =>
            LongValueStore.ValidatePayload(LongValueDescriptor.Inline(1), 0, 4096, 1, 0));
        Assert.Equal(JetErrorCode.UnreadableLongValue, truncated.ErrorCode);
        JetLimitationException rows = Assert.Throws<JetLimitationException>(() =>
            LongValueStore.ValidatePayload(LongValueDescriptor.Chained(1, 256, 0), 0, 4096, 1, 0));
        Assert.Equal(JetErrorCode.ValueTooLarge, rows.ErrorCode);
        LongValueStore.ValidatePayload(LongValueDescriptor.Chained(0, 256, 0), 0, 4096, 0, 0);
    }

    /// <summary>The total range selector agrees with mathematical addition even at unsigned limits.</summary>
    [Fact]
    public void DescriptorRange_FullWidthBounds_MatchesMathematicalOracle()
    {
        uint[] boundaries = [0, 1, 11, 12, 13, 16, int.MaxValue, 0x80000000, uint.MaxValue - 12, uint.MaxValue];
        foreach (uint bufferLength in boundaries)
        {
            foreach (uint start in boundaries)
            {
                foreach (uint count in boundaries)
                {
                    bool expected = count >= 12 && ((ulong)start + count) <= bufferLength;
                    Assert.Equal(expected, LongValueKernel.HasDescriptorRange(bufferLength, start, count));
                }
            }
        }

        byte[] bytes = new byte[12];
        LongValueKernel.ReadDescriptor(bytes, uint.MaxValue, uint.MaxValue, out bool valid, out uint length, out byte mode, out uint pointer, out uint token);
        Assert.False(valid);
        Assert.Equal(0u, length);
        Assert.Equal(0, mode);
        Assert.Equal(0u, pointer);
        Assert.Equal(0u, token);
    }

    private static void AssertDecodedWord(byte[] bytes)
    {
        uint word = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        Assert.Equal(word, LongValueKernel.DecodeWord(bytes[0], bytes[1], bytes[2], bytes[3]));
        Assert.Equal(word & 0x3FFFFFFF, LongValueKernel.DecodeLength(word));
        Assert.Equal(checked((byte)(word >> 30 << 6)), LongValueKernel.DecodeStorageMode(word));
        Assert.True(LongValueDescriptor.TryRead(bytes, out LongValueDescriptor decoded));
        Assert.Equal(checked((int)(word & 0x3FFFFFFF)), decoded.Length);
        Assert.Equal(checked((byte)(bytes[3] & 0xC0)), decoded.StorageMode);
        Assert.Equal(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(4)), decoded.FirstDp);
        Assert.Equal(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8)), decoded.Token);
    }
}
