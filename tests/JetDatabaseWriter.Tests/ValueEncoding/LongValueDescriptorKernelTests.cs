namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Exceptions;
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
