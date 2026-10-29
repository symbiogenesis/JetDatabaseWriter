namespace JetDatabaseWriter.LongValues.Models;

using System;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Verification;

/// <summary>
/// Parsed 12-byte JET long-value descriptor stored in MEMO/OLE row bodies.
/// </summary>
/// <param name="Length">The length.</param>
/// <param name="StorageMode">The storage mode.</param>
/// <param name="FirstDp">The first data page.</param>
/// <param name="Token">The token.</param>
internal readonly record struct LongValueDescriptor(int Length, byte StorageMode, uint FirstDp, uint Token)
{
    /// <summary>The maximum native length; the upper two bits of the length word select storage mode.</summary>
    internal const int MaxLength = 0x3FFFFFFF;

    public bool IsInline => this.StorageMode == Constants.LongValue.InlineStorageMode;

    public bool IsSinglePage => this.StorageMode == Constants.LongValue.SinglePageStorageMode;

    public bool IsExternal => !this.IsInline;

    public bool UsesChainedPages => this.IsExternal && !this.IsSinglePage;

    public static LongValueDescriptor Inline(int length) => new(length, Constants.LongValue.InlineStorageMode, 0, 0);

    public static LongValueDescriptor SinglePage(int length, uint firstDp, uint token)
        => new(length, Constants.LongValue.SinglePageStorageMode, firstDp, token);

    public static LongValueDescriptor Chained(int length, uint firstDp, uint token)
        => new(length, Constants.LongValue.ChainedStorageMode, firstDp, token);

    public static bool TryRead(byte[] source, int start, int count, out LongValueDescriptor descriptor)
    {
        descriptor = default;
        if (start < 0 || count < 0)
        {
            return false;
        }

        LongValueKernel.ReadDescriptor(source, checked((uint)start), checked((uint)count), out bool valid, out uint length, out byte mode, out uint firstDp, out uint token);
        if (valid)
        {
            descriptor = new LongValueDescriptor(checked((int)length), mode, firstDp, token);
        }

        return valid;
    }

    public static bool TryRead(ReadOnlySpan<byte> source, out LongValueDescriptor descriptor)
    {
        descriptor = default;
        uint count = checked((uint)source.Length);
        if (!LongValueKernel.HasDescriptorRange(count, 0, count))
        {
            return false;
        }

        LongValueKernel.DecodeFields(source[0], source[1], source[2], source[3], source[4], source[5], source[6], source[7], source[8], source[9], source[10], source[11], out uint length, out byte mode, out uint firstDp, out uint token);
        descriptor = new LongValueDescriptor(checked((int)length), mode, firstDp, token);
        return true;
    }

    public byte[] ToHeaderBytes()
    {
        byte[] header = new byte[Constants.LongValue.HeaderSize];
        this.WriteTo(header);
        return header;
    }

    public void WriteTo(byte[] destination)
    {
        if (destination.Length < Constants.LongValue.HeaderSize)
        {
            throw new ArgumentException("The destination span is too small for a long-value descriptor.", nameof(destination));
        }

        _ = this.EncodeLengthAndMode();
        if (!LongValueKernel.WriteDescriptor(destination, 0, checked((uint)destination.Length), checked((uint)this.Length), this.StorageMode, this.FirstDp, this.Token))
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The long-value descriptor could not be serialized.");
        }
    }

    public void WriteTo(Span<byte> destination)
    {
        uint count = checked((uint)destination.Length);
        if (!LongValueKernel.HasDescriptorRange(count, 0, count))
        {
            throw new ArgumentException("The destination span is too small for a long-value descriptor.", nameof(destination));
        }

        uint lengthWithFlags = this.EncodeLengthAndMode();
        for (byte position = 0; position < sizeof(uint); position++)
        {
            destination[position] = LongValueKernel.WordByte(lengthWithFlags, position);
            destination[4 + position] = LongValueKernel.WordByte(this.FirstDp, position);
            destination[8 + position] = LongValueKernel.WordByte(this.Token, position);
        }
    }

    private uint EncodeLengthAndMode()
    {
        if (this.Length < 0)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The native long-value length must fit its 30-bit field.");
        }

        ulong packed = LongValueKernel.EncodeLengthAndMode(checked((uint)this.Length), this.StorageMode);
        if (packed == 4294967296)
        {
            if (LongValueKernel.IsSupportedStorageMode(this.StorageMode))
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The native long-value length must fit its 30-bit field.");
            }

            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The long-value descriptor storage mode is unsupported.");
        }

        return checked((uint)packed);
    }
}
