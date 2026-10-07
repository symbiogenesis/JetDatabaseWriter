namespace JetDatabaseWriter.LongValues.Models;

using System;
using System.Buffers.Binary;

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

    public static bool TryRead(ReadOnlySpan<byte> source, out LongValueDescriptor descriptor)
    {
        descriptor = default;
        if (source.Length < Constants.LongValue.HeaderSize)
        {
            return false;
        }

        int length = BinaryPrimitives.ReadInt32LittleEndian(source) & MaxLength;
        byte storageMode = (byte)(source[3] & Constants.LongValue.StorageModeMask);
        uint firstDp = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(4, 4));
        uint token = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(8, 4));
        descriptor = new LongValueDescriptor(length, storageMode, firstDp, token);
        return true;
    }

    public byte[] ToHeaderBytes()
    {
        byte[] header = new byte[Constants.LongValue.HeaderSize];
        this.WriteTo(header);
        return header;
    }

    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < Constants.LongValue.HeaderSize)
        {
            throw new ArgumentException("The destination span is too small for a long-value descriptor.", nameof(destination));
        }

        if (this.Length is < 0 or > MaxLength)
        {
            throw new ArgumentOutOfRangeException(nameof(this.Length), "The native long-value length must fit its 30-bit field.");
        }

        uint lengthWithFlags = checked((uint)this.Length) | ((uint)this.StorageMode << 24);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, lengthWithFlags);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(4, 4), this.FirstDp);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(8, 4), this.Token);
    }
}
