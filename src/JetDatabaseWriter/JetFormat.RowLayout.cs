namespace JetDatabaseWriter;

using System;

internal sealed partial class JetFormat
{
    /// <summary>
    /// Returns the number of bytes needed to hold a row's null bitmap: one bit
    /// per column, rounded up to a whole byte.
    /// </summary>
    /// <param name="columnCount">The number of columns in the row.</param>
    internal static int GetNullMaskSizeBytes(int columnCount) => (columnCount + 7) / 8;

    /// <summary>
    /// Returns <see langword="true"/> when the null-bitmap bit for
    /// <paramref name="columnNumber"/> is set in <paramref name="mask"/> — the column
    /// is present (non-null), or for a Boolean column holds <see langword="true"/>.
    /// Returns <see langword="false"/> when the bit lies outside <paramref name="mask"/>.
    /// </summary>
    /// <param name="mask">The row's null-bitmap bytes (bit 0 = column 0).</param>
    /// <param name="columnNumber">The zero-based column number.</param>
    internal static bool IsNullMaskBitSet(ReadOnlySpan<byte> mask, int columnNumber)
    {
        if (columnNumber < 0)
        {
            return false;
        }

        int byteOffset = columnNumber / 8;
        return byteOffset < mask.Length
            && (mask[byteOffset] & (1 << (columnNumber % 8))) != 0;
    }

    /// <summary>
    /// Sets or clears the null-bitmap bit for <paramref name="columnNumber"/> in
    /// <paramref name="mask"/>. No-ops when the bit lies outside <paramref name="mask"/>.
    /// </summary>
    /// <param name="mask">The row's null-bitmap bytes (bit 0 = column 0).</param>
    /// <param name="columnNumber">The zero-based column number.</param>
    /// <param name="state">The bit value to write.</param>
    internal static void SetNullMaskBit(byte[] mask, int columnNumber, bool state)
        => SetNullMaskBit(mask.AsSpan(), columnNumber, state);

    /// <summary>
    /// Sets or clears the null-bitmap bit for <paramref name="columnNumber"/> in the
    /// span-backed <paramref name="mask"/>. No-ops when the bit lies outside
    /// <paramref name="mask"/>.
    /// </summary>
    /// <param name="mask">The row's null-bitmap bytes (bit 0 = column 0).</param>
    /// <param name="columnNumber">The zero-based column number.</param>
    /// <param name="state">The bit value to write.</param>
    internal static void SetNullMaskBit(Span<byte> mask, int columnNumber, bool state)
    {
        if (columnNumber < 0)
        {
            return;
        }

        int byteOffset = columnNumber / 8;
        int bitOffset = columnNumber % 8;
        if (byteOffset >= mask.Length)
        {
            return;
        }

        if (state)
        {
            mask[byteOffset] |= (byte)(1 << bitOffset);
        }
        else
        {
            mask[byteOffset] &= unchecked((byte)~(1 << bitOffset));
        }
    }
}
