namespace JetDatabaseWriter.Internal.Helpers;

using System;
using System.Buffers.Binary;

/// <summary>
/// Shared <see cref="decimal"/> decomposition for the JET 17-byte NUMERIC
/// column slot (<c>AccessWriter.EncodeNumericValue</c>) and the index-key
/// encoder (<c>IndexKeyEncoder.EncodeNumericKey</c>). Both formats start
/// from the same 96-bit unsigned mantissa + sign + scale extracted from
/// <see cref="decimal.GetBits(decimal)"/>.
/// </summary>
internal static class DecimalNumeric
{
    /// <summary>
    /// Decomposes <paramref name="value"/> into sign, scale (0..28), and the
    /// unsigned 96-bit mantissa, writing the mantissa as 12 little-endian
    /// bytes into <paramref name="mantissaLe"/>.
    /// </summary>
    public static void Decompose(decimal value, Span<byte> mantissaLe, out bool negative, out int scale)
    {
        int[] bits = decimal.GetBits(value);
        int flags = bits[3];
        negative = (flags & unchecked((int)0x80000000)) != 0;
        scale = (flags >> 16) & 0x7F;
        BinaryPrimitives.WriteInt32LittleEndian(mantissaLe.Slice(0, 4), bits[0]);
        BinaryPrimitives.WriteInt32LittleEndian(mantissaLe.Slice(4, 4), bits[1]);
        BinaryPrimitives.WriteInt32LittleEndian(mantissaLe.Slice(8, 4), bits[2]);
    }

    /// <summary>
    /// Counts the decimal digits of the unsigned 96-bit mantissa whose
    /// little-endian bytes were produced by <see cref="Decompose"/>, clamped
    /// to <c>1..28</c> (the range Access stores in the NUMERIC precision byte).
    /// </summary>
    public static byte ComputePrecision(ReadOnlySpan<byte> mantissaLe)
    {
        int lo = BinaryPrimitives.ReadInt32LittleEndian(mantissaLe.Slice(0, 4));
        int mid = BinaryPrimitives.ReadInt32LittleEndian(mantissaLe.Slice(4, 4));
        int hi = BinaryPrimitives.ReadInt32LittleEndian(mantissaLe.Slice(8, 4));
        var mantissa = new decimal(lo, mid, hi, isNegative: false, scale: 0);
        byte precision = 1;
        while (mantissa >= 10m)
        {
            mantissa = decimal.Truncate(mantissa / 10m);
            precision++;
        }

        return precision > 28 ? (byte)28 : precision;
    }
}
