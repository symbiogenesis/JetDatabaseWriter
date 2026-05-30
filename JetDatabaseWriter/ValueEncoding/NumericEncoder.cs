namespace JetDatabaseWriter.ValueEncoding;

using System;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Shared <see cref="decimal"/> decomposition for the JET 17-byte NUMERIC
/// column slot (<c>AccessWriter.EncodeNumericValue</c>) and the index-key
/// encoder (<c>IndexKeyEncoder.EncodeNumericKey</c>). Both formats start
/// from the same 96-bit unsigned mantissa + sign + scale extracted from
/// <see cref="decimal.GetBits(decimal)"/>.
/// </summary>
internal static class NumericEncoder
{
    /// <summary>
    /// Decomposes <paramref name="value"/> into sign, scale (0..28), and the
    /// unsigned 96-bit mantissa, writing the mantissa as 12 little-endian
    /// bytes into <paramref name="mantissaLe"/>.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="mantissaLe">The mantissa le.</param>
    /// <param name="negative">The negative.</param>
    /// <param name="scale">The scale.</param>
    public static void Decompose(decimal value, Span<byte> mantissaLe, out bool negative, out int scale)
    {
        int[] bits = decimal.GetBits(value);
        int flags = bits[3];
        negative = (flags & unchecked((int)0x80000000)) != 0;
        scale = (flags >> 16) & 0x7F;
        Wi32(mantissaLe, 0, bits[0]);
        Wi32(mantissaLe, 4, bits[1]);
        Wi32(mantissaLe, 8, bits[2]);
    }

    /// <summary>
    /// Counts the decimal digits of the unsigned 96-bit mantissa whose
    /// little-endian bytes were produced by <see cref="Decompose"/>, clamped
    /// to <c>1..28</c> (the range Access stores in the NUMERIC precision byte).
    /// </summary>
    /// <param name="mantissaLe">The mantissa le.</param>
    public static byte ComputePrecision(ReadOnlySpan<byte> mantissaLe)
    {
        int lo = Ri32(mantissaLe, 0);
        int mid = Ri32(mantissaLe, 4);
        int hi = Ri32(mantissaLe, 8);
        decimal mantissa = new(lo, mid, hi, isNegative: false, scale: 0);
        byte precision = 1;
        while (mantissa >= 10m)
        {
            mantissa = decimal.Truncate(mantissa / 10m);
            precision++;
        }

        return precision > 28 ? (byte)28 : precision;
    }
}
