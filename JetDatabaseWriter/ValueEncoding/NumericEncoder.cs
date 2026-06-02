namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Globalization;
using System.Numerics;
using JetDatabaseWriter.ValueEncoding.Models;
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
    /// Converts <paramref name="value"/> into fixed-point NUMERIC payload
    /// metadata and writes the target-scale unsigned magnitude as 16
    /// big-endian bytes into <paramref name="magnitudeBe"/>.
    /// </summary>
    /// <param name="value">The decimal value.</param>
    /// <param name="targetScale">The fixed-point scale to encode at.</param>
    /// <param name="magnitudeBe">The 16-byte big-endian unsigned magnitude destination.</param>
    /// <param name="payload">The fixed-point payload metadata.</param>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="magnitudeBe"/> is smaller than 16 bytes or
    /// when <paramref name="targetScale"/> is below the value's natural scale.
    /// </exception>
    public static bool TryEncodeFixedPointPayload(
        decimal value,
        int targetScale,
        Span<byte> magnitudeBe,
        out FixedPointPayload payload)
    {
        if (magnitudeBe.Length < 16)
        {
            throw new ArgumentException("Numeric fixed-point magnitude buffer must be at least 16 bytes.", nameof(magnitudeBe));
        }

        Span<byte> output = magnitudeBe[..16];
        output.Clear();

        bool negative = value < 0;
        decimal magnitudeValue = negative ? decimal.Negate(value) : value;

        byte[] leMantissa = new byte[13];
        Decompose(magnitudeValue, leMantissa.AsSpan(0, 12), out _, out int naturalScale);
        if (targetScale < naturalScale)
        {
            throw new ArgumentException(
                $"targetScale ({targetScale}) must be >= the value's natural scale ({naturalScale}).",
                nameof(targetScale));
        }

        var magnitude = new BigInteger(leMantissa);
        if (targetScale > naturalScale)
        {
            magnitude *= BigInteger.Pow(10, targetScale - naturalScale);
        }

        int digitCount = magnitude.IsZero ? 1 : magnitude.ToString(CultureInfo.InvariantCulture).Length;
        byte[] magnitudeLe = magnitude.ToByteArray();
        int magnitudeLength = magnitudeLe.Length;
        while (magnitudeLength > 0 && magnitudeLe[magnitudeLength - 1] == 0)
        {
            magnitudeLength--;
        }

        payload = new FixedPointPayload(negative, naturalScale, digitCount, magnitudeLength);
        if (magnitudeLength > 16)
        {
            return false;
        }

        for (int i = 0; i < magnitudeLength; i++)
        {
            output[16 - 1 - i] = magnitudeLe[i];
        }

        return true;
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
