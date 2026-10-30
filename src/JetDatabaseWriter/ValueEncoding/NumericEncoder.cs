namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;
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
    /// <summary>Projects a source value with the exact coefficient and scale its ordinary Numeric cell stores.</summary>
    /// <param name="column">The physical Numeric column.</param>
    /// <param name="value">The value before storage rounding.</param>
    /// <returns>The fixed-scale stored Decimal.</returns>
    /// <exception cref="JetLimitationException">The coefficient or declared column precision cannot hold the value.</exception>
    internal static decimal NormalizeStoredValue(ColumnInfo column, decimal value)
    {
        Span<byte> magnitude = stackalloc byte[16];
        FixedPointPayload payload = EncodeColumnPayload(column, value, magnitude);
        if (BinaryPrimitives.ReadUInt32BigEndian(magnitude) != 0)
        {
            throw new JetLimitationException(JetErrorCode.NumericOverflow, "The stored Numeric coefficient exceeds the .NET Decimal range.");
        }

        return new decimal(
            unchecked((int)BinaryPrimitives.ReadUInt32BigEndian(magnitude[12..])),
            unchecked((int)BinaryPrimitives.ReadUInt32BigEndian(magnitude.Slice(8, 4))),
            unchecked((int)BinaryPrimitives.ReadUInt32BigEndian(magnitude.Slice(4, 4))),
            payload.Negative,
            column.NumericScale);
    }

    /// <summary>Validates and encodes an ordinary Numeric column's target-scale magnitude.</summary>
    /// <param name="column">The physical Numeric column.</param>
    /// <param name="value">The value before storage rounding.</param>
    /// <param name="magnitudeBe">The sixteen-byte big-endian magnitude destination.</param>
    /// <returns>The magnitude metadata.</returns>
    /// <exception cref="JetLimitationException">The value exceeds the declared precision or sixteen-byte magnitude.</exception>
    internal static FixedPointPayload EncodeColumnPayload(ColumnInfo column, decimal value, Span<byte> magnitudeBe)
    {
        byte precision = column.NumericPrecision == 0 ? (byte)18 : column.NumericPrecision;
        byte scale = column.NumericScale;
        decimal rounded = decimal.Round(value, scale, MidpointRounding.ToEven);
        bool fits = TryEncodeFixedPointPayload(rounded, scale, magnitudeBe, out FixedPointPayload payload);
        if (payload.DigitCount > precision)
        {
            throw new JetLimitationException(JetErrorCode.NumericOverflow, $"Numeric value '{value}' exceeds NUMERIC({precision},{scale}) precision after rounding.");
        }

        if (!fits)
        {
            throw new JetLimitationException(JetErrorCode.NumericOverflow, $"Numeric value '{value}' requires {payload.MagnitudeByteCount} bytes, exceeding the 16-byte NUMERIC mantissa.");
        }

        return payload;
    }

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
#if NET5_0_OR_GREATER
        Span<int> bits = stackalloc int[4];
        _ = decimal.GetBits(value, bits);
#else
        int[] bits = decimal.GetBits(value);
#endif
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

        Span<byte> leMantissa = stackalloc byte[12];
        Decompose(magnitudeValue, leMantissa, out _, out int naturalScale);
        if (targetScale < naturalScale)
        {
            throw new ArgumentException(
                $"targetScale ({targetScale}) must be >= the value's natural scale ({naturalScale}).",
                nameof(targetScale));
        }

        var magnitude = new BigInteger(leMantissa, isUnsigned: true);
        if (targetScale > naturalScale)
        {
            magnitude *= BigInteger.Pow(10, targetScale - naturalScale);
        }

        int digitCount = magnitude.IsZero ? 1 : magnitude.ToString(CultureInfo.InvariantCulture).Length;
        int magnitudeLength = magnitude.IsZero ? 0 : magnitude.GetByteCount(isUnsigned: true);

        payload = new FixedPointPayload(negative, naturalScale, digitCount, magnitudeLength);
        if (magnitudeLength > 16)
        {
            return false;
        }

        return magnitudeLength == 0
            || magnitude.TryWriteBytes(output[(16 - magnitudeLength)..], out _, isUnsigned: true, isBigEndian: true);
    }
}
