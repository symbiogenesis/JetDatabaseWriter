namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Globalization;
using System.Numerics;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Schema.Models;

/// <summary>Encodes complete typed complex-parent predicates without lossy string conversion.</summary>
internal static class ComplexParentKeyComparer
{
    /// <summary>Builds a comparison key using the native column semantics.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="column">The predicate column.</param>
    /// <param name="value">The predicate or decoded value.</param>
    /// <returns>The complete comparison bytes.</returns>
    /// <exception cref="ArgumentException">The value is incompatible with the column.</exception>
    /// <exception cref="JetNotSupportedException">The column or text collation is unsupported.</exception>
    internal static byte[] Encode(JetFormat format, ColumnInfo column, object? value)
    {
        if (column.Type is ColumnType.ComplexType)
        {
            throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, $"Complex column '{column.Name}' cannot identify a parent row.", new JetErrorInfo { ColumnName = column.Name });
        }

        if (value is null or DBNull)
        {
            return [0];
        }

        if (column.Type is ColumnType.TextType or ColumnType.MemoType)
        {
            if (value is not string text)
            {
                throw new ArgumentException($"Column '{column.Name}' requires a Text value.", nameof(value));
            }

            byte[] comparison = new JetTextCollation(column.TextSortOrder).EncodeComparisonKey(text);
            return [1, .. comparison];
        }

        if (column.Type == ColumnType.BinaryType)
        {
            byte[] bytes = (byte[])IndexKeyEncoder.NormalizeFixedBinaryValue(value, fixedLength: 0)!;
            if (column.IsFixed && !column.IsCalculated && bytes.Length > column.Size)
            {
                throw new ArgumentException($"Binary key occupies {bytes.Length} bytes but its fixed column holds {column.Size}.", nameof(value));
            }

            // DAO compares supplied binary predicates at their actual length. Fixed
            // storage pads a row on write, but must not pad the caller's predicate.
            return IndexKeyEncoder.EncodeEntry(column.Type, bytes);
        }

        object normalized = value;
        if (!HasExactNumericPrecision(column, ref normalized))
        {
            // Ascending scalar entries begin with 0x7F; this token cannot equal
            // a stored numeric key and avoids manufacturing a match by rounding.
            return [2];
        }

        if (column.Type == ColumnType.FloatType && Convert.ToSingle(normalized, CultureInfo.InvariantCulture) == 0)
        {
            normalized = 0f;
        }
        else if (column.Type == ColumnType.DoubleType && Convert.ToDouble(normalized, CultureInfo.InvariantCulture) == 0)
        {
            normalized = 0d;
        }

        return IndexKeyEncoder.EncodeColumnEntry(format, column, normalized);
    }

    private static bool HasExactNumericPrecision(ColumnInfo column, ref object value)
    {
        if (column.Type is ColumnType.ByteType or ColumnType.IntegerType or ColumnType.LongIntegerType or ColumnType.BigIntType)
        {
            return value switch
            {
                double real => Math.Truncate(real) == real,
                float real => Math.Truncate(real) == real,
                _ => IsWholeDecimal(value),
            };
        }

        if (column.Type is ColumnType.MoneyType or ColumnType.NumericType)
        {
            int scale = column.Type == ColumnType.MoneyType ? 4 : column.NumericScale;
            if (!TryReadUnroundedDecimal(value, out decimal number) || decimal.Round(number, scale) != number)
            {
                return false;
            }

            value = number;
            return true;
        }

        return column.Type != ColumnType.FloatType || IsExactSingle(value);
    }

    private static bool IsWholeDecimal(object value)
    {
        decimal number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        return decimal.Truncate(number) == number;
    }

    private static bool TryReadUnroundedDecimal(object value, out decimal number)
    {
        // Convert.ToDecimal rounds binary floating-point operands to 7/15 digits.
        // Preserve their round-trip decimal precision before checking storage scale;
        // values too small or large for Decimal cannot identify a Decimal/Currency row.
        if (value is double real)
        {
            return decimal.TryParse(real.ToString("R", CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture, out number)
                && (number != 0 || real == 0);
        }

        if (value is float single)
        {
            return decimal.TryParse(single.ToString("R", CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture, out number)
                && (number != 0 || single == 0);
        }

        number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool IsExactSingle(object value)
    {
        float single = Convert.ToSingle(value, CultureInfo.InvariantCulture);
        if (float.IsNaN(single) || float.IsInfinity(single))
        {
            return false;
        }

        if (value is float)
        {
            return true;
        }

        if (value is double wide)
        {
            return single == wide;
        }

        decimal number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        int[] decimalBits = decimal.GetBits(number);
        BigInteger magnitude = ((BigInteger)unchecked((uint)decimalBits[2]) << 64)
            | ((BigInteger)unchecked((uint)decimalBits[1]) << 32)
            | unchecked((uint)decimalBits[0]);
        if (decimalBits[3] < 0)
        {
            magnitude = -magnitude;
        }

        int scale = (decimalBits[3] >> 16) & 0xFF;
        uint bits = unchecked((uint)BitConverter.SingleToInt32Bits(single));
        int exponent = (int)((bits >> 23) & 0xFF);
        BigInteger binary = (bits & 0x7FFFFF) | (exponent == 0 ? 0u : 0x800000u);
        if ((bits & 0x80000000) != 0)
        {
            binary = -binary;
        }

        exponent = exponent == 0 ? -149 : exponent - 150;
        BigInteger denominator = BigInteger.One;
        if (exponent < 0)
        {
            denominator <<= -exponent;
        }
        else
        {
            binary <<= exponent;
        }

        return magnitude * denominator == binary * BigInteger.Pow(10, scale);
    }
}
