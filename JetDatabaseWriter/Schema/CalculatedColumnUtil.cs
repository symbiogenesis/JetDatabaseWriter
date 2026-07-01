namespace JetDatabaseWriter.Schema;

using System;
using System.Buffers.Binary;
using System.Globalization;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using static JetDatabaseWriter.Constants;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Wrap / unwrap helpers for the 23-byte on-disk envelope every Access 2010+
/// calculated-column value carries. Translated from Jackcess
/// <c>CalculatedColumnUtil</c> (<c>wrapCalculatedValue</c> / <c>unwrapCalculatedValue</c>);
/// see <see href="docs/design/calculated-columns-format-notes.md" /> for the layout.
/// </summary>
/// <remarks>
/// Per Jackcess: bytes 0..15 of the wrapper are reserved (Access uses them for
/// version + CRC + scratch; we emit zeros and ignore them on read), bytes 16..19
/// hold the little-endian uint32 payload length, and bytes 20.. carry the actual
/// value encoded per the column's result type. The reserved region is part of
/// the persisted format and the 23-byte total is invariant across all result
/// types; only the trailing payload length differs.
/// </remarks>
internal static class CalculatedColumnUtil
{
    /// <summary>
    /// Returns a fresh byte array containing <paramref name="payload"/> wrapped
    /// in the 23-byte calculated-value envelope. <paramref name="payload"/>
    /// must already be encoded per the column's result type (the caller is
    /// responsible for that encoding \u2014 e.g. UCS-2 LE for <c>Text</c>,
    /// little-endian int32 for <c>LongInteger</c>).
    /// </summary>
    /// <param name="payload">The payload.</param>
    public static byte[] Wrap(byte[] payload)
    {
        Guard.NotNull(payload, nameof(payload));
        byte[] wrapped = new byte[CalculatedColumn.ExtraDataLen + payload.Length];
        Wi32(wrapped, CalculatedColumn.DataLenOffset, payload.Length);
        Buffer.BlockCopy(payload, 0, wrapped, CalculatedColumn.DataOffset, payload.Length);
        return wrapped;
    }

    /// <summary>
    /// Strips the 23-byte calculated-value envelope and returns the inner
    /// payload bytes. Returns <paramref name="data"/> unchanged when shorter
    /// than the wrapper header (defensive parity with Jackcess).
    /// </summary>
    /// <param name="data">The data bytes or values.</param>
    public static byte[] Unwrap(byte[] data)
    {
        Guard.NotNull(data, nameof(data));
        if (data.Length < CalculatedColumn.DataOffset)
        {
            return data;
        }

        int dataLen = Ri32(data, CalculatedColumn.DataLenOffset);
        int available = data.Length - CalculatedColumn.DataOffset;
        int copyLen = Math.Max(0, Math.Min(available, dataLen));
        byte[] unwrapped = new byte[copyLen];
        Buffer.BlockCopy(data, CalculatedColumn.DataOffset, unwrapped, 0, copyLen);
        return unwrapped;
    }

    internal static byte[] Unwrap(ReadOnlySpan<byte> data)
    {
        if (data.Length < CalculatedColumn.DataOffset)
        {
            return data.ToArray();
        }

        int dataLen = Ri32(data, CalculatedColumn.DataLenOffset);
        int available = data.Length - CalculatedColumn.DataOffset;
        int copyLen = Math.Max(0, Math.Min(available, dataLen));
        byte[] unwrapped = new byte[copyLen];
        data.Slice(CalculatedColumn.DataOffset, copyLen).CopyTo(unwrapped);
        return unwrapped;
    }

    internal static string ReadPayloadString(ReadOnlySpan<byte> payload, ColumnType type, bool strictNumeric)
    {
        if (type == BooleanType)
        {
            return ReadBooleanPayload(payload) ? "True" : "False";
        }

        if (type == NumericType)
        {
            object numeric = ReadNumericPayload(payload, strictNumeric);
            return numeric is decimal decimalValue
                ? decimalValue.ToString("G", CultureInfo.InvariantCulture)
                : string.Empty;
        }

        if (TryGetVariableSlotFixedPayloadSize(type, out int required))
        {
            return payload.Length >= required
                ? ReadFixedString(payload, 0, type, required, strictNumeric)
                : string.Empty;
        }

        if (type is BinaryType or TextType or OleType or MemoType)
        {
            return string.Empty;
        }

        throw new InvalidOperationException($"Unknown column type: {GetTypeDisplayName(type)}");
    }

    internal static object ReadPayloadTyped(ReadOnlySpan<byte> payload, ColumnType type, bool strictNumeric)
    {
        if (type == BooleanType)
        {
            return ReadBooleanPayload(payload);
        }

        if (type == NumericType)
        {
            return ReadNumericPayload(payload, strictNumeric);
        }

        if (TryGetVariableSlotFixedPayloadSize(type, out int required))
        {
            return payload.Length >= required
                ? ReadFixedTyped(payload, 0, type, required, strictNumeric)
                : DBNull.Value;
        }

        if (type is BinaryType or TextType or OleType or MemoType)
        {
            return DBNull.Value;
        }

        throw new InvalidOperationException($"Unknown column type: {GetTypeDisplayName(type)}");
    }

    private static bool ReadBooleanPayload(ReadOnlySpan<byte> payload)
        => payload.Length > 0 && payload[0] != 0;

    private static object ReadNumericPayload(ReadOnlySpan<byte> payload, bool strict)
    {
        if (payload.Length < 4)
        {
            if (strict)
            {
                throw new JetLimitationException($"Calculated Numeric payload is too short (need at least 4 bytes, have {payload.Length}).");
            }

            return DBNull.Value;
        }

        short storedLen = Ri16(payload, 0);
        int mantissaLen = AlignToFour((storedLen > 0 ? storedLen : payload.Length - 2) - 2);
        mantissaLen = Math.Min(mantissaLen, payload.Length - 4);
        if (mantissaLen <= 0)
        {
            return DBNull.Value;
        }

        if (mantissaLen > 12)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"Calculated Numeric mantissa is {mantissaLen} bytes, which exceeds the .NET decimal limit of 12 bytes.");
            }

            return DBNull.Value;
        }

        byte scale = payload[2];
        if (scale > 28)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"Calculated Numeric scale {scale} exceeds the .NET decimal maximum of 28.");
            }

            return DBNull.Value;
        }

        byte[] mantissa = payload.Slice(4, mantissaLen).ToArray();
        FixCalculatedNumericByteOrder(mantissa);

        Span<byte> padded = stackalloc byte[12];
        mantissa.CopyTo(padded[(12 - mantissa.Length)..]);

        uint hi = BinaryPrimitives.ReadUInt32BigEndian(padded[..4]);
        uint mid = BinaryPrimitives.ReadUInt32BigEndian(padded.Slice(4, 4));
        uint lo = BinaryPrimitives.ReadUInt32BigEndian(padded.Slice(8, 4));
        bool negative = payload[3] != 0;

        try
        {
            return new decimal(unchecked((int)lo), unchecked((int)mid), unchecked((int)hi), negative, scale);
        }
        catch (OverflowException ex)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"Calculated Numeric value overflow (hi=0x{hi:X8}, mid=0x{mid:X8}, lo=0x{lo:X8}, scale={scale})",
                    ex);
            }

            return DBNull.Value;
        }
    }

    private static int AlignToFour(int value)
        => value <= 0 ? 0 : (value + 3) / 4 * 4;

    private static void FixCalculatedNumericByteOrder(byte[] bytes)
    {
        int pos = 0;
        if (bytes.Length % 8 != 0 && bytes.Length >= 4)
        {
            Array.Reverse(bytes, 0, 4);
            pos = 4;
        }

        for (; pos + 8 <= bytes.Length; pos += 8)
        {
            Array.Reverse(bytes, pos, 8);
        }
    }
}
