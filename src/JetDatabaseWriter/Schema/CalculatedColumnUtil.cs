namespace JetDatabaseWriter.Schema;

using System;
using System.Buffers.Binary;
using System.Globalization;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
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
/// Bytes 0..3 carry the signed native status for the measured error forms;
/// ordinary cached values use zero. The rest of bytes 0..15 are reserved. Bytes 16..19
/// hold the little-endian uint32 payload length, and bytes 20.. carry the actual
/// value encoded per the column's result type. The reserved region is part of
/// the persisted format and the 23-byte total is invariant across all result
/// types; only the trailing payload length differs.
/// </remarks>
internal static class CalculatedColumnUtil
{
    internal static CalculatedError? ReadError(ReadOnlySpan<byte> data)
    {
        if (data.Length < Constants.CalculatedColumn.ExtraDataLen || Ri32(data, Constants.CalculatedColumn.DataLenOffset) != 0)
        {
            return null;
        }

        int code = Ri32(data, 0);
        if (code is not (5 or 6 or 11 or 13 or 94 or 440 or -3030))
        {
            return null;
        }

        foreach (byte value in data[sizeof(int)..])
        {
            if (value != 0)
            {
                return null;
            }
        }

        return new CalculatedError(code, data.Length);
    }

    internal static byte[] EncodeError(CalculatedError error, bool isOle = false)
    {
        byte[] data = new byte[isOle ? 27 : error.EnvelopeLength];
        Wi32(data, 0, error.NativeCode);
        return data;
    }

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

        throw new JetOperationException(JetErrorCode.InvalidOperationState, $"Unknown column type: {GetTypeDisplayName(type)}");
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

        throw new JetOperationException(JetErrorCode.InvalidOperationState, $"Unknown column type: {GetTypeDisplayName(type)}");
    }

    private static bool ReadBooleanPayload(ReadOnlySpan<byte> payload)
        => payload.Length > 0 && payload[0] != 0;

    private static object ReadNumericPayload(ReadOnlySpan<byte> payload, bool strict)
    {
        if (payload.Length < 4)
        {
            if (strict)
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, $"Calculated Numeric payload is too short (need at least 4 bytes, have {payload.Length}).");
            }

            return DBNull.Value;
        }

        if (payload.Length > 16)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    JetErrorCode.NumericOverflow,
                    $"Calculated Numeric payload is {payload.Length} bytes, which exceeds the native sixteen-byte Decimal layout.");
            }

            return DBNull.Value;
        }

        byte scale = payload[2];
        if (scale > 28)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    JetErrorCode.NumericOverflow,
                    $"Calculated Numeric scale {scale} exceeds the .NET decimal maximum of 28.");
            }

            return DBNull.Value;
        }

        // Native ACE ignores the reserved ushort, which is commonly 0 or 14.
        // It is not a length. Partial caches retain the fixed DECIMAL offsets
        // and zero-fill missing trailing bytes, including individual word bytes.
        Span<byte> padded = stackalloc byte[16];
        padded.Clear();
        payload.CopyTo(padded);

        uint hi = BinaryPrimitives.ReadUInt32LittleEndian(padded.Slice(4, 4));
        uint lo = BinaryPrimitives.ReadUInt32LittleEndian(padded.Slice(8, 4));
        uint mid = BinaryPrimitives.ReadUInt32LittleEndian(padded.Slice(12, 4));
        bool negative = payload[3] != 0;
        return new decimal(unchecked((int)lo), unchecked((int)mid), unchecked((int)hi), negative, scale);
    }
}
