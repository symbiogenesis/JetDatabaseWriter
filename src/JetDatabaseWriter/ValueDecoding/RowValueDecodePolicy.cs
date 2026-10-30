namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Runtime.ExceptionServices;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;

internal static class RowValueDecodePolicy
{
    internal static object MalformedFixedDate(ColumnInfo column, Exception exception, bool strictParsing)
        => MalformedDate(column, exception, strictParsing, "fixed");

    internal static object MalformedFixedValue(ColumnInfo column, Exception exception, bool strictParsing)
        => MalformedValue(column, exception, strictParsing, "fixed", JetTypeInfo.GetTypeDisplayName(column.Type));

    private static DBNull MalformedDate(ColumnInfo column, Exception exception, bool strictParsing, string storage)
        => MalformedValue(column, exception, strictParsing, storage, "Date/Time");

    private static DBNull MalformedValue(ColumnInfo column, Exception exception, bool strictParsing, string storage, string valueType)
    {
        if (strictParsing)
        {
            throw new JetCorruptDataException(
                JetErrorCode.MalformedValue,
                $"Malformed {storage} {valueType} payload for column '{column.Name}'.",
                new JetErrorInfo { ColumnName = column.Name, Reason = exception.Message },
                exception);
        }

        DiagnosticTrace.Warning($"The {storage} {valueType} value for column '{column.Name}' is unreadable and was returned as a missing value.");

        return DBNull.Value;
    }

    internal static bool IsMalformedValueException(Exception exception)
        => exception is ArgumentException or IndexOutOfRangeException or OverflowException or JetCorruptDataException { ErrorCode: JetErrorCode.MalformedValue };

    internal static bool HasFixedPayload(ColumnInfo column, int length, bool strictParsing)
    {
        if (!JetTypeInfo.TryGetVariableSlotFixedPayloadSize(column.Type, out int required) || length >= required)
        {
            return true;
        }

        _ = FixedVariableSlotTooShort(column, length, required, strictParsing);
        return false;
    }

    internal static byte[] UnwrapCalculatedPayload(ReadOnlySpan<byte> data)
    {
        if (data.Length < Constants.CalculatedColumn.ExtraDataLen)
        {
            throw new JetCorruptDataException(JetErrorCode.MalformedValue, "Calculated value envelope is truncated.");
        }

        int length = JetTypeInfo.Ri32(data, Constants.CalculatedColumn.DataLenOffset);

        // The 23-byte envelope includes three bytes after the cached payload.
        if (length < 0 || length > data.Length - Constants.CalculatedColumn.ExtraDataLen)
        {
            throw new JetCorruptDataException(JetErrorCode.MalformedValue, "Calculated value envelope declares an invalid payload length.");
        }

        return CalculatedColumnUtil.Unwrap(data);
    }

    internal static bool HasCalculatedPayload(ReadOnlySpan<byte> payload, ColumnType type)
    {
        // Native Access calculated scalar slots keep their envelope when the
        // cached result is null; an assigned result has a nonempty payload.
        if (payload.IsEmpty)
        {
            return false;
        }

        int required;
        if (type == BooleanType)
        {
            required = 1;
        }
        else if (type == NumericType)
        {
            required = 4;
        }
        else
        {
            required = JetTypeInfo.TryGetVariableSlotFixedPayloadSize(type, out int size) ? size : 0;
        }

        if (payload.Length < required)
        {
            throw new JetCorruptDataException(JetErrorCode.MalformedValue, $"Calculated payload needs {required} byte(s), found {payload.Length}.");
        }

        return true;
    }

    internal static byte[] UnwrapCalculatedScalarPayload(ReadOnlySpan<byte> data, ColumnInfo column)
    {
        byte[] payload = UnwrapCalculatedPayload(data);

        // DAO120 Byte overflow controls contain this exact 39-byte form.
        // Its empty payload is an error, unlike the 23-byte Null envelope.
        // Other result types and envelope forms remain separate native evidence
        // gaps; do not assign meaning to their reserved header bytes here.
        if (JetTypeInfo.ResolveValueType(column) == ByteType && data.Length == 39 && JetTypeInfo.Ri32(data, 0) == 6)
        {
            bool zeroTail = true;
            for (int index = sizeof(int); index < data.Length; index++)
            {
                zeroTail &= data[index] == 0;
            }

            if (zeroTail)
            {
                throw new JetLimitationException(
                    JetErrorCode.NumericOverflow,
                    $"Calculated Byte column '{column.Name}' contains a native calculation error (code 6); it cannot be read as an ordinary Null.",
                    new JetErrorInfo { ColumnName = column.Name, Reason = "Native calculated Byte error envelope, code 6." });
            }
        }

        return payload;
    }

    internal static object EmptyVariableValue(ColumnInfo column)
    {
        if (column.Type is TextType or MemoType)
        {
            return string.Empty;
        }

        if (column.Type is BinaryType or OleType)
        {
            return Array.Empty<byte>();
        }

        return DBNull.Value;
    }

    internal static object FixedVariableSlotTooShort(ColumnInfo column, int actualLength, int requiredLength, bool strictParsing)
        => MalformedVariableValue(
            column,
            new ArgumentException($"{JetTypeInfo.GetTypeDisplayName(JetTypeInfo.ResolveValueType(column))} payload needs {requiredLength} byte(s), found {Math.Max(0, actualLength)}."),
            strictParsing);

    internal static object MalformedVariableValue(ColumnInfo column, Exception exception, bool strictParsing)
    {
        if (!IsMalformedValueException(exception))
        {
            ExceptionDispatchInfo.Capture(exception).Throw();
        }

        return MalformedValue(
            column,
            exception,
            strictParsing,
            column.IsCalculated ? "calculated" : "variable-area",
            JetTypeInfo.GetTypeDisplayName(JetTypeInfo.ResolveValueType(column)));
    }
}
