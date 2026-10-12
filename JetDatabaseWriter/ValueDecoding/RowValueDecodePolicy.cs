namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
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

        try
        {
            Trace.TraceWarning("The {0} {1} value for column '{2}' is unreadable and was returned as a missing value.", storage, valueType, column.Name);
        }
#pragma warning disable CA1031 // A diagnostic listener must not turn an explicitly lenient read into a failure.
        catch (Exception)
#pragma warning restore CA1031
        {
            return DBNull.Value;
        }

        return DBNull.Value;
    }

    internal static bool IsMalformedValueException(Exception exception)
        => exception is ArgumentException or IndexOutOfRangeException or OverflowException;

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
        if (data.Length < Constants.CalculatedColumn.DataOffset)
        {
            throw new ArgumentException("Calculated value envelope is truncated.", nameof(data));
        }

        int length = JetTypeInfo.Ri32(data, Constants.CalculatedColumn.DataLenOffset);
        if (length < 0 || length > data.Length - Constants.CalculatedColumn.DataOffset)
        {
            throw new ArgumentException("Calculated value envelope declares an invalid payload length.", nameof(data));
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
            throw new ArgumentException($"Calculated payload needs {required} byte(s), found {payload.Length}.", nameof(payload));
        }

        return true;
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
