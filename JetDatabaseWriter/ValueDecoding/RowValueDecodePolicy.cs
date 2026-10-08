namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Diagnostics;
using System.IO;
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

    private static DBNull MalformedDate(ColumnInfo column, Exception exception, bool strictParsing, string storage)
    {
        if (strictParsing)
        {
            throw new JetCorruptDataException(
                JetErrorCode.MalformedValue,
                $"Malformed {storage} Date/Time payload for column '{column.Name}'.",
                new JetErrorInfo { ColumnName = column.Name, Reason = exception.Message },
                exception);
        }

        try
        {
            Trace.TraceWarning("The {0} Date/Time value for column '{1}' is unreadable and was returned as a missing value.", storage, column.Name);
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
    {
        if (JetTypeInfo.ResolveValueType(column) is DateTimeType or DateTimeExtendedType)
        {
            return MalformedDate(column, new ArgumentException($"Date/Time payload needs {requiredLength} byte(s), found {Math.Max(0, actualLength)}."), strictParsing, column.IsCalculated ? "calculated" : "variable-area");
        }

        if (strictParsing)
        {
            throw new InvalidDataException(
                $"Variable-area payload for column '{column.Name}' is too short for type {JetTypeInfo.GetTypeDisplayName(column.Type)}: need {requiredLength} byte(s), found {Math.Max(0, actualLength)}.");
        }

        return DBNull.Value;
    }

    internal static object MalformedVariableValue(ColumnInfo column, Exception exception, bool strictParsing)
    {
        if (exception is JetLimitationException)
        {
            ExceptionDispatchInfo.Capture(exception).Throw();
        }

        if (JetTypeInfo.ResolveValueType(column) is DateTimeType or DateTimeExtendedType)
        {
            return MalformedDate(column, exception, strictParsing, column.IsCalculated ? "calculated" : "variable-area");
        }

        if (strictParsing)
        {
            throw new InvalidDataException(
                $"Malformed variable-area payload for column '{column.Name}' (type {JetTypeInfo.GetTypeDisplayName(column.Type)}).",
                exception);
        }

        return DBNull.Value;
    }
}
