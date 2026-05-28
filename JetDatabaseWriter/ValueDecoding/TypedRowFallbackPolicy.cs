namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.IO;
using System.Runtime.ExceptionServices;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

internal static class TypedRowFallbackPolicy
{
    internal static object EmptyVariableValue(ColumnInfo column)
        => column.Type switch
        {
            TextType or MemoType => string.Empty,
            BinaryType or OleType => Array.Empty<byte>(),
            _ => DBNull.Value,
        };

    internal static object FixedVariableSlotTooShort(ColumnInfo column, int actualLength, int requiredLength, bool strictParsing)
    {
        if (strictParsing)
        {
            throw new InvalidDataException(
                $"Variable-area payload for column '{column.Name}' is too short for type 0x{column.Type:X2}: need {requiredLength} byte(s), found {Math.Max(0, actualLength)}.");
        }

        return DBNull.Value;
    }

    internal static object MalformedVariableValue(ColumnInfo column, Exception exception, bool strictParsing)
    {
        if (exception is JetLimitationException)
        {
            ExceptionDispatchInfo.Capture(exception).Throw();
        }

        if (strictParsing)
        {
            throw new InvalidDataException(
                $"Malformed variable-area payload for column '{column.Name}' (type 0x{column.Type:X2}).",
                exception);
        }

        return DBNull.Value;
    }
}
