namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Runtime.ExceptionServices;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

internal static class LongValueReadPolicy
{
    internal static object MissingValue(string columnName, Exception failure, bool strictParsing)
    {
        if (failure is not JetCorruptDataException)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
        }

        if (strictParsing)
        {
            throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, $"The stored long value for column '{columnName}' is unreadable.", new JetErrorInfo { ColumnName = columnName }, failure);
        }

        DiagnosticTrace.Warning($"The stored long value for column '{columnName}' is unreadable and was returned as a missing value.");

        return DBNull.Value;
    }
}
