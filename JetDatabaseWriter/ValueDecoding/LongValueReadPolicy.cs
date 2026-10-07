namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Diagnostics;
using System.IO;

internal static class LongValueReadPolicy
{
    internal static object MissingValue(string columnName, InvalidDataException failure, bool strictParsing)
    {
        if (strictParsing)
        {
            throw new InvalidDataException($"The stored long value for column '{columnName}' is unreadable.", failure);
        }

        try
        {
            Trace.TraceWarning("The stored long value for column '{0}' is unreadable and was returned as a missing value.", columnName);
        }
#pragma warning disable CA1031 // A diagnostic listener must not turn an explicitly lenient read into a failure.
        catch (Exception)
#pragma warning restore CA1031 // A diagnostic listener must not turn an explicitly lenient read into a failure.
        {
            return DBNull.Value;
        }

        return DBNull.Value;
    }
}
