namespace JetDatabaseWriter.Scaffold;

using System;

/// <summary>
/// Converts Access table/column names into valid C# identifiers.
/// </summary>
internal static class NameCleaner
{
    /// <summary>Converts a table name to PascalCase class name.</summary>
    public static string ToClassName(string tableName)
    {
        return SanitizeToPascalCase(tableName);
    }

    /// <summary>Converts a column name to PascalCase property name.</summary>
    public static string ToPropertyName(string columnName)
    {
        return SanitizeToPascalCase(columnName);
    }

    private static string SanitizeToPascalCase(string raw)
    {
        // Reserve index 0 for a possible '_' prefix when the first char is a digit.
        Span<char> buffer = raw.Length < 128
            ? stackalloc char[raw.Length + 1]
            : new char[raw.Length + 1];

        int len = 1; // start writing at index 1
        bool capitalizeNext = true;

        foreach (char c in raw)
        {
            if (c is ' ' or '-' or '.' or '_')
            {
                if (len > 1)
                {
                    capitalizeNext = true;
                }
            }
            else if (char.IsLetterOrDigit(c))
            {
                buffer[len++] = capitalizeNext
                    ? char.ToUpperInvariant(c)
                    : c;
                capitalizeNext = false;
            }
        }

        if (len == 1)
        {
            return "Unknown";
        }

        int start = 1;
        if (char.IsDigit(buffer[1]))
        {
            buffer[0] = '_';
            start = 0;
        }

        // PascalCase always uppercases the first letter, so the result
        // can never collide with a C# keyword (all fully lowercase).
        return new string(buffer[start..len]);
    }
}
