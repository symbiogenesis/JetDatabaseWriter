namespace JetDatabaseWriter.Scaffold;

using System;

/// <summary>
/// Converts Access table/column names into valid C# identifiers.
/// </summary>
internal static class NameCleaner
{
    /// <summary>Converts a table name to PascalCase class name.</summary>
    /// <param name="tableName">The table name.</param>
    internal static string ToClassName(string tableName) => SanitizeToPascalCase(tableName);

    /// <summary>Converts a column name to PascalCase property name.</summary>
    /// <param name="columnName">The column name.</param>
    internal static string ToPropertyName(string columnName) => SanitizeToPascalCase(columnName);

    /// <summary>
    /// Produces a simple English plural of a PascalCase identifier for collection
    /// navigation names (for example <c>Order</c> -&gt; <c>Orders</c>,
    /// <c>Category</c> -&gt; <c>Categories</c>, <c>Address</c> -&gt; <c>Addresses</c>).
    /// </summary>
    /// <param name="name">The singular identifier.</param>
    internal static string Pluralize(string name)
    {
        if (name.Length == 0)
        {
            return name;
        }

        char last = name[^1];
        if ((last is 'y' or 'Y') && name.Length >= 2 && !IsVowel(name[^2]))
        {
            return name[..^1] + "ies";
        }

        if (last is 's' or 'S' or 'x' or 'X' or 'z' or 'Z'
            || name.EndsWith("ch", StringComparison.OrdinalIgnoreCase)
            || name.EndsWith("sh", StringComparison.OrdinalIgnoreCase))
        {
            return name + "es";
        }

        return name + "s";
    }

    private static bool IsVowel(char c) => c is 'a' or 'e' or 'i' or 'o' or 'u' or 'A' or 'E' or 'I' or 'O' or 'U';

    internal static string SanitizeToPascalCase(string raw)
    {
        if (raw.Length == 0)
        {
            return "Unknown";
        }

        // Fast path: if already a valid PascalCase identifier, return as-is.
        if (char.IsUpper(raw[0]))
        {
            bool clean = true;
            for (int i = 1; i < raw.Length; i++)
            {
                if (!char.IsLetterOrDigit(raw[i]))
                {
                    clean = false;
                    break;
                }
            }

            if (clean)
            {
                return raw;
            }
        }

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
