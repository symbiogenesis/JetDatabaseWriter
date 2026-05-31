namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

internal static class RelationshipKeyBuilder
{
    public static string? Build(object?[] row, int[] columnIndexes)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < columnIndexes.Length; i++)
        {
            int idx = columnIndexes[i];
            if (idx < 0 || idx >= row.Length)
            {
                return null;
            }

            object? v = row[idx];
            if (v is null or DBNull)
            {
                return null;
            }

            sb.Append('|');
            AppendNormalized(sb, v);
        }

        return sb.ToString();
    }

    public static List<object?[]> ProjectNonNullKeys(IReadOnlyList<object?[]> rows, int[] columnIndexes)
    {
        var projectedRows = new List<object?[]>(rows.Count);
        foreach (object?[] row in rows)
        {
            object?[] projected = new object?[columnIndexes.Length];
            bool hasNullComponent = false;
            for (int columnIndex = 0; columnIndex < columnIndexes.Length; columnIndex++)
            {
                int sourceIndex = columnIndexes[columnIndex];
                if (sourceIndex < 0 || sourceIndex >= row.Length)
                {
                    hasNullComponent = true;
                    break;
                }

                object? value = row[sourceIndex];
                if (value is DBNull)
                {
                    value = null;
                }

                if (value == null)
                {
                    hasNullComponent = true;
                    break;
                }

                projected[columnIndex] = value;
            }

            if (!hasNullComponent)
            {
                projectedRows.Add(projected);
            }
        }

        return projectedRows;
    }

    public static HashSet<string> BuildSetFromProjectedKeys(IReadOnlyList<object?[]> keyRows)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        int[]? identity = null;
        foreach (object?[] keyRow in keyRows)
        {
            if (identity == null || identity.Length != keyRow.Length)
            {
                identity = CreateIdentityOrdinals(keyRow.Length);
            }

            string? key = Build(keyRow, identity);
            if (key != null)
            {
                _ = set.Add(key);
            }
        }

        return set;
    }

    public static int[] CreateIdentityOrdinals(int count)
    {
        int[] identity = new int[count];
        for (int index = 0; index < identity.Length; index++)
        {
            identity[index] = index;
        }

        return identity;
    }

    private static void AppendNormalized(StringBuilder sb, object value)
    {
        switch (value)
        {
            case string s:
                sb.Append('S').Append(':').Append(s.ToUpperInvariant());
                break;
            case Guid g:
                sb.Append('G').Append(':').Append(g.ToString("N"));
                break;
            case byte[] b:
                sb.Append('B').Append(':').Append(Convert.ToBase64String(b));
                break;
            case DateTime dt:
                sb.Append('D').Append(':').Append(dt.ToUniversalTime().Ticks.ToString(CultureInfo.InvariantCulture));
                break;
            case bool bl:
                sb.Append('?').Append(':').Append(bl ? '1' : '0');
                break;
            case IConvertible c:
                try
                {
                    decimal d = c.ToDecimal(CultureInfo.InvariantCulture);
                    sb.Append('N').Append(':').Append(d.ToString(CultureInfo.InvariantCulture));
                }
                catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
                {
                    sb.Append('X').Append(':').Append(value.ToString() ?? string.Empty);
                }

                break;
            default:
                sb.Append('X').Append(':').Append(value.ToString() ?? string.Empty);
                break;
        }
    }
}
