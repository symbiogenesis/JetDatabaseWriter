namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Schema.Models;

internal static class RelationshipKeyBuilder
{
    public static string? Build(object?[] row, int[] columnIndexes, int[]? fixedBinaryLengths = null)
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
            int fixedLength = fixedBinaryLengths is null ? 0 : fixedBinaryLengths[i];
            object normalized = fixedLength > 0 || v is byte[] or ArraySegment<byte> or Memory<byte> or ReadOnlyMemory<byte>
                ? IndexKeyEncoder.NormalizeFixedBinaryValue(v, fixedLength)!
                : v;
            AppendNormalized(sb, normalized);
        }

        return sb.ToString();
    }

    public static string? Build(object?[] row, int[] columnIndexes, TableDef definition)
        => Build(row, columnIndexes, GetFixedBinaryLengths(definition, columnIndexes));

    public static int[] GetFixedBinaryLengths(TableDef definition, int[] columnIndexes)
    {
        int[] lengths = new int[columnIndexes.Length];
        for (int index = 0; index < lengths.Length; index++)
        {
            ColumnInfo column = definition.Columns[columnIndexes[index]];
            lengths[index] = column.Type == ColumnType.BinaryType && column.IsFixed && !column.IsCalculated ? column.Size : 0;
        }

        return lengths;
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

    public static HashSet<string> BuildSetFromProjectedKeys(IReadOnlyList<object?[]> keyRows, int[]? fixedBinaryLengths = null)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        int[]? identity = null;
        foreach (object?[] keyRow in keyRows)
        {
            if (identity == null || identity.Length != keyRow.Length)
            {
                identity = CreateIdentityOrdinals(keyRow.Length);
            }

            string? key = Build(keyRow, identity, fixedBinaryLengths);
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
                string normalized = s.ToUpperInvariant();
                sb.Append('S').Append(':').Append(normalized.Length.ToString(CultureInfo.InvariantCulture)).Append(':').Append(normalized);
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
