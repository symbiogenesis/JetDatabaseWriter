namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Helpers;

internal static class RelationshipKeyBuilder
{
    public static string? Build(object?[] row, int[] columnIndexes)
        => IndexHelpers.BuildCompositeKey(row, columnIndexes);

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
        var identity = new int[count];
        for (int index = 0; index < identity.Length; index++)
        {
            identity[index] = index;
        }

        return identity;
    }
}
