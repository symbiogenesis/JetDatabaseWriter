namespace JetDatabaseWriter.ValueDecoding;

using System;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Reads one column of a row as a string without following long-value
/// chains: bool, fixed-width and inline variable (Text and Binary) columns.
/// Catalog walks use it for the scalar metadata columns of system tables.
/// </summary>
internal static class ScalarColumnReader
{
    /// <summary>
    /// Reads a single column value as a string, supporting bool, fixed-width and inline-var
    /// (Text / Binary) columns. Variable-width MEMO / OLE / Complex columns are NOT
    /// followed (they require LVAL chain traversal); those return <see cref="string.Empty"/>
    /// here.
    /// </summary>
    /// <param name="format">The file's format profile.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="column">The column.</param>
    /// <returns>The value as a string, or <see cref="string.Empty"/>.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the column type is unknown.</exception>
    internal static string DecodeSimpleColumnValue(JetFormat format, byte[] page, int rowStart, int rowSize, ColumnInfo column)
    {
        if (column == null || rowSize < format.RowFields.NumCols)
        {
            return string.Empty;
        }

        if (!RowDecodePlan.TryParseRowLayout(format.RowFields, page, rowStart, rowSize, hasVarColumns: true, out RowLayout layout))
        {
            return string.Empty;
        }

        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, rowStart, rowSize, layout, column);
        switch (slice.Kind)
        {
            case ColumnSliceKind.Bool:
                return slice.BoolValue ? "True" : "False";

            case ColumnSliceKind.Null:
            case ColumnSliceKind.Empty:
                return string.Empty;

            case ColumnSliceKind.Fixed:
                return ReadFixedString(page, rowStart + slice.DataStart, column, slice.DataLen);

            case ColumnSliceKind.Var:
                if (slice.DataLen <= 0)
                {
                    return string.Empty;
                }

                if (TryGetVariableSlotFixedPayloadSize(column.Type, out int required))
                {
                    return slice.DataLen >= required
                        ? ReadFixedString(page, rowStart + slice.DataStart, column, required)
                        : string.Empty;
                }

                if (column.Type == TextType)
                {
                    return format.DecodeText(page, rowStart + slice.DataStart, slice.DataLen);
                }

                if (column.Type == BinaryType)
                {
                    return ToHexStringNoSeparator(page.AsSpan(rowStart + slice.DataStart, slice.DataLen));
                }

                if (column.Type is BooleanType or OleType or MemoType)
                {
                    return string.Empty;
                }

                throw new InvalidOperationException($"Unknown column type: {GetTypeDisplayName(column.Type)}");

            default:
                return string.Empty;
        }
    }
}
