namespace JetDatabaseWriter.ComplexColumns;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Schema.Models;

/// <summary>Encodes complete typed complex-parent predicates without lossy string conversion.</summary>
internal static class ComplexParentKeyComparer
{
    /// <summary>Builds a comparison key using the native column semantics.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="column">The predicate column.</param>
    /// <param name="value">The predicate or decoded value.</param>
    /// <returns>The complete comparison bytes.</returns>
    /// <exception cref="ArgumentException">The value is incompatible with the column.</exception>
    /// <exception cref="NotSupportedException">The column or text collation is unsupported.</exception>
    internal static byte[] Encode(JetFormat format, ColumnInfo column, object? value)
    {
        if (column.Type is ColumnType.ComplexType or ColumnType.AttachmentType)
        {
            throw new NotSupportedException($"Complex column '{column.Name}' cannot identify a parent row.");
        }

        if (value is null or DBNull)
        {
            return [0];
        }

        if (column.Type is ColumnType.TextType or ColumnType.MemoType)
        {
            if (value is not string text)
            {
                throw new ArgumentException($"Column '{column.Name}' requires a Text value.", nameof(value));
            }

            byte[] comparison = new JetTextCollation(column.TextSortOrder).EncodeComparisonKey(text);
            return [1, .. comparison];
        }

        if (column.Type == ColumnType.BinaryType)
        {
            value = IndexKeyEncoder.NormalizeFixedBinaryValue(value, column.IsFixed && !column.IsCalculated ? column.Size : 0);
        }

        return IndexKeyEncoder.EncodeColumnEntry(format, column, value);
    }
}
