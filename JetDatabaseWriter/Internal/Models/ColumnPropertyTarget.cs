namespace JetDatabaseWriter.Internal.Models;

using System;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Core;

/// <summary>
/// A single property target within a <see cref="ColumnPropertyBlock"/> — typically a
/// column, but the table itself may also be a target for table-level properties
/// (e.g. table <c>Description</c>).
/// </summary>
/// <param name="Name">Target name from the property block header (column or table name).</param>
/// <param name="ChunkType">Chunk-type code the block was carried under (0x00, 0x01, or 0x02).</param>
/// <param name="Entries">Property entries owned by this target, in source order.</param>
internal sealed record ColumnPropertyTarget(
    string Name,
    ColumnPropertyChunkType ChunkType,
    IReadOnlyList<ColumnPropertyEntry> Entries)
{
    /// <summary>Returns the first entry with the given property name (case-insensitive), or <see langword="null"/>.</summary>
    public ColumnPropertyEntry? Find(string propertyName)
    {
        foreach (ColumnPropertyEntry e in Entries)
        {
            if (string.Equals(e.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                return e;
            }
        }

        return null;
    }

    /// <summary>
    /// Returns the value of a Text-typed (<see cref="ColumnPropertyBlock.DataTypeText"/>)
    /// or Memo-typed property as a string, or <see langword="null"/> if absent or non-textual.
    /// </summary>
    /// <param name="propertyName">Property name (case-insensitive).</param>
    /// <param name="format">Database format (selects Jet3 vs Jet4 decoding).</param>
    public string? GetTextValue(string propertyName, DatabaseFormat format)
    {
        ColumnPropertyEntry? entry = Find(propertyName);
        if (entry is null)
        {
            return null;
        }

        if (entry.DataType != ColumnPropertyBlock.DataTypeText &&
            entry.DataType != ColumnPropertyBlock.DataTypeMemo)
        {
            return null;
        }

        return format == DatabaseFormat.Jet3Mdb
            ? Encoding.GetEncoding(1252).GetString(entry.Value)
            : Encoding.Unicode.GetString(entry.Value);
    }
}
