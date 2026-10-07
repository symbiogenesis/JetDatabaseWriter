namespace JetDatabaseWriter.Schema.Models;

using System;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Enums;

/// <summary>
/// A single property target within a <see cref="ColumnPropertyBlock"/> — typically a
/// column, but the table itself may also be a target for table-level properties
/// (e.g. table <c>Description</c>).
/// </summary>
/// <param name="Name">Target name from the property block header (column name, or empty for the table-level target).</param>
/// <param name="ChunkType">Chunk-type code the block was carried under (0x00, 0x01, or 0x02).</param>
/// <param name="Entries">Property entries owned by this target, in source order.</param>
internal sealed record ColumnPropertyTarget(
    string Name,
    ColumnPropertyChunkType ChunkType,
    IReadOnlyList<ColumnPropertyEntry> Entries)
{
    /// <summary>Gets the position of this target in the source chunk sequence.</summary>
    internal int SourceChunkIndex { get; init; } = -1;

    /// <summary>Gets the four-byte inner header preserved from the source target.</summary>
    internal uint? SourceHeader { get; init; }

    /// <summary>Gets a value indicating whether the source header matches the native target-name byte count.</summary>
    internal bool SourceHeaderIsNameLength { get; init; }

    /// <summary>Gets the actual stored text encoding when parsed from a property blob.</summary>
    internal Encoding? TextEncoding { get; init; }

    /// <summary>Returns the first entry with the given property name (case-insensitive), or <see langword="null"/>.</summary>
    /// <param name="propertyName">The property name.</param>
    public ColumnPropertyEntry? Find(string propertyName)
    {
        foreach (ColumnPropertyEntry e in this.Entries)
        {
            if (string.Equals(e.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                return e;
            }
        }

        return null;
    }

    /// <summary>
    /// Returns the value of a Text-typed (<see cref="ColumnType.TextType"/>)
    /// or Memo-typed property as a string, or <see langword="null"/> if absent or non-textual.
    /// </summary>
    /// <param name="propertyName">Property name (case-insensitive).</param>
    /// <param name="format">Database format (selects Jet3 vs Jet4 decoding).</param>
    public string? GetTextValue(string propertyName, JetFormat format)
    {
        ColumnPropertyEntry? entry = this.Find(propertyName);
        if (entry is null)
        {
            return null;
        }

        if (entry.DataType is not ColumnType.TextType and
            not ColumnType.MemoType)
        {
            return null;
        }

        return (this.TextEncoding ?? format.PropertyTextEncoding).GetString(entry.Value);
    }

    /// <summary>
    /// Returns the value of a Boolean-typed (<see cref="ColumnType.BooleanType"/>)
    /// property as a <see cref="bool"/>, or <see langword="null"/> if absent or non-boolean.
    /// On-disk representation: single byte; any non-zero value reads as <see langword="true"/>.
    /// </summary>
    /// <param name="propertyName">Property name (case-insensitive).</param>
    public bool? GetBooleanValue(string propertyName)
    {
        ColumnPropertyEntry? entry = this.Find(propertyName);
        if (entry is null || entry.DataType != ColumnType.BooleanType || entry.Value.Length == 0)
        {
            return null;
        }

        return entry.Value[0] != 0;
    }
}
