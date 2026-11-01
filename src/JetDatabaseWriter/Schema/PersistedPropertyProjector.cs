namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Projects a table's persisted properties (its <c>MSysObjects.LvProp</c> blob)
/// through a schema rewrite: AddColumn, DropColumn and RenameColumn rebuild the
/// table, and the rebuilt table's blob is the original one with only what the
/// rewrite changes changed. Every target and entry the writer does not model
/// (a column's <c>Caption</c>, <c>Format</c>, <c>GUID</c> or <c>AppendOnly</c>,
/// the table-level <c>Filter</c>, <c>OrderBy</c> or <c>ValidationRule</c>) is
/// kept byte for byte, and so are unknown chunks.
/// </summary>
internal static class PersistedPropertyProjector
{
    /// <summary>The table-level name-map property, which a column rewrite drops.</summary>
    private const string NameMap = "NameMap";

    /// <summary>
    /// Returns the persisted properties of the rebuilt table:
    /// <list type="number">
    /// <item><description>The table-level target (empty name) is kept wherever it is. Column edits remove its <c>NameMap</c>, which the writer cannot keep in step with changed columns; an index-only rewrite retains it.</description></item>
    /// <item><description>A column's target follows <paramref name="mapColumnName"/>: it is removed when the column is dropped and renamed with it.</description></item>
    /// <item><description>A target that names no column is kept, unless a column of the rebuilt table takes its name.</description></item>
    /// <item><description>A surviving column keeps every entry as stored, except an entry the writer models (any property <see cref="JetExpressionConverter.ApplyColumn"/> emits) whose value the rewrite changes, such as an expression that names a renamed column. That entry takes the new value in place, keeping its data type when both are text and its DDL flag.</description></item>
    /// <item><description>An added column gets the properties <see cref="JetExpressionConverter.ApplyColumn"/> emits for it.</description></item>
    /// </list>
    /// </summary>
    /// <param name="original">The table's stored properties, or <see langword="null"/> when it has none.</param>
    /// <param name="existingDefs">The table's columns before the rewrite, as the writer models them.</param>
    /// <param name="newDefs">The rebuilt table's columns.</param>
    /// <param name="mapColumnName">Maps a current column name to its name after the rewrite, or to <see langword="null"/> for a dropped column.</param>
    /// <param name="format">The database format.</param>
    /// <param name="droppedIndexNames">Names of source logical indexes removed by the rewrite; other index maps survive.</param>
    /// <param name="preserveNameMap">Whether an index-only rewrite retains the unchanged column name map.</param>
    /// <returns>The projected properties; <see cref="ColumnPropertyBlock.Empty"/> when nothing is left.</returns>
    internal static ColumnPropertyBlock ProjectForRewrite(
        ColumnPropertyBlock? original,
        IReadOnlyList<ColumnDefinition> existingDefs,
        IReadOnlyList<ColumnDefinition> newDefs,
        Func<string, string?> mapColumnName,
        JetFormat format,
        IReadOnlyCollection<string>? droppedIndexNames = null,
        bool preserveNameMap = false)
    {
        Guard.NotNull(existingDefs, nameof(existingDefs));
        Guard.NotNull(newDefs, nameof(newDefs));
        Guard.NotNull(mapColumnName, nameof(mapColumnName));

        ColumnPropertyBlockBuilder builder = original is null ? new ColumnPropertyBlockBuilder() : ColumnPropertyBlockBuilder.FromBlock(original);
        var newNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnDefinition column in newDefs)
        {
            _ = newNames.Add(column.Name);
        }

        RetargetColumns(builder, existingDefs, newNames, mapColumnName, droppedIndexNames, preserveNameMap);

        var survivors = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnDefinition existing in existingDefs)
        {
            if (mapColumnName(existing.Name) is not { } newName
                || FindDefinition(newDefs, newName) is not { } projected)
            {
                continue;
            }

            _ = survivors.Add(projected.Name);
            ReconcileModelledEntries(builder, existing, projected, format);
        }

        foreach (ColumnDefinition column in newDefs)
        {
            if (!survivors.Contains(column.Name))
            {
                JetExpressionConverter.ApplyColumn(builder, column, format);
            }
        }

        byte[]? bytes = builder.ToBytes(format);
        return ColumnPropertyBlock.Parse(bytes, format) ?? ColumnPropertyBlock.Empty(format);
    }

    /// <summary>
    /// Drops the table-level <c>NameMap</c> for a column edit, and renames, drops or keeps each
    /// column target as the rewrite renames, drops or keeps its column.
    /// </summary>
    /// <param name="builder">The properties being projected.</param>
    /// <param name="existingDefs">The table's columns before the rewrite.</param>
    /// <param name="newNames">The rebuilt table's column names.</param>
    /// <param name="mapColumnName">Maps a current column name to its new name, or to <see langword="null"/>.</param>
    /// <param name="droppedIndexNames">Names of source logical indexes removed by the rewrite.</param>
    /// <param name="preserveNameMap">Whether column identities are unchanged and their name map is retained.</param>
    private static void RetargetColumns(
        ColumnPropertyBlockBuilder builder,
        IReadOnlyList<ColumnDefinition> existingDefs,
        HashSet<string> newNames,
        Func<string, string?> mapColumnName,
        IReadOnlyCollection<string>? droppedIndexNames,
        bool preserveNameMap)
    {
        HashSet<string>? indexNames = droppedIndexNames is null ? null : new(droppedIndexNames, StringComparer.OrdinalIgnoreCase);
        var kept = new List<ColumnPropertyTargetBuilder>(builder.Targets.Count);
        foreach (ColumnPropertyTargetBuilder target in builder.Targets)
        {
            if (target.ChunkType == ColumnPropertyChunkType.PropertyBlock && target.Name.Length == 0)
            {
                if (!preserveNameMap)
                {
                    _ = target.Entries.RemoveAll(static e => string.Equals(e.Name, NameMap, StringComparison.OrdinalIgnoreCase));
                }

                kept.Add(target);
                continue;
            }

            if (target.ChunkType != ColumnPropertyChunkType.PropertyBlockAlt1)
            {
                if (target.ChunkType != ColumnPropertyChunkType.PropertyBlockAlt2 || indexNames?.Contains(target.Name) != true)
                {
                    kept.Add(target);
                }

                continue;
            }

            if (FindDefinition(existingDefs, target.Name) is { } existing)
            {
                if (mapColumnName(existing.Name) is { } newName)
                {
                    if (!string.Equals(newName, existing.Name, StringComparison.Ordinal))
                    {
                        target.Name = newName;
                    }

                    kept.Add(target);
                }

                continue;
            }

            // A target that names no column is kept, unless a column of the
            // rebuilt table takes its name and brings its own properties.
            if (!newNames.Contains(target.Name))
            {
                kept.Add(target);
            }
        }

        builder.Targets.Clear();
        builder.Targets.AddRange(kept);
    }

    /// <summary>
    /// Writes into a surviving column's target each modelled property whose value
    /// the rewrite changes, and leaves every other entry as stored.
    /// </summary>
    /// <param name="builder">The properties being projected.</param>
    /// <param name="existing">The column before the rewrite.</param>
    /// <param name="projected">The column after the rewrite.</param>
    /// <param name="format">The database format.</param>
    private static void ReconcileModelledEntries(
        ColumnPropertyBlockBuilder builder,
        ColumnDefinition existing,
        ColumnDefinition projected,
        JetFormat format)
    {
        List<ColumnPropertyEntryBuilder> before = ModelledEntries(builder, existing, format);
        List<ColumnPropertyEntryBuilder> after = ModelledEntries(builder, projected, format);

        foreach (ColumnPropertyEntryBuilder entry in after)
        {
            if (FindEntry(before, entry.Name) is { } unchanged && SameValue(unchanged, entry))
            {
                continue;
            }

            bool found = false;
            foreach (ColumnPropertyTargetBuilder target in builder.Targets)
            {
                if (target.ChunkType != ColumnPropertyChunkType.PropertyBlockAlt1
                    || !string.Equals(target.Name, projected.Name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                foreach (ColumnPropertyEntryBuilder stored in target.Entries)
                {
                    if (!string.Equals(stored.Name, entry.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    found = true;
                    stored.Value = entry.Value;
                    if (!(IsText(stored.DataType) && IsText(entry.DataType)))
                    {
                        stored.DataType = entry.DataType;
                    }
                }
            }

            if (!found)
            {
                builder.GetOrAddTarget(projected.Name).Entries.Add(entry);
            }
        }

        foreach (ColumnPropertyEntryBuilder entry in before)
        {
            if (FindEntry(after, entry.Name) is null)
            {
                foreach (ColumnPropertyTargetBuilder target in builder.Targets)
                {
                    if (target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt1
                        && string.Equals(target.Name, projected.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        _ = target.Entries.RemoveAll(e => string.Equals(e.Name, entry.Name, StringComparison.OrdinalIgnoreCase));
                    }
                }
            }
        }
    }

    /// <summary>Returns the entries <see cref="JetExpressionConverter.ApplyColumn"/> emits for <paramref name="column"/>.</summary>
    /// <param name="source">The source property builder.</param>
    /// <param name="column">The column definition.</param>
    /// <param name="format">The database format.</param>
    private static List<ColumnPropertyEntryBuilder> ModelledEntries(ColumnPropertyBlockBuilder source, ColumnDefinition column, JetFormat format)
    {
        ColumnPropertyBlockBuilder scratch = source.CreateEmptyWithSameEncoding();
        JetExpressionConverter.ApplyColumn(scratch, column, format);
        return scratch.Targets.Count == 0 ? [] : scratch.Targets[0].Entries;
    }

    private static ColumnDefinition? FindDefinition(IReadOnlyList<ColumnDefinition> columns, string name)
    {
        foreach (ColumnDefinition column in columns)
        {
            if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return null;
    }

    private static ColumnPropertyEntryBuilder? FindEntry(List<ColumnPropertyEntryBuilder> entries, string name)
        => entries.Find(e => string.Equals(e.Name, name, StringComparison.OrdinalIgnoreCase));

    private static bool SameValue(ColumnPropertyEntryBuilder left, ColumnPropertyEntryBuilder right)
        => left.DataType == right.DataType && left.Value.AsSpan().SequenceEqual(right.Value);

    private static bool IsText(ColumnType type) => type is ColumnType.TextType or ColumnType.MemoType;
}
