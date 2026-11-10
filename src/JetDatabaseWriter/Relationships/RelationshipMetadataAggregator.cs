namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;

/// <summary>
/// Aggregates raw <c>MSysRelationships</c> rows (one per foreign-key column)
/// into per-relationship <see cref="RelationshipMetadata"/> records. Consumed by
/// <see cref="AccessReader.ListRelationshipsAsync"/> and EF schema migration;
/// the grouping is a pure projection that touches no reader or writer state.
/// </summary>
internal static class RelationshipMetadataAggregator
{
    /// <summary>
    /// Groups the rows of <paramref name="table"/> by <c>szRelationship</c>
    /// (preserving first-seen order), orders each group's columns by
    /// <c>icolumn</c> when present, and emits one <see cref="RelationshipMetadata"/>
    /// per relationship. Returns an empty list when the required catalog columns
    /// are absent and skips malformed groups.
    /// </summary>
    /// <param name="table">The decoded <c>MSysRelationships</c> table.</param>
    public static List<RelationshipMetadata> Aggregate(DataTable table)
        => Aggregate(
            table.Rows.Cast<DataRow>(),
            name => table.Columns.Contains(name) && table.Columns[name] is DataColumn column ? column.Ordinal : -1,
            static (row, ordinal) => row[ordinal]);

    /// <summary>Groups transaction-visible catalog rows using the same rules as reader metadata.</summary>
    /// <param name="definition">The catalog table's acquired definition.</param>
    /// <param name="rows">Decoded catalog rows in storage order.</param>
    /// <returns>The relationship groups admitted by the reader's metadata policy.</returns>
    internal static List<RelationshipMetadata> Aggregate(TableDef definition, IEnumerable<object[]> rows)
        => Aggregate(rows, definition.FindColumnIndex, static (row, ordinal) => row[ordinal]);

    private static List<RelationshipMetadata> Aggregate<TRow>(
        IEnumerable<TRow> source,
        Func<string, int> findColumn,
        Func<TRow, int, object?> getValue)
    {
        static string Str(object? value) => value as string ?? string.Empty;
        static int Int(object? value) => value switch
        {
            int i => i,
            short s => s,
            byte b => b,
            long l => unchecked((int)l),
            _ => 0,
        };

        int nameOrdinal = findColumn("szRelationship");
        int foreignTableOrdinal = findColumn("szObject");
        int primaryTableOrdinal = findColumn("szReferencedObject");
        int foreignColumnOrdinal = findColumn("szColumn");
        int primaryColumnOrdinal = findColumn("szReferencedColumn");
        if (nameOrdinal < 0 || foreignTableOrdinal < 0 || primaryTableOrdinal < 0
            || foreignColumnOrdinal < 0 || primaryColumnOrdinal < 0)
        {
            return [];
        }

        int columnOrdinal = findColumn("icolumn");
        int flagsOrdinal = findColumn("grbit");

        // Group one MSysRelationships row per FK column back into a single
        // relationship, preserving first-seen order for a deterministic result.
        var groups = new Dictionary<string, List<TRow>>(StringComparer.OrdinalIgnoreCase);
        var order = new List<string>();
        foreach (TRow row in source)
        {
            string name = Str(getValue(row, nameOrdinal));
            if (name.Length == 0)
            {
                continue;
            }

            if (!groups.TryGetValue(name, out List<TRow>? list))
            {
                list = [];
                groups[name] = list;
                order.Add(name);
            }

            list.Add(row);
        }

        var result = new List<RelationshipMetadata>(groups.Count);
        foreach (string name in order)
        {
            List<TRow> rows = groups[name];
            if (columnOrdinal >= 0)
            {
                rows.Sort((left, right) => Int(getValue(left, columnOrdinal)).CompareTo(Int(getValue(right, columnOrdinal))));
            }

            string primaryTable = Str(getValue(rows[0], primaryTableOrdinal));
            string foreignTable = Str(getValue(rows[0], foreignTableOrdinal));
            if (primaryTable.Length == 0 || foreignTable.Length == 0)
            {
                continue;
            }

            string[] primaryColumns = new string[rows.Count];
            string[] foreignColumns = new string[rows.Count];
            bool malformed = false;
            for (int i = 0; i < rows.Count; i++)
            {
                primaryColumns[i] = Str(getValue(rows[i], primaryColumnOrdinal));
                foreignColumns[i] = Str(getValue(rows[i], foreignColumnOrdinal));
                if (primaryColumns[i].Length == 0 || foreignColumns[i].Length == 0)
                {
                    malformed = true;
                }
            }

            if (malformed)
            {
                continue;
            }

            int grbit = flagsOrdinal >= 0 ? Int(getValue(rows[0], flagsOrdinal)) : 0;
            result.Add(new RelationshipMetadata
            {
                Name = name,
                PrimaryTable = primaryTable,
                PrimaryColumns = primaryColumns,
                ForeignTable = foreignTable,
                ForeignColumns = foreignColumns,
                EnforcesReferentialIntegrity = (grbit & (int)Constants.RelationshipFlags.NoRefIntegrity) == 0,
                CascadeUpdates = (grbit & (int)Constants.RelationshipFlags.CascadeUpdates) != 0,
                CascadeDeletes = (grbit & (int)Constants.RelationshipFlags.CascadeDeletes) != 0,
            });
        }

        return result;
    }
}
