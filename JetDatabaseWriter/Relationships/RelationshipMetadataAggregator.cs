namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using JetDatabaseWriter.Models;

/// <summary>
/// Aggregates raw <c>MSysRelationships</c> rows (one per foreign-key column)
/// into per-relationship <see cref="RelationshipMetadata"/> records. Consumed by
/// <see cref="AccessReader.ListRelationshipsAsync"/>; the grouping is a pure
/// projection over a <see cref="DataTable"/> that touches no reader state.
/// </summary>
internal static class RelationshipMetadataAggregator
{
    /// <summary>
    /// Groups the rows of <paramref name="table"/> by <c>szRelationship</c>
    /// (preserving first-seen order), orders each group's columns by
    /// <c>icolumn</c> when present, and emits one <see cref="RelationshipMetadata"/>
    /// per relationship. Returns an empty list when the required catalog columns
    /// are absent (Jet3 / slim-catalog files) and skips malformed groups.
    /// </summary>
    /// <param name="table">The decoded <c>MSysRelationships</c> table.</param>
    public static List<RelationshipMetadata> Aggregate(DataTable table)
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

        if (!table.Columns.Contains("szRelationship")
            || !table.Columns.Contains("szObject")
            || !table.Columns.Contains("szReferencedObject")
            || !table.Columns.Contains("szColumn")
            || !table.Columns.Contains("szReferencedColumn"))
        {
            return [];
        }

        bool hasIcolumn = table.Columns.Contains("icolumn");
        bool hasGrbit = table.Columns.Contains("grbit");

        // Group one MSysRelationships row per FK column back into a single
        // relationship, preserving first-seen order for a deterministic result.
        var groups = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
        var order = new List<string>();
        foreach (DataRow row in table.Rows)
        {
            string name = Str(row["szRelationship"]);
            if (name.Length == 0)
            {
                continue;
            }

            if (!groups.TryGetValue(name, out List<DataRow>? list))
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
            List<DataRow> rows = groups[name];
            if (hasIcolumn)
            {
                rows.Sort((left, right) => Int(left["icolumn"]).CompareTo(Int(right["icolumn"])));
            }

            string primaryTable = Str(rows[0]["szReferencedObject"]);
            string foreignTable = Str(rows[0]["szObject"]);
            if (primaryTable.Length == 0 || foreignTable.Length == 0)
            {
                continue;
            }

            string[] primaryColumns = new string[rows.Count];
            string[] foreignColumns = new string[rows.Count];
            bool malformed = false;
            for (int i = 0; i < rows.Count; i++)
            {
                primaryColumns[i] = Str(rows[i]["szReferencedColumn"]);
                foreignColumns[i] = Str(rows[i]["szColumn"]);
                if (primaryColumns[i].Length == 0 || foreignColumns[i].Length == 0)
                {
                    malformed = true;
                }
            }

            if (malformed)
            {
                continue;
            }

            int grbit = hasGrbit ? Int(rows[0]["grbit"]) : 0;
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
