namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Schema.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Update;

public sealed partial class JetDatabase
{
    private static async ValueTask<List<IUpdateEntry>> OrderUniqueHandoffsAsync(
        List<IUpdateEntry> entries,
        TableSplitEntryIndex splitIndex,
        AccessWriter writer,
        CancellationToken cancellationToken)
    {
        var insertedTables = entries.Where(static entry => entry.EntityState == EntityState.Added)
            .Select(static entry => JetEntityTypeMetadata.GetTableName(entry.EntityType)).ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (!entries.Any(entry => insertedTables.Contains(JetEntityTypeMetadata.GetTableName(entry.EntityType))
            && GetSaveRowEntries(entry, splitIndex).Any(source => source.EntityState is EntityState.Modified or EntityState.Deleted
                && source.EntityType.GetIndexes().Any(index => index.IsUnique
                    && (source.EntityState == EntityState.Deleted || index.Properties.Any(source.IsModified))))))
        {
            return entries;
        }

        Dictionary<string, ResolvedTable> tables = [with(StringComparer.OrdinalIgnoreCase)];
        Dictionary<IIndex, Dictionary<string, List<int>>> releases = [];
        List<(IIndex Index, string Key, int EntryIndex)> acquisitions = [];
        for (int entryIndex = 0; entryIndex < entries.Count; entryIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IUpdateEntry entry = entries[entryIndex];
            foreach (IUpdateEntry source in GetSaveRowEntries(entry, splitIndex))
            {
                foreach (IIndex index in source.EntityType.GetIndexes().Where(static index => index.IsUnique))
                {
                    string tableName = JetEntityTypeMetadata.GetTableName(entry.EntityType);
                    if (!tables.TryGetValue(tableName, out ResolvedTable? table))
                    {
                        table = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false)
                            ?? throw new InvalidOperationException($"Table '{tableName}' does not exist.");
                        tables[tableName] = table;
                    }

                    bool isInsert = entry.EntityState == EntityState.Added;
                    string? currentKey = entry.EntityState == EntityState.Deleted || source.EntityState == EntityState.Deleted
                        ? null
                        : EncodeUniqueKey(source, index.Properties, table, originalValues: false, isInsert);
                    string? originalKey = isInsert || source.EntityState == EntityState.Added
                        ? null
                        : EncodeUniqueKey(source, index.Properties, table, originalValues: true, isInsert: false);
                    if (originalKey == currentKey)
                    {
                        continue;
                    }

                    if (originalKey is not null)
                    {
                        if (!releases.TryGetValue(index, out Dictionary<string, List<int>>? keys))
                        {
                            keys = [with(StringComparer.Ordinal)];
                            releases[index] = keys;
                        }

                        if (!keys.TryGetValue(originalKey, out List<int>? owners))
                        {
                            owners = [];
                            keys[originalKey] = owners;
                        }

                        owners.Add(entryIndex);
                    }

                    if (currentKey is not null)
                    {
                        acquisitions.Add((index, currentKey, entryIndex));
                    }
                }
            }
        }

        var edges = new HashSet<int>[entries.Count];
        for (int i = 0; i < edges.Length; i++)
        {
            edges[i] = [];
        }

        bool hasInsertHandoff = false;
        foreach ((IIndex index, string key, int target) in acquisitions)
        {
            if (!releases.TryGetValue(index, out Dictionary<string, List<int>>? keys)
                || !keys.TryGetValue(key, out List<int>? owners))
            {
                continue;
            }

            foreach (int owner in owners)
            {
                _ = edges[owner].Add(target);
                hasInsertHandoff |= entries[target].EntityState == EntityState.Added;
            }
        }

        if (!hasInsertHandoff)
        {
            return entries;
        }

        AddForeignKeyOrderingEdges(entries, splitIndex, edges);
        return OrderDependencyComponents(entries, splitIndex, edges);
    }

    private static List<IUpdateEntry> GetSaveRowEntries(IUpdateEntry entry, TableSplitEntryIndex splitIndex)
        => splitIndex.RowEntries.TryGetValue(entry, out List<IUpdateEntry>? rowEntries) ? rowEntries : [entry];

    private static string? EncodeUniqueKey(IUpdateEntry entry, IReadOnlyList<IProperty> properties, ResolvedTable table, bool originalValues, bool isInsert)
    {
        string[] components = new string[properties.Count];
        bool hasValue = false;
        for (int i = 0; i < properties.Count; i++)
        {
            IProperty property = properties[i];
            object? value = originalValues ? entry.GetOriginalValue(property) : entry.GetCurrentValue(property);
            if (!originalValues && isInsert && (entry.HasTemporaryValue(property)
                || (property.ValueGenerated != ValueGenerated.Never && IsClrDefaultOrSentinel(property, value))))
            {
                return null;
            }

            value = JetEntityTypeMetadata.ConvertToProvider(property, value);
            hasValue |= value is not (null or DBNull);
            int ordinal = table.Definition.FindColumnIndex(JetEntityTypeMetadata.GetColumnName(property));
            if (ordinal < 0)
            {
                return null;
            }

            ColumnInfo column = table.Definition.Columns[ordinal];
            byte[]? encoded = IndexKeyEncoder.TryEncodeColumnEntry(
                column.Type,
                value,
                ascending: true,
                column.NumericScale,
                legacyNumeric: false,
                column.TextSortOrder,
                column.IsFixed && !column.IsCalculated ? column.Size : 0);
            if (encoded is null)
            {
                return null;
            }

            components[i] = Convert.ToBase64String(encoded);
        }

        // Access permits repeated all-null unique keys; partial-null keys still compete.
        return hasValue ? string.Join(":", components) : null;
    }

    private static void AddForeignKeyOrderingEdges(List<IUpdateEntry> entries, TableSplitEntryIndex splitIndex, HashSet<int>[] edges)
    {
        Dictionary<(IKey Key, EntityState State), Dictionary<StructuralKey, List<int>>> principals = [];
        Dictionary<StructuralKey, List<int>> PrincipalIndex(IKey key, EntityState state)
        {
            if (principals.TryGetValue((key, state), out Dictionary<StructuralKey, List<int>>? existing))
            {
                return existing;
            }

            Dictionary<StructuralKey, List<int>> result = [];
            for (int i = 0; i < entries.Count; i++)
            {
                foreach (IUpdateEntry entry in GetSaveRowEntries(entries[i], splitIndex))
                {
                    if (entry.EntityState != state || !key.DeclaringEntityType.IsAssignableFrom(entry.EntityType)
                        || !TryExtractEntryKey(entry, key.Properties, state == EntityState.Deleted, out StructuralKey value))
                    {
                        continue;
                    }

                    if (!result.TryGetValue(value, out List<int>? owners))
                    {
                        owners = [];
                        result[value] = owners;
                    }

                    owners.Add(i);
                }
            }

            principals[(key, state)] = result;
            return result;
        }

        for (int i = 0; i < entries.Count; i++)
        {
            foreach (IUpdateEntry entry in GetSaveRowEntries(entries[i], splitIndex))
            {
                foreach (IForeignKey foreignKey in entry.EntityType.GetForeignKeys())
                {
                    if (entry.EntityState != EntityState.Deleted
                        && TryExtractEntryKey(entry, foreignKey.Properties, useOriginalValues: false, out StructuralKey current, foreignKey.PrincipalKey.Properties)
                        && PrincipalIndex(foreignKey.PrincipalKey, EntityState.Added).TryGetValue(current, out List<int>? added))
                    {
                        foreach (int parent in added)
                        {
                            if (parent != i)
                            {
                                _ = edges[parent].Add(i);
                            }
                        }
                    }

                    if (entry.EntityState != EntityState.Added
                        && TryExtractEntryKey(entry, foreignKey.Properties, useOriginalValues: true, out StructuralKey original, foreignKey.PrincipalKey.Properties)
                        && PrincipalIndex(foreignKey.PrincipalKey, EntityState.Deleted).TryGetValue(original, out List<int>? deleted))
                    {
                        foreach (int parent in deleted)
                        {
                            if (parent != i)
                            {
                                _ = edges[i].Add(parent);
                            }
                        }
                    }
                }
            }
        }
    }

    private static List<IUpdateEntry> OrderDependencyComponents(List<IUpdateEntry> entries, TableSplitEntryIndex splitIndex, HashSet<int>[] edges)
    {
        var reverse = new List<int>[entries.Count];
        for (int i = 0; i < reverse.Length; i++)
        {
            reverse[i] = [];
        }

        for (int i = 0; i < edges.Length; i++)
        {
            foreach (int target in edges[i])
            {
                reverse[target].Add(i);
            }
        }

        bool[] visited = new bool[entries.Count];
        List<int> finished = [];
        Stack<(int Node, bool Exit)> pending = [];
        for (int i = 0; i < entries.Count; i++)
        {
            pending.Push((i, false));
            while (pending.TryPop(out (int Node, bool Exit) frame))
            {
                if (frame.Exit)
                {
                    finished.Add(frame.Node);
                    continue;
                }

                if (visited[frame.Node])
                {
                    continue;
                }

                visited[frame.Node] = true;
                pending.Push((frame.Node, true));
                foreach (int target in edges[frame.Node])
                {
                    pending.Push((target, false));
                }
            }
        }

        int[] componentOf = new int[entries.Count];
        Array.Fill(componentOf, -1);
        List<List<int>> components = [];
        Stack<int> search = [];
        for (int i = finished.Count - 1; i >= 0; i--)
        {
            int start = finished[i];
            if (componentOf[start] >= 0)
            {
                continue;
            }

            List<int> component = [];
            int id = components.Count;
            search.Push(start);
            componentOf[start] = id;
            while (search.TryPop(out int node))
            {
                component.Add(node);
                foreach (int previous in reverse[node])
                {
                    if (componentOf[previous] < 0)
                    {
                        componentOf[previous] = id;
                        search.Push(previous);
                    }
                }
            }

            component.Sort();
            string table = JetEntityTypeMetadata.GetTableName(entries[component[0]].EntityType);
            if (component.Count > 1 && component.Any(node => !IsRowUpdate(entries[node], splitIndex)
                || !string.Equals(table, JetEntityTypeMetadata.GetTableName(entries[node].EntityType), StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException("SaveChangesAsync cannot order the unique-value and foreign-key dependencies without violating a constraint.");
            }

            // Cyclic value exchanges within one table remain one atomic update batch.
            components.Add(component);
        }

        var componentEdges = new HashSet<int>[components.Count];
        int[] indegree = new int[components.Count];
        for (int i = 0; i < componentEdges.Length; i++)
        {
            componentEdges[i] = [];
        }

        for (int i = 0; i < edges.Length; i++)
        {
            foreach (int target in edges[i])
            {
                int sourceComponent = componentOf[i];
                int targetComponent = componentOf[target];
                if (sourceComponent != targetComponent && componentEdges[sourceComponent].Add(targetComponent))
                {
                    indegree[targetComponent]++;
                }
            }
        }

        PriorityQueue<int, int> ready = new();
        for (int i = 0; i < components.Count; i++)
        {
            if (indegree[i] == 0)
            {
                ready.Enqueue(i, components[i][0]);
            }
        }

        List<IUpdateEntry> ordered = [with(capacity: entries.Count)];
        while (ready.TryDequeue(out int component, out _))
        {
            ordered.AddRange(components[component].Select(node => entries[node]));
            foreach (int target in componentEdges[component])
            {
                if (--indegree[target] == 0)
                {
                    ready.Enqueue(target, components[target][0]);
                }
            }
        }

        return ordered;
    }
}
