namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;

/// <summary>
/// Eagerly loads navigation properties for a set of already-materialized root
/// entities by inferring the foreign-key relationship from the
/// <c>MSysRelationships</c> catalog, batch-reading the related table once, and
/// stitching the related entities onto each root.
/// </summary>
/// <remarks>
/// Join keys are read from each root POCO by column name (case-insensitive), the
/// same convention the row mapper uses. A reference navigation matches the child's
/// foreign-key columns to the parent's key; a collection navigation groups child
/// rows by their foreign-key columns. When the related table has an index covering
/// the join columns (a primary key or foreign-key index, inferred automatically) and
/// the distinct keys are only a small share of that table, the related rows are loaded
/// with one index seek per distinct key; otherwise (no covering index, a Jet3 file, or
/// too many distinct keys relative to the table) it scans the table once and groups in
/// memory.
/// </remarks>
internal static class IncludeLoader
{
    /// <summary>
    /// Seek one index entry per distinct key only when those keys are at most a
    /// 1/<c>SeekKeyCountTableFraction</c> share of the related table; above that the
    /// per-seek B-tree descents do more total work than a single sequential scan.
    /// </summary>
    private const int SeekKeyCountTableFraction = 4;

    public static async ValueTask ApplyAsync(
        AccessReader reader,
        string parentTable,
        IReadOnlyList<object> roots,
        IReadOnlyList<PropertyInfo> includes,
        CancellationToken cancellationToken)
    {
        if (roots.Count == 0)
        {
            return;
        }

        IReadOnlyList<RelationshipMetadata> relationships = await reader.ListRelationshipsAsync(cancellationToken).ConfigureAwait(false);

        foreach (PropertyInfo navigation in includes)
        {
            Type? elementType = GetEnumerableElementType(navigation.PropertyType);
            if (elementType is not null)
            {
                RelationshipMetadata relationship = FindCollectionRelationship(relationships, parentTable, elementType)
                    ?? throw NoRelationship(navigation, parentTable, elementType);
                await LoadCollectionAsync(reader, roots, navigation, elementType, relationship, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                Type relatedType = navigation.PropertyType;
                RelationshipMetadata relationship = FindReferenceRelationship(relationships, parentTable, relatedType)
                    ?? throw NoRelationship(navigation, parentTable, relatedType);
                await LoadReferenceAsync(reader, roots, navigation, relatedType, relationship, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async ValueTask LoadReferenceAsync(
        AccessReader reader,
        IReadOnlyList<object> roots,
        PropertyInfo navigation,
        Type relatedType,
        RelationshipMetadata relationship,
        CancellationToken cancellationToken)
    {
        EnsureConstructible(relatedType, navigation);
        Dictionary<string, object?[]> distinctKeys = CollectDistinctKeys(roots, relationship.ForeignColumns);
        Dictionary<string, object> parentsByKey = await BuildParentLookupAsync(
            reader, relationship.PrimaryTable, relatedType, relationship.PrimaryColumns, distinctKeys, cancellationToken).ConfigureAwait(false);

        foreach (object root in roots)
        {
            string? key = BuildKeyFromObject(root, relationship.ForeignColumns);
            object? parent = key is not null && parentsByKey.TryGetValue(key, out object? match) ? match : null;
            navigation.SetValue(root, parent);
        }
    }

    private static async ValueTask LoadCollectionAsync(
        AccessReader reader,
        IReadOnlyList<object> roots,
        PropertyInfo navigation,
        Type relatedType,
        RelationshipMetadata relationship,
        CancellationToken cancellationToken)
    {
        EnsureConstructible(relatedType, navigation);
        Dictionary<string, object?[]> distinctKeys = CollectDistinctKeys(roots, relationship.PrimaryColumns);
        Dictionary<string, List<object>> childrenByKey = await BuildChildGroupsAsync(
            reader, relationship.ForeignTable, relatedType, relationship.ForeignColumns, distinctKeys, cancellationToken).ConfigureAwait(false);

        foreach (object root in roots)
        {
            string? key = BuildKeyFromObject(root, relationship.PrimaryColumns);
            IList list = RuntimeRowMapper.CreateList(relatedType);
            if (key is not null && childrenByKey.TryGetValue(key, out List<object>? children))
            {
                foreach (object child in children)
                {
                    list.Add(child);
                }
            }

            navigation.SetValue(root, list);
        }
    }

    private static async ValueTask<Dictionary<string, object>> BuildParentLookupAsync(
        AccessReader reader,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        Dictionary<string, object?[]> distinctKeys,
        CancellationToken cancellationToken)
    {
        // One parent per key: seek the parent key index when one covers the key
        // columns and the keys are few relative to the table, otherwise scan once.
        SeekPlan? plan = distinctKeys.Count > 0
            ? await ResolveSeekPlanAsync(reader, table, keyColumns, distinctKeys.Count, cancellationToken).ConfigureAwait(false)
            : null;
        if (plan is not SeekPlan seek)
        {
            return await IndexRelatedAsync(reader, table, type, keyColumns, cancellationToken).ConfigureAwait(false);
        }

        var map = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?[]> entry in distinctKeys)
        {
            IndexQueryCriteria criteria = BuildSeekCriteria(seek.IndexColumnCount, keyColumns.Count, entry.Value);
            await foreach (object[] row in reader.ReadIndexRowsAsObjectsAsync(table, seek.IndexName, criteria, cancellationToken).ConfigureAwait(false))
            {
                map[entry.Key] = RuntimeRowMapper.Map(type, seek.Headers, row);
                break;
            }
        }

        return map;
    }

    private static async ValueTask<Dictionary<string, List<object>>> BuildChildGroupsAsync(
        AccessReader reader,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        Dictionary<string, object?[]> distinctKeys,
        CancellationToken cancellationToken)
    {
        // Many children per key: seek the foreign-key index when one covers the key
        // columns and the keys are few relative to the table, otherwise scan once.
        SeekPlan? plan = distinctKeys.Count > 0
            ? await ResolveSeekPlanAsync(reader, table, keyColumns, distinctKeys.Count, cancellationToken).ConfigureAwait(false)
            : null;
        if (plan is not SeekPlan seek)
        {
            return await GroupRelatedAsync(reader, table, type, keyColumns, cancellationToken).ConfigureAwait(false);
        }

        var map = new Dictionary<string, List<object>>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?[]> entry in distinctKeys)
        {
            var list = new List<object>();
            IndexQueryCriteria criteria = BuildSeekCriteria(seek.IndexColumnCount, keyColumns.Count, entry.Value);
            await foreach (object[] row in reader.ReadIndexRowsAsObjectsAsync(table, seek.IndexName, criteria, cancellationToken).ConfigureAwait(false))
            {
                list.Add(RuntimeRowMapper.Map(type, seek.Headers, row));
            }

            if (list.Count > 0)
            {
                map[entry.Key] = list;
            }
        }

        return map;
    }

    private static async ValueTask<SeekPlan?> ResolveSeekPlanAsync(
        AccessReader reader,
        string table,
        IReadOnlyList<string> keyColumns,
        int distinctKeyCount,
        CancellationToken cancellationToken)
    {
        // Index seeks are Jet4/ACE only; everything else falls back to a scan.
        if (reader.Format == DatabaseFormat.Jet3Mdb)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(table, cancellationToken).ConfigureAwait(false);
        IndexMetadata? index = FindCoveringIndex(indexes, keyColumns);
        if (index is null)
        {
            return null;
        }

        // Cost guard: seeking K keys (each a B-tree descent) only beats one scan when K
        // is a small fraction of the related table. When the declared row count says we
        // would seek a large share of the table, scan instead. A row count of 0 (unknown
        // or empty) leaves the seek path enabled.
        long rowCount = await reader.GetDeclaredRowCountAsync(table, cancellationToken).ConfigureAwait(false);
        if (rowCount > 0 && (long)distinctKeyCount * SeekKeyCountTableFraction > rowCount)
        {
            return null;
        }

        (string[] headers, _) = await ReadHeadersAsync(reader, table, keyColumns, cancellationToken).ConfigureAwait(false);
        return new SeekPlan(index.Name, index.Columns.Count, headers);
    }

    private static IndexMetadata? FindCoveringIndex(IReadOnlyList<IndexMetadata> indexes, IReadOnlyList<string> joinColumns)
    {
        IndexMetadata? prefixMatch = null;
        foreach (IndexMetadata index in indexes)
        {
            if (index.FirstDp <= 0 || index.Columns.Count < joinColumns.Count)
            {
                continue;
            }

            bool leadingMatch = true;
            for (int i = 0; i < joinColumns.Count; i++)
            {
                if (!string.Equals(index.Columns[i].Name, joinColumns[i], StringComparison.OrdinalIgnoreCase))
                {
                    leadingMatch = false;
                    break;
                }
            }

            if (!leadingMatch)
            {
                continue;
            }

            if (index.Columns.Count == joinColumns.Count)
            {
                return index;
            }

            prefixMatch ??= index;
        }

        return prefixMatch;
    }

    private static IndexQueryCriteria BuildSeekCriteria(int indexColumnCount, int joinColumnCount, object?[] values) =>
        indexColumnCount == joinColumnCount ? IndexQueryCriteria.Exact(values) : IndexQueryCriteria.KeyPrefix(values);

    private static Dictionary<string, object?[]> CollectDistinctKeys(IReadOnlyList<object> roots, IReadOnlyList<string> columns)
    {
        var result = new Dictionary<string, object?[]>(StringComparer.Ordinal);
        foreach (object root in roots)
        {
            (string? key, object?[]? values) = BuildKeyAndValues(root, columns);
            if (key is not null)
            {
                result.TryAdd(key, values!);
            }
        }

        return result;
    }

    private static (string? Key, object?[]? Values) BuildKeyAndValues(object instance, IReadOnlyList<string> columns)
    {
        Dictionary<string, PropertyInfo> properties = RuntimeRowMapper.GetProperties(instance.GetType());
        object?[] values = new object?[columns.Count];
        string[] parts = new string[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            if (!properties.TryGetValue(columns[i], out PropertyInfo? property))
            {
                return (null, null);
            }

            object? value = property.GetValue(instance);
            if (Normalize(value) is not string component)
            {
                return (null, null);
            }

            values[i] = value;
            parts[i] = component;
        }

        return (string.Join("|", parts), values);
    }

    private static async ValueTask<Dictionary<string, object>> IndexRelatedAsync(
        AccessReader reader,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        CancellationToken cancellationToken)
    {
        (string[] headers, int[] keyIndices) = await ReadHeadersAsync(reader, table, keyColumns, cancellationToken).ConfigureAwait(false);
        var map = new Dictionary<string, object>(StringComparer.Ordinal);
        await foreach (object[] row in reader.Rows(table, progress: null, cancellationToken).ConfigureAwait(false))
        {
            string? key = BuildKeyFromRow(row, keyIndices);
            if (key is null)
            {
                continue;
            }

            map.TryAdd(key, RuntimeRowMapper.Map(type, headers, row));
        }

        return map;
    }

    private static async ValueTask<Dictionary<string, List<object>>> GroupRelatedAsync(
        AccessReader reader,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        CancellationToken cancellationToken)
    {
        (string[] headers, int[] keyIndices) = await ReadHeadersAsync(reader, table, keyColumns, cancellationToken).ConfigureAwait(false);
        var map = new Dictionary<string, List<object>>(StringComparer.Ordinal);
        await foreach (object[] row in reader.Rows(table, progress: null, cancellationToken).ConfigureAwait(false))
        {
            string? key = BuildKeyFromRow(row, keyIndices);
            if (key is null)
            {
                continue;
            }

            if (!map.TryGetValue(key, out List<object>? list))
            {
                list = [];
                map[key] = list;
            }

            list.Add(RuntimeRowMapper.Map(type, headers, row));
        }

        return map;
    }

    private static async ValueTask<(string[] Headers, int[] KeyIndices)> ReadHeadersAsync(
        AccessReader reader,
        string table,
        IReadOnlyList<string> keyColumns,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, cancellationToken).ConfigureAwait(false);
        string[] headers = new string[meta.Count];
        for (int i = 0; i < meta.Count; i++)
        {
            headers[i] = meta[i].Name;
        }

        return (headers, ResolveKeyIndices(headers, keyColumns));
    }

    private static int[] ResolveKeyIndices(string[] headers, IReadOnlyList<string> keyColumns)
    {
        int[] indices = new int[keyColumns.Count];
        for (int i = 0; i < keyColumns.Count; i++)
        {
            int found = -1;
            for (int h = 0; h < headers.Length; h++)
            {
                if (string.Equals(headers[h], keyColumns[i], StringComparison.OrdinalIgnoreCase))
                {
                    found = h;
                    break;
                }
            }

            indices[i] = found >= 0
                ? found
                : throw new InvalidOperationException($"Relationship key column '{keyColumns[i]}' was not found in the related table.");
        }

        return indices;
    }

    private static string? BuildKeyFromObject(object instance, IReadOnlyList<string> columns)
    {
        Dictionary<string, PropertyInfo> properties = RuntimeRowMapper.GetProperties(instance.GetType());
        string[] parts = new string[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            if (!properties.TryGetValue(columns[i], out PropertyInfo? property))
            {
                return null;
            }

            if (Normalize(property.GetValue(instance)) is not string component)
            {
                return null;
            }

            parts[i] = component;
        }

        return string.Join("|", parts);
    }

    private static string? BuildKeyFromRow(object?[] row, int[] keyIndices)
    {
        string[] parts = new string[keyIndices.Length];
        for (int i = 0; i < keyIndices.Length; i++)
        {
            object? value = keyIndices[i] < row.Length ? row[keyIndices[i]] : null;
            if (Normalize(value) is not string component)
            {
                return null;
            }

            parts[i] = component;
        }

        return string.Join("|", parts);
    }

    private static string? Normalize(object? value) => value switch
    {
        null or DBNull => null,
        bool b => b ? "b1" : "b0",
        byte or sbyte or short or ushort or int or uint or long =>
            "i" + Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        ulong ul => "u" + ul.ToString(CultureInfo.InvariantCulture),
        float or double or decimal =>
            "d" + Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        Guid g => "g" + g.ToString("N"),
        DateTime dt => "t" + dt.Ticks.ToString(CultureInfo.InvariantCulture),
        string s => "s" + s,
        _ => "o" + value,
    };

    private static Type? GetEnumerableElementType(Type type)
    {
        if (type == typeof(string))
        {
            return null;
        }

        if (type.IsInterface && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            return type.GetGenericArguments()[0];
        }

        foreach (Type candidate in type.GetInterfaces())
        {
            if (candidate.IsGenericType && candidate.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                return candidate.GetGenericArguments()[0];
            }
        }

        return null;
    }

    private static RelationshipMetadata? FindCollectionRelationship(
        IReadOnlyList<RelationshipMetadata> relationships,
        string parentTable,
        Type childType)
    {
        string parent = Simplify(parentTable);
        string child = Simplify(childType.Name);
        foreach (RelationshipMetadata relationship in relationships)
        {
            if (Simplify(relationship.PrimaryTable) == parent && Simplify(relationship.ForeignTable) == child)
            {
                return relationship;
            }
        }

        return null;
    }

    private static RelationshipMetadata? FindReferenceRelationship(
        IReadOnlyList<RelationshipMetadata> relationships,
        string childTable,
        Type parentType)
    {
        string child = Simplify(childTable);
        string parent = Simplify(parentType.Name);
        foreach (RelationshipMetadata relationship in relationships)
        {
            if (Simplify(relationship.ForeignTable) == child && Simplify(relationship.PrimaryTable) == parent)
            {
                return relationship;
            }
        }

        return null;
    }

    private static string Simplify(string name)
    {
        var builder = new StringBuilder(name.Length);
        foreach (char c in name)
        {
            if (char.IsLetterOrDigit(c))
            {
                builder.Append(char.ToUpperInvariant(c));
            }
        }

        return builder.ToString();
    }

    private static void EnsureConstructible(Type type, PropertyInfo navigation)
    {
        if (type.IsAbstract || type.IsInterface || type.GetConstructor(Type.EmptyTypes) is null)
        {
            throw new InvalidOperationException(
                $"Include navigation '{navigation.Name}' targets type '{type}', which must be a concrete class with a parameterless constructor.");
        }
    }

    private static InvalidOperationException NoRelationship(PropertyInfo navigation, string table, Type relatedType) =>
        new($"Could not infer a relationship for navigation '{navigation.Name}' between table '{table}' and type '{relatedType.Name}'. "
            + "Ensure a foreign key linking the two tables exists in MSysRelationships.");

    private readonly record struct SeekPlan(string IndexName, int IndexColumnCount, string[] Headers);
}
