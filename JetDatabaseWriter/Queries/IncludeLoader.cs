namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
/// rows by their foreign-key columns. The related table is read in full per include,
/// so includes trade memory and a table scan for a single pass.
/// </remarks>
internal static class IncludeLoader
{
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
        Dictionary<string, object> parentsByKey = await IndexRelatedAsync(
            reader, relationship.PrimaryTable, relatedType, relationship.PrimaryColumns, cancellationToken).ConfigureAwait(false);

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
        Dictionary<string, List<object>> childrenByKey = await GroupRelatedAsync(
            reader, relationship.ForeignTable, relatedType, relationship.ForeignColumns, cancellationToken).ConfigureAwait(false);

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
}
