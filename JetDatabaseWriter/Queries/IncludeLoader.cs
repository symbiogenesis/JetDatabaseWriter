namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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
/// same convention the row mapper uses. Keys are compared by normalized value rather
/// than CLR type, so a relationship still matches when the two sides use different
/// numeric types (for example a parent <c>int</c> <c>Id</c> against a child
/// <c>double</c> <c>ParentId</c>). Binary (<c>byte[]</c>) keys compare by content, and a
/// key whose CLR type is not a supported scalar (numeric, <c>bool</c>, <c>char</c>,
/// <c>string</c>, <c>Guid</c>, <c>DateTime</c>, or <c>byte[]</c>) is treated as unmatchable
/// rather than coerced through an arbitrary <c>ToString()</c>. The related table is
/// identified from the navigation's target type: by default the type name must match the
/// table name ignoring case and non-alphanumeric separators (so <c>OrderLine</c> binds to
/// <c>Order_Line</c>), or an explicit
/// <see cref="System.ComponentModel.DataAnnotations.Schema.TableAttribute"/>
/// (<c>[Table("Orders")]</c>) on the type overrides that convention so a differently named
/// POCO still binds. A reference navigation matches the child's
/// foreign-key columns to the parent's key; a collection navigation groups child
/// rows by their foreign-key columns. When more than one relationship links the same
/// pair of tables (for example two foreign keys to the same parent), the navigation
/// property name is matched against the foreign-key column name, EF-style, so a
/// <c>Buyer</c> navigation binds to the <c>BuyerId</c> column and <c>Seller</c> to
/// <c>SellerId</c>. When the related table has an index covering
/// the join columns (a primary key or foreign-key index, inferred automatically) and
/// the distinct keys are only a small share of that table, the related rows are loaded
/// with one index seek per distinct key; otherwise (no covering index, a Jet3 file, or
/// too many distinct keys relative to the table) it scans the table once and groups in
/// memory. A collection navigation may also carry inline EF-style filter / order / page
/// operators (<c>Include(o =&gt; o.Items.Where(...).OrderBy(...).Take(n))</c>); those run
/// in memory per parent after the related rows load, so a <c>Take</c> bounds the children
/// per parent and a following <c>ThenInclude</c> descends only into the kept rows.
/// </remarks>
internal static class IncludeLoader
{
    /// <summary>
    /// Seek one index entry per distinct key only when those keys are at most a
    /// 1/<c>SeekKeyCountTableFraction</c> share of the related table; above that the
    /// per-seek B-tree descents do more total work than a single sequential scan.
    /// </summary>
    private const int SeekKeyCountTableFraction = 4;

    /// <summary>
    /// Conservative magnitude bound (just inside <see cref="decimal.MaxValue"/>) below
    /// which a finite <see cref="double"/> is guaranteed to cast to <see cref="decimal"/>
    /// without overflow, so it can join the unified numeric key space.
    /// </summary>
    private const double NumericKeyDecimalMax = 7.9e28;

    /// <summary>
    /// Negative counterpart of <see cref="NumericKeyDecimalMax"/>.
    /// </summary>
    private const double NumericKeyDecimalMin = -7.9e28;

    public static async ValueTask ApplyAsync(
        AccessReader reader,
        string parentTable,
        IReadOnlyList<object> roots,
        IReadOnlyList<IReadOnlyList<IncludeStep>> includePaths,
        CancellationToken cancellationToken)
    {
        if (roots.Count == 0 || includePaths.Count == 0)
        {
            return;
        }

        IReadOnlyList<IncludeNode> forest = BuildForest(includePaths);
        IReadOnlyList<RelationshipMetadata> relationships = await reader.ListRelationshipsAsync(cancellationToken).ConfigureAwait(false);
        var metadata = new IncludeMetadataCache(reader);
        await LoadNodesAsync(metadata, parentTable, roots, forest, relationships, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask LoadNodesAsync(
        IncludeMetadataCache metadata,
        string table,
        IReadOnlyList<object> entities,
        IReadOnlyList<IncludeNode> nodes,
        IReadOnlyList<RelationshipMetadata> relationships,
        CancellationToken cancellationToken)
    {
        if (entities.Count == 0)
        {
            return;
        }

        foreach (IncludeNode node in nodes)
        {
            PropertyInfo navigation = node.Navigation;
            Type? elementType = GetEnumerableElementType(navigation.PropertyType);
            List<object> loaded;
            string relatedTable;
            if (elementType is not null)
            {
                RelationshipMetadata relationship = FindCollectionRelationship(relationships, table, elementType, navigation.Name)
                    ?? throw NoRelationship(navigation, table, elementType);
                loaded = await LoadCollectionAsync(metadata, entities, navigation, elementType, relationship, node.Operations, cancellationToken).ConfigureAwait(false);
                relatedTable = relationship.ForeignTable;
            }
            else
            {
                Type relatedType = navigation.PropertyType;
                RelationshipMetadata relationship = FindReferenceRelationship(relationships, table, relatedType, navigation.Name)
                    ?? throw NoRelationship(navigation, table, relatedType);
                loaded = await LoadReferenceAsync(metadata, entities, navigation, relatedType, relationship, cancellationToken).ConfigureAwait(false);
                relatedTable = relationship.PrimaryTable;
            }

            // Descend into the entities just loaded for this navigation: a ThenInclude
            // chain on this node loads against the related table and the related rows.
            if (node.Children.Count > 0)
            {
                await LoadNodesAsync(metadata, relatedTable, loaded, node.Children, relationships, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static List<IncludeNode> BuildForest(IReadOnlyList<IReadOnlyList<IncludeStep>> paths)
    {
        // Merge the include paths into a navigation tree so a shared prefix (for example
        // two ThenIncludes off the same Include) loads once and both branches descend from
        // the same loaded entities instead of re-loading and overwriting the first branch.
        var roots = new List<IncludeNode>();
        foreach (IReadOnlyList<IncludeStep> path in paths)
        {
            List<IncludeNode> level = roots;
            foreach (IncludeStep step in path)
            {
                IncludeNode node = FindOrAddNode(level, step);
                level = node.Children;
            }
        }

        return roots;
    }

    private static IncludeNode FindOrAddNode(List<IncludeNode> level, IncludeStep step)
    {
        foreach (IncludeNode existing in level)
        {
            if (existing.Navigation.Equals(step.Navigation))
            {
                // Shared prefix: adopt the inline operators from whichever branch specifies
                // them so a plain reference to the same navigation doesn't drop the filter.
                if (existing.Operations.Count == 0 && step.Operations.Count > 0)
                {
                    existing.Operations = step.Operations;
                }

                return existing;
            }
        }

        var created = new IncludeNode(step.Navigation) { Operations = step.Operations };
        level.Add(created);
        return created;
    }

    private static async ValueTask<List<object>> LoadReferenceAsync(
        IncludeMetadataCache metadata,
        IReadOnlyList<object> roots,
        PropertyInfo navigation,
        Type relatedType,
        RelationshipMetadata relationship,
        CancellationToken cancellationToken)
    {
        EnsureConstructible(relatedType, navigation);
        Dictionary<string, object?[]> distinctKeys = CollectDistinctKeys(roots, relationship.ForeignColumns);
        Dictionary<string, object> parentsByKey = await BuildParentLookupAsync(
            metadata, relationship.PrimaryTable, relatedType, relationship.PrimaryColumns, distinctKeys, cancellationToken).ConfigureAwait(false);

        foreach (object root in roots)
        {
            string? key = BuildKeyFromObject(root, relationship.ForeignColumns);
            object? parent = key is not null && parentsByKey.TryGetValue(key, out object? match) ? match : null;
            navigation.SetValue(root, parent);
        }

        return [.. parentsByKey.Values];
    }

    private static async ValueTask<List<object>> LoadCollectionAsync(
        IncludeMetadataCache metadata,
        IReadOnlyList<object> roots,
        PropertyInfo navigation,
        Type relatedType,
        RelationshipMetadata relationship,
        IReadOnlyList<IncludeOperation> operations,
        CancellationToken cancellationToken)
    {
        EnsureConstructible(relatedType, navigation);
        Dictionary<string, object?[]> distinctKeys = CollectDistinctKeys(roots, relationship.PrimaryColumns);
        Dictionary<string, List<object>> childrenByKey = await BuildChildGroupsAsync(
            metadata, relationship.ForeignTable, relatedType, relationship.ForeignColumns, distinctKeys, cancellationToken).ConfigureAwait(false);

        var loaded = new List<object>();
        foreach (object root in roots)
        {
            string? key = BuildKeyFromObject(root, relationship.PrimaryColumns);
            IList list = RuntimeRowMapper.CreateList(relatedType);
            if (key is not null && childrenByKey.TryGetValue(key, out List<object>? children))
            {
                // Filtered / ordered include operators run in memory per parent, so each
                // parent's children are filtered, ordered, and paged independently before the
                // navigation is set and before a ThenInclude descends into the kept rows.
                foreach (object child in ApplyOperations(children, operations))
                {
                    list.Add(child);
                    loaded.Add(child);
                }
            }

            navigation.SetValue(root, list);
        }

        return loaded;
    }

    private static IEnumerable<object> ApplyOperations(IEnumerable<object> source, IReadOnlyList<IncludeOperation> operations)
    {
        foreach (IncludeOperation operation in operations)
        {
            source = operation.Apply(source);
        }

        return source;
    }

    private static async ValueTask<Dictionary<string, object>> BuildParentLookupAsync(
        IncludeMetadataCache metadata,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        Dictionary<string, object?[]> distinctKeys,
        CancellationToken cancellationToken)
    {
        // One parent per key: seek the parent key index when one covers the key
        // columns and the keys are few relative to the table, otherwise scan once.
        SeekPlan? plan = distinctKeys.Count > 0
            ? await ResolveSeekPlanAsync(metadata, table, keyColumns, distinctKeys.Count, cancellationToken).ConfigureAwait(false)
            : null;
        if (plan is not SeekPlan seek)
        {
            return await IndexRelatedAsync(metadata, table, type, keyColumns, cancellationToken).ConfigureAwait(false);
        }

        var map = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?[]> entry in distinctKeys)
        {
            IndexQueryCriteria criteria = BuildSeekCriteria(seek.IndexColumnCount, keyColumns.Count, entry.Value);
            await foreach (object[] row in metadata.Reader.ReadIndexRowsAsObjectsAsync(table, seek.IndexName, criteria, cancellationToken).ConfigureAwait(false))
            {
                map[entry.Key] = RuntimeRowMapper.Map(type, seek.Headers, row);
                break;
            }
        }

        return map;
    }

    private static async ValueTask<Dictionary<string, List<object>>> BuildChildGroupsAsync(
        IncludeMetadataCache metadata,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        Dictionary<string, object?[]> distinctKeys,
        CancellationToken cancellationToken)
    {
        // Many children per key: seek the foreign-key index when one covers the key
        // columns and the keys are few relative to the table, otherwise scan once.
        SeekPlan? plan = distinctKeys.Count > 0
            ? await ResolveSeekPlanAsync(metadata, table, keyColumns, distinctKeys.Count, cancellationToken).ConfigureAwait(false)
            : null;
        if (plan is not SeekPlan seek)
        {
            return await GroupRelatedAsync(metadata, table, type, keyColumns, cancellationToken).ConfigureAwait(false);
        }

        var map = new Dictionary<string, List<object>>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?[]> entry in distinctKeys)
        {
            var list = new List<object>();
            IndexQueryCriteria criteria = BuildSeekCriteria(seek.IndexColumnCount, keyColumns.Count, entry.Value);
            await foreach (object[] row in metadata.Reader.ReadIndexRowsAsObjectsAsync(table, seek.IndexName, criteria, cancellationToken).ConfigureAwait(false))
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
        IncludeMetadataCache metadata,
        string table,
        IReadOnlyList<string> keyColumns,
        int distinctKeyCount,
        CancellationToken cancellationToken)
    {
        // Index seeks are Jet4/ACE only; everything else falls back to a scan.
        if (metadata.Reader.Format == DatabaseFormat.Jet3Mdb)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await metadata.GetIndexesAsync(table, cancellationToken).ConfigureAwait(false);
        IndexMetadata? index = FindCoveringIndex(indexes, keyColumns);
        if (index is null)
        {
            return null;
        }

        // Cost guard: seeking K keys (each a B-tree descent) only beats one scan when K
        // is a small fraction of the related table. When the declared row count says we
        // would seek a large share of the table, scan instead. A row count of 0 (unknown
        // or empty) leaves the seek path enabled.
        long rowCount = await metadata.GetRowCountAsync(table, cancellationToken).ConfigureAwait(false);
        if (rowCount > 0 && (long)distinctKeyCount * SeekKeyCountTableFraction > rowCount)
        {
            return null;
        }

        (string[] headers, _) = await ReadHeadersAsync(metadata, table, keyColumns, cancellationToken).ConfigureAwait(false);
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
        IncludeMetadataCache metadata,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        CancellationToken cancellationToken)
    {
        (string[] headers, int[] keyIndices) = await ReadHeadersAsync(metadata, table, keyColumns, cancellationToken).ConfigureAwait(false);
        var map = new Dictionary<string, object>(StringComparer.Ordinal);
        await foreach (object[] row in metadata.Reader.Rows(table, progress: null, cancellationToken).ConfigureAwait(false))
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
        IncludeMetadataCache metadata,
        string table,
        Type type,
        IReadOnlyList<string> keyColumns,
        CancellationToken cancellationToken)
    {
        (string[] headers, int[] keyIndices) = await ReadHeadersAsync(metadata, table, keyColumns, cancellationToken).ConfigureAwait(false);
        var map = new Dictionary<string, List<object>>(StringComparer.Ordinal);
        await foreach (object[] row in metadata.Reader.Rows(table, progress: null, cancellationToken).ConfigureAwait(false))
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
        IncludeMetadataCache metadata,
        string table,
        IReadOnlyList<string> keyColumns,
        CancellationToken cancellationToken)
    {
        string[] headers = await metadata.GetHeadersAsync(table, cancellationToken).ConfigureAwait(false);
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

    internal static string? Normalize(object? value) => value switch
    {
        null or DBNull => null,
        bool b => b ? "b1" : "b0",
        byte or sbyte or short or ushort or int or uint or long =>
            "n" + FormatNumeric(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        ulong ul => "n" + FormatNumeric(ul),
        decimal m => "n" + FormatNumeric(m),
        float or double => NormalizeReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        char c => "s" + c,
        Guid g => "g" + g.ToString("N"),
        DateTime dt => "t" + dt.Ticks.ToString(CultureInfo.InvariantCulture),
        string s => "s" + s,
        byte[] bytes => "x" + BitConverter.ToString(bytes),
        _ => null,
    };

    private static string NormalizeReal(double value)
    {
        // Finite values inside decimal's range join the unified numeric key space so a
        // float/double matches an int, long, or decimal of the same value; non-finite or
        // out-of-range magnitudes (never realistic join keys) keep a self-consistent key.
        if (double.IsFinite(value) && value >= NumericKeyDecimalMin && value <= NumericKeyDecimalMax)
        {
            return "n" + FormatNumeric((decimal)value);
        }

        return "r" + value.ToString("R", CultureInfo.InvariantCulture);
    }

    private static string FormatNumeric(decimal value)
    {
        // Collapse equal values to one canonical string regardless of CLR type or scale so
        // both sides of a relationship match: integral values format as a plain integer (5,
        // 5.0, and 5m all yield "5") and fractional values strip trailing-zero scale (5.10m
        // and 5.1m both yield "5.1").
        if (decimal.Truncate(value) == value)
        {
            if (value is >= long.MinValue and <= long.MaxValue)
            {
                return ((long)value).ToString(CultureInfo.InvariantCulture);
            }

            if (value is > long.MaxValue and <= ulong.MaxValue)
            {
                return ((ulong)value).ToString(CultureInfo.InvariantCulture);
            }
        }

        return (value / 1.0000000000000000000000000000m).ToString(CultureInfo.InvariantCulture);
    }

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
        Type childType,
        string navigationName)
    {
        string parent = Simplify(parentTable);
        string child = Simplify(ResolveTableName(childType));
        RelationshipMetadata? firstMatch = null;
        foreach (RelationshipMetadata relationship in relationships)
        {
            if (Simplify(relationship.PrimaryTable) == parent && Simplify(relationship.ForeignTable) == child)
            {
                firstMatch ??= relationship;
                if (NavigationMatchesColumns(navigationName, relationship.ForeignColumns))
                {
                    return relationship;
                }
            }
        }

        return firstMatch;
    }

    private static RelationshipMetadata? FindReferenceRelationship(
        IReadOnlyList<RelationshipMetadata> relationships,
        string childTable,
        Type parentType,
        string navigationName)
    {
        string child = Simplify(childTable);
        string parent = Simplify(ResolveTableName(parentType));
        RelationshipMetadata? firstMatch = null;
        foreach (RelationshipMetadata relationship in relationships)
        {
            if (Simplify(relationship.ForeignTable) == child && Simplify(relationship.PrimaryTable) == parent)
            {
                firstMatch ??= relationship;
                if (NavigationMatchesColumns(navigationName, relationship.ForeignColumns))
                {
                    return relationship;
                }
            }
        }

        return firstMatch;
    }

    private static bool NavigationMatchesColumns(string navigationName, IReadOnlyList<string> foreignColumns)
    {
        // Disambiguates which relationship a navigation binds to when several link the
        // same table pair: a navigation name matches a foreign-key column when it equals
        // the column or the column with a trailing "Id" removed (EF-style: Buyer -> BuyerId).
        string nav = Simplify(navigationName);
        if (nav.Length == 0)
        {
            return false;
        }

        foreach (string column in foreignColumns)
        {
            string col = Simplify(column);
            if (col == nav)
            {
                return true;
            }

            if (col.Length > 2 && col.EndsWith("ID", StringComparison.Ordinal) && col[..^2] == nav)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Resolves the Access table name to match for a navigation's target POCO type: an
    /// explicit <see cref="TableAttribute"/> overrides the default convention that the POCO
    /// type name equals the table name, so a differently named type (such as a DTO or a
    /// "tbl"-prefixed table) still binds.
    /// </summary>
    /// <param name="type">The navigation target entity type.</param>
    /// <returns>The table name to match against the relationship catalog.</returns>
    private static string ResolveTableName(Type type) =>
        type.GetCustomAttribute<TableAttribute>(inherit: true) is { Name.Length: > 0 } table
            ? table.Name
            : type.Name;

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
            + "Ensure a foreign key linking the two tables exists in MSysRelationships, and that the related type name matches its "
            + "table name (ignoring case and non-alphanumeric separators) or carries a [Table(\"...\")] attribute naming the table.");

    private readonly record struct SeekPlan(string IndexName, int IndexColumnCount, string[] Headers);

    private sealed class IncludeNode(PropertyInfo navigation)
    {
        public PropertyInfo Navigation { get; } = navigation;

        public IReadOnlyList<IncludeOperation> Operations { get; set; } = [];

        public List<IncludeNode> Children { get; } = [];
    }

    private sealed class IncludeMetadataCache(AccessReader reader)
    {
        private readonly Dictionary<string, IReadOnlyList<IndexMetadata>> indexes = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string[]> headers = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, long> rowCounts = new(StringComparer.OrdinalIgnoreCase);

        public AccessReader Reader { get; } = reader;

        public async ValueTask<IReadOnlyList<IndexMetadata>> GetIndexesAsync(string table, CancellationToken cancellationToken)
        {
            if (this.indexes.TryGetValue(table, out IReadOnlyList<IndexMetadata>? cached))
            {
                return cached;
            }

            IReadOnlyList<IndexMetadata> value = await this.Reader.ListIndexesAsync(table, cancellationToken).ConfigureAwait(false);
            this.indexes[table] = value;
            return value;
        }

        public async ValueTask<long> GetRowCountAsync(string table, CancellationToken cancellationToken)
        {
            if (this.rowCounts.TryGetValue(table, out long cached))
            {
                return cached;
            }

            long value = await this.Reader.GetDeclaredRowCountAsync(table, cancellationToken).ConfigureAwait(false);
            this.rowCounts[table] = value;
            return value;
        }

        public async ValueTask<string[]> GetHeadersAsync(string table, CancellationToken cancellationToken)
        {
            if (this.headers.TryGetValue(table, out string[]? cached))
            {
                return cached;
            }

            IReadOnlyList<ColumnMetadata> meta = await this.Reader.GetColumnMetadataAsync(table, cancellationToken).ConfigureAwait(false);
            string[] value = new string[meta.Count];
            for (int i = 0; i < meta.Count; i++)
            {
                value[i] = meta[i].Name;
            }

            this.headers[table] = value;
            return value;
        }
    }
}
