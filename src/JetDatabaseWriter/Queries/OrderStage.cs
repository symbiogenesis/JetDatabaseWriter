namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Verification;

/// <summary>
/// A contiguous run of <c>OrderBy</c>/<c>ThenBy</c> keys. Ordering buffers the source
/// and sorts it; later stages observe the sorted sequence.
/// </summary>
internal sealed class OrderStage : QueryStage
{
    public List<OrderingKey> Keys { get; } = [];

    public override async IAsyncEnumerable<T> Apply<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new List<T>();
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(item);
        }

        foreach (T item in Sort(buffer, this.Keys))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Finds an index whose key order is provably identical to this stage's stable sort, so
    /// the provider can read the source in key order and skip the in-memory sort. Returns
    /// <see langword="null"/> when any key is not an order-safe integer column access or no
    /// index covers the ordering.
    /// </summary>
    /// <param name="indexes">The candidate indexes on the queried table.</param>
    /// <param name="definition">The actual native column types and identities.</param>
    /// <param name="entityType">The concrete entity type the provider materializes.</param>
    /// <returns>The covering index, or <see langword="null"/> when none qualifies.</returns>
    public IndexMetadata? FindCoveringIndex(IReadOnlyList<IndexMetadata> indexes, TableDef definition, Type entityType)
    {
        var columns = new List<(string Column, bool Descending)>(this.Keys.Count);
        foreach (OrderingKey key in this.Keys)
        {
            if (!TryDescribeIntegerKey(key, definition, entityType, out string? column))
            {
                return null;
            }

            columns.Add((column!, key.Descending));
        }

        foreach (IndexMetadata index in indexes)
        {
            if (Covers(index, columns))
            {
                return index;
            }
        }

        return null;
    }

    private static List<T> Sort<T>(List<T> buffer, List<OrderingKey> keys)
    {
        var compiled = new QuerySortKey<T>[keys.Count];
        for (int i = 0; i < keys.Count; i++)
        {
            compiled[i] = new QuerySortKey<T>(CompileKey<T>(keys[i].KeySelector), QueryKeyComparer.Instance, keys[i].Descending);
        }

        return QueryOrdering.Apply(buffer, compiled).ToList();
    }

    private static Func<T, object?> CompileKey<T>(LambdaExpression selector)
    {
        ParameterExpression parameter = selector.Parameters[0];
        Expression body = Expression.Convert(selector.Body, typeof(object));
        return QueryLambdaCompiler.Shared.Compile(Expression.Lambda<Func<T, object?>>(body, parameter));
    }

    private static bool TryDescribeIntegerKey(OrderingKey key, TableDef definition, Type entityType, out string? column)
    {
        column = null;

        // Only a direct access to a mapped property of the lambda parameter (e.g. i => i.Id)
        // can reuse native order. Casts keep their full key evaluation, including checked
        // overflow, and custom accessors can transform a mapped value. The generated gate
        // admits only exact integral native-to-property mappings; Text and lossy numeric
        // projections cannot borrow the order of the stored keys.
        if (key.KeySelector.Body is not MemberExpression { Member: PropertyInfo property, Expression: ParameterExpression parameter }
            || parameter != key.KeySelector.Parameters[0] || parameter.Type != entityType)
        {
            return false;
        }

        // Every mapped setter runs during materialization. A custom setter on
        // another property can change even an automatic key property. Covariant
        // queries likewise must not substitute a base type's different mapping.
        var map = EntityMap.For(entityType);
        if (map.Properties.All(static candidate => IsAutomaticProperty(candidate.Property))
            && map.FindByMember(property) is EntityProperty mapped && mapped.Property.Equals(property)
            && definition.FindColumn(mapped.ColumnName) is ColumnInfo { IsCalculated: false } native
            && IndexMutationKernel.AdmitIntegerOrdering((byte)native.Type, IntegerTypeCode(property.PropertyType)))
        {
            column = mapped.ColumnName;
            return true;
        }

        return false;
    }

    private static bool IsAutomaticProperty(PropertyInfo property)
        => property.GetMethod is { IsVirtual: false } getter && property.SetMethod is { IsVirtual: false } setter
            && getter.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
            && setter.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false);

    private static byte IntegerTypeCode(Type type)
    {
        Type underlying = Nullable.GetUnderlyingType(type) ?? type;
        if (underlying == typeof(byte))
        {
            return (byte)ColumnType.ByteType;
        }

        if (underlying == typeof(short))
        {
            return (byte)ColumnType.IntegerType;
        }

        if (underlying == typeof(int))
        {
            return (byte)ColumnType.LongIntegerType;
        }

        return underlying == typeof(long) ? (byte)ColumnType.BigIntType : (byte)0;
    }

    private static bool Covers(IndexMetadata index, List<(string Column, bool Descending)> keys)
    {
        // The index serves the ordering only when it has a usable B-tree root, never omits
        // rows (no ignore-nulls), and has unique nonnull keys, ruling out stable-sort ties
        // and null-to-default mapping. Its columns must match the ordering one-for-one,
        // including sort direction. Optional unique indexes can have repeated Null keys.
        if (index.FirstDp <= 0 || !index.EnforcesUniqueness || index.IgnoreNulls
            || (index.Kind != IndexKind.PrimaryKey && !index.IsRequired) || index.Columns.Count != keys.Count)
        {
            return false;
        }

        for (int i = 0; i < keys.Count; i++)
        {
            IndexColumnReference column = index.Columns[i];
            bool ascendingRequested = !keys[i].Descending;
            if (column.IsAscending != ascendingRequested
                || !string.Equals(column.Name, keys[i].Column, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }
}
