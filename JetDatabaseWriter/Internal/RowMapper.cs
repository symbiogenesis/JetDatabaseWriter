namespace JetDatabaseWriter.Internal;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;

/// <summary>
/// Maps <c>object[]</c> rows (keyed by column headers) to POCO instances of <typeparamref name="T"/>.
/// Column-to-property matching is case-insensitive. Unmatched properties are left at their default value.
/// Uses compiled expression trees for high-performance property access.
/// </summary>
internal static class RowMapper<T>
    where T : new()
{
    private static readonly Dictionary<string, Accessor> PropertyMap = BuildPropertyMap();

    private static readonly MethodInfo CoerceToTargetMethod =
        typeof(RowMapper<T>).GetMethod(nameof(CoerceToTarget), BindingFlags.NonPublic | BindingFlags.Static)!;

    // Per-TableDef cache for the compiled write delegate. Keyed by
    // reference identity so the same TableDef instance reused across many
    // rows pays the expression-compilation cost exactly once. TableDef is
    // already cached upstream by AccessWriter, so a ConditionalWeakTable
    // lets the entries fall out naturally when a TableDef is evicted.
    private static readonly ConditionalWeakTable<TableDef, Func<T, object[]>> WriteCache = new();

    /// <summary>
    /// Builds the index mapping from column headers to compiled property accessors.
    /// Returns an array whose length equals <paramref name="headers"/>.Count.
    /// Each element is either an <see cref="Accessor"/> for a matched property, or <c>null</c> if no match.
    /// </summary>
    public static Accessor?[] BuildIndex(IReadOnlyList<string> headers)
    {
        int count = headers.Count;
        var index = new Accessor?[count];
        for (int i = 0; i < count; i++)
        {
            PropertyMap.TryGetValue(headers[i], out index[i]);
        }

        return index;
    }

    /// <summary>
    /// Compiles a single delegate that materializes a fresh <typeparamref name="T"/>
    /// from an <c>object?[]</c> row in one call. The <c>new T()</c> is baked into
    /// the compiled expression tree itself — no captured delegates, no extra
    /// per-row allocations.
    ///
    /// When <paramref name="sourceTypes"/> is supplied and a column's source type
    /// matches the target property's underlying type, the generated expression
    /// emits a direct unbox-and-assign (skipping the runtime <c>GetType()</c>
    /// check and the <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>
    /// fallback). Hyperlink ↔ string interop is preserved via the
    /// <see cref="CoerceToTarget"/> helper. Phase 5 of typed-row-read-perf-plan.
    /// </summary>
    public static Func<object?[], T> Build(IReadOnlyList<string> headers, IReadOnlyList<Type>? sourceTypes = null)
    {
        Guard.NotNull(headers, nameof(headers));

        var rowParam = Expression.Parameter(typeof(object?[]), "row");
        var itemLocal = Expression.Variable(typeof(T), "item");
        var lenLocal = Expression.Variable(typeof(int), "len");

        // Hoist the null constant so duplicate AST nodes aren't allocated per column.
        var nullObj = Expression.Constant(null, typeof(object));

        int columnCount = headers.Count;
        int sourceCount = sourceTypes?.Count ?? 0;

        // Pre-size: 2 prelude assigns + up to columnCount per-column blocks + return.
        var statements = new List<Expression>(columnCount + 3)
        {
            Expression.Assign(itemLocal, Expression.New(typeof(T))),
            Expression.Assign(lenLocal, Expression.ArrayLength(rowParam)),
        };

        for (int i = 0; i < columnCount; i++)
        {
            if (!PropertyMap.TryGetValue(headers[i], out Accessor? acc))
            {
                continue;
            }

            PropertyInfo prop = acc.Property;
            Type propType = prop.PropertyType;
            Type underlying = Nullable.GetUnderlyingType(propType) ?? propType;
            Type? sourceType = i < sourceCount ? sourceTypes![i] : null;
            bool fastDirect = sourceType != null && sourceType == underlying;

            var indexConst = Expression.Constant(i);
            var valueLocal = Expression.Variable(typeof(object), "v");
            var fetchValue = Expression.Assign(
                valueLocal,
                Expression.ArrayAccess(rowParam, indexConst));
            var notNullOrDbNull = Expression.AndAlso(
                Expression.NotEqual(valueLocal, nullObj),
                Expression.Not(Expression.TypeIs(valueLocal, typeof(DBNull))));

            Expression assignBlock;
            if (fastDirect)
            {
                // Direct unbox + assign — no Hyperlink/Convert detour. The
                // `Expression.Convert(v, propType)` form handles both the
                // boxed→T unbox and the implicit T→Nullable<T> wrap.
                assignBlock = Expression.Assign(
                    Expression.Property(itemLocal, prop),
                    Expression.Convert(valueLocal, propType));
            }
            else
            {
                // Mixed/unknown source type: defer to the shared coercion
                // helper, then assign only when it returns a non-null result
                // (matches Map's "skip on Hyperlink.Parse failure" semantics).
                // Reuse `valueLocal` as the in/out slot so we don't need a
                // second local — the original `value` is no longer needed
                // after the coerce call.
                var coerceCall = Expression.Call(
                    CoerceToTargetMethod,
                    valueLocal,
                    Expression.Constant(underlying, typeof(Type)));
                var assign = Expression.Assign(
                    Expression.Property(itemLocal, prop),
                    Expression.Convert(valueLocal, propType));
                assignBlock = Expression.Block(
                    Expression.Assign(valueLocal, coerceCall),
                    Expression.IfThen(Expression.NotEqual(valueLocal, nullObj), assign));
            }

            statements.Add(Expression.IfThen(
                Expression.LessThan(indexConst, lenLocal),
                Expression.Block(
                    [valueLocal],
                    fetchValue,
                    Expression.IfThen(notNullOrDbNull, assignBlock))));
        }

        // Final expression: return item.
        statements.Add(itemLocal);

        var body = Expression.Block(typeof(T), [itemLocal, lenLocal], statements);
        return Expression.Lambda<Func<object?[], T>>(body, rowParam).Compile();
    }

    /// <summary>
    /// Convenience overload that pulls the header list and CLR source-type list
    /// from a column-metadata sequence in one shot.
    /// </summary>
    public static Func<object?[], T> Build(IReadOnlyList<ColumnMetadata> meta)
    {
        Guard.NotNull(meta, nameof(meta));
        var headers = new string[meta.Count];
        var types = new Type[meta.Count];
        for (int i = 0; i < meta.Count; i++)
        {
            headers[i] = meta[i].Name;
            types[i] = meta[i].ClrType;
        }

        return Build(headers, types);
    }

    /// <summary>
    /// Convenience overload that pulls the header list from <c>td.Columns</c>
    /// and the CLR source types from the cached <see cref="TableDef.ClrTypes"/>
    /// projection (populated by <c>InitializeColumnMetadata</c>).
    /// </summary>
    public static Func<object?[], T> Build(TableDef td)
    {
        Guard.NotNull(td, nameof(td));
        var headers = new string[td.Columns.Count];
        for (int i = 0; i < td.Columns.Count; i++)
        {
            headers[i] = td.Columns[i].Name;
        }

        return Build(headers, td.ClrTypes);
    }

    /// <summary>
    /// Projects <paramref name="item"/> to an <c>object[]</c> in column order
    /// using a compiled delegate cached on <paramref name="td"/>. The
    /// expression compilation happens at most once per <see cref="TableDef"/>
    /// instance and is transparently amortised across batch writes.
    /// </summary>
    public static object[] ToRow(TableDef td, T item)
    {
        Guard.NotNull(td, nameof(td));
        Func<T, object[]> writer = WriteCache.GetValue(td, static key => BuildToRow(key));
        return writer(item);
    }

    /// <summary>
    /// Compiles a delegate that extracts property values from a
    /// <typeparamref name="T"/> into an <c>object[]</c> in the column order
    /// of <paramref name="td"/>. Unmatched columns produce <see cref="DBNull.Value"/>.
    /// </summary>
    private static Func<T, object[]> BuildToRow(TableDef td)
    {
        int count = td.Columns.Count;
        var itemParam = Expression.Parameter(typeof(T), "item");
        var dbNull = Expression.Constant(DBNull.Value, typeof(object));

        // Build the array via NewArrayInit so the compiled body is a single
        // `newarr` followed by inline `stelem.ref` per element — no scratch
        // local, no Block, no per-element ArrayAccess assignment expression.
        var values = new Expression[count];
        for (int i = 0; i < count; i++)
        {
            Expression valueExpr;
            if (PropertyMap.TryGetValue(td.Columns[i].Name, out Accessor? acc))
            {
                Type pt = acc.Property.PropertyType;
                Expression propAccess = Expression.Property(itemParam, acc.Property);
                if (pt.IsValueType && Nullable.GetUnderlyingType(pt) == null)
                {
                    // Non-nullable value type — can never be null, so skip the
                    // Coalesce(null, DBNull) check that would otherwise force
                    // an avoidable box just to compare against null.
                    valueExpr = Expression.Convert(propAccess, typeof(object));
                }
                else
                {
                    // Reference or Nullable<T>: box (if needed) then coalesce
                    // a null payload to DBNull.Value.
                    Expression boxed = pt.IsValueType
                        ? Expression.Convert(propAccess, typeof(object))
                        : propAccess;
                    valueExpr = Expression.Coalesce(boxed, dbNull);
                }
            }
            else
            {
                valueExpr = dbNull;
            }

            values[i] = valueExpr;
        }

        var body = Expression.NewArrayInit(typeof(object), values);
        return Expression.Lambda<Func<T, object[]>>(body, itemParam).Compile();
    }

    /// <summary>
    /// Runtime coercion fallback used by <see cref="Build(IReadOnlyList{string}, IReadOnlyList{Type}?)"/>
    /// and <see cref="Map"/> for columns whose source type is unknown or differs
    /// from the property's underlying type. Returns <see langword="null"/> when a
    /// Hyperlink-typed property cannot parse the supplied string (signals "skip
    /// this assignment").
    /// </summary>
    private static object? CoerceToTarget(object value, Type targetUnderlying)
    {
        if (value.GetType() == targetUnderlying)
        {
            return value;
        }

        if (targetUnderlying == typeof(Hyperlink) && value is string hs)
        {
            return Hyperlink.Parse(hs);
        }

        if (targetUnderlying == typeof(string) && value is Hyperlink hv)
        {
            return hv.ToString();
        }

        return Convert.ChangeType(value, targetUnderlying, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Maps a single row to a new instance of <typeparamref name="T"/> using the
    /// pre-built <paramref name="index"/>. Reflection-driven path retained for
    /// tests and ad-hoc callers; hot read paths should use <see cref="Build(IReadOnlyList{string}, IReadOnlyList{Type}?)"/>.
    /// </summary>
    public static T Map(IReadOnlyList<object?> row, Accessor?[] index)
    {
        T item = new();
        int len = Math.Min(row.Count, index.Length);

        for (int i = 0; i < len; i++)
        {
            Accessor? acc = index[i];
            if (acc == null)
            {
                continue;
            }

            object? value = row[i];
            if (value == null || value is DBNull)
            {
                continue;
            }

            object? coerced = CoerceToTarget(value, acc.TargetType);
            if (coerced != null)
            {
                acc.Setter(item, coerced);
            }
        }

        return item;
    }

    private static Dictionary<string, Accessor> BuildPropertyMap()
    {
        PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var map = new Dictionary<string, Accessor>(props.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < props.Length; i++)
        {
            PropertyInfo prop = props[i];
            if (prop.CanWrite)
            {
                map[prop.Name] = new Accessor(prop);
            }
        }

        return map;
    }

    /// <summary>
    /// Pre-compiled property accessor holding a setter, getter, and pre-resolved target type.
    /// </summary>
    internal sealed class Accessor
    {
        public Accessor(PropertyInfo prop)
        {
            Property = prop;
            TargetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            var instance = Expression.Parameter(typeof(T), "i");
            var value = Expression.Parameter(typeof(object), "v");
            Setter = Expression.Lambda<Action<T, object>>(
                Expression.Assign(
                    Expression.Property(instance, prop),
                    Expression.Convert(value, prop.PropertyType)),
                instance,
                value).Compile();
        }

        public Action<T, object> Setter { get; }

        public Type TargetType { get; }

        internal PropertyInfo Property { get; }
    }
}
