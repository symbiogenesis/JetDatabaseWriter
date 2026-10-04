namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;

/// <summary>
/// Maps <c>object[]</c> rows (keyed by column headers) to POCO instances of <typeparamref name="T"/>.
/// Each column binds to the property <see cref="EntityMap"/> maps it to: the property's <c>[Column("...")]</c>
/// name, or else its own name, compared case-insensitively; <c>[NotMapped]</c> properties are skipped.
/// Unmatched properties are left at their default value. A value of another type than its
/// property converts through <see cref="ValueCoercer"/>, which throws
/// <see cref="InvalidCastException"/> naming the column and the property when it cannot.
/// Uses compiled expression trees for high-performance property access.
/// </summary>
/// <typeparam name="T">The row type whose public properties are bound to column headers.</typeparam>
internal static class RowMapper<T>
    where T : new()
{
    /// <summary>
    /// Per-TableDef cache for the compiled write delegate. Keyed by
    /// reference identity so the same TableDef instance reused across many
    /// rows pays the expression-compilation cost exactly once. TableDef is
    /// already cached upstream by AccessWriter, so a ConditionalWeakTable
    /// lets the entries fall out naturally when a TableDef is evicted.
    /// </summary>
    private static readonly ConditionalWeakTable<TableDef, Func<T, object[]>> WriteCache = [];

    /// <summary>
    /// Gets the column name to accessor map, built from <see cref="EntityMap"/> on
    /// first use. It is built lazily rather than by the static initializer so that a mapping
    /// error, such as two properties naming one column, surfaces as its own exception
    /// instead of a <see cref="TypeInitializationException"/> that poisons the type.
    /// </summary>
    private static Dictionary<string, Accessor> PropertyMap => field ??= BuildPropertyMap();

    /// <summary>
    /// Returns a boolean mask the same length as <paramref name="headers"/> indicating
    /// which header indices the compiled mapper actually consumes (i.e. those whose
    /// header name maps to a settable property on <typeparamref name="T"/>). Used by
    /// the read-path projection optimisation to skip per-row decode of columns
    /// that <typeparamref name="T"/> never reads.
    /// </summary>
    /// <param name="headers">The headers.</param>
    public static bool[] GetBoundColumnMask(IReadOnlyList<string> headers)
    {
        Guard.NotNull(headers, nameof(headers));
        int count = headers.Count;
        bool[] mask = new bool[count];
        for (int i = 0; i < count; i++)
        {
            mask[i] = PropertyMap.ContainsKey(headers[i]);
        }

        return mask;
    }

    /// <summary>
    /// Returns the <see cref="Accessor"/> for the property mapped to the
    /// column <paramref name="header"/> (case-insensitive), or
    /// <see langword="null"/> when no property matches. Used by the
    /// direct-decoder builder.
    /// </summary>
    /// <param name="header">The header.</param>
    internal static Accessor? TryGetAccessor(string header)
    {
        PropertyMap.TryGetValue(header, out Accessor? acc);
        return acc;
    }

    /// <summary>
    /// <para>
    /// Compiles a single delegate that materializes a fresh <typeparamref name="T"/>
    /// from an <c>object?[]</c> row in one call. The <c>new T()</c> is baked into
    /// the compiled expression tree itself — no captured delegates, no extra
    /// per-row allocations.
    /// </para>
    /// <para>
    /// When <paramref name="sourceTypes"/> is supplied and a column's source type
    /// matches the target property's underlying type, the generated expression
    /// emits a direct unbox-and-assign. Every other column converts through
    /// <see cref="ValueCoercer.Coerce(object, Type, string, PropertyInfo)"/>,
    /// which passes through a value the property can already hold.
    /// </para>
    /// </summary>
    /// <param name="headers">The headers.</param>
    /// <param name="sourceTypes">The source types.</param>
    /// <returns>The mapper. It throws <see cref="InvalidCastException"/> for a value its property cannot hold.</returns>
    public static Func<object?[], T> Build(IReadOnlyList<string> headers, IReadOnlyList<Type>? sourceTypes = null)
    {
        Guard.NotNull(headers, nameof(headers));

        ParameterExpression rowParam = Expression.Parameter(typeof(object?[]), "row");
        ParameterExpression itemLocal = Expression.Variable(typeof(T), "item");
        ParameterExpression lenLocal = Expression.Variable(typeof(int), "len");

        // Hoist the null constant so duplicate AST nodes aren't allocated per column.
        ConstantExpression nullObj = Expression.Constant(null, typeof(object));

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

            ConstantExpression indexConst = Expression.Constant(i);
            ParameterExpression valueLocal = Expression.Variable(typeof(object), "v");
            BinaryExpression fetchValue = Expression.Assign(
                valueLocal,
                Expression.ArrayAccess(rowParam, indexConst));
            BinaryExpression notNullOrDbNull = Expression.AndAlso(
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
                // Mixed/unknown source type: defer to the shared coercer,
                // then assign only when it returns a non-null result
                // ("skip on an empty Hyperlink").
                // Reuse `valueLocal` as the in/out slot so we don't need a
                // second local — the original `value` is no longer needed
                // after the coerce call.
                MethodCallExpression coerceCall = Expression.Call(
                    ValueCoercer.CoerceMethod,
                    valueLocal,
                    Expression.Constant(underlying, typeof(Type)),
                    Expression.Constant(headers[i], typeof(string)),
                    Expression.Constant(prop, typeof(PropertyInfo)));
                BinaryExpression assign = Expression.Assign(
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

        BlockExpression body = Expression.Block(typeof(T), [itemLocal, lenLocal], statements);
        return Expression.Lambda<Func<object?[], T>>(body, rowParam).Compile();
    }

    /// <summary>
    /// Convenience overload that pulls the header list and CLR source-type list
    /// from a column-metadata sequence in one shot.
    /// </summary>
    /// <param name="meta">The column metadata.</param>
    public static Func<object?[], T> Build(IReadOnlyList<ColumnMetadata> meta)
    {
        Guard.NotNull(meta, nameof(meta));
        string[] headers = new string[meta.Count];
        var types = new Type[meta.Count];
        for (int i = 0; i < meta.Count; i++)
        {
            headers[i] = meta[i].Name;
            types[i] = meta[i].ClrType;
        }

        return Build(headers, types);
    }

    /// <summary>
    /// Projects <paramref name="item"/> to an <c>object[]</c> in column order
    /// using a compiled delegate cached on <paramref name="td"/>. The
    /// expression compilation happens at most once per <see cref="TableDef"/>
    /// instance and is transparently amortised across batch writes.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="item">The source item.</param>
    public static object[] ToRow(TableDef td, T item)
    {
        Guard.NotNull(td, nameof(td));
        Func<T, object[]> writer = WriteCache.GetValue(td, static key => BuildToRow(key));
        return writer(item);
    }

    /// <summary>
    /// Compiles a delegate that extracts property values from a
    /// <typeparamref name="T"/> into an <c>object[]</c> in the column order
    /// of <paramref name="td"/>, for an insert. A mapped property whose value is
    /// <see langword="null"/> produces <see cref="DBNull.Value"/>, which stores
    /// database null. A column no readable property maps to is left out of the
    /// insert: it produces <see cref="DbDefault.Value"/>, so it gets its default.
    /// A non-nullable numeric property mapped to an AutoNumber column produces
    /// <see cref="DbDefault.Value"/> while it holds its CLR default (<c>0</c>), so
    /// the insert generates the next AutoNumber, as Entity Framework treats a
    /// generated key; any other value is stored as given. Only inserts use this
    /// delegate, so an update never turns a 0 into a generated value.
    /// </summary>
    /// <param name="td">Parsed table definition.</param>
    private static Func<T, object[]> BuildToRow(TableDef td)
    {
        int count = td.Columns.Count;
        ParameterExpression itemParam = Expression.Parameter(typeof(T), "item");
        ConstantExpression dbNull = Expression.Constant(DBNull.Value, typeof(object));
        ConstantExpression dbDefault = Expression.Constant(DbDefault.Value, typeof(object));

        // Build the array via NewArrayInit so the compiled body is a single
        // `newarr` followed by inline `stelem.ref` per element — no scratch
        // local, no Block, no per-element ArrayAccess assignment expression.
        var values = new Expression[count];
        for (int i = 0; i < count; i++)
        {
            Expression valueExpr;
            if (PropertyMap.TryGetValue(td.Columns[i].Name, out Accessor? acc) && acc.Property.CanRead)
            {
                Type pt = acc.Property.PropertyType;
                Expression propAccess = Expression.Property(itemParam, acc.Property);
                if (pt.IsValueType && Nullable.GetUnderlyingType(pt) == null && td.Columns[i].IsAutoNumber && CanCompareToDefault(pt))
                {
                    // Non-nullable value type on an AutoNumber column: its CLR
                    // default (0) means "not supplied", so the insert generates
                    // the next value, as for null; any other value is stored.
                    ParameterExpression local = Expression.Variable(pt, "id");
                    valueExpr = Expression.Block(
                        typeof(object),
                        [local],
                        Expression.Assign(local, propAccess),
                        Expression.Condition(
                            Expression.Equal(local, Expression.Default(pt)),
                            dbDefault,
                            Expression.Convert(local, typeof(object))));
                }
                else if (pt.IsValueType && Nullable.GetUnderlyingType(pt) == null)
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
                valueExpr = dbDefault;
            }

            values[i] = valueExpr;
        }

        NewArrayExpression body = Expression.NewArrayInit(typeof(object), values);
        return Expression.Lambda<Func<T, object[]>>(body, itemParam).Compile();
    }

    /// <summary>
    /// Returns whether <see cref="Expression.Equal(Expression, Expression)"/> can compare a
    /// value of <paramref name="type"/> with its CLR default: the primitives,
    /// <see cref="decimal"/> and enums, which cover every type an AutoNumber property can have.
    /// </summary>
    /// <param name="type">A non-nullable value type.</param>
    private static bool CanCompareToDefault(Type type) => type.IsPrimitive || type.IsEnum || type == typeof(decimal);

    private static Dictionary<string, Accessor> BuildPropertyMap()
    {
        IReadOnlyList<EntityProperty> properties = EntityMap.For(typeof(T)).Properties;
        var map = new Dictionary<string, Accessor>(properties.Count, StringComparer.OrdinalIgnoreCase);
        foreach (EntityProperty property in properties)
        {
            map[property.ColumnName] = new Accessor(property.Property);
        }

        return map;
    }

    /// <summary>
    /// Property accessor holding the mapped property and its pre-resolved target type.
    /// </summary>
    /// <param name="prop">The mapped property.</param>
    internal sealed class Accessor(PropertyInfo prop)
    {
        public Type TargetType { get; } = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

        internal PropertyInfo Property { get; } = prop;
    }
}
