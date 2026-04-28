namespace JetDatabaseWriter.Internal;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Models;

/// <summary>
/// Maps <c>object[]</c> rows (keyed by column headers) to POCO instances of <typeparamref name="T"/>.
/// Column-to-property matching is case-insensitive. Unmatched properties are left at their default value.
/// Uses compiled expression trees for high-performance property access.
/// </summary>
internal static class RowMapper<T>
    where T : new()
{
    private static readonly Func<T> Factory = Expression.Lambda<Func<T>>(Expression.New(typeof(T))).Compile();
    private static readonly Dictionary<string, Accessor> PropertyMap = BuildPropertyMap();

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
    /// Maps a single row to a new instance of <typeparamref name="T"/>
    /// using the pre-built <paramref name="index"/>.
    /// </summary>
    public static T Map(IReadOnlyList<object?> row, Accessor?[] index)
    {
        T item = Factory();
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

            if (acc.TargetType != value.GetType())
            {
                // Hyperlink ↔ string interop: a Hyperlink-typed property bound
                // to a column read as raw string (e.g. plain MEMO without the
                // hyperlink flag) is parsed; a string-typed property bound to
                // a hyperlink-flagged column receives the encoded form.
                if (acc.TargetType == typeof(Hyperlink) && value is string hs)
                {
                    Hyperlink? parsed = Hyperlink.Parse(hs);
                    if (parsed == null)
                    {
                        continue;
                    }

                    value = parsed;
                }
                else if (acc.TargetType == typeof(string) && value is Hyperlink hv)
                {
                    value = hv.ToString();
                }
                else
                {
                    value = Convert.ChangeType(value, acc.TargetType, CultureInfo.InvariantCulture);
                }
            }

            acc.Setter(item, value);
        }

        return item;
    }

    /// <summary>
    /// Extracts property values from <paramref name="item"/> into an <c>object[]</c>
    /// ordered by the pre-built <paramref name="index"/>.
    /// Unmatched columns (null entries in the index) produce <see cref="DBNull.Value"/>.
    /// </summary>
    public static object[] ToRow(T item, Accessor?[] index)
    {
        var row = new object[index.Length];
        for (int i = 0; i < index.Length; i++)
        {
            Accessor? acc = index[i];
            row[i] = acc != null ? (acc.Getter(item) ?? DBNull.Value) : DBNull.Value;
        }

        return row;
    }

    private static Dictionary<string, Accessor> BuildPropertyMap()
    {
        var map = new Dictionary<string, Accessor>(StringComparer.OrdinalIgnoreCase);
        foreach (PropertyInfo prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
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
            Name = prop.Name;
            TargetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

            var instance = Expression.Parameter(typeof(T), "i");
            var value = Expression.Parameter(typeof(object), "v");
            Setter = Expression.Lambda<Action<T, object>>(
                Expression.Assign(
                    Expression.Property(instance, prop),
                    Expression.Convert(value, prop.PropertyType)),
                instance,
                value).Compile();

            var getInst = Expression.Parameter(typeof(T), "i");
            Getter = Expression.Lambda<Func<T, object>>(
                Expression.Convert(Expression.Property(getInst, prop), typeof(object)),
                getInst).Compile();
        }

        public string Name { get; }

        public Action<T, object> Setter { get; }

        public Func<T, object> Getter { get; }

        public Type TargetType { get; }
    }
}
