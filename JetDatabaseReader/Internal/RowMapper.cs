namespace JetDatabaseReader;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;

/// <summary>
/// Maps <c>object[]</c> rows (keyed by column headers) to POCO instances of <typeparamref name="T"/>.
/// Column-to-property matching is case-insensitive. Unmatched properties are left at their default value.
/// </summary>
internal static class RowMapper<T>
    where T : new()
{
    private static readonly ConcurrentDictionary<string, PropertyInfo> PropertyMap = BuildPropertyMap();

    /// <summary>
    /// Builds the index mapping from column headers to property setters.
    /// Returns an array whose length equals <paramref name="headers"/>.Count.
    /// Each element is either a <see cref="PropertyInfo"/> for a matched property, or <c>null</c> if no match.
    /// </summary>
    public static PropertyInfo?[] BuildIndex(IReadOnlyList<string> headers)
    {
        var index = new PropertyInfo?[headers.Count];
        for (int i = 0; i < headers.Count; i++)
        {
            PropertyMap.TryGetValue(headers[i], out index[i]);
        }

        return index;
    }

    /// <summary>
    /// Maps a single <c>object[]</c> row to a new instance of <typeparamref name="T"/>
    /// using the pre-built <paramref name="index"/>.
    /// </summary>
    public static T Map(object[] row, PropertyInfo?[] index)
    {
        var item = new T();
        int len = Math.Min(row.Length, index.Length);

        for (int i = 0; i < len; i++)
        {
            PropertyInfo? prop = index[i];
            if (prop == null)
            {
                continue;
            }

            object value = row[i];
            if (value == null || value is DBNull)
            {
                continue;
            }

            Type targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            prop.SetValue(item, targetType == value.GetType() ? value : Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture));
        }

        return item;
    }

    /// <summary>
    /// Extracts property values from <paramref name="item"/> into an <c>object[]</c>
    /// ordered by the pre-built <paramref name="index"/>.
    /// Unmatched columns (null entries in the index) produce <see cref="DBNull.Value"/>.
    /// </summary>
    public static object[] ToRow(T item, PropertyInfo?[] index)
    {
        var row = new object[index.Length];
        for (int i = 0; i < index.Length; i++)
        {
            PropertyInfo? prop = index[i];
            row[i] = prop?.GetValue(item) ?? DBNull.Value;
        }

        return row;
    }

    private static ConcurrentDictionary<string, PropertyInfo> BuildPropertyMap()
    {
        var map = new ConcurrentDictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (PropertyInfo prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (prop.CanWrite)
            {
                map[prop.Name] = prop;
            }
        }

        return map;
    }
}
