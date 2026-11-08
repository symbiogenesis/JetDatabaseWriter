namespace JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Reflection;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

/// <summary>
/// Resolves JET/ACE table names, column names, storage types, and value conversions for EF Core model metadata.
/// </summary>
internal static class JetEntityTypeMetadata
{
    private const string RelationalTableNameAnnotation = "Relational:TableName";
    private const string RelationalColumnNameAnnotation = "Relational:ColumnName";
    private const string RelationalDefaultValueAnnotation = "Relational:DefaultValue";
    private const string JetDefaultValueAnnotation = "Jet:DefaultValue";

    /// <summary>
    /// Resolves the physical JET table name for <paramref name="entityType"/>.
    /// </summary>
    /// <param name="entityType">The EF Core entity type.</param>
    /// <returns>The resolved table name.</returns>
    internal static string GetTableName(IReadOnlyEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        if (entityType.FindAnnotation(RelationalTableNameAnnotation)?.Value is string annotatedName
            && !string.IsNullOrWhiteSpace(annotatedName))
        {
            return annotatedName;
        }

        TableAttribute? directTableAttribute = entityType.ClrType.GetCustomAttribute<TableAttribute>(inherit: false);
        if (directTableAttribute is not null && !string.IsNullOrWhiteSpace(directTableAttribute.Name))
        {
            return directTableAttribute.Name;
        }

        IReadOnlyEntityType rootType = entityType.GetRootType();
        if (!ReferenceEquals(rootType, entityType))
        {
            return GetTableName(rootType);
        }

        TableAttribute? inheritedTableAttribute = entityType.ClrType.GetCustomAttribute<TableAttribute>(inherit: true);
        if (inheritedTableAttribute is not null && !string.IsNullOrWhiteSpace(inheritedTableAttribute.Name))
        {
            return inheritedTableAttribute.Name;
        }

        string shortName = entityType.ShortName();
        return !string.IsNullOrWhiteSpace(shortName) ? shortName : entityType.ClrType.Name;
    }

    /// <summary>
    /// Returns all distinct properties declared across the entity type's inheritance hierarchy.
    /// </summary>
    /// <param name="entityType">The EF Core entity type.</param>
    /// <returns>The properties across the root type and any derived types.</returns>
    internal static List<IProperty> GetHierarchyProperties(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        IEntityType rootType = entityType.GetRootType();
        List<IProperty> properties = [.. rootType.GetProperties()];
        HashSet<IProperty> seen = [.. properties];
        foreach (IEntityType derivedType in rootType.GetDerivedTypes())
        {
            foreach (IProperty prop in derivedType.GetDeclaredProperties())
            {
                if (seen.Add(prop))
                {
                    properties.Add(prop);
                }
            }
        }

        return properties;
    }

    /// <summary>
    /// Resolves the physical JET column name for <paramref name="property"/>.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns>The resolved column name.</returns>
    internal static string GetColumnName(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (property.FindAnnotation(RelationalColumnNameAnnotation)?.Value is string annotatedName
            && !string.IsNullOrWhiteSpace(annotatedName))
        {
            return annotatedName;
        }

        ColumnAttribute? columnAttribute = property.PropertyInfo?.GetCustomAttribute<ColumnAttribute>(inherit: true);
        if (columnAttribute is not null && !string.IsNullOrWhiteSpace(columnAttribute.Name))
        {
            return columnAttribute.Name;
        }

        return property.Name;
    }

    /// <summary>
    /// Attempts to read a configured default value annotation or <see cref="DefaultValueAttribute"/> for <paramref name="property"/>.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <param name="defaultValue">When this method returns, contains the configured default value if present.</param>
    /// <returns><see langword="true"/> if a default value was configured; otherwise, <see langword="false"/>.</returns>
    internal static bool TryGetDefaultValue(IReadOnlyProperty property, out object? defaultValue)
    {
        ArgumentNullException.ThrowIfNull(property);

        IAnnotation? annotation = property.FindAnnotation(RelationalDefaultValueAnnotation)
            ?? property.FindAnnotation(JetDefaultValueAnnotation);
        if (annotation is not null)
        {
            defaultValue = annotation.Value;
            return true;
        }

        DefaultValueAttribute? defaultAttr = property.PropertyInfo?.GetCustomAttribute<DefaultValueAttribute>(inherit: true);
        if (defaultAttr is not null)
        {
            defaultValue = defaultAttr.Value;
            return true;
        }

        defaultValue = null;
        return false;
    }

    /// <summary>
    /// Determines whether <paramref name="property"/> is an integer key column generated by JET AutoNumber on insert.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns><see langword="true"/> if the property is an AutoNumber column; otherwise, <see langword="false"/>.</returns>
    internal static bool IsAutoIncrementProperty(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (property.ValueGenerated != ValueGenerated.OnAdd || !property.IsPrimaryKey())
        {
            return false;
        }

        if (property.GetValueConverter() is not null || property.FindTypeMapping()?.Converter is not null)
        {
            return false;
        }

        IReadOnlyKey? primaryKey = property.DeclaringType.ContainingEntityType.FindPrimaryKey();
        if (primaryKey is null || primaryKey.Properties.Count != 1)
        {
            return false;
        }

        Type clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
        return clrType == typeof(int) || clrType == typeof(long) || clrType == typeof(short);
    }

    /// <summary>
    /// Resolves the non-nullable CLR type used by <see cref="JetDatabaseWriter"/> to store <paramref name="property"/>.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns>The normalized storage CLR type.</returns>
    internal static Type GetStorageClrType(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (IsAutoIncrementProperty(property))
        {
            Type autoKeyType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
            if (autoKeyType == typeof(long))
            {
                return typeof(int);
            }
        }

        if (property.IsForeignKey())
        {
            foreach (IReadOnlyForeignKey fk in property.GetContainingForeignKeys())
            {
                for (int i = 0; i < fk.Properties.Count; i++)
                {
                    if (ReferenceEquals(fk.Properties[i], property)
                        && i < fk.PrincipalKey.Properties.Count
                        && IsAutoIncrementProperty(fk.PrincipalKey.Properties[i]))
                    {
                        return GetStorageClrType(fk.PrincipalKey.Properties[i]);
                    }
                }
            }
        }

        ValueConverter? converter = property.GetValueConverter() ?? property.FindTypeMapping()?.Converter;
        Type baseType = converter is not null ? converter.ProviderClrType : property.ClrType;
        return NormalizeStorageClrType(baseType);
    }

    /// <summary>
    /// Normalizes a model or provider CLR type to a non-nullable CLR type supported by <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <param name="clrType">The input CLR type.</param>
    /// <returns>The supported storage CLR type.</returns>
    internal static Type NormalizeStorageClrType(Type clrType)
    {
        ArgumentNullException.ThrowIfNull(clrType);

        Type unwrapped = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (unwrapped.IsEnum)
        {
            unwrapped = Enum.GetUnderlyingType(unwrapped);
        }

        if (unwrapped == typeof(sbyte))
        {
            return typeof(short);
        }

        if (unwrapped == typeof(ushort))
        {
            return typeof(int);
        }

        if (unwrapped == typeof(uint))
        {
            return typeof(double);
        }

        if (unwrapped == typeof(ulong) || unwrapped == typeof(TimeSpan))
        {
            return typeof(decimal);
        }

        if (unwrapped == typeof(char))
        {
            return typeof(string);
        }

        if (unwrapped == typeof(DateOnly) || unwrapped == typeof(TimeOnly) || unwrapped == typeof(DateTimeOffset))
        {
            return typeof(DateTime);
        }

        return unwrapped;
    }

    /// <summary>
    /// Converts a model CLR value for <paramref name="property"/> to its <see cref="JetDatabaseWriter"/> storage representation.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <param name="modelValue">The model value.</param>
    /// <returns>The provider value, or <see langword="null"/> for database null.</returns>
    internal static object? ConvertToProvider(IReadOnlyProperty property, object? modelValue)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (modelValue is null or DBNull)
        {
            return null;
        }

        object? value = modelValue;
        ValueConverter? converter = property.GetValueConverter() ?? property.FindTypeMapping()?.Converter;
        if (converter is not null)
        {
            value = converter.ConvertToProvider(value);
            if (value is null or DBNull)
            {
                return null;
            }
        }

        Type storageType = GetStorageClrType(property);
        return CoerceToType(value, storageType);
    }

    /// <summary>
    /// Converts a stored value decoded by <see cref="JetDatabaseWriter"/> back to <paramref name="property"/>'s model CLR type.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <param name="storeValue">The decoded store value.</param>
    /// <returns>The converted model value, or <see langword="null"/>.</returns>
    internal static object? ConvertFromProvider(IReadOnlyProperty property, object? storeValue)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (storeValue is null or DBNull)
        {
            return null;
        }

        ValueConverter? converter = property.GetValueConverter() ?? property.FindTypeMapping()?.Converter;
        if (converter is not null)
        {
            object? providerValue = CoerceToType(storeValue, converter.ProviderClrType);
            return converter.ConvertFromProvider(providerValue);
        }

        return CoerceToType(storeValue, property.ClrType);
    }

    /// <summary>
    /// Coerces <paramref name="value"/> to <paramref name="targetType"/>.
    /// </summary>
    /// <param name="value">The input value.</param>
    /// <param name="targetType">The target CLR type.</param>
    /// <returns>The coerced value.</returns>
    internal static object? CoerceToType(object? value, Type targetType)
    {
        if (value is null or DBNull)
        {
            return null;
        }

        Type unwrappedTarget = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (unwrappedTarget.IsInstanceOfType(value))
        {
            return value;
        }

        if (unwrappedTarget.IsEnum)
        {
            if (value is string enumText)
            {
                return Enum.Parse(unwrappedTarget, enumText, ignoreCase: true);
            }

            object underlyingValue = Convert.ChangeType(value, Enum.GetUnderlyingType(unwrappedTarget), CultureInfo.InvariantCulture);
            return Enum.ToObject(unwrappedTarget, underlyingValue);
        }

        if (value is Enum)
        {
            Type enumUnderlying = Enum.GetUnderlyingType(value.GetType());
            object rawEnum = Convert.ChangeType(value, enumUnderlying, CultureInfo.InvariantCulture);
            return Convert.ChangeType(rawEnum, unwrappedTarget, CultureInfo.InvariantCulture);
        }

        if (unwrappedTarget == typeof(Hyperlink))
        {
            return value switch
            {
                Hyperlink h => h,
                string s => Hyperlink.Parse(s),
                _ => Hyperlink.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)),
            };
        }

        if (unwrappedTarget == typeof(Guid))
        {
            return value switch
            {
                Guid g => g,
                string s => Guid.Parse(s),
                byte[] b => new Guid(b),
                _ => new Guid(Convert.ToString(value, CultureInfo.InvariantCulture)!),
            };
        }

        if (unwrappedTarget == typeof(DateOnly))
        {
            return value switch
            {
                DateOnly d => d,
                DateTime dt => DateOnly.FromDateTime(dt),
                DateTimeOffset dto => DateOnly.FromDateTime(dto.DateTime),
                string s => DateOnly.Parse(s, CultureInfo.InvariantCulture),
                _ => DateOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture)),
            };
        }

        if (unwrappedTarget == typeof(TimeOnly))
        {
            return value switch
            {
                TimeOnly t => t,
                DateTime dt => TimeOnly.FromDateTime(dt),
                DateTimeOffset dto => TimeOnly.FromDateTime(dto.DateTime),
                TimeSpan ts => TimeOnly.FromTimeSpan(ts),
                string s => TimeOnly.Parse(s, CultureInfo.InvariantCulture),
                _ => TimeOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture)),
            };
        }

        if (unwrappedTarget == typeof(DateTimeOffset))
        {
            return value switch
            {
                DateTimeOffset dto => dto,
                DateTime dt => new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt),
                string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture),
                _ => new DateTimeOffset(Convert.ToDateTime(value, CultureInfo.InvariantCulture), TimeSpan.Zero),
            };
        }

        if (unwrappedTarget == typeof(TimeSpan))
        {
            return value switch
            {
                TimeSpan ts => ts,
                decimal decTicks => TimeSpan.FromTicks(decimal.ToInt64(decimal.Truncate(decTicks))),
                long ticks => TimeSpan.FromTicks(ticks),
                int intTicks => TimeSpan.FromTicks(intTicks),
                double tickDouble => TimeSpan.FromTicks((long)Math.Round(tickDouble)),
                string s => TimeSpan.Parse(s, CultureInfo.InvariantCulture),
                _ => TimeSpan.FromTicks(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
            };
        }

        if (unwrappedTarget == typeof(DateTime))
        {
            return value switch
            {
                DateTime dt => dt,
                DateOnly d => d.ToDateTime(TimeOnly.MinValue),
                TimeOnly t => new DateTime(1899, 12, 30).Add(t.ToTimeSpan()),
                DateTimeOffset dto => dto.UtcDateTime,
                string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
                _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture),
            };
        }

        if (unwrappedTarget == typeof(decimal) && value is TimeSpan spanToDecimal)
        {
            return (decimal)spanToDecimal.Ticks;
        }

        if (unwrappedTarget == typeof(double) && value is TimeSpan spanToDouble)
        {
            return (double)spanToDouble.Ticks;
        }

        if (unwrappedTarget == typeof(long) && value is TimeSpan span)
        {
            return span.Ticks;
        }

        if (unwrappedTarget == typeof(bool))
        {
            return value switch
            {
                bool b => b,
                string s => bool.Parse(s),
                _ => Convert.ToBoolean(value, CultureInfo.InvariantCulture),
            };
        }

        if (unwrappedTarget == typeof(char))
        {
            string text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
            return text.Length > 0 ? text[0] : '\0';
        }

        return Convert.ChangeType(value, unwrappedTarget, CultureInfo.InvariantCulture);
    }
}
