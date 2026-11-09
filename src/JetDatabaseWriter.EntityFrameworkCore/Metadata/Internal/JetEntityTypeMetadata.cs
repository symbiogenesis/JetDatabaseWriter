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
using Microsoft.EntityFrameworkCore.Update;

/// <summary>
/// Resolves JET/ACE table names, column names, storage types, and value conversions for EF Core model metadata.
/// </summary>
internal static class JetEntityTypeMetadata
{
    private const string RelationalTableNameAnnotation = "Relational:TableName";
    private const string RelationalColumnNameAnnotation = "Relational:ColumnName";
    private const string RelationalDefaultValueAnnotation = "Relational:DefaultValue";
    private const string RelationalDefaultValueSqlAnnotation = "Relational:DefaultValueSql";
    private const string RelationalComputedColumnSqlAnnotation = "Relational:ComputedColumnSql";
    private const string RelationalCascadeUpdatesAnnotation = "Relational:CascadeUpdates";
    private const string JetDefaultValueAnnotation = "Jet:DefaultValue";
    private const string JetDefaultValueExpressionAnnotation = "Jet:DefaultValueExpression";
    private const string JetCalculatedExpressionAnnotation = "Jet:CalculatedExpression";
    private const string JetValidationRuleAnnotation = "Jet:ValidationRule";
    private const string JetValidationTextAnnotation = "Jet:ValidationText";
    private const string JetDescriptionAnnotation = "Jet:Description";
    private const string JetAllowZeroLengthAnnotation = "Jet:AllowZeroLength";
    private const string JetIsCurrencyAnnotation = "Jet:IsCurrency";
    private const string JetIsDateTimeExtendedAnnotation = "Jet:IsDateTimeExtended";
    private const string JetCascadeUpdatesAnnotation = "Jet:CascadeUpdates";

    /// <summary>
    /// Determines whether <paramref name="entityType"/> is a single-reference owned entity type (<c>OwnsOne</c>)
    /// mapped via table splitting into its principal owner's table.
    /// </summary>
    /// <param name="entityType">The EF Core entity type.</param>
    /// <returns><see langword="true"/> if table-split into the owner's table; otherwise, <see langword="false"/>.</returns>
    internal static bool IsTableSplitOwned(IReadOnlyEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        if (!entityType.IsOwned() || entityType.FindOwnership() is not { IsUnique: true } ownership)
        {
            return false;
        }

        if (TryGetExplicitTableName(entityType, out string? explicitTable))
        {
            string ownerTable = GetTableName(ownership.PrincipalEntityType);
            return string.Equals(explicitTable, ownerTable, StringComparison.OrdinalIgnoreCase);
        }

        return true;
    }

    /// <summary>
    /// Resolves the root table-owner entity type for <paramref name="entityType"/>, walking up table-split ownership hops.
    /// </summary>
    /// <param name="entityType">The EF Core entity type.</param>
    /// <returns>The non-table-split root entity type that owns the physical table.</returns>
    internal static IEntityType GetTableOwnerEntityType(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        IEntityType current = entityType;
        while (IsTableSplitOwned(current) && current.FindOwnership() is { } ownership)
        {
            current = ownership.PrincipalEntityType;
        }

        return current.GetRootType();
    }

    /// <summary>
    /// Returns all table-split <c>OwnsOne</c> entity types folded into <paramref name="ownerRootType"/>'s table,
    /// ordered from outer owners to nested owned types.
    /// </summary>
    /// <param name="ownerRootType">The root entity type owning the table.</param>
    /// <returns>The table-split owned entity types in ownership order.</returns>
    internal static List<IEntityType> GetTableSplitOwnedEntityTypes(IEntityType ownerRootType)
    {
        ArgumentNullException.ThrowIfNull(ownerRootType);

        IEntityType root = ownerRootType.GetRootType();
        List<IEntityType> result = [];
        HashSet<IEntityType> visited = [];

        void CollectFrom(IEntityType currentOwner)
        {
            foreach (INavigation nav in currentOwner.GetNavigations())
            {
                IEntityType target = nav.TargetEntityType;
                if (nav.ForeignKey.IsOwnership
                    && !nav.IsOnDependent
                    && IsTableSplitOwned(target)
                    && visited.Add(target))
                {
                    result.Add(target);
                    CollectFrom(target);
                }
            }

            foreach (IEntityType derived in currentOwner.GetDerivedTypes())
            {
                foreach (INavigation nav in derived.GetDeclaredNavigations())
                {
                    IEntityType target = nav.TargetEntityType;
                    if (nav.ForeignKey.IsOwnership
                        && !nav.IsOnDependent
                        && IsTableSplitOwned(target)
                        && visited.Add(target))
                    {
                        result.Add(target);
                        CollectFrom(target);
                    }
                }
            }
        }

        CollectFrom(root);
        return result;
    }

    /// <summary>
    /// Resolves the physical JET table name for <paramref name="entityType"/>.
    /// </summary>
    /// <param name="entityType">The EF Core entity type.</param>
    /// <returns>The resolved table name.</returns>
    internal static string GetTableName(IReadOnlyEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        if (TryGetExplicitTableName(entityType, out string? explicitName))
        {
            return explicitName!;
        }

        if (entityType.IsOwned() && entityType.FindOwnership() is { IsUnique: true } ownership)
        {
            return GetTableName(ownership.PrincipalEntityType);
        }

        IReadOnlyEntityType rootType = entityType.GetRootType();
        if (!ReferenceEquals(rootType, entityType))
        {
            return GetTableName(rootType);
        }

        if (!entityType.IsOwned() && !entityType.HasSharedClrType)
        {
            TableAttribute? inheritedTableAttribute = entityType.ClrType.GetCustomAttribute<TableAttribute>(inherit: true);
            if (inheritedTableAttribute is not null && !string.IsNullOrWhiteSpace(inheritedTableAttribute.Name))
            {
                return inheritedTableAttribute.Name;
            }
        }

        string shortName = entityType.ShortName();
        return !string.IsNullOrWhiteSpace(shortName) ? shortName : entityType.ClrType.Name;
    }

    /// <summary>
    /// Returns all distinct flattened scalar properties (including complex-property members) declared across the entity type's inheritance hierarchy.
    /// </summary>
    /// <param name="entityType">The EF Core entity type.</param>
    /// <returns>The flattened scalar properties across the root type and any derived types.</returns>
    internal static List<IProperty> GetHierarchyProperties(IEntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        IEntityType rootType = entityType.GetRootType();
        List<IProperty> properties = [.. rootType.GetFlattenedProperties()];
        HashSet<IProperty> seen = [.. properties];
        foreach (IEntityType derivedType in rootType.GetDerivedTypes())
        {
            foreach (IProperty prop in derivedType.GetFlattenedDeclaredProperties())
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

        if (property.DeclaringType is IReadOnlyEntityType declaringEntity
            && IsTableSplitOwned(declaringEntity)
            && declaringEntity.FindOwnership() is { } ownership)
        {
            for (int i = 0; i < ownership.Properties.Count; i++)
            {
                if (ReferenceEquals(ownership.Properties[i], property) && i < ownership.PrincipalKey.Properties.Count)
                {
                    return GetColumnName(ownership.PrincipalKey.Properties[i]);
                }
            }
        }

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

        List<string>? prefixSegments = null;
        IReadOnlyTypeBase currentType = property.DeclaringType;
        while (currentType is IReadOnlyComplexType complexType)
        {
            prefixSegments ??= [];
            prefixSegments.Add(complexType.ComplexProperty.Name);
            currentType = complexType.ComplexProperty.DeclaringType;
        }

        if (currentType is IReadOnlyEntityType currentEntity)
        {
            IReadOnlyEntityType walkEntity = currentEntity;
            while (IsTableSplitOwned(walkEntity) && walkEntity.FindOwnership() is { } walkOwnership)
            {
                prefixSegments ??= [];
                string navName = walkOwnership.PrincipalToDependent?.Name ?? walkEntity.ShortName();
                prefixSegments.Add(navName);
                walkEntity = walkOwnership.PrincipalEntityType;
            }
        }

        if (prefixSegments is { Count: > 0 })
        {
            prefixSegments.Reverse();
            return $"{string.Join("_", prefixSegments)}_{property.Name}";
        }

        return property.Name;
    }

    /// <summary>
    /// Determines whether <paramref name="property"/> is declared inside a nullable <see cref="IReadOnlyComplexProperty"/> chain.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns><see langword="true"/> if any containing complex property is nullable; otherwise, <see langword="false"/>.</returns>
    internal static bool IsInNullableComplexType(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        IReadOnlyTypeBase current = property.DeclaringType;
        while (current is IReadOnlyComplexType complexType)
        {
            if (complexType.ComplexProperty.IsNullable)
            {
                return true;
            }

            current = complexType.ComplexProperty.DeclaringType;
        }

        return false;
    }

    /// <summary>
    /// Determines whether <paramref name="property"/> belongs to an optional table-split <c>OwnsOne</c> entity type
    /// or a nullable complex property, requiring its physical column in the shared table to allow <c>NULL</c>.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns><see langword="true"/> if the physical column must be nullable; otherwise, <see langword="false"/>.</returns>
    internal static bool IsInOptionalComplexOrTableSplitOwned(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (IsInNullableComplexType(property))
        {
            return true;
        }

        IReadOnlyEntityType containingEntity = property.DeclaringType.ContainingEntityType;
        while (IsTableSplitOwned(containingEntity) && containingEntity.FindOwnership() is { } ownership)
        {
            if (!ownership.IsRequiredDependent)
            {
                return true;
            }

            containingEntity = ownership.PrincipalEntityType;
        }

        return false;
    }

    /// <summary>
    /// Reads the current value of <paramref name="property"/> from <paramref name="entry"/>, returning <see langword="null"/>
    /// when any enclosing <see cref="IComplexProperty"/> instance on <paramref name="entry"/> is <see langword="null"/>.
    /// </summary>
    /// <param name="entry">The update entry.</param>
    /// <param name="property">The scalar property.</param>
    /// <returns>The current property value, or <see langword="null"/>.</returns>
    internal static object? GetPropertyCurrentValue(IUpdateEntry entry, IProperty property)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(property);

        ITypeBase current = property.DeclaringType;
        while (current is IComplexType complexType)
        {
            IComplexProperty complexProp = complexType.ComplexProperty;
            if (entry.GetCurrentValue(complexProp) is null)
            {
                return null;
            }

            current = complexProp.DeclaringType;
        }

        return entry.GetCurrentValue(property);
    }

    /// <summary>
    /// Gets a value indicating whether <paramref name="foreignKey"/> is configured to cascade primary-key updates.
    /// </summary>
    /// <param name="foreignKey">The EF Core foreign key.</param>
    /// <returns><see langword="true"/> if cascade updates are enabled; otherwise, <see langword="false"/>.</returns>
    internal static bool GetCascadeUpdates(IReadOnlyForeignKey foreignKey)
    {
        ArgumentNullException.ThrowIfNull(foreignKey);

        return foreignKey.FindAnnotation(JetCascadeUpdatesAnnotation)?.Value is true
            || foreignKey.FindAnnotation(RelationalCascadeUpdatesAnnotation)?.Value is true;
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
    /// Gets the configured Access default-value expression for <paramref name="property"/>, if any.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns>The configured default-value expression, or <see langword="null"/>.</returns>
    internal static string? GetDefaultValueExpression(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetDefaultValueExpressionAnnotation)?.Value as string
            ?? property.FindAnnotation(RelationalDefaultValueSqlAnnotation)?.Value as string;
    }

    /// <summary>
    /// Gets the configured Access calculated-column expression for <paramref name="property"/>, if any.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns>The configured calculated-column expression, or <see langword="null"/>.</returns>
    internal static string? GetCalculatedExpression(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetCalculatedExpressionAnnotation)?.Value as string
            ?? property.FindAnnotation(RelationalComputedColumnSqlAnnotation)?.Value as string;
    }

    /// <summary>
    /// Gets the configured Access validation-rule expression for <paramref name="property"/>, if any.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns>The configured validation-rule expression, or <see langword="null"/>.</returns>
    internal static string? GetValidationRuleExpression(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetValidationRuleAnnotation)?.Value as string;
    }

    /// <summary>
    /// Gets the configured Access validation message for <paramref name="property"/>, if any.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns>The configured validation message, or <see langword="null"/>.</returns>
    internal static string? GetValidationText(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetValidationTextAnnotation)?.Value as string;
    }

    /// <summary>
    /// Gets the configured Access column description for <paramref name="property"/>, if any.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns>The configured column description, or <see langword="null"/>.</returns>
    internal static string? GetDescription(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetDescriptionAnnotation)?.Value as string;
    }

    /// <summary>
    /// Gets the configured <c>AllowZeroLength</c> flag for <paramref name="property"/>, defaulting to <see langword="true"/>.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns><see langword="true"/> if empty strings are allowed; otherwise, <see langword="false"/>.</returns>
    internal static bool GetAllowZeroLength(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetAllowZeroLengthAnnotation)?.Value is not bool flag || flag;
    }

    /// <summary>
    /// Gets a value indicating whether <paramref name="property"/> is explicitly configured as an Access <c>Currency</c> column.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns><see langword="true"/> if configured as <c>Currency</c>; otherwise, <see langword="false"/>.</returns>
    internal static bool GetIsCurrency(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetIsCurrencyAnnotation)?.Value is true;
    }

    /// <summary>
    /// Gets a value indicating whether <paramref name="property"/> is explicitly configured as an Access <c>Date/Time Extended</c> column.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns><see langword="true"/> if configured as <c>Date/Time Extended</c>; otherwise, <see langword="false"/>.</returns>
    internal static bool GetIsDateTimeExtended(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        return property.FindAnnotation(JetIsDateTimeExtendedAnnotation)?.Value is true;
    }

    /// <summary>
    /// Determines whether <paramref name="property"/> is an integer key column generated by JET AutoNumber on insert.
    /// </summary>
    /// <param name="property">The EF Core property.</param>
    /// <returns><see langword="true"/> if the property is an AutoNumber column; otherwise, <see langword="false"/>.</returns>
    internal static bool IsAutoIncrementProperty(IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);

        if (property.ValueGenerated != ValueGenerated.OnAdd || !property.IsPrimaryKey() || property.IsForeignKey())
        {
            return false;
        }

        if (property.GetValueConverter() is not null || property.FindTypeMapping()?.Converter is not null)
        {
            return false;
        }

        IReadOnlyEntityType containingEntity = property.DeclaringType.ContainingEntityType;
        if (IsTableSplitOwned(containingEntity))
        {
            return false;
        }

        IReadOnlyKey? primaryKey = containingEntity.FindPrimaryKey();
        if (primaryKey is null)
        {
            return false;
        }

        if (primaryKey.Properties.Count != 1)
        {
            if (!containingEntity.IsOwned())
            {
                return false;
            }

            for (int i = 0; i < primaryKey.Properties.Count; i++)
            {
                IReadOnlyProperty pkProp = primaryKey.Properties[i];
                if (!pkProp.IsForeignKey() && pkProp.ValueGenerated == ValueGenerated.OnAdd && IsIntegerAutoKeyClrType(pkProp.ClrType))
                {
                    return ReferenceEquals(pkProp, property);
                }
            }

            return false;
        }

        return IsIntegerAutoKeyClrType(property.ClrType);
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

    private static bool TryGetExplicitTableName(IReadOnlyEntityType entityType, out string? tableName)
    {
        if (entityType.FindAnnotation(RelationalTableNameAnnotation)?.Value is string annotatedName
            && !string.IsNullOrWhiteSpace(annotatedName))
        {
            tableName = annotatedName;
            return true;
        }

        if (!entityType.IsOwned() && !entityType.HasSharedClrType)
        {
            TableAttribute? directTableAttribute = entityType.ClrType.GetCustomAttribute<TableAttribute>(inherit: false);
            if (directTableAttribute is not null && !string.IsNullOrWhiteSpace(directTableAttribute.Name))
            {
                tableName = directTableAttribute.Name;
                return true;
            }
        }

        tableName = null;
        return false;
    }

    private static bool IsIntegerAutoKeyClrType(Type clrType)
    {
        Type unwrapped = Nullable.GetUnderlyingType(clrType) ?? clrType;
        return unwrapped == typeof(int) || unwrapped == typeof(long) || unwrapped == typeof(short);
    }
}
