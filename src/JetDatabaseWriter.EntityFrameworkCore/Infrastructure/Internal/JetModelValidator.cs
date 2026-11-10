namespace JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

/// <summary>
/// Validates EF Core models configured for the <see cref="JetDatabaseWriter"/> provider.
/// </summary>
/// <param name="dependencies">Parameter object containing dependencies for this service.</param>
public sealed class JetModelValidator(ModelValidatorDependencies dependencies) : ModelValidator(dependencies)
{
    /// <inheritdoc/>
    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        ArgumentNullException.ThrowIfNull(model);
        base.Validate(model, logger);
        ValidateSharedColumns(model);

        foreach (IEntityType entityType in model.GetEntityTypes())
        {
            foreach (IProperty property in entityType.GetFlattenedProperties())
            {
                JetEntityTypeMetadata.ValidateDefaultValue(property);
            }

            foreach (IComplexProperty complexProperty in entityType.GetFlattenedComplexProperties())
            {
                if (complexProperty.IsCollection)
                {
                    throw new InvalidOperationException(
                        $"Complex collection property '{entityType.DisplayName()}.{complexProperty.Name}' is not supported by JetDatabaseWriter.EntityFrameworkCore. " +
                        "Configure an owned entity collection via OwnsMany instead.");
                }
            }

            if (entityType.IsOwned())
            {
                continue;
            }

            if (entityType.FindAnnotation("Relational:MappingStrategy")?.Value is string strategy
                && !string.Equals(strategy, "TPH", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Entity type '{entityType.DisplayName()}' is configured with mapping strategy '{strategy}'. " +
                    "JetDatabaseWriter.EntityFrameworkCore only supports table-per-hierarchy (TPH) inheritance mapping.");
            }

            IEntityType rootType = entityType.GetRootType();
            if (!ReferenceEquals(entityType, rootType)
                && entityType.FindAnnotation("Relational:TableName")?.Value is string derivedTable
                && !string.IsNullOrWhiteSpace(derivedTable))
            {
                string rootTable = JetEntityTypeMetadata.GetTableName(rootType);
                if (!string.Equals(derivedTable, rootTable, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"Derived entity type '{entityType.DisplayName()}' is mapped to table '{derivedTable}', " +
                        $"which differs from its root entity type's table '{rootTable}'. " +
                        "JetDatabaseWriter.EntityFrameworkCore only supports single-table (TPH) inheritance.");
                }
            }
        }
    }

    private static void ValidateSharedColumns(IModel model)
    {
        Dictionary<string, Dictionary<string, List<IProperty>>> tables = [with(StringComparer.OrdinalIgnoreCase)];
        Dictionary<string, IEntityType> tableOwners = [with(StringComparer.OrdinalIgnoreCase)];
        foreach (IEntityType entityType in model.GetEntityTypes())
        {
            string tableName = JetEntityTypeMetadata.GetTableName(entityType);
            IEntityType tableOwner = JetEntityTypeMetadata.GetTableOwnerEntityType(entityType);
            if (!tables.TryGetValue(tableName, out Dictionary<string, List<IProperty>>? columns))
            {
                columns = [with(StringComparer.OrdinalIgnoreCase)];
                tables.Add(tableName, columns);
                tableOwners.Add(tableName, tableOwner);
            }
            else if (!ReferenceEquals(tableOwners[tableName], tableOwner))
            {
                throw new InvalidOperationException(
                    $"Entity types '{tableOwners[tableName].DisplayName()}' and '{tableOwner.DisplayName()}' cannot share table '{tableName}'. " +
                    "Shared tables require TPH inheritance or a table-split ownership relationship.");
            }

            foreach (IProperty property in entityType.GetFlattenedDeclaredProperties())
            {
                string columnName = JetEntityTypeMetadata.GetColumnName(property);
                if (!columns.TryGetValue(columnName, out List<IProperty>? mappedProperties))
                {
                    mappedProperties = [];
                    columns.Add(columnName, mappedProperties);
                }

                foreach (IProperty other in mappedProperties)
                {
                    bool sharedOwnershipKey = ReferenceEquals(GetOwnershipPrincipalProperty(property), GetOwnershipPrincipalProperty(other));
                    bool sharedConcurrencyRow = HaveSharedConcurrencyRow(property, other);
                    if ((!sharedOwnershipKey && !sharedConcurrencyRow && !HaveExclusiveHierarchyRows(property, other))
                        || !HaveCompatibleSharedColumn(property, other, sharedOwnershipKey, sharedConcurrencyRow))
                    {
                        throw new InvalidOperationException(
                            $"Properties '{other.DeclaringType.DisplayName()}.{other.Name}' and '{property.DeclaringType.DisplayName()}.{property.Name}' " +
                            $"cannot share column '{tableName}.{columnName}'. Shared columns require compatible types, converters and facets, " +
                            "and must represent an ownership key, a shared concurrency token or mutually exclusive TPH rows.");
                    }
                }

                mappedProperties.Add(property);
            }
        }
    }

    private static IProperty GetOwnershipPrincipalProperty(IProperty property)
    {
        while (property.DeclaringType is IEntityType entityType
            && JetEntityTypeMetadata.IsTableSplitOwned(entityType)
            && entityType.FindOwnership() is { } ownership)
        {
            int position = -1;
            for (int i = 0; i < ownership.Properties.Count; i++)
            {
                if (ReferenceEquals(ownership.Properties[i], property))
                {
                    position = i;
                    break;
                }
            }

            if (position < 0)
            {
                break;
            }

            property = ownership.PrincipalKey.Properties[position];
        }

        return property;
    }

    private static bool HaveExclusiveHierarchyRows(IProperty left, IProperty right)
    {
        static IEntityType GetRowEntityType(IProperty property)
        {
            IEntityType entityType = property.DeclaringType.ContainingEntityType;
            while (JetEntityTypeMetadata.IsTableSplitOwned(entityType) && entityType.FindOwnership() is { } ownership)
            {
                entityType = ownership.PrincipalEntityType;
            }

            return entityType;
        }

        IEntityType leftType = GetRowEntityType(left);
        IEntityType rightType = GetRowEntityType(right);
        return ReferenceEquals(leftType.GetRootType(), rightType.GetRootType())
            && !leftType.IsAssignableFrom(rightType)
            && !rightType.IsAssignableFrom(leftType);
    }

    private static bool HaveSharedConcurrencyRow(IProperty left, IProperty right)
    {
        IEntityType leftType = left.DeclaringType.ContainingEntityType;
        IEntityType rightType = right.DeclaringType.ContainingEntityType;
        return left.IsConcurrencyToken && right.IsConcurrencyToken
            && !ReferenceEquals(leftType, rightType)
            && (JetEntityTypeMetadata.IsTableSplitOwned(leftType) || JetEntityTypeMetadata.IsTableSplitOwned(rightType))
            && ReferenceEquals(JetEntityTypeMetadata.GetTableOwnerEntityType(leftType), JetEntityTypeMetadata.GetTableOwnerEntityType(rightType));
    }

    private static bool HaveCompatibleSharedColumn(IProperty left, IProperty right, bool sharedOwnershipKey, bool sharedConcurrencyRow)
    {
        Type leftType = Nullable.GetUnderlyingType(left.ClrType) ?? left.ClrType;
        Type rightType = Nullable.GetUnderlyingType(right.ClrType) ?? right.ClrType;
        if (leftType != rightType)
        {
            return false;
        }

        ValueConverter? leftConverter = left.GetValueConverter() ?? left.FindTypeMapping()?.Converter;
        ValueConverter? rightConverter = right.GetValueConverter() ?? right.FindTypeMapping()?.Converter;
        if (!ReferenceEquals(leftConverter, rightConverter)
            && (leftConverter is null || rightConverter is null
                || leftConverter.ModelClrType != rightConverter.ModelClrType
                || leftConverter.ProviderClrType != rightConverter.ProviderClrType
                || !ExpressionEqualityComparer.Instance.Equals(leftConverter.ConvertToProviderExpression, rightConverter.ConvertToProviderExpression)
                || !ExpressionEqualityComparer.Instance.Equals(leftConverter.ConvertFromProviderExpression, rightConverter.ConvertFromProviderExpression)))
        {
            return false;
        }

        ColumnDefinition leftColumn = JetDatabaseCreator.CreateColumnDefinition(left, DatabaseFormat.AceAccdb).WithName("Shared");
        ColumnDefinition rightColumn = JetDatabaseCreator.CreateColumnDefinition(right, DatabaseFormat.AceAccdb).WithName("Shared");
        if (sharedOwnershipKey)
        {
            leftColumn = leftColumn with { IsAutoIncrement = false, IsNullable = false };
            rightColumn = rightColumn with { IsAutoIncrement = false, IsNullable = false };
        }
        else if (sharedConcurrencyRow)
        {
            leftColumn = leftColumn with { IsNullable = false };
            rightColumn = rightColumn with { IsNullable = false };
        }

        return leftColumn == rightColumn;
    }
}
