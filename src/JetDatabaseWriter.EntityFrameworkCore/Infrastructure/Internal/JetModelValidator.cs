namespace JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;

using System;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

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

        foreach (IEntityType entityType in model.GetEntityTypes())
        {
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
}
