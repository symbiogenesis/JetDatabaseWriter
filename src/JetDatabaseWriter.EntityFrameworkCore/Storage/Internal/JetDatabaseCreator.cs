namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.ValueDecoding;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

/// <summary>
/// Creates, migrates, and deletes Microsoft Access databases and their schema from an EF Core <see cref="IModel"/>.
/// </summary>
public sealed class JetDatabaseCreator : IDatabaseCreator
{
    private readonly IJetClientConnection connection;
    private readonly IModel model;
    private readonly ICurrentDbContext currentContext;
    private readonly CoreOptionsExtension? options;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetDatabaseCreator"/> class.
    /// </summary>
    /// <param name="connection">The client connection for the current context scope.</param>
    /// <param name="designTimeModel">The design-time EF Core model describing the schema.</param>
    /// <param name="options">Options containing the configured asynchronous seed callback.</param>
    /// <param name="currentContext">The current EF Core context.</param>
    public JetDatabaseCreator(
        IJetClientConnection connection,
        IDesignTimeModel designTimeModel,
        IDbContextOptions options,
        ICurrentDbContext currentContext)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(designTimeModel);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(currentContext);

        this.connection = connection;
        this.model = designTimeModel.Model;
        this.options = options.FindExtension<CoreOptionsExtension>();
        this.currentContext = currentContext;
    }

    /// <inheritdoc/>
    public bool EnsureDeleted()
        => throw new NotSupportedException(
            "Synchronous EnsureDeleted is not supported by JetDatabaseWriter.EntityFrameworkCore. Use EnsureDeletedAsync instead.");

    /// <inheritdoc/>
    public async Task<bool> EnsureDeletedAsync(CancellationToken cancellationToken = default)
        => await this.connection.DeleteDatabaseAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public bool EnsureCreated()
        => throw new NotSupportedException(
            "Synchronous EnsureCreated is not supported by JetDatabaseWriter.EntityFrameworkCore. Use EnsureCreatedAsync instead.");

    /// <inheritdoc/>
    public async Task<bool> EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        bool operationsPerformed = await this.EnsureSchemaCreatedAsync(cancellationToken).ConfigureAwait(false);
        await this.RunAsyncSeederAsync(operationsPerformed, cancellationToken).ConfigureAwait(false);
        return operationsPerformed;
    }

    /// <summary>
    /// Synchronously migrates the database schema to match the current EF Core model.
    /// Synchronous migration is not supported; use <see cref="MigrateAsync"/> instead.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown because synchronous I/O is not supported.</exception>
    public void Migrate()
        => throw new NotSupportedException(
            "Synchronous Migrate is not supported by JetDatabaseWriter.EntityFrameworkCore. Use MigrateAsync instead.");

    /// <summary>
    /// Asynchronously creates the database if it does not exist, or incrementally migrates an existing database schema
    /// to match the current EF Core model by creating missing tables, adding missing columns, creating missing indexes
    /// and foreign-key relationships, and inserting any missing seed data.
    /// </summary>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous migration operation.</returns>
    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        bool operationsPerformed = await this.MigrateSchemaAsync(cancellationToken).ConfigureAwait(false);
        await this.RunAsyncSeederAsync(operationsPerformed, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public bool CanConnect()
        => throw new NotSupportedException(
            "Synchronous CanConnect is not supported by JetDatabaseWriter.EntityFrameworkCore. Use CanConnectAsync instead.");

    /// <inheritdoc/>
    public async Task<bool> CanConnectAsync(CancellationToken cancellationToken = default)
        => await this.connection.CanConnectAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Resolves the physical definition used to persist a model property.
    /// </summary>
    /// <param name="property">The mapped model property.</param>
    /// <param name="format">The database format.</param>
    /// <returns>The physical column definition.</returns>
    /// <exception cref="NotSupportedException">The property's decimal facets cannot be represented in Jet3.</exception>
    internal static ColumnDefinition CreateColumnDefinition(IProperty property, DatabaseFormat format)
    {
        string columnName = JetEntityTypeMetadata.GetColumnName(property);
        Type storageType = JetEntityTypeMetadata.GetStorageClrType(property);
        bool isAutoIncrement = JetEntityTypeMetadata.IsAutoIncrementProperty(property);
        string? calculatedExpression = JetEntityTypeMetadata.GetCalculatedExpression(property);
        bool isCalculated = !string.IsNullOrWhiteSpace(calculatedExpression);
        IReadOnlyEntityType containingEntity = property.DeclaringType.ContainingEntityType;
        bool isDerivedProperty = !ReferenceEquals(containingEntity, containingEntity.GetRootType());
        bool isInOptionalOwnerOrComplex = JetEntityTypeMetadata.IsInOptionalComplexOrTableSplitOwned(property);
        bool isTableSplitOwned = JetEntityTypeMetadata.IsTableSplitOwned(containingEntity);
        bool isNullable = !isAutoIncrement
            && (!property.IsPrimaryKey() || isTableSplitOwned)
            && (property.IsNullable || isDerivedProperty || isInOptionalOwnerOrComplex);
        object? defaultValue = isCalculated ? null : TryResolveSupportedDefaultValue(property, isAutoIncrement);
        string? defaultValueExpression = isAutoIncrement || isCalculated
            ? null
            : JetEntityTypeMetadata.GetDefaultValueExpression(property);
        string? validationRule = JetEntityTypeMetadata.GetValidationRuleExpression(property);
        string? validationText = JetEntityTypeMetadata.GetValidationText(property);
        string? description = JetEntityTypeMetadata.GetDescription(property);

        if (storageType == typeof(string))
        {
            int? maxLength = property.GetMaxLength();
            bool participatesInIndex = property.IsPrimaryKey()
                || property.IsForeignKey()
                || property.IsKey()
                || property.GetContainingIndexes().Any();
            int resolvedMaxLength;
            if (maxLength is > 0 and <= 255)
            {
                resolvedMaxLength = maxLength.Value;
            }
            else if (participatesInIndex)
            {
                resolvedMaxLength = 255;
            }
            else
            {
                resolvedMaxLength = 0;
            }

            return new ColumnDefinition(columnName, typeof(string), resolvedMaxLength)
            {
                IsNullable = isNullable,
                AllowZeroLength = JetEntityTypeMetadata.GetAllowZeroLength(property),
                DefaultValue = defaultValue,
                DefaultValueExpression = defaultValueExpression,
                IsCalculated = isCalculated,
                CalculationExpression = calculatedExpression,
                ValidationRuleExpression = validationRule,
                ValidationText = validationText,
                Description = description,
            };
        }

        if (storageType == typeof(decimal))
        {
            ValueConverter? converter = property.GetValueConverter() ?? property.FindTypeMapping()?.Converter;
            Type providerType = converter?.ProviderClrType ?? property.ClrType;
            Type unwrappedClr = Nullable.GetUnderlyingType(providerType) ?? providerType;
            if (unwrappedClr.IsEnum)
            {
                unwrappedClr = Enum.GetUnderlyingType(unwrappedClr);
            }

            bool isIntegerBackedDecimal = unwrappedClr == typeof(TimeSpan) || unwrappedClr == typeof(ulong);
            int defaultPrecision = isIntegerBackedDecimal ? 20 : 18;
            int defaultScale = isIntegerBackedDecimal ? 0 : 4;
            int precision = Math.Clamp(property.GetPrecision() ?? defaultPrecision, 1, 28);
            int scale = Math.Clamp(property.GetScale() ?? defaultScale, 0, precision);
            bool inferredJet3Integer = format == DatabaseFormat.Jet3Mdb && isIntegerBackedDecimal
                && property.GetPrecision() is null && property.GetScale() is null;
            bool isCurrency = JetEntityTypeMetadata.GetIsCurrency(property) || inferredJet3Integer;
            if (format == DatabaseFormat.Jet3Mdb && !isCurrency && (precision - scale > 14 || scale > 4))
            {
                throw new NotSupportedException(
                    $"Decimal({precision},{scale}) for property '{property.DeclaringType.DisplayName()}.{property.Name}' cannot be represented by Jet3 Currency. " +
                    "Configure a scale of at most 4 and precision of at most scale + 14, explicitly configure UseJetCurrency(), or use Jet4 or ACE.");
            }

            return new ColumnDefinition(columnName, typeof(decimal))
            {
                IsNullable = isNullable,
                IsCurrency = isCurrency,
                NumericPrecision = (byte)precision,
                NumericScale = (byte)scale,
                DefaultValue = defaultValue,
                DefaultValueExpression = defaultValueExpression,
                IsCalculated = isCalculated,
                CalculationExpression = calculatedExpression,
                ValidationRuleExpression = validationRule,
                ValidationText = validationText,
                Description = description,
            };
        }

        return new ColumnDefinition(columnName, storageType, storageType == typeof(byte[]) ? property.GetMaxLength() ?? 0 : 0)
        {
            IsNullable = isNullable,
            IsAutoIncrement = isAutoIncrement,
            IsDateTimeExtended = storageType == typeof(DateTime) && JetEntityTypeMetadata.GetIsDateTimeExtended(property),
            DefaultValue = defaultValue,
            DefaultValueExpression = defaultValueExpression,
            IsCalculated = isCalculated,
            CalculationExpression = calculatedExpression,
            ValidationRuleExpression = validationRule,
            ValidationText = validationText,
            Description = description,
        };
    }

    private static object? TryResolveSupportedDefaultValue(IProperty property, bool isAutoIncrement)
    {
        if (isAutoIncrement
            || !JetEntityTypeMetadata.TryGetDefaultValue(property, out object? rawDefault)
            || rawDefault is null or DBNull)
        {
            return null;
        }

        object? converted = JetEntityTypeMetadata.ConvertToProvider(property, rawDefault);
        return converted switch
        {
            string or bool or byte or short or int or long or float or double or decimal or DateTime or Guid => converted,
            _ => null,
        };
    }

    private static object? GetDefaultProviderValueForClrType(Type clrType)
    {
        if (clrType == typeof(string))
        {
            return string.Empty;
        }

        if (clrType == typeof(bool))
        {
            return false;
        }

        if (clrType == typeof(byte))
        {
            return (byte)0;
        }

        if (clrType == typeof(short))
        {
            return (short)0;
        }

        if (clrType == typeof(int))
        {
            return 0;
        }

        if (clrType == typeof(long))
        {
            return 0L;
        }

        if (clrType == typeof(float))
        {
            return 0f;
        }

        if (clrType == typeof(double))
        {
            return 0d;
        }

        if (clrType == typeof(decimal))
        {
            return 0m;
        }

        if (clrType == typeof(DateTime))
        {
            return new DateTime(1900, 1, 1);
        }

        if (clrType == typeof(Guid))
        {
            return Guid.Empty;
        }

        return null;
    }

    private static List<IEntityType> SortEntityTypesTopologically(List<IEntityType> entityTypes)
    {
        List<IEntityType> result = [];
        HashSet<IEntityType> visited = [];
        HashSet<IEntityType> candidateSet = [.. entityTypes];

        void Visit(IEntityType current)
        {
            if (!visited.Add(current))
            {
                return;
            }

            foreach (IForeignKey fk in current.GetForeignKeys())
            {
                IEntityType principal = JetEntityTypeMetadata.GetTableOwnerEntityType(fk.PrincipalEntityType);
                if (principal != current && candidateSet.Contains(principal))
                {
                    Visit(principal);
                }
            }

            result.Add(current);
        }

        foreach (IEntityType entityType in entityTypes)
        {
            Visit(entityType);
        }

        return result;
    }

    private static string BuildIndexName(string tableName, IReadOnlyList<string> columns, string? explicitName)
    {
        if (!string.IsNullOrWhiteSpace(explicitName))
        {
            if (explicitName.Length > 64)
            {
                throw new InvalidOperationException($"Explicit index name '{explicitName}' exceeds the Access limit of 64 characters.");
            }

            return explicitName;
        }

        string joined = string.Join("_", columns);
        return TruncateObjectName($"IX_{tableName}_{joined}");
    }

    private static string BuildRelationshipName(string foreignTable, string primaryTable, IReadOnlyList<string> foreignColumns)
    {
        string joined = string.Join("_", foreignColumns);
        return TruncateObjectName($"FK_{foreignTable}_{primaryTable}_{joined}");
    }

    private static string TruncateObjectName(string name)
    {
        if (name.Length <= 64)
        {
            return name;
        }

        string suffix = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..16];
        return $"{name[..47]}_{suffix}";
    }

    private static List<(string TableName, List<ColumnDefinition> Columns, List<IndexDefinition> Indexes)> BuildDesiredTableDefinitions(
        List<IEntityType> allEntityTypes,
        DatabaseFormat format)
    {
        Dictionary<string, List<IEntityType>> tableGroups = [with(StringComparer.OrdinalIgnoreCase)];
        foreach (IEntityType entityType in allEntityTypes)
        {
            string tableName = JetEntityTypeMetadata.GetTableName(entityType);
            if (!tableGroups.TryGetValue(tableName, out List<IEntityType>? group))
            {
                group = [];
                tableGroups[tableName] = group;
            }

            group.Add(entityType);
        }

        List<(string TableName, List<ColumnDefinition> Columns, List<IndexDefinition> Indexes)> result = [];
        foreach (KeyValuePair<string, List<IEntityType>> tableEntry in tableGroups)
        {
            string tableName = tableEntry.Key;
            List<IEntityType> mappedTypes = tableEntry.Value;
            mappedTypes.Sort(static (a, b) =>
                JetEntityTypeMetadata.IsTableSplitOwned(a).CompareTo(JetEntityTypeMetadata.IsTableSplitOwned(b)));

            List<ColumnDefinition> columns = [];
            HashSet<string> addedColumns = [with(StringComparer.OrdinalIgnoreCase)];

            foreach (IEntityType entityType in mappedTypes)
            {
                foreach (IProperty property in entityType.GetFlattenedProperties())
                {
                    string columnName = JetEntityTypeMetadata.GetColumnName(property);
                    if (addedColumns.Add(columnName))
                    {
                        columns.Add(CreateColumnDefinition(property, format));
                    }
                }
            }

            IEntityType rootType = JetEntityTypeMetadata.GetTableOwnerEntityType(mappedTypes[0]);
            List<IndexDefinition> tableIndexes = [];
            HashSet<string> createdIndexNames = [with(StringComparer.OrdinalIgnoreCase)];

            IKey? primaryKey = rootType.FindPrimaryKey();
            List<string> pkCols = [];
            if (primaryKey is { Properties.Count: > 0 })
            {
                foreach (IProperty pkProp in primaryKey.Properties)
                {
                    pkCols.Add(JetEntityTypeMetadata.GetColumnName(pkProp));
                }

                IndexDefinition pkIndex = new("PrimaryKey", pkCols)
                {
                    IsPrimaryKey = true,
                    IsUnique = true,
                    IgnoreNulls = false,
                };
                tableIndexes.Add(pkIndex);
                _ = createdIndexNames.Add(pkIndex.Name);
            }

            foreach (IEntityType entityType in mappedTypes)
            {
                bool isTableSplit = JetEntityTypeMetadata.IsTableSplitOwned(entityType);
                if (!isTableSplit)
                {
                    foreach (IKey key in entityType.GetDeclaredKeys())
                    {
                        if (key.IsPrimaryKey() || key.Properties.Count == 0)
                        {
                            continue;
                        }

                        List<string> akCols = [];
                        foreach (IProperty akProp in key.Properties)
                        {
                            akCols.Add(JetEntityTypeMetadata.GetColumnName(akProp));
                        }

                        string akName = TruncateObjectName($"AK_{tableName}_{string.Join("_", akCols)}");
                        if (createdIndexNames.Add(akName))
                        {
                            tableIndexes.Add(new IndexDefinition(akName, akCols)
                            {
                                IsPrimaryKey = false,
                                IsUnique = true,
                                IgnoreNulls = false,
                            });
                        }
                        else
                        {
                            throw new InvalidOperationException($"Multiple indexes on table '{tableName}' use the Access name '{akName}'.");
                        }
                    }
                }

                foreach (IIndex index in entityType.GetDeclaredIndexes())
                {
                    if (index.Properties.Count == 0)
                    {
                        continue;
                    }

                    List<string> idxCols = [];
                    List<string> descendingCols = [];
                    IReadOnlyList<bool>? descending = index.IsDescending;
                    for (int i = 0; i < index.Properties.Count; i++)
                    {
                        string colName = JetEntityTypeMetadata.GetColumnName(index.Properties[i]);
                        idxCols.Add(colName);
                        bool isDesc = descending is not null
                            && (descending.Count == 0 || (i < descending.Count && descending[i]));
                        if (isDesc)
                        {
                            descendingCols.Add(colName);
                        }
                    }

                    if (pkCols.Count > 0 && idxCols.SequenceEqual(pkCols, StringComparer.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string indexName = BuildIndexName(tableName, idxCols, index.Name);
                    if (!createdIndexNames.Add(indexName))
                    {
                        throw new InvalidOperationException($"Multiple indexes on table '{tableName}' use the Access name '{indexName}'.");
                    }

                    bool anyNullable = index.Properties.Any(static p =>
                        p.IsNullable || JetEntityTypeMetadata.IsInOptionalComplexOrTableSplitOwned(p));
                    IndexDefinition indexDef = new(indexName, idxCols)
                    {
                        IsPrimaryKey = false,
                        IsUnique = index.IsUnique,
                        DescendingColumns = descendingCols,
                        IgnoreNulls = !index.IsUnique && anyNullable,
                    };
                    tableIndexes.Add(indexDef);
                }
            }

            result.Add((tableName, columns, tableIndexes));
        }

        return result;
    }

    private static List<RelationshipDefinition> BuildDesiredRelationships(List<IEntityType> allEntityTypes)
    {
        List<RelationshipDefinition> result = [];
        HashSet<string> createdRelationships = [with(StringComparer.OrdinalIgnoreCase)];

        foreach (IEntityType entityType in allEntityTypes)
        {
            string foreignTable = JetEntityTypeMetadata.GetTableName(entityType);
            foreach (IForeignKey foreignKey in entityType.GetDeclaredForeignKeys())
            {
                if (foreignKey.IsOwnership && JetEntityTypeMetadata.IsTableSplitOwned(entityType))
                {
                    continue;
                }

                IEntityType principalType = foreignKey.PrincipalEntityType;
                string primaryTable = JetEntityTypeMetadata.GetTableName(principalType);
                if (foreignKey.PrincipalKey.Properties.Count != foreignKey.Properties.Count
                    || foreignKey.Properties.Count == 0)
                {
                    continue;
                }

                bool typesMatch = true;
                List<string> primaryColumns = [];
                List<string> foreignColumns = [];
                for (int i = 0; i < foreignKey.Properties.Count; i++)
                {
                    IProperty pkProp = foreignKey.PrincipalKey.Properties[i];
                    IProperty fkProp = foreignKey.Properties[i];
                    if (JetEntityTypeMetadata.GetStorageClrType(pkProp) != JetEntityTypeMetadata.GetStorageClrType(fkProp))
                    {
                        typesMatch = false;
                        break;
                    }

                    primaryColumns.Add(JetEntityTypeMetadata.GetColumnName(pkProp));
                    foreignColumns.Add(JetEntityTypeMetadata.GetColumnName(fkProp));
                }

                if (!typesMatch)
                {
                    continue;
                }

                if (string.Equals(primaryTable, foreignTable, StringComparison.OrdinalIgnoreCase)
                    && primaryColumns.SequenceEqual(foreignColumns, StringComparer.OrdinalIgnoreCase))
                {
                    continue;
                }

                string relName = BuildRelationshipName(foreignTable, primaryTable, foreignColumns);
                if (!createdRelationships.Add(relName))
                {
                    throw new InvalidOperationException($"Multiple relationships use the Access name '{relName}'.");
                }

                result.Add(new RelationshipDefinition(
                    relName,
                    primaryTable,
                    primaryColumns,
                    foreignTable,
                    foreignColumns)
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = foreignKey.DeleteBehavior == DeleteBehavior.Cascade,
                    CascadeUpdates = JetEntityTypeMetadata.GetCascadeUpdates(foreignKey),
                });
            }
        }

        return result;
    }

    private static List<(string TableName, IKey? PrimaryKey, List<RowValues> SeedRows)> BuildDesiredSeedData(
        List<IEntityType> allEntityTypes)
    {
        List<(string TableName, IKey? PrimaryKey, List<RowValues> SeedRows)> result = [];
        List<IEntityType> tableOwnerTypes = [.. allEntityTypes.Where(static et => !JetEntityTypeMetadata.IsTableSplitOwned(et))];
        foreach (IEntityType entityType in SortEntityTypesTopologically(tableOwnerTypes))
        {
            List<IDictionary<string, object?>> rawSeeds = [.. entityType.GetSeedData()];
            if (rawSeeds.Count == 0)
            {
                continue;
            }

            IReadOnlyList<IProperty> properties = JetEntityTypeMetadata.GetHierarchyProperties(entityType);
            Dictionary<string, IProperty> propertyByName = [with(StringComparer.Ordinal)];
            foreach (IProperty prop in properties)
            {
                propertyByName[prop.Name] = prop;
            }

            string tableName = JetEntityTypeMetadata.GetTableName(entityType);
            List<RowValues> seedRows = [];
            foreach (IDictionary<string, object?> seedDict in rawSeeds)
            {
                RowValues row = [];
                foreach (KeyValuePair<string, object?> kvp in seedDict)
                {
                    if (propertyByName.TryGetValue(kvp.Key, out IProperty? prop))
                    {
                        string colName = JetEntityTypeMetadata.GetColumnName(prop);
                        row[colName] = JetEntityTypeMetadata.ConvertToProvider(prop, kvp.Value);
                    }
                }

                if (entityType.FindDiscriminatorProperty() is { } discriminator
                    && !seedDict.ContainsKey(discriminator.Name))
                {
                    row[JetEntityTypeMetadata.GetColumnName(discriminator)] =
                        JetEntityTypeMetadata.ConvertToProvider(discriminator, entityType.GetDiscriminatorValue());
                }

                if (row.Count > 0)
                {
                    seedRows.Add(row);
                }
            }

            if (seedRows.Count > 0)
            {
                result.Add((tableName, entityType.GetRootType().FindPrimaryKey(), seedRows));
            }
        }

        return result;
    }

    private static bool HasMatchingIndex(IReadOnlyList<IndexMetadata> existingIndexes, IndexDefinition candidate)
    {
        IndexMetadata? named = existingIndexes.FirstOrDefault(existing => string.Equals(existing.Name, candidate.Name, StringComparison.OrdinalIgnoreCase));
        if (named is not null)
        {
            if (!IndexMatches(named, candidate))
            {
                throw new InvalidOperationException(
                    $"Existing index '{candidate.Name}' differs from the EF model. Additive migration cannot change an existing index definition.");
            }

            return true;
        }

        return existingIndexes.Any(existing => IndexMatches(existing, candidate));
    }

    private static bool IndexMatches(IndexMetadata existing, IndexDefinition candidate)
    {
        if (existing.Kind == IndexKind.PrimaryKey != candidate.IsPrimaryKey
            || existing.EnforcesUniqueness != (candidate.IsUnique || candidate.IsPrimaryKey)
            || existing.IgnoreNulls != candidate.IgnoreNulls
            || existing.IsRequired != (candidate.IsRequired || candidate.IsPrimaryKey)
            || existing.Columns.Count != candidate.Columns.Count)
        {
            return false;
        }

        for (int i = 0; i < existing.Columns.Count; i++)
        {
            string column = candidate.Columns[i];
            bool descending = candidate.DescendingColumns.Contains(column, StringComparer.OrdinalIgnoreCase);
            if (!string.Equals(existing.Columns[i].Name, column, StringComparison.OrdinalIgnoreCase)
                || existing.Columns[i].IsAscending == descending)
            {
                return false;
            }
        }

        return true;
    }

    private static bool HasMatchingRelationship(IReadOnlyList<RelationshipDefinition> existingRels, RelationshipDefinition candidate)
    {
        RelationshipDefinition? existing = existingRels.FirstOrDefault(relationship => string.Equals(relationship.Name, candidate.Name, StringComparison.OrdinalIgnoreCase))
            ?? existingRels.FirstOrDefault(relationship => RelationshipColumnsMatch(relationship, candidate));
        if (existing is null)
        {
            return false;
        }

        if (!RelationshipColumnsMatch(existing, candidate)
            || existing.EnforceReferentialIntegrity != candidate.EnforceReferentialIntegrity
            || existing.CascadeUpdates != candidate.CascadeUpdates
            || existing.CascadeDeletes != candidate.CascadeDeletes)
        {
            throw new InvalidOperationException(
                $"Existing relationship '{candidate.Name}' differs from the EF model. Additive migration cannot change an existing relationship definition.");
        }

        return true;
    }

    private static bool RelationshipColumnsMatch(RelationshipDefinition existing, RelationshipDefinition candidate)
        => string.Equals(existing.PrimaryTable, candidate.PrimaryTable, StringComparison.OrdinalIgnoreCase)
            && string.Equals(existing.ForeignTable, candidate.ForeignTable, StringComparison.OrdinalIgnoreCase)
            && existing.PrimaryColumns.SequenceEqual(candidate.PrimaryColumns, StringComparer.OrdinalIgnoreCase)
            && existing.ForeignColumns.SequenceEqual(candidate.ForeignColumns, StringComparer.OrdinalIgnoreCase);

    private static async ValueTask<List<RelationshipDefinition>> ReadExistingRelationshipsAsync(
        AccessWriter writer,
        CancellationToken cancellationToken)
    {
        ResolvedTable? msysRel = await writer.Services.Snapshots.ResolveTableAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        if (msysRel is null)
        {
            return [];
        }

        List<LocatedRow> rows = await writer.Services.Snapshots.ReadRowsAsync(msysRel.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        List<RelationshipDefinition> result = [];
        foreach (RelationshipMetadata relationship in RelationshipMetadataAggregator.Aggregate(msysRel.Definition, rows.Select(static row => row.Values)))
        {
            result.Add(new RelationshipDefinition(relationship.Name, relationship.PrimaryTable, relationship.PrimaryColumns, relationship.ForeignTable, relationship.ForeignColumns)
            {
                EnforceReferentialIntegrity = relationship.EnforcesReferentialIntegrity,
                CascadeUpdates = relationship.CascadeUpdates,
                CascadeDeletes = relationship.CascadeDeletes,
            });
        }

        return result;
    }

    private static async ValueTask<bool> SeedExistsAsync(
        AccessWriter writer,
        ResolvedTable table,
        RowCriteria criteria,
        bool[] keyColumns,
        CancellationToken cancellationToken)
    {
        var predicate = RowCriteriaEvaluator.Compile(criteria, table.Definition, table.Entry.Name, nameof(criteria));
        List<LocatedRow>? indexedRows = await writer.Services.Snapshots.TryReadIndexedRowsAsync(table, criteria, cancellationToken).ConfigureAwait(false);
        if (indexedRows is not null)
        {
            return indexedRows.Exists(row => predicate.Matches(row.Values));
        }

        bool exists = false;
        await writer.Services.Snapshots.ForEachDecodedRowAsync(
            table.Entry.TDefPage,
            table.Definition,
            table.Entry.Name,
            (row, _) =>
            {
                exists = predicate.Matches(row.Values);
                return new ValueTask<bool>(!exists);
            },
            cancellationToken,
            resolveLongValueColumns: keyColumns,
            captureRawSlots: false).ConfigureAwait(false);
        return exists;
    }

    private static async ValueTask<bool> ExecuteInSchemaTransactionAsync(
        AccessWriter writer,
        Func<AccessWriter, CancellationToken, ValueTask<bool>> action,
        CancellationToken cancellationToken)
    {
        if (writer.Services.Transactions.ActiveTransaction is { NestingLevel: >= 5 })
        {
            throw new InvalidOperationException(
                "Schema changes cannot preserve atomicity at the current transaction nesting limit. Release a savepoint before migrating.");
        }

        JetTransaction schemaTransaction = await writer.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            bool operationsPerformed = await action(writer, cancellationToken).ConfigureAwait(false);
            await schemaTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            return operationsPerformed;
        }
        catch
        {
#pragma warning disable CA1031, RCS1075 // Preserve original exception when rollback fails.
            try
            {
                await schemaTransaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
#pragma warning restore CA1031, RCS1075

            throw;
        }
        finally
        {
            await schemaTransaction.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task<bool> EnsureSchemaCreatedAsync(CancellationToken cancellationToken)
    {
        bool canConnect = await this.connection.CanConnectAsync(cancellationToken).ConfigureAwait(false);
        if (canConnect && await this.connection.HasUserTablesAsync(cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        AccessWriter? writer = null;
        bool rentedOwned = false;
        try
        {
            if (!canConnect)
            {
                writer = await this.connection.CreateDatabaseWriterAsync(cancellationToken).ConfigureAwait(false);
                rentedOwned = !string.IsNullOrWhiteSpace(this.connection.DatabasePath);
            }
            else
            {
                (writer, rentedOwned) = await this.connection.RentWriterAsync(cancellationToken).ConfigureAwait(false);
            }

            await ExecuteInSchemaTransactionAsync(writer, this.BuildSchemaCoreAsync, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            if (writer is not null)
            {
                await this.connection.ReleaseWriterAsync(writer, ownedByCaller: rentedOwned).ConfigureAwait(false);
            }
        }
    }

    private async Task<bool> MigrateSchemaAsync(CancellationToken cancellationToken)
    {
        bool canConnect = await this.connection.CanConnectAsync(cancellationToken).ConfigureAwait(false);
        AccessWriter? writer = null;
        bool rentedOwned = false;
        try
        {
            if (!canConnect)
            {
                writer = await this.connection.CreateDatabaseWriterAsync(cancellationToken).ConfigureAwait(false);
                rentedOwned = !string.IsNullOrWhiteSpace(this.connection.DatabasePath);
                await ExecuteInSchemaTransactionAsync(writer, this.BuildSchemaCoreAsync, cancellationToken).ConfigureAwait(false);
                return true;
            }

            (writer, rentedOwned) = await this.connection.RentWriterAsync(cancellationToken).ConfigureAwait(false);
            return await ExecuteInSchemaTransactionAsync(writer, this.MigrateSchemaCoreAsync, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (writer is not null)
            {
                await this.connection.ReleaseWriterAsync(writer, ownedByCaller: rentedOwned).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask RunAsyncSeederAsync(bool operationsPerformed, CancellationToken cancellationToken)
    {
        if (this.options?.AsyncSeeder is not { } seeder)
        {
            if (this.options?.Seeder is not null)
            {
                throw new InvalidOperationException(CoreStrings.MissingAsyncSeeder);
            }

            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        DbContext context = this.currentContext.Context;
        IDbContextTransaction? transaction = this.connection.ActiveTransaction is null
            ? await context.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
            : null;
        try
        {
            await seeder(context, operationsPerformed, cancellationToken).ConfigureAwait(false);
            if (transaction is not null)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (transaction is not null)
            {
#pragma warning disable CA1031, RCS1075 // Preserve the seed callback exception if rollback fails.
                try
                {
                    await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
#pragma warning restore CA1031, RCS1075
            }

            throw;
        }
        finally
        {
            if (transaction is not null)
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<bool> BuildSchemaCoreAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        DatabaseFormat format = this.connection.Format;
        List<IEntityType> allEntityTypes = [.. this.model.GetEntityTypes()];

        foreach ((string tableName, List<ColumnDefinition> columns, List<IndexDefinition> indexes) in BuildDesiredTableDefinitions(allEntityTypes, format))
        {
            await writer.CreateTableAsync(tableName, columns, indexes, cancellationToken).ConfigureAwait(false);
        }

        foreach (RelationshipDefinition relDef in BuildDesiredRelationships(allEntityTypes))
        {
            await writer.CreateRelationshipAsync(relDef, cancellationToken).ConfigureAwait(false);
        }

        foreach ((string tableName, _, List<RowValues> seedRows) in BuildDesiredSeedData(allEntityTypes))
        {
            await writer.InsertRowsAsync(tableName, seedRows, cancellationToken).ConfigureAwait(false);
        }

        return allEntityTypes.Count > 0;
    }

    private async ValueTask<bool> MigrateSchemaCoreAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        List<CatalogEntry> existingTableEntries = await writer.Services.Catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        if (existingTableEntries.Count == 0)
        {
            return await this.BuildSchemaCoreAsync(writer, cancellationToken).ConfigureAwait(false);
        }

        bool operationsPerformed = false;
        DatabaseFormat format = this.connection.Format;
        List<IEntityType> allEntityTypes = [.. this.model.GetEntityTypes()];
        HashSet<string> existingTables = [with(StringComparer.OrdinalIgnoreCase), .. existingTableEntries.Select(static e => e.Name)];
        HashSet<string> newlyCreatedTables = [with(StringComparer.OrdinalIgnoreCase)];

        foreach ((string tableName, List<ColumnDefinition> columns, List<IndexDefinition> indexes) in BuildDesiredTableDefinitions(allEntityTypes, format))
        {
            if (!existingTables.Contains(tableName))
            {
                await writer.CreateTableAsync(tableName, columns, indexes, cancellationToken).ConfigureAwait(false);
                operationsPerformed = true;
                _ = existingTables.Add(tableName);
                _ = newlyCreatedTables.Add(tableName);
                continue;
            }

            ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (resolved is null)
            {
                continue;
            }

            HashSet<string> existingColNames = [with(StringComparer.OrdinalIgnoreCase), .. resolved.Definition.Columns.Select(static c => c.Name)];
            foreach (ColumnDefinition col in columns)
            {
                if (existingColNames.Add(col.Name))
                {
                    await writer.AddColumnAsync(tableName, col, cancellationToken).ConfigureAwait(false);
                    operationsPerformed = true;
                    if (!col.IsCalculated && !col.IsAutoIncrement)
                    {
                        object? backfill = col.DefaultValue ?? (!col.IsNullable ? GetDefaultProviderValueForClrType(col.ClrType) : null);
                        if (backfill is not null)
                        {
                            _ = await writer.UpdateRowsAsync(
                                tableName,
                                [ColumnPredicate.IsNull(col.Name)],
                                new RowValues { [col.Name] = backfill },
                                cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
            }

            IReadOnlyList<IndexMetadata> existingIndexes = await writer.Services.Snapshots.ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
            foreach (IndexDefinition idx in indexes)
            {
                if (HasMatchingIndex(existingIndexes, idx))
                {
                    continue;
                }

                if (idx.IsPrimaryKey)
                {
                    throw new InvalidOperationException(
                        $"Existing table '{tableName}' has no matching primary key '{idx.Name}'. Additive migration cannot change an existing primary key definition.");
                }

                await writer.CreateIndexAsync(tableName, idx, cancellationToken).ConfigureAwait(false);
                operationsPerformed = true;
            }
        }

        List<RelationshipDefinition> existingRels = await ReadExistingRelationshipsAsync(writer, cancellationToken).ConfigureAwait(false);
        foreach (RelationshipDefinition relDef in BuildDesiredRelationships(allEntityTypes))
        {
            if (!HasMatchingRelationship(existingRels, relDef))
            {
                await writer.CreateRelationshipAsync(relDef, cancellationToken).ConfigureAwait(false);
                operationsPerformed = true;
                existingRels.Add(relDef);
            }
        }

        foreach ((string tableName, IKey? primaryKey, List<RowValues> seedRows) in BuildDesiredSeedData(allEntityTypes))
        {
            if (newlyCreatedTables.Contains(tableName))
            {
                await writer.InsertRowsAsync(tableName, seedRows, cancellationToken).ConfigureAwait(false);
                operationsPerformed = true;
                continue;
            }

            if (primaryKey is not { Properties.Count: > 0 })
            {
                continue;
            }

            ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            if (resolved is null)
            {
                continue;
            }

            string[] pkColNames = new string[primaryKey.Properties.Count];
            bool[] keyColumns = new bool[resolved.Definition.Columns.Count];
            bool allPkFound = true;
            for (int i = 0; i < primaryKey.Properties.Count; i++)
            {
                string colName = JetEntityTypeMetadata.GetColumnName(primaryKey.Properties[i]);
                pkColNames[i] = colName;
                int ord = resolved.Definition.FindColumnIndex(colName);
                if (ord < 0)
                {
                    allPkFound = false;
                    break;
                }

                keyColumns[ord] = true;
            }

            if (!allPkFound)
            {
                continue;
            }

            List<RowValues> missingSeedRows = [];
            foreach (RowValues seedRow in seedRows)
            {
                RowCriteria criteria = [];
                foreach (string keyColumn in pkColNames)
                {
                    _ = seedRow.TryGetValue(keyColumn, out object? seedKeyValue);
                    criteria.Add(ColumnPredicate.EqualTo(keyColumn, seedKeyValue));
                }

                if (!await SeedExistsAsync(writer, resolved, criteria, keyColumns, cancellationToken).ConfigureAwait(false))
                {
                    missingSeedRows.Add(seedRow);
                }
            }

            if (missingSeedRows.Count > 0)
            {
                await writer.InsertRowsAsync(tableName, missingSeedRows, cancellationToken).ConfigureAwait(false);
                operationsPerformed = true;
            }
        }

        return operationsPerformed;
    }
}
