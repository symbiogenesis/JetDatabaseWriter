namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Creates, migrates, and deletes Microsoft Access databases and their schema from an EF Core <see cref="IModel"/>.
/// </summary>
public sealed class JetDatabaseCreator : IDatabaseCreator
{
    private readonly IJetClientConnection connection;
    private readonly IModel model;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetDatabaseCreator"/> class.
    /// </summary>
    /// <param name="connection">The client connection for the current context scope.</param>
    /// <param name="designTimeModel">The design-time EF Core model describing the schema.</param>
    public JetDatabaseCreator(IJetClientConnection connection, IDesignTimeModel designTimeModel)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(designTimeModel);

        this.connection = connection;
        this.model = designTimeModel.Model;
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
        bool canConnect = await this.connection.CanConnectAsync(cancellationToken).ConfigureAwait(false);
        if (canConnect)
        {
            int existingTableCount = this.connection.ActiveWriter is { } activeWriter
                ? (await activeWriter.Services.Catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false)).Count
                : (await (await this.connection.GetReaderAsync(cancellationToken).ConfigureAwait(false))
                    .ListTablesAsync(cancellationToken).ConfigureAwait(false)).Count;
            if (existingTableCount > 0)
            {
                return false;
            }
        }

        AccessWriter? writer = null;
        bool rentedOwned = false;
        try
        {
            if (!canConnect)
            {
                writer = await this.connection.CreateDatabaseWriterAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                (writer, rentedOwned) = await this.connection.RentWriterAsync(cancellationToken).ConfigureAwait(false);
            }

            await this.ExecuteInSchemaTransactionAsync(writer, this.BuildSchemaCoreAsync, cancellationToken).ConfigureAwait(false);
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
        bool canConnect = await this.connection.CanConnectAsync(cancellationToken).ConfigureAwait(false);
        AccessWriter? writer = null;
        bool rentedOwned = false;
        try
        {
            if (!canConnect)
            {
                writer = await this.connection.CreateDatabaseWriterAsync(cancellationToken).ConfigureAwait(false);
                await this.ExecuteInSchemaTransactionAsync(writer, this.BuildSchemaCoreAsync, cancellationToken).ConfigureAwait(false);
                return;
            }

            (writer, rentedOwned) = await this.connection.RentWriterAsync(cancellationToken).ConfigureAwait(false);
            await this.ExecuteInSchemaTransactionAsync(writer, this.MigrateSchemaCoreAsync, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (writer is not null)
            {
                await this.connection.ReleaseWriterAsync(writer, ownedByCaller: rentedOwned).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    public bool CanConnect()
        => throw new NotSupportedException(
            "Synchronous CanConnect is not supported by JetDatabaseWriter.EntityFrameworkCore. Use CanConnectAsync instead.");

    /// <inheritdoc/>
    public async Task<bool> CanConnectAsync(CancellationToken cancellationToken = default)
        => await this.connection.CanConnectAsync(cancellationToken).ConfigureAwait(false);

    private static ColumnDefinition CreateColumnDefinition(IProperty property, DatabaseFormat format)
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
            Type unwrappedClr = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
            bool isIntegerBackedDecimal = unwrappedClr == typeof(TimeSpan) || unwrappedClr == typeof(ulong);
            int defaultPrecision = isIntegerBackedDecimal ? 20 : 18;
            int defaultScale = isIntegerBackedDecimal ? 0 : 4;
            int precision = Math.Clamp(property.GetPrecision() ?? defaultPrecision, 1, 28);
            int scale = Math.Clamp(property.GetScale() ?? defaultScale, 0, precision);
            bool isCurrency = JetEntityTypeMetadata.GetIsCurrency(property)
                || (format == DatabaseFormat.Jet3Mdb && (precision - scale > 14 || scale > 4));
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

        return new ColumnDefinition(columnName, storageType)
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
            return TruncateObjectName(explicitName);
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
        => name.Length <= 64 ? name : name[..64];

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
                        continue;
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
                    continue;
                }

                result.Add(new RelationshipDefinition(
                    relName,
                    primaryTable,
                    primaryColumns,
                    foreignTable,
                    foreignColumns)
                {
                    EnforceReferentialIntegrity = foreignKey.DeleteBehavior != DeleteBehavior.NoAction,
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

            List<IProperty> properties = JetEntityTypeMetadata.GetHierarchyProperties(entityType);
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
        foreach (IndexMetadata existing in existingIndexes)
        {
            if (string.Equals(existing.Name, candidate.Name, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (existing.EnforcesUniqueness == candidate.IsUnique
                && existing.Columns.Count == candidate.Columns.Count)
            {
                bool columnsMatch = true;
                for (int i = 0; i < existing.Columns.Count; i++)
                {
                    if (!string.Equals(existing.Columns[i].Name, candidate.Columns[i], StringComparison.OrdinalIgnoreCase))
                    {
                        columnsMatch = false;
                        break;
                    }
                }

                if (columnsMatch)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static bool HasMatchingRelationship(IReadOnlyList<RelationshipDefinition> existingRels, RelationshipDefinition candidate)
    {
        foreach (RelationshipDefinition existing in existingRels)
        {
            if (string.Equals(existing.Name, candidate.Name, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (string.Equals(existing.PrimaryTable, candidate.PrimaryTable, StringComparison.OrdinalIgnoreCase)
                && string.Equals(existing.ForeignTable, candidate.ForeignTable, StringComparison.OrdinalIgnoreCase)
                && existing.PrimaryColumns.SequenceEqual(candidate.PrimaryColumns, StringComparer.OrdinalIgnoreCase)
                && existing.ForeignColumns.SequenceEqual(candidate.ForeignColumns, StringComparer.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask<List<RelationshipDefinition>> ReadExistingRelationshipsAsync(
        AccessWriter writer,
        CancellationToken cancellationToken)
    {
        ResolvedTable? msysRel = await writer.Services.Snapshots.ResolveTableAsync("MSysRelationships", cancellationToken).ConfigureAwait(false);
        if (msysRel is null)
        {
            return [];
        }

        int relNameOrd = msysRel.Definition.FindColumnIndex("szRelationship");
        int foreignTableOrd = msysRel.Definition.FindColumnIndex("szObject");
        int primaryTableOrd = msysRel.Definition.FindColumnIndex("szReferencedObject");
        int foreignColOrd = msysRel.Definition.FindColumnIndex("szColumn");
        int primaryColOrd = msysRel.Definition.FindColumnIndex("szReferencedColumn");
        int iColOrd = msysRel.Definition.FindColumnIndex("icolumn");
        if (relNameOrd < 0 || foreignTableOrd < 0 || primaryTableOrd < 0 || foreignColOrd < 0 || primaryColOrd < 0)
        {
            return [];
        }

        List<LocatedRow> rows = await writer.Services.Snapshots.ReadRowsAsync(msysRel.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        Dictionary<string, (string PrimaryTable, string ForeignTable, List<(int Order, string PrimaryCol, string ForeignCol)> Columns)> grouped =
            [with(StringComparer.OrdinalIgnoreCase)];
        foreach (LocatedRow row in rows)
        {
            object?[] vals = row.Values;
            if (vals[relNameOrd] is not string relName || string.IsNullOrEmpty(relName)
                || vals[primaryTableOrd] is not string primaryTable || string.IsNullOrEmpty(primaryTable)
                || vals[foreignTableOrd] is not string foreignTable || string.IsNullOrEmpty(foreignTable)
                || vals[primaryColOrd] is not string primaryCol || string.IsNullOrEmpty(primaryCol)
                || vals[foreignColOrd] is not string foreignCol || string.IsNullOrEmpty(foreignCol))
            {
                continue;
            }

            int order = iColOrd >= 0 && vals[iColOrd] is int iCol ? iCol : 0;
            if (!grouped.TryGetValue(relName, out (string PrimaryTable, string ForeignTable, List<(int Order, string PrimaryCol, string ForeignCol)> Columns) entry))
            {
                entry = (primaryTable, foreignTable, []);
                grouped[relName] = entry;
            }

            entry.Columns.Add((order, primaryCol, foreignCol));
        }

        List<RelationshipDefinition> result = [];
        foreach (KeyValuePair<string, (string PrimaryTable, string ForeignTable, List<(int Order, string PrimaryCol, string ForeignCol)> Columns)> kvp in grouped)
        {
            kvp.Value.Columns.Sort(static (a, b) => a.Order.CompareTo(b.Order));
            List<string> primaryCols = [.. kvp.Value.Columns.Select(static c => c.PrimaryCol)];
            List<string> foreignCols = [.. kvp.Value.Columns.Select(static c => c.ForeignCol)];
            result.Add(new RelationshipDefinition(kvp.Key, kvp.Value.PrimaryTable, primaryCols, kvp.Value.ForeignTable, foreignCols));
        }

        return result;
    }

    private async ValueTask ExecuteInSchemaTransactionAsync(
        AccessWriter writer,
        Func<AccessWriter, CancellationToken, ValueTask> action,
        CancellationToken cancellationToken)
    {
        JetTransaction? schemaTransaction = this.connection.ActiveTransaction is null
            ? await writer.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
            : null;
        try
        {
            await action(writer, cancellationToken).ConfigureAwait(false);
            if (schemaTransaction is not null)
            {
                await schemaTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (schemaTransaction is not null)
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
            }

            throw;
        }
        finally
        {
            if (schemaTransaction is not null)
            {
                await schemaTransaction.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask BuildSchemaCoreAsync(AccessWriter writer, CancellationToken cancellationToken)
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
    }

    private async ValueTask MigrateSchemaCoreAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        List<CatalogEntry> existingTableEntries = await writer.Services.Catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        if (existingTableEntries.Count == 0)
        {
            await this.BuildSchemaCoreAsync(writer, cancellationToken).ConfigureAwait(false);
            return;
        }

        DatabaseFormat format = this.connection.Format;
        List<IEntityType> allEntityTypes = [.. this.model.GetEntityTypes()];
        HashSet<string> existingTables = [with(StringComparer.OrdinalIgnoreCase), .. existingTableEntries.Select(static e => e.Name)];
        HashSet<string> newlyCreatedTables = [with(StringComparer.OrdinalIgnoreCase)];

        foreach ((string tableName, List<ColumnDefinition> columns, List<IndexDefinition> indexes) in BuildDesiredTableDefinitions(allEntityTypes, format))
        {
            if (!existingTables.Contains(tableName))
            {
                await writer.CreateTableAsync(tableName, columns, indexes, cancellationToken).ConfigureAwait(false);
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
                if (idx.IsPrimaryKey || HasMatchingIndex(existingIndexes, idx))
                {
                    continue;
                }

                await writer.CreateIndexAsync(tableName, idx, cancellationToken).ConfigureAwait(false);
            }
        }

        List<RelationshipDefinition> existingRels = await ReadExistingRelationshipsAsync(writer, cancellationToken).ConfigureAwait(false);
        foreach (RelationshipDefinition relDef in BuildDesiredRelationships(allEntityTypes))
        {
            if (!HasMatchingRelationship(existingRels, relDef))
            {
                await writer.CreateRelationshipAsync(relDef, cancellationToken).ConfigureAwait(false);
                existingRels.Add(relDef);
            }
        }

        foreach ((string tableName, IKey? primaryKey, List<RowValues> seedRows) in BuildDesiredSeedData(allEntityTypes))
        {
            if (newlyCreatedTables.Contains(tableName))
            {
                await writer.InsertRowsAsync(tableName, seedRows, cancellationToken).ConfigureAwait(false);
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
            int[] pkOrdinals = new int[primaryKey.Properties.Count];
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

                pkOrdinals[i] = ord;
            }

            if (!allPkFound)
            {
                continue;
            }

            List<LocatedRow> existingRows = await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
            List<RowValues> missingSeedRows = [];
            foreach (RowValues seedRow in seedRows)
            {
                bool exists = false;
                foreach (LocatedRow existingRow in existingRows)
                {
                    bool matchesAllKeys = true;
                    for (int k = 0; k < pkColNames.Length; k++)
                    {
                        if (!seedRow.TryGetValue(pkColNames[k], out object? seedKeyVal))
                        {
                            matchesAllKeys = false;
                            break;
                        }

                        object? existingVal = pkOrdinals[k] < existingRow.Values.Length ? existingRow.Values[pkOrdinals[k]] : null;
                        if (existingVal is DBNull)
                        {
                            existingVal = null;
                        }

                        if (!Equals(existingVal, seedKeyVal))
                        {
                            matchesAllKeys = false;
                            break;
                        }
                    }

                    if (matchesAllKeys)
                    {
                        exists = true;
                        break;
                    }
                }

                if (!exists)
                {
                    missingSeedRows.Add(seedRow);
                }
            }

            if (missingSeedRows.Count > 0)
            {
                await writer.InsertRowsAsync(tableName, missingSeedRows, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
