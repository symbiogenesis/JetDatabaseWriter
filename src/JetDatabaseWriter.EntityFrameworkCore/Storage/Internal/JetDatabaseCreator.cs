namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Creates and deletes Microsoft Access databases and their schema from an EF Core <see cref="IModel"/>.
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
            AccessReader reader = await this.connection.GetReaderAsync(cancellationToken).ConfigureAwait(false);
            IReadOnlyList<string> existingTables = await reader.ListTablesAsync(cancellationToken).ConfigureAwait(false);
            if (existingTables.Count > 0)
            {
                return false;
            }
        }

        AccessWriter? createdWriter = null;
        AccessWriter? writer = null;
        bool rentedOwned = false;
        try
        {
            if (!canConnect)
            {
                createdWriter = await this.connection.CreateDatabaseWriterAsync(cancellationToken).ConfigureAwait(false);
                writer = createdWriter;
            }
            else
            {
                (writer, rentedOwned) = await this.connection.RentWriterAsync(cancellationToken).ConfigureAwait(false);
            }

            await this.BuildSchemaAsync(writer, cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            if (createdWriter is not null)
            {
                await createdWriter.DisposeAsync().ConfigureAwait(false);
            }
            else if (rentedOwned && writer is not null)
            {
                await this.connection.ReleaseWriterAsync(writer, ownedByCaller: true).ConfigureAwait(false);
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
        bool isDerivedProperty = !ReferenceEquals(property.DeclaringType, property.DeclaringType.ContainingEntityType.GetRootType());
        bool isNullable = !isAutoIncrement && !property.IsPrimaryKey() && (property.IsNullable || isDerivedProperty);
        object? defaultValue = TryResolveSupportedDefaultValue(property, isAutoIncrement);

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
                AllowZeroLength = true,
                DefaultValue = defaultValue,
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
            bool isCurrency = format == DatabaseFormat.Jet3Mdb && (precision - scale > 14 || scale > 4);
            return new ColumnDefinition(columnName, typeof(decimal))
            {
                IsNullable = isNullable,
                IsCurrency = isCurrency,
                NumericPrecision = (byte)precision,
                NumericScale = (byte)scale,
                DefaultValue = defaultValue,
            };
        }

        return new ColumnDefinition(columnName, storageType)
        {
            IsNullable = isNullable,
            IsAutoIncrement = isAutoIncrement,
            DefaultValue = defaultValue,
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
                IEntityType principal = fk.PrincipalEntityType;
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

    private async ValueTask BuildSchemaAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        DatabaseFormat format = this.connection.Format;
        List<IEntityType> entityTypes = [.. this.model.GetEntityTypes().Where(static et => !et.IsOwned())];
        Dictionary<string, List<IEntityType>> tableGroups = [with(StringComparer.OrdinalIgnoreCase)];
        foreach (IEntityType entityType in entityTypes)
        {
            string tableName = JetEntityTypeMetadata.GetTableName(entityType);
            if (!tableGroups.TryGetValue(tableName, out List<IEntityType>? group))
            {
                group = [];
                tableGroups[tableName] = group;
            }

            group.Add(entityType);
        }

        foreach (KeyValuePair<string, List<IEntityType>> tableEntry in tableGroups)
        {
            string tableName = tableEntry.Key;
            List<IEntityType> mappedTypes = tableEntry.Value;
            List<ColumnDefinition> columns = [];
            HashSet<string> addedColumns = [with(StringComparer.OrdinalIgnoreCase)];

            foreach (IEntityType entityType in mappedTypes)
            {
                foreach (IProperty property in entityType.GetProperties())
                {
                    string columnName = JetEntityTypeMetadata.GetColumnName(property);
                    if (addedColumns.Add(columnName))
                    {
                        columns.Add(CreateColumnDefinition(property, format));
                    }
                }
            }

            IEntityType rootType = mappedTypes[0].GetRootType();
            List<IndexDefinition> tableIndexes = [];
            HashSet<string> createdIndexNames = [with(StringComparer.OrdinalIgnoreCase)];

            IKey? primaryKey = rootType.FindPrimaryKey();
            if (primaryKey is { Properties.Count: > 0 })
            {
                List<string> pkCols = [];
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

                foreach (IIndex index in entityType.GetDeclaredIndexes())
                {
                    if (index.Properties.Count == 0)
                    {
                        continue;
                    }

                    if (primaryKey is not null
                        && index.Properties.Count == primaryKey.Properties.Count
                        && index.Properties.SequenceEqual(primaryKey.Properties))
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

                    string indexName = BuildIndexName(tableName, idxCols, index.Name);
                    if (!createdIndexNames.Add(indexName))
                    {
                        continue;
                    }

                    IndexDefinition indexDef = new(indexName, idxCols)
                    {
                        IsPrimaryKey = false,
                        IsUnique = index.IsUnique,
                        DescendingColumns = descendingCols,
                        IgnoreNulls = !index.IsUnique && index.Properties.Any(static p => p.IsNullable),
                    };
                    tableIndexes.Add(indexDef);
                }
            }

            await writer.CreateTableAsync(tableName, columns, tableIndexes, cancellationToken).ConfigureAwait(false);
        }

        HashSet<string> createdRelationships = [with(StringComparer.OrdinalIgnoreCase)];
        foreach (IEntityType entityType in entityTypes)
        {
            string foreignTable = JetEntityTypeMetadata.GetTableName(entityType);
            foreach (IForeignKey foreignKey in entityType.GetDeclaredForeignKeys())
            {
                IEntityType principalType = foreignKey.PrincipalEntityType;
                string primaryTable = JetEntityTypeMetadata.GetTableName(principalType);
                if (string.Equals(primaryTable, foreignTable, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

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

                string relName = BuildRelationshipName(foreignTable, primaryTable, foreignColumns);
                if (!createdRelationships.Add(relName))
                {
                    continue;
                }

                RelationshipDefinition relDef = new(
                    relName,
                    primaryTable,
                    primaryColumns,
                    foreignTable,
                    foreignColumns)
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = foreignKey.DeleteBehavior == DeleteBehavior.Cascade,
                };
                await writer.CreateRelationshipAsync(relDef, cancellationToken).ConfigureAwait(false);
            }
        }

        foreach (IEntityType entityType in SortEntityTypesTopologically(entityTypes))
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
                await writer.InsertRowsAsync(tableName, seedRows, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
