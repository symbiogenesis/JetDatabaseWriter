namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
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

    private static ColumnDefinition CreateColumnDefinition(IProperty property)
    {
        string columnName = JetEntityTypeMetadata.GetColumnName(property);
        Type storageType = JetEntityTypeMetadata.GetStorageClrType(property);
        bool isAutoIncrement = JetEntityTypeMetadata.IsAutoIncrementProperty(property);
        bool isNullable = !isAutoIncrement && !property.IsPrimaryKey() && property.IsNullable;

        if (storageType == typeof(string))
        {
            int? maxLength = property.GetMaxLength();
            bool participatesInIndex = property.IsPrimaryKey()
                || property.IsForeignKey()
                || property.GetContainingIndexes().Any();
            int resolvedMaxLength;
            if (maxLength is > 255 && !participatesInIndex)
            {
                resolvedMaxLength = 0;
            }
            else if (maxLength is > 0 and <= 255)
            {
                resolvedMaxLength = maxLength.Value;
            }
            else
            {
                resolvedMaxLength = 255;
            }

            return new ColumnDefinition(columnName, typeof(string), resolvedMaxLength)
            {
                IsNullable = isNullable,
                AllowZeroLength = true,
            };
        }

        if (storageType == typeof(decimal))
        {
            int precision = Math.Clamp(property.GetPrecision() ?? 18, 1, 28);
            int scale = Math.Clamp(property.GetScale() ?? 4, 0, precision);
            return new ColumnDefinition(columnName, typeof(decimal))
            {
                IsNullable = isNullable,
                NumericPrecision = (byte)precision,
                NumericScale = (byte)scale,
            };
        }

        return new ColumnDefinition(columnName, storageType)
        {
            IsNullable = isNullable,
            IsAutoIncrement = isAutoIncrement,
        };
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
                        columns.Add(CreateColumnDefinition(property));
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
    }
}
