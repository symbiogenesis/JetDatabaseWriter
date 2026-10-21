namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;

/// <summary>Checks native restrictions on relationship key descriptors.</summary>
internal static class RelationshipColumnPolicy
{
    /// <summary>Refuses calculated relationship keys, which Access cannot index.</summary>
    /// <param name="definition">The native table definition.</param>
    /// <param name="columns">The relationship key names.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="relationshipName">The relationship name.</param>
    /// <exception cref="JetOperationException">A relationship key is calculated.</exception>
    internal static void ThrowIfCalculated(TableDef definition, IReadOnlyList<string> columns, string tableName, string relationshipName)
    {
        foreach (string columnName in columns)
        {
            int ordinal = definition.FindColumnIndex(columnName);
            if (ordinal >= 0 && definition.Columns[ordinal].IsCalculated)
            {
                throw new JetOperationException(
                    JetErrorCode.KeyColumnInRelationship,
                    $"Relationship '{relationshipName}' cannot use calculated column '{tableName}.{columnName}'. Access does not permit calculated relationship keys.",
                    errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = columnName, RelationshipName = relationshipName });
            }
        }
    }
}
