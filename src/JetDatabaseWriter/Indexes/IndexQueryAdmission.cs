namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>Shared descriptor and key checks for reader and transaction-snapshot index queries.</summary>
internal static class IndexQueryAdmission
{
    /// <summary>Retains indexes whose native columns and text sort orders can be resolved.</summary>
    /// <param name="tableDef">The acquired table definition.</param>
    /// <param name="indexes">The candidate indexes in preference order.</param>
    /// <returns>The supported candidates in their original order.</returns>
    internal static List<IndexMetadata> FilterSeekableIndexes(TableDef tableDef, IReadOnlyList<IndexMetadata> indexes)
    {
        List<IndexMetadata> result = [];
        foreach (IndexMetadata index in indexes)
        {
            bool supported = true;
            foreach (IndexColumnReference key in index.Columns)
            {
                ColumnInfo? column = tableDef.Columns.FirstOrDefault(c => c.ColNum == key.ColumnNumber);
                if (column is null || (column.Type is TextType or MemoType && !column.TextSortOrder.IsSupported))
                {
                    supported = false;
                    break;
                }
            }

            if (supported)
            {
                result.Add(index);
            }
        }

        return result;
    }

    /// <summary>Checks every key or range bound before a query chooses an index.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="definition">The acquired table definition.</param>
    /// <param name="tableName">The table name used in diagnostics.</param>
    /// <param name="plan">The candidate index and criteria.</param>
    /// <returns>Whether every supplied key can be encoded.</returns>
    internal static bool CanEncodePlan(JetFormat format, TableDef definition, string tableName, IndexPlan plan)
        => (plan.Criteria.Values is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Values))
            && (plan.Criteria.Lower is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Lower.Values))
            && (plan.Criteria.Upper is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Upper.Values));

    /// <summary>Checks join collation and every tuple before choosing a multi-key index seek.</summary>
    /// <param name="format">The database format and join collation.</param>
    /// <param name="tableDef">The acquired table definition.</param>
    /// <param name="tableName">The table name used in diagnostics.</param>
    /// <param name="index">The candidate index.</param>
    /// <param name="keys">The provider-converted join keys.</param>
    /// <returns>Whether each key can safely narrow join candidates.</returns>
    internal static bool CanSeekJoinKeys(JetFormat format, TableDef tableDef, string tableName, IndexMetadata index, IEnumerable<object?[]> keys)
    {
        foreach (object?[] key in keys)
        {
            for (int ordinal = 0; ordinal < key.Length && ordinal < index.Columns.Count; ordinal++)
            {
                ColumnInfo? column = tableDef.Columns.FirstOrDefault(value => string.Equals(value.Name, index.Columns[ordinal].Name, StringComparison.OrdinalIgnoreCase));
                if (column?.Type is TextType or MemoType
                    && TextCollationFamily(column.TextSortOrder) != TextCollationFamily(format.DefaultTextSortOrder))
                {
                    // Joins match by database collation. A differently collated
                    // index cannot safely narrow their candidate rows.
                    return false;
                }
            }

            if (!CanEncodePlan(format, tableDef, tableName, new IndexPlan(index, IndexQueryCriteria.KeyPrefix(key), key.Length)))
            {
                return false;
            }
        }

        return true;
    }

    private static int TextCollationFamily(TextSortOrder order)
    {
        if (order.Value == 0 || (order.HasVersion && order.Version == 0))
        {
            return 0;
        }

        return order.HasVersion ? 1 : 2;
    }
}
