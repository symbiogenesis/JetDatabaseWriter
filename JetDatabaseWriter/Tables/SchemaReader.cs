namespace JetDatabaseWriter.Tables;

using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Schema and database metadata reads behind <see cref="Interfaces.IAccessReader"/>:
/// table lists and statistics, column metadata (merged with persisted column
/// properties), foreign-key relationships, linked-table and complex-column
/// descriptions, and declared row counts. Each operation enters the reader's
/// operation gate so disposal waits for it.
/// </summary>
/// <param name="format">The file's format profile, which the statistics describe.</param>
/// <param name="pageFile">The file's pages, whose count and length the statistics report.</param>
/// <param name="tableDefs">Reads table definitions.</param>
/// <param name="pages">The reader's page cache, whose hit rate the statistics report.</param>
/// <param name="catalog">Resolves tables and reads persisted column properties.</param>
/// <param name="complexColumns">Describes complex columns and their subtypes.</param>
/// <param name="linked">Lists linked tables and describes their columns.</param>
/// <param name="tables">Reads the <c>MSysRelationships</c> catalog table.</param>
/// <param name="operations">The reader's operation gate.</param>
internal sealed class SchemaReader(
    JetFormat format,
    PageFile pageFile,
    TableDefReader tableDefs,
    ReaderPageCache pages,
    CatalogReader catalog,
    ComplexColumnReader complexColumns,
    LinkedTableReader linked,
    TableReader tables,
    AsyncReentrantOperationGate operations)
{
    /// <summary>Reads the empty-name table validation property target.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The table rule or absence.</returns>
    /// <exception cref="JetObjectNotFoundException">The table does not exist.</exception>
    internal async ValueTask<TableValidationRule?> GetTableValidationRuleAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        ResolvedTable table = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false)
            ?? throw new JetObjectNotFoundException(JetErrorCode.TableNotFound, $"Table '{tableName}' was not found.", nameof(tableName), new JetErrorInfo { TableName = tableName });
        ColumnPropertyBlock? properties = await catalog.ReadLvPropForTableAsync(table.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        ColumnPropertyTarget? target = properties?.FindTableTarget();
        string? expression = PersistedExpressionText.Normalize(target?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, format));
        return string.IsNullOrWhiteSpace(expression) ? null : new TableValidationRule(expression, target?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, format));
    }

    /// <summary>Returns the names of all user tables in the database.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        List<CatalogEntry> userTables = await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        return userTables.ConvertAll(e => e.Name);
    }

    /// <summary>Returns detached copies of the linked tables defined in the catalog.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        List<LinkedTableInfo> links = await linked.GetLinkedTablesAsync(cancellationToken).ConfigureAwait(false);
        return links.ConvertAll(static link => link with { }); // Clone to detach from internal cache instances
    }

    /// <summary>Returns name, stored row-count, and column-count for every user table.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> entries = await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        var result = new List<TableStat>(entries.Count);

        foreach (CatalogEntry entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TableDef? td = await tableDefs.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            result.Add(new TableStat
            {
                Name = entry.Name,
                RowCount = (await tableDefs.ReadTableCountersAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false))?.RowCount ?? 0L,
                ColumnCount = td?.Columns.Count ?? 0,
            });
        }

        return result;
    }

    /// <summary>Returns table metadata as a DataTable with columns TableName, RowCount, and ColumnCount.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        DataTable? dt = null;
        try
        {
            dt = new DataTable("Tables");
            _ = dt.Columns.Add("TableName", typeof(string));
            _ = dt.Columns.Add("RowCount", typeof(long));
            _ = dt.Columns.Add("ColumnCount", typeof(int));

            IReadOnlyList<TableStat> stats = await this.GetTableStatsAsync(cancellationToken).ConfigureAwait(false);
            foreach (TableStat s in stats)
            {
                _ = dt.Rows.Add(s.Name, s.RowCount, s.ColumnCount);
            }

            DataTable result = dt;
            dt = null;
            return result;
        }
        finally
        {
            dt?.Dispose();
        }
    }

    /// <summary>Returns statistical information about the database.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> userTables = await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        var tableRowCounts = new Dictionary<string, long>();
        long totalRows = 0;

        foreach (CatalogEntry table in userTables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TableCounters? counters = await tableDefs.ReadTableCountersAsync(table.TDefPage, cancellationToken).ConfigureAwait(false);
            if (counters is { } current)
            {
                tableRowCounts[table.Name] = current.RowCount;
                totalRows += current.RowCount;
            }
        }

        long cacheHits = pages.Hits;
        long cacheMisses = pages.Misses;
        long totalAccess = cacheHits + cacheMisses;
        int pageCacheHitRate = totalAccess > 0 ? (int)(cacheHits * 100 / totalAccess) : 0;

        return new DatabaseStatistics
        {
            TotalPages = pageFile.PageCount,
            DatabaseSizeBytes = pageFile.LengthBytes,
            TableCount = userTables.Count,
            TotalRows = totalRows,
            TableRowCounts = tableRowCounts,
            PageCacheHitRate = pageCacheHitRate,
            Version = format.VersionName,
            Format = format.Kind,
            CodePage = format.CodePage,
        };
    }

    /// <summary>Returns rich metadata for all columns in the specified table.</summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            IReadOnlyList<ColumnMetadata>? linkedMetadata = await linked.TryGetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
            return linkedMetadata ?? [];
        }

        // Complex columns are named by subtype, keyed by ComplexID (the descriptor's misc slot).
        Dictionary<int, string>? complexTypeNames = resolved.Definition.Columns.Any(c => c.Type is ComplexType)
            ? await complexColumns.ReadColumnTypeNamesAsync(tableName, cancellationToken).ConfigureAwait(false)
            : null;

        ColumnPropertyBlock? properties = await catalog.ReadLvPropForTableAsync(
            resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);

        return resolved.Definition.Columns.Select((col, index) =>
        {
            ColumnPropertyTarget? target = properties?.FindTarget(col.Name);
            bool isCalc = col.IsCalculated;
            string? calcExpr = isCalc
                ? PersistedExpressionText.Normalize(target?.GetTextValue(Constants.ColumnPropertyNames.Expression, format))
                : null;
            ColumnType calcResultType = isCalc ? CatalogReader.ResolveCalculatedResultType(target) : default;

            return new ColumnMetadata
            {
                Name = col.Name,
                TypeName = col.Type is ComplexType
                    && complexTypeNames != null
                    && complexTypeNames.TryGetValue(col.Misc, out string? complexTypeName)
                        ? complexTypeName
                        : ResolveTypeName(col),
                ClrType = ResolveClrType(col),
                MaxLength = GetMetadataMaxLength(col),
                IsNullable = ResolveIsNullable(col, target),
                IsFixedLength = col.IsFixed,
                IsHyperlink = IsHyperlinkColumn(col),
                Ordinal = index,
                Size = GetColumnSize(ResolveValueType(col), GetMetadataDeclaredSize(col)),
                DefaultValueExpression = PersistedExpressionText.Normalize(target?.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, format)),
                ValidationRuleExpression = PersistedExpressionText.Normalize(target?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, format)),
                ValidationText = target?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, format),
                Description = target?.GetTextValue(Constants.ColumnPropertyNames.Description, format),
                NumericPrecision = col.NumericPrecision,
                NumericScale = col.NumericScale,
                IsCurrency = ResolveValueType(col) == MoneyType,
                IsCalculated = isCalc,
                CalculationExpression = calcExpr,
                CalculatedResultType = (byte)(calcResultType != default ? calcResultType : col.CalculatedResultType),
            };
        }).ToList();
    }

    /// <summary>
    /// Returns metadata for every foreign-key relationship declared in the database's
    /// <c>MSysRelationships</c> catalog.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<RelationshipMetadata>> ListRelationshipsAsync(CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();

        // MSysRelationships is a system table; ReadTableAsync resolves it through
        // the catalog fallback; absence means there are no stored relationships.
        // The gate is reentrant, so the nested
        // ReadTableAsync call joins this root operation rather than blocking.
        if (!await tables.TryLookupTableAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false))
        {
            return [];
        }

        DataTable table = await tables.ReadTableAsync(Constants.SystemTableNames.Relationships, maxRows: null, progress: null, cancellationToken).ConfigureAwait(false);
        try
        {
            return RelationshipMetadataAggregator.Aggregate(table);
        }
        finally
        {
            table.Dispose();
        }
    }

    /// <summary>
    /// Returns metadata for every Access 2007+ complex column declared on <paramref name="tableName"/>.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();
        await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        return await complexColumns.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the declared row count from <paramref name="tableName"/>'s TDEF header
    /// (a cheap lookup with no row scan), or 0 for a linked table. Used
    /// as a cost estimate when choosing between per-key index seeks and a single scan.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The declared row count, or 0 for a linked table.</returns>
    internal async ValueTask<long> GetDeclaredRowCountAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();
        await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        return resolved is null ? 0 : (await tableDefs.ReadTableCountersAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false))?.RowCount ?? 0;
    }

    /// <summary>
    /// Resolves nullability from the persisted Required property and AutoNumber flags.
    /// </summary>
    /// <param name="col">The column descriptor.</param>
    /// <param name="target">Column property metadata read from <c>MSysObjects.LvProp</c>.</param>
    private static bool ResolveIsNullable(ColumnInfo col, ColumnPropertyTarget? target)
    {
        if (col.IsAutoNumber)
        {
            return false;
        }

        bool? required = target?.GetBooleanValue(Constants.ColumnPropertyNames.Required);
        if (required is bool r)
        {
            return !r;
        }

        return true;
    }

    private static int? GetMetadataMaxLength(ColumnInfo col)
    {
        int declaredSize = GetMetadataDeclaredSize(col);
        return declaredSize > 0 ? declaredSize : null;
    }

    private static int GetMetadataDeclaredSize(ColumnInfo col)
    {
        if (col.IsCalculated && (col.Type == TextType || col.Type == BinaryType) && col.Size > Constants.CalculatedColumn.ExtraDataLen)
        {
            return col.Size - Constants.CalculatedColumn.ExtraDataLen;
        }

        return col.Size;
    }

    private static string ResolveTypeName(ColumnInfo col) =>
        IsHyperlinkColumn(col) ? "Hyperlink" : GetTypeDisplayName(ResolveValueType(col));
}
