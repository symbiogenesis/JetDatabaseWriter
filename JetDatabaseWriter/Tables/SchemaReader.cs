namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Relationships;
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
/// <param name="db">The database page I/O and format context.</param>
/// <param name="pages">The reader's page cache, whose hit rate the statistics report.</param>
/// <param name="catalog">Resolves tables and reads persisted column properties.</param>
/// <param name="complexColumns">Describes complex columns and their subtypes.</param>
/// <param name="linked">Lists linked tables and describes their columns.</param>
/// <param name="tables">Reads the <c>MSysRelationships</c> catalog table.</param>
/// <param name="operations">The reader's operation gate.</param>
internal sealed class SchemaReader(
    DatabaseFile db,
    ReaderPageCache pages,
    CatalogReader catalog,
    ComplexColumnReader complexColumns,
    LinkedTableReader linked,
    TableReader tables,
    AsyncReentrantOperationGate operations)
{
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
            TableDef? td = await db.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            result.Add(new TableStat
            {
                Name = entry.Name,
                RowCount = td?.RowCount ?? 0L,
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
            TableDef? td = await db.ReadTableDefAsync(table.TDefPage, cancellationToken).ConfigureAwait(false);
            if (td != null)
            {
                tableRowCounts[table.Name] = td.RowCount;
                totalRows += td.RowCount;
            }
        }

        long cacheHits = pages.Hits;
        long cacheMisses = pages.Misses;
        long totalAccess = cacheHits + cacheMisses;
        int pageCacheHitRate = totalAccess > 0 ? (int)(cacheHits * 100 / totalAccess) : 0;

        return new DatabaseStatistics
        {
            TotalPages = db.DatabaseStream.Length / db.PageSizeBytes,
            DatabaseSizeBytes = db.DatabaseStream.Length,
            TableCount = userTables.Count,
            TotalRows = totalRows,
            TableRowCounts = tableRowCounts,
            PageCacheHitRate = pageCacheHitRate,
            Version = db.Format == DatabaseFormat.Jet3Mdb ? "Jet3" : "Jet4/ACE",
            Format = db.Format,
            CodePage = db.CodePage,
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

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            IReadOnlyList<ColumnMetadata>? linkedMetadata = await linked.TryGetColumnMetadataAsync(tableName, cancellationToken).ConfigureAwait(false);
            return linkedMetadata ?? [];
        }

        Dictionary<string, string> complexSubtypes = new(StringComparer.OrdinalIgnoreCase);
        bool hasComplex = resolved.Definition.Columns.Any(c => c.Type is ComplexType or AttachmentType);
        if (hasComplex)
        {
            complexSubtypes = await complexColumns.ReadColumnSubtypesAsync(tableName, cancellationToken).ConfigureAwait(false);
        }

        ColumnPropertyBlock? properties = await catalog.ReadLvPropForTableAsync(
            resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);

        return resolved.Definition.Columns.Select((col, index) =>
        {
            ColumnPropertyTarget? target = properties?.FindTarget(col.Name);
            bool isCalc = col.IsCalculated;
            string? calcExpr = isCalc
                ? target?.GetTextValue(Constants.ColumnPropertyNames.Expression, db.Format)
                : null;
            ColumnType calcResultType = isCalc ? CatalogReader.ResolveCalculatedResultType(target) : default;

            return new ColumnMetadata
            {
                Name = col.Name,
                TypeName = (col.Type == ComplexType && complexSubtypes.TryGetValue(col.Name, out string? subtype))
                    ? subtype
                    : ResolveTypeName(col),
                ClrType = ResolveClrType(col),
                MaxLength = GetMetadataMaxLength(col),
                IsNullable = ResolveIsNullable(col, target),
                IsFixedLength = col.IsFixed,
                IsHyperlink = IsHyperlinkColumn(col),
                Ordinal = index,
                Size = GetColumnSize(ResolveValueType(col), GetMetadataDeclaredSize(col)),
                DefaultValueExpression = target?.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, db.Format),
                ValidationRuleExpression = target?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, db.Format),
                ValidationText = target?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, db.Format),
                Description = target?.GetTextValue(Constants.ColumnPropertyNames.Description, db.Format),
                NumericPrecision = col.NumericPrecision,
                NumericScale = col.NumericScale,
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
        // the catalog fallback and returns an empty table when it is absent (Jet3
        // or slim-catalog files). The operation gate is reentrant, so the nested
        // ReadTableAsync call joins this root operation rather than blocking.
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
        return await complexColumns.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the declared row count from <paramref name="tableName"/>'s TDEF header
    /// (a cheap lookup with no row scan), or 0 when the table cannot be resolved. Used
    /// as a cost estimate when choosing between per-key index seeks and a single scan.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The declared row count, or 0 when unknown.</returns>
    internal async ValueTask<long> GetDeclaredRowCountAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        return resolved?.Definition.RowCount ?? 0;
    }

    /// <summary>
    /// Resolves a column's <c>IsNullable</c> from the persisted <c>Required</c>
    /// LvProp property when present, falling back to the legacy writer-private
    /// TDEF flag bit <c>0x08</c> for back-compat with files written by older
    /// JetDatabaseWriter revisions. DAO/Access never emit <c>0x08</c> in the
    /// flag byte, so the fallback reads as <c>true</c> (nullable) for any file
    /// authored outside this library.
    /// </summary>
    /// <param name="col">The column descriptor.</param>
    /// <param name="target">Column property metadata read from <c>MSysObjects.LvProp</c>.</param>
    private static bool ResolveIsNullable(ColumnInfo col, ColumnPropertyTarget? target)
    {
        if ((col.Flags & Constants.ColumnDescriptorFlags.AutoNumber) != 0)
        {
            return false;
        }

        bool? required = target?.GetBooleanValue(Constants.ColumnPropertyNames.Required);
        if (required is bool r)
        {
            return !r;
        }

        return (col.Flags & Constants.ColumnDescriptorFlags.LegacyNotNull) == 0;
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
