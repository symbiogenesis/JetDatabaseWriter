namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Decodes the writer's own rows, index metadata, and persisted column
/// properties for writer workflows that read a table before they change it.
/// Every page is read through the writer's own pages, so inside a transaction
/// these reads see the pages the transaction has journaled, and encrypted
/// files are decrypted with the writer's own page keys. Nothing is cached
/// between calls: <paramref name="rows"/> reads pages uncached, the writer's
/// <paramref name="tableDefs"/> and <paramref name="ownedPages"/> memoize
/// nothing, and table names resolve through the writer's
/// <see cref="TableCatalog"/>, which schema changes invalidate.
/// </summary>
/// <param name="format">The writer's format profile: the TDEF and row layouts.</param>
/// <param name="pages">The writer's pages, through which disposal is checked.</param>
/// <param name="tableDefs">Reads the writer's TDEF bytes.</param>
/// <param name="ownedPages">Walks the writer's table rows.</param>
/// <param name="rows">Decodes rows from pages read through <paramref name="pages"/>; its page cache must be disabled.</param>
/// <param name="catalog">Resolves user and system table names over the writer's table catalog and reads persisted column properties.</param>
internal sealed class TableSnapshotReader(JetFormat format, IPageSource pages, TableDefReader tableDefs, OwnedDataPages ownedPages, RowDecoder rows, CatalogReader catalog)
{
    /// <summary>
    /// Returns the values of a snapshot row with <see langword="null"/> cells
    /// replaced by <see cref="DBNull.Value"/>, matching the writer's row-array
    /// convention.
    /// </summary>
    /// <param name="row">The snapshot row.</param>
    internal static object[] GetDbNullNormalizedItemArray(DataRow row)
    {
        Guard.NotNull(row, nameof(row));

        object?[] values = row.ItemArray;
        for (int i = 0; i < values.Length; i++)
        {
            values[i] ??= DBNull.Value;
        }

        return (object[])values;
    }

    /// <summary>
    /// Decodes every live row of the table rooted at <paramref name="tdefPage"/>,
    /// each paired with the location it was decoded from, in the page and row
    /// order of <see cref="OwnedDataPages.ForEachLiveTableRowAsync"/>. Rows too
    /// short or malformed to decode are left out, so a caller that mutates
    /// <see cref="LocatedRow.Location"/> changes exactly the row it read.
    /// Returns an empty list when the page holds no table definition.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<List<LocatedRow>> ReadRowsAsync(long tdefPage, CancellationToken cancellationToken)
    {
        pages.ThrowIfDisposedOrCancelled(cancellationToken);

        TableDef? tableDef = await catalog.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        return tableDef is null
            ? []
            : await this.DecodeRowsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves <paramref name="tableName"/>, a user or system table, to its
    /// catalog entry and table definition, with calculated columns' result
    /// types hydrated. Returns <see langword="null"/> when no table with
    /// columns has that name.
    /// </summary>
    /// <param name="tableName">The table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<ResolvedTable?> ResolveTableAsync(string tableName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        return catalog.ResolveTableAsync(tableName, cancellationToken);
    }

    /// <summary>
    /// Reads every live row of <paramref name="tableName"/> (a user or system
    /// table) into a <see cref="DataTable"/>, for writer workflows that insert
    /// the rows again. Complex columns stay as their raw references, OLE cells
    /// hold the stored bytes, as in every typed read, and a MEMO / OLE value whose stored data cannot be read becomes an
    /// <see cref="ValueDecoding.Models.UnreadableLongValue"/>, which the writer
    /// refuses to store, instead of a placeholder. MEMO / OLE columns are
    /// therefore typed <see cref="object"/>. Returns an empty table when no
    /// table has that name.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<DataTable> ReadTableSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        List<LocatedRow> snapshotRows = resolved is null
            ? []
            : await this.DecodeRowsAsync(resolved.Entry.TDefPage, resolved.Definition, cancellationToken).ConfigureAwait(false);

        DataTable? table = null;
        try
        {
            table = new DataTable(tableName);
            if (resolved is not null)
            {
                foreach (ColumnInfo column in resolved.Definition.Columns)
                {
                    // Complex columns hold raw references, and MEMO / OLE cells
                    // (including a calculated column whose result type is MEMO or
                    // OLE) may hold an UnreadableLongValue, so those columns are untyped.
                    Type clrType = column.Type is ComplexType or AttachmentType || ResolveValueType(column) is MemoType or OleType
                        ? typeof(object)
                        : ResolveClrType(column);
                    _ = table.Columns.Add(column.Name, clrType);
                }
            }

            table.BeginLoadData();
            foreach (LocatedRow row in snapshotRows)
            {
                _ = table.Rows.Add(row.Values);
            }

            table.EndLoadData();

            DataTable result = table;
            table = null;
            return result;
        }
        finally
        {
            table?.Dispose();
        }
    }

    /// <summary>
    /// Enumerates <paramref name="tableName"/>'s logical indexes via the same parser
    /// that <see cref="Interfaces.IAccessReader.ListIndexesAsync"/> uses, so schema
    /// rewrites can forward existing index definitions.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<IndexMetadata>> ReadIndexMetadataSnapshotAsync(string tableName, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            return [];
        }

        byte[]? tdef = await tableDefs.ReadTDefBytesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        return tdef is null || tdef.Length < format.TDef.BlockEnd
            ? []
            : IndexCatalogReader.ReadMetadata(format, tdef, resolved.Definition.Columns);
    }

    /// <summary>
    /// Reads and parses the stored <c>MSysObjects.LvProp</c> bytes of the catalog
    /// row whose <c>Id</c> is exactly <paramref name="tdefPage"/>. Returns
    /// <see langword="null"/> when the catalog has no <c>LvProp</c> column or the row
    /// has no property blob.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<ColumnPropertyBlock?> ReadLvPropBlockAsync(long tdefPage, CancellationToken cancellationToken)
    {
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        return catalog.ReadLvPropForTableAsync(tdefPage, cancellationToken);
    }

    /// <summary>Visits strict typed projected rows without retaining the table in memory.</summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="tableDef">The hydrated table definition.</param>
    /// <param name="wantedColumns">The selected columns.</param>
    /// <param name="visit">The row callback; false stops the walk.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetCorruptDataException">A live row cannot be decoded.</exception>
    internal async ValueTask ForEachTypedRowAsync(
        long tdefPage,
        TableDef tableDef,
        bool[] wantedColumns,
        Func<RowLocation, object?[], CancellationToken, ValueTask<bool>> visit,
        CancellationToken cancellationToken)
    {
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        var plan = RowDecodePlan.CreateTypedForWriteBack(tableDef, strictParsing: true, wantedColumns);
        await ownedPages.ForEachLiveTableRowAsync(
            tdefPage,
            async (row, token) =>
            {
                object?[] values = await rows.CrackRowTypedAsync(row.Page, row.Location.RowStart, row.Location.RowSize, plan, token).ConfigureAwait(false)
                    ?? throw new JetCorruptDataException(JetErrorCode.MalformedValue, "A complex-parent predicate row cannot be decoded.", new JetErrorInfo { PageNumber = row.Location.PageNumber });

                for (int index = 0; index < values.Length; index++)
                {
                    if (values[index] is UnreadableLongValue)
                    {
                        throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, "A selected complex-parent long value cannot be read.", new JetErrorInfo { PageNumber = row.Location.PageNumber, ColumnName = tableDef.Columns[index].Name });
                    }
                }

                return await visit(row.Location, values, token).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Decodes every live row on the data pages owned by <paramref name="tdefPage"/>
    /// together with its location. Rows too short or malformed to decode are skipped.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="tableDef">The table definition, with calculated-column result types hydrated.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<List<LocatedRow>> DecodeRowsAsync(long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        var result = new List<LocatedRow>();

        // Updates, cascades and schema rewrites insert these values again, so
        // OLE cells keep their stored bytes exactly and an unreadable MEMO /
        // OLE value becomes an UnreadableLongValue rather than a placeholder.
        var decodePlan = RowDecodePlan.CreateTypedForWriteBack(tableDef, rows.StrictParsing);
        await ownedPages.ForEachLiveTableRowAsync(
            tdefPage,
            async (row, token) =>
            {
                if (row.Location.RowSize < format.RowFields.NumCols)
                {
                    return true;
                }

                object?[]? values = await rows.CrackRowTypedAsync(row.Page, row.Location.RowStart, row.Location.RowSize, decodePlan, token).ConfigureAwait(false);
                if (values is null)
                {
                    return true;
                }

                if (tableDef.HasHyperlinkColumns)
                {
                    TableReader.WrapHyperlinkColumns(values, tableDef.ClrTypes);
                }

                for (int i = 0; i < values.Length; i++)
                {
                    values[i] ??= DBNull.Value;
                }

                result.Add(new LocatedRow(row.Location, (object[])values));
                return true;
            },
            cancellationToken).ConfigureAwait(false);

        return result;
    }
}
