namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Decodes the writer's own rows, index metadata, and persisted column
/// properties for writer workflows that read a table before they change it.
/// Every page is read through the writer's <see cref="DatabaseFile"/>, so
/// inside a transaction these reads see the pages the transaction has
/// journaled, and encrypted files are decrypted with the writer's own page
/// keys. Nothing is cached between calls: <paramref name="rows"/> reads pages
/// uncached, and table names resolve through the writer's
/// <see cref="TableCatalog"/>, which schema changes invalidate.
/// </summary>
/// <param name="db">The writer's database file.</param>
/// <param name="rows">Decodes rows from pages read through <paramref name="db"/>; its page cache must be disabled.</param>
/// <param name="catalog">Resolves user and system table names over the writer's table catalog and reads persisted column properties.</param>
internal sealed class TableSnapshotReader(DatabaseFile db, RowDecoder rows, CatalogReader catalog)
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
    /// order of <see cref="DatabaseFile.ForEachLiveTableRowAsync"/>. Rows too
    /// short or malformed to decode are left out, so a caller that mutates
    /// <see cref="LocatedRow.Location"/> changes exactly the row it read.
    /// Returns an empty list when the page holds no table definition.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<List<LocatedRow>> ReadRowsAsync(long tdefPage, CancellationToken cancellationToken)
    {
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        TableDef? tableDef = await catalog.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        return tableDef is null
            ? []
            : await this.DecodeRowsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads every live row of <paramref name="tableName"/> (a user or system
    /// table) into a <see cref="DataTable"/>, for writer workflows that insert
    /// the rows again. Complex columns stay as their raw references, OLE cells
    /// hold the stored bytes exactly (no package unwrap or signature slicing),
    /// and a MEMO / OLE value whose stored data cannot be read becomes an
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
        db.ThrowIfDisposedOrCancelled(cancellationToken);

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
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            return [];
        }

        byte[]? tdef = await db.ReadTDefBytesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        return tdef is null || tdef.Length < db.TDef.BlockEnd
            ? []
            : IndexCatalogReader.ReadMetadata(db, tdef, resolved.Definition.Columns);
    }

    /// <summary>
    /// Reads and parses the <c>MSysObjects.LvProp</c> blob for the catalog row whose
    /// <c>Id</c> low-24 bits equal <paramref name="tdefPage"/>. Returns
    /// <see langword="null"/> when the catalog has no <c>LvProp</c> column or the row
    /// has no property blob.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<ColumnPropertyBlock?> ReadLvPropBlockAsync(long tdefPage, CancellationToken cancellationToken)
    {
        db.ThrowIfDisposedOrCancelled(cancellationToken);
        return catalog.ReadLvPropForTableAsync(tdefPage, cancellationToken);
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
        await db.ForEachLiveTableRowAsync(
            tdefPage,
            async (row, token) =>
            {
                if (row.Location.RowSize < db.RowFields.NumCols)
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
