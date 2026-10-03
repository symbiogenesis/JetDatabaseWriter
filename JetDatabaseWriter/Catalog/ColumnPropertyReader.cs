namespace JetDatabaseWriter.Catalog;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Reads a table's persisted column properties (the <c>MSysObjects.LvProp</c>
/// blob) and applies the calculated-column <c>ResultType</c> they record to a
/// <see cref="TableDef"/>. Shared by the reader and writer service graphs, so
/// both decode and encode a calculated column's cached value by the same type.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="rows">Decodes <c>MSysObjects</c> rows, with OLE columns as their stored bytes.</param>
internal sealed class ColumnPropertyReader(DatabaseFile db, RowDecoder rows)
{
    /// <summary>
    /// Returns the <c>ResultType</c> a calculated column's persisted properties
    /// declare, or <see langword="default"/> when none is recorded.
    /// </summary>
    /// <param name="target">The column's persisted property target.</param>
    internal static ColumnType ResolveCalculatedResultType(ColumnPropertyTarget? target)
    {
        ColumnPropertyEntry? rt = target?.Find(Constants.ColumnPropertyNames.ResultType);
        return rt?.Value.Length >= 1
            && (rt.DataType == ByteType
                || rt.DataType == IntegerType
                || rt.DataType == LongIntegerType)
            ? (ColumnType)rt.Value[0]
            : default;
    }

    /// <summary>
    /// Reads and parses the <c>MSysObjects.LvProp</c> blob of the catalog row whose
    /// <c>Id</c> is exactly <paramref name="tdefPage"/>. The blob is the column's
    /// stored bytes, decoded with only <c>Id</c> and <c>LvProp</c> of each row.
    /// Matching the whole Id matters: a form, report, module or query can have an
    /// Id with the high bit set whose low 24 bits equal a table's TDEF page.
    /// Returns <see langword="null"/> when the catalog has no <c>LvProp</c> column
    /// (slim schemas written by older versions of this library), the row is
    /// missing, the blob is empty or cannot be read, or its magic header is
    /// unrecognised.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<ColumnPropertyBlock?> ReadLvPropForTableAsync(long tdefPage, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await db.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys is null)
        {
            return null;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxLvProp = msys.FindColumnIndex("LvProp");
        if (idxId < 0 || idxLvProp < 0)
        {
            return null;
        }

        bool[] wantedColumns = new bool[msys.Columns.Count];
        wantedColumns[idxId] = true;
        wantedColumns[idxLvProp] = true;
        await foreach (object?[] row in rows.EnumerateTypedRowsForTdefAsync(2, msys, wantedColumns, cancellationToken).ConfigureAwait(false))
        {
            if (row[idxId] is int id && id == tdefPage)
            {
                return ColumnPropertyBlock.Parse(row[idxLvProp] as byte[], db.Format);
            }
        }

        return null;
    }

    /// <summary>
    /// Sets <see cref="ColumnInfo.CalculatedResultType"/> on each calculated column of
    /// <paramref name="tableDef"/> from the table's persisted properties, and
    /// re-initializes the column metadata when any column changed.
    /// </summary>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="tableDef">The table definition to update in place.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when any column's result type changed.</returns>
    internal async ValueTask<bool> HydrateCalculatedResultTypesAsync(long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        if (!tableDef.Columns.Exists(static col => col.IsCalculated))
        {
            return false;
        }

        ColumnPropertyBlock? properties = await this.ReadLvPropForTableAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (properties is null)
        {
            return false;
        }

        bool changed = false;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            if (!col.IsCalculated)
            {
                continue;
            }

            ColumnType resultType = ResolveCalculatedResultType(properties.FindTarget(col.Name));
            if (resultType != default && resultType != col.CalculatedResultType)
            {
                tableDef.Columns[i] = col.WithCalculatedResultType(resultType);
                changed = true;
            }
        }

        if (changed)
        {
            tableDef.InitializeColumnMetadata();
        }

        return changed;
    }
}
