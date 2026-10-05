namespace JetDatabaseWriter.Catalog;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Reads a table's persisted column properties (the <c>MSysObjects.LvProp</c>
/// blob) and applies the calculated-column <c>ResultType</c> they record to a
/// <see cref="TableDef"/>. Shared by the reader and writer service graphs, so
/// both decode and encode a calculated column's cached value by the same type.
/// </summary>
/// <param name="format">The file's format profile, whose format the property blocks are parsed for.</param>
/// <param name="tableDefs">Reads the <c>MSysObjects</c> table definition.</param>
/// <param name="rows">Decodes <c>MSysObjects</c> rows, with OLE columns as their stored bytes.</param>
internal sealed class ColumnPropertyReader(JetFormat format, TableDefReader tableDefs, RowDecoder rows)
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

        TableDef? msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
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
                return ColumnPropertyBlock.Parse(row[idxLvProp] as byte[], format.Kind);
            }
        }

        return null;
    }
}
