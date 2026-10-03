namespace JetDatabaseWriter.ValueDecoding;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>
/// Reads a few columns of one row, by location, as typed values, without
/// following long-value chains. The writer's index and foreign-key checks use
/// it for key columns.
/// </summary>
internal static class PartialColumnReader
{
    /// <summary>
    /// Reads <paramref name="columnOrdinals"/>'s typed values out of a single
    /// row at <paramref name="loc"/> on a data page belonging to
    /// <paramref name="tableDef"/>. Returns <see langword="null"/> when the
    /// row layout cannot be parsed OR when any requested column needs
    /// long-value (Memo, Ole) or complex (Complex, Attachment)
    /// traversal outside this inline reader; the cascade-seek caller falls back to the snapshot
    /// path in that case. Index-key column types (the focus of this helper)
    /// usually include scalar fixed and var-inline kinds. Memo is indexable
    /// but routes through the snapshot path when pre-write uniqueness checks
    /// need existing-row values; OLE / Attachment / Complex columns are
    /// rejected by <see cref="Indexes.Helpers.IndexHelpers.ResolveIndexes"/>.
    /// </summary>
    /// <param name="db">
    /// The open database file, whose pages are read and which supplies the
    /// format to <see cref="RowDecodePlan"/>; core-split-b narrows this to the
    /// page source and the format profile together with
    /// <see cref="RowDecodePlan"/>.
    /// </param>
    /// <param name="loc">The row location; its bytes are read from <see cref="RowLocation.DataPageNumber"/>.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="columnOrdinals">The column ordinals.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The values, or <see langword="null"/>.</returns>
    internal static async ValueTask<object?[]?> TryReadColumnValuesTypedAsync(
        DatabaseFile db,
        RowLocation loc,
        TableDef tableDef,
        int[] columnOrdinals,
        CancellationToken cancellationToken)
    {
        byte[] pageBytes = await db.Pages.ReadPageAsync(loc.DataPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (pageBytes[0] != Constants.PageTypes.Data)
            {
                return null;
            }

            var decodePlan = RowDecodePlan.CreatePartial(tableDef, columnOrdinals);
            object?[] result = new object?[columnOrdinals.Length];
            return decodePlan.TryDecodePartialColumns(db, pageBytes, loc.RowStart, loc.RowSize, result)
                ? result
                : null;
        }
        finally
        {
            PageBuffers.Return(pageBytes);
        }
    }
}
