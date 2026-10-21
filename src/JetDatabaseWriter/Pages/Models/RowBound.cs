namespace JetDatabaseWriter.Pages.Models;

/// <summary>One entry of a data page's row directory: a row-offset slot and the bytes it covers.</summary>
/// <param name="RowIndex">The slot's row index on the page.</param>
/// <param name="RowStart">The offset of the slot's bytes on the page.</param>
/// <param name="RowSize">The number of bytes up to the next greater row offset on the page.</param>
/// <param name="IsOverflowPointer">
/// Whether the slot is an overflow row's header (flagged
/// <see cref="Constants.DataPage.OverflowRowFlag"/>), whose bytes are a pointer to
/// the row data rather than the row itself; see
/// <see cref="OwnedDataPages.TryResolveOverflowRowAsync"/>.
/// </param>
internal readonly record struct RowBound(int RowIndex, int RowStart, int RowSize, bool IsOverflowPointer = false);
