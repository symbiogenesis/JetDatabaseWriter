namespace JetDatabaseWriter.Pages.Models;

/// <summary>
/// The row data an overflow header points at, as resolved by
/// <see cref="OwnedDataPages.TryResolveOverflowRowAsync"/>.
/// </summary>
/// <param name="PageNumber">The page holding the row data.</param>
/// <param name="RowIndex">The row data's slot on <paramref name="PageNumber"/>.</param>
/// <param name="Page">The bytes of <paramref name="PageNumber"/>, as returned by the page reader the caller supplied.</param>
/// <param name="Bound">The row data's bounds on <paramref name="Page"/>.</param>
internal readonly record struct OverflowRowTarget(long PageNumber, int RowIndex, byte[] Page, RowBound Bound);
