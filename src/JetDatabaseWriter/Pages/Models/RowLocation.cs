namespace JetDatabaseWriter.Pages.Models;

/// <summary>
/// Per-row coordinates that include the owning data page number — used by writer-side
/// scans that need to round-trip back to the page (update / delete / re-encrypt).
/// <see cref="PageNumber"/> and <see cref="RowIndex"/> are the row's identity: the slot
/// that index entries name and that a delete flags. For an overflow row that slot is
/// the header, and the row's bytes are at <see cref="DataPageNumber"/> /
/// <see cref="DataRowIndex"/>, which <see cref="RowStart"/> and <see cref="RowSize"/>
/// index into.
/// </summary>
/// <param name="PageNumber">The page holding the row's slot (the overflow header for an overflow row).</param>
/// <param name="RowIndex">The row's slot index on <paramref name="PageNumber"/>.</param>
/// <param name="RowStart">The offset of the row's bytes on <see cref="DataPageNumber"/>.</param>
/// <param name="RowSize">The length of the row's bytes.</param>
internal readonly record struct RowLocation(long PageNumber, int RowIndex, int RowStart, int RowSize)
{
    /// <summary>Gets the page holding the row's bytes; differs from <see cref="PageNumber"/> only for an overflow row moved to another page.</summary>
    public long DataPageNumber { get; init; } = PageNumber;

    /// <summary>Gets the slot holding the row's bytes; differs from <see cref="RowIndex"/> only for an overflow row.</summary>
    public int DataRowIndex { get; init; } = RowIndex;

    /// <summary>Gets a value indicating whether the row's bytes live in another slot than its identity (an overflow row).</summary>
    public bool IsOverflow => this.DataPageNumber != this.PageNumber || this.DataRowIndex != this.RowIndex;
}
