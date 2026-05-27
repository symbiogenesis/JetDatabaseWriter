namespace JetDatabaseWriter.Pages.Models;

/// <summary>Per-row coordinates that include the owning data page number — used by writer-side
/// scans that need to round-trip back to the page (update / delete / re-encrypt).</summary>
/// <param name="PageNumber">The page number.</param>
/// <param name="RowIndex">The row index.</param>
/// <param name="RowStart">The row start.</param>
/// <param name="RowSize">The row size.</param>
internal readonly record struct RowLocation(long PageNumber, int RowIndex, int RowStart, int RowSize);
