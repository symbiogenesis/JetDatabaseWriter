namespace JetDatabaseWriter.Pages.Models;

/// <summary>Per-row coordinates that include the owning data page number — used by writer-side
/// scans that need to round-trip back to the page (update / delete / re-encrypt).</summary>
internal readonly record struct RowLocation(long PageNumber, int RowIndex, int RowStart, int RowSize);
