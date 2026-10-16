namespace JetDatabaseWriter.LongValues.Models;

using JetDatabaseWriter.Pages.Models;

/// <summary>A native TDEF trailer record naming one column's owned and free LVAL maps.</summary>
/// <param name="ColumnNumber">The row-layout column number, independent of its native identity.</param>
/// <param name="OwnedOffset">The owned pointer's logical TDEF byte offset.</param>
/// <param name="FreeOffset">The free pointer's logical TDEF byte offset.</param>
/// <param name="Owned">The owned-pages map row.</param>
/// <param name="Free">The free-space map row.</param>
internal readonly record struct LongValueUsageMap(int ColumnNumber, int OwnedOffset, int FreeOffset, UsageMapPointer Owned, UsageMapPointer Free);
