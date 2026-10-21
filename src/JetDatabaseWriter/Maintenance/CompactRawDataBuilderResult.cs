namespace JetDatabaseWriter.Maintenance;

/// <summary>One private data allocation and its complete, source-bookmark-ordered relocation ledger.</summary>
/// <param name="FirstPageNumber">The destination EOF before packing; no page is appended for an empty table.</param>
/// <param name="PageCount">The appended data-page count.</param>
/// <param name="RowCount">The packed live-row count.</param>
/// <param name="RelocationBytes">The ledger length, eight bytes per original-header/destination bookmark pair.</param>
internal sealed record CompactRawDataBuilderResult(long FirstPageNumber, long PageCount, long RowCount, long RelocationBytes)
{
    /// <summary>Gets a value indicating whether the destination database has been validated; always false.</summary>
    public bool IsValidated { get; }
}
