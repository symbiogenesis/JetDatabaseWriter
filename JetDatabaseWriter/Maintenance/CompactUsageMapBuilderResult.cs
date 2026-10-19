namespace JetDatabaseWriter.Maintenance;

using JetDatabaseWriter.Pages.Models;

/// <summary>Completed private map storage and membership counts, requiring independent database validation.</summary>
/// <param name="Pointer">The native row pointer for a TDEF table, index or LVAL map reference.</param>
/// <param name="FirstPageNumber">The first appended page, containing the map row.</param>
/// <param name="PageCount">The appended row page and reference bitmap page count.</param>
/// <param name="ListedPages">The exact number of included pages.</param>
/// <param name="InputRuns">The consumed allocation run count.</param>
internal sealed record CompactUsageMapBuilderResult(UsageMapPointer Pointer, long FirstPageNumber, long PageCount, long ListedPages, long InputRuns)
{
    /// <summary>Gets a value indicating whether the complete destination database has been validated; always false.</summary>
    public bool IsValidated { get; }
}
