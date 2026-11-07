namespace JetDatabaseWriter.Maintenance;

/// <summary>
/// Result of packing rebuilt table pages into scrubbed interior prefix holes.
/// </summary>
/// <param name="PackedPages">The number of interior prefix holes filled with rebuilt table pages.</param>
/// <param name="RemainingScrubbedPages">The number of scrubbed interior prefix pages remaining free.</param>
/// <param name="DestinationPages">The final destination page count after hole packing.</param>
/// <param name="ReadBytes">Total bytes read during hole packing and validation.</param>
/// <param name="WrittenBytes">Total bytes written during hole packing.</param>
internal sealed record CompactPrefixHolePackResult(
    int PackedPages,
    int RemainingScrubbedPages,
    long DestinationPages,
    long ReadBytes,
    long WrittenBytes);
