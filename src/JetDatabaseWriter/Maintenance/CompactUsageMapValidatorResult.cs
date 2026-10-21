namespace JetDatabaseWriter.Maintenance;

/// <summary>Exact membership and complete private map-allocation comparison evidence.</summary>
/// <param name="Pages">The compared row and bitmap pages.</param>
/// <param name="ListedPages">The compared members.</param>
/// <param name="InputRuns">The compared expected runs.</param>
/// <param name="ReadBytes">The admitted read requests including retries.</param>
/// <param name="BitmapBytes">The inspected bitmap bytes including zero bytes.</param>
internal sealed record CompactUsageMapValidatorResult(long Pages, long ListedPages, long InputRuns, long ReadBytes, long BitmapBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }
}
