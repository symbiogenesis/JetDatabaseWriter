namespace JetDatabaseWriter.Maintenance;

/// <summary>Private native global-map installation or exact comparison, without database acceptance.</summary>
/// <param name="PinnedPages">The immutable original prefix retained in full.</param>
/// <param name="AppendPointPages">The independently supplied destination EOF before map installation.</param>
/// <param name="DestinationPages">The complete EOF including newly appended map storage.</param>
/// <param name="BitmapPages">The new contiguous bitmap allocation beginning at the append point, or zero.</param>
/// <param name="MapRowStart">The only permitted changed prefix row's byte offset on page one.</param>
/// <param name="MapRowSize">The original native row's exact byte length.</param>
/// <param name="ReadBytes">Admitted source and private read requests, including partial-read retries.</param>
/// <param name="WrittenBytes">Complete private page bytes written; zero for independent comparison.</param>
internal sealed record CompactGlobalMapResult(long PinnedPages, long AppendPointPages, long DestinationPages, long BitmapPages, int MapRowStart, int MapRowSize, long ReadBytes, long WrittenBytes)
{
    /// <summary>Gets a value indicating whether the whole database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether original pages may be reclaimed; always false.</summary>
    public bool ReclamationAuthorized { get; }
}
