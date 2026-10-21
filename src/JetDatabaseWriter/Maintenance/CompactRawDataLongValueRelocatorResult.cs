namespace JetDatabaseWriter.Maintenance;

/// <summary>Completed private raw-row descriptor retargeting evidence for one known table.</summary>
/// <param name="FirstPageNumber">The destination EOF before appending the copied long values.</param>
/// <param name="PageCount">The appended LVAL-page count.</param>
/// <param name="DataRows">The source rows compared against their mapped destination bytes.</param>
/// <param name="Values">The assigned raw long values completely copied or retained inline.</param>
/// <param name="LongValueRows">The copied external rows, including trailing storage.</param>
/// <param name="RawBytes">The complete columns and copied external-row bytes.</param>
/// <param name="DestinationReadBytes">The admitted destination read requests, including retries.</param>
/// <param name="PointerBytes">The four-byte descriptor-pointer writes.</param>
internal sealed record CompactRawDataLongValueRelocatorResult(long FirstPageNumber, long PageCount, long DataRows, long Values, long LongValueRows, long RawBytes, long DestinationReadBytes, long PointerBytes)
{
    /// <summary>Gets a value indicating whether the complete destination database has been validated; always false.</summary>
    public bool IsValidated { get; }

    /// <summary>Gets a value indicating whether incoming references and exclusive ownership have been proven; always false.</summary>
    public bool OwnershipProven { get; }
}
