namespace JetDatabaseWriter.Maintenance;

/// <summary>Independent comparison and complete disjoint coverage of one private raw data/LVAL allocation.</summary>
/// <param name="DataRows">The original rows and exact complete row-ledger records compared.</param>
/// <param name="Values">The assigned long values independently audited and compared.</param>
/// <param name="LongValueRows">The copied external rows compared, including trailing storage.</param>
/// <param name="LongValuePages">The private LVAL pages completely covered without shared slots.</param>
/// <param name="RawBytes">The complete long-value columns and external-row bytes compared.</param>
/// <param name="ReadBytes">The admitted destination and ledger read requests, including retries.</param>
internal sealed record CompactRawDataLongValueValidatorResult(long DataRows, long Values, long LongValueRows, long LongValuePages, long RawBytes, long ReadBytes)
{
    /// <summary>Gets a value indicating whether the complete destination database has been validated; always false.</summary>
    public bool IsValidated { get; }

    /// <summary>Gets a value indicating whether global incoming references and source ownership have been proven; always false.</summary>
    public bool OwnershipProven { get; }
}
