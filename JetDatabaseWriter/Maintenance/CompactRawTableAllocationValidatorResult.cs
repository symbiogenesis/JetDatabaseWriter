namespace JetDatabaseWriter.Maintenance;

/// <summary>Independent raw allocation and original-column correspondence evidence.</summary>
/// <param name="RawValues">The complete raw data/LVAL comparison evidence.</param>
/// <param name="AllocationRecords">The completely covered per-column allocation ledger records.</param>
/// <param name="AllocationReadBytes">The separately admitted allocation-ledger read requests.</param>
internal sealed record CompactRawTableAllocationValidatorResult(CompactRawDataLongValueValidatorResult RawValues, long AllocationRecords, long AllocationReadBytes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }
}
