namespace JetDatabaseWriter.Maintenance;

/// <summary>Complete private raw-table and map comparison evidence, excluding installed TDEFs and indexes.</summary>
/// <param name="RawValues">The independent raw bytes and original-column ledger comparison.</param>
/// <param name="Pages">The completely covered contiguous data, LVAL and map allocation pages.</param>
/// <param name="Maps">The compared table and per-column maps.</param>
/// <param name="ReadBytes">All requested destination and ledger bytes, including retries.</param>
internal sealed record CompactRawTableValidatorResult(CompactRawTableAllocationValidatorResult RawValues, long Pages, long Maps, long ReadBytes)
{
    /// <summary>Gets a value indicating whether the complete database is validated; always false.</summary>
    public bool DatabaseValidated { get; }
}
