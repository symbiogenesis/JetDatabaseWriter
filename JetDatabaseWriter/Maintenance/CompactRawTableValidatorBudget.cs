namespace JetDatabaseWriter.Maintenance;

/// <summary>Aggregate admission for comparing a private raw table and every native map.</summary>
internal sealed class CompactRawTableValidatorBudget
{
    /// <summary>Gets the independent raw row, field and LVAL comparison limits.</summary>
    public CompactRawDataLongValueValidatorBudget RawValues { get; init; } = new();

    /// <summary>Gets map limits shared cumulatively across the table and every column.</summary>
    public CompactUsageMapValidatorBudget Maps { get; init; } = new();

    /// <summary>Gets cumulative destination and both ledger read requests, including partial retries.</summary>
    public long MaxReadBytes { get; init; } = 32L * 1024 * 1024 * 1024;

    /// <summary>Gets simultaneous additional parser, image, buffer and nested comparison storage admission.</summary>
    /// <remarks>Caller inputs, runtime overhead and shared pools are separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 1024 * 1024;
}
