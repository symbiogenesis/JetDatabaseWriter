namespace JetDatabaseWriter.Maintenance;

/// <summary>Aggregate private allocation and I/O limits for composing one raw table and its native maps.</summary>
internal sealed record CompactRawTableBuilderBudget
{
    /// <summary>Gets the maximum packed original rows.</summary>
    public long MaxDataRows { get; init; } = 8_388_608;

    /// <summary>Gets the maximum assigned long values, including inline descriptors.</summary>
    public long MaxValues { get; init; } = 8_388_608;

    /// <summary>Gets the maximum copied external rows, including trailing storage.</summary>
    public long MaxLongValueRows { get; init; } = 8_388_608;

    /// <summary>Gets the maximum appended pages across data, LVAL and every map allocation.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum appended destination bytes across all staging phases.</summary>
    public long MaxOutputBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum complete row bookmark ledger bytes.</summary>
    public long MaxRowLedgerBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>Gets the maximum twelve-byte column/allocation records written to the disk-capable LVAL ledger.</summary>
    public long MaxAllocationLedgerBytes { get; init; } = 96L * 1024 * 1024;

    /// <summary>Gets the maximum copied complete long-value column and external-row bytes.</summary>
    public long MaxRawBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the cumulative private destination and ledger read admission, including partial retries.</summary>
    public long MaxReadBytes { get; init; } = 32L * 1024 * 1024 * 1024;

    /// <summary>Gets the additional assembly-owned image, parser, retained per-column result and fixed-buffer storage allowance.</summary>
    /// <remarks>Component reservations also consume the discovery session's simultaneous storage allowance.</remarks>
    public long MaxWorkingBytes { get; init; } = 1024 * 1024;
}
