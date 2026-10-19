namespace JetDatabaseWriter.Maintenance;

/// <summary>Aggregate limits for independently validating one private packed table and its copied raw LVAL storage.</summary>
internal sealed class CompactRawDataLongValueValidatorBudget
{
    /// <summary>Gets the maximum compared original data rows.</summary>
    public long MaxDataRows { get; init; } = 8_388_608;

    /// <summary>Gets the maximum assigned long values, including inline and empty external values.</summary>
    public long MaxValues { get; init; } = 8_388_608;

    /// <summary>Gets the maximum compared external rows, including trailing storage.</summary>
    public long MaxLongValueRows { get; init; } = 8_388_608;

    /// <summary>Gets the maximum combined declared data and LVAL allocation pages.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum complete long-value columns and external rows compared.</summary>
    public long MaxRawBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum destination and ledger read requests, including partial-read retries.</summary>
    public long MaxReadBytes { get; init; } = 32L * 1024 * 1024 * 1024;

    /// <summary>Gets the fixed pass storage allowance, reserved simultaneously with source row and LVAL scans.</summary>
    /// <remarks>Caller streams, definitions, projections, runtime overhead and shared-pool retention are separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 128 * 1024;
}
