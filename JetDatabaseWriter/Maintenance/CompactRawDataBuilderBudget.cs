namespace JetDatabaseWriter.Maintenance;

/// <summary>Limits for packing one table's raw rows into private data pages.</summary>
internal sealed class CompactRawDataBuilderBudget
{
    /// <summary>Gets the maximum staged live rows.</summary>
    public long MaxRows { get; init; } = 8_388_608;

    /// <summary>Gets the maximum appended data pages.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum appended destination bytes.</summary>
    public long MaxOutputBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum bytes written to the fixed-width bookmark ledger.</summary>
    public long MaxRelocationBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>Gets the maximum admitted builder storage, held simultaneously with discovery storage.</summary>
    /// <remarks>Caller streams, runtime overhead and shared-pool retention are separate from this admission limit.</remarks>
    public long MaxWorkingBytes { get; init; } = 128 * 1024;
}
