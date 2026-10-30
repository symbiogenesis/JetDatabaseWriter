namespace JetDatabaseWriter.Maintenance;

/// <summary>Aggregate limits for retargeting one packed table's assigned raw long values.</summary>
internal sealed record CompactRawDataLongValueRelocatorBudget
{
    /// <summary>Gets the maximum original data rows compared with their packed destinations.</summary>
    public long MaxDataRows { get; init; } = 8_388_608;

    /// <summary>Gets the maximum assigned long values, including inline and empty external descriptors.</summary>
    public long MaxValues { get; init; } = 8_388_608;

    /// <summary>Gets the maximum copied external long-value rows, including trailing storage.</summary>
    public long MaxLongValueRows { get; init; } = 8_388_608;

    /// <summary>Gets the maximum appended private LVAL pages across all values.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum appended private destination bytes.</summary>
    public long MaxOutputBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum complete columns and referenced raw rows across all values.</summary>
    public long MaxRawBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum destination read requests, including partial retries and failed attempts.</summary>
    public long MaxDestinationReadBytes { get; init; } = 32L * 1024 * 1024 * 1024;

    /// <summary>Gets the maximum four-byte descriptor-pointer writes.</summary>
    public long MaxPointerBytes { get; init; } = 32L * 1024 * 1024;

    /// <summary>Gets the maximum admitted pass storage, held simultaneously with source and value scans.</summary>
    /// <remarks>Caller streams, definition/projection storage, runtime overhead and shared-pool retention are separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 128 * 1024;

    /// <summary>Gets the separate per-value builder storage allowance, also reserved against session accounting.</summary>
    public long MaxValueWorkingBytes { get; init; } = 128 * 1024;
}
