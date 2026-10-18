namespace JetDatabaseWriter.Maintenance;

/// <summary>Whole-session limits shared by inventory, raw table, index and long-value discovery.</summary>
/// <remarks>Working storage is conservative admission accounting. Runtime overhead, shared-pool retention and caller-owned buffers, streams and retained observations are separate.</remarks>
internal sealed class CompactDiscoveryBudget
{
    /// <summary>Gets the maximum source physical page count.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the total physical read allowance, including repeated reads across objects.</summary>
    public long MaxPageReads { get; init; } = 8_388_608;

    /// <summary>Gets the total table and index-root invocation allowance.</summary>
    public long MaxObjects { get; init; } = 1_048_576;

    /// <summary>Gets the total catalog identities admitted before decoding, including repeated inventories and failed traversals.</summary>
    public long MaxCatalogObjects { get; init; } = 65_536;

    /// <summary>Gets the total physical definition bytes inspected by inventory metadata readers, including repeated reads.</summary>
    public long MaxSchemaBytes { get; init; } = 1024L * 1024 * 1024;

    /// <summary>Gets the maximum logical definition size admitted by each inventory reader.</summary>
    public int MaxDefinitionBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Gets the total inventory map bytes and reference-directory slots inspected, including zeroes.</summary>
    public long MaxMapSteps { get; init; } = 268_435_456;

    /// <summary>Gets the total inventory uncertainty diagnostic allowance.</summary>
    public long MaxDiagnostics { get; init; } = 1_024;

    /// <summary>Gets the total delivered live-row allowance.</summary>
    public long MaxRows { get; init; } = 8_388_608;

    /// <summary>Gets the total raw-row, inventory and long-value directory slots admitted, including repeated inspections.</summary>
    public long MaxDirectorySlots { get; init; } = 268_435_456;

    /// <summary>Gets the total long-value audit and copy invocation allowance.</summary>
    public long MaxLongValues { get; init; } = 8_388_608;

    /// <summary>Gets the total observed complete long-value column and external-row byte allowance.</summary>
    public long MaxLongValueBytes { get; init; } = 1024L * 1024 * 1024;

    /// <summary>Gets the total declared payload bytes admitted to raw long-value copies.</summary>
    public long MaxLongValuePayloadBytes { get; init; } = 1024L * 1024 * 1024;

    /// <summary>Gets the total external long-value rows visited during auditing and copying.</summary>
    public long MaxLongValueRows { get; init; } = 8_388_608;

    /// <summary>Gets the total discovered index pages across roots.</summary>
    public long MaxIndexPages { get; init; } = 1_048_576;

    /// <summary>Gets the total reconstructed index-entry allowance.</summary>
    public long MaxIndexEntries { get; init; } = 4_194_304;

    /// <summary>Gets the total reconstructed index bytes, including pointer trailers.</summary>
    public long MaxIndexCanonicalBytes { get; init; } = 1024L * 1024 * 1024;

    /// <summary>Gets the total observed inventory, overflow-row, index and long-value reference allowance.</summary>
    public long MaxReferences { get; init; } = 8_388_608;

    /// <summary>Gets the total graph-traversal step allowance.</summary>
    public long MaxGraphSteps { get; init; } = 67_108_864;

    /// <summary>Gets the total streamed inventory, row, index and long-value observation allowance.</summary>
    public long MaxRecords { get; init; } = 8_388_608;

    /// <summary>Gets the maximum simultaneous admitted working storage, including nested value copies.</summary>
    public long MaxWorkingBytes { get; init; } = 512L * 1024 * 1024;

    /// <summary>Gets the maximum one-value declared payload length.</summary>
    public int MaxValueBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Gets the maximum external-row tracking capacity for one long value.</summary>
    public int MaxChainRows { get; init; } = 1_048_576;
}
