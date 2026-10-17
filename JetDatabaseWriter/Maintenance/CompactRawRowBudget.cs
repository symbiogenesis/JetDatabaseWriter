namespace JetDatabaseWriter.Maintenance;

/// <summary>Admission and traversal limits for one table's raw physical row scan.</summary>
internal sealed class CompactRawRowBudget
{
    /// <summary>Gets the maximum source size in physical pages, checked before reading.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the total read allowance, including repeated overflow and physical reads.</summary>
    public long MaxPageReads { get; init; } = 8_388_608;

    /// <summary>Gets the maximum number of live rows delivered to the visitor.</summary>
    public long MaxRows { get; init; } = 8_388_608;

    /// <summary>Gets the total directory slots inspected, including deleted, empty and repeatedly read slots.</summary>
    public long MaxDirectorySlots { get; init; } = 268_435_456;

    /// <summary>Gets the conservative allowance for fixed scan storage, excluding caller storage and pool retention.</summary>
    /// <remarks>This is admission accounting, not an exact runtime heap measurement.</remarks>
    public long MaxWorkingBytes { get; init; } = 1024 * 1024;

    /// <summary>Gets the maximum overflow-pointer reads for one row.</summary>
    public int MaxOverflowHops { get; init; } = Constants.DataPage.MaxOverflowHops;
}
