namespace JetDatabaseWriter.Maintenance;

/// <summary>Lifetime limits for one source-bound active-table observation, including repeated consumption.</summary>
internal sealed class CompactActiveTableBudget
{
    /// <summary>Gets optional surrounding discovery admission; child attempts are admitted before parent work.</summary>
    internal CompactDiscoveryAccounting? Accounting { get; init; }

    /// <summary>Gets cumulative work and simultaneous storage admission for the entire evidence lifetime.</summary>
    public CompactDiscoveryBudget Work { get; init; } = new();

    /// <summary>Gets the maximum independently covered active rows.</summary>
    public long MaxActiveRows { get; init; } = 1_048_576;

    /// <summary>Gets the maximum complete derived scalar key length.</summary>
    public int MaxKeyBytes { get; init; } = 65_536;
}
