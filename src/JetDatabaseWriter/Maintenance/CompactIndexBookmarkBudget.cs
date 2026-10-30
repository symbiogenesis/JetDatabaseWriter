namespace JetDatabaseWriter.Maintenance;

/// <summary>Hard limits for one root's repeated bookmark-target inspections, separate from its index-tree limits.</summary>
internal sealed record CompactIndexBookmarkBudget
{
    /// <summary>Gets the optional whole-session accounting shared with the index scan and page source.</summary>
    internal CompactDiscoveryAccounting? Accounting { get; init; }

    /// <summary>Gets the total target-page read allowance, including repeated bookmarks and overflow hops.</summary>
    public long MaxPageReads { get; init; } = 8_388_608;

    /// <summary>Gets the total target-directory slot allowance, including repeated inspections.</summary>
    public long MaxDirectorySlots { get; init; } = 268_435_456;

    /// <summary>Gets the maximum followed overflow pointers for one bookmark.</summary>
    public int MaxOverflowHops { get; init; } = Constants.DataPage.MaxOverflowHops;

    /// <summary>Gets the conservative bookmark-verifier storage admission limit, additional to the simultaneous raw index scan's reservation.</summary>
    /// <remarks>Runtime overhead, shared-pool retention and visitor retention remain separate.</remarks>
    public long MaxWorkingBytes { get; init; } = 16 * 1024 * 1024;
}
