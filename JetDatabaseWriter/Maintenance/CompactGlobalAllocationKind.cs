namespace JetDatabaseWriter.Maintenance;

/// <summary>The retained or newly staged role of a private destination allocation.</summary>
internal enum CompactGlobalAllocationKind
{
    /// <summary>The entire immutable source prefix, including opaque and possibly unused storage.</summary>
    PinnedSource = 0,

    /// <summary>Appended raw table or catalog data pages.</summary>
    Data = 1,

    /// <summary>Appended external long-value pages.</summary>
    LongValue = 2,

    /// <summary>Appended stored-key index pages.</summary>
    Index = 3,

    /// <summary>Appended map rows and their reference bitmap storage, including global map storage.</summary>
    UsageMap = 4,

    /// <summary>Appended table-definition chain storage.</summary>
    TableDefinition = 5,
}
