namespace JetDatabaseWriter.Maintenance;

/// <summary>Distinct private backing-storage roles used by the append-only database coordinator.</summary>
internal enum CompactPinnedDatabaseFile
{
    /// <summary>The private destination; its owner argument is zero.</summary>
    Destination = 0,

    /// <summary>The complete private allocation ledger; its owner argument is zero.</summary>
    AllocationLedger = 1,

    /// <summary>A table's retained immutable row relocation ledger, keyed by its original owner.</summary>
    Rows = 2,

    /// <summary>A table's retained immutable long-value allocation ledger, keyed by its original owner.</summary>
    LongValues = 3,

    /// <summary>A table's disposable first index fence stream, keyed by its original owner.</summary>
    FirstFence = 4,

    /// <summary>A table's disposable second index fence stream, keyed by its original owner.</summary>
    SecondFence = 5,
}
