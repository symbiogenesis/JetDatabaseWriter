namespace JetDatabaseWriter.Pages.Paging;

/// <summary>Storage guarantees used when choosing page and commit strategies.</summary>
/// <param name="IsFileBacked">Whether storage is an OS file.</param>
/// <param name="PositionalReads">Whether reads bypass the shared stream position.</param>
/// <param name="DurableFlush">Whether flush can reach the device.</param>
/// <param name="AtomicCommit">Whether the store replaces its image atomically.</param>
/// <param name="ByteRangeLocks">Whether page writes acquire cooperative locks.</param>
/// <param name="InPlacePages">Whether individual pages can be overwritten.</param>
internal readonly record struct StoreCapabilities(
    bool IsFileBacked,
    bool PositionalReads,
    bool DurableFlush,
    bool AtomicCommit,
    bool ByteRangeLocks,
    bool InPlacePages);
