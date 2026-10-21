namespace JetDatabaseWriter.Maintenance;

/// <summary>Describes independently verified private allocation coverage and exact stored-entry preservation.</summary>
/// <param name="EntryCount">The original and staged leaf entry count.</param>
/// <param name="PageCount">The completely covered private page count.</param>
/// <param name="Levels">The independently observed private tree height.</param>
/// <param name="ReadBytes">The cumulative destination read admission, including partial-read retries.</param>
internal sealed record CompactRawIndexValidatorResult(long EntryCount, long PageCount, int Levels, long ReadBytes)
{
    /// <summary>Gets a value indicating whether every declared private page and stored entry passed independent comparison.</summary>
    public bool PrivateAllocationValidated { get; } = true;

    /// <summary>Gets a value indicating whether complete database validity or incoming-reference ownership was established; always false.</summary>
    public bool DatabaseValidated { get; }
}
