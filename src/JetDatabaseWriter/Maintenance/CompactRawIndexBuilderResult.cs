namespace JetDatabaseWriter.Maintenance;

/// <summary>Describes a completed private tree build, without authorizing publication or reclamation.</summary>
/// <param name="RootPageNumber">The appended root page.</param>
/// <param name="FirstPageNumber">The first appended page and first leaf.</param>
/// <param name="LastLeafPageNumber">The last appended leaf.</param>
/// <param name="PageCount">The appended leaf and intermediate page count.</param>
/// <param name="EntryCount">The consumed leaf entry count.</param>
/// <param name="Levels">The tree height, including leaves.</param>
/// <param name="FenceBytes">The aggregate fence bytes read and written.</param>
internal sealed record CompactRawIndexBuilderResult(long RootPageNumber, long FirstPageNumber, long LastLeafPageNumber, long PageCount, long EntryCount, int Levels, long FenceBytes)
{
    /// <summary>Gets a value indicating whether the destination database has been validated; always false.</summary>
    public bool IsValidated { get; }
}
