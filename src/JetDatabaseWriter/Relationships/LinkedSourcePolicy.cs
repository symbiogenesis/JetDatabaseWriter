namespace JetDatabaseWriter.Relationships;

/// <summary>Shared reader configuration and runtime context, with the logical host path anchoring relative source hints.</summary>
/// <param name="Context">The reader's explicit opening context, inherited by linked sources.</param>
/// <param name="HostDatabasePath">The host database path, or empty when the host was opened from a stream.</param>
internal sealed record LinkedSourcePolicy(ReaderOpenContext Context, string HostDatabasePath)
{
    /// <summary>Gets the host's unchanged configuration, including its linked-source authorization policy.</summary>
    internal AccessReaderOptions Options => this.Context.Options;
}
