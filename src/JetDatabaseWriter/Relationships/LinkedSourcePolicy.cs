namespace JetDatabaseWriter.Relationships;

/// <summary>
/// How a reader may resolve and open the sources its linked tables point at:
/// the options linked Access sources are opened with (which carry the
/// authorized-stream resolver, so transitively linked databases inherit the
/// same policy), and the host database path used only to anchor relative hints.
/// </summary>
/// <param name="OpenOptions">The options used to open linked sources.</param>
/// <param name="HostDatabasePath">The host database path, or empty when the host was opened from a stream.</param>
internal sealed record LinkedSourcePolicy(AccessReaderOptions OpenOptions, string HostDatabasePath);
