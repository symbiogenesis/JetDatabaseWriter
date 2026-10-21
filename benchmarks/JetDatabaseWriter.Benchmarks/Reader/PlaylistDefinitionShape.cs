namespace JetDatabaseWriter.Benchmarks.Reader;

/// <summary>Definition distribution in a retained playlist snapshot.</summary>
public enum PlaylistDefinitionShape
{
    /// <summary>Mostly null definitions, including occasional sort-only playlists.</summary>
    Sparse = 0,

    /// <summary>Every playlist retains a 4 KiB sort definition; every seventh also retains a filter.</summary>
    LargeSortOnly = 1,
}
