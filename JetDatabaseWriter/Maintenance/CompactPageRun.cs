namespace JetDatabaseWriter.Maintenance;

/// <summary>A contiguous run of already reserved private destination pages.</summary>
/// <param name="FirstPageNumber">The first included page.</param>
/// <param name="PageCount">The positive number of included pages.</param>
internal readonly record struct CompactPageRun(long FirstPageNumber, long PageCount);
