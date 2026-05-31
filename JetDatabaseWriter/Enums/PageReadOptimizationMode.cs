namespace JetDatabaseWriter.Enums;

/// <summary>
/// Controls reader page-I/O optimizations such as random-access page reads and
/// table-scan read-ahead.
/// </summary>
public enum PageReadOptimizationMode
{
    /// <summary>
    /// Let the reader enable page-I/O optimizations when its structural safety
    /// checks indicate they are appropriate.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Disable random-access page reads and table-scan read-ahead.
    /// </summary>
    Disabled = 1,

    /// <summary>
    /// Allow random-access page reads and table-scan read-ahead whenever the
    /// reader's safety checks permit them.
    /// </summary>
    Enabled = 2,
}
