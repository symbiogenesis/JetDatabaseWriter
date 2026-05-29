namespace JetDatabaseWriter.Enums;

using JetDatabaseWriter.Indexes;

/// <summary>
/// Outcome of <see cref="IndexMaintainer.ReadTdefPreambleAsync"/>'s parse.
/// </summary>
internal enum TdefPreambleStatus
{
    /// <summary>Header parsed successfully; index work should proceed.</summary>
    Ok = 0,

    /// <summary>The TDEF declares no logical or real indexes; there is nothing to maintain.</summary>
    Empty = 1,

    /// <summary>numIdx or numRealIdx exceeded the sanity cap (corrupt header suspected).</summary>
    TooMany = 2,

    /// <summary>The column-name walk failed before reaching the real-idx descriptor block.</summary>
    ColumnNameWalkFailed = 3,
}
