namespace JetDatabaseWriter.Enums;

using JetDatabaseWriter.Internal;

/// <summary>
/// Outcome of <see cref="IndexMaintainer.ReadTdefPreambleAsync"/>'s parse.
/// </summary>
internal enum TdefPreambleStatus
{
    /// <summary>Header parsed successfully; index work should proceed.</summary>
    Ok,

    /// <summary>The TDEF declares no logical or real indexes; there is nothing to maintain.</summary>
    Empty,

    /// <summary>numIdx or numRealIdx exceeded the sanity cap (corrupt header suspected).</summary>
    TooMany,

    /// <summary>The column-name walk failed before reaching the real-idx descriptor block.</summary>
    ColumnNameWalkFailed,
}