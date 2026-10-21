namespace JetDatabaseWriter.Enums;

/// <summary>
/// Controls whether marking a data-page row deleted also scrubs that row's
/// stored payload bytes, in addition to flipping its deleted flag.
/// </summary>
internal enum DeletedRowDataMode
{
    /// <summary>
    /// Flip only the deleted flag. The row's payload bytes are scrubbed solely
    /// when the writer's secure-erase option is
    /// <see cref="SecureEraseMode.DeletedRowsAndFreedPages"/>.
    /// </summary>
    Default = 0,

    /// <summary>
    /// Always scrub (zero) the deleted row's payload bytes, regardless of the
    /// writer's secure-erase option. Used for internal structural rows (catalog
    /// and complex-column backing tables) that must not retain stale bytes after
    /// deletion.
    /// </summary>
    Clear = 1,
}
