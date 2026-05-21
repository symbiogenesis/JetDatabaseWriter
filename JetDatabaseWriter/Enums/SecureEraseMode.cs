namespace JetDatabaseWriter.Enums;

/// <summary>
/// Controls whether destructive writer operations overwrite deleted payloads
/// before making their storage reusable.
/// </summary>
public enum SecureEraseMode
{
    /// <summary>
    /// Preserve historical JET behavior: deletes are logical and freed pages may
    /// retain their previous bytes until reused.
    /// </summary>
    None = 0,

    /// <summary>
    /// Overwrite deleted row bodies and freed page payloads before marking them
    /// reusable in the global page free list.
    /// </summary>
    DeletedRowsAndFreedPages = 1,
}
