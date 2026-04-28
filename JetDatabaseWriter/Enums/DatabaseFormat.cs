namespace JetDatabaseWriter.Enums;

/// <summary>
/// Specifies the JET database format.
/// </summary>
public enum DatabaseFormat
{
    /// <summary>
    /// Jet3 format (.mdb) — compatible with Access 97.
    /// Uses 2048-byte pages and ANSI text encoding.
    /// </summary>
    Jet3Mdb = 0,

    /// <summary>
    /// Jet4 format (.mdb) — compatible with Access 2000–2003.
    /// Uses 4096-byte pages and UCS-2 text encoding.
    /// </summary>
    Jet4Mdb = 1,

    /// <summary>
    /// ACE format (.accdb) — compatible with Access 2007 and later.
    /// Uses 4096-byte pages and UCS-2 text encoding.
    /// </summary>
    AceAccdb = 2,
}
