namespace JetDatabaseReader;

/// <summary>
/// Specifies the JET database format to use when creating a new database.
/// </summary>
public enum JetDatabaseFormat
{
    /// <summary>
    /// Jet4 format (.mdb) — compatible with Access 2000–2003.
    /// Uses 4096-byte pages and UCS-2 text encoding.
    /// </summary>
    Jet4Mdb = 0,

    /// <summary>
    /// ACE format (.accdb) — compatible with Access 2007 and later.
    /// Uses 4096-byte pages and UCS-2 text encoding.
    /// </summary>
    AceAccdb = 1,
}
