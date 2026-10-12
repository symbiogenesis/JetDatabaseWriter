namespace JetDatabaseWriter.Enums;

/// <summary>
/// Identifies the recognized JET or ACE database generation.
/// Generation recognition does not imply complete coverage of its feature variants.
/// Pre-Jet3 generations are unsupported.
/// </summary>
public enum DatabaseFormat
{
    /// <summary>
    /// Jet3 format (.mdb), used by Access 97.
    /// Uses 2048-byte pages and ANSI text encoding.
    /// </summary>
    Jet3Mdb = 0,

    /// <summary>
    /// Jet4 format (.mdb), introduced with Access 2000.
    /// Uses 4096-byte pages and UCS-2 text encoding.
    /// </summary>
    Jet4Mdb = 1,

    /// <summary>
    /// ACE format (.accdb), introduced with Access 2007.
    /// Uses 4096-byte pages and UCS-2 text encoding.
    /// </summary>
    AceAccdb = 2,
}
