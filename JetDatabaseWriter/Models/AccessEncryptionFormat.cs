namespace JetDatabaseWriter;

/// <summary>
/// Identifies the on-disk encryption layout of a JET / ACE database.
/// Used by <see cref="AccessWriter.EncryptAsync(string, string, AccessEncryptionFormat, AccessWriterOptions?, System.Threading.CancellationToken)"/>
/// to choose which scheme to apply when encrypting a previously-unencrypted
/// file, and returned by <see cref="AccessWriter.DetectEncryptionFormatAsync(string, System.Threading.CancellationToken)"/>
/// for inspection.
/// </summary>
public enum AccessEncryptionFormat
{
    /// <summary>The database is unencrypted.</summary>
    None = 0,

    /// <summary>
    /// Jet4 RC4 page encryption (Access 2000 – 2003 <c>.mdb</c>). Encryption flag
    /// <c>0x02</c> at header offset <c>0x62</c>, password XOR-verified at
    /// <c>0x42</c>, RC4 database key at <c>0x3E</c>, per-page RC4 with key
    /// <c>MD5(dbKey ‖ pageNumber)[..4]</c>.
    /// </summary>
    Jet4Rc4 = 1,

    /// <summary>
    /// ACCDB legacy password-only protection (<c>;pwd=...</c> introduced by
    /// <c>DBEngine.CompactDatabase</c>). Encryption flag <c>0x07</c> at header
    /// offset <c>0x62</c>; password XOR-verified at <c>0x42</c>; pages are not
    /// encrypted.
    /// </summary>
    AccdbLegacyPassword = 2,

    /// <summary>
    /// Synthetic legacy AES-128 layout used by Access 2007 <c>.accdb</c> files
    /// that have a CFB magic prefix (<c>D0 CF 11 E0 …</c>) but a flat per-page
    /// AES-128-ECB body beneath. Key is <c>SHA-256(password)[..16]</c>; password
    /// is XOR-verified at <c>0x42</c> using the Jet4 mask.
    /// </summary>
    AccdbAesCfbWrapped = 3,

    /// <summary>
    /// Office Crypto API ECMA-376 "Agile" encryption used by Access 2010 SP1+
    /// and Microsoft 365 (<c>.accdb</c>). The file is a real OLE Compound File
    /// containing <c>EncryptionInfo</c> (XML descriptor: SHA-512 PBKDF, AES-CBC)
    /// and <c>EncryptedPackage</c> (4096-byte segmented AES-CBC of the inner
    /// ACCDB).
    /// </summary>
    AccdbAgile = 4,
}
