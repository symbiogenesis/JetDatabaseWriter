namespace JetDatabaseWriter.Enums;

/// <summary>
/// Identifies the on-disk encryption layout of a JET / ACE database.
/// Used by <see cref="AccessWriter.EncryptAsync(string, System.ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, System.Threading.CancellationToken)"/>
/// to choose which scheme to apply when encrypting a previously-unencrypted
/// file, and returned by <see cref="AccessWriter.DetectEncryptionFormatAsync(string, System.Threading.CancellationToken)"/>
/// for inspection.
/// </summary>
public enum AccessEncryptionFormat
{
    /// <summary>The database is unencrypted.</summary>
    None = 0,

    /// <summary>
    /// Jet4 RC4 page encryption (Access 2000 – 2003 <c>.mdb</c>). Password
    /// XOR-verified in the header password area at <c>0x42</c>, encryption flag
    /// <c>0x02</c> or <c>0x03</c> in raw header byte <c>0x62</c>, RC4 database
    /// key at <c>0x3E</c>, per-page RC4 with key <c>MD5(dbKey ‖ pageNumber)[..4]</c>.
    /// Byte <c>0x62</c> lies inside the password area, which Access masks and,
    /// on a file without a password, fills with a pattern derived from the
    /// creation date, so the flag is detected only when the area holds a password.
    /// </summary>
    Jet4Rc4 = 1,

    /// <summary>
    /// ACCDB legacy password-only protection, on every ACE version. Password
    /// XOR-verified in the header password area at <c>0x42</c>, flag <c>0x07</c>
    /// in raw header byte <c>0x62</c>, detected, as for <see cref="Jet4Rc4"/>,
    /// only when the area holds a password; pages are not encrypted.
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
    /// Access-native ECMA-376 "Agile" encryption used by Access 2010 SP1+
    /// and Microsoft 365 (<c>.accdb</c>). The <c>EncryptionInfo</c> descriptor
    /// is embedded in page 0 and data pages are encrypted in place. This is the
    /// default <c>EncryptAsync</c> target for new encrypted ACCDB output.
    /// <c>AccessWriter.OpenAsync</c> rejects files in this format with
    /// <see cref="System.NotSupportedException"/>; use <see cref="AccdbAgileCfb"/>
    /// for encrypted files the writer must open.
    /// </summary>
    AccdbAgile = 4,

    /// <summary>
    /// Office 2007 (ECMA-376) "Standard" encryption used by Access 2007
    /// (<c>.accdb</c>). The file is a real OLE Compound File containing
    /// <c>EncryptionInfo</c> (binary descriptor: SHA-1 PBKDF, AES-128-CBC)
    /// and <c>EncryptedPackage</c> (AES-128-CBC with zero IV of the inner
    /// ACCDB).
    /// </summary>
    AccdbStandard = 5,

    /// <summary>
    /// Office Crypto API ECMA-376 "Agile" encryption in a CFB v4 compound
    /// document. The outer OLE container stores <c>EncryptionInfo</c> and
    /// <c>EncryptedPackage</c> streams; the encrypted package contains the
    /// inner clean ACCDB image.
    /// </summary>
    AccdbAgileCfb = 6,
}
