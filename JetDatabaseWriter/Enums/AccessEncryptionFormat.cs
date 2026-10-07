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

    /// <summary>Native Jet4 RC4 page encryption: the unmasked four-byte encoding key XOR the little-endian page number. Password protection uses the creation-date-masked UTF-16 header field independently of page encryption.</summary>
    Jet4Rc4 = 1,

    /// <summary>
    /// The library's ACCDB password-only protection, on every ACE version. Password
    /// XOR-verified in the header password area at <c>0x42</c>, flag <c>0x07</c>
    /// in raw header byte <c>0x62</c>, detected, as for <see cref="Jet4Rc4"/>,
    /// only when the area holds a password; pages are not encrypted. No file
    /// Access wrote is known to use it: its password mask was fitted to a
    /// fixture that holds no password.
    /// </summary>
    AccdbLegacyPassword = 2,

    /// <summary>
    /// Access-native ECMA-376 "Agile" encryption used by Access 2010 SP1+
    /// and Microsoft 365 (<c>.accdb</c>). The <c>EncryptionInfo</c> descriptor
    /// is embedded in page 0 and data pages are encrypted in place. This is the
    /// default <c>EncryptAsync</c> target for new encrypted ACCDB output.
    /// <c>AccessWriter.OpenAsync</c> rejects files in this format with
    /// <see cref="System.NotSupportedException"/>; use <see cref="AccdbAgileCfb"/>
    /// for encrypted files the writer must open.
    /// </summary>
    AccdbAgile = 3,

    /// <summary>
    /// Office 2007 (ECMA-376) "Standard" encryption used by Access 2007
    /// (<c>.accdb</c>). The file is a real OLE Compound File containing
    /// <c>EncryptionInfo</c> (binary descriptor: SHA-1 PBKDF, AES-128-CBC)
    /// and <c>EncryptedPackage</c> (AES-128-CBC with zero IV of the inner
    /// ACCDB).
    /// </summary>
    AccdbStandard = 4,

    /// <summary>
    /// Office Crypto API ECMA-376 "Agile" encryption in a CFB v4 compound
    /// document. The outer OLE container stores <c>EncryptionInfo</c> and
    /// <c>EncryptedPackage</c> streams; the encrypted package contains the
    /// inner clean ACCDB image.
    /// </summary>
    AccdbAgileCfb = 5,
}
