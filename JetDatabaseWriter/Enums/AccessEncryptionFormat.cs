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
    /// Access-native ECMA-376 "Agile" encryption used by Access 2010 SP1+
    /// and Microsoft 365 (<c>.accdb</c>). The <c>EncryptionInfo</c> descriptor
    /// is embedded in page 0 and data pages are encrypted in place. This is the
    /// native provider used by Microsoft Access.
    /// Existing native encrypted databases support reads and page updates with their original password.
    /// </summary>
    AccdbAgile = 2,

    /// <summary>Native Jet3 RC4 page encryption using its unmasked encoding key. Its single-byte header password is independent of page encryption.</summary>
    Jet3Rc4 = 3,
}
