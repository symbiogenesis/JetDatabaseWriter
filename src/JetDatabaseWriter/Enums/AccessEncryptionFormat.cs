namespace JetDatabaseWriter.Enums;

/// <summary>
/// Identifies the on-disk encryption layout of a JET / ACE database.
/// Returned by <see cref="AccessDatabaseEncryption.DetectEncryptionFormatAsync(string, System.Threading.CancellationToken)"/>
/// for inspection. Existing encrypted databases retain their native provider
/// during page updates. Creation defaults to native JET RC4 or ACE Agile AES.
/// Maintenance preserves JET password-only mode when changing its password;
/// ACE password changes use fresh Agile AES encryption. Third-party extensible
/// providers and non-AES Agile ciphers are unsupported.
/// </summary>
public enum AccessEncryptionFormat
{
    /// <summary>The database has no page encryption; JET header password protection can still be present.</summary>
    None = 0,

    /// <summary>Native Jet4 RC4 page encryption: the unmasked four-byte encoding key XOR the little-endian page number. Password protection uses the creation-date-masked UTF-16 header field independently of page encryption.</summary>
    Jet4Rc4 = 1,

    /// <summary>
    /// Access-native ECMA-376 Agile page encryption (<c>.accdb</c>), with its
    /// descriptor embedded in page 0. Reads and updates support AES-128/192/256,
    /// CBC/CFB8 and SHA-1/256/384/512; non-AES ciphers remain unsupported.
    /// New ACE encryption uses AES-256-CBC/SHA-512. Supported native fixtures
    /// establish the tested combinations, not every historical provider variant.
    /// </summary>
    AccdbAgile = 2,

    /// <summary>Native Jet3 RC4 page encryption using its unmasked encoding key. Its single-byte header password is independent of page encryption.</summary>
    Jet3Rc4 = 3,

    /// <summary>Access-native RC4 CryptoAPI encryption with an embedded binary provider descriptor.</summary>
    AccdbRc4CryptoApi = 4,

    /// <summary>Access-native ECMA-376 Standard AES page encryption with an embedded binary provider descriptor.</summary>
    AccdbStandard = 5,
}
