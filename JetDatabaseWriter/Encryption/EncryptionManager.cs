namespace JetDatabaseWriter.Encryption;

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Transactions;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Centralizes all JET / ACE / ACCDB encryption logic — header detection,
/// password verification, key derivation, and per-page decryption.
/// </summary>
internal static class EncryptionManager
{
    /// <summary>
    /// Jet3 XOR mask (128 bytes, applied cyclically to pages 1+ when the
    /// Office97 password flag is set on a Jet3 .mdb). Sourced from mdbtools
    /// HACKING.md.
    /// </summary>
    internal static readonly byte[] Jet3PageXorMask =
    [
        0xEC, 0x7B, 0x28, 0x07, 0x77, 0x26, 0x13, 0x82,
        0x75, 0x4E, 0x22, 0x04, 0x42, 0xCE, 0xB3, 0x19,
        0xA1, 0x32, 0x75, 0x46, 0xE3, 0x66, 0x27, 0x37,
        0x19, 0x9E, 0xA3, 0x56, 0x85, 0x3A, 0xD6, 0xDE,
        0xEC, 0x03, 0xE6, 0xFC, 0xF8, 0x85, 0x8F, 0xA0,
        0x1B, 0x20, 0xAD, 0xE5, 0x0E, 0x7A, 0xF7, 0x38,
        0x54, 0xFC, 0x10, 0x4E, 0x25, 0x22, 0xBD, 0xC7,
        0x5D, 0x62, 0x5E, 0x44, 0xBB, 0x6D, 0xCB, 0xB5,
        0x90, 0x14, 0xDE, 0xC5, 0xD7, 0xA5, 0x4F, 0x84,
        0xBE, 0xE5, 0x06, 0x62, 0xC5, 0xF1, 0xBB, 0xBB,
        0xE3, 0xBB, 0x4C, 0xFD, 0x38, 0x7B, 0xDA, 0x88,
        0x1F, 0x5C, 0x2E, 0x5A, 0x49, 0xEB, 0x47, 0xE2,
        0xCA, 0xAD, 0xCE, 0x73, 0xBB, 0x25, 0xF9, 0xED,
        0x47, 0x59, 0x4C, 0x42, 0xEF, 0xF0, 0xB1, 0x58,
        0x45, 0x58, 0x5D, 0xF3, 0xBC, 0x27, 0xBC, 0x60,
        0x19, 0xEB, 0xB1, 0xF9, 0x4F, 0x5D, 0xD1, 0x12,
    ];

    /// <summary>
    /// Jet4 password XOR mask (mdbtools / jackcess). Applied together with
    /// the 4-byte creation date at offset 0x72 to decode the stored password.
    /// </summary>
    internal static readonly byte[] Jet4PasswordMask =
    [
        0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
        0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
        0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
        0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
        0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
    ];

    /// <summary>
    /// ACE legacy password mask used for password-only ACCDB files
    /// created via DBEngine.CompactDatabase(..., ";pwd=...").
    /// </summary>
    internal static readonly byte[] AccdbLegacyPasswordMask =
    [
        0x1F, 0x9B, 0xB7, 0xCA, 0xD4, 0x24, 0xD0, 0x07,
        0x49, 0x3E, 0x62, 0x1B, 0xF9, 0xD6, 0xB4, 0x9D,
        0xBE, 0xF4, 0x45, 0xCB, 0x1F, 0x12, 0xE1, 0x4C,
        0x9D, 0x94, 0x2D, 0xBE, 0x25, 0xCF, 0x8F, 0xCE,
        0xDE, 0x01, 0x47, 0xA6, 0x78, 0xD5, 0x42, 0xD7,
    ];

    /// <summary>
    /// Gets a read-only view of the Jet4 password XOR mask used for encoding /
    /// decoding the 40-byte password area at header offset <c>0x42</c>.
    /// Exposed for <see cref="EncryptionConverter"/> so it can re-encode
    /// passwords when re-keying or applying encryption to a clean file.
    /// </summary>
    internal static ReadOnlySpan<byte> Jet4PasswordMaskForWrite => Jet4PasswordMask;

    /// <summary>
    /// Gets a read-only view of the ACCDB legacy password XOR mask (the one used by
    /// <c>DBEngine.CompactDatabase(..., ";pwd=...")</c>). Exposed for
    /// <see cref="EncryptionConverter"/>.
    /// </summary>
    internal static ReadOnlySpan<byte> AccdbLegacyPasswordMaskForWrite => AccdbLegacyPasswordMask;

    /// <summary>Returns true when the file begins with the OLE2 Compound File Binary magic bytes.</summary>
    /// <param name="header">The header.</param>
    public static bool IsCompoundFileEncrypted(byte[] header) =>
        header?.Length >= 4 &&
        header[0] == 0xD0 && header[1] == 0xCF && header[2] == 0x11 && header[3] == 0xE0;

    /// <summary>
    /// Returns the Jet3 page XOR mask if the header has the Jet3 Office97 password
    /// flag set (offset 0x62, bit 0x01); otherwise returns null.
    /// </summary>
    /// <param name="format">The format.</param>
    /// <param name="hdr">The database header bytes.</param>
    private static byte[]? GetJet3PageMask(DatabaseFormat format, byte[] hdr)
    {
        if (format == DatabaseFormat.Jet3Mdb && hdr.Length > 0x62 && (hdr[0x62] & 0x01) != 0)
        {
            return Jet3PageXorMask;
        }

        return null;
    }

    private const int HeaderPasswordLength = 40;

    private const int HeaderPasswordLengthPrefixLength = 4;

    private const int HeaderPasswordCharSize = 2;

    private const int HeaderPasswordNormalizedLength = HeaderPasswordLengthPrefixLength + HeaderPasswordLength;

    /// <summary>
    /// Inspects the database header for Jet3 / Jet4 / ACCDB page-encryption or
    /// password flags, verifies the supplied password where required, and returns
    /// the owned page-decryption keys for the database header and password context.
    /// </summary>
    /// <param name="header">The database header bytes.</param>
    /// <param name="format">The format.</param>
    /// <param name="isLegacyAesCfb">Whether the database uses the legacy AES CFB page-encryption path.</param>
    /// <param name="password">The password.</param>
    /// <exception cref="UnauthorizedAccessException">Thrown when the database requires a password and the supplied password is missing or incorrect.</exception>
    internal static PageDecryptionKeys CreatePageDecryptionKeys(
        byte[] header,
        DatabaseFormat format,
        bool isLegacyAesCfb,
        ReadOnlyMemory<char> password)
    {
        uint? rc4DbKey = null;
        byte[]? aesPageKey = null;

        // Offset 0x14: Jet/ACE format version byte.
        byte ver = header[0x14];

        // Jet4 .mdb (Access 2000 – 2003) — flag at 0x62 governs encryption.
        // ACCDB format (ver >= 2, Access 2007+) reuses this offset for unrelated
        // bits, so the Jet4 detection only applies to ver == 1.
        if (format == DatabaseFormat.Jet4Mdb && header.Length > 0x62)
        {
            byte encFlag = header[0x62];

            // Jet4 encryption flag values:
            //   0x01 = Office97 password only (no page encryption)
            //   0x02 = RC4 page encryption
            //   0x03 = RC4 + password
            if (encFlag is >= 0x01 and <= 0x03)
            {
                if (password.IsEmpty)
                {
                    throw new UnauthorizedAccessException(
                        "This database is encrypted or password-protected. " +
                        "Provide a password via AccessReaderOptions.Password, or " +
                        "remove the password in Microsoft Access (File > Info > Encrypt with Password) and try again.");
                }

                if (!HeaderPasswordMatches(header, Jet4PasswordMask, password.Span))
                {
                    throw new UnauthorizedAccessException(
                        "The provided password is incorrect for this database.");
                }

                if ((encFlag & 0x02) != 0)
                {
                    rc4DbKey = Ru32(header, 0x3E);
                }
            }
        }

        // ACCDB legacy password-only mode (standard ACCDB header, ver >= 3).
        // Many normal ACCDB files reuse overlapping bits at 0x62, so we only
        // enforce password verification for the known legacy-password signature
        // emitted by Access 2010+ CompactDatabase(";pwd=...") test fixtures.
        if (format == DatabaseFormat.AceAccdb && ver >= 3 && !isLegacyAesCfb && header.Length > 0x62)
        {
            byte encFlag = header[0x62];
            if (encFlag == 0x07)
            {
                if (password.IsEmpty)
                {
                    throw new UnauthorizedAccessException(
                        "This database is password-protected. " +
                        "Provide a password via AccessReaderOptions.Password.");
                }

                if (!HeaderPasswordMatches(header, AccdbLegacyPasswordMask, password.Span))
                {
                    throw new UnauthorizedAccessException(
                        "The provided password is incorrect for this database.");
                }
            }
        }

        // ACCDB genuine AES encryption (CFB-wrapped file presented as a raw
        // header by the synthetic legacy path).
        if (isLegacyAesCfb)
        {
            if (password.IsEmpty)
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Access 2007+ AES encryption. " +
                    "Provide the database password via AccessReaderOptions.Password to open it, " +
                    "or remove the password in Microsoft Access (File > Info > Decrypt Database) and try again.");
            }

            // ACCDB uses the same XOR scheme as Jet4 for the header password area.
            if (!HeaderPasswordMatches(header, Jet4PasswordMask, password.Span))
            {
                throw new UnauthorizedAccessException(
                    "The provided password is incorrect for this database.");
            }

            aesPageKey = DeriveAesPageKey(password.Span);
        }

        try
        {
            PageDecryptionKeys keys = new(GetJet3PageMask(format, header), rc4DbKey, aesPageKey);
            aesPageKey = null;
            return keys;
        }
        finally
        {
            OfficeCryptoPrimitives.ZeroIfNotNull(aesPageKey);
        }
    }

    /// <summary>
    /// Constant RC4 key Microsoft Access applies to header bytes [0x18 .. 0x18+126]
    /// (Jet3) or [0x18 .. 0x18+128] (Jet4/ACE) at file write time. The same key
    /// unscrambles the bytes again at read time. mdbtools applies it
    /// unconditionally in mdb_handle_from_stream (src/libmdb/file.c).
    /// </summary>
    private static readonly byte[] HeaderRc4Key = [0xC7, 0xDA, 0x39, 0x6B];

    /// <summary>
    /// Reads the database codepage from a raw, freshly-loaded page-0 header.
    /// The codepage word at offset 0x3C is scrambled by the constant-key RC4
    /// stream Microsoft Access applies to the header, so this helper
    /// descrambles a local copy of the relevant byte range before returning the
    /// codepage value.
    /// Returns 0 when the header does not carry a recognizable codepage, in
    /// which case callers should fall back to a sensible default (1252).
    /// </summary>
    /// <param name="hdr">The database header bytes.</param>
    /// <param name="format">The format.</param>
    public static int DecodeHeaderCodePage(byte[] hdr, DatabaseFormat format)
    {
        if (hdr is null || hdr.Length < 0x3E)
        {
            return 0;
        }

        int rc4Length = format == DatabaseFormat.Jet3Mdb ? 126 : 128;
        if (hdr.Length < 0x18 + rc4Length)
        {
            rc4Length = hdr.Length - 0x18;
        }

        byte[] copy = new byte[rc4Length];
        Buffer.BlockCopy(hdr, 0x18, copy, 0, rc4Length);
        Rc4Transform(copy, 0, rc4Length, HeaderRc4Key);

        // Codepage lives at hdr[0x3C..0x3D]; in the descrambled copy that is at
        // offset 0x3C - 0x18 = 0x24.
        const int codePageOffsetInCopy = 0x3C - 0x18;
        if (copy.Length < codePageOffsetInCopy + 2)
        {
            return 0;
        }

        return Ru16(copy, codePageOffsetInCopy);
    }

    /// <summary>
    /// Applies or removes the fixed RC4 mask Access uses for page-0 header
    /// bytes <c>0x18..0x97</c>. The transform is symmetric.
    /// </summary>
    /// <param name="headerPage">The header page.</param>
    internal static void TransformHeaderMask(byte[] headerPage)
    {
        Guard.NotNull(headerPage, nameof(headerPage));
        int length = Math.Min(128, headerPage.Length - 0x18);
        if (length > 0)
        {
            Rc4Transform(headerPage, 0x18, length, HeaderRc4Key);
        }
    }

    /// <summary>
    /// Detects the on-disk encryption format of the database at
    /// <paramref name="path"/>. Returns <see cref="AccessEncryptionFormat.None"/>
    /// when the file is unencrypted. The file is read but not modified.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static async ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        await using FileStream fs = FileStreamFactory.Open(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            FileOptions.Asynchronous);
        return await DetectEncryptionFormatAsync(fs, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Detects the on-disk encryption format of the database in <paramref name="stream"/>
    /// without modifying it. The stream must be seekable.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static async ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        Guard.RequireReadableSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        long origin = stream.Position;
        try
        {
            _ = stream.Seek(0, SeekOrigin.Begin);
            byte[] sniff = new byte[Constants.PageSizes.Jet4];
            _ = await stream.ReadAtLeastAsync(
                sniff.AsMemory(),
                sniff.Length,
                throwOnEndOfStream: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            AccessEncryptionFormat headerFormat = EncryptionConverter.Detect(sniff);
            if (headerFormat == AccessEncryptionFormat.AccdbAgileCfb)
            {
                _ = stream.Seek(0, SeekOrigin.Begin);
                AccessEncryptionFormat cfbFormat = await TryDetectCompoundFileFormatAsync(stream, cancellationToken)
                    .ConfigureAwait(false);
                if (cfbFormat != AccessEncryptionFormat.None)
                {
                    return cfbFormat;
                }
            }

            return headerFormat;
        }
        finally
        {
            _ = stream.Seek(origin, SeekOrigin.Begin);
        }
    }

    /// <summary>
    /// Changes the password of an already-encrypted JET / ACE database in place,
    /// preserving the existing on-disk encryption format.
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="options">The options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public static ValueTask ChangePasswordAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        return ReencryptFileAsync(
            path,
            oldPassword,
            newPassword,
            targetFormat: null,
            requireSourceEncrypted: true,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Encrypts a currently-unencrypted JET / ACE database in place, applying the
    /// requested <paramref name="targetFormat"/>.
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="targetFormat">The target format.</param>
    /// <param name="options">The options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetFormat"/> is <see cref="AccessEncryptionFormat.None"/>.</exception>
    public static ValueTask EncryptAsync(
        string path,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        if (targetFormat == AccessEncryptionFormat.None)
        {
            throw new ArgumentException(
                "Target format must not be None. Use DecryptAsync to remove encryption.",
                nameof(targetFormat));
        }

        return ReencryptFileAsync(
            path,
            oldPassword: null,
            newPassword,
            targetFormat,
            requireSourceEncrypted: false,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Removes encryption from a JET / ACE database in place.
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="options">The options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public static ValueTask DecryptAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        return ReencryptFileAsync(
            path,
            oldPassword,
            newPassword: null,
            targetFormat: AccessEncryptionFormat.None,
            requireSourceEncrypted: true,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of <see cref="ChangePasswordAsync(string,ReadOnlyMemory{char},ReadOnlyMemory{char},AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public static ValueTask ChangePasswordAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        return ReencryptStreamAsync(
            stream,
            oldPassword,
            newPassword,
            targetFormat: null,
            requireSourceEncrypted: true,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of <see cref="EncryptAsync(string,ReadOnlyMemory{char},AccessEncryptionFormat?,AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="targetFormat">The target format.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetFormat"/> is <see cref="AccessEncryptionFormat.None"/>.</exception>
    public static ValueTask EncryptAsync(
        Stream stream,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        if (targetFormat == AccessEncryptionFormat.None)
        {
            throw new ArgumentException(
                "Target format must not be None. Use DecryptAsync to remove encryption.",
                nameof(targetFormat));
        }

        return ReencryptStreamAsync(
            stream,
            oldPassword: null,
            newPassword,
            targetFormat,
            requireSourceEncrypted: false,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of <see cref="DecryptAsync(string,ReadOnlyMemory{char},AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public static ValueTask DecryptAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        return ReencryptStreamAsync(
            stream,
            oldPassword,
            newPassword: null,
            targetFormat: AccessEncryptionFormat.None,
            requireSourceEncrypted: true,
            cancellationToken);
    }

    /// <summary>
    /// If <paramref name="header"/> has CFB magic and the contained streams
    /// describe an Office Crypto API ("Agile" or "Standard") encrypted package,
    /// or the stream is an Access-native flat Agile ACCDB, returns the decrypted
    /// inner ACCDB bytes. Returns <c>null</c> when the file is not an encrypted
    /// Office document.
    /// Throws <see cref="UnauthorizedAccessException"/> when an encrypted package
    /// is detected but no password was supplied.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="header">The header.</param>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="UnauthorizedAccessException">Thrown when a flat Agile encrypted database is detected and no password was supplied.</exception>
    public static async ValueTask<byte[]?> TryDecryptAgileCompoundFileAsync(
        Stream stream,
        byte[] header,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken)
    {
        if (!CompoundFileReader.HasCompoundFileMagic(header))
        {
            long originalPosition = stream.Position;
            try
            {
                _ = stream.Seek(0, SeekOrigin.Begin);
                byte[] rawFile = new byte[stream.Length];
                await stream.ReadExactlyAsync(rawFile.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (!OfficeCryptoAgile.IsFlatAgileEncrypted(rawFile))
                {
                    return null;
                }

                if (password.IsEmpty)
                {
                    throw new UnauthorizedAccessException(
                        "This .accdb file is encrypted with Access Agile encryption. " +
                        "Provide the database password via AccessReaderOptions.Password to open it.");
                }

                return OfficeCryptoAgile.DecryptFlatDatabase(rawFile, password.Span);
            }
            finally
            {
                if (stream.CanSeek)
                {
                    _ = stream.Seek(originalPosition, SeekOrigin.Begin);
                }
            }
        }

        (byte[]? plaintext, _) = await TryDecryptCompoundFileWithFormatAsync(stream, header, password, cancellationToken)
            .ConfigureAwait(false);
        return plaintext;
    }

    /// <summary>
    /// Same as <see cref="TryDecryptAgileCompoundFileAsync"/> but also returns
    /// the detected <see cref="AccessEncryptionFormat"/> so callers can
    /// distinguish Standard from Agile encryption.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="header">The header.</param>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="UnauthorizedAccessException">Thrown when an encrypted Standard or Agile package is detected and no password was supplied.</exception>
    internal static async ValueTask<(byte[]? Plaintext, AccessEncryptionFormat Format)> TryDecryptCompoundFileWithFormatAsync(
        Stream stream,
        byte[] header,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken)
    {
        if (!CompoundFileReader.HasCompoundFileMagic(header))
        {
            return (null, AccessEncryptionFormat.None);
        }

        Dictionary<string, byte[]>? streams = null;
        try
        {
            streams = await CompoundFileReader.ReadStreamsAsync(stream, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException)
        {
            // Not a real CFB — fall through.
        }
        catch (EndOfStreamException)
        {
            // Truncated/legacy CFB-magic file — fall through.
        }

        if (streams == null ||
            !streams.TryGetValue("EncryptionInfo", out byte[]? encryptionInfo) ||
            !streams.TryGetValue("EncryptedPackage", out byte[]? encryptedPackage))
        {
            return (null, AccessEncryptionFormat.None);
        }

        if (OfficeCryptoAgile.IsStandardEncryptionInfo(encryptionInfo))
        {
            if (password.IsEmpty)
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Office 2007 Standard encryption (AES-128). " +
                    "Provide the database password via AccessReaderOptions.Password to open it, " +
                    "or remove the password in Microsoft Access (File > Info > Decrypt Database) and try again.");
            }

            return (OfficeCryptoStandard.Decrypt(encryptionInfo, encryptedPackage, password.Span),
                    AccessEncryptionFormat.AccdbStandard);
        }

        if (!OfficeCryptoAgile.IsAgileEncryptionInfo(encryptionInfo))
        {
            return (null, AccessEncryptionFormat.None);
        }

        if (password.IsEmpty)
        {
            throw new UnauthorizedAccessException(
                "This .accdb file is encrypted with Office Crypto API 'Agile' encryption. " +
                "Provide the database password via AccessReaderOptions.Password to open it, " +
                "or remove the password in Microsoft Access (File > Info > Decrypt Database) and try again.");
        }

        return (OfficeCryptoAgile.Decrypt(encryptionInfo, encryptedPackage, password.Span),
            AccessEncryptionFormat.AccdbAgileCfb);
    }

    /// <summary>
    /// Re-encrypts an already-decrypted ACCDB package and writes the resulting
    /// Office Crypto compound file (CFB) to <paramref name="destination"/>. This
    /// is the inverse of <see cref="TryDecryptCompoundFileWithFormatAsync"/> and
    /// is invoked when an Office Crypto <c>.accdb</c> opened for writing is
    /// disposed: the in-memory decrypted database is re-wrapped so the caller's
    /// outer encrypted stream ends up with every buffered write.
    /// </summary>
    /// <param name="decryptedInner">In-memory stream holding the decrypted inner ACCDB. Must be a <see cref="MemoryStream"/>.</param>
    /// <param name="destination">The outer stream that receives the re-encrypted CFB document.</param>
    /// <param name="format">The Office Crypto format to apply (<see cref="AccessEncryptionFormat.AccdbStandard"/> or Agile).</param>
    /// <param name="password">The database password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="decryptedInner"/> is not the expected in-memory backing stream.</exception>
    internal static async ValueTask RewrapDecryptedCompoundFileAsync(
        Stream decryptedInner,
        Stream destination,
        AccessEncryptionFormat format,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken = default)
    {
        MemoryStream memory = decryptedInner as MemoryStream
            ?? throw new InvalidOperationException("Agile-encrypted writer expected an in-memory backing stream.");

        byte[] inner = memory.ToArray();

        OfficeEncryptedPackage package = format == AccessEncryptionFormat.AccdbStandard
            ? OfficeCryptoStandard.Encrypt(inner, password.Span)
            : OfficeCryptoAgile.Encrypt(inner, password.Span);

        byte[] cfb = EncryptionConverter.BuildOfficeCryptoCompoundFile(package);

        _ = destination.Seek(0, SeekOrigin.Begin);
        await destination.WriteAsync(cfb.AsMemory(), cancellationToken).ConfigureAwait(false);
        destination.SetLength(cfb.Length);
        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<AccessEncryptionFormat> TryDetectCompoundFileFormatAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        Dictionary<string, byte[]>? streams = null;
        try
        {
            streams = await CompoundFileReader.ReadStreamsAsync(stream, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException)
        {
            return AccessEncryptionFormat.None;
        }
        catch (EndOfStreamException)
        {
            return AccessEncryptionFormat.None;
        }

        if (streams == null || !streams.TryGetValue("EncryptionInfo", out byte[]? encryptionInfo))
        {
            return AccessEncryptionFormat.None;
        }

        if (OfficeCryptoAgile.IsStandardEncryptionInfo(encryptionInfo))
        {
            return AccessEncryptionFormat.AccdbStandard;
        }

        return OfficeCryptoAgile.IsAgileEncryptionInfo(encryptionInfo)
            ? AccessEncryptionFormat.AccdbAgileCfb
            : AccessEncryptionFormat.None;
    }

    private static async ValueTask ReencryptFileAsync(
        string path,
        ReadOnlyMemory<char>? oldPassword,
        ReadOnlyMemory<char>? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        AccessWriterOptions? options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        using var lockFile = LockFileCoordinator.ForReencrypt(path, options);
        lockFile.Acquire();

        byte[] sourceBytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        await using var sourceStream = new MemoryStream(sourceBytes, writable: false);

        byte[] result = await ReencryptCoreAsync(
            sourceStream,
            oldPassword,
            newPassword,
            targetFormat,
            requireSourceEncrypted,
            cancellationToken).ConfigureAwait(false);

        await ReplaceFileAtomicAsync(path, result, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask ReplaceFileAtomicAsync(string path, byte[] contents, CancellationToken cancellationToken)
    {
        string tempPath = path + ".reenc-" + Guid.NewGuid().ToString("N") + ".tmp";
        await using (FileStream fs = FileStreamFactory.Open(
            tempPath,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            FileOptions.Asynchronous,
            preallocationSize: contents.Length))
        {
            await fs.WriteAsync(contents.AsMemory(), cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            File.Replace(tempPath, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
        }
        catch (PlatformNotSupportedException)
        {
            File.Delete(path);
            File.Move(tempPath, path);
        }
        catch (IOException)
        {
            File.Delete(path);
            File.Move(tempPath, path);
        }
    }

    private static async ValueTask ReencryptStreamAsync(
        Stream stream,
        ReadOnlyMemory<char>? oldPassword,
        ReadOnlyMemory<char>? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        Guard.RequireReadWriteSeekableStream(stream, nameof(stream));

        byte[] result = await ReencryptCoreAsync(
            stream,
            oldPassword,
            newPassword,
            targetFormat,
            requireSourceEncrypted,
            cancellationToken).ConfigureAwait(false);

        _ = stream.Seek(0, SeekOrigin.Begin);
        await stream.WriteAsync(result.AsMemory(), cancellationToken).ConfigureAwait(false);
        stream.SetLength(result.Length);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<byte[]> ReencryptCoreAsync(
        Stream source,
        ReadOnlyMemory<char>? oldPassword,
        ReadOnlyMemory<char>? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        ReadOnlyMemory<char> oldPwd = oldPassword.GetValueOrDefault();
        ReadOnlyMemory<char> newPwd = newPassword.GetValueOrDefault();

        long origPos = source.Position;
        AccessEncryptionFormat detectedFormat = await DetectEncryptionFormatAsync(source, cancellationToken).ConfigureAwait(false);
        _ = source.Seek(origPos, SeekOrigin.Begin);

        if (requireSourceEncrypted && detectedFormat == AccessEncryptionFormat.None)
        {
            throw new InvalidOperationException(
                "The source database is not encrypted. Use EncryptAsync to add a password.");
        }

        if (!requireSourceEncrypted && detectedFormat != AccessEncryptionFormat.None)
        {
            throw new InvalidOperationException(
                $"The source database is already encrypted ({detectedFormat}). Use ChangePasswordAsync or DecryptAsync.");
        }

        (byte[] plaintext, AccessEncryptionFormat sourceFormat) = await EncryptionConverter
            .ReadDecryptedAsync(source, oldPwd, cancellationToken)
            .ConfigureAwait(false);

        AccessEncryptionFormat effectiveTarget = targetFormat
            ?? (requireSourceEncrypted
                ? sourceFormat
                : EncryptionConverter.ResolveBestTargetFormat(plaintext));
        return EncryptionConverter.ApplyEncryption(plaintext, effectiveTarget, newPwd);
    }

    /// <summary>
    /// Applies any active page decryption (Jet3 XOR, Jet4 RC4, ACCDB AES) to
    /// <paramref name="buf"/> in place. A no-op when no keys are configured or
    /// when <paramref name="pageNumber"/> is 0 (the unencrypted header page).
    /// </summary>
    /// <param name="buf">The page buffer.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="keys">The page encryption keys.</param>
    public static void DecryptPageInPlace(byte[] buf, long pageNumber, int pageSize, PageDecryptionKeys keys) =>
        DecryptPageInPlace(buf, 0, pageNumber, pageSize, keys);

    /// <summary>
    /// Variant of <see cref="DecryptPageInPlace(byte[],long,int,PageDecryptionKeys)"/>
    /// that decrypts a page sitting at <paramref name="offset"/> within a larger
    /// backing array. Lets bulk callers skip per-page slice copies.
    /// </summary>
    /// <param name="buf">The page buffer.</param>
    /// <param name="offset">The offset.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="keys">The page encryption keys.</param>
    public static void DecryptPageInPlace(byte[] buf, int offset, long pageNumber, int pageSize, PageDecryptionKeys keys)
    {
        if (pageNumber < 1 || keys == null)
        {
            return;
        }

        if (keys.HasJet3XorMask)
        {
            ApplyJet3Xor(buf, offset, pageNumber, pageSize, keys.Jet3XorMask);
        }

        if (keys.TryGetRc4DbKey(out uint dbKey))
        {
            Span<byte> rc4Key = stackalloc byte[4];
            try
            {
                DeriveRc4PageKey(dbKey, (uint)pageNumber, rc4Key);
                Rc4Transform(buf, offset, pageSize, rc4Key);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(rc4Key);
            }
        }

        if (keys.HasAesPageKey)
        {
            AesEcbInPlace(keys.GetAesDecryptor(), buf, offset, pageSize);
        }
    }

    /// <summary>
    /// Applies any active page encryption (Jet3 XOR, Jet4 RC4, ACCDB AES) to
    /// <paramref name="buf"/> in place — the inverse of
    /// <see cref="DecryptPageInPlace(byte[],long,int,PageDecryptionKeys)"/>.
    /// A no-op when no keys are configured
    /// or when <paramref name="pageNumber"/> is 0 (the unencrypted header
    /// page). Operations are applied in reverse order so a page round-trips
    /// back to its original ciphertext.
    /// </summary>
    /// <param name="buf">The page buffer.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="keys">The page encryption keys.</param>
    public static void EncryptPageInPlace(byte[] buf, long pageNumber, int pageSize, PageDecryptionKeys keys) =>
        EncryptPageInPlace(buf, 0, pageNumber, pageSize, keys);

    /// <summary>
    /// Variant of <see cref="EncryptPageInPlace(byte[],long,int,PageDecryptionKeys)"/>
    /// that encrypts a page sitting at <paramref name="offset"/> within a larger
    /// backing array.
    /// </summary>
    /// <param name="buf">The page buffer.</param>
    /// <param name="offset">The offset.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="keys">The page encryption keys.</param>
    public static void EncryptPageInPlace(byte[] buf, int offset, long pageNumber, int pageSize, PageDecryptionKeys keys)
    {
        if (pageNumber < 1 || keys == null)
        {
            return;
        }

        // Inverse order of DecryptPageInPlace: AES → RC4 → Jet3 XOR.
        if (keys.HasAesPageKey)
        {
            AesEcbInPlace(keys.GetAesEncryptor(), buf, offset, pageSize);
        }

        if (keys.TryGetRc4DbKey(out uint dbKey))
        {
            // RC4 is symmetric: same operation encrypts and decrypts.
            Span<byte> rc4Key = stackalloc byte[4];
            try
            {
                DeriveRc4PageKey(dbKey, (uint)pageNumber, rc4Key);
                Rc4Transform(buf, offset, pageSize, rc4Key);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(rc4Key);
            }
        }

        if (keys.HasJet3XorMask)
        {
            // XOR is symmetric.
            ApplyJet3Xor(buf, offset, pageNumber, pageSize, keys.Jet3XorMask);
        }
    }

    /// <summary>
    /// Applies the Jet3 cyclic XOR mask to a single page in place. Symmetric:
    /// the same operation encrypts and decrypts.
    /// </summary>
    /// <param name="buf">The page buffer.</param>
    /// <param name="offset">The offset.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="mask">The encryption mask or page bitmask.</param>
    private static void ApplyJet3Xor(byte[] buf, int offset, long pageNumber, int pageSize, ReadOnlySpan<byte> mask)
    {
        long fileOffset = pageNumber * pageSize;
        for (int b = 0; b < pageSize; b++)
        {
            buf[offset + b] ^= mask[(int)((fileOffset + b - pageSize) % mask.Length)];
        }
    }

    /// <summary>Returns true when <paramref name="keys"/> has any active page encryption configured.</summary>
    /// <param name="keys">The page encryption keys.</param>
    public static bool HasPageEncryption(PageDecryptionKeys keys) =>
        keys != null && (keys.HasJet3XorMask || keys.HasRc4DbKey || keys.HasAesPageKey);

    // ── Crypto primitives ────────────────────────────────────────────

    /// <summary>
    /// Derives the RC4 key for a specific page: MD5(dbKey LE + pageNumber LE)[0..4].
    /// </summary>
    /// <param name="dbKey">The db key.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="destination">The destination.</param>
    /// <exception cref="CryptographicException">Thrown when the MD5 page-key hash cannot be computed.</exception>
    private static void DeriveRc4PageKey(uint dbKey, uint pageNumber, Span<byte> destination)
    {
        Span<byte> input = stackalloc byte[8];
        Wu32(input, 0, dbKey);
        Wu32(input, 4, pageNumber);
        Span<byte> hash = stackalloc byte[16];
        try
        {
#pragma warning disable CA5351, RS0030 // MD5 is required by the Jet4 RC4 key derivation spec, and this code is not used for any security-sensitive purpose. The 8-byte input is too short to be meaningfully brute-forced, and the output is truncated to 4 bytes for the actual key, so collision resistance is not a concern.
#if NETSTANDARD2_1
            using (var md5 = MD5.Create())
            {
                if (!md5.TryComputeHash(input, hash, out _))
                {
                    throw new CryptographicException("MD5 hash computation failed.");
                }
            }
#else
            if (MD5.HashData(input, hash) != hash.Length)
            {
                throw new CryptographicException("MD5 hash computation failed.");
            }
#endif
#pragma warning restore CA5351, RS0030 // MD5 is required by the Jet4 RC4 key derivation spec, and this code is not used for any security-sensitive purpose. The 8-byte input is too short to be meaningfully brute-forced, and the output is truncated to 4 bytes for the actual key, so collision resistance is not a concern.

            hash[..4].CopyTo(destination);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(input);
            CryptographicOperations.ZeroMemory(hash);
        }
    }

    /// <summary>In-place RC4 transform (encrypt and decrypt are the same operation).</summary>
    /// <param name="data">The data bytes or values.</param>
    /// <param name="offset">The offset.</param>
    /// <param name="length">The length.</param>
    /// <param name="key">The key bytes or index key.</param>
    private static void Rc4Transform(byte[] data, int offset, int length, ReadOnlySpan<byte> key)
    {
        Span<byte> s = stackalloc byte[256];
        try
        {
            for (int i = 0; i < 256; i++)
            {
                s[i] = (byte)i;
            }

            int j = 0;
            for (int i = 0; i < 256; i++)
            {
                j = (j + s[i] + key[i % key.Length]) & 0xFF;
                (s[i], s[j]) = (s[j], s[i]);
            }

            int x = 0, y = 0;
            for (int k = 0; k < length; k++)
            {
                x = (x + 1) & 0xFF;
                y = (y + s[x]) & 0xFF;
                (s[x], s[y]) = (s[y], s[x]);
                data[offset + k] ^= s[(s[x] + s[y]) & 0xFF];
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(s);
        }
    }

#pragma warning disable CA5358 // ECB mode is required to match the ACCDB AES page encryption scheme
    /// <summary>
    /// Runs an ECB <see cref="ICryptoTransform"/> over a buffer in place.
    /// ECB has no chaining state between 16-byte blocks, so passing the same
    /// array as both input and output is safe and avoids any temporary
    /// allocations or block copies.
    /// </summary>
    /// <param name="xform">The xform.</param>
    /// <param name="data">The data bytes or values.</param>
    /// <param name="offset">The offset.</param>
    /// <param name="length">The length.</param>
    /// <exception cref="CryptographicException">Thrown when the AES transform writes an unexpected byte count.</exception>
    private static void AesEcbInPlace(ICryptoTransform xform, byte[] data, int offset, int length)
    {
        int written = xform.TransformBlock(data, offset, length, data, offset);
        if (written != length)
        {
            throw new CryptographicException(
                $"AES-ECB TransformBlock processed {written} bytes but {length} were expected.");
        }
    }
#pragma warning restore CA5358

    private static bool HeaderPasswordMatches(byte[] hdr, ReadOnlySpan<byte> mask, ReadOnlySpan<char> password)
    {
        Span<byte> storedNormalized = stackalloc byte[HeaderPasswordNormalizedLength];
        Span<byte> suppliedNormalized = stackalloc byte[HeaderPasswordNormalizedLength];

        try
        {
            NormalizeStoredHeaderPassword(hdr, mask, storedNormalized);
            NormalizeSuppliedHeaderPassword(password, suppliedNormalized);

            return CryptographicOperations.FixedTimeEquals(storedNormalized, suppliedNormalized);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(storedNormalized);
            CryptographicOperations.ZeroMemory(suppliedNormalized);
        }
    }

    private static void NormalizeStoredHeaderPassword(byte[] hdr, ReadOnlySpan<byte> mask, Span<byte> destination)
    {
        destination.Clear();
        Span<byte> passwordBytes = destination.Slice(HeaderPasswordLengthPrefixLength, HeaderPasswordLength);

        for (int offset = 0; offset < HeaderPasswordLength; offset++)
        {
            passwordBytes[offset] = (byte)(hdr[0x42 + offset] ^ mask[offset] ^ hdr[0x72 + (offset % 4)]);
        }

        int passwordByteLength = HeaderPasswordLength;
        for (int offset = 0; offset < HeaderPasswordLength; offset += HeaderPasswordCharSize)
        {
            if (passwordBytes[offset] == 0 && passwordBytes[offset + 1] == 0)
            {
                passwordByteLength = offset;
                passwordBytes[offset..].Clear();
                break;
            }
        }

        Wu32(destination, 0, (uint)passwordByteLength);
    }

    private static void NormalizeSuppliedHeaderPassword(ReadOnlySpan<char> password, Span<byte> destination)
    {
        destination.Clear();
        Span<byte> passwordBytes = destination.Slice(HeaderPasswordLengthPrefixLength, HeaderPasswordLength);
        const int maxPasswordChars = HeaderPasswordLength / HeaderPasswordCharSize;
        int charsToEncode = Math.Min(password.Length, maxPasswordChars);

        if (charsToEncode > 0)
        {
            _ = Encoding.Unicode.GetBytes(password[..charsToEncode], passwordBytes);
        }

        uint passwordByteLength = password.Length <= maxPasswordChars
            ? (uint)(password.Length * HeaderPasswordCharSize)
            : uint.MaxValue;
        Wu32(destination, 0, passwordByteLength);
    }

    /// <summary>
    /// Derives a 128-bit AES key from a password using SHA-256 (truncated to 16 bytes).
    /// </summary>
    /// <param name="password">The password.</param>
    private static byte[] DeriveAesPageKey(ReadOnlySpan<char> password)
    {
        // Header-area passwords are capped at 15 UTF-16 chars by
        // EncryptionConverter.EncodeJet4StylePassword, so 256 bytes of stack is
        // ample headroom; we still allocate a heap buffer for any unexpectedly
        // long password rather than risk a stack overflow.
        int maxBytes = Encoding.UTF8.GetMaxByteCount(password.Length);
        Span<byte> stackBuf = stackalloc byte[256];
        byte[]? rented = maxBytes > stackBuf.Length ? new byte[maxBytes] : null;
        Span<byte> utf8 = rented ?? stackBuf;

        try
        {
            int utf8Len = Encoding.UTF8.GetBytes(password, utf8);
            Span<byte> hash = stackalloc byte[32];
            try
            {
                OfficeCryptoPrimitives.HashSha256(utf8[..utf8Len], hash);
                return hash[..16].ToArray(); // AES-128
            }
            finally
            {
                CryptographicOperations.ZeroMemory(hash);
            }
        }
        finally
        {
            // Scrub any password bytes from the buffer we used.
            CryptographicOperations.ZeroMemory(utf8);
        }
    }
}
