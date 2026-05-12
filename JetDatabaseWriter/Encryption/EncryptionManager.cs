namespace JetDatabaseWriter.Encryption;

using System;
using System.Buffers.Binary;
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

/// <summary>
/// Centralizes all JET / ACE / ACCDB encryption logic — header detection,
/// password verification, key derivation, and per-page decryption.
/// </summary>
internal static class EncryptionManager
{
    // Jet3 XOR mask (128 bytes, applied cyclically to pages 1+ when the
    // Office97 password flag is set on a Jet3 .mdb). Sourced from mdbtools
    // HACKING.md.
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

    // Jet4 password XOR mask (mdbtools / jackcess). Applied together with
    // the 4-byte creation date at offset 0x72 to decode the stored password.
    internal static readonly byte[] Jet4PasswordMask =
    [
        0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
        0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
        0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
        0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
        0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
    ];

    // ACE legacy password mask used for password-only ACCDB files
    // created via DBEngine.CompactDatabase(..., ";pwd=...").
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
    public static bool IsCompoundFileEncrypted(byte[] header) =>
        header != null && header.Length >= 4 &&
        header[0] == 0xD0 && header[1] == 0xCF && header[2] == 0x11 && header[3] == 0xE0;

    /// <summary>
    /// Returns the Jet3 page XOR mask if the header has the Jet3 Office97 password
    /// flag set (offset 0x62, bit 0x01); otherwise returns null.
    /// </summary>
    public static byte[]? GetJet3PageMask(DatabaseFormat format, byte[] hdr)
    {
        if (format == DatabaseFormat.Jet3Mdb && hdr.Length > 0x62 && (hdr[0x62] & 0x01) != 0)
        {
            return Jet3PageXorMask;
        }

        return null;
    }

    // Constant RC4 key Microsoft Access applies to header bytes [0x18 .. 0x18+126]
    // (Jet3) or [0x18 .. 0x18+128] (Jet4/ACE) at file write time. The same key
    // unscrambles the bytes again at read time. mdbtools applies it
    // unconditionally in mdb_handle_from_stream (src/libmdb/file.c).
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
        const int CodePageOffsetInCopy = 0x3C - 0x18;
        if (copy.Length < CodePageOffsetInCopy + 2)
        {
            return 0;
        }

        return BinaryPrimitives.ReadUInt16LittleEndian(copy.AsSpan(CodePageOffsetInCopy, 2));
    }

    /// <summary>
    /// Applies or removes the fixed RC4 mask Access uses for page-0 header
    /// bytes <c>0x18..0x97</c>. The transform is symmetric.
    /// </summary>
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
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
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
        Guard.NotNull(stream, nameof(stream));
        if (!stream.CanRead || !stream.CanSeek)
        {
            throw new ArgumentException("Stream must be readable and seekable.", nameof(stream));
        }

        cancellationToken.ThrowIfCancellationRequested();

        long origin = stream.Position;
        try
        {
            _ = stream.Seek(0, SeekOrigin.Begin);
            byte[] sniff = new byte[Constants.PageSizes.Jet4];
            int read = 0;
            while (read < sniff.Length)
            {
                int got = await stream.ReadAsync(sniff.AsMemory(read, sniff.Length - read), cancellationToken).ConfigureAwait(false);
                if (got == 0)
                {
                    break;
                }

                read += got;
            }

            return EncryptionConverter.Detect(sniff);
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
    public static ValueTask ChangePasswordAsync(
        string path,
        string oldPassword,
        string newPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
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
    public static ValueTask EncryptAsync(
        string path,
        string newPassword,
        AccessEncryptionFormat targetFormat,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
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
    public static ValueTask DecryptAsync(
        string path,
        string oldPassword,
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
    /// Stream-based equivalent of <see cref="ChangePasswordAsync(string,string,string,AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    public static ValueTask ChangePasswordAsync(
        Stream stream,
        string oldPassword,
        string newPassword,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
        return ReencryptStreamAsync(
            stream,
            oldPassword,
            newPassword,
            targetFormat: null,
            requireSourceEncrypted: true,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of <see cref="EncryptAsync(string,string,AccessEncryptionFormat,AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    public static ValueTask EncryptAsync(
        Stream stream,
        string newPassword,
        AccessEncryptionFormat targetFormat,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotNullOrEmpty(newPassword, nameof(newPassword));
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
    /// Stream-based equivalent of <see cref="DecryptAsync(string,string,AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    public static ValueTask DecryptAsync(
        Stream stream,
        string oldPassword,
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
    /// Inspects the database header for Jet4 / ACCDB-legacy / ACCDB-AES encryption
    /// flags, verifies the supplied password where required, and returns the
    /// derived per-page keys (RC4 database key and / or AES-128 page key).
    /// Throws <see cref="UnauthorizedAccessException"/> when the database is
    /// encrypted but no / wrong password was supplied.
    /// </summary>
    public static (uint? Rc4DbKey, byte[]? AesPageKey) ResolveReaderPageKeys(
        byte[] hdr,
        DatabaseFormat format,
        bool isCompoundFileEncrypted,
        ReadOnlyMemory<char> password)
    {
        uint? rc4DbKey = null;
        byte[]? aesPageKey = null;

        // Offset 0x14: Jet/ACE format version byte.
        byte ver = hdr[0x14];

        // Jet4 .mdb (Access 2000 – 2003) — flag at 0x62 governs encryption.
        // ACCDB format (ver >= 2, Access 2007+) reuses this offset for unrelated
        // bits, so the Jet4 detection only applies to ver == 1.
        if (format == DatabaseFormat.Jet4Mdb && hdr.Length > 0x62)
        {
            byte encFlag = hdr[0x62];

            // Jet4 encryption flag values:
            //   0x01 = Office97 password only (no page encryption)
            //   0x02 = RC4 page encryption
            //   0x03 = RC4 + password
            if (encFlag >= 0x01 && encFlag <= 0x03)
            {
                if (password.IsEmpty)
                {
                    throw new UnauthorizedAccessException(
                        "This database is encrypted or password-protected. " +
                        "Provide a password via AccessReaderOptions.Password, or " +
                        "remove the password in Microsoft Access (File > Info > Encrypt with Password) and try again.");
                }

                string storedPassword = DecodeHeaderPassword(hdr, Jet4PasswordMask);
                if (!password.Span.SequenceEqual(storedPassword.AsSpan()))
                {
                    throw new UnauthorizedAccessException(
                        "The provided password is incorrect for this database.");
                }

                if ((encFlag & 0x02) != 0)
                {
                    rc4DbKey = BinaryPrimitives.ReadUInt32LittleEndian(hdr.AsSpan(0x3E, 4));
                }
            }
        }

        // ACCDB legacy password-only mode (standard ACCDB header, ver >= 3).
        // Many normal ACCDB files reuse overlapping bits at 0x62, so we only
        // enforce password verification for the known legacy-password signature
        // emitted by Access 2010+ CompactDatabase(";pwd=...") test fixtures.
        if (format == DatabaseFormat.AceAccdb && ver >= 3 && !isCompoundFileEncrypted && hdr.Length > 0x62)
        {
            byte encFlag = hdr[0x62];
            if (encFlag == 0x07)
            {
                if (password.IsEmpty)
                {
                    throw new UnauthorizedAccessException(
                        "This database is password-protected. " +
                        "Provide a password via AccessReaderOptions.Password.");
                }

                string storedPassword = DecodeHeaderPassword(hdr, AccdbLegacyPasswordMask);
                if (!password.Span.SequenceEqual(storedPassword.AsSpan()))
                {
                    throw new UnauthorizedAccessException(
                        "The provided password is incorrect for this database.");
                }
            }
        }

        // ACCDB genuine AES encryption (CFB-wrapped file presented as a raw
        // header by the synthetic legacy path).
        if (isCompoundFileEncrypted)
        {
            if (password.IsEmpty)
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Access 2007+ AES encryption. " +
                    "Provide the database password via AccessReaderOptions.Password to open it, " +
                    "or remove the password in Microsoft Access (File > Info > Decrypt Database) and try again.");
            }

            // ACCDB uses the same XOR scheme as Jet4 for the header password area.
            string storedPassword = DecodeHeaderPassword(hdr, Jet4PasswordMask);
            if (!password.Span.SequenceEqual(storedPassword.AsSpan()))
            {
                throw new UnauthorizedAccessException(
                    "The provided password is incorrect for this database.");
            }

            aesPageKey = DeriveAesPageKey(password.Span);
        }

        return (rc4DbKey, aesPageKey);
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
                AccessEncryptionFormat.AccdbAgile);
    }

    private static async ValueTask ReencryptFileAsync(
        string path,
        string? oldPassword,
        string? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        AccessWriterOptions? options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        LockFileSlotWriter? lockSlot = AcquireReencryptLockSlot(path, options);
        try
        {
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
        finally
        {
            if (lockSlot != null)
            {
                lockSlot.Dispose();
            }
        }
    }

    private static LockFileSlotWriter? AcquireReencryptLockSlot(string path, AccessWriterOptions? options)
    {
        if (options?.UseLockFile == false)
        {
            return null;
        }

        return LockFileSlotWriter.Open(
            path,
            nameof(AccessWriter),
            respectExisting: options?.RespectExistingLockFile ?? true,
            machineName: options?.LockFileMachineName,
            userName: options?.LockFileUserName);
    }

    private static async ValueTask ReplaceFileAtomicAsync(string path, byte[] contents, CancellationToken cancellationToken)
    {
        string tempPath = path + ".reenc-" + Guid.NewGuid().ToString("N") + ".tmp";
        await using (var fs = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
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
        string? oldPassword,
        string? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(stream, nameof(stream));
        if (!stream.CanRead || !stream.CanWrite || !stream.CanSeek)
        {
            throw new ArgumentException("Stream must be readable, writable, and seekable.", nameof(stream));
        }

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
        string? oldPassword,
        string? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        ReadOnlyMemory<char> oldPwd = oldPassword.AsMemory();
        ReadOnlyMemory<char> newPwd = newPassword.AsMemory();

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

        AccessEncryptionFormat effectiveTarget = targetFormat ?? sourceFormat;
        return EncryptionConverter.ApplyEncryption(plaintext, effectiveTarget, newPwd);
    }

    /// <summary>
    /// Applies any active page decryption (Jet3 XOR, Jet4 RC4, ACCDB AES) to
    /// <paramref name="buf"/> in place. A no-op when no keys are configured or
    /// when <paramref name="pageNumber"/> is 0 (the unencrypted header page).
    /// </summary>
    public static void DecryptPageInPlace(byte[] buf, long pageNumber, int pageSize, PageDecryptionKeys keys) =>
        DecryptPageInPlace(buf, 0, pageNumber, pageSize, keys);

    /// <summary>
    /// Variant of <see cref="DecryptPageInPlace(byte[],long,int,PageDecryptionKeys)"/>
    /// that decrypts a page sitting at <paramref name="offset"/> within a larger
    /// backing array. Lets bulk callers skip per-page slice copies.
    /// </summary>
    public static void DecryptPageInPlace(byte[] buf, int offset, long pageNumber, int pageSize, PageDecryptionKeys keys)
    {
        if (pageNumber < 1 || keys == null)
        {
            return;
        }

        if (keys.Jet3XorMask is { } jet3Mask)
        {
            ApplyJet3Xor(buf, offset, pageNumber, pageSize, jet3Mask);
        }

        if (keys.Rc4DbKey is uint dbKey)
        {
            Span<byte> rc4Key = stackalloc byte[4];
            DeriveRc4PageKey(dbKey, (uint)pageNumber, rc4Key);
            Rc4Transform(buf, offset, pageSize, rc4Key);
        }

        if (keys.AesPageKey is not null)
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
    public static void EncryptPageInPlace(byte[] buf, long pageNumber, int pageSize, PageDecryptionKeys keys) =>
        EncryptPageInPlace(buf, 0, pageNumber, pageSize, keys);

    /// <summary>
    /// Variant of <see cref="EncryptPageInPlace(byte[],long,int,PageDecryptionKeys)"/>
    /// that encrypts a page sitting at <paramref name="offset"/> within a larger
    /// backing array.
    /// </summary>
    public static void EncryptPageInPlace(byte[] buf, int offset, long pageNumber, int pageSize, PageDecryptionKeys keys)
    {
        if (pageNumber < 1 || keys == null)
        {
            return;
        }

        // Inverse order of DecryptPageInPlace: AES → RC4 → Jet3 XOR.
        if (keys.AesPageKey is not null)
        {
            AesEcbInPlace(keys.GetAesEncryptor(), buf, offset, pageSize);
        }

        if (keys.Rc4DbKey is uint dbKey)
        {
            // RC4 is symmetric: same operation encrypts and decrypts.
            Span<byte> rc4Key = stackalloc byte[4];
            DeriveRc4PageKey(dbKey, (uint)pageNumber, rc4Key);
            Rc4Transform(buf, offset, pageSize, rc4Key);
        }

        if (keys.Jet3XorMask is { } jet3Mask)
        {
            // XOR is symmetric.
            ApplyJet3Xor(buf, offset, pageNumber, pageSize, jet3Mask);
        }
    }

    /// <summary>
    /// Applies the Jet3 cyclic XOR mask to a single page in place. Symmetric:
    /// the same operation encrypts and decrypts.
    /// </summary>
    private static void ApplyJet3Xor(byte[] buf, int offset, long pageNumber, int pageSize, byte[] mask)
    {
        long fileOffset = pageNumber * pageSize;
        for (int b = 0; b < pageSize; b++)
        {
            buf[offset + b] ^= mask[(int)((fileOffset + b - pageSize) % mask.Length)];
        }
    }

    /// <summary>Returns true when <paramref name="keys"/> has any active page encryption configured.</summary>
    public static bool HasPageEncryption(PageDecryptionKeys keys) =>
        keys != null && (keys.Jet3XorMask != null || keys.Rc4DbKey.HasValue || keys.AesPageKey != null);

    // ── Crypto primitives ────────────────────────────────────────────

#pragma warning disable CA5351 // MD5 is required by the Jet4 RC4 key derivation spec
    /// <summary>
    /// Derives the RC4 key for a specific page: MD5(dbKey LE + pageNumber LE)[0..4].
    /// </summary>
    private static void DeriveRc4PageKey(uint dbKey, uint pageNumber, Span<byte> destination)
    {
        Span<byte> input = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(input.Slice(0, 4), dbKey);
        BinaryPrimitives.WriteUInt32LittleEndian(input.Slice(4, 4), pageNumber);
        Span<byte> hash = stackalloc byte[16];
        using (var md5 = MD5.Create())
        {
            if (!md5.TryComputeHash(input, hash, out _))
            {
                throw new CryptographicException("MD5 hash computation failed.");
            }
        }

        hash.Slice(0, 4).CopyTo(destination);
    }
#pragma warning restore CA5351

    /// <summary>In-place RC4 transform (encrypt and decrypt are the same operation).</summary>
    private static void Rc4Transform(byte[] data, int offset, int length, ReadOnlySpan<byte> key)
    {
        Span<byte> s = stackalloc byte[256];
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

#pragma warning disable CA5358 // ECB mode is required to match the ACCDB AES page encryption scheme
    /// <summary>
    /// Runs an ECB <see cref="ICryptoTransform"/> over a buffer in place.
    /// ECB has no chaining state between 16-byte blocks, so passing the same
    /// array as both input and output is safe and avoids any temporary
    /// allocations or block copies.
    /// </summary>
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

    /// <summary>
    /// Shared XOR-mask password decoder for the 40-byte header password area
    /// at offset 0x42. The 40 bytes are XOR'd with a fixed mask and the 4-byte
    /// creation date at offset 0x72. Decodes as UTF-16LE and stops at the first
    /// NUL char because the encryption flag at 0x62 overlaps byte 32 of the area
    /// and can produce a non-null artifact.
    /// </summary>
    private static string DecodeHeaderPassword(byte[] hdr, ReadOnlySpan<byte> mask)
    {
        Span<byte> decoded = stackalloc byte[40];
        for (int i = 0; i < 40; i++)
        {
            decoded[i] = (byte)(hdr[0x42 + i] ^ mask[i] ^ hdr[0x72 + (i % 4)]);
        }

        string raw = Encoding.Unicode.GetString(decoded);
        int nullIdx = raw.IndexOf('\0', StringComparison.Ordinal);
        return nullIdx >= 0 ? raw.Substring(0, nullIdx) : raw;
    }

    /// <summary>
    /// Derives a 128-bit AES key from a password using SHA-256 (truncated to 16 bytes).
    /// </summary>
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
            using var sha = SHA256.Create();
            if (!sha.TryComputeHash(utf8.Slice(0, utf8Len), hash, out _))
            {
                throw new CryptographicException("SHA-256 hash computation failed.");
            }

            byte[] key = hash.Slice(0, 16).ToArray(); // AES-128
            hash.Clear();
            return key;
        }
        finally
        {
            // Scrub any password bytes from the buffer we used.
            (rented ?? stackBuf).Clear();
        }
    }
}
