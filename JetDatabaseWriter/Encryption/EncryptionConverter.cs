namespace JetDatabaseWriter.Encryption;

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// <para>
/// Implements the read-decrypt-rewrite pipeline used by
/// <see cref="AccessWriter.ChangePasswordAsync(string, ReadOnlyMemory{char}, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>,
/// <see cref="AccessWriter.EncryptAsync(string, ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, CancellationToken)"/>,
/// and <see cref="AccessWriter.DecryptAsync(string, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>.
/// </para>
/// <para>
/// All public entry points are pure byte-array transforms — they never touch
/// the filesystem directly, so the caller can decide whether to seek-and-rewrite
/// an existing stream or write to a temp file and rename atomically.
/// </para>
/// </summary>
internal static class EncryptionConverter
{
    private const int HeaderLength = 0x80;

    /// <summary>
    /// Resolves the strongest writer-supported password encryption format for
    /// a clean database image based on its JET / ACE format.
    /// </summary>
    /// <param name="plaintext">The plaintext database bytes.</param>
    /// <returns>The default target encryption format.</returns>
    /// <exception cref="InvalidDataException">Thrown when <paramref name="plaintext"/> is shorter than a JET header.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database format has no password encryption target.</exception>
    public static AccessEncryptionFormat ResolveBestTargetFormat(byte[] plaintext)
    {
        Guard.NotNull(plaintext, nameof(plaintext));
        if (plaintext.Length < HeaderLength)
        {
            throw new InvalidDataException("Plaintext database is shorter than the JET header.");
        }

        return JetFormat.DetectFormat(plaintext) switch
        {
            DatabaseFormat.Jet4Mdb => AccessEncryptionFormat.Jet4Rc4,
            DatabaseFormat.AceAccdb => AccessEncryptionFormat.AccdbAgile,
            DatabaseFormat.Jet3Mdb => throw new NotSupportedException(
                "Jet3 (.mdb) databases do not have a supported password encryption target. " +
                "Use a Jet4 .mdb or ACE .accdb database for password encryption."),
            _ => throw new InvalidDataException("The database format is unknown."),
        };
    }

    /// <summary>
    /// Reads <paramref name="source"/>, applies any active decryption, and
    /// returns a fully-plaintext copy of the database (encoding key 0, the
    /// empty-password pattern Access writes in the password area, header
    /// magic restored). The returned byte
    /// array has the same length as the inner Jet/ACE database (which may
    /// differ from <paramref name="source"/>.Length when the source was an
    /// Agile CFB container).
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="UnauthorizedAccessException">Thrown when an encrypted flat Agile database is detected and no password was supplied.</exception>
    public static async ValueTask<(byte[] Plaintext, AccessEncryptionFormat SourceFormat)> ReadDecryptedAsync(
        Stream source,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));

        _ = source.Seek(0, SeekOrigin.Begin);
        byte[] header = new byte[HeaderLength];
        await source.ReadExactlyAsync(header.AsMemory(), cancellationToken).ConfigureAwait(false);

        // Agile / Standard is the outermost container — when present, decrypt its
        // EncryptedPackage and recurse on the inner ACCDB bytes.
        if (EncryptionManager.IsCompoundFileEncrypted(header))
        {
            _ = source.Seek(0, SeekOrigin.Begin);
            (byte[]? cfbInner, AccessEncryptionFormat cfbFormat) = await EncryptionManager
                .TryDecryptCompoundFileWithFormatAsync(source, header, password, EncryptionManager.OldPasswordArgument, cancellationToken)
                .ConfigureAwait(false);

            if (cfbInner != null)
            {
                await using var innerStream = new MemoryStream(cfbInner, writable: false);
                (byte[] inner, _) = await ReadDecryptedAsync(innerStream, password: default, cancellationToken).ConfigureAwait(false);
                return (inner, cfbFormat);
            }

            throw new InvalidDataException("Compound file does not contain a supported encrypted package.");
        }

        _ = source.Seek(0, SeekOrigin.Begin);
        byte[] rawFile = new byte[source.Length];
        await source.ReadExactlyAsync(rawFile.AsMemory(), cancellationToken).ConfigureAwait(false);

        if (OfficeCryptoAgile.IsFlatAgileEncrypted(rawFile))
        {
            if (password.IsEmpty)
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Access Agile encryption. " +
                    $"Provide the database password via {EncryptionManager.OldPasswordArgument} to open it.");
            }

            return (OfficeCryptoAgile.DecryptFlatDatabase(rawFile, password.Span), AccessEncryptionFormat.AccdbAgile);
        }

        DatabaseFormat fmt = JetFormat.DetectFormat(header);
        AccessEncryptionFormat src = DetectFlatFormat(rawFile, fmt);
        await using var rawStream = new MemoryStream(rawFile, writable: false);
        byte[] plaintext = await ReadFlatDecryptedAsync(rawStream, header, password, cancellationToken)
            .ConfigureAwait(false);

        return (plaintext, src);
    }

    /// <summary>
    /// Encodes <paramref name="plaintext"/> in the requested target encryption
    /// format and returns the resulting on-disk bytes. <paramref name="plaintext"/>
    /// must already be a clean (no-encryption) Jet3/Jet4/ACE database.
    /// </summary>
    /// <param name="plaintext">The plaintext.</param>
    /// <param name="targetFormat">The target format.</param>
    /// <param name="targetPassword">The target password.</param>
    /// <exception cref="InvalidDataException">Thrown when <paramref name="plaintext"/> is shorter than a JET header.</exception>
    /// <exception cref="NotSupportedException">Thrown when <paramref name="targetFormat"/> is incompatible with the database format or is unrecognized.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetPassword"/> is empty for an encrypted target format.</exception>
    public static byte[] ApplyEncryption(
        byte[] plaintext,
        AccessEncryptionFormat targetFormat,
        ReadOnlyMemory<char> targetPassword)
    {
        Guard.NotNull(plaintext, nameof(plaintext));
        if (plaintext.Length < HeaderLength)
        {
            throw new InvalidDataException("Plaintext database is shorter than the JET header.");
        }

        DatabaseFormat fmt = JetFormat.DetectFormat(plaintext);
        int pageSize = fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

        return targetFormat switch
        {
            AccessEncryptionFormat.None => (byte[])plaintext.Clone(),
            AccessEncryptionFormat.Jet4Rc4 when fmt != DatabaseFormat.Jet4Mdb
                => throw new NotSupportedException($"Target format {targetFormat} is only valid for Jet4 (.mdb) databases."),
            AccessEncryptionFormat.AccdbLegacyPassword or
            AccessEncryptionFormat.AccdbAgile or
            AccessEncryptionFormat.AccdbStandard or
            AccessEncryptionFormat.AccdbAgileCfb when fmt != DatabaseFormat.AceAccdb
                => throw new NotSupportedException($"Target format {targetFormat} is only valid for ACE (.accdb) databases."),
            AccessEncryptionFormat.Jet4Rc4 or
            AccessEncryptionFormat.AccdbLegacyPassword or
            AccessEncryptionFormat.AccdbAgile or
            AccessEncryptionFormat.AccdbStandard or
            AccessEncryptionFormat.AccdbAgileCfb when targetPassword.IsEmpty
                => throw new ArgumentException("A non-empty password is required to apply encryption.", nameof(targetPassword)),
            AccessEncryptionFormat.Jet4Rc4 => BuildJet4Rc4(plaintext, pageSize, targetPassword.Span),
            AccessEncryptionFormat.AccdbLegacyPassword => BuildAccdbLegacy(plaintext, targetPassword.Span),
            AccessEncryptionFormat.AccdbAgile => BuildAccdbAgile(plaintext, targetPassword.Span),
            AccessEncryptionFormat.AccdbStandard => BuildAccdbStandard(plaintext, targetPassword.Span),
            AccessEncryptionFormat.AccdbAgileCfb => BuildAccdbAgileCfb(plaintext, targetPassword.Span),
            _ => throw new NotSupportedException($"Unhandled target encryption format: {targetFormat}."),
        };
    }

    /// <summary>Detects the on-disk encryption format of <paramref name="rawFile"/> without modifying it.</summary>
    /// <param name="rawFile">The raw file.</param>
    public static AccessEncryptionFormat Detect(byte[] rawFile)
    {
        if (rawFile == null || rawFile.Length < HeaderLength)
        {
            return AccessEncryptionFormat.None;
        }

        if (EncryptionManager.IsCompoundFileEncrypted(rawFile))
        {
            return IsValidCompoundFileHeader(rawFile)
                ? AccessEncryptionFormat.AccdbAgileCfb
                : throw new InvalidDataException("Invalid compound-file header.");
        }

        DatabaseFormat fmt = JetFormat.DetectFormat(rawFile);
        return DetectFlatFormat(rawFile, fmt);
    }

    internal static byte[] BuildOfficeCryptoCompoundFile(OfficeEncryptedPackage package)
    {
        Guard.NotNull(package.EncryptionInfo, nameof(package.EncryptionInfo));
        Guard.NotNull(package.EncryptedPackage, nameof(package.EncryptedPackage));

        return CompoundFileWriter.BuildOfficeCrypto(
        [
            new KeyValuePair<string, byte[]>("EncryptionInfo", PadEncryptionInfoForRegularFat(package.EncryptionInfo)),
            new KeyValuePair<string, byte[]>("EncryptedPackage", package.EncryptedPackage),
        ]);
    }

    /// <summary>
    /// Reads pages 0..N-1 from a flat (non-CFB) Jet/ACE source, decrypts pages
    /// 1+ using the password-derived keys, and returns a fully plaintext copy
    /// (clean header, no encryption flags, no password residue).
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="header">The header.</param>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">Thrown when the source database is shorter than one whole JET page.</exception>
    private static async ValueTask<byte[]> ReadFlatDecryptedAsync(
        Stream source,
        byte[] header,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken)
    {
        DatabaseFormat fmt = JetFormat.DetectFormat(header);
        int pageSize = fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

        using IPageCodec pageKeys = PageCodecFactory.Open(header, fmt, password, EncryptionManager.OldPasswordArgument);

        long length = source.Length;
        if (length % pageSize != 0)
        {
            // Some Access tools leave a trailing partial page; truncate to the
            // last whole page so we don't try to decrypt a short tail.
            length -= length % pageSize;
        }

        if (length < pageSize)
        {
            throw new InvalidDataException("Source database is shorter than a single JET page.");
        }

        byte[] result = new byte[length];

        // Page 0: copy the header verbatim, then sanitise it.
        _ = source.Seek(0, SeekOrigin.Begin);
        await source.ReadExactlyAsync(result.AsMemory(0, pageSize), cancellationToken).ConfigureAwait(false);
        StripEncryptionFromHeader(result, fmt);

        bool hasPageEncryption = pageKeys.HasEncryption;

        // Pages 1+: read raw, decrypt in place.
        for (long page = 1, offset = pageSize; offset < length; page++, offset += pageSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _ = source.Seek(offset, SeekOrigin.Begin);
            await source.ReadExactlyAsync(result.AsMemory((int)offset, pageSize), cancellationToken).ConfigureAwait(false);

            if (hasPageEncryption)
            {
                pageKeys.Decode(result, checked((int)offset), page, pageSize);
            }
        }

        return result;
    }

    private static byte[] BuildJet4Rc4(byte[] plaintext, int pageSize, ReadOnlySpan<char> password)
    {
        byte[] result = (byte[])plaintext.Clone();

        // Generate a random 32-bit RC4 db key.
        byte[] dbKeyBytes = new byte[4];
        RandomNumberGenerator.Fill(dbKeyBytes);
        uint dbKey = Ru32(dbKeyBytes, 0);

        Buffer.BlockCopy(dbKeyBytes, 0, result, 0x3E, 4);
        CryptographicOperations.ZeroMemory(dbKeyBytes);
        EncodeJet4StylePassword(result, password, useAccdbLegacyMask: false);

        // The 40-byte password area at 0x42 overlaps the encryption flag at
        // 0x62 (offset 32 inside the area), so the flag MUST be written last
        // — after the password encoding — to match the layout produced by
        // Microsoft Access.
        result[0x62] = 0x03;

        using var keys = new Jet4Rc4PageCodec(dbKey);
        EncryptAllPages(result, pageSize, keys);
        return result;
    }

    private static byte[] BuildAccdbLegacy(byte[] plaintext, ReadOnlySpan<char> password)
    {
        byte[] result = (byte[])plaintext.Clone();
        EncodeJet4StylePassword(result, password, useAccdbLegacyMask: true);

        // Flag last — it overlaps the password area (see BuildJet4Rc4).
        result[0x62] = 0x07;

        // The legacy password scheme encrypts no pages.
        return result;
    }

    private static byte[] BuildAccdbAgile(byte[] plaintext, ReadOnlySpan<char> password) => OfficeCryptoAgile.EncryptFlatDatabase(plaintext, password);

    private static byte[] BuildAccdbAgileCfb(byte[] plaintext, ReadOnlySpan<char> password)
    {
        OfficeEncryptedPackage package = OfficeCryptoAgile.Encrypt(plaintext, password);

        return BuildOfficeCryptoCompoundFile(package);
    }

    private static byte[] BuildAccdbStandard(byte[] plaintext, ReadOnlySpan<char> password)
    {
        // Standard wraps a clean (unencrypted) inner ACCDB. The plaintext bytes
        // we have are already in that shape — pass them through
        // OfficeCryptoStandard.Encrypt and emit the resulting CFB document.
        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, password);

        return BuildOfficeCryptoCompoundFile(package);
    }

    private static byte[] PadEncryptionInfoForRegularFat(byte[] encryptionInfo)
    {
        if (encryptionInfo.Length >= Constants.CompoundFile.StandardMiniStreamCutoff)
        {
            return encryptionInfo;
        }

        byte[] padded = new byte[Constants.CompoundFile.StandardMiniStreamCutoff];
        Buffer.BlockCopy(encryptionInfo, 0, padded, 0, encryptionInfo.Length);
        Array.Fill(padded, (byte)' ', encryptionInfo.Length, padded.Length - encryptionInfo.Length);
        return padded;
    }

    private static void EncryptAllPages(byte[] db, int pageSize, IPageCodec keys)
    {
        if (!keys.HasEncryption)
        {
            return;
        }

        long pages = db.Length / pageSize;
        for (long page = 1; page < pages; page++)
        {
            int offset = (int)(page * pageSize);
            keys.Encode(db, offset, page, pageSize);
        }
    }

    /// <summary>
    /// Removes any encryption residue from a freshly-read header so the page
    /// becomes the header Access writes for an unencrypted database. In
    /// the masked header region, sets the
    /// encoding key to 0 and writes the empty-password pattern over the
    /// password area, which also covers the Jet4 / ACE flag byte at
    /// <c>0x62</c>; on Jet3, whose password area ends at <c>0x55</c>, the
    /// flag byte is cleared on its own.
    /// </summary>
    /// <param name="db">The database input.</param>
    /// <param name="fmt">The database format.</param>
    private static void StripEncryptionFromHeader(byte[] db, DatabaseFormat fmt)
    {
        // The encoding key, the password area and the flag byte all lie in
        // the masked header region, so edit them unmasked. Writing raw zeros
        // instead would leave an unmasked key of 0x4EBC8AFB, which Jackcess and
        // mdbtools read as an encrypted file.
        EncryptionManager.TransformHeaderMask(db);
        Array.Clear(db, Constants.DatabaseHeader.EncodingKey, 4);
        EncryptionManager.WriteEmptyHeaderPassword(db, fmt);
        if (fmt == DatabaseFormat.Jet3Mdb)
        {
            db[0x62] = 0;
        }

        EncryptionManager.TransformHeaderMask(db);
    }

    /// <summary>
    /// Encodes <paramref name="password"/> into the 40-byte header password
    /// area at offset <c>0x42</c>, using either the Jet4 XOR mask (Jet4 RC4).
    /// The alternative is the ACCDB legacy password mask.
    /// The encoding is the inverse of <see cref="EncryptionManager"/>'s
    /// <c>DecodeJet4Password</c> / <c>DecodeAccdbPassword</c>.
    /// </summary>
    /// <param name="header">The header.</param>
    /// <param name="password">The password.</param>
    /// <param name="useAccdbLegacyMask">Whether to use the ACCDB legacy password mask instead of the Jet4 mask.</param>
    /// <exception cref="JetLimitationException">Thrown when the password is too long for the fixed header password area.</exception>
    private static void EncodeJet4StylePassword(byte[] header, ReadOnlySpan<char> password, bool useAccdbLegacyMask)
    {
        ReadOnlySpan<byte> mask = useAccdbLegacyMask
            ? EncryptionManager.AccdbLegacyPasswordMaskForWrite
            : EncryptionManager.Jet4PasswordMaskForWrite;

        // The 40-byte password area at 0x42 overlaps the encryption flag at
        // hdr[0x62] (offset 32 inside the area). The flag is rewritten after
        // password encoding, so any password byte at offset 32 or later would
        // be corrupted on read-back. Decoding stops at the first NUL char, so
        // the password (UTF-16LE) plus its NUL terminator must fit in
        // bytes 0..31 — i.e. at most 15 characters.
        const int maxPasswordLength = 15;
        if (password.Length > maxPasswordLength)
        {
            throw new JetLimitationException(
                $"Password is too long for this database format: {password.Length} characters (maximum {maxPasswordLength}). " +
                "The Jet4 RC4, ACCDB legacy password formats all store the password in a fixed " +
                "40-byte header area whose 32nd byte is reused by the encryption flag, restricting the password to " +
                $"{maxPasswordLength} UTF-16 characters. Use AccessEncryptionFormat.AccdbAgile or " +
                "AccessEncryptionFormat.AccdbAgileCfb for longer passwords.");
        }

        Span<byte> padded = stackalloc byte[40];
        if (!password.IsEmpty)
        {
            // Remaining bytes are already zero from stackalloc.
            _ = System.Text.Encoding.Unicode.GetBytes(password, padded);
        }

        for (int i = 0; i < 40; i++)
        {
            header[0x42 + i] = (byte)(padded[i] ^ mask[i] ^ header[0x72 + (i % 4)]);
        }

        CryptographicOperations.ZeroMemory(padded);
    }

    private static AccessEncryptionFormat DetectFlatFormat(byte[] header, DatabaseFormat fmt)
    {
        if (fmt == DatabaseFormat.AceAccdb && OfficeCryptoAgile.IsFlatAgileEncrypted(header))
        {
            return AccessEncryptionFormat.AccdbAgile;
        }

        if (header.Length <= 0x62)
        {
            return AccessEncryptionFormat.None;
        }

        // Raw byte 0x62 is byte 32 of the masked header password area. On a
        // file without a password it is a creation-date byte, so a flag value
        // there counts only when the area holds a password.
        byte flag = header[0x62];

        // 0x02 / 0x03 = RC4 page encryption.
        if (fmt == DatabaseFormat.Jet4Mdb && flag is 0x02 or 0x03 && EncryptionManager.HasHeaderPassword(header, fmt))
        {
            return AccessEncryptionFormat.Jet4Rc4;
        }

        if (fmt == DatabaseFormat.AceAccdb && flag == 0x07 && EncryptionManager.HasHeaderPassword(header, fmt))
        {
            return AccessEncryptionFormat.AccdbLegacyPassword;
        }

        return AccessEncryptionFormat.None;
    }

    private static bool IsValidCompoundFileHeader(byte[] header)
    {
        if (header.Length < 0x20)
        {
            return false;
        }

        ushort majorVersion = Ru16(header, Constants.CompoundFile.HeaderOffsets.MajorVersion);
        ushort sectorShift = Ru16(header, Constants.CompoundFile.HeaderOffsets.SectorShift);

        return (majorVersion == Constants.CompoundFile.V3.MajorVersion && sectorShift == Constants.CompoundFile.V3.SectorShift) ||
            (majorVersion == Constants.CompoundFile.V4.MajorVersion && sectorShift == Constants.CompoundFile.V4.SectorShift);
    }
}
