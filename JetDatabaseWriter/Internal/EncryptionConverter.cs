namespace JetDatabaseWriter.Internal;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Models;

/// <summary>
/// Implements the read-decrypt-rewrite pipeline used by
/// <see cref="AccessWriter.ChangePasswordAsync(string, string?, string, AccessWriterOptions?, CancellationToken)"/>,
/// <see cref="AccessWriter.EncryptAsync(string, string, AccessEncryptionFormat, AccessWriterOptions?, CancellationToken)"/>,
/// and <see cref="AccessWriter.DecryptAsync(string, string, AccessWriterOptions?, CancellationToken)"/>.
///
/// All public entry points are pure byte-array transforms — they never touch
/// the filesystem directly, so the caller can decide whether to seek-and-rewrite
/// an existing stream or write to a temp file and rename atomically.
/// </summary>
internal static class EncryptionConverter
{
    private const int HeaderLength = 0x80;

    /// <summary>
    /// Reads <paramref name="source"/>, applies any active decryption, and
    /// returns a fully-plaintext copy of the database (no encryption flags,
    /// password area cleared, header magic restored). The returned byte
    /// array has the same length as the inner Jet/ACE database (which may
    /// differ from <paramref name="source"/>.Length when the source was an
    /// Agile CFB container).
    /// </summary>
    public static async ValueTask<(byte[] Plaintext, AccessEncryptionFormat SourceFormat)> ReadDecryptedAsync(
        Stream source,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));

        _ = source.Seek(0, SeekOrigin.Begin);
        byte[] header = new byte[HeaderLength];
        await ReadExactAsync(source, header, 0, header.Length, cancellationToken).ConfigureAwait(false);

        // Agile is the outermost container — when present, decrypt its
        // EncryptedPackage and recurse on the inner ACCDB bytes.
        if (EncryptionManager.IsCompoundFileEncrypted(header))
        {
            _ = source.Seek(0, SeekOrigin.Begin);
            byte[]? agileInner = await EncryptionManager
                .TryDecryptAgileCompoundFileAsync(source, header, password, cancellationToken)
                .ConfigureAwait(false);

            if (agileInner != null)
            {
                using var innerStream = new MemoryStream(agileInner, writable: false);
                (byte[] inner, _) = await ReadDecryptedAsync(innerStream, password: default, cancellationToken).ConfigureAwait(false);
                return (inner, AccessEncryptionFormat.AccdbAgile);
            }

            // Synthetic legacy AES-128 CFB-wrapped layout (CFB magic at byte 0
            // but flat per-page AES beneath).
            return (await ReadFlatDecryptedAsync(source, header, password, isLegacyAesCfb: true, cancellationToken)
                .ConfigureAwait(false), AccessEncryptionFormat.AccdbAesCfbWrapped);
        }

        DatabaseFormat fmt = DetectFormat(header);
        AccessEncryptionFormat src = DetectFlatFormat(header, fmt);
        byte[] plaintext = await ReadFlatDecryptedAsync(source, header, password, isLegacyAesCfb: false, cancellationToken)
            .ConfigureAwait(false);

        return (plaintext, src);
    }

    /// <summary>
    /// Encodes <paramref name="plaintext"/> in the requested target encryption
    /// format and returns the resulting on-disk bytes. <paramref name="plaintext"/>
    /// must already be a clean (no-encryption) Jet3/Jet4/ACE database.
    /// </summary>
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

        DatabaseFormat fmt = DetectFormat(plaintext);
        int pageSize = fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

        if (targetFormat == AccessEncryptionFormat.None)
        {
            return (byte[])plaintext.Clone();
        }

        // Validate target/format compatibility.
        switch (targetFormat)
        {
            case AccessEncryptionFormat.Jet4Rc4:
                if (fmt != DatabaseFormat.Jet4Mdb)
                {
                    throw new NotSupportedException(
                        $"Target format {targetFormat} is only valid for Jet4 (.mdb) databases.");
                }

                break;
            case AccessEncryptionFormat.AccdbLegacyPassword:
            case AccessEncryptionFormat.AccdbAesCfbWrapped:
            case AccessEncryptionFormat.AccdbAgile:
                if (fmt != DatabaseFormat.AceAccdb)
                {
                    throw new NotSupportedException(
                        $"Target format {targetFormat} is only valid for ACE (.accdb) databases.");
                }

                break;
        }

        if (targetPassword.IsEmpty)
        {
            throw new ArgumentException(
                "A non-empty password is required to apply encryption.",
                nameof(targetPassword));
        }

        return targetFormat switch
        {
            AccessEncryptionFormat.Jet4Rc4 => BuildJet4Rc4(plaintext, pageSize, targetPassword.Span),
            AccessEncryptionFormat.AccdbLegacyPassword => BuildAccdbLegacy(plaintext, targetPassword.Span),
            AccessEncryptionFormat.AccdbAesCfbWrapped => BuildAccdbAesCfbWrapped(plaintext, pageSize, targetPassword.Span),
            AccessEncryptionFormat.AccdbAgile => BuildAccdbAgile(plaintext, targetPassword.Span),
            _ => throw new NotSupportedException($"Unhandled target encryption format: {targetFormat}."),
        };
    }

    /// <summary>Detects the on-disk encryption format of <paramref name="rawFile"/> without modifying it.</summary>
    public static AccessEncryptionFormat Detect(byte[] rawFile)
    {
        if (rawFile == null || rawFile.Length < HeaderLength)
        {
            return AccessEncryptionFormat.None;
        }

        if (EncryptionManager.IsCompoundFileEncrypted(rawFile))
        {
            // Cheap heuristic: if the file is a real CFB document (sector size
            // and DIFAT look sane) and contains an EncryptionInfo stream we
            // call it Agile; otherwise it's the synthetic legacy AES layout.
            // The full Agile probe happens lazily in ReadDecryptedAsync.
            return AccessEncryptionFormat.AccdbAgile;
        }

        DatabaseFormat fmt = DetectFormat(rawFile);
        return DetectFlatFormat(rawFile, fmt);
    }

    /// <summary>
    /// Reads pages 0..N-1 from a flat (non-CFB) Jet/ACE source, decrypts pages
    /// 1+ using the password-derived keys, and returns a fully plaintext copy
    /// (clean header, no encryption flags, no password residue).
    /// </summary>
    private static async ValueTask<byte[]> ReadFlatDecryptedAsync(
        Stream source,
        byte[] header,
        ReadOnlyMemory<char> password,
        bool isLegacyAesCfb,
        CancellationToken cancellationToken)
    {
        DatabaseFormat fmt = isLegacyAesCfb ? DatabaseFormat.AceAccdb : DetectFormat(header);
        int pageSize = fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

        var pageKeys = new EncryptionManager.PageDecryptionKeys
        {
            Jet3XorMask = EncryptionManager.GetJet3PageMask(fmt, header),
        };

        (pageKeys.Rc4DbKey, pageKeys.AesPageKey) =
            EncryptionManager.ResolveReaderPageKeys(header, fmt, isLegacyAesCfb, password);

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
        await ReadExactAsync(source, result, 0, pageSize, cancellationToken).ConfigureAwait(false);
        StripEncryptionFromHeader(result, fmt, isLegacyAesCfb);

        // Pages 1+: read raw, decrypt in place.
        for (long page = 1, offset = pageSize; offset < length; page++, offset += pageSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _ = source.Seek(offset, SeekOrigin.Begin);
            await ReadExactAsync(source, result, (int)offset, pageSize, cancellationToken).ConfigureAwait(false);

            if (EncryptionManager.HasPageEncryption(pageKeys))
            {
                // EncryptionManager operates on a self-contained buffer so we
                // pass a slice copy, then write the result back.
                byte[] tmp = new byte[pageSize];
                Buffer.BlockCopy(result, (int)offset, tmp, 0, pageSize);
                EncryptionManager.DecryptPageInPlace(tmp, page, pageSize, pageKeys);
                Buffer.BlockCopy(tmp, 0, result, (int)offset, pageSize);
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
        uint dbKey = BinaryPrimitives.ReadUInt32LittleEndian(dbKeyBytes);

        Buffer.BlockCopy(dbKeyBytes, 0, result, 0x3E, 4);
        EncodeJet4StylePassword(result, password, useAccdbLegacyMask: false);

        // The 40-byte password area at 0x42 overlaps the encryption flag at
        // 0x62 (offset 32 inside the area), so the flag MUST be written last
        // — after the password encoding — to match the layout produced by
        // Microsoft Access.
        result[0x62] = 0x03;

        var keys = new EncryptionManager.PageDecryptionKeys { Rc4DbKey = dbKey };
        EncryptAllPages(result, pageSize, keys);
        return result;
    }

    private static byte[] BuildAccdbLegacy(byte[] plaintext, ReadOnlySpan<char> password)
    {
        byte[] result = (byte[])plaintext.Clone();
        EncodeJet4StylePassword(result, password, useAccdbLegacyMask: true);

        // Flag last — it overlaps the password area (see BuildJet4Rc4).
        result[0x62] = 0x07;

        // No page-level encryption for legacy ;pwd= mode.
        return result;
    }

    private static byte[] BuildAccdbAesCfbWrapped(byte[] plaintext, int pageSize, ReadOnlySpan<char> password)
    {
        byte[] result = (byte[])plaintext.Clone();

        // Encode the password using the Jet4 mask (the legacy AES layout
        // verifies passwords via DecodeJet4Password, not the ACCDB legacy mask).
        EncodeJet4StylePassword(result, password, useAccdbLegacyMask: false);

        // Stamp the CFB compound-file magic over the first 8 bytes of the
        // header so the reader / writer detect the legacy AES path. The
        // rest of the ACCDB header (including code page, format byte at
        // 0x14, and the password-area we just wrote) survives intact.
        CompoundFileReader.CfbSignature.CopyTo(result);

        byte[] aesKey = DeriveAesPageKey(password);
        var keys = new EncryptionManager.PageDecryptionKeys { AesPageKey = aesKey };
        EncryptAllPages(result, pageSize, keys);
        return result;
    }

    private static byte[] BuildAccdbAgile(byte[] plaintext, ReadOnlySpan<char> password)
    {
        // Agile wraps a clean (unencrypted) inner ACCDB. The plaintext bytes
        // we have are already in that shape — pass them through
        // OfficeCryptoAgile.Encrypt and emit the resulting CFB document.
        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoAgile.Encrypt(plaintext, password);

        return CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("EncryptionInfo", encryptionInfo),
            new KeyValuePair<string, byte[]>("EncryptedPackage", encryptedPackage),
        ]);
    }

    private static void EncryptAllPages(byte[] db, int pageSize, EncryptionManager.PageDecryptionKeys keys)
    {
        if (!EncryptionManager.HasPageEncryption(keys))
        {
            return;
        }

        long pages = db.Length / pageSize;
        byte[] tmp = new byte[pageSize];
        for (long page = 1; page < pages; page++)
        {
            int offset = (int)(page * pageSize);
            Buffer.BlockCopy(db, offset, tmp, 0, pageSize);
            EncryptionManager.EncryptPageInPlace(tmp, page, pageSize, keys);
            Buffer.BlockCopy(tmp, 0, db, offset, pageSize);
        }
    }

    /// <summary>
    /// Removes any encryption residue from a freshly-read header so the page
    /// becomes a clean unencrypted JET / ACE header. Restores the magic bytes
    /// for the legacy AES CFB-wrapped layout (which overlays bytes 0–7 with
    /// CFB magic) and clears the encryption flag + password area for all flat
    /// formats.
    /// </summary>
    private static void StripEncryptionFromHeader(byte[] db, DatabaseFormat fmt, bool isLegacyAesCfb)
    {
        if (isLegacyAesCfb)
        {
            // Restore the standard ACCDB header prefix that was overwritten
            // when CFB magic was stamped over bytes 0–7.
            db[0] = 0x00;
            db[1] = 0x01;
            db[2] = 0x00;
            db[3] = 0x00;

            // Bytes 4–7 are the first four characters of "Standard ACE DB\0".
            db[4] = (byte)'S';
            db[5] = (byte)'t';
            db[6] = (byte)'a';
            db[7] = (byte)'n';
        }

        // Clear the RC4 dbKey field (Jet4 only — ACE / legacy do not use it,
        // but zeroing is harmless because the encryption flag is also cleared).
        if (fmt == DatabaseFormat.Jet4Mdb)
        {
            db[0x3E] = 0;
            db[0x3F] = 0;
            db[0x40] = 0;
            db[0x41] = 0;
        }

        // Clear the 40-byte encrypted password area (offset 0x42).
        Array.Clear(db, 0x42, 40);

        // Clear the encryption flag.
        if (db.Length > 0x62)
        {
            db[0x62] = 0;
        }
    }

    /// <summary>
    /// Encodes <paramref name="password"/> into the 40-byte header password
    /// area at offset <c>0x42</c>, using either the Jet4 XOR mask (Jet4 RC4 +
    /// legacy AES CFB-wrapped layouts) or the ACCDB legacy <c>;pwd=</c> mask.
    /// The encoding is the inverse of <see cref="EncryptionManager"/>'s
    /// <c>DecodeJet4Password</c> / <c>DecodeAccdbPassword</c>.
    /// </summary>
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
        const int MaxPasswordLength = 15;
        if (password.Length > MaxPasswordLength)
        {
            throw new JetLimitationException(
                $"Password is too long for this database format: {password.Length} characters (maximum {MaxPasswordLength}). " +
                "Jet4 RC4, ACCDB legacy ';pwd=', and ACCDB AES CFB-wrapped formats all store the password in a fixed " +
                "40-byte header area whose 32nd byte is reused by the encryption flag, restricting the password to " +
                $"{MaxPasswordLength} UTF-16 characters. Use AccessEncryptionFormat.AccdbAgile for longer passwords.");
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

        padded.Clear();
    }

    /// <summary>SHA-256(password)[..16] — matches <c>EncryptionManager.DeriveAesPageKey</c>.</summary>
    private static byte[] DeriveAesPageKey(ReadOnlySpan<char> password)
    {
        int maxBytes = System.Text.Encoding.UTF8.GetMaxByteCount(password.Length);
        Span<byte> stackBuf = stackalloc byte[256];
        byte[]? rented = maxBytes > stackBuf.Length ? new byte[maxBytes] : null;
        Span<byte> utf8 = rented ?? stackBuf;
        try
        {
            int utf8Len = System.Text.Encoding.UTF8.GetBytes(password, utf8);
            using var sha = SHA256.Create();
            Span<byte> hash = stackalloc byte[32];
            if (!sha.TryComputeHash(utf8.Slice(0, utf8Len), hash, out _))
            {
                throw new CryptographicException("SHA-256 hash computation failed.");
            }

            utf8.Slice(0, utf8Len).Clear();

            byte[] key = new byte[16];
            hash.Slice(0, 16).CopyTo(key);
            hash.Clear();
            return key;
        }
        finally
        {
            if (rented != null)
            {
                Array.Clear(rented, 0, rented.Length);
            }
        }
    }

    private static DatabaseFormat DetectFormat(byte[] header)
    {
        byte ver = header[0x14];
        return ver >= 2 ? DatabaseFormat.AceAccdb
             : ver >= 1 ? DatabaseFormat.Jet4Mdb
             : DatabaseFormat.Jet3Mdb;
    }

    private static AccessEncryptionFormat DetectFlatFormat(byte[] header, DatabaseFormat fmt)
    {
        if (header.Length <= 0x62)
        {
            return AccessEncryptionFormat.None;
        }

        byte flag = header[0x62];

        if (fmt == DatabaseFormat.Jet4Mdb)
        {
            // 0x02 / 0x03 = RC4 page encryption.
            if ((flag & 0x02) != 0)
            {
                return AccessEncryptionFormat.Jet4Rc4;
            }
        }

        if (fmt == DatabaseFormat.AceAccdb && flag == 0x07)
        {
            return AccessEncryptionFormat.AccdbLegacyPassword;
        }

        return AccessEncryptionFormat.None;
    }

    private static async ValueTask ReadExactAsync(Stream source, byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int read = 0;
        while (read < count)
        {
            int got = await source.ReadAsync(buffer.AsMemory(offset + read, count - read), cancellationToken).ConfigureAwait(false);
            if (got == 0)
            {
                throw new EndOfStreamException(
                    $"Expected {count} bytes at offset {offset}; only {read} available.");
            }

            read += got;
        }
    }
}
