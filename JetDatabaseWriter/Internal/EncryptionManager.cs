namespace JetDatabaseWriter.Internal;

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Centralizes all JET / ACE / ACCDB encryption logic — header detection,
/// password verification, key derivation, and per-page decryption.
/// </summary>
internal static class EncryptionManager
{
    // Jet3 XOR mask (128 bytes, applied cyclically to pages 1+ when the
    // Office97 password flag is set on a Jet3 .mdb). Sourced from mdbtools
    // HACKING.md.
    private static readonly byte[] Jet3PageXorMask =
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
    private static readonly byte[] Jet4PasswordMask =
    [
        0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
        0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
        0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
        0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
        0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
    ];

    // ACE legacy password mask used for password-only ACCDB files
    // created via DBEngine.CompactDatabase(..., ";pwd=...").
    private static readonly byte[] AccdbLegacyPasswordMask =
    [
        0x1F, 0x9B, 0xB7, 0xCA, 0xD4, 0x24, 0xD0, 0x07,
        0x49, 0x3E, 0x62, 0x1B, 0xF9, 0xD6, 0xB4, 0x9D,
        0xBE, 0xF4, 0x45, 0xCB, 0x1F, 0x12, 0xE1, 0x4C,
        0x9D, 0x94, 0x2D, 0xBE, 0x25, 0xCF, 0x8F, 0xCE,
        0xDE, 0x01, 0x47, 0xA6, 0x78, 0xD5, 0x42, 0xD7,
    ];

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
        SecureString? password)
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
                if (SecureStringUtilities.IsNullOrEmpty(password))
                {
                    throw new UnauthorizedAccessException(
                        "This database is encrypted or password-protected. " +
                        "Provide a password via AccessReaderOptions.Password, or " +
                        "remove the password in Microsoft Access (File > Info > Encrypt with Password) and try again.");
                }

                string storedPassword = DecodeJet4Password(hdr);
                if (!SecureStringUtilities.EqualsPlainText(password, storedPassword))
                {
                    throw new UnauthorizedAccessException(
                        "The provided password is incorrect for this database.");
                }

                if ((encFlag & 0x02) != 0)
                {
                    rc4DbKey = BitConverter.ToUInt32(hdr, 0x3E);
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
                if (SecureStringUtilities.IsNullOrEmpty(password))
                {
                    throw new UnauthorizedAccessException(
                        "This database is password-protected. " +
                        "Provide a password via AccessReaderOptions.Password.");
                }

                string storedPassword = DecodeAccdbPassword(hdr);
                if (!SecureStringUtilities.EqualsPlainText(password, storedPassword))
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
            if (SecureStringUtilities.IsNullOrEmpty(password))
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Access 2007+ AES encryption. " +
                    "Provide the database password via AccessReaderOptions.Password to open it, " +
                    "or remove the password in Microsoft Access (File > Info > Decrypt Database) and try again.");
            }

            // ACCDB uses the same XOR scheme as Jet4 for the header password area.
            string storedPassword = DecodeJet4Password(hdr);
            if (!SecureStringUtilities.EqualsPlainText(password, storedPassword))
            {
                throw new UnauthorizedAccessException(
                    "The provided password is incorrect for this database.");
            }

            aesPageKey = DeriveAesPageKey(password!);
        }

        return (rc4DbKey, aesPageKey);
    }

    /// <summary>
    /// If <paramref name="header"/> has CFB magic and the contained streams
    /// describe an Office Crypto API ("Agile") encrypted package, returns the
    /// decrypted inner ACCDB bytes. Returns <c>null</c> when the file is not
    /// an Agile-encrypted CFB document.
    /// Throws <see cref="UnauthorizedAccessException"/> when an Agile package
    /// is detected but no password was supplied.
    /// </summary>
    public static async ValueTask<byte[]?> TryDecryptAgileCompoundFileAsync(
        Stream stream,
        byte[] header,
        SecureString? password,
        CancellationToken cancellationToken)
    {
        if (!CompoundFileReader.HasCompoundFileMagic(header))
        {
            return null;
        }

        System.Collections.Generic.Dictionary<string, byte[]>? streams = null;
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
            !streams.TryGetValue("EncryptedPackage", out byte[]? encryptedPackage) ||
            !OfficeCryptoAgile.IsAgileEncryptionInfo(encryptionInfo))
        {
            return null;
        }

        if (SecureStringUtilities.IsNullOrEmpty(password))
        {
            throw new UnauthorizedAccessException(
                "This .accdb file is encrypted with Office Crypto API 'Agile' encryption. " +
                "Provide the database password via AccessReaderOptions.Password to open it, " +
                "or remove the password in Microsoft Access (File > Info > Decrypt Database) and try again.");
        }

        return OfficeCryptoAgile.Decrypt(encryptionInfo, encryptedPackage, password!);
    }

    /// <summary>
    /// Applies any active page decryption (Jet3 XOR, Jet4 RC4, ACCDB AES) to
    /// <paramref name="buf"/> in place. A no-op when no keys are configured or
    /// when <paramref name="pageNumber"/> is 0 (the unencrypted header page).
    /// </summary>
    public static void DecryptPageInPlace(byte[] buf, long pageNumber, int pageSize, PageDecryptionKeys keys)
    {
        if (pageNumber < 1 || keys == null)
        {
            return;
        }

        if (keys.Jet3XorMask is { } jet3Mask)
        {
            long fileOffset = pageNumber * pageSize;
            for (int b = 0; b < pageSize; b++)
            {
                buf[b] ^= jet3Mask[(int)((fileOffset + b - pageSize) % jet3Mask.Length)];
            }
        }

        if (keys.Rc4DbKey is uint dbKey)
        {
            byte[] rc4Key = DeriveRc4PageKey(dbKey, (uint)pageNumber);
            Rc4Transform(buf, 0, pageSize, rc4Key);
        }

        if (keys.AesPageKey is { } aesKey)
        {
            AesEcbDecryptInPlace(buf, 0, pageSize, aesKey);
        }
    }

    // ── Crypto primitives ────────────────────────────────────────────

#pragma warning disable CA5351 // MD5 is required by the Jet4 RC4 key derivation spec
    /// <summary>
    /// Derives the RC4 key for a specific page: MD5(dbKey LE + pageNumber LE)[0..4].
    /// </summary>
    private static byte[] DeriveRc4PageKey(uint dbKey, uint pageNumber)
    {
        byte[] input = new byte[8];
        BitConverter.GetBytes(dbKey).CopyTo(input, 0);
        BitConverter.GetBytes(pageNumber).CopyTo(input, 4);
        using var md5 = MD5.Create();
        byte[] hash = md5.ComputeHash(input);
        byte[] key = new byte[4];
        Buffer.BlockCopy(hash, 0, key, 0, 4);
        return key;
    }
#pragma warning restore CA5351

    /// <summary>In-place RC4 transform (encrypt and decrypt are the same operation).</summary>
    private static void Rc4Transform(byte[] data, int offset, int length, byte[] key)
    {
        byte[] s = new byte[256];
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
    /// <summary>In-place AES-128-ECB decryption of a page buffer.</summary>
    private static void AesEcbDecryptInPlace(byte[] data, int offset, int length, byte[] key)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.None;

        using var decryptor = aes.CreateDecryptor();
        byte[] block = new byte[length];
        Buffer.BlockCopy(data, offset, block, 0, length);

        byte[] decrypted = decryptor.TransformFinalBlock(block, 0, length);
        Buffer.BlockCopy(decrypted, 0, data, offset, length);
    }
#pragma warning restore CA5358

    /// <summary>
    /// Decodes the stored Jet4 password from the database header. The 40 bytes
    /// at offset 0x42 are XOR'd with a fixed mask and the 4-byte creation date
    /// at offset 0x72.
    /// </summary>
    private static string DecodeJet4Password(byte[] hdr)
    {
        var decoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            decoded[i] = (byte)(hdr[0x42 + i] ^ Jet4PasswordMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        // Decode as UTF-16LE. Stop at the first null character rather than
        // trimming from the end, because the encryption flag at 0x62 overlaps
        // byte 32 of the password area and may produce a non-null artifact.
        string raw = Encoding.Unicode.GetString(decoded);
        int nullIdx = raw.IndexOf('\0', StringComparison.Ordinal);
        return nullIdx >= 0 ? raw.Substring(0, nullIdx) : raw;
    }

    private static string DecodeAccdbPassword(byte[] hdr)
    {
        var decoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            decoded[i] = (byte)(hdr[0x42 + i] ^ AccdbLegacyPasswordMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        // Offset 0x62 overlaps the password area for 40-byte decode blocks,
        // so stop at the first null character.
        string raw = Encoding.Unicode.GetString(decoded);
        int nullIdx = raw.IndexOf('\0', StringComparison.Ordinal);
        return nullIdx >= 0 ? raw.Substring(0, nullIdx) : raw;
    }

    /// <summary>
    /// Derives a 128-bit AES key from a <see cref="SecureString"/> password
    /// using SHA-256 (truncated to 16 bytes).
    /// </summary>
    private static byte[] DeriveAesPageKey(SecureString password)
    {
        IntPtr ptr = IntPtr.Zero;
        try
        {
            ptr = Marshal.SecureStringToGlobalAllocUnicode(password);
            int charCount = password.Length;

            // Single-pass: read chars from unmanaged memory and encode to UTF-8
            // directly, eliminating the intermediate char buffer and a separate
            // Encoding.UTF8.GetBytes pass.
            Span<byte> utf8 = stackalloc byte[charCount * 3];
            int utf8Len = 0;

            // Batch-read 4 chars (8 bytes) per Marshal call to reduce P/Invoke overhead.
            int i = 0;
            for (; i + 3 < charCount; i += 4)
            {
                long val = Marshal.ReadInt64(ptr, i * 2);
                char c0 = (char)(val & 0xFFFF);
                char c1 = (char)((val >> 16) & 0xFFFF);
                char c2 = (char)((val >> 32) & 0xFFFF);
                char c3 = (char)((val >> 48) & 0xFFFF);

                // Fast path: all 4 chars are ASCII — skip per-char branching.
                if ((c0 | c1 | c2 | c3) < 0x80)
                {
                    utf8[utf8Len] = (byte)c0;
                    utf8[utf8Len + 1] = (byte)c1;
                    utf8[utf8Len + 2] = (byte)c2;
                    utf8[utf8Len + 3] = (byte)c3;
                    utf8Len += 4;
                }
                else
                {
                    Utf8EncodeChar(c0, utf8, ref utf8Len);
                    Utf8EncodeChar(c1, utf8, ref utf8Len);
                    Utf8EncodeChar(c2, utf8, ref utf8Len);
                    Utf8EncodeChar(c3, utf8, ref utf8Len);
                }
            }

            for (; i < charCount; i++)
            {
                char c = (char)Marshal.ReadInt16(ptr, i * 2);
                if (c < 0x80)
                {
                    utf8[utf8Len++] = (byte)c;
                }
                else
                {
                    Utf8EncodeChar(c, utf8, ref utf8Len);
                }
            }

            Span<byte> hash = stackalloc byte[32];
            using var sha = SHA256.Create();
            if (!sha.TryComputeHash(utf8.Slice(0, utf8Len), hash, out _))
            {
                throw new CryptographicException("SHA-256 hash computation failed.");
            }

            utf8[..utf8Len].Clear();

            byte[] key = new byte[16];
            hash[..16].CopyTo(key);
            hash.Clear();
            return key; // AES-128
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                Marshal.ZeroFreeGlobalAllocUnicode(ptr);
            }
        }
    }

    /// <summary>Encodes a single BMP character as UTF-8 into the destination span.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Utf8EncodeChar(char c, Span<byte> buf, ref int pos)
    {
        if (c < 0x80)
        {
            buf[pos++] = (byte)c;
        }
        else if (c < 0x800)
        {
            buf[pos++] = (byte)(0xC0 | (c >> 6));
            buf[pos++] = (byte)(0x80 | (c & 0x3F));
        }
        else
        {
            buf[pos++] = (byte)(0xE0 | (c >> 12));
            buf[pos++] = (byte)(0x80 | ((c >> 6) & 0x3F));
            buf[pos++] = (byte)(0x80 | (c & 0x3F));
        }
    }

    /// <summary>
    /// Mutable holder for the three page-decryption keys an open database may need.
    /// Populated during reader construction; consulted by every page read.
    /// </summary>
    public sealed class PageDecryptionKeys
    {
        /// <summary>Gets or sets the Jet3 XOR mask, non-null when Jet3 page encryption is active.</summary>
        public byte[]? Jet3XorMask { get; set; }

        /// <summary>Gets or sets the Jet4 RC4 database key (header offset 0x3E), non-null when RC4 page encryption is active.</summary>
        public uint? Rc4DbKey { get; set; }

        /// <summary>Gets or sets the AES-128 page decryption key, non-null when ACCDB CFB AES page encryption is active.</summary>
        public byte[]? AesPageKey { get; set; }
    }
}
