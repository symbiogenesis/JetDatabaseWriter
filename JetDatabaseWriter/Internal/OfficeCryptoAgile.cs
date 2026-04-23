namespace JetDatabaseWriter;

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Xml;

#pragma warning disable CA5358 // Cipher modes are fixed by the ECMA-376 Agile spec.
#pragma warning disable CA5379 // SHA-512 is the spec-mandated Agile hash; spinCount is honoured.
#pragma warning disable CA5401 // AES-CBC IVs are spec-derived (salt / blockKey / segment index).

/// <summary>
/// ECMA-376 §2.3.4.10–.13 ("Agile") encryption support — used by Office
/// Crypto API encrypted .accdb files (Access 2010 SP1 and later, and
/// Microsoft 365). Reads the EncryptionInfo descriptor, derives the
/// intermediate key from the user password, verifies the password, and
/// decrypts the EncryptedPackage stream in 4096-byte AES-CBC segments.
/// </summary>
internal static class OfficeCryptoAgile
{
    // Agile spec block-key constants (ECMA-376 §2.3.4.13 — "Password Verifier").
    private static readonly byte[] BlockKeyVerifierHashInput =
        [0xFE, 0xA7, 0xD2, 0x76, 0x3B, 0x4B, 0x9E, 0x79];

    private static readonly byte[] BlockKeyVerifierHashValue =
        [0xD7, 0xAA, 0x0F, 0x6D, 0x30, 0x61, 0x34, 0x4E];

    private static readonly byte[] BlockKeyEncryptedKeyValue =
        [0x14, 0x6E, 0x0B, 0xE7, 0xAB, 0xAC, 0xD0, 0xD6];

    /// <summary>
    /// Returns true when the EncryptionInfo header indicates the Agile
    /// (version 4.4 with the 0x40 AgileEncryption flag) variant.
    /// </summary>
    public static bool IsAgileEncryptionInfo(byte[] encryptionInfo)
    {
        if (encryptionInfo == null || encryptionInfo.Length < 8)
        {
            return false;
        }

        ushort major = (ushort)(encryptionInfo[0] | (encryptionInfo[1] << 8));
        ushort minor = (ushort)(encryptionInfo[2] | (encryptionInfo[3] << 8));
        uint flags = (uint)(encryptionInfo[4] | (encryptionInfo[5] << 8) |
            (encryptionInfo[6] << 16) | (encryptionInfo[7] << 24));

        // Agile = (4, 4) with AgileEncryption flag (0x40) set.
        return major == 4 && minor == 4 && (flags & 0x40) != 0;
    }

    /// <summary>
    /// Decrypts an Agile-encrypted Office package. Throws
    /// <see cref="UnauthorizedAccessException"/> when the password fails
    /// verification.
    /// </summary>
    public static byte[] Decrypt(byte[] encryptionInfo, byte[] encryptedPackage, SecureString password)
    {
        Guard.NotNull(encryptionInfo, nameof(encryptionInfo));
        Guard.NotNull(encryptedPackage, nameof(encryptedPackage));

        if (!IsAgileEncryptionInfo(encryptionInfo))
        {
            throw new InvalidDataException(
                "EncryptionInfo header is not in Agile (version 4.4) format.");
        }

        AgileDescriptor descriptor = ParseDescriptor(encryptionInfo);
        byte[] passwordUtf16 = SecureStringToUtf16(password);
        try
        {
            byte[] intermediateKey = ResolvePassword(descriptor, passwordUtf16);
            return DecryptPackage(descriptor, intermediateKey, encryptedPackage);
        }
        finally
        {
            Array.Clear(passwordUtf16, 0, passwordUtf16.Length);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // EncryptionInfo XML parser
    // ════════════════════════════════════════════════════════════════

    private static AgileDescriptor ParseDescriptor(byte[] encryptionInfo)
    {
        // Skip 8-byte header (version + flags), parse the trailing UTF-8 XML.
        string xml = Encoding.UTF8.GetString(encryptionInfo, 8, encryptionInfo.Length - 8);

        var settings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            IgnoreComments = true,
            IgnoreWhitespace = true,
        };

        var d = new AgileDescriptor();
        bool inPasswordKeyEncryptor = false;

        using var sr = new StringReader(xml);
        using var reader = XmlReader.Create(sr, settings);
        while (reader.Read())
        {
            if (reader.NodeType != XmlNodeType.Element)
            {
                continue;
            }

            string local = reader.LocalName;
            if (local == "keyData")
            {
                d.KeyDataSaltSize = ReadIntAttr(reader, "saltSize");
                d.KeyDataBlockSize = ReadIntAttr(reader, "blockSize");
                d.KeyDataKeyBits = ReadIntAttr(reader, "keyBits");
                d.KeyDataHashSize = ReadIntAttr(reader, "hashSize");
                d.KeyDataCipherAlgorithm = reader.GetAttribute("cipherAlgorithm") ?? string.Empty;
                d.KeyDataCipherChaining = reader.GetAttribute("cipherChaining") ?? string.Empty;
                d.KeyDataHashAlgorithm = reader.GetAttribute("hashAlgorithm") ?? string.Empty;
                d.KeyDataSalt = ReadBase64Attr(reader, "saltValue");
            }
            else if (local == "keyEncryptor")
            {
                string? uri = reader.GetAttribute("uri");
                inPasswordKeyEncryptor = uri == "http://schemas.microsoft.com/office/2006/keyEncryptor/password";
            }
            else if (local == "encryptedKey" && inPasswordKeyEncryptor)
            {
                d.SpinCount = ReadIntAttr(reader, "spinCount");
                d.PasswordSaltSize = ReadIntAttr(reader, "saltSize");
                d.PasswordBlockSize = ReadIntAttr(reader, "blockSize");
                d.PasswordKeyBits = ReadIntAttr(reader, "keyBits");
                d.PasswordHashSize = ReadIntAttr(reader, "hashSize");
                d.PasswordCipherAlgorithm = reader.GetAttribute("cipherAlgorithm") ?? string.Empty;
                d.PasswordHashAlgorithm = reader.GetAttribute("hashAlgorithm") ?? string.Empty;
                d.PasswordSalt = ReadBase64Attr(reader, "saltValue");
                d.EncryptedVerifierHashInput = ReadBase64Attr(reader, "encryptedVerifierHashInput");
                d.EncryptedVerifierHashValue = ReadBase64Attr(reader, "encryptedVerifierHashValue");
                d.EncryptedKeyValue = ReadBase64Attr(reader, "encryptedKeyValue");
            }
        }

        if (d.KeyDataSalt.Length == 0 || d.PasswordSalt.Length == 0 ||
            d.EncryptedKeyValue.Length == 0 || d.EncryptedVerifierHashInput.Length == 0 ||
            d.EncryptedVerifierHashValue.Length == 0)
        {
            throw new InvalidDataException("Agile EncryptionInfo XML is missing required fields.");
        }

        if (!string.Equals(d.PasswordHashAlgorithm, "SHA512", StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(d.KeyDataHashAlgorithm, "SHA512", StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException(
                $"Agile encryption hash algorithm '{d.PasswordHashAlgorithm}' / '{d.KeyDataHashAlgorithm}' is not supported (only SHA512).");
        }

        if (!string.Equals(d.PasswordCipherAlgorithm, "AES", StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(d.KeyDataCipherAlgorithm, "AES", StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException(
                $"Agile encryption cipher '{d.PasswordCipherAlgorithm}' / '{d.KeyDataCipherAlgorithm}' is not supported (only AES).");
        }

        return d;
    }

    private static int ReadIntAttr(XmlReader reader, string name)
    {
        string? raw = reader.GetAttribute(name);
        return string.IsNullOrEmpty(raw) ? 0 : int.Parse(raw, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static byte[] ReadBase64Attr(XmlReader reader, string name)
    {
        string? raw = reader.GetAttribute(name);
        return string.IsNullOrEmpty(raw) ? [] : Convert.FromBase64String(raw);
    }

    // ════════════════════════════════════════════════════════════════
    // Password resolution + intermediate-key recovery
    // ════════════════════════════════════════════════════════════════

    private static byte[] ResolvePassword(AgileDescriptor d, byte[] passwordUtf16)
    {
        // 1. Verify the password by decrypting verifierHashInput / Value.
        byte[] verifierInputKey = DeriveKey(passwordUtf16, d.PasswordSalt, BlockKeyVerifierHashInput, d.SpinCount, d.PasswordKeyBits / 8);
        byte[] verifierHashKey = DeriveKey(passwordUtf16, d.PasswordSalt, BlockKeyVerifierHashValue, d.SpinCount, d.PasswordKeyBits / 8);
        byte[] keyValueKey = DeriveKey(passwordUtf16, d.PasswordSalt, BlockKeyEncryptedKeyValue, d.SpinCount, d.PasswordKeyBits / 8);

        byte[] verifierInput = AesCbcDecrypt(d.EncryptedVerifierHashInput, verifierInputKey, d.PasswordSalt);
        byte[] storedHash = AesCbcDecrypt(d.EncryptedVerifierHashValue, verifierHashKey, d.PasswordSalt);

        byte[] expectedHash;
        using (var sha = SHA512.Create())
        {
            expectedHash = sha.ComputeHash(Truncate(verifierInput, d.PasswordSaltSize));
        }

        if (!CryptographicEquals(expectedHash, storedHash, expectedHash.Length))
        {
            throw new UnauthorizedAccessException(
                "The provided password is incorrect for this database.");
        }

        // 2. Recover the intermediate key.
        byte[] intermediate = AesCbcDecrypt(d.EncryptedKeyValue, keyValueKey, d.PasswordSalt);
        return Truncate(intermediate, d.KeyDataKeyBits / 8);
    }

    private static byte[] DeriveKey(byte[] passwordUtf16, byte[] salt, byte[] blockKey, int spinCount, int keyByteCount)
    {
        // Agile PBKDF (ECMA-376 §2.3.4.11):
        //   H0      = SHA512(salt || passwordUtf16Le)
        //   H_(i+1) = SHA512(uint32_le(i) || H_i)
        //   H_final = SHA512(H_spinCount || blockKey)
        //   key     = H_final truncated, or padded with 0x36 to keyByteCount.
        using var sha = SHA512.Create();

        byte[] buf = new byte[salt.Length + passwordUtf16.Length];
        Buffer.BlockCopy(salt, 0, buf, 0, salt.Length);
        Buffer.BlockCopy(passwordUtf16, 0, buf, salt.Length, passwordUtf16.Length);
        byte[] h = sha.ComputeHash(buf);

        byte[] iter = new byte[4 + h.Length];
        for (int i = 0; i < spinCount; i++)
        {
            unchecked
            {
                iter[0] = (byte)i;
                iter[1] = (byte)(i >> 8);
                iter[2] = (byte)(i >> 16);
                iter[3] = (byte)(i >> 24);
            }

            Buffer.BlockCopy(h, 0, iter, 4, h.Length);
            h = sha.ComputeHash(iter);
        }

        byte[] final = new byte[h.Length + blockKey.Length];
        Buffer.BlockCopy(h, 0, final, 0, h.Length);
        Buffer.BlockCopy(blockKey, 0, final, h.Length, blockKey.Length);
        byte[] hf = sha.ComputeHash(final);

        if (hf.Length >= keyByteCount)
        {
            return Truncate(hf, keyByteCount);
        }

        byte[] padded = new byte[keyByteCount];
        Buffer.BlockCopy(hf, 0, padded, 0, hf.Length);
        for (int i = hf.Length; i < keyByteCount; i++)
        {
            padded[i] = 0x36;
        }

        return padded;
    }

    // ════════════════════════════════════════════════════════════════
    // EncryptedPackage decryption (4096-byte AES-CBC segments)
    // ════════════════════════════════════════════════════════════════

    private static byte[] DecryptPackage(AgileDescriptor d, byte[] intermediateKey, byte[] encryptedPackage)
    {
        if (encryptedPackage.Length < 8)
        {
            throw new InvalidDataException("EncryptedPackage stream is too small (missing size prefix).");
        }

        long decryptedSize =
            encryptedPackage[0] |
            ((long)encryptedPackage[1] << 8) |
            ((long)encryptedPackage[2] << 16) |
            ((long)encryptedPackage[3] << 24) |
            ((long)encryptedPackage[4] << 32) |
            ((long)encryptedPackage[5] << 40) |
            ((long)encryptedPackage[6] << 48) |
            ((long)encryptedPackage[7] << 56);

        if (decryptedSize < 0 || decryptedSize > int.MaxValue)
        {
            throw new InvalidDataException($"EncryptedPackage decrypted size out of range: {decryptedSize}.");
        }

        const int segmentSize = 4096;
        int blockSize = d.KeyDataBlockSize;

        byte[] result = new byte[decryptedSize];
        int writeOffset = 0;
        int readOffset = 8;
        int segmentIndex = 0;

        while (writeOffset < decryptedSize)
        {
            int remaining = (int)decryptedSize - writeOffset;
            int segmentLen = Math.Min(segmentSize, remaining);
            int paddedLen = ((segmentLen + blockSize - 1) / blockSize) * blockSize;

            if (readOffset + paddedLen > encryptedPackage.Length)
            {
                throw new InvalidDataException(
                    $"EncryptedPackage stream truncated at segment {segmentIndex}.");
            }

            byte[] iv = SegmentIv(d.KeyDataSalt, segmentIndex, blockSize);
            byte[] cipher = new byte[paddedLen];
            Buffer.BlockCopy(encryptedPackage, readOffset, cipher, 0, paddedLen);
            byte[] plain = AesCbcRaw(cipher, intermediateKey, iv, encrypt: false);
            Buffer.BlockCopy(plain, 0, result, writeOffset, segmentLen);

            readOffset += paddedLen;
            writeOffset += segmentLen;
            segmentIndex++;
        }

        return result;
    }

    private static byte[] SegmentIv(byte[] keyDataSalt, int segmentIndex, int blockSize)
    {
        byte[] data = new byte[keyDataSalt.Length + 4];
        Buffer.BlockCopy(keyDataSalt, 0, data, 0, keyDataSalt.Length);
        unchecked
        {
            data[keyDataSalt.Length] = (byte)segmentIndex;
            data[keyDataSalt.Length + 1] = (byte)(segmentIndex >> 8);
            data[keyDataSalt.Length + 2] = (byte)(segmentIndex >> 16);
            data[keyDataSalt.Length + 3] = (byte)(segmentIndex >> 24);
        }

        byte[] hash;
        using (var sha = SHA512.Create())
        {
            hash = sha.ComputeHash(data);
        }

        return Truncate(hash, blockSize);
    }

    // ════════════════════════════════════════════════════════════════
    // AES-CBC helpers
    // ════════════════════════════════════════════════════════════════

    private static byte[] AesCbcDecrypt(byte[] cipher, byte[] key, byte[] iv)
    {
        // Agile uses raw (no PKCS#7) AES-CBC throughout; the IV is the salt
        // truncated/padded to the AES block size.
        byte[] paddedIv = NormalizeIv(iv, 16);
        return AesCbcRaw(cipher, key, paddedIv, encrypt: false);
    }

    private static byte[] AesCbcRaw(byte[] data, byte[] key, byte[] iv, bool encrypt)
    {
        using var aes = Aes.Create();
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.None;
        aes.Key = key;
        aes.IV = iv;

        using ICryptoTransform t = encrypt ? aes.CreateEncryptor() : aes.CreateDecryptor();
        return t.TransformFinalBlock(data, 0, data.Length);
    }

    private static byte[] NormalizeIv(byte[] salt, int blockSize)
    {
        if (salt.Length == blockSize)
        {
            return salt;
        }

        byte[] iv = new byte[blockSize];
        if (salt.Length >= blockSize)
        {
            Buffer.BlockCopy(salt, 0, iv, 0, blockSize);
        }
        else
        {
            Buffer.BlockCopy(salt, 0, iv, 0, salt.Length);
            for (int i = salt.Length; i < blockSize; i++)
            {
                iv[i] = 0x36;
            }
        }

        return iv;
    }

    // ════════════════════════════════════════════════════════════════
    // Misc helpers
    // ════════════════════════════════════════════════════════════════

    private static byte[] SecureStringToUtf16(SecureString password)
    {
        if (password == null || password.Length == 0)
        {
            return [];
        }

        IntPtr ptr = IntPtr.Zero;
        try
        {
            ptr = Marshal.SecureStringToGlobalAllocUnicode(password);
            byte[] bytes = new byte[password.Length * 2];
            Marshal.Copy(ptr, bytes, 0, bytes.Length);
            return bytes;
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                Marshal.ZeroFreeGlobalAllocUnicode(ptr);
            }
        }
    }

    private static byte[] Truncate(byte[] source, int length)
    {
        if (source.Length == length)
        {
            return source;
        }

        byte[] r = new byte[length];
        Buffer.BlockCopy(source, 0, r, 0, Math.Min(length, source.Length));
        return r;
    }

    private static bool CryptographicEquals(byte[] a, byte[] b, int length)
    {
        if (a.Length < length || b.Length < length)
        {
            return false;
        }

        int diff = 0;
        for (int i = 0; i < length; i++)
        {
            diff |= a[i] ^ b[i];
        }

        return diff == 0;
    }

    private sealed class AgileDescriptor
    {
        public int KeyDataSaltSize { get; set; }

        public int KeyDataBlockSize { get; set; }

        public int KeyDataKeyBits { get; set; }

        public int KeyDataHashSize { get; set; }

        public string KeyDataCipherAlgorithm { get; set; } = string.Empty;

        public string KeyDataCipherChaining { get; set; } = string.Empty;

        public string KeyDataHashAlgorithm { get; set; } = string.Empty;

        public byte[] KeyDataSalt { get; set; } = [];

        public int SpinCount { get; set; }

        public int PasswordSaltSize { get; set; }

        public int PasswordBlockSize { get; set; }

        public int PasswordKeyBits { get; set; }

        public int PasswordHashSize { get; set; }

        public string PasswordCipherAlgorithm { get; set; } = string.Empty;

        public string PasswordHashAlgorithm { get; set; } = string.Empty;

        public byte[] PasswordSalt { get; set; } = [];

        public byte[] EncryptedVerifierHashInput { get; set; } = [];

        public byte[] EncryptedVerifierHashValue { get; set; } = [];

        public byte[] EncryptedKeyValue { get; set; } = [];
    }
}
