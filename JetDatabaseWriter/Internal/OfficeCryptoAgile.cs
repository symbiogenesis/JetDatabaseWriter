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
    // Fresh-encryption parameters. Match the descriptors emitted by Office
    // 2016+ for password-protected .accdb files.
    private const int EncryptSaltSize = 16;
    private const int EncryptBlockSize = 16;       // AES block
    private const int EncryptKeyBytes = 32;        // AES-256
    private const int EncryptHashBytes = 64;       // SHA-512
    private const int EncryptSpinCount = 100_000;
    private const int EncryptSegmentSize = 4096;

    // Agile spec block-key constants (ECMA-376 §2.3.4.13 — "Password Verifier").
    private static readonly byte[] BlockKeyVerifierHashInput =
        [0xFE, 0xA7, 0xD2, 0x76, 0x3B, 0x4B, 0x9E, 0x79];

    private static readonly byte[] BlockKeyVerifierHashValue =
        [0xD7, 0xAA, 0x0F, 0x6D, 0x30, 0x61, 0x34, 0x4E];

    private static readonly byte[] BlockKeyEncryptedKeyValue =
        [0x14, 0x6E, 0x0B, 0xE7, 0xAB, 0xAC, 0xD0, 0xD6];

    // Block-key constants for the dataIntegrity HMAC (MS-OFFCRYPTO §2.3.4.14).
    // Emitted on encryption so MS Office files round-trip cleanly; the reader
    // tolerates a placeholder HMAC value because integrity is optional for
    // open-time decryption.
    private static readonly byte[] BlockKeyHmacKey =
        [0x5F, 0xB2, 0xAD, 0x01, 0x0C, 0xB9, 0xE1, 0xF6];

    private static readonly byte[] BlockKeyHmacValue =
        [0xA0, 0x67, 0x7F, 0x02, 0xB2, 0x2C, 0x84, 0x33];

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

    /// <summary>
    /// Encrypts <paramref name="innerPackage"/> with a freshly-generated set of
    /// Agile parameters (AES-256-CBC, SHA-512, <see cref="EncryptSpinCount"/>
    /// PBKDF iterations) and returns the resulting <c>EncryptionInfo</c>
    /// descriptor and <c>EncryptedPackage</c> stream bytes.
    /// </summary>
    public static (byte[] EncryptionInfo, byte[] EncryptedPackage) Encrypt(
        byte[] innerPackage,
        SecureString password)
    {
        Guard.NotNull(innerPackage, nameof(innerPackage));
        Guard.NotNull(password, nameof(password));

        byte[] passwordUtf16 = SecureStringToUtf16(password);
        try
        {
            // Fresh random material on every encrypt: salts and the
            // intermediate key must never be reused across sessions.
            byte[] keyDataSalt = RandomBytes(EncryptSaltSize);
            byte[] passwordSalt = RandomBytes(EncryptSaltSize);
            byte[] verifierHashInput = RandomBytes(EncryptSaltSize);
            byte[] intermediateKey = RandomBytes(EncryptKeyBytes);

            // Pre-derive the three password-bound keys once: each PBKDF
            // iteration is 100k SHA-512s, so we share the iterated state
            // by computing it inline rather than reusing DeriveKey thrice.
            (byte[] verifierInputKey, byte[] verifierHashKey, byte[] keyValueKey,
             byte[] hmacKeyKey, byte[] hmacValueKey) = DeriveAllPasswordKeys(passwordUtf16, passwordSalt);

            // Encrypt the verifier triple. Agile uses the password salt as
            // the IV directly (block-size = salt-size = 16) for these fields.
            byte[] verifierInputCipher = AesCbcRaw(
                PadToBlock(verifierHashInput),
                verifierInputKey,
                NormalizeIv(passwordSalt, EncryptBlockSize),
                encrypt: true);

            byte[] verifierHash;
            using (var sha2 = SHA512.Create())
            {
                verifierHash = sha2.ComputeHash(verifierHashInput);
            }

            byte[] verifierHashCipher = AesCbcRaw(
                PadToBlock(verifierHash),
                verifierHashKey,
                NormalizeIv(passwordSalt, EncryptBlockSize),
                encrypt: true);

            byte[] keyValueCipher = AesCbcRaw(
                PadToBlock(intermediateKey),
                keyValueKey,
                NormalizeIv(passwordSalt, EncryptBlockSize),
                encrypt: true);

            // dataIntegrity placeholder: random HMAC key wrapped with the
            // intermediate key + spec-derived IV; placeholder zero HMAC value.
            byte[] hmacKeyCipher = AesCbcRaw(
                PadToBlock(RandomBytes(EncryptHashBytes)),
                intermediateKey,
                HmacIv(keyDataSalt, BlockKeyHmacKey),
                encrypt: true);

            byte[] hmacValueCipher = AesCbcRaw(
                PadToBlock(new byte[EncryptHashBytes]),
                intermediateKey,
                HmacIv(keyDataSalt, BlockKeyHmacValue),
                encrypt: true);

            string xml = BuildAgileXml(
                keyDataSalt,
                passwordSalt,
                verifierInputCipher,
                verifierHashCipher,
                keyValueCipher,
                hmacKeyCipher,
                hmacValueCipher);

            byte[] xmlBytes = Encoding.UTF8.GetBytes(xml);
            byte[] encryptionInfo = new byte[8 + xmlBytes.Length];

            // 4-byte version (major=4, minor=4) + 4-byte flags (0x40 = AgileEncryption).
            encryptionInfo[0] = 0x04;
            encryptionInfo[1] = 0x00;
            encryptionInfo[2] = 0x04;
            encryptionInfo[3] = 0x00;
            encryptionInfo[4] = 0x40;
            Buffer.BlockCopy(xmlBytes, 0, encryptionInfo, 8, xmlBytes.Length);

            byte[] encryptedPackage = EncryptPackage(intermediateKey, keyDataSalt, innerPackage);
            return (encryptionInfo, encryptedPackage);
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

    // ════════════════════════════════════════════════════════════════
    // Encryption helpers
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Performs the expensive Agile PBKDF (100k SHA-512 iterations) once and
    /// returns all five password-bound keys (verifier input/value, encrypted
    /// key value, HMAC key/value) by mixing each block-key constant into the
    /// shared iterated hash. Avoids running PBKDF five separate times.
    /// </summary>
    private static (byte[] VerifierInput, byte[] VerifierHash, byte[] KeyValue, byte[] HmacKey, byte[] HmacValue)
        DeriveAllPasswordKeys(byte[] passwordUtf16, byte[] passwordSalt)
    {
        using var sha = SHA512.Create();

        // Initial: H0 = SHA512(salt || passwordUtf16Le).
        byte[] init = new byte[passwordSalt.Length + passwordUtf16.Length];
        Buffer.BlockCopy(passwordSalt, 0, init, 0, passwordSalt.Length);
        Buffer.BlockCopy(passwordUtf16, 0, init, passwordSalt.Length, passwordUtf16.Length);
        byte[] h = sha.ComputeHash(init);

        // Iterate: H_(i+1) = SHA512(uint32_le(i) || H_i).
        byte[] iter = new byte[4 + h.Length];
        for (int i = 0; i < EncryptSpinCount; i++)
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

        // Mix in each block-key constant.
        return (
            FinalizeKey(sha, h, BlockKeyVerifierHashInput),
            FinalizeKey(sha, h, BlockKeyVerifierHashValue),
            FinalizeKey(sha, h, BlockKeyEncryptedKeyValue),
            FinalizeKey(sha, h, BlockKeyHmacKey),
            FinalizeKey(sha, h, BlockKeyHmacValue));
    }

    private static byte[] FinalizeKey(SHA512 sha, byte[] iteratedHash, byte[] blockKey)
    {
        byte[] buf = new byte[iteratedHash.Length + blockKey.Length];
        Buffer.BlockCopy(iteratedHash, 0, buf, 0, iteratedHash.Length);
        Buffer.BlockCopy(blockKey, 0, buf, iteratedHash.Length, blockKey.Length);
        byte[] hf = sha.ComputeHash(buf);

        if (hf.Length >= EncryptKeyBytes)
        {
            return Truncate(hf, EncryptKeyBytes);
        }

        byte[] padded = new byte[EncryptKeyBytes];
        Buffer.BlockCopy(hf, 0, padded, 0, hf.Length);
        for (int i = hf.Length; i < EncryptKeyBytes; i++)
        {
            padded[i] = 0x36;
        }

        return padded;
    }

    private static byte[] EncryptPackage(byte[] intermediateKey, byte[] keyDataSalt, byte[] plaintext)
    {
        // 8-byte little-endian decrypted size prefix + per-segment AES-CBC.
        int totalSegments = (plaintext.Length + EncryptSegmentSize - 1) / EncryptSegmentSize;

        // Only the trailing segment can introduce padding.
        int paddedTail = 0;
        if (totalSegments > 0)
        {
            int lastLen = plaintext.Length - ((totalSegments - 1) * EncryptSegmentSize);
            paddedTail = ((lastLen + EncryptBlockSize - 1) / EncryptBlockSize) * EncryptBlockSize;
        }

        int paddedTotal = totalSegments == 0
            ? 0
            : ((totalSegments - 1) * EncryptSegmentSize) + paddedTail;

        byte[] result = new byte[8 + paddedTotal];
        long size = plaintext.Length;
        for (int i = 0; i < 8; i++)
        {
            unchecked
            {
                result[i] = (byte)(size >> (i * 8));
            }
        }

        using var aes = Aes.Create();
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.None;
        aes.Key = intermediateKey;

        int writeOffset = 8;
        for (int seg = 0; seg < totalSegments; seg++)
        {
            int offset = seg * EncryptSegmentSize;
            int segLen = Math.Min(EncryptSegmentSize, plaintext.Length - offset);
            int paddedLen = ((segLen + EncryptBlockSize - 1) / EncryptBlockSize) * EncryptBlockSize;

            byte[] block = new byte[paddedLen];
            Buffer.BlockCopy(plaintext, offset, block, 0, segLen);

            aes.IV = SegmentIv(keyDataSalt, seg, EncryptBlockSize);
            using ICryptoTransform t = aes.CreateEncryptor();
            byte[] cipher = t.TransformFinalBlock(block, 0, paddedLen);
            Buffer.BlockCopy(cipher, 0, result, writeOffset, paddedLen);

            writeOffset += paddedLen;
        }

        return result;
    }

    private static byte[] HmacIv(byte[] keyDataSalt, byte[] blockKey)
    {
        byte[] data = new byte[keyDataSalt.Length + blockKey.Length];
        Buffer.BlockCopy(keyDataSalt, 0, data, 0, keyDataSalt.Length);
        Buffer.BlockCopy(blockKey, 0, data, keyDataSalt.Length, blockKey.Length);
        byte[] hash;
        using (var sha = SHA512.Create())
        {
            hash = sha.ComputeHash(data);
        }

        return Truncate(hash, EncryptBlockSize);
    }

    private static byte[] PadToBlock(byte[] data)
    {
        int padded = ((data.Length + EncryptBlockSize - 1) / EncryptBlockSize) * EncryptBlockSize;
        if (padded == data.Length)
        {
            return data;
        }

        byte[] r = new byte[padded];
        Buffer.BlockCopy(data, 0, r, 0, data.Length);
        return r;
    }

    private static byte[] RandomBytes(int length)
    {
        byte[] b = new byte[length];
        RandomNumberGenerator.Fill(b);
        return b;
    }

    private static string BuildAgileXml(
        byte[] keyDataSalt,
        byte[] passwordSalt,
        byte[] verifierInputCipher,
        byte[] verifierHashCipher,
        byte[] keyValueCipher,
        byte[] hmacKeyCipher,
        byte[] hmacValueCipher)
    {
        return
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<encryption xmlns=\"http://schemas.microsoft.com/office/2006/encryption\" " +
            "xmlns:p=\"http://schemas.microsoft.com/office/2006/keyEncryptor/password\" " +
            "xmlns:c=\"http://schemas.microsoft.com/office/2006/keyEncryptor/certificate\">" +
            $"<keyData saltSize=\"{EncryptSaltSize}\" blockSize=\"{EncryptBlockSize}\" " +
            $"keyBits=\"{EncryptKeyBytes * 8}\" hashSize=\"{EncryptHashBytes}\" " +
            "cipherAlgorithm=\"AES\" cipherChaining=\"ChainingModeCBC\" hashAlgorithm=\"SHA512\" " +
            $"saltValue=\"{Convert.ToBase64String(keyDataSalt)}\"/>" +
            $"<dataIntegrity encryptedHmacKey=\"{Convert.ToBase64String(hmacKeyCipher)}\" " +
            $"encryptedHmacValue=\"{Convert.ToBase64String(hmacValueCipher)}\"/>" +
            "<keyEncryptors>" +
            "<keyEncryptor uri=\"http://schemas.microsoft.com/office/2006/keyEncryptor/password\">" +
            $"<p:encryptedKey spinCount=\"{EncryptSpinCount}\" saltSize=\"{EncryptSaltSize}\" " +
            $"blockSize=\"{EncryptBlockSize}\" keyBits=\"{EncryptKeyBytes * 8}\" " +
            $"hashSize=\"{EncryptHashBytes}\" cipherAlgorithm=\"AES\" cipherChaining=\"ChainingModeCBC\" " +
            $"hashAlgorithm=\"SHA512\" saltValue=\"{Convert.ToBase64String(passwordSalt)}\" " +
            $"encryptedVerifierHashInput=\"{Convert.ToBase64String(verifierInputCipher)}\" " +
            $"encryptedVerifierHashValue=\"{Convert.ToBase64String(verifierHashCipher)}\" " +
            $"encryptedKeyValue=\"{Convert.ToBase64String(keyValueCipher)}\"/>" +
            "</keyEncryptor>" +
            "</keyEncryptors>" +
            "</encryption>";
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
