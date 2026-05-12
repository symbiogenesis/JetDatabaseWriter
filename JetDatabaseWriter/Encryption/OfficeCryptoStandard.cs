namespace JetDatabaseWriter.Encryption;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using JetDatabaseWriter.Infrastructure;

#pragma warning disable CA5350 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
#pragma warning disable CA5358 // AES-CBC with IV=0 is the spec-mandated mode for Standard encryption.
#pragma warning disable CA5401 // IV is fixed to zeros per the Standard encryption specification.

/// <summary>
/// MS-OFFCRYPTO §2.3.6 "Standard" encryption support — used by Office 2007
/// (Access 2007) password-encrypted .accdb files. Parses the binary
/// EncryptionInfo descriptor, derives the AES-128 key from the user password
/// via SHA-1 PBKDF, verifies the password, and decrypts the EncryptedPackage
/// stream using AES-128-CBC with a zero IV.
/// </summary>
internal static class OfficeCryptoStandard
{
    // AlgID constants per MS-OFFCRYPTO §2.3.6.1.
    private const int AlgIdAes128 = 0x6601;
    private const int AlgIdAes192 = 0x6602;
    private const int AlgIdAes256 = 0x6603;

    // AlgIDHash constants.
    private const int AlgIdHashSha1 = 0x8004;

    /// <summary>
    /// Decrypts a Standard-encrypted Office package. Throws
    /// <see cref="UnauthorizedAccessException"/> when the password fails
    /// verification.
    /// </summary>
    public static byte[] Decrypt(byte[] encryptionInfo, byte[] encryptedPackage, ReadOnlySpan<char> password)
    {
        Guard.NotNull(encryptionInfo, nameof(encryptionInfo));
        Guard.NotNull(encryptedPackage, nameof(encryptedPackage));

        if (!OfficeCryptoAgile.IsStandardEncryptionInfo(encryptionInfo))
        {
            throw new InvalidDataException(
                "EncryptionInfo header is not in Standard (version 3.2 or 4.2) format.");
        }

        StandardDescriptor descriptor = ParseDescriptor(encryptionInfo);
        byte[] key = DeriveKey(password, descriptor);

        VerifyPassword(key, descriptor);
        return DecryptPackage(key, encryptedPackage, descriptor.KeyBits / 8);
    }

    /// <summary>
    /// Encrypts <paramref name="innerPackage"/> with Standard encryption
    /// parameters (AES-128-CBC, SHA-1, 50000 spin iterations) and returns
    /// the resulting <c>EncryptionInfo</c> descriptor and
    /// <c>EncryptedPackage</c> stream bytes.
    /// </summary>
    public static (byte[] EncryptionInfo, byte[] EncryptedPackage) Encrypt(
        byte[] innerPackage,
        ReadOnlySpan<char> password)
    {
        Guard.NotNull(innerPackage, nameof(innerPackage));

        const int keyBits = 128;
        const int spinCount = 50_000;

        byte[] salt = new byte[16];
        RandomNumberGenerator.Fill(salt);

        byte[] verifier = new byte[16];
        RandomNumberGenerator.Fill(verifier);

        byte[] key = DeriveKeyCore(password, salt, keyBits / 8, spinCount);

        // Encrypt verifier (16 bytes → 16 bytes via AES-CBC, IV=0).
        byte[] encryptedVerifier = AesCbcZeroIv(verifier, key, encrypt: true);

        // Hash the verifier with SHA-1 → 20 bytes, then pad to 32 for encryption.
        byte[] verifierHash;
        using (var sha = SHA1.Create())
        {
            verifierHash = sha.ComputeHash(verifier);
        }

        // Pad to 32 bytes (next multiple of AES block size above 20).
        byte[] verifierHashPadded = new byte[32];
        Buffer.BlockCopy(verifierHash, 0, verifierHashPadded, 0, verifierHash.Length);
        byte[] encryptedVerifierHash = AesCbcZeroIv(verifierHashPadded, key, encrypt: true);

        // Build EncryptionInfo binary blob.
        byte[] encryptionInfo = BuildEncryptionInfo(salt, encryptedVerifier, encryptedVerifierHash, keyBits, spinCount);

        // Build EncryptedPackage: 8-byte LE size prefix + AES-CBC encrypted data.
        byte[] encryptedPackage = EncryptPackage(innerPackage, key);

        return (encryptionInfo, encryptedPackage);
    }

    // ════════════════════════════════════════════════════════════════
    // EncryptionInfo binary parser (MS-OFFCRYPTO §2.3.6.1)
    // ════════════════════════════════════════════════════════════════

    private static StandardDescriptor ParseDescriptor(byte[] encryptionInfo)
    {
        // Layout after the 8-byte version+flags header:
        //   [8..11]  HeaderSize (uint32 LE)
        //   [12..]   EncryptionHeader (HeaderSize bytes)
        //   [12+HeaderSize..] EncryptionVerifier
        if (encryptionInfo.Length < 12)
        {
            throw new InvalidDataException("EncryptionInfo is too short to contain a header size.");
        }

        int headerSize = BinaryPrimitives.ReadInt32LittleEndian(encryptionInfo.AsSpan(8, 4));
        int headerStart = 12;
        int headerEnd = headerStart + headerSize;

        if (headerEnd > encryptionInfo.Length || headerSize < 28)
        {
            throw new InvalidDataException(
                $"EncryptionHeader size ({headerSize}) is invalid or exceeds stream length.");
        }

        ReadOnlySpan<byte> hdr = encryptionInfo.AsSpan(headerStart, headerSize);

        // Parse EncryptionHeader fields.
        // uint flags = BinaryPrimitives.ReadUInt32LittleEndian(hdr.Slice(0, 4));
        // uint sizeExtra = BinaryPrimitives.ReadUInt32LittleEndian(hdr.Slice(4, 4));
        int algId = BinaryPrimitives.ReadInt32LittleEndian(hdr.Slice(8, 4));
        int algIdHash = BinaryPrimitives.ReadInt32LittleEndian(hdr.Slice(12, 4));
        int keyBits = BinaryPrimitives.ReadInt32LittleEndian(hdr.Slice(16, 4));

        if (algId != AlgIdAes128 && algId != AlgIdAes192 && algId != AlgIdAes256)
        {
            throw new NotSupportedException(
                $"Standard encryption AlgID 0x{algId:X4} is not supported (only AES-128/192/256).");
        }

        if (algIdHash != AlgIdHashSha1)
        {
            throw new NotSupportedException(
                $"Standard encryption AlgIDHash 0x{algIdHash:X4} is not supported (only SHA-1 0x8004).");
        }

        // Parse EncryptionVerifier (immediately after EncryptionHeader).
        int verifierStart = headerEnd;
        if (encryptionInfo.Length < verifierStart + 52)
        {
            throw new InvalidDataException("EncryptionInfo is too short to contain EncryptionVerifier.");
        }

        ReadOnlySpan<byte> ver = encryptionInfo.AsSpan(verifierStart);
        int saltSize = BinaryPrimitives.ReadInt32LittleEndian(ver.Slice(0, 4));
        if (saltSize != 16)
        {
            throw new InvalidDataException($"Standard encryption salt size must be 16, got {saltSize}.");
        }

        byte[] salt = ver.Slice(4, 16).ToArray();
        byte[] encryptedVerifier = ver.Slice(20, 16).ToArray();
        int verifierHashSize = BinaryPrimitives.ReadInt32LittleEndian(ver.Slice(36, 4));

        // EncryptedVerifierHash is keyBits/8 bytes for the key length,
        // but always at least 32 bytes (padded to AES block boundary above SHA-1's 20-byte output).
        int encryptedVerifierHashLen = keyBits / 8;
        if (encryptedVerifierHashLen < 32)
        {
            encryptedVerifierHashLen = 32;
        }

        if (encryptionInfo.Length < verifierStart + 40 + encryptedVerifierHashLen)
        {
            throw new InvalidDataException("EncryptionInfo is too short to contain EncryptedVerifierHash.");
        }

        byte[] encryptedVerifierHash = ver.Slice(40, encryptedVerifierHashLen).ToArray();

        // SpinCount: per MS-OFFCRYPTO §2.3.6.2, Standard encryption uses 50000
        // iterations. The spin count is not stored in the binary EncryptionInfo
        // for version (3,2)/(4,2) — it is implied.
        // However, some implementations may vary. We use the standard 50000.
        int spinCount = 50_000;

        return new StandardDescriptor
        {
            AlgId = algId,
            KeyBits = keyBits,
            SpinCount = spinCount,
            Salt = salt,
            EncryptedVerifier = encryptedVerifier,
            VerifierHashSize = verifierHashSize,
            EncryptedVerifierHash = encryptedVerifierHash,
        };
    }

    // ════════════════════════════════════════════════════════════════
    // Key derivation (MS-OFFCRYPTO §2.3.6.2)
    // ════════════════════════════════════════════════════════════════

    private static byte[] DeriveKey(ReadOnlySpan<char> password, StandardDescriptor d)
    {
        return DeriveKeyCore(password, d.Salt, d.KeyBits / 8, d.SpinCount);
    }

    private static byte[] DeriveKeyCore(ReadOnlySpan<char> password, byte[] salt, int keyByteCount, int spinCount)
    {
        // Step 1: H0 = SHA1(salt || password_UTF16LE)
        byte[] passwordBytes = Encoding.Unicode.GetBytes(password.ToArray());
        try
        {
            byte[] h;
            using (var sha = SHA1.Create())
            {
                byte[] initial = new byte[salt.Length + passwordBytes.Length];
                Buffer.BlockCopy(salt, 0, initial, 0, salt.Length);
                Buffer.BlockCopy(passwordBytes, 0, initial, salt.Length, passwordBytes.Length);
                h = sha.ComputeHash(initial);

                // Step 2: For i = 0 to spinCount-1: H = SHA1(LE32(i) || H)
                byte[] iterBuf = new byte[4 + 20]; // 4-byte iterator + 20-byte hash
                for (int i = 0; i < spinCount; i++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(iterBuf.AsSpan(0, 4), i);
                    Buffer.BlockCopy(h, 0, iterBuf, 4, h.Length);
                    h = sha.ComputeHash(iterBuf);
                }

                // Step 3: Hderived = SHA1(H || blockKey) where blockKey = 0x00000000
                byte[] finalBuf = new byte[h.Length + 4];
                Buffer.BlockCopy(h, 0, finalBuf, 0, h.Length);

                // blockKey bytes are already zero from allocation.
                h = sha.ComputeHash(finalBuf);
            }

            // Step 4: Derive key of required length.
            // cbHash = 20 (SHA-1), cbRequiredKeyLength = keyByteCount
            const int cbHash = 20;
            if (keyByteCount <= cbHash)
            {
                // Truncate to required length.
                byte[] key = new byte[keyByteCount];
                Buffer.BlockCopy(h, 0, key, 0, keyByteCount);
                return key;
            }
            else
            {
                // Extend using X1/X2 derivation (MS-OFFCRYPTO §2.3.6.2).
                byte[] derivedBuf = new byte[64];
                for (int i = 0; i < 64; i++)
                {
                    derivedBuf[i] = (byte)(i < cbHash ? (h[i] ^ 0x36) : 0x36);
                }

                byte[] x1;
                using (var sha = SHA1.Create())
                {
                    x1 = sha.ComputeHash(derivedBuf);
                }

                for (int i = 0; i < 64; i++)
                {
                    derivedBuf[i] = (byte)(i < cbHash ? (h[i] ^ 0x5C) : 0x5C);
                }

                byte[] x2;
                using (var sha = SHA1.Create())
                {
                    x2 = sha.ComputeHash(derivedBuf);
                }

                byte[] x3 = new byte[x1.Length + x2.Length];
                Buffer.BlockCopy(x1, 0, x3, 0, x1.Length);
                Buffer.BlockCopy(x2, 0, x3, x1.Length, x2.Length);

                byte[] key = new byte[keyByteCount];
                Buffer.BlockCopy(x3, 0, key, 0, keyByteCount);
                return key;
            }
        }
        finally
        {
            Array.Clear(passwordBytes, 0, passwordBytes.Length);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Password verification (MS-OFFCRYPTO §2.3.6.3)
    // ════════════════════════════════════════════════════════════════

    private static void VerifyPassword(byte[] key, StandardDescriptor d)
    {
        // Decrypt the 16-byte EncryptedVerifier → plaintext verifier.
        byte[] verifier = AesCbcZeroIv(d.EncryptedVerifier, key, encrypt: false);

        // Decrypt the EncryptedVerifierHash → padded hash.
        byte[] verifierHash = AesCbcZeroIv(d.EncryptedVerifierHash, key, encrypt: false);

        // Compute expected hash: SHA1(verifier).
        byte[] expectedHash;
        using (var sha = SHA1.Create())
        {
            expectedHash = sha.ComputeHash(verifier);
        }

        // Compare first VerifierHashSize bytes (20 for SHA-1).
        int compareLen = Math.Min(d.VerifierHashSize, expectedHash.Length);
        if (!CryptographicEquals(expectedHash, verifierHash, compareLen))
        {
            throw new UnauthorizedAccessException(
                "The provided password is incorrect for this database.");
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Package decryption / encryption
    // ════════════════════════════════════════════════════════════════

    private static byte[] DecryptPackage(byte[] key, byte[] encryptedPackage, int keyByteCount)
    {
        if (encryptedPackage.Length < 8)
        {
            throw new InvalidDataException("EncryptedPackage stream is too small (missing size prefix).");
        }

        long decryptedSize = BinaryPrimitives.ReadInt64LittleEndian(encryptedPackage.AsSpan(0, 8));
        if (decryptedSize < 0 || decryptedSize > int.MaxValue)
        {
            throw new InvalidDataException($"EncryptedPackage decrypted size out of range: {decryptedSize}.");
        }

        int cipherLen = encryptedPackage.Length - 8;

        // Pad cipher length to AES block boundary if needed (shouldn't happen
        // with well-formed files, but be defensive).
        int paddedLen = ((cipherLen + 15) / 16) * 16;
        byte[] cipherBlock = new byte[paddedLen];
        Buffer.BlockCopy(encryptedPackage, 8, cipherBlock, 0, cipherLen);

        byte[] plaintext = AesCbcZeroIv(cipherBlock, key, encrypt: false);

        // Truncate to the declared decrypted size.
        if (plaintext.Length == (int)decryptedSize)
        {
            return plaintext;
        }

        byte[] result = new byte[(int)decryptedSize];
        Buffer.BlockCopy(plaintext, 0, result, 0, (int)decryptedSize);
        return result;
    }

    private static byte[] EncryptPackage(byte[] plaintext, byte[] key)
    {
        // Pad plaintext to AES block boundary.
        int paddedLen = ((plaintext.Length + 15) / 16) * 16;
        byte[] padded = new byte[paddedLen];
        Buffer.BlockCopy(plaintext, 0, padded, 0, plaintext.Length);

        byte[] cipher = AesCbcZeroIv(padded, key, encrypt: true);

        // Prepend 8-byte LE size prefix.
        byte[] result = new byte[8 + cipher.Length];
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(0, 8), plaintext.Length);
        Buffer.BlockCopy(cipher, 0, result, 8, cipher.Length);
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    // EncryptionInfo builder (for Encrypt path)
    // ════════════════════════════════════════════════════════════════

    private static byte[] BuildEncryptionInfo(byte[] salt, byte[] encryptedVerifier, byte[] encryptedVerifierHash, int keyBits, int spinCount)
    {
        // CSPName: "Microsoft Enhanced RSA and AES Cryptographic Provider\0" in UTF-16LE.
        string cspName = "Microsoft Enhanced RSA and AES Cryptographic Provider";
        byte[] cspNameBytes = Encoding.Unicode.GetBytes(cspName + '\0');

        // EncryptionHeader: 7 fixed uint32 fields (28 bytes) + CSPName.
        int headerSize = 28 + cspNameBytes.Length;

        // EncryptionVerifier: saltSize(4) + salt(16) + encVerifier(16) + hashSize(4) + encVerifierHash(32).
        int verifierSize = 4 + 16 + 16 + 4 + encryptedVerifierHash.Length;

        // Total: version(4) + flags(4) + headerSize(4) + header + verifier.
        int totalSize = 4 + 4 + 4 + headerSize + verifierSize;
        byte[] info = new byte[totalSize];
        int pos = 0;

        // Version: (4, 2) for Standard encryption with mandatory AES+SHA1.
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(pos, 2), 4);
        pos += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(pos, 2), 2);
        pos += 2;

        // Flags: fCryptoAPI (0x04) | fAES (0x20) = 0x24.
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos, 4), 0x24);
        pos += 4;

        // HeaderSize.
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos, 4), headerSize);
        pos += 4;

        // EncryptionHeader fields.
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos, 4), 0x24); // Flags (same)
        pos += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos, 4), 0); // SizeExtra
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos, 4), AlgIdAes128); // AlgID
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos, 4), AlgIdHashSha1); // AlgIDHash
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos, 4), keyBits); // KeySize
        pos += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos, 4), 0x18); // ProviderType (AES)
        pos += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos, 4), 0); // Reserved1
        pos += 4;

        // Reserved2 is not stored separately — it's part of the CSPName area.
        Buffer.BlockCopy(cspNameBytes, 0, info, pos, cspNameBytes.Length);
        pos += cspNameBytes.Length;

        // EncryptionVerifier.
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos, 4), 16); // SaltSize
        pos += 4;
        Buffer.BlockCopy(salt, 0, info, pos, 16); // Salt
        pos += 16;
        Buffer.BlockCopy(encryptedVerifier, 0, info, pos, 16); // EncryptedVerifier
        pos += 16;
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos, 4), 20); // VerifierHashSize (SHA-1)
        pos += 4;
        Buffer.BlockCopy(encryptedVerifierHash, 0, info, pos, encryptedVerifierHash.Length);

        return info;
    }

    // ════════════════════════════════════════════════════════════════
    // AES-CBC helper (IV = all zeros, per MS-OFFCRYPTO Standard spec)
    // ════════════════════════════════════════════════════════════════

    private static byte[] AesCbcZeroIv(byte[] data, byte[] key, bool encrypt)
    {
        Aes? aes = Aes.Create();
#pragma warning disable CA1508 // InferSharp treats Aes.Create as unknown/null-capable.
        if (aes is null)
        {
            throw new CryptographicException("AES provider creation failed.");
        }
#pragma warning restore CA1508

        using (aes)
        {
#if NET6_0_OR_GREATER
            aes.Key = key;
            ReadOnlySpan<byte> zeroIv = stackalloc byte[16];
            return encrypt
                ? aes.EncryptCbc(data, zeroIv, PaddingMode.None)
                : aes.DecryptCbc(data, zeroIv, PaddingMode.None);
#else
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.None;
            aes.Key = key;
            aes.IV = new byte[16]; // All zeros.

            ICryptoTransform? transform = encrypt ? aes.CreateEncryptor() : aes.CreateDecryptor();
            if (transform is null)
            {
                throw new CryptographicException("AES transform creation failed.");
            }

            using (transform)
            {
                byte[]? result = transform.TransformFinalBlock(data, 0, data.Length);
                return result ?? throw new CryptographicException("AES transform returned no data.");
            }
#endif
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Misc helpers
    // ════════════════════════════════════════════════════════════════

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

    private sealed class StandardDescriptor
    {
        public int AlgId { get; set; }

        public int KeyBits { get; set; }

        public int SpinCount { get; set; }

        public byte[] Salt { get; set; } = [];

        public byte[] EncryptedVerifier { get; set; } = [];

        public int VerifierHashSize { get; set; }

        public byte[] EncryptedVerifierHash { get; set; } = [];
    }
}
