namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using JetDatabaseWriter.Tests.Core;

#pragma warning disable CA5358 // ECMA-376 Agile encryption uses fixed cipher modes per spec.
#pragma warning disable CA5379 // SHA-512 is the spec-mandated hash for Agile here.
#pragma warning disable CA5401 // AES-CBC IVs are spec-derived (salt/blockKey) for fixture interoperability.

/// <summary>
/// Builds a real, ECMA-376 §2.3.4 "Agile"–encrypted .accdb fixture in
/// memory. The output is a CFB v4 (4096-byte sector) compound document
/// containing two streams:
///   • <c>EncryptionInfo</c>  — version (4,4) header + UTF-8 XML descriptor
///   • <c>EncryptedPackage</c> — 8-byte LE size + AES-256-CBC encrypted
///                               segments (4096 bytes each)
///
/// Used exclusively by <see cref="AgileEncryptionTests"/>. Once the
/// production reader implements Agile decryption, the test public-API
/// assertions in that test class verify the round-trip.
/// </summary>
internal static class AgileEncryptionFixtureBuilder
{
    // ── Agile parameters (must match the XML descriptor we emit) ────────

    private const int SaltSize = 16;
    private const int BlockSize = 16;          // AES block
    private const int KeyBytes = 32;           // AES-256
    private const int HashBytes = 64;          // SHA-512
    private const int SpinCount = 100_000;
    private const int SegmentSize = 4096;

    // ── CFB constants ───────────────────────────────────────────────────

    private const uint FreeSect = 0xFFFFFFFFu;
    private const uint EndOfChain = 0xFFFFFFFEu;
    private const uint FatSect = 0xFFFFFFFDu;
    private const int CfbSectorSize = 4096;    // major version 4
    private const int DirEntrySize = 128;

    // CFB v4 magic signature (MS-CFB §2.2). Cached so we don't reallocate on every header write.
    private static readonly byte[] CfbMagic =
        [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];

    // ── Agile spec block-key constants (ECMA-376 §2.3.4.13) ─────────────

    private static readonly byte[] BlockKeyVerifierHashInput =
        [0xFE, 0xA7, 0xD2, 0x76, 0x3B, 0x4B, 0x9E, 0x79];

    private static readonly byte[] BlockKeyVerifierHashValue =
        [0xD7, 0xAA, 0x0F, 0x6D, 0x30, 0x61, 0x34, 0x4E];

    private static readonly byte[] BlockKeyEncryptedKeyValue =
        [0x14, 0x6E, 0x0B, 0xE7, 0xAB, 0xAC, 0xD0, 0xD6];

    private static readonly byte[] BlockKeyHmacKey =
        [0x5F, 0xB2, 0xAD, 0x01, 0x0C, 0xB9, 0xE1, 0xF6];

    private static readonly byte[] BlockKeyHmacValue =
        [0xA0, 0x67, 0x7F, 0x02, 0xB2, 0x2C, 0x84, 0x33];

    /// <summary>
    /// Returns a deterministic <see cref="Parameters"/> instance suitable
    /// for self-testing the key-derivation primitive without rebuilding
    /// the full CFB.
    /// </summary>
    public static Parameters DeterministicParameters() => new()
    {
        KeyDataSalt = Repeated(0x11, SaltSize),
        PasswordSalt = Repeated(0x22, SaltSize),
        VerifierHashInput = Repeated(0x33, SaltSize),
        IntermediateKey = Repeated(0x44, KeyBytes),
    };

    /// <summary>
    /// Decrypts the Agile <c>encryptedVerifierHashInput</c> field using the
    /// supplied parameters and password.
    /// </summary>
    public static byte[] DecryptVerifierHashInput(Parameters p, string password)
    {
        byte[] encrypted = EncryptVerifierHashInput(p, password);
        byte[] derived = DeriveKey(password, p.PasswordSalt, BlockKeyVerifierHashInput);
        byte[] decrypted = AesCbc(encrypted, derived, p.PasswordSalt, encrypt: false);
        return Truncate(decrypted, SaltSize);
    }

    /// <summary>
    /// Decrypts the Agile <c>encryptedVerifierHashValue</c> field using the
    /// supplied parameters and password.
    /// </summary>
    public static byte[] DecryptVerifierHashValue(Parameters p, string password)
    {
        byte[] encrypted = EncryptVerifierHashValue(p, password);
        byte[] derived = DeriveKey(password, p.PasswordSalt, BlockKeyVerifierHashValue);
        byte[] decrypted = AesCbc(encrypted, derived, p.PasswordSalt, encrypt: false);
        return Truncate(decrypted, HashBytes);
    }

    /// <summary>
    /// Builds the in-memory Agile-encrypted .accdb fixture wrapping <paramref name="innerAccdb"/>.
    /// </summary>
    public static byte[] Build(byte[] innerAccdb, string password)
    {
        var p = new Parameters
        {
            KeyDataSalt = RandomBytes(SaltSize),
            PasswordSalt = RandomBytes(SaltSize),
            VerifierHashInput = RandomBytes(SaltSize),
            IntermediateKey = RandomBytes(KeyBytes),
        };

        byte[] encryptionInfo = BuildEncryptionInfo(p, password);
        byte[] encryptedPackage = BuildEncryptedPackage(p, innerAccdb);

        return BuildCompoundFile(encryptionInfo, encryptedPackage);
    }

    // ═══════════════════════════════════════════════════════════════════
    // EncryptionInfo (version 4.4 + Agile XML)
    // ═══════════════════════════════════════════════════════════════════

    private static byte[] BuildEncryptionInfo(Parameters p, string password)
    {
        byte[] verifierEncInput = EncryptVerifierHashInput(p, password);
        byte[] verifierEncValue = EncryptVerifierHashValue(p, password);
        byte[] keyEncValue = EncryptIntermediateKey(p, password);

        // Data integrity (MS-OFFCRYPTO §2.3.4.14): a random HMAC key is
        // encrypted with the intermediate key + salt-derived IV; the HMAC
        // value over the encrypted package is similarly stored. We emit a
        // zero-valued placeholder for the HMAC value because its content
        // is not enforced by readers at open-time (only verified during
        // integrity check, which is optional for read-only access).
        byte[] hmacKeyEnc = EncryptHmacField(p, RandomBytes(HashBytes), BlockKeyHmacKey);
        byte[] hmacValueEnc = EncryptHmacField(p, new byte[HashBytes], BlockKeyHmacValue);

        string xml = BuildAgileXml(p, verifierEncInput, verifierEncValue, keyEncValue, hmacKeyEnc, hmacValueEnc);

        // 4-byte version (major=4, minor=4) + 4-byte flags (0x40 = AgileEncryption) + XML.
        int xmlByteCount = Encoding.UTF8.GetByteCount(xml);
        byte[] result = new byte[8 + xmlByteCount];
        BinaryPrimitives.WriteUInt64LittleEndian(result, 0x0000_0040_0004_0004UL);
        Encoding.UTF8.GetBytes(xml, result.AsSpan(8));
        return result;
    }

    private static string BuildAgileXml(
        Parameters p,
        byte[] verifierEncInput,
        byte[] verifierEncValue,
        byte[] keyEncValue,
        byte[] hmacKeyEnc,
        byte[] hmacValueEnc)
    {
        string keyDataSalt = Convert.ToBase64String(p.KeyDataSalt);
        string passwordSalt = Convert.ToBase64String(p.PasswordSalt);
        string verifierIn = Convert.ToBase64String(verifierEncInput);
        string verifierVal = Convert.ToBase64String(verifierEncValue);
        string keyVal = Convert.ToBase64String(keyEncValue);
        string hmacKey = Convert.ToBase64String(hmacKeyEnc);
        string hmacVal = Convert.ToBase64String(hmacValueEnc);

        return
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<encryption xmlns=\"http://schemas.microsoft.com/office/2006/encryption\" " +
            "xmlns:p=\"http://schemas.microsoft.com/office/2006/keyEncryptor/password\" " +
            "xmlns:c=\"http://schemas.microsoft.com/office/2006/keyEncryptor/certificate\">" +
            $"<keyData saltSize=\"{SaltSize}\" blockSize=\"{BlockSize}\" keyBits=\"{KeyBytes * 8}\" " +
            $"hashSize=\"{HashBytes}\" cipherAlgorithm=\"AES\" cipherChaining=\"ChainingModeCBC\" " +
            $"hashAlgorithm=\"SHA512\" saltValue=\"{keyDataSalt}\"/>" +
            $"<dataIntegrity encryptedHmacKey=\"{hmacKey}\" encryptedHmacValue=\"{hmacVal}\"/>" +
            "<keyEncryptors>" +
            "<keyEncryptor uri=\"http://schemas.microsoft.com/office/2006/keyEncryptor/password\">" +
            $"<p:encryptedKey spinCount=\"{SpinCount}\" saltSize=\"{SaltSize}\" blockSize=\"{BlockSize}\" " +
            $"keyBits=\"{KeyBytes * 8}\" hashSize=\"{HashBytes}\" cipherAlgorithm=\"AES\" " +
            "cipherChaining=\"ChainingModeCBC\" hashAlgorithm=\"SHA512\" " +
            $"saltValue=\"{passwordSalt}\" encryptedVerifierHashInput=\"{verifierIn}\" " +
            $"encryptedVerifierHashValue=\"{verifierVal}\" encryptedKeyValue=\"{keyVal}\"/>" +
            "</keyEncryptor>" +
            "</keyEncryptors>" +
            "</encryption>";
    }

    // ═══════════════════════════════════════════════════════════════════
    // EncryptedPackage = 8-byte LE size + segmented AES-CBC ciphertext
    // ═══════════════════════════════════════════════════════════════════

    private static byte[] BuildEncryptedPackage(Parameters p, byte[] innerAccdb)
    {
        // SegmentSize is a multiple of BlockSize, so only the trailing segment
        // can introduce padding. That lets us size the buffer in O(1).
        int totalSegments = (innerAccdb.Length + SegmentSize - 1) / SegmentSize;
        int paddedSegmentTotal = 0;
        if (totalSegments > 0)
        {
            int lastLen = innerAccdb.Length - ((totalSegments - 1) * SegmentSize);
            int paddedLastLen = ((lastLen + BlockSize - 1) / BlockSize) * BlockSize;
            paddedSegmentTotal = ((totalSegments - 1) * SegmentSize) + paddedLastLen;
        }

        byte[] result = new byte[8 + paddedSegmentTotal];
        BitConverter.TryWriteBytes(result.AsSpan(0, 8), (long)innerAccdb.Length);

        // Reuse a single Aes instance across all segments; only the IV changes.
        using var aes = Aes.Create();
        aes.Key = p.IntermediateKey;

        Span<byte> tail = stackalloc byte[BlockSize * 2]; // generous enough for one padded last block group
        int writeOffset = 8;
        for (int seg = 0; seg < totalSegments; seg++)
        {
            int offset = seg * SegmentSize;
            int length = Math.Min(SegmentSize, innerAccdb.Length - offset);
            int paddedLen = ((length + BlockSize - 1) / BlockSize) * BlockSize;

            byte[] iv = SegmentIv(p.KeyDataSalt, seg);

            scoped ReadOnlySpan<byte> plaintext;
            if (paddedLen == length)
            {
                // Block-aligned: encrypt directly from the source span.
                plaintext = innerAccdb.AsSpan(offset, length);
            }
            else if (paddedLen <= tail.Length)
            {
                // Final partial segment: zero-pad in the stack scratch buffer
                // and encrypt in-place to avoid escaping the stack span.
                Span<byte> scratch = tail[..paddedLen];
                innerAccdb.AsSpan(offset, length).CopyTo(scratch);
                scratch[length..].Clear();
                aes.EncryptCbc(scratch, iv, result.AsSpan(writeOffset, paddedLen), PaddingMode.None);
                writeOffset += paddedLen;
                continue;
            }
            else
            {
                // Oversized partial segment: fall back to a heap buffer.
                byte[] scratch = new byte[paddedLen];
                innerAccdb.AsSpan(offset, length).CopyTo(scratch);
                plaintext = scratch;
            }

            aes.EncryptCbc(plaintext, iv, result.AsSpan(writeOffset, paddedLen), PaddingMode.None);
            writeOffset += paddedLen;
        }

        return result;
    }

    private static byte[] SegmentIv(byte[] keyDataSalt, int segmentIndex)
    {
        Span<byte> data = stackalloc byte[keyDataSalt.Length + 4];
        keyDataSalt.CopyTo(data);
        BitConverter.TryWriteBytes(data[keyDataSalt.Length..], segmentIndex);

        Span<byte> hash = stackalloc byte[HashBytes];
        SHA512.HashData(data, hash);

        byte[] iv = new byte[BlockSize];
        hash[..BlockSize].CopyTo(iv);
        return iv;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Agile key derivation + verifier helpers
    // ═══════════════════════════════════════════════════════════════════

    private static byte[] EncryptVerifierHashInput(Parameters p, string password)
    {
        byte[] derived = DeriveKey(password, p.PasswordSalt, BlockKeyVerifierHashInput);
        byte[] padded = PadToBlock(p.VerifierHashInput);
        return AesCbc(padded, derived, p.PasswordSalt, encrypt: true);
    }

    private static byte[] EncryptVerifierHashValue(Parameters p, string password)
    {
        byte[] derived = DeriveKey(password, p.PasswordSalt, BlockKeyVerifierHashValue);
        byte[] hash = SHA512.HashData(p.VerifierHashInput);
        byte[] padded = PadToBlock(hash);
        return AesCbc(padded, derived, p.PasswordSalt, encrypt: true);
    }

    private static byte[] EncryptIntermediateKey(Parameters p, string password)
    {
        byte[] derived = DeriveKey(password, p.PasswordSalt, BlockKeyEncryptedKeyValue);
        byte[] padded = PadToBlock(p.IntermediateKey);
        return AesCbc(padded, derived, p.PasswordSalt, encrypt: true);
    }

    private static byte[] EncryptHmacField(Parameters p, byte[] value, byte[] blockKey)
    {
        byte[] iv = HmacIv(p.KeyDataSalt, blockKey);
        byte[] padded = PadToBlock(value);
        return AesCbc(padded, p.IntermediateKey, iv, encrypt: true);
    }

    private static byte[] HmacIv(byte[] keyDataSalt, byte[] blockKey)
    {
        // Salt + blockKey are tiny (≤24 bytes in practice); keep both the
        // input buffer and the SHA-512 output on the stack and copy only the
        // 16-byte IV prefix to the heap result.
        int inputLen = keyDataSalt.Length + blockKey.Length;
        Span<byte> data = stackalloc byte[inputLen];
        keyDataSalt.CopyTo(data);
        blockKey.CopyTo(data[keyDataSalt.Length..]);

        Span<byte> hash = stackalloc byte[HashBytes];
        SHA512.HashData(data, hash);

        byte[] iv = new byte[BlockSize];
        hash[..BlockSize].CopyTo(iv);
        return iv;
    }

    /// <summary>
    /// Agile PBKDF (ECMA-376 §2.3.4.11): H0 = H(salt || pwdUTF16LE);
    /// Hi+1 = H(uint32_le(i) || Hi); Hfinal = H(HspinCount || blockKey);
    /// truncate or pad-with-0x36 to keyBits/8.
    /// </summary>
    private static byte[] DeriveKey(string password, byte[] salt, byte[] blockKey)
    {
        // Initial hash: H(salt || pwdUTF16LE). Password is short in tests, so
        // a stack buffer (capped) is safe; fall back to heap for paranoia.
        int pwdByteCount = Encoding.Unicode.GetByteCount(password);
        int initialLen = salt.Length + pwdByteCount;
        Span<byte> initialBuf = initialLen <= 512 ? stackalloc byte[initialLen] : new byte[initialLen];
        salt.CopyTo(initialBuf);
        Encoding.Unicode.GetBytes(password, initialBuf[salt.Length..]);

        Span<byte> h = stackalloc byte[HashBytes];
        SHA512.HashData(initialBuf, h);

        // Iteration: H_{i+1} = H(uint32_le(i) || H_i). Fixed-size buffer, no allocations in loop.
        Span<byte> iter = stackalloc byte[4 + HashBytes];
        h.CopyTo(iter[4..]);
        for (int i = 0; i < SpinCount; i++)
        {
            BitConverter.TryWriteBytes(iter[..4], i);
            SHA512.HashData(iter, h);
            h.CopyTo(iter[4..]);
        }

        // Final: H(H_spin || blockKey).
        int finalLen = HashBytes + blockKey.Length;
        Span<byte> finalBuf = finalLen <= 256 ? stackalloc byte[finalLen] : new byte[finalLen];
        h.CopyTo(finalBuf);
        blockKey.CopyTo(finalBuf[HashBytes..]);

        Span<byte> hf = stackalloc byte[HashBytes];
        SHA512.HashData(finalBuf, hf);

        byte[] result = new byte[KeyBytes];
        if (hf.Length >= KeyBytes)
        {
            hf[..KeyBytes].CopyTo(result);
        }
        else
        {
            hf.CopyTo(result);
            result.AsSpan(hf.Length).Fill(0x36);
        }

        return result;
    }

    // ═══════════════════════════════════════════════════════════════════
    // CFB v4 (4096-byte sector) compound document writer
    // ═══════════════════════════════════════════════════════════════════

    private static byte[] BuildCompoundFile(byte[] encryptionInfo, byte[] encryptedPackage)
    {
        // Sector layout (sector index → file offset = 4096 + idx*4096):
        //   0           : FAT
        //   1           : Directory
        //   2..2+ei-1   : EncryptionInfo  (ei sectors)
        //   2+ei..end-1 : EncryptedPackage
        int eiSectors = SectorsFor(encryptionInfo.Length);
        int epSectors = SectorsFor(encryptedPackage.Length);

        const int fatSectorIndex = 0;
        const int dirSectorIndex = 1;
        const int eiStartSector = 2;
        int epStartSector = eiStartSector + eiSectors;
        int totalDataSectors = 2 + eiSectors + epSectors;

        // One FAT sector holds 1024 entries (4096 / 4); ensure we don't overflow.
        const int entriesPerFatSector = CfbSectorSize / 4;
        if (totalDataSectors > entriesPerFatSector)
        {
            throw new InvalidOperationException(
                $"Inner database too large for single-FAT-sector fixture (needs {totalDataSectors} sectors).");
        }

        // Compose file: Header (4096) | FAT | Dir | EI | EP. Allocated up
        // front so every section can be written in place — no intermediate
        // sector buffers, no final BlockCopy pass.
        int fileSize = CfbSectorSize + ((1 + 1 + eiSectors + epSectors) * CfbSectorSize);
        byte[] file = new byte[fileSize];

        WriteHeader(file, fatSectorIndex, dirSectorIndex, totalDirSectors: 1);

        // ── FAT sector ──────────────────────────────────────────────
        // FREESECT (0xFFFFFFFF) is all-ones, so a single byte-fill seeds
        // every entry; we then overwrite only the slots actually in use.
        Span<byte> fatSpan = file.AsSpan(SectorOffset(fatSectorIndex), CfbSectorSize);
        fatSpan.Fill(0xFF);
        BinaryPrimitives.WriteUInt32LittleEndian(fatSpan.Slice(fatSectorIndex * 4, 4), FatSect);
        BinaryPrimitives.WriteUInt32LittleEndian(fatSpan.Slice(dirSectorIndex * 4, 4), EndOfChain);

        for (int i = 0; i < eiSectors; i++)
        {
            int sec = eiStartSector + i;
            uint next = (i == eiSectors - 1) ? EndOfChain : (uint)(sec + 1);
            BinaryPrimitives.WriteUInt32LittleEndian(fatSpan.Slice(sec * 4, 4), next);
        }

        for (int i = 0; i < epSectors; i++)
        {
            int sec = epStartSector + i;
            uint next = (i == epSectors - 1) ? EndOfChain : (uint)(sec + 1);
            BinaryPrimitives.WriteUInt32LittleEndian(fatSpan.Slice(sec * 4, 4), next);
        }

        // ── Directory sector ────────────────────────────────────────
        // Written straight into the file buffer; the rest of the sector
        // is already zero-initialized, which is the correct CFB
        // representation for unallocated entries past index 3.
        int dirOffset = SectorOffset(dirSectorIndex);
        WriteRootEntry(file, dirOffset);
        WriteStreamEntry(
            file,
            dirOffset + DirEntrySize,
            "EncryptionInfo",
            eiStartSector,
            encryptionInfo.Length,
            leftSibling: FreeSect,
            rightSibling: 2);
        WriteStreamEntry(
            file,
            dirOffset + (DirEntrySize * 2),
            "EncryptedPackage",
            (uint)epStartSector,
            encryptedPackage.Length,
            leftSibling: FreeSect,
            rightSibling: FreeSect);
        WriteUnusedEntry(file, dirOffset + (DirEntrySize * 3));

        Buffer.BlockCopy(encryptionInfo, 0, file, SectorOffset(eiStartSector), encryptionInfo.Length);
        Buffer.BlockCopy(encryptedPackage, 0, file, SectorOffset(epStartSector), encryptedPackage.Length);

        return file;
    }

    private static int SectorOffset(int sectorIndex) => CfbSectorSize + (sectorIndex * CfbSectorSize);

    private static int SectorsFor(int byteLength) => (byteLength + CfbSectorSize - 1) / CfbSectorSize;

    private static void WriteHeader(byte[] file, int firstFatSector, int firstDirSector, int totalDirSectors)
    {
        // CFB header is 512 bytes; in v4 the rest of the 4096-byte sector is zero padding
        // (the backing array is already zero-initialized, so we only write the populated fields).
        Span<byte> h = file.AsSpan(0, 512);

        CfbMagic.CopyTo(h);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x18, 2), 0x003E); // minor version
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1A, 2), 0x0004); // major version (v4 = 4096-byte sectors)
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1C, 2), 0xFFFE); // little-endian byte order marker
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1E, 2), 0x000C); // sector shift = 12 → 4096
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x20, 2), 0x0006); // mini sector shift = 6 → 64
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x28, 4), totalDirSectors);
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x2C, 4), 1); // # FAT sectors
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x30, 4), firstDirSector);

        // 0x34 (transaction signature) and 0x38 (mini stream cutoff = 0 → all streams in regular FAT)
        // are left at their zero-init defaults.
        // 0x40 (# mini-FAT sectors) and 0x48 (# DIFAT sectors) stay at their zero-init defaults.
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x3C, 4), EndOfChain); // first mini-FAT sector
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x44, 4), EndOfChain); // first DIFAT sector

        // 0x4C..0xFF: DIFAT array (109 × 4 bytes). Slot 0 points at our single FAT sector;
        // the remaining 108 slots are FREESECT (0xFFFFFFFF), which we set with a single byte fill.
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x4C, 4), firstFatSector);
        h.Slice(0x4C + 4, 108 * 4).Fill(0xFF);
    }

    private static void WriteRootEntry(byte[] sector, int offset)
    {
        WriteDirEntry(
            sector,
            offset,
            "Root Entry",
            objectType: 5,
            startSector: EndOfChain,
            sizeLow: 0,
            sizeHigh: 0,
            leftSibling: FreeSect,
            rightSibling: FreeSect,
            child: 1);
    }

    private static void WriteStreamEntry(
        byte[] sector,
        int offset,
        string name,
        uint startSector,
        long size,
        uint leftSibling,
        uint rightSibling)
    {
        WriteDirEntry(
            sector,
            offset,
            name,
            objectType: 2,
            startSector: startSector,
            sizeLow: (uint)(size & 0xFFFFFFFFu),
            sizeHigh: (uint)((size >> 32) & 0xFFFFFFFFu),
            leftSibling: leftSibling,
            rightSibling: rightSibling,
            child: FreeSect);
    }

    private static void WriteUnusedEntry(byte[] sector, int offset)
    {
        Span<byte> e = sector.AsSpan(offset, DirEntrySize);
        e.Clear();
        e[0x42] = 0x00; // unallocated
        BitConverter.TryWriteBytes(e.Slice(0x44, 4), FreeSect);
        BitConverter.TryWriteBytes(e.Slice(0x48, 4), FreeSect);
        BitConverter.TryWriteBytes(e.Slice(0x4C, 4), FreeSect);
    }

    private static void WriteDirEntry(
        byte[] sector,
        int offset,
        string name,
        byte objectType,
        uint startSector,
        uint sizeLow,
        uint sizeHigh,
        uint leftSibling,
        uint rightSibling,
        uint child)
    {
        Span<byte> e = sector.AsSpan(offset, DirEntrySize);
        e.Clear();

        byte[] nameBytes = Encoding.Unicode.GetBytes(name);
        if (nameBytes.Length > 62)
        {
            throw new ArgumentException("Directory entry name too long.", nameof(name));
        }

        nameBytes.CopyTo(e);
        ushort nameLen = (ushort)(nameBytes.Length + 2); // include UTF-16 NUL terminator
        BitConverter.TryWriteBytes(e.Slice(0x40, 2), nameLen);

        e[0x42] = objectType;
        e[0x43] = 0x01; // black (red-black tree color)

        BitConverter.TryWriteBytes(e.Slice(0x44, 4), leftSibling);
        BitConverter.TryWriteBytes(e.Slice(0x48, 4), rightSibling);
        BitConverter.TryWriteBytes(e.Slice(0x4C, 4), child);

        BitConverter.TryWriteBytes(e.Slice(0x74, 4), startSector);
        BitConverter.TryWriteBytes(e.Slice(0x78, 4), sizeLow);
        BitConverter.TryWriteBytes(e.Slice(0x7C, 4), sizeHigh);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Misc helpers
    // ═══════════════════════════════════════════════════════════════════

    private static byte[] AesCbc(byte[] data, byte[] key, byte[] iv, bool encrypt)
    {
        using var aes = Aes.Create();
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.None;
        aes.Key = key;
        aes.IV = iv;

        using ICryptoTransform t = encrypt ? aes.CreateEncryptor() : aes.CreateDecryptor();
        return t.TransformFinalBlock(data, 0, data.Length);
    }

    private static byte[] PadToBlock(byte[] data)
    {
        int padded = ((data.Length + BlockSize - 1) / BlockSize) * BlockSize;
        if (padded == data.Length)
        {
            return data;
        }

        byte[] result = new byte[padded];
        Buffer.BlockCopy(data, 0, result, 0, data.Length);
        return result;
    }

    private static byte[] Truncate(byte[] source, int length)
    {
        if (source.Length == length)
        {
            return source;
        }

        byte[] r = new byte[length];
        Buffer.BlockCopy(source, 0, r, 0, length);
        return r;
    }

    private static byte[] RandomBytes(int length)
    {
        byte[] b = new byte[length];
        RandomNumberGenerator.Fill(b);
        return b;
    }

    private static byte[] Repeated(byte value, int length)
    {
        byte[] b = new byte[length];
        Array.Fill(b, value);
        return b;
    }

    /// <summary>
    /// Holds all per-fixture random material so the same parameters can be
    /// shared between the fixture builder and the verification self-test.
    /// </summary>
#pragma warning disable CA1515 // Nested type intentionally exposed via the surrounding internal static helper.
    public sealed class Parameters
#pragma warning restore CA1515
    {
        public byte[] KeyDataSalt { get; init; } = new byte[SaltSize];

        public byte[] PasswordSalt { get; init; } = new byte[SaltSize];

        public byte[] VerifierHashInput { get; init; } = new byte[SaltSize];

        public byte[] IntermediateKey { get; init; } = new byte[KeyBytes];
    }
}
