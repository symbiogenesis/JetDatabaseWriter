namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Office Standard cryptographic primitive and malformed descriptor tests; these are not database-container interoperability tests.</summary>
public sealed class StandardEncryptionTests
{
    private const string TestPassword = "secret";
    private const string WrongPassword = "wrong_password_123";

    [Fact]
    public void Standard_Decrypt_TruncatedEncryptionInfo_ThrowsInvalidDataException()
    {
        // EncryptionInfo too short for a header size field.
        byte[] truncatedInfo = new byte[10];
        BinaryPrimitives.WriteUInt16LittleEndian(truncatedInfo.AsSpan(0), 4); // major
        BinaryPrimitives.WriteUInt16LittleEndian(truncatedInfo.AsSpan(2), 2); // minor

        byte[] pkg = new byte[64];

        Assert.Throws<InvalidDataException>(
            () => OfficeCryptoStandard.Decrypt(truncatedInfo, pkg, TestPassword));
    }

    [Fact]
    public void Standard_Decrypt_InvalidHeaderSize_ThrowsInvalidDataException()
    {
        // HeaderSize exceeds the stream length.
        byte[] info = new byte[32];
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(0), 4); // major
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(2), 2); // minor
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(4), 0x24); // flags
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(8), 9999); // bogus HeaderSize

        byte[] pkg = new byte[64];

        Assert.Throws<InvalidDataException>(
            () => OfficeCryptoStandard.Decrypt(info, pkg, TestPassword));
    }

    [Fact]
    public void Standard_Decrypt_UnsupportedAlgId_ThrowsNotSupportedException()
    {
        // Build a valid-looking header with an unsupported AlgID (RC4 = 0x6801).
        byte[] info = BuildFakeStandardEncryptionInfo(algId: 0x6801);
        byte[] pkg = new byte[64];

        Assert.Throws<NotSupportedException>(
            () => OfficeCryptoStandard.Decrypt(info, pkg, TestPassword));
    }

    [Fact]
    public void Standard_Decrypt_UnsupportedHashAlg_ThrowsNotSupportedException()
    {
        // Build a valid-looking header with an unsupported hash algorithm.
        byte[] info = BuildFakeStandardEncryptionInfo(algIdHash: 0x0000);
        byte[] pkg = new byte[64];

        Assert.Throws<NotSupportedException>(
            () => OfficeCryptoStandard.Decrypt(info, pkg, TestPassword));
    }

    [Fact]
    public void Standard_Decrypt_TruncatedVerifier_ThrowsInvalidDataException()
    {
        // Build an EncryptionInfo where the verifier section is truncated.
        byte[] info = BuildFakeStandardEncryptionInfo(truncateVerifier: true);
        byte[] pkg = new byte[64];

        Assert.Throws<InvalidDataException>(
            () => OfficeCryptoStandard.Decrypt(info, pkg, TestPassword));
    }

    [Fact]
    public void Standard_Decrypt_InvalidSaltSize_ThrowsInvalidDataException()
    {
        // Standard encryption requires salt size = 16.
        byte[] info = BuildFakeStandardEncryptionInfo(saltSize: 32);
        byte[] pkg = new byte[64];

        Assert.Throws<InvalidDataException>(
            () => OfficeCryptoStandard.Decrypt(info, pkg, TestPassword));
    }

    [Fact]
    public void Standard_Decrypt_TruncatedEncryptedPackage_ThrowsInvalidDataException()
    {
        // EncryptedPackage shorter than the 8-byte size prefix.
        byte[] info = BuildValidStandardEncryptionInfo();
        byte[] pkg = new byte[4]; // Too short.

        Assert.Throws<InvalidDataException>(
            () => OfficeCryptoStandard.Decrypt(info, pkg, TestPassword));
    }

    [Fact]
    public void Standard_Decrypt_NonStandardVersionHeader_ThrowsInvalidDataException()
    {
        // Pass an Agile version header (4,4) — should be rejected.
        byte[] agileInfo = new byte[16];
        BinaryPrimitives.WriteUInt16LittleEndian(agileInfo.AsSpan(0), 4); // major
        BinaryPrimitives.WriteUInt16LittleEndian(agileInfo.AsSpan(2), 4); // minor (Agile)
        agileInfo[4] = 0x40; // AgileEncryption flag

        byte[] pkg = new byte[64];

        Assert.Throws<InvalidDataException>(
            () => OfficeCryptoStandard.Decrypt(agileInfo, pkg, TestPassword));
    }

    [Fact]
    public void Standard_TamperedCiphertext_ProducesCorruptedData()
    {
        // Unlike Agile, Standard encryption has no HMAC integrity check.
        // Tampering with the ciphertext changes decrypted plaintext and
        // the password verifier still passes (it's stored separately from
        // the package data). This verifies that corruption in the package
        // at least produces different plaintext output.
        byte[] plaintext = new byte[256];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        // Tamper a byte in the ciphertext (past the 8-byte size header).
        package.EncryptedPackage[16] ^= 0xFF;

        // Decryption still succeeds (no integrity check) but produces
        // different output due to corrupted AES-CBC ciphertext.
        byte[] decrypted = OfficeCryptoStandard.Decrypt(package.EncryptionInfo, package.EncryptedPackage, TestPassword);
        Assert.NotEqual(plaintext, decrypted);
    }

    /// <summary>A pinned independent Standard Office package verifies the required AES identifiers, derivation and ECB transform.</summary>
    [Fact]
    public async Task Standard_DecryptsIndependentOfficePackage()
    {
        await using FileStream stream = File.OpenRead(Path.Combine(TestDatabases.EncryptedRoot, "Upstream-OfficeStandard.docx"));
        Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken);
        byte[] expected = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "Upstream-OfficeStandard-plain.docx"), TestContext.Current.CancellationToken);
        byte[] actual = OfficeCryptoStandard.Decrypt(streams["EncryptionInfo"], streams["EncryptedPackage"], "Password1234_");
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(19)]
    [InlineData(21)]
    public void Standard_RejectsInvalidVerifierHashLength(int hashBytes)
    {
        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(new byte[16], TestPassword);
        int headerBytes = BinaryPrimitives.ReadInt32LittleEndian(package.EncryptionInfo.AsSpan(8));
        BinaryPrimitives.WriteInt32LittleEndian(package.EncryptionInfo.AsSpan(12 + headerBytes + 36), hashBytes);
        Assert.Throws<InvalidDataException>(() => OfficeCryptoStandard.Decrypt(package.EncryptionInfo, package.EncryptedPackage, TestPassword));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 5. NULL GUARDS
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public void Standard_Decrypt_NullEncryptionInfo_ThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoStandard.Decrypt(null!, new byte[64], TestPassword));

    [Fact]
    public void Standard_Decrypt_NullEncryptedPackage_ThrowsArgumentNullException()
    {
        byte[] fakeInfo = new byte[16];
        BinaryPrimitives.WriteUInt16LittleEndian(fakeInfo.AsSpan(0), 4); // major
        BinaryPrimitives.WriteUInt16LittleEndian(fakeInfo.AsSpan(2), 2); // minor

        Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoStandard.Decrypt(fakeInfo, null!, TestPassword));
    }

    [Fact]
    public void Standard_Encrypt_NullInnerPackage_ThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoStandard.Encrypt(null!, TestPassword));

    // ═══════════════════════════════════════════════════════════════════
    // 6. FORMAT DETECTION
    // ═══════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData(3, 2)]
    [InlineData(4, 2)]
    public void IsStandardEncryptionInfo_ValidVersions_ReturnsTrue(ushort major, ushort minor)
    {
        byte[] info = new byte[8];
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(0), major);
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(2), minor);

        Assert.True(OfficeCryptoAgile.IsStandardEncryptionInfo(info));
    }

    [Theory]
    [InlineData(4, 4)] // Agile
    [InlineData(2, 2)] // Unknown
    [InlineData(1, 1)] // Unknown
    [InlineData(5, 0)] // Unknown
    public void IsStandardEncryptionInfo_InvalidVersions_ReturnsFalse(ushort major, ushort minor)
    {
        byte[] info = new byte[8];
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(0), major);
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(2), minor);

        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo(info));
    }

    [Fact]
    public void IsStandardEncryptionInfo_NullOrTooShort_ReturnsFalse()
    {
        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo(null!));
        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo([]));
        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo(new byte[7]));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 7. ENCRYPT + DECRYPT ROUND-TRIP (unit-level, no CFB)
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public void Standard_EncryptThenDecrypt_RoundTripsPlaintext()
    {
        byte[] plaintext = new byte[8192];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        byte[] decrypted = OfficeCryptoStandard.Decrypt(package.EncryptionInfo, package.EncryptedPackage, TestPassword);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void Standard_EncryptThenDecrypt_WithDifferentPassword_Throws()
    {
        byte[] plaintext = new byte[4096];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        Assert.Throws<UnauthorizedAccessException>(
            () => OfficeCryptoStandard.Decrypt(package.EncryptionInfo, package.EncryptedPackage, WrongPassword));
    }

    [Fact]
    public void Standard_EncryptThenDecrypt_SmallPlaintext_RoundTrips()
    {
        // Test with data smaller than one AES block (16 bytes).
        byte[] plaintext = [0x01, 0x02, 0x03, 0x04, 0x05];

        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        byte[] decrypted = OfficeCryptoStandard.Decrypt(package.EncryptionInfo, package.EncryptedPackage, TestPassword);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void Standard_EncryptThenDecrypt_ExactBlockSizePlaintext_RoundTrips()
    {
        // Test with data that is exactly a multiple of 16 bytes.
        byte[] plaintext = new byte[256];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        byte[] decrypted = OfficeCryptoStandard.Decrypt(package.EncryptionInfo, package.EncryptedPackage, TestPassword);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void Standard_Encrypt_ProducesStandardVersionHeader()
    {
        byte[] plaintext = new byte[128];

        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        // Verify the version header identifies this as Standard encryption.
        Assert.True(OfficeCryptoAgile.IsStandardEncryptionInfo(package.EncryptionInfo));

        ushort major = BinaryPrimitives.ReadUInt16LittleEndian(package.EncryptionInfo.AsSpan(0, 2));
        ushort minor = BinaryPrimitives.ReadUInt16LittleEndian(package.EncryptionInfo.AsSpan(2, 2));
        Assert.Equal(4, major);
        Assert.Equal(2, minor);
    }

    [Fact]
    public void Standard_Encrypt_DifferentInvocations_ProduceDifferentCiphertext()
    {
        // Verify salt randomization: two encryptions of the same data should differ.
        byte[] plaintext = new byte[128];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        OfficeEncryptedPackage package1 = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);
        OfficeEncryptedPackage package2 = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        // The encrypted packages must differ because the salt is random.
        Assert.False(
            package1.EncryptedPackage.AsSpan().SequenceEqual(package2.EncryptedPackage.AsSpan()),
            "Two encryptions of the same data should produce different ciphertext (different salts).");

        // But both must decrypt back to the same plaintext.
        Assert.Equal(plaintext, OfficeCryptoStandard.Decrypt(package1.EncryptionInfo, package1.EncryptedPackage, TestPassword));
        Assert.Equal(plaintext, OfficeCryptoStandard.Decrypt(package2.EncryptionInfo, package2.EncryptedPackage, TestPassword));
    }

    // ═══════════════════════════════════════════════════════════════════
    // Descriptor construction helpers
    // ═══════════════════════════════════════════════════════════════════

    private static byte[] BuildFakeStandardEncryptionInfo(
        int algId = 0x660E,
        int algIdHash = 0x8004,
        int saltSize = 16,
        bool truncateVerifier = false)
    {
        const string cspName = "Microsoft Enhanced RSA and AES Cryptographic Provider";
        byte[] cspNameBytes = Encoding.Unicode.GetBytes(cspName + '\0');
        int headerSize = 32 + cspNameBytes.Length;

        int verifierSize = truncateVerifier ? 10 : (4 + 16 + 16 + 4 + 32);

        int totalSize = 4 + 4 + 4 + headerSize + verifierSize;
        byte[] info = new byte[totalSize];
        int pos = 0;

        // Version (4, 2).
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(pos), 4);
        pos += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(pos), 2);
        pos += 2;

        // Flags.
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos), 0x24);
        pos += 4;

        // HeaderSize.
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos), headerSize);
        pos += 4;

        // EncryptionHeader.
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos), 0x24); // Flags
        pos += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos), 0); // SizeExtra
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos), algId); // AlgID
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos), algIdHash); // AlgIDHash
        pos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos), 128); // KeySize
        pos += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos), 0x18); // ProviderType
        pos += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos), 0); // Reserved1
        pos += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(pos), 0); // Reserved2
        pos += 4;
        Buffer.BlockCopy(cspNameBytes, 0, info, pos, cspNameBytes.Length);
        pos += cspNameBytes.Length;

        // EncryptionVerifier (potentially truncated).
        if (!truncateVerifier)
        {
            BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos), saltSize); // SaltSize
            pos += 4;

            // Salt (16 bytes of zeros).
            pos += 16;

            // EncryptedVerifier (16 bytes of zeros).
            pos += 16;

            // VerifierHashSize.
            BinaryPrimitives.WriteInt32LittleEndian(info.AsSpan(pos), 20);
            pos += 4;

            // EncryptedVerifierHash (32 bytes of zeros — will fail password check).
        }

        return info;
    }

    /// <summary>
    /// Builds a valid EncryptionInfo that will pass parsing but fail password
    /// verification (used for testing truncated EncryptedPackage).
    /// </summary>
    private static byte[] BuildValidStandardEncryptionInfo()
    {
        // Encrypt a dummy plaintext to get a real, valid EncryptionInfo.
        byte[] plaintext = new byte[128];
        OfficeEncryptedPackage package = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);
        return package.EncryptionInfo;
    }
}
