namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Text;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Agile cryptographic primitive regressions. Office package results are not treated as Microsoft Access database containers.</summary>
public sealed class AgileEncryptionTests
{
    /// <summary>
    /// Round-trip self-check: deriving the intermediate key with the same
    /// inputs the fixture builder used must reproduce the verifierHashInput
    /// → verifierHashValue chain, proving the spec primitives are wired
    /// correctly. When the reader implementation lands and exposes the same
    /// primitive (or this helper is migrated next to it), this acts as a
    /// regression guard.
    /// </summary>
    [Fact]
    public void Agile_KeyDerivation_VerifierRoundTripsAgainstStoredHash()
    {
        // Use a fixed, known set of Agile parameters — any change to the
        // PBKDF, block-key constants, or hashing chain breaks this test.
        AgileEncryptionFixtureBuilder.Parameters p = AgileEncryptionFixtureBuilder.DeterministicParameters();

        byte[] verifierInput = AgileEncryptionFixtureBuilder.DecryptVerifierHashInput(p, TestDatabases.AesEncryptedPassword);
        byte[] expectedHash = System.Security.Cryptography.SHA512.HashData(verifierInput);
        byte[] storedHash = AgileEncryptionFixtureBuilder.DecryptVerifierHashValue(p, TestDatabases.AesEncryptedPassword);

        Assert.Equal(expectedHash, storedHash);
    }

    /// <summary>
    /// The reader derives the three password keys (verifier hash input,
    /// verifier hash value, encrypted key value) from one PBKDF chain, where
    /// ECMA-376 §2.3.4.11 states one chain per key, as the fixture builder
    /// runs it. Decrypting the builder's package with the right password
    /// proves the shared chain yields the same three keys. The iteration
    /// count and key size are read from the descriptor, so a count and key
    /// size other than the writer's 100,000 and 256 bits must work too.
    /// </summary>
    /// <param name="spinCount">The descriptor's PBKDF iteration count.</param>
    /// <param name="keyBits">The descriptor's key size, for the key data and the password key encryptor.</param>
    [Theory]
    [InlineData(1_000, 128)]
    [InlineData(0, 192)]
    [InlineData(100_000, 256)]
    public void Agile_ResolvePassword_SharedChainMatchesPerBlockKeyDerivation(int spinCount, int keyBits)
    {
        AgileEncryptionFixtureBuilder.Parameters p = AgileEncryptionFixtureBuilder.DeterministicParameters(spinCount, keyBits);
        byte[] inner = new byte[10_000];
        for (int i = 0; i < inner.Length; i++)
        {
            inner[i] = (byte)(i % 251);
        }

        (byte[] encryptionInfo, byte[] encryptedPackage) = AgileEncryptionFixtureBuilder.BuildStreams(p, inner, TestDatabases.AesEncryptedPassword);
        string descriptor = Encoding.UTF8.GetString(encryptionInfo, 8, encryptionInfo.Length - 8);
        Assert.Contains($"spinCount=\"{spinCount}\"", descriptor, StringComparison.Ordinal);
        Assert.Contains($"keyBits=\"{keyBits}\"", descriptor, StringComparison.Ordinal);

        Assert.Equal(inner, OfficeCryptoAgile.Decrypt(encryptionInfo, encryptedPackage, TestDatabases.AesEncryptedPassword));
        Assert.Throws<UnauthorizedAccessException>(() => OfficeCryptoAgile.Decrypt(encryptionInfo, encryptedPackage, "not_the_password"));
    }

    /// <summary>Package encryption preserves zero padding and segment boundaries.</summary>
    /// <param name="length">The plaintext length around AES and Agile segment boundaries.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(4096)]
    [InlineData(4097)]
    [InlineData(8193)]
    public void Agile_EncryptPrimitive_RoundTripsSegmentBoundaries(int length)
    {
        byte[] plaintext = new byte[length];
        for (int i = 0; i < plaintext.Length; i++)
        {
            plaintext[i] = (byte)(i % 251);
        }

        OfficeEncryptedPackage package = OfficeCryptoAgile.Encrypt(plaintext, "segment password");
        Assert.Equal(8 + ((length + 15) / 16 * 16), package.EncryptedPackage.Length);
        Assert.Equal(plaintext, OfficeCryptoAgile.Decrypt(package.EncryptionInfo, package.EncryptedPackage, "segment password"));
    }

    /// <summary>The Office package primitive authenticates its ciphertext before decryption.</summary>
    [Fact]
    public void Agile_TamperedPackagePrimitive_RejectsIntegrityFailure()
    {
        OfficeEncryptedPackage package = OfficeCryptoAgile.Encrypt(new byte[256], "primitive password");
        package.EncryptedPackage[24] ^= 0xFF;
        InvalidDataException error = Assert.Throws<InvalidDataException>(() => OfficeCryptoAgile.Decrypt(package.EncryptionInfo, package.EncryptedPackage, "primitive password"));
        Assert.Contains("integrity", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Password verification remains effective when the descriptor omits optional package integrity fields.</summary>
    [Fact]
    public void Agile_WrongPasswordPrimitive_RejectsWithoutHmac()
    {
        AgileEncryptionFixtureBuilder.Parameters parameters = AgileEncryptionFixtureBuilder.DeterministicParameters();
        (byte[] info, byte[] package) = AgileEncryptionFixtureBuilder.BuildStreams(parameters, new byte[256], "primitive password");
        Assert.Throws<UnauthorizedAccessException>(() => OfficeCryptoAgile.Decrypt(info, package, "wrong"));
    }

    [Fact]
    public void Agile_Decrypt_NullEncryptionInfo_ThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoAgile.Decrypt(null!, new byte[64], "pw"));

    [Fact]
    public void Agile_Decrypt_NullEncryptedPackage_ThrowsArgumentNullException()
    {
        // Build a minimal valid-looking EncryptionInfo header (version 4.4, flag 0x40).
        byte[] fakeInfo = new byte[16];
        fakeInfo[0] = 0x04; // major
        fakeInfo[2] = 0x04; // minor
        fakeInfo[4] = 0x40; // AgileEncryption flag

        Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoAgile.Decrypt(fakeInfo, null!, "pw"));
    }

    [Fact]
    public void Agile_Encrypt_NullInnerPackage_ThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoAgile.Encrypt(null!, "pw"));
}
