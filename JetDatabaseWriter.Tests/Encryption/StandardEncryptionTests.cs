namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Regression tests for MS-OFFCRYPTO §2.3.6 "Standard" encryption support
/// in <see cref="AccessReader"/> and <see cref="AccessWriter"/>.
///
/// Standard encryption is the scheme used by password-encrypted .accdb files
/// produced by Access 2007. The file is a CFB compound document with:
///   • <c>EncryptionInfo</c>  — version (3,2) or (4,2) binary header with
///                              AES-128 AlgID, SHA-1 AlgIDHash, salt, and
///                              encrypted verifier/hash fields.
///   • <c>EncryptedPackage</c> — 8-byte LE decrypted size + AES-128-CBC
///                              encrypted data (IV = all zeros).
///
/// Tests are structured as:
///   1. Password enforcement (no password, wrong password)
///   2. Happy path (successful open, catalog, data reading)
///   3. Write support (decrypt-on-open, re-encrypt-on-dispose)
///   4. Malformed encryption data (truncated, corrupt headers)
///   5. Null guards
///   6. Format detection.
/// </summary>
public sealed class StandardEncryptionTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string TestPassword = "secret";
    private const string WrongPassword = "wrong_password_123";

    // ═══════════════════════════════════════════════════════════════════
    // 1. PASSWORD ENFORCEMENT
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Standard_OpenWithoutPassword_ThrowsUnauthorizedAccessException()
    {
        byte[] data = await BuildStandardEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(
                ms,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Standard_OpenWithWrongPassword_ThrowsUnauthorizedAccessException()
    {
        byte[] data = await BuildStandardEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        var options = new AccessReaderOptions
        {
            Password = WrongPassword.AsMemory(),
            UseLockFile = false,
        };

        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Standard_OpenWithEmptyPassword_ThrowsUnauthorizedAccessException()
    {
        byte[] data = await BuildStandardEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        var options = new AccessReaderOptions
        {
            Password = string.Empty.AsMemory(),
            UseLockFile = false,
        };

        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. HAPPY PATH — successful decryption and data access
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Standard_OpenWithCorrectPassword_Succeeds()
    {
        byte[] data = await BuildStandardEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);
    }

    [Fact]
    public async Task Standard_ListTables_MatchesUnencryptedSource()
    {
        var sourceReader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        List<string> expected = (await sourceReader.ListTablesAsync(TestContext.Current.CancellationToken))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        byte[] data = await BuildStandardEncryptedFixtureAsync();
        await using var ms = new MemoryStream(data, writable: false);
        await using var encReader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> actual = (await encReader.ListTablesAsync(TestContext.Current.CancellationToken))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Standard_ReadDataTable_ReturnsRows()
    {
        byte[] data = await BuildStandardEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        DataTable? dt = await reader.ReadDataTableAsync(
            tables[0],
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.True(dt!.Rows.Count > 0, "Standard-decrypted table should contain rows.");
    }

    [Fact]
    public async Task Standard_StreamRows_YieldsRows()
    {
        byte[] data = await BuildStandardEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int count = await reader.Rows(
            tables[0],
            cancellationToken: TestContext.Current.CancellationToken)
            .CountAsync(TestContext.Current.CancellationToken);

        Assert.True(count > 0, "Standard-decrypted stream should yield rows.");
    }

    [Fact]
    public async Task Standard_RowCounts_MatchUnencryptedSource()
    {
        var sourceReader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        List<TableStat> expected = await sourceReader.GetTableStatsAsync(TestContext.Current.CancellationToken);

        byte[] data = await BuildStandardEncryptedFixtureAsync();
        await using var ms = new MemoryStream(data, writable: false);
        await using var encReader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<TableStat> actual = await encReader.GetTableStatsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected.Count, actual.Count);
        foreach (TableStat exp in expected)
        {
            TableStat? act = actual.FirstOrDefault(s => s.Name == exp.Name);
            Assert.NotNull(act);
            Assert.Equal(exp.RowCount, act!.RowCount);
        }
    }

    [Fact]
    public async Task Standard_ColumnMetadata_MatchesUnencryptedSource()
    {
        var sourceReader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        List<string> tables = await sourceReader.ListTablesAsync(TestContext.Current.CancellationToken);
        var expectedCols = new Dictionary<string, List<ColumnMetadata>>(StringComparer.Ordinal);
        foreach (string table in tables)
        {
            expectedCols[table] = await sourceReader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
        }

        byte[] data = await BuildStandardEncryptedFixtureAsync();
        await using var ms = new MemoryStream(data, writable: false);
        await using var encReader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> actual = await encReader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.Equal(expectedCols[table].Count, actual.Count);

            for (int i = 0; i < actual.Count; i++)
            {
                Assert.Equal(expectedCols[table][i].Name, actual[i].Name);
                Assert.Equal(expectedCols[table][i].TypeName, actual[i].TypeName);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. WRITE SUPPORT — decrypt-on-open + re-encrypt-on-dispose
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Standard_WriterRoundTrip_InsertedRow_VisibleAfterReopen()
    {
        const string TableName = "StandardWriteRoundTrip";

        byte[] data = await BuildStandardEncryptedFixtureAsync();
        string temp = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.accdb");
        await File.WriteAllBytesAsync(temp, data, TestContext.Current.CancellationToken);

        try
        {
            var writerOptions = new AccessWriterOptions
            {
                UseLockFile = false,
                Password = TestPassword.AsMemory(),
            };

            await using (var writer = await AccessWriter.OpenAsync(temp, writerOptions, TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync(
                    TableName,
                    [
                        new ColumnDefinition("Id", typeof(int)),
                        new ColumnDefinition("Label", typeof(string), maxLength: 64),
                    ],
                    TestContext.Current.CancellationToken);

                await writer.InsertRowAsync(
                    TableName,
                    [99, "standard-write-roundtrip"],
                    TestContext.Current.CancellationToken);
            }

            await AssertStandardCfbV4FileAsync(temp, TestContext.Current.CancellationToken);

            // Reopen via AccessReader: must detect Standard encryption,
            // decrypt, and surface the freshly-inserted row.
            await using var reader = await AccessReader.OpenAsync(temp, CorrectPasswordOptions(), TestContext.Current.CancellationToken);
            DataTable dt = (await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken))!;

            Assert.NotNull(dt);
            Assert.Single(dt.Rows);
            Assert.Equal(99, dt.Rows[0]["Id"]);
            Assert.Equal("standard-write-roundtrip", dt.Rows[0]["Label"]);
        }
        finally
        {
            if (File.Exists(temp))
            {
                File.Delete(temp);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 4. MALFORMED ENCRYPTION DATA
    // ═══════════════════════════════════════════════════════════════════

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

        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        // Tamper a byte in the ciphertext (past the 8-byte size header).
        encryptedPackage[16] ^= 0xFF;

        // Decryption still succeeds (no integrity check) but produces
        // different output due to corrupted AES-CBC ciphertext.
        byte[] decrypted = OfficeCryptoStandard.Decrypt(encryptionInfo, encryptedPackage, TestPassword);
        Assert.NotEqual(plaintext, decrypted);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 5. NULL GUARDS
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public void Standard_Decrypt_NullEncryptionInfo_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoStandard.Decrypt(null!, new byte[64], TestPassword));
    }

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
    public void Standard_Encrypt_NullInnerPackage_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(
            () => OfficeCryptoStandard.Encrypt(null!, TestPassword));
    }

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

        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        byte[] decrypted = OfficeCryptoStandard.Decrypt(encryptionInfo, encryptedPackage, TestPassword);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void Standard_EncryptThenDecrypt_WithDifferentPassword_Throws()
    {
        byte[] plaintext = new byte[4096];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        Assert.Throws<UnauthorizedAccessException>(
            () => OfficeCryptoStandard.Decrypt(encryptionInfo, encryptedPackage, WrongPassword));
    }

    [Fact]
    public void Standard_EncryptThenDecrypt_SmallPlaintext_RoundTrips()
    {
        // Test with data smaller than one AES block (16 bytes).
        byte[] plaintext = [0x01, 0x02, 0x03, 0x04, 0x05];

        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        byte[] decrypted = OfficeCryptoStandard.Decrypt(encryptionInfo, encryptedPackage, TestPassword);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void Standard_EncryptThenDecrypt_ExactBlockSizePlaintext_RoundTrips()
    {
        // Test with data that is exactly a multiple of 16 bytes.
        byte[] plaintext = new byte[256];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        byte[] decrypted = OfficeCryptoStandard.Decrypt(encryptionInfo, encryptedPackage, TestPassword);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void Standard_Encrypt_ProducesStandardVersionHeader()
    {
        byte[] plaintext = new byte[128];

        (byte[] encryptionInfo, _) = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        // Verify the version header identifies this as Standard encryption.
        Assert.True(OfficeCryptoAgile.IsStandardEncryptionInfo(encryptionInfo));

        ushort major = BinaryPrimitives.ReadUInt16LittleEndian(encryptionInfo.AsSpan(0, 2));
        ushort minor = BinaryPrimitives.ReadUInt16LittleEndian(encryptionInfo.AsSpan(2, 2));
        Assert.Equal(4, major);
        Assert.Equal(2, minor);
    }

    [Fact]
    public void Standard_Encrypt_DifferentInvocations_ProduceDifferentCiphertext()
    {
        // Verify salt randomization: two encryptions of the same data should differ.
        byte[] plaintext = new byte[128];
        System.Security.Cryptography.RandomNumberGenerator.Fill(plaintext);

        (byte[] info1, byte[] pkg1) = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);
        (byte[] info2, byte[] pkg2) = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);

        // The encrypted packages must differ because the salt is random.
        Assert.False(
            pkg1.AsSpan().SequenceEqual(pkg2.AsSpan()),
            "Two encryptions of the same data should produce different ciphertext (different salts).");

        // But both must decrypt back to the same plaintext.
        Assert.Equal(plaintext, OfficeCryptoStandard.Decrypt(info1, pkg1, TestPassword));
        Assert.Equal(plaintext, OfficeCryptoStandard.Decrypt(info2, pkg2, TestPassword));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 8. ENCRYPTION MUTATION API — encrypt / change-password / decrypt
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Standard_EncryptAsync_ProducesEncryptedFile()
    {
        string path = await CloneTempFileAsync(".accdb");
        try
        {
            await AccessWriter.EncryptAsync(
                path,
                TestPassword,
                AccessEncryptionFormat.AccdbStandard,
                NoLockOptions(),
                TestContext.Current.CancellationToken);

            await AssertStandardCfbV4FileAsync(path, TestContext.Current.CancellationToken);

            // Should still be openable with the password.
            await using var reader = await AccessReader.OpenAsync(
                path, CorrectPasswordOptions(), TestContext.Current.CancellationToken);
            List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.NotEmpty(tables);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Standard_ChangePassword_OnlyNewPasswordWorks()
    {
        const string NewPassword = "rotated!Pa$$";
        string path = await CloneTempFileAsync(".accdb");
        try
        {
            await AccessWriter.EncryptAsync(
                path,
                TestPassword,
                AccessEncryptionFormat.AccdbStandard,
                NoLockOptions(),
                TestContext.Current.CancellationToken);

            await AccessWriter.ChangePasswordAsync(
                path,
                TestPassword,
                NewPassword,
                NoLockOptions(),
                TestContext.Current.CancellationToken);

            await AssertStandardCfbV4FileAsync(path, TestContext.Current.CancellationToken);

            // Old password should fail.
            var oldOpts = new AccessReaderOptions { Password = TestPassword.AsMemory(), UseLockFile = false };
            await Assert.ThrowsAsync<UnauthorizedAccessException>(
                async () => await AccessReader.OpenAsync(path, oldOpts, TestContext.Current.CancellationToken));

            // New password should work.
            var newOpts = new AccessReaderOptions { Password = NewPassword.AsMemory(), UseLockFile = false };
            await using var reader = await AccessReader.OpenAsync(path, newOpts, TestContext.Current.CancellationToken);
            List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.NotEmpty(tables);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Standard_Decrypt_RemovesEncryption()
    {
        string path = await CloneTempFileAsync(".accdb");
        try
        {
            await AccessWriter.EncryptAsync(
                path,
                TestPassword,
                AccessEncryptionFormat.AccdbStandard,
                NoLockOptions(),
                TestContext.Current.CancellationToken);

            await AccessWriter.DecryptAsync(
                path,
                TestPassword,
                NoLockOptions(),
                TestContext.Current.CancellationToken);

            // Should now be openable without any password.
            await using var reader = await AccessReader.OpenAsync(
                path,
                new AccessReaderOptions { UseLockFile = false },
                TestContext.Current.CancellationToken);
            List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.NotEmpty(tables);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    private static AccessReaderOptions CorrectPasswordOptions() => new()
    {
        Password = TestPassword.AsMemory(),
        UseLockFile = false,
    };

    private static AccessWriterOptions NoLockOptions() => new() { UseLockFile = false };

    private static async Task AssertStandardCfbV4FileAsync(string path, CancellationToken cancellationToken)
    {
        byte[] bytes = await File.ReadAllBytesAsync(path, cancellationToken);

        Assert.True(CompoundFileReader.HasCompoundFileMagic(bytes));

        ushort majorVersion = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(0x1A, 2));
        ushort sectorShift = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(0x1E, 2));

        Assert.Equal(Constants.CompoundFile.V4.MajorVersion, majorVersion);
        Assert.Equal(Constants.CompoundFile.V4.SectorShift, sectorShift);

        await using var stream = new MemoryStream(bytes, writable: false);
        var streams = await CompoundFileReader.ReadStreamsAsync(stream, cancellationToken);

        Assert.True(streams.ContainsKey("EncryptionInfo"));
        Assert.True(streams.ContainsKey("EncryptedPackage"));
        Assert.True(OfficeCryptoAgile.IsStandardEncryptionInfo(streams["EncryptionInfo"]));
    }

    /// <summary>
    /// Builds a fake EncryptionInfo with customizable fields for negative tests.
    /// The resulting EncryptionInfo has the Standard version header but the
    /// verifier data is garbage (so password verification will fail or parsing
    /// will error out depending on which field is wrong).
    /// </summary>
    private static byte[] BuildFakeStandardEncryptionInfo(
        int algId = 0x6601,
        int algIdHash = 0x8004,
        int saltSize = 16,
        bool truncateVerifier = false)
    {
        string cspName = "Microsoft Enhanced RSA and AES Cryptographic Provider";
        byte[] cspNameBytes = Encoding.Unicode.GetBytes(cspName + '\0');
        int headerSize = 28 + cspNameBytes.Length;

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
        (byte[] encryptionInfo, _) = OfficeCryptoStandard.Encrypt(plaintext, TestPassword);
        return encryptionInfo;
    }

    private async Task<byte[]> BuildStandardEncryptedFixtureAsync()
    {
        byte[] inner = await db.GetFileAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);

        // Use the production OfficeCryptoStandard.Encrypt to build the fixture,
        // then wrap it in a CFB compound document — matching the format the
        // reader expects for Standard-encrypted .accdb files.
        (byte[] encryptionInfo, byte[] encryptedPackage) =
            OfficeCryptoStandard.Encrypt(inner, TestPassword);

        return CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("EncryptionInfo", encryptionInfo),
            new KeyValuePair<string, byte[]>("EncryptedPackage", encryptedPackage),
        ]);
    }

    private async Task<string> CloneTempFileAsync(string ext)
    {
        byte[] bytes = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string temp = Path.Combine(Path.GetTempPath(), $"jdwstd_{Guid.NewGuid():N}{ext}");
        await File.WriteAllBytesAsync(temp, bytes, TestContext.Current.CancellationToken);
        return temp;
    }
}
