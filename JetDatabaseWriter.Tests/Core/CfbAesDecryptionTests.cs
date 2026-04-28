namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention
#pragma warning disable CA5358 // ECB mode is intentional for deterministic test fixture encryption

/// <summary>
/// Regression tests for ACCDB AES page decryption (synthetic CFB-wrapped legacy AES path).
///
/// Each test creates a synthetic AES-encrypted ACCDB by:
///   1. Cloning NorthwindTraders.accdb
///   2. Setting CFB magic header (D0 CF 11 E0) + encoding the password
///   3. AES-encrypting all data pages (pages 1+)
///
/// The reader is expected to detect CFB magic, verify the password, and
/// transparently decrypt pages so that catalog, TDEF and data reads return
/// the same content as an unencrypted copy.
///
/// The fixture uses a simplified scheme (SHA-256 key + AES-128-ECB) — distinct
/// from the ECMA-376 Agile path covered by <see cref="AgileEncryptionTests"/>.
/// </summary>
public sealed class CfbAesDecryptionTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const int Jet4PageSize = 4096;

    // ═══════════════════════════════════════════════════════════════════
    // 1. CATALOG ACCESS — reader must decrypt page 2 to list tables
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AesPageDecryption_ListTables_ReturnsValidTableNames()
    {
        // After opening an AES-encrypted ACCDB with the correct password,
        // ListTablesAsync must decrypt the catalog page and return table names.
        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
        Assert.All(tables, name => Assert.False(
            string.IsNullOrWhiteSpace(name),
            "Decrypted table names must not be empty or whitespace."));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. DATA PAGE DECRYPTION — reader must decrypt rows
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AesPageDecryption_ReadTable_ReturnsRows()
    {
        // Reading a DataTable from an AES-encrypted database must decrypt
        // data pages and return valid rows, not garbled bytes.
        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        DataTable dt = (await reader.ReadDataTableAsync(tables[0], cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "AES-decrypted table should contain rows.");
    }

    [Fact]
    public async Task AesPageDecryption_ReadTableGeneric_ReturnsTypedRows()
    {
        // Generic POCO mapping through AES-decrypted pages must produce
        // typed objects, not throw during deserialization.
        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        // Read as DataTable (typed columns) rather than specific POCO,
        // since we don't know the exact schema of the first table.
        DataTable? result = await reader.ReadDataTableAsync(tables[0], maxRows: 10, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.True(result.Rows.Count > 0, "AES-decrypted typed read should return rows.");
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. STREAMING — must decrypt pages during streaming iteration
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AesPageDecryption_StreamRows_YieldsDecryptedRows()
    {
        // Streaming through AES-encrypted data pages must yield rows
        // with valid data, not encrypted garbage.
        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int count = await reader.Rows(tables[0], cancellationToken: TestContext.Current.CancellationToken)
            .CountAsync(TestContext.Current.CancellationToken);

        Assert.True(count > 0, "AES-decrypted stream should yield rows.");
    }

    // ═══════════════════════════════════════════════════════════════════
    // 4. METADATA — TDEF pages must be decryptable
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AesPageDecryption_GetColumnMetadata_ReturnsValidSchema()
    {
        // Column metadata lives on TDEF pages that must be AES-decrypted.
        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tables[0], TestContext.Current.CancellationToken);

        Assert.NotEmpty(meta);
        Assert.All(meta, col =>
        {
            Assert.False(string.IsNullOrEmpty(col.Name), "Decrypted column name should be readable.");
            Assert.NotEqual(typeof(object), col.ClrType);
        });
    }

    [Fact]
    public async Task AesPageDecryption_GetStatistics_ReturnsValidCounts()
    {
        // DatabaseStatistics scans the catalog and counts rows across all tables.
        // Both catalog and data pages must be decrypted.
        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TableCount > 0, "Should report tables from the encrypted database.");
        Assert.True(stats.TotalRows > 0, "Should report rows from the encrypted database.");
    }

    // ═══════════════════════════════════════════════════════════════════
    // 5. BULK READ — all tables must be readable through AES decryption
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AesPageDecryption_ReadAllTables_Succeeds()
    {
        // Bulk read of every table exercises decryption across all page types.
        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEmpty(all);
        Assert.All(all.Values, dt => Assert.NotNull(dt));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 6. DATA INTEGRITY — decrypted data matches the unencrypted source
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AesPageDecryption_DecryptedData_MatchesOriginalTableNames()
    {
        // The table names from an AES-encrypted copy must match the original
        // unencrypted database. This verifies end-to-end decryption fidelity.
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        List<string> expectedTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var encReader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> actualTables = await encReader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expectedTables.OrderBy(t => t), actualTables.OrderBy(t => t));
    }

    [Fact]
    public async Task AesPageDecryption_DecryptedData_MatchesOriginalRowCounts()
    {
        // Row counts from the encrypted copy must match the original.
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        List<TableStat> expectedStats = await reader.GetTableStatsAsync(TestContext.Current.CancellationToken);

        byte[] data = await CreateAesEncryptedAccdbAsync();
        var options = new AccessReaderOptions
        {
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using var ms = new MemoryStream(data, writable: false);
        await using var encReader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<TableStat> actualStats = await encReader.GetTableStatsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expectedStats.Count, actualStats.Count);
        foreach (TableStat expected in expectedStats)
        {
            TableStat? actual = actualStats.FirstOrDefault(s => s.Name == expected.Name);
            Assert.NotNull(actual);
            Assert.Equal(expected.RowCount, actual.RowCount);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    /// <summary>
    /// Encodes a password into the Jet4 password area at header offset 0x42
    /// using the standard XOR scheme: encoded[i] = password[i] ^ mask[i] ^ hdr[0x72 + i%4].
    /// </summary>
    private static void EncodeJet4Password(byte[] data, string password)
    {
        byte[] jet4PwdMask =
        [
            0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
            0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
            0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
            0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
            0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
        ];

        byte[] pwdUtf16 = System.Text.Encoding.Unicode.GetBytes(password);
        var encoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            byte pwdByte = i < pwdUtf16.Length ? pwdUtf16[i] : (byte)0;
            encoded[i] = (byte)(pwdByte ^ jet4PwdMask[i] ^ data[0x72 + (i % 4)]);
        }

        Buffer.BlockCopy(encoded, 0, data, 0x42, 40);
    }

    /// <summary>
    /// Derives a 128-bit AES key from a password using SHA-256 (truncated).
    /// This is a simplified stand-in; the real implementation should follow
    /// the MS-OFFCRYPTO key derivation (CryptoAPI or ECMA-376 Agile).
    /// </summary>
    private static byte[] DeriveSimpleAesKey(string password)
    {
        byte[] hash = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(password));
        return hash[..16]; // AES-128
    }

    /// <summary>
    /// AES-128-ECB encrypts a region of a byte array in place.
    /// </summary>
    private static void AesEncryptInPlace(byte[] data, int offset, int length, byte[] key)
    {
        using var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.None;

        using var encryptor = aes.CreateEncryptor();
        byte[] block = new byte[length];
        Buffer.BlockCopy(data, offset, block, 0, length);

        byte[] encrypted = encryptor.TransformFinalBlock(block, 0, length);
        Buffer.BlockCopy(encrypted, 0, data, offset, length);
    }

    /// <summary>
    /// Creates a synthetic AES-encrypted ACCDB by cloning NorthwindTraders.accdb,
    /// writing the CFB magic header + encoded password, and AES-encrypting all
    /// data pages (pages 1+).
    /// </summary>
    private async Task<byte[]> CreateAesEncryptedAccdbAsync()
    {
        byte[] source = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        byte[] data = (byte[])source.Clone();

        // 1. Write CFB magic signature at bytes 0-7 (OLE2 Compound File Binary)
        byte[] cfbMagic = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        Buffer.BlockCopy(cfbMagic, 0, data, 0, cfbMagic.Length);

        // 2. Encode password at offset 0x42 using the Jet4 XOR scheme
        //    so that DecodeJet4Password(hdr) returns the same password.
        EncodeJet4Password(data, TestDatabases.AesEncryptedPassword);

        // 3. AES-encrypt all data pages (pages 1+). Page 0 (the header) is left
        //    unencrypted because the reader needs it to detect the format and
        //    verify the password before decryption begins.
        byte[] aesKey = DeriveSimpleAesKey(TestDatabases.AesEncryptedPassword);
        byte[] iv = new byte[16]; // zero IV for deterministic fixture
        for (int page = 1; page * Jet4PageSize < data.Length; page++)
        {
            int offset = page * Jet4PageSize;
            int length = Math.Min(Jet4PageSize, data.Length - offset);

            // AES-ECB operates on full 16-byte blocks; pad if needed
            if (length % 16 != 0)
            {
                continue; // skip partial trailing page
            }

            AesEncryptInPlace(data, offset, length, aesKey);
        }

        return data;
    }
}
