namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for ACCDB (ACE) legacy password verification.
///
/// README limitation addressed:
///   "ACCDB (AES) encryption — Header-level detection and password verification work;
///    full AES page decryption for genuinely-encrypted Access 2007+ .accdb files is
///    not yet supported."
///
/// The <c>AesEncrypted.accdb</c> fixture was created with:
///   <c>$access.DBEngine.CompactDatabase(plain, dest, ";;", 4, ";pwd=secret")</c>
/// using Access 16 COM automation. Its characteristics:
///   • Header bytes: 00 01 00 00 (standard ACCDB, NOT CFB magic)
///   • Version byte 0x14: 0x03 (Access 2010 / ACE 14)
///   • Encryption flag 0x62: 0x07 (bits 0/1/2 set)
///   • Data pages: NOT AES-encrypted; readable without decryption
///   • Password: stored via ACE internal scheme (not the Jet4 XOR mask at 0x42)
///
/// ── Current behaviour (all tests below FAIL) ────────────────────────────────
///   The reader's ACCDB password check only fires on CFB magic (D0 CF 11 E0).
///   Since this file has a standard ACCDB header, the password check is skipped
///   entirely. The reader opens the file without requiring or verifying a password.
///
/// ── Desired behaviour (when implemented) ────────────────────────────────────
///   The reader should detect the ACE password flag at offset 0x62 for ACCDB files
///   (ver ≥ 2) and verify the password using the ACE internal scheme. Opening without
///   a password, or with the wrong password, should throw UnauthorizedAccessException.
///   Opening with the correct password ("secret") should succeed and return data.
///
/// ── Implementation hints ────────────────────────────────────────────────────
///   The ACE password encoding differs from Jet4:
///     Jet4 (ver == 1): XOR with fixed mask + creation-date bytes at 0x42.
///     ACE (ver >= 2): uses a different mask and/or hash scheme at 0x42.
///   The Jackcess library (Java) and mdb-tools have reference implementations.
///   Key areas to update:
///     1. AccessReader constructor: add ACCDB password detection for ver >= 2
///        when byte 0x62 has password bits set (but NOT CFB magic).
///     2. Implement DecodeAccdbPassword() with the ACE-specific XOR mask.
///     3. Throw UnauthorizedAccessException on missing/wrong password.
/// </summary>
public sealed class AcePasswordVerificationTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static readonly AccessReaderOptions CorrectPasswordOptions = new()
    {
        Password = SecureStringTestHelper.FromString("secret"),
        UseLockFile = false,
    };

    // ═══════════════════════════════════════════════════════════════════
    // 1. PASSWORD DETECTION — reader must detect that a password is required
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AccdbPassword_OpenWithoutPassword_ThrowsUnauthorizedAccessException()
    {
        // AesEncrypted.accdb has a password set via ACE CompactDatabase.
        // The reader must detect the password flag and throw when no password is provided.
        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(TestDatabases.AesEncrypted, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AccdbPassword_OpenWithWrongPassword_ThrowsUnauthorizedAccessException()
    {
        // Providing an incorrect password must produce a clear error, not silent data corruption.
        var options = new AccessReaderOptions
        {
            Password = SecureStringTestHelper.FromString("definitely_wrong"),
            UseLockFile = false,
        };

        await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(TestDatabases.AesEncrypted, options, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AccdbPassword_OpenWithEmptyPassword_ThrowsUnauthorizedAccessException()
    {
        // An empty-string password is not the same as no password — it should still fail.
        var options = new AccessReaderOptions
        {
            Password = SecureStringTestHelper.FromString(string.Empty),
            UseLockFile = false,
        };

        await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(TestDatabases.AesEncrypted, options, TestContext.Current.CancellationToken));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. CORRECT PASSWORD — reader opens successfully and returns data
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AccdbPassword_OpenWithCorrectPassword_Succeeds()
    {
        // The correct password ("secret") should open the database without error.
        var reader = await db.GetReaderAsync(TestDatabases.AesEncrypted, CorrectPasswordOptions, TestContext.Current.CancellationToken);
        await reader.ListTablesAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AccdbPassword_ListTables_WithCorrectPassword_ReturnsNonEmpty()
    {
        // After authentication, ListTables should return the original database tables.
        var reader = await db.GetReaderAsync(TestDatabases.AesEncrypted, CorrectPasswordOptions, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    [Fact]
    public async Task AccdbPassword_ReadTable_WithCorrectPassword_ReturnsRows()
    {
        // Reading table data after password verification should return valid rows.
        var reader = await db.GetReaderAsync(TestDatabases.AesEncrypted, CorrectPasswordOptions, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        DataTable dt = (await reader.ReadDataTableAsync(tables[0], cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "Table should contain rows after password authentication.");
    }

    [Fact]
    public async Task AccdbPassword_StreamRows_WithCorrectPassword_ReturnsRows()
    {
        // Streaming rows should work normally after password verification.
        var reader = await db.GetReaderAsync(TestDatabases.AesEncrypted, CorrectPasswordOptions, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int count = await reader.StreamRowsAsync(tables[0], cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        Assert.True(count > 0, "StreamRows should yield rows after password authentication.");
    }

    [Fact]
    public async Task AccdbPassword_GetStatistics_WithCorrectPassword_ReturnsValidStats()
    {
        // Statistics should be accessible after correct password authentication.
        var reader = await db.GetReaderAsync(TestDatabases.AesEncrypted, CorrectPasswordOptions, TestContext.Current.CancellationToken);
        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TableCount > 0, "Should report tables after authentication.");
        Assert.True(stats.TotalRows > 0, "Should report rows after authentication.");
    }

    [Fact]
    public async Task AccdbPassword_GetColumnMetadata_WithCorrectPassword_ReturnsColumns()
    {
        // Column metadata should be fully readable after password verification.
        var reader = await db.GetReaderAsync(TestDatabases.AesEncrypted, CorrectPasswordOptions, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tables[0], TestContext.Current.CancellationToken);
        Assert.NotEmpty(meta);
        Assert.All(meta, col =>
        {
            Assert.False(string.IsNullOrEmpty(col.Name), "Column name should be readable.");
            Assert.NotEqual(typeof(object), col.ClrType);
        });
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. HEADER DETECTION — verify the fixture has the expected header
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AccdbPassword_Fixture_IsNotCfbWrapped()
    {
        // The fixture must NOT have CFB magic — it uses ACE internal password, not AES page encryption.
        // This confirms the gap: the CFB-based detection path does NOT fire for this file.
        byte[] bytes = await db.GetFileAsync(TestDatabases.AesEncrypted, TestContext.Current.CancellationToken);

        Assert.NotEqual(0xD0, bytes[0]);
        Assert.Equal(0x00, bytes[0]);
        Assert.Equal(0x01, bytes[1]);
    }

    [Fact]
    public async Task AccdbPassword_Fixture_HasAccdbVersion()
    {
        // Version byte at 0x14 must be >= 2 (ACCDB format).
        byte[] bytes = await db.GetFileAsync(TestDatabases.AesEncrypted, TestContext.Current.CancellationToken);
        Assert.True(bytes[0x14] >= 2, $"Expected ACCDB version >= 2, got 0x{bytes[0x14]:X2}");
    }

    [Fact]
    public async Task AccdbPassword_Fixture_HasPasswordBitsSet()
    {
        // Byte 0x62 should have password-related bits set.
        // Access 16 CompactDatabase with ";pwd=secret" sets byte 0x62 = 0x07.
        byte[] bytes = await db.GetFileAsync(TestDatabases.AesEncrypted, TestContext.Current.CancellationToken);
        byte encFlag = bytes[0x62];

        Assert.True(
            (encFlag & 0x01) != 0,
            $"Expected password bit (0x01) set at offset 0x62, got 0x{encFlag:X2}");
    }

    // ═══════════════════════════════════════════════════════════════════
    // 4. PASSWORD IS NOT THE JET4 XOR SCHEME
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AccdbPassword_Jet4XorDecode_DoesNotReturnSecret()
    {
        // The existing DecodeJet4Password logic uses the Jet4 XOR mask.
        // For ACCDB files, the ACE internal password scheme is different.
        // Decoding with the Jet4 mask should NOT produce "secret".
        byte[] hdr = await db.GetFileAsync(TestDatabases.AesEncrypted, TestContext.Current.CancellationToken);

        // Replicate the Jet4 XOR decode logic
        byte[] jet4PwdMask =
        [
            0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
            0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
            0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
            0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
            0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
        ];

        var decoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            decoded[i] = (byte)(hdr[0x42 + i] ^ jet4PwdMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        string raw = System.Text.Encoding.Unicode.GetString(decoded);
        int nullIdx = raw.IndexOf('\0', StringComparison.Ordinal);
        string jet4Password = nullIdx >= 0 ? raw.Substring(0, nullIdx) : raw;

        // The Jet4 XOR scheme does not decode to "secret" for ACE-format files.
        // This confirms a different decode algorithm is needed.
        Assert.NotEqual("secret", jet4Password);
    }
}
