namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Regression tests for ACCDB (ACE) legacy password verification.
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
/// The reader detects the ACE password flag at offset 0x62 for ACCDB files
/// (ver ≥ 2) and verifies the password using the ACE internal scheme. Opening
/// without a password, or with the wrong password, throws
/// <see cref="UnauthorizedAccessException"/>; opening with the correct password
/// succeeds and returns data.
/// </summary>
public sealed class AcePasswordVerificationTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static readonly AccessReaderOptions CorrectPasswordOptions = new()
    {
        Password = SecureStringTestHelper.FromString(TestDatabases.AesEncryptedPassword),
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
        // The correct password should open the database without error.
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

        int count = await reader.Rows(tables[0], cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
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
}
