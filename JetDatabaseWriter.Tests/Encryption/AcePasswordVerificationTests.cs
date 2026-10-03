namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// <para>Regression tests for ACCDB (ACE) legacy password verification.</para>
/// <para>
/// The protected database is <c>NorthwindTraders.accdb</c> (ACE version 3)
/// encrypted with <see cref="AccessEncryptionFormat.AccdbLegacyPassword"/>:
/// flag <c>0x07</c> in raw header byte <c>0x62</c> and the password
/// XOR-encoded in the header password area at <c>0x42</c>; data pages stay
/// unencrypted. Opening without a password, or with a wrong or empty one,
/// throws <see cref="UnauthorizedAccessException"/>; the right password
/// opens it and returns data.
/// </para>
/// <para>
/// These tests used to open <c>AesEncrypted.accdb</c>, which was taken for a
/// password-protected file. It holds no password: its creation day makes raw
/// byte <c>0x62</c> read <c>0x07</c>, and the library's legacy password mask
/// had been fitted to its empty-password pattern, so "secret" happened to
/// verify against it.
/// </para>
/// </summary>
public sealed class AcePasswordVerificationTests
{
    private const string Password = TestDatabases.AesEncryptedPassword;

    private static readonly AccessReaderOptions CorrectPasswordOptions = new()
    {
        Password = Password.AsMemory(),
        UseLockFile = false,
    };

    private static readonly Lazy<Task<byte[]>> ProtectedNorthwind = new(BuildProtectedNorthwindAsync);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ═══════════════════════════════════════════════════════════════════
    // 1. PASSWORD DETECTION — reader must detect that a password is required
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AccdbPassword_OpenWithoutPassword_ThrowsUnauthorizedAccessException()
    {
        UnauthorizedAccessException ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await OpenProtectedAsync(new AccessReaderOptions { UseLockFile = false }));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AccdbPassword_OpenWithWrongPassword_ThrowsUnauthorizedAccessException()
    {
        // Providing an incorrect password must produce a clear error, not silent data corruption.
        var options = new AccessReaderOptions
        {
            Password = "definitely_wrong".AsMemory(),
            UseLockFile = false,
        };

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await OpenProtectedAsync(options));
    }

    [Fact]
    public async Task AccdbPassword_OpenWithEmptyPassword_ThrowsUnauthorizedAccessException()
    {
        // An empty-string password is not the same as no password — it should still fail.
        var options = new AccessReaderOptions
        {
            Password = string.Empty.AsMemory(),
            UseLockFile = false,
        };

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await OpenProtectedAsync(options));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. CORRECT PASSWORD — reader opens successfully and returns data
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AccdbPassword_OpenWithCorrectPassword_Succeeds()
    {
        await using AccessReader reader = await OpenProtectedAsync(CorrectPasswordOptions);
        await reader.ListTablesAsync(Ct);
    }

    [Fact]
    public async Task AccdbPassword_ListTables_WithCorrectPassword_ReturnsTheSourceTables()
    {
        await using AccessReader reader = await OpenProtectedAsync(CorrectPasswordOptions);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(Ct);

        await using AccessReader source = await TestDatabases.OpenAsync(TestDatabases.NorthwindTraders, cancellationToken: Ct);
        Assert.NotEmpty(tables);
        Assert.Equal(await source.ListTablesAsync(Ct), tables);
    }

    [Fact]
    public async Task AccdbPassword_ReadTable_WithCorrectPassword_ReturnsRows()
    {
        await using AccessReader reader = await OpenProtectedAsync(CorrectPasswordOptions);
        string table = await FirstTableWithRowsAsync(reader);

        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: Ct);
        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "Table should contain rows after password authentication.");
    }

    [Fact]
    public async Task AccdbPassword_StreamRows_WithCorrectPassword_ReturnsRows()
    {
        await using AccessReader reader = await OpenProtectedAsync(CorrectPasswordOptions);
        string table = await FirstTableWithRowsAsync(reader);

        int count = await reader.Rows(table, cancellationToken: Ct).CountAsync(Ct);
        Assert.True(count > 0, "StreamRows should yield rows after password authentication.");
    }

    [Fact]
    public async Task AccdbPassword_GetStatistics_WithCorrectPassword_ReturnsValidStats()
    {
        await using AccessReader reader = await OpenProtectedAsync(CorrectPasswordOptions);
        DatabaseStatistics stats = await reader.GetStatisticsAsync(Ct);

        Assert.True(stats.TableCount > 0, "Should report tables after authentication.");
        Assert.True(stats.TotalRows > 0, "Should report rows after authentication.");
    }

    [Fact]
    public async Task AccdbPassword_GetColumnMetadata_WithCorrectPassword_ReturnsColumns()
    {
        await using AccessReader reader = await OpenProtectedAsync(CorrectPasswordOptions);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(Ct);
        Assert.NotEmpty(tables);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tables[0], Ct);
        Assert.NotEmpty(meta);
        Assert.All(meta, col =>
        {
            Assert.False(string.IsNullOrEmpty(col.Name), "Column name should be readable.");
            Assert.NotEqual(typeof(object), col.ClrType);
        });
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. AesEncrypted.accdb — the fixture these tests once used has no password
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AesEncryptedFixture_IsNotPasswordProtected()
    {
        byte[] bytes = await File.ReadAllBytesAsync(TestDatabases.AesEncrypted, Ct);

        // ACE version 3, and raw byte 0x62 reads 0x07, the legacy password
        // flag value, yet the header password area holds no password.
        Assert.Equal(3, bytes[0x14]);
        Assert.Equal(0x07, bytes[0x62]);
        Assert.False(EncryptionManager.HasHeaderPassword(bytes, DatabaseFormat.AceAccdb));
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(TestDatabases.AesEncrypted, Ct));

        await using AccessReader reader = await AccessReader.OpenAsync(TestDatabases.AesEncrypted, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.NotEmpty(await reader.ListTablesAsync(Ct));
    }

    // ───── Helpers ───────────────────────────────────────────────────

    private static async Task<byte[]> BuildProtectedNorthwindAsync()
    {
        byte[] source = await File.ReadAllBytesAsync(TestDatabases.NorthwindTraders, CancellationToken.None);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(source.AsMemory(), CancellationToken.None);
        stream.Position = 0;
        await AccessWriter.EncryptAsync(stream, Password.AsMemory(), AccessEncryptionFormat.AccdbLegacyPassword, CancellationToken.None);
        return stream.ToArray();
    }

    private static async Task<AccessReader> OpenProtectedAsync(AccessReaderOptions options)
    {
        byte[] bytes = await ProtectedNorthwind.Value;
        Assert.Equal(AccessEncryptionFormat.AccdbLegacyPassword, EncryptionConverter.Detect(bytes));
        return await AccessReader.OpenAsync(new MemoryStream(bytes, writable: false), options, leaveOpen: false, Ct);
    }

    private static async Task<string> FirstTableWithRowsAsync(AccessReader reader)
    {
        foreach (string table in await reader.ListTablesAsync(Ct))
        {
            if (await reader.GetRealRowCountAsync(table, Ct) > 0)
            {
                return table;
            }
        }

        throw new InvalidOperationException("No table has rows.");
    }
}
