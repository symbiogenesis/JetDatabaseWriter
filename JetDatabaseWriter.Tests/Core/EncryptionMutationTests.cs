namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Round-trip tests for the encryption-mutation API exposed by
/// <see cref="AccessWriter.EncryptAsync(string, string, AccessEncryptionFormat, AccessWriterOptions?, System.Threading.CancellationToken)"/>,
/// <see cref="AccessWriter.ChangePasswordAsync(string, string, string, AccessWriterOptions?, System.Threading.CancellationToken)"/>,
/// and <see cref="AccessWriter.DecryptAsync(string, string, AccessWriterOptions?, System.Threading.CancellationToken)"/>.
///
/// Each format follows the same round-trip:
///   1. Clone an unencrypted source database to a temp file.
///   2. Encrypt with one of the four supported formats.
///   3. Verify <see cref="AccessReader"/> can re-open with the new password.
///   4. Change the password; verify only the new password works.
///   5. Decrypt; verify the file opens with no password and matches the original tables.
/// </summary>
public sealed class EncryptionMutationTests(DatabaseCache db) : IClassFixture<DatabaseCache>, IDisposable
{
    private const string FirstPassword = "OriginalPa$$";
    private const string SecondPassword = "Rotated2!Pa$";

    private readonly List<string> _tempFiles = [];

    public void Dispose()
    {
        foreach (string path in _tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // best-effort cleanup
            }
        }
    }

    // ───── Detection ─────────────────────────────────────────────────

    [Fact]
    public async Task DetectEncryptionFormat_OnUnencryptedFile_ReturnsNone()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        AccessEncryptionFormat fmt = await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken);

        Assert.Equal(AccessEncryptionFormat.None, fmt);
    }

    // ───── Jet4 RC4 ──────────────────────────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_Jet4Rc4_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions(), TestContext.Current.CancellationToken);

        // Open BEFORE detect.
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        Assert.Equal(AccessEncryptionFormat.Jet4Rc4, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB legacy ;pwd= ────────────────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbLegacy_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.AccdbLegacyPassword, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB AES-128 CFB-wrapped (synthetic legacy) ──────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAesCfbWrapped_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAesCfbWrapped, NoLockOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));

        // The cheap detector cannot distinguish synthetic CFB-wrapped AES from
        // real Agile by header magic alone; use the reader as the source of
        // truth.
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB Agile (Office Crypto API) ───────────────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAgile_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAgile, NoLockOptions(), TestContext.Current.CancellationToken);

        // Open BEFORE detect.
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── Cross-format re-encryption ────────────────────────────────

    [Fact]
    public async Task ReEncrypt_AccdbLegacyToAgile_PreservesData()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, FirstPassword, NoLockOptions(), TestContext.Current.CancellationToken);
        await AccessWriter.EncryptAsync(path, SecondPassword, AccessEncryptionFormat.AccdbAgile, NoLockOptions(), TestContext.Current.CancellationToken);

        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, SecondPassword, originalTables);
    }

    // ───── Negative cases ────────────────────────────────────────────

    [Fact]
    public async Task EncryptAsync_OnAlreadyEncryptedFile_Throws()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");
        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions(), TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.EncryptAsync(path, SecondPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DecryptAsync_OnUnencryptedFile_Throws()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.DecryptAsync(path, FirstPassword, NoLockOptions(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangePasswordAsync_OnUnencryptedFile_Throws()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangePasswordAsync_WithWrongOldPassword_Throws()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions(), TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await AccessWriter.ChangePasswordAsync(path, "totally-wrong", SecondPassword, NoLockOptions(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EncryptAsync_Jet4Rc4_OnAccdbFile_ThrowsNotSupported()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EncryptAsync_Agile_OnMdbFile_ThrowsNotSupported()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAgile, NoLockOptions(), TestContext.Current.CancellationToken));
    }

    // ───── Stream overload sanity check ──────────────────────────────

    [Fact]
    public async Task EncryptAndDecryptAsync_StreamOverload_RoundTrips()
    {
        byte[] original = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var ms = new MemoryStream(original.Length);
        await ms.WriteAsync(original.AsMemory(), TestContext.Current.CancellationToken);
        ms.Position = 0;

        await AccessWriter.EncryptAsync(ms, FirstPassword, AccessEncryptionFormat.AccdbAgile, TestContext.Current.CancellationToken);

        ms.Position = 0;
        await AccessWriter.DecryptAsync(ms, FirstPassword, TestContext.Current.CancellationToken);

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);
    }

    // ───── Helpers ───────────────────────────────────────────────────

    private static AccessWriterOptions NoLockOptions() => new() { UseLockFile = false };

    private static async Task<List<string>> ListTablesAsync(string path, string? password)
    {
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password == null ? null : SecureStringTestHelper.FromString(password),
        };
        await using var reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        return await reader.ListTablesAsync(TestContext.Current.CancellationToken);
    }

    private static async Task AssertOpenableAsync(string path, string? password, List<string> expectedTables)
    {
        List<string> tables = await ListTablesAsync(path, password);
        Assert.NotEmpty(tables);
        Assert.Equal(expectedTables.Count, tables.Count);

        // Sanity check: read at least one row from the first user table (if any).
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password == null ? null : SecureStringTestHelper.FromString(password),
        };
        await using var reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        DataTable dt = await reader.ReadDataTableAsync(tables[0], maxRows: 5, cancellationToken: TestContext.Current.CancellationToken)
            ?? throw new InvalidOperationException("ReadDataTableAsync returned null.");
        Assert.True(dt.Columns.Count > 0);
    }

    private static async Task AssertWrongPasswordAsync(string path, string wrongPassword)
    {
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = SecureStringTestHelper.FromString(wrongPassword),
        };
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken));
    }

    private async Task<string> CloneAsync(string sourcePath, string ext)
    {
        byte[] bytes = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        string temp = Path.Combine(Path.GetTempPath(), $"jdwenc_{Guid.NewGuid():N}{ext}");
        await File.WriteAllBytesAsync(temp, bytes, TestContext.Current.CancellationToken);
        _tempFiles.Add(temp);
        return temp;
    }
}
