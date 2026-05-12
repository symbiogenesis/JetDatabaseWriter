namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>
/// Round-trip tests for the encryption-mutation API exposed by
/// <see cref="AccessWriter.EncryptAsync(string, string, AccessEncryptionFormat, AccessWriterOptions?, System.Threading.CancellationToken)"/>,
/// <see cref="AccessWriter.ChangePasswordAsync(string, string, string, AccessWriterOptions?, System.Threading.CancellationToken)"/>,
/// and <see cref="AccessWriter.DecryptAsync(string, string, AccessWriterOptions?, System.Threading.CancellationToken)"/>.
///
/// Each selectable format follows the same round-trip:
///   1. Clone an unencrypted source database to a temp file.
///   2. Encrypt with one of the supported target formats.
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

    // ───── Lock files ────────────────────────────────────────────────

    [Fact]
    public async Task EncryptAsync_WithLockFileEnabled_RespectsExistingLockFile()
    {
        var ct = TestContext.Current.CancellationToken;

        string blockedPath = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        string blockedLockPath = LockFileSlotWriter.GetLockFilePath(blockedPath);
        await File.WriteAllBytesAsync(blockedLockPath, new byte[] { 1 }, ct);
        _tempFiles.Add(blockedLockPath);

        await Assert.ThrowsAsync<IOException>(async () =>
            await AccessWriter.EncryptAsync(
                blockedPath,
                FirstPassword,
                AccessEncryptionFormat.AccdbLegacyPassword,
                cancellationToken: ct));

        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(blockedPath, ct));

        string allowedPath = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        string allowedLockPath = LockFileSlotWriter.GetLockFilePath(allowedPath);
        await File.WriteAllBytesAsync(allowedLockPath, new byte[LockFileSlotWriter.SlotSize], ct);
        _tempFiles.Add(allowedLockPath);

        var options = new AccessWriterOptions
        {
            UseLockFile = true,
            RespectExistingLockFile = false,
        };

        await AccessWriter.EncryptAsync(allowedPath, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, options, ct);

        Assert.Equal(AccessEncryptionFormat.AccdbLegacyPassword, await AccessWriter.DetectEncryptionFormatAsync(allowedPath, ct));
        Assert.False(File.Exists(allowedLockPath));
        await AssertOpenableAsync(allowedPath, FirstPassword, await ListTablesAsync(blockedPath, password: null));
    }

    // ───── Jet4 RC4 ──────────────────────────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_Jet4Rc4_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken);

        // Open BEFORE detect.
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        Assert.Equal(AccessEncryptionFormat.Jet4Rc4, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB legacy ;pwd= ────────────────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbLegacy_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.AccdbLegacyPassword, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB AES-128 CFB-wrapped (synthetic legacy) ──────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAesCfbWrapped_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAesCfbWrapped, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(
            AccessEncryptionFormat.AccdbAesCfbWrapped,
            await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB Agile (Access-native flat layout) ───────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAgile_RoundTripsThroughChangePassword()
    {
        var ct = TestContext.Current.CancellationToken;

        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAgile, NoLockOptions, ct);
        await AssertFlatAgileAsync(path, ct);

        // Open BEFORE detect.
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, ct));

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, ct);
        await AssertFlatAgileAsync(path, ct);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, ct);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB Agile (Office Crypto CFB v4 layout) ─────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAgileCfb_RoundTripsThroughChangePassword()
    {
        var ct = TestContext.Current.CancellationToken;

        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAgileCfb, NoLockOptions, ct);
        await AssertOfficeCryptoAgileCfbV4Async(path, ct);
        Assert.Equal(AccessEncryptionFormat.AccdbAgileCfb, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, ct);
        await AssertOfficeCryptoAgileCfbV4Async(path, ct);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, ct);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB Standard (Office 2007) ──────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbStandard_RoundTripsThroughChangePassword()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbStandard, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(
            AccessEncryptionFormat.AccdbStandard,
            await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));

        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── Cross-format re-encryption ────────────────────────────────

    [Fact]
    public async Task ReEncrypt_AccdbLegacyToAgile_PreservesData()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        List<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, FirstPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.EncryptAsync(path, SecondPassword, AccessEncryptionFormat.AccdbAgile, NoLockOptions, TestContext.Current.CancellationToken);

        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, SecondPassword, originalTables);
    }

    // ───── Negative cases ────────────────────────────────────────────

    [Fact]
    public async Task EncryptAsync_OnAlreadyEncryptedFile_Throws()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");
        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.EncryptAsync(path, SecondPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DecryptAsync_OnUnencryptedFile_Throws()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.DecryptAsync(path, FirstPassword, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangePasswordAsync_OnUnencryptedFile_Throws()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangePasswordAsync_WithWrongOldPassword_Throws()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await AccessWriter.ChangePasswordAsync(path, "totally-wrong", SecondPassword, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EncryptAsync_Jet4Rc4_OnAccdbFile_ThrowsNotSupported()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EncryptAsync_Agile_OnMdbFile_ThrowsNotSupported()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAgile, NoLockOptions, TestContext.Current.CancellationToken));
    }

    // ───── CreateDatabaseAsync ignores password ────────────────────────

    /// <summary>
    /// <c>AccessWriter.CreateDatabaseAsync</c> always produces an
    /// unencrypted file even when the options carry a password — the intended
    /// workflow is create-then-encrypt via <c>AccessWriter.EncryptAsync</c>.
    /// </summary>
    [Fact]
    public async Task CreateDatabaseAsync_WithPasswordOption_ProducesUnencryptedFile()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions("ignoredpassword") { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains("T", tables);
    }

    // ───── Stream overload sanity check ──────────────────────────────

    [Fact]
    public async Task EncryptAndDecryptAsync_StreamOverload_RoundTrips()
    {
        byte[] original = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        await using var ms = new MemoryStream(original.Length);
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

    // ───── LvProp / column metadata preservation ───────────────────

    /// <summary>
    /// Verifies that MSysObjects.LvProp (column property metadata) survives
    /// Encrypt → ChangePassword → Decrypt operations. The concern is that
    /// re-encryption might corrupt internal metadata blocks containing
    /// column-level property information (expression definitions, format
    /// strings, etc.).
    /// </summary>
    [Fact]
    public async Task EncryptDecrypt_AccdbAgile_PreservesColumnMetadata()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        var originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAgile, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);

        var afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_Jet4Rc4_PreservesColumnMetadata()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");

        var originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);

        var afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_AccdbLegacyPassword_PreservesColumnMetadata()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        var originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);

        var afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_AccdbAesCfbWrapped_PreservesColumnMetadata()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        var originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbAesCfbWrapped, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);

        var afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_AccdbStandard_PreservesColumnMetadata()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        var originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPassword, AccessEncryptionFormat.AccdbStandard, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPassword, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPassword, NoLockOptions, TestContext.Current.CancellationToken);

        var afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    // ───── Helpers ───────────────────────────────────────────────────

    private static void AssertColumnMetadataEqual(
        Dictionary<string, List<ColumnMetadata>> expected,
        Dictionary<string, List<ColumnMetadata>> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (table, columns) in expected)
        {
            Assert.True(actual.ContainsKey(table), $"Table '{table}' missing after password change.");
            var afterCols = actual[table];
            Assert.Equal(columns.Count, afterCols.Count);
            for (int i = 0; i < columns.Count; i++)
            {
                Assert.Equal(columns[i].Name, afterCols[i].Name);
                Assert.Equal(columns[i].ClrType, afterCols[i].ClrType);
                Assert.Equal(columns[i].TypeName, afterCols[i].TypeName);
                Assert.Equal(columns[i].MaxLength, afterCols[i].MaxLength);
            }
        }
    }

    private static async Task<Dictionary<string, List<ColumnMetadata>>> GetAllColumnMetadataAsync(string path, string? password)
    {
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password.AsMemory(),
        };
        await using var reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        var result = new Dictionary<string, List<ColumnMetadata>>(StringComparer.Ordinal);
        foreach (string table in tables)
        {
            List<ColumnMetadata> cols = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            result[table] = cols;
        }

        return result;
    }

    private static readonly AccessWriterOptions NoLockOptions = new() { UseLockFile = false };

    private static async Task AssertFlatAgileAsync(string path, CancellationToken cancellationToken)
    {
        byte[] bytes = await File.ReadAllBytesAsync(path, cancellationToken);

        Assert.False(CompoundFileReader.HasCompoundFileMagic(bytes));
        Assert.True(OfficeCryptoAgile.IsFlatAgileEncrypted(bytes));
    }

    private static async Task AssertOfficeCryptoAgileCfbV4Async(string path, CancellationToken cancellationToken)
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
        Assert.True(OfficeCryptoAgile.IsAgileEncryptionInfo(streams["EncryptionInfo"]));
    }

    private static async Task<List<string>> ListTablesAsync(string path, string? password)
    {
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password.AsMemory(),
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
            Password = password.AsMemory(),
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
            Password = wrongPassword.AsMemory(),
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
