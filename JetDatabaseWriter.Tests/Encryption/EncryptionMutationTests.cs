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
/// <para>
/// Round-trip tests for the encryption-mutation API exposed by
/// <see cref="AccessWriter.EncryptAsync(string, ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, CancellationToken)"/>,
/// <see cref="AccessWriter.ChangePasswordAsync(string, ReadOnlyMemory{char}, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>,
/// and <see cref="AccessWriter.DecryptAsync(string, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>.
/// </para>
/// <para>
/// Each selectable format follows the same round-trip:
///   1. Clone an unencrypted source database to a temp file.
///   2. Encrypt with one of the supported target formats.
///   3. Verify <see cref="AccessReader"/> can re-open with the new password.
///   4. Change the password; verify only the new password works.
///   5. Decrypt; verify the file opens with no password and matches the original tables.
/// </para>
/// </summary>
/// <param name="db">The database input.</param>
public sealed class EncryptionMutationTests(DatabaseCache db) : IClassFixture<DatabaseCache>, IDisposable
{
    private const string FirstPassword = "OriginalPa$$";
    private const string SecondPassword = "Rotated2!Pa$";

    private static ReadOnlyMemory<char> FirstPasswordMemory => FirstPassword.AsMemory();

    private static ReadOnlyMemory<char> SecondPasswordMemory => SecondPassword.AsMemory();

    private static ReadOnlyMemory<char> GoldenPasswordMemory => TestDatabases.EncryptedFixturePassword.AsMemory();

    private readonly List<string> tempFiles = [];

    private readonly List<string> tempDirectories = [];

    public void Dispose()
    {
        foreach (string path in this.tempFiles)
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

        foreach (string directory in this.tempDirectories)
        {
            try
            {
                Directory.Delete(directory, recursive: true);
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
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        AccessEncryptionFormat fmt = await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken);

        Assert.Equal(AccessEncryptionFormat.None, fmt);
    }

    // ───── Default target selection ─────────────────────────────────

    [Fact]
    public async Task EncryptAsync_DefaultTarget_OnAccdbFile_UsesAccessNativeAgile()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, options: NoLockOptions, cancellationToken: ct);

        await AssertFlatAgileAsync(path, ct);
        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, FirstPassword, originalTables);
    }

    [Fact]
    public async Task EncryptAsync_DefaultTarget_OnJet4MdbFile_UsesJet4Rc4()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string path = await this.CloneAsync(TestDatabases.AdventureWorks, ".mdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, options: NoLockOptions, cancellationToken: ct);

        Assert.Equal(AccessEncryptionFormat.Jet4Rc4, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, FirstPassword, originalTables);
    }

    [Fact]
    public async Task EncryptAsync_DefaultTarget_OnJet3MdbFile_ThrowsNotSupported()
    {
        string path = await this.CloneAsync(TestDatabases.Jet3Test, ".mdb");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(
                path,
                FirstPasswordMemory,
                options: NoLockOptions,
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
    }

    // ───── Lock files ────────────────────────────────────────────────

    [Fact]
    public async Task EncryptAsync_WithLockFileEnabled_RespectsExistingLockFile()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string blockedPath = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        string blockedLockPath = LockFileSlotWriter.GetLockFilePath(blockedPath);
        await File.WriteAllBytesAsync(blockedLockPath, [1], ct);
        this.tempFiles.Add(blockedLockPath);

        await Assert.ThrowsAsync<IOException>(async () =>
            await AccessWriter.EncryptAsync(
                blockedPath,
                FirstPasswordMemory,
                AccessEncryptionFormat.AccdbLegacyPassword,
                cancellationToken: ct));

        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(blockedPath, ct));

        string allowedPath = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        string allowedLockPath = LockFileSlotWriter.GetLockFilePath(allowedPath);
        await File.WriteAllBytesAsync(allowedLockPath, new byte[LockFileSlotWriter.SlotSize], ct);
        this.tempFiles.Add(allowedLockPath);

        var options = new AccessWriterOptions
        {
            UseLockFile = true,
            RespectExistingLockFile = false,
        };

        await AccessWriter.EncryptAsync(allowedPath, FirstPasswordMemory, AccessEncryptionFormat.AccdbLegacyPassword, options, ct);

        Assert.Equal(AccessEncryptionFormat.AccdbLegacyPassword, await AccessWriter.DetectEncryptionFormatAsync(allowedPath, ct));
        Assert.False(File.Exists(allowedLockPath));
        await AssertOpenableAsync(allowedPath, FirstPassword, await ListTablesAsync(blockedPath, password: null));
    }

    // ───── Jet4 RC4 ──────────────────────────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_Jet4Rc4_RoundTripsThroughChangePassword()
    {
        string path = await this.CloneAsync(TestDatabases.AdventureWorks, ".mdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken);

        // Open BEFORE detect.
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        Assert.Equal(AccessEncryptionFormat.Jet4Rc4, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));

        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB legacy password ─────────────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbLegacy_RoundTripsThroughChangePassword()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.AccdbLegacyPassword, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB AES-128 CFB-wrapped (synthetic legacy) ──────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAesCfbWrapped_RoundTripsThroughChangePassword()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbAesCfbWrapped, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(
            AccessEncryptionFormat.AccdbAesCfbWrapped,
            await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB Agile (Access-native flat layout) ───────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAgile_RoundTripsThroughChangePassword()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbAgile, NoLockOptions, ct);
        await AssertFlatAgileAsync(path, ct);

        // Open BEFORE detect.
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, ct));

        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, ct);
        await AssertFlatAgileAsync(path, ct);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, ct);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB Agile (Office Crypto CFB v4 layout) ─────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbAgileCfb_RoundTripsThroughChangePassword()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbAgileCfb, NoLockOptions, ct);
        await AssertOfficeCryptoAgileCfbV4Async(path, ct);
        Assert.Equal(AccessEncryptionFormat.AccdbAgileCfb, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, ct);
        await AssertOfficeCryptoAgileCfbV4Async(path, ct);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, ct);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, ct));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── ACCDB Standard (Office 2007) ──────────────────────────────

    [Fact]
    public async Task EncryptDecrypt_AccdbStandard_RoundTripsThroughChangePassword()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbStandard, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(
            AccessEncryptionFormat.AccdbStandard,
            await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));

        await AssertOpenableAsync(path, FirstPassword, originalTables);

        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AssertWrongPasswordAsync(path, FirstPassword);
        await AssertOpenableAsync(path, SecondPassword, originalTables);

        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, password: null, originalTables);
    }

    // ───── DecryptAsync restores Access's unencrypted header ─────────

    /// <summary>
    /// Decrypting must leave page 0 as Access writes an unencrypted header:
    /// encoding key 0 and the empty-password pattern (the creation day
    /// repeated) under the fixed header mask, with the creation date and
    /// sort order kept. Here that is the source's own page 0.
    /// </summary>
    /// <param name="source">"AdventureWorks", "Northwind", "WriterJet4" or "WriterAce".</param>
    /// <param name="format">The encryption to apply and remove.</param>
    [Theory]
    [InlineData("AdventureWorks", AccessEncryptionFormat.Jet4Rc4)]
    [InlineData("WriterJet4", AccessEncryptionFormat.Jet4Rc4)]
    [InlineData("Northwind", AccessEncryptionFormat.AccdbLegacyPassword)]
    [InlineData("WriterAce", AccessEncryptionFormat.AccdbLegacyPassword)]
    [InlineData("Northwind", AccessEncryptionFormat.AccdbAesCfbWrapped)]
    [InlineData("Northwind", AccessEncryptionFormat.AccdbAgile)]
    [InlineData("Northwind", AccessEncryptionFormat.AccdbAgileCfb)]
    [InlineData("Northwind", AccessEncryptionFormat.AccdbStandard)]
    public async Task DecryptAsync_RestoresAccessUnencryptedHeader(string source, AccessEncryptionFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await this.OpenSourceAsync(source, ct);
        byte[] original = ms.ToArray();
        DatabaseFormat databaseFormat = EncryptionConverter.DetectFormat(original);
        IReadOnlyList<string> originalTables = await ListTablesAsync(ms, password: null);

        ms.Position = 0;
        await AccessWriter.EncryptAsync(ms, FirstPasswordMemory, format, ct);
        ms.Position = 0;
        await AccessWriter.DecryptAsync(ms, FirstPasswordMemory, ct);

        byte[] decrypted = ms.ToArray();
        Assert.Equal(original.AsSpan(0, Constants.PageSizes.Jet4).ToArray(), decrypted.AsSpan(0, Constants.PageSizes.Jet4).ToArray());
        Assert.False(EncryptionManager.HasHeaderPassword(decrypted, databaseFormat));
        Assert.Equal(AccessEncryptionFormat.None, await AccessWriter.DetectEncryptionFormatAsync(ms, ct));
        Assert.Equal(originalTables, await ListTablesAsync(ms, password: null));
    }

    // ───── Replacing the file ────────────────────────────────────────

    /// <summary>
    /// The path overloads write the re-encrypted file to a temp file beside
    /// the database and swap it in with <see cref="File.Replace(string, string, string?)"/>.
    /// A handle opened without <see cref="FileShare.Delete"/> blocks that,
    /// and the old fallback deleted the database before moving the temp
    /// file over it; here the delete failed too, so the call threw a bare
    /// sharing violation and left the temp file behind.
    /// </summary>
    [Fact]
    public async Task ChangePassword_WhenTargetHeldWithoutShareDelete_ThrowsAndLeavesOriginalAndNoTempFile()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Only Windows refuses to replace a file another handle holds without FileShare.Delete.");
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = this.CopyToNewDirectory(TestDatabases.EncryptedAccdbLegacyPassword);
        byte[] original = await File.ReadAllBytesAsync(path, ct);

        IOException ex;
        await using (var holder = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            ex = await Assert.ThrowsAsync<IOException>(async () =>
                await AccessWriter.ChangePasswordAsync(path, GoldenPasswordMemory, SecondPasswordMemory, NoLockOptions, ct));
        }

        Assert.Contains(path, ex.Message, StringComparison.Ordinal);
        Assert.Contains("FileShare.Delete", ex.Message, StringComparison.Ordinal);
        Assert.Equal(original, await File.ReadAllBytesAsync(path, ct));
        Assert.Equal([path], Directory.GetFiles(Path.GetDirectoryName(path)!));
        await AssertOpenableAsync(path, TestDatabases.EncryptedFixturePassword, ["T"]);
    }

    /// <summary>
    /// An <see cref="AccessReader"/> opened by path shares the file with
    /// <see cref="FileShare.ReadWrite"/> by default, without
    /// <see cref="FileShare.Delete"/>. A password change under it must
    /// either swap the file whole or refuse and leave it byte for byte as it
    /// was; Windows refuses. No temp file stays behind either way, and the
    /// change succeeds once the reader is closed.
    /// </summary>
    [Fact]
    public async Task ChangePassword_WithOpenReader_LeavesOriginalIntact()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = this.CopyToNewDirectory(TestDatabases.EncryptedAccdbLegacyPassword);
        byte[] original = await File.ReadAllBytesAsync(path, ct);

        Exception? failure;
        var readerOptions = new AccessReaderOptions { UseLockFile = false, Password = GoldenPasswordMemory };
        await using (AccessReader reader = await AccessReader.OpenAsync(path, readerOptions, ct))
        {
            failure = await Record.ExceptionAsync(async () =>
                await AccessWriter.ChangePasswordAsync(path, GoldenPasswordMemory, SecondPasswordMemory, NoLockOptions, ct));
            Assert.Equal(["T"], await reader.ListTablesAsync(ct));
        }

        Assert.Equal([path], Directory.GetFiles(Path.GetDirectoryName(path)!));
        if (failure is null)
        {
            await AssertWrongPasswordAsync(path, TestDatabases.EncryptedFixturePassword);
            await AssertOpenableAsync(path, SecondPassword, ["T"]);
            return;
        }

        Assert.IsType<IOException>(failure);
        Assert.Equal(original, await File.ReadAllBytesAsync(path, ct));

        await AccessWriter.ChangePasswordAsync(path, GoldenPasswordMemory, SecondPasswordMemory, NoLockOptions, ct);
        Assert.Equal([path], Directory.GetFiles(Path.GetDirectoryName(path)!));
        await AssertWrongPasswordAsync(path, TestDatabases.EncryptedFixturePassword);
        await AssertOpenableAsync(path, SecondPassword, ["T"]);
    }

    /// <summary>
    /// A successful password change leaves the database and nothing else in
    /// its directory, whatever the format, including the CFB containers
    /// whose length changes.
    /// </summary>
    /// <param name="fixture">The frozen encrypted fixture to start from.</param>
    [Theory]
    [InlineData(nameof(TestDatabases.EncryptedJet4Rc4))]
    [InlineData(nameof(TestDatabases.EncryptedAccdbLegacyPassword))]
    [InlineData(nameof(TestDatabases.EncryptedAccdbAgileCfb))]
    public async Task ChangePassword_Success_LeavesNoTempFile(string fixture)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = this.CopyToNewDirectory((string)typeof(TestDatabases).GetField(fixture)!.GetValue(null)!);

        await AccessWriter.ChangePasswordAsync(path, GoldenPasswordMemory, SecondPasswordMemory, NoLockOptions, ct);

        Assert.Equal([path], Directory.GetFiles(Path.GetDirectoryName(path)!));
        await AssertWrongPasswordAsync(path, TestDatabases.EncryptedFixturePassword);
        await AssertOpenableAsync(path, SecondPassword, ["T"]);
    }

    // ───── Cross-format re-encryption ────────────────────────────────

    [Fact]
    public async Task ReEncrypt_AccdbLegacyToAgile_PreservesData()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        IReadOnlyList<string> originalTables = await ListTablesAsync(path, password: null);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, FirstPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.EncryptAsync(path, SecondPasswordMemory, AccessEncryptionFormat.AccdbAgile, NoLockOptions, TestContext.Current.CancellationToken);

        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await AssertOpenableAsync(path, SecondPassword, originalTables);
    }

    // ───── Negative cases ────────────────────────────────────────────

    [Fact]
    public async Task EncryptAsync_OnAlreadyEncryptedFile_Throws()
    {
        string path = await this.CloneAsync(TestDatabases.AdventureWorks, ".mdb");
        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.EncryptAsync(path, SecondPasswordMemory, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DecryptAsync_OnUnencryptedFile_Throws()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.DecryptAsync(path, FirstPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangePasswordAsync_OnUnencryptedFile_Throws()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangePasswordAsync_WithWrongOldPassword_Throws()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");
        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await AccessWriter.ChangePasswordAsync(path, "totally-wrong".AsMemory(), SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(AccessEncryptionFormat.Jet4Rc4, FirstPassword + "x")]
    [InlineData(AccessEncryptionFormat.Jet4Rc4, FirstPassword + "\0")]
    [InlineData(AccessEncryptionFormat.AccdbLegacyPassword, FirstPassword + "x")]
    [InlineData(AccessEncryptionFormat.AccdbLegacyPassword, FirstPassword + "\0")]
    [InlineData(AccessEncryptionFormat.AccdbAesCfbWrapped, FirstPassword + "x")]
    [InlineData(AccessEncryptionFormat.AccdbAesCfbWrapped, FirstPassword + "\0")]
    public async Task HeaderPasswordFormats_OpenWithWrongPassword_ThrowsUnauthorizedAccessException(
        AccessEncryptionFormat format,
        string wrongPassword)
    {
        string sourcePath = format == AccessEncryptionFormat.Jet4Rc4
            ? TestDatabases.AdventureWorks
            : TestDatabases.NorthwindTraders;
        string extension = format == AccessEncryptionFormat.Jet4Rc4 ? ".mdb" : ".accdb";
        string path = await this.CloneAsync(sourcePath, extension);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, format, NoLockOptions, TestContext.Current.CancellationToken);

        await AssertWrongPasswordAsync(path, wrongPassword);
    }

    [Fact]
    public async Task EncryptAsync_Jet4Rc4_OnAccdbFile_ThrowsNotSupported()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EncryptAsync_Agile_OnMdbFile_ThrowsNotSupported()
    {
        string path = await this.CloneAsync(TestDatabases.AdventureWorks, ".mdb");

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbAgile, NoLockOptions, TestContext.Current.CancellationToken));
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
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
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

        await AccessWriter.EncryptAsync(ms, FirstPasswordMemory, cancellationToken: TestContext.Current.CancellationToken);

        ms.Position = 0;
        await AccessWriter.DecryptAsync(ms, FirstPasswordMemory, TestContext.Current.CancellationToken);

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
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
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        Dictionary<string, IReadOnlyList<ColumnMetadata>> originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbAgile, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);

        Dictionary<string, IReadOnlyList<ColumnMetadata>> afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_Jet4Rc4_PreservesColumnMetadata()
    {
        string path = await this.CloneAsync(TestDatabases.AdventureWorks, ".mdb");

        Dictionary<string, IReadOnlyList<ColumnMetadata>> originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.Jet4Rc4, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);

        Dictionary<string, IReadOnlyList<ColumnMetadata>> afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_AccdbLegacyPassword_PreservesColumnMetadata()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        Dictionary<string, IReadOnlyList<ColumnMetadata>> originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbLegacyPassword, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);

        Dictionary<string, IReadOnlyList<ColumnMetadata>> afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_AccdbAesCfbWrapped_PreservesColumnMetadata()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        Dictionary<string, IReadOnlyList<ColumnMetadata>> originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbAesCfbWrapped, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);

        Dictionary<string, IReadOnlyList<ColumnMetadata>> afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    [Fact]
    public async Task EncryptDecrypt_AccdbStandard_PreservesColumnMetadata()
    {
        string path = await this.CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        Dictionary<string, IReadOnlyList<ColumnMetadata>> originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        await AccessWriter.EncryptAsync(path, FirstPasswordMemory, AccessEncryptionFormat.AccdbStandard, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, FirstPasswordMemory, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, SecondPasswordMemory, NoLockOptions, TestContext.Current.CancellationToken);

        Dictionary<string, IReadOnlyList<ColumnMetadata>> afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        AssertColumnMetadataEqual(originalMeta, afterMeta);
    }

    // ───── Helpers ───────────────────────────────────────────────────

    private static void AssertColumnMetadataEqual(
        Dictionary<string, IReadOnlyList<ColumnMetadata>> expected,
        Dictionary<string, IReadOnlyList<ColumnMetadata>> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (KeyValuePair<string, IReadOnlyList<ColumnMetadata>> entry in expected)
        {
            string table = entry.Key;
            IReadOnlyList<ColumnMetadata> columns = entry.Value;
            Assert.True(actual.ContainsKey(table), $"Table '{table}' missing after password change.");
            IReadOnlyList<ColumnMetadata> afterCols = actual[table];
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

    private static async Task<Dictionary<string, IReadOnlyList<ColumnMetadata>>> GetAllColumnMetadataAsync(string path, string? password)
    {
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password.AsMemory(),
        };
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        var result = new Dictionary<string, IReadOnlyList<ColumnMetadata>>(StringComparer.Ordinal);
        foreach (string table in tables)
        {
            result[table] = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
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
        Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(stream, cancellationToken);

        Assert.True(streams.ContainsKey("EncryptionInfo"));
        Assert.True(streams.ContainsKey("EncryptedPackage"));
        Assert.True(OfficeCryptoAgile.IsAgileEncryptionInfo(streams["EncryptionInfo"]));
    }

    private static async Task<IReadOnlyList<string>> ListTablesAsync(string path, string? password)
    {
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password.AsMemory(),
        };
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        return await reader.ListTablesAsync(TestContext.Current.CancellationToken);
    }

    private static async Task AssertOpenableAsync(string path, string? password, IReadOnlyList<string> expectedTables)
    {
        IReadOnlyList<string> tables = await ListTablesAsync(path, password);
        Assert.NotEmpty(tables);
        Assert.Equal(expectedTables.Count, tables.Count);

        // Sanity check: read at least one row from the first user table (if any).
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password.AsMemory(),
        };
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
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

    private static async Task<IReadOnlyList<string>> ListTablesAsync(MemoryStream stream, string? password)
    {
        stream.Position = 0;
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = password.AsMemory(),
        };
        await using AccessReader reader = await AccessReader.OpenAsync(stream, options, leaveOpen: true, TestContext.Current.CancellationToken);
        return await reader.ListTablesAsync(TestContext.Current.CancellationToken);
    }

    private async Task<MemoryStream> OpenSourceAsync(string source, CancellationToken cancellationToken)
    {
        if (source is "WriterJet4" or "WriterAce")
        {
            var created = new MemoryStream();
            DatabaseFormat format = source == "WriterJet4" ? DatabaseFormat.Jet4Mdb : DatabaseFormat.AceAccdb;
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(created, format, NoLockOptions, leaveOpen: true, cancellationToken))
            {
                await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], cancellationToken);
                await writer.InsertRowAsync("T", [1], cancellationToken);
            }

            return created;
        }

        return await db.CopyToStreamAsync(
            source == "AdventureWorks" ? TestDatabases.AdventureWorks : TestDatabases.NorthwindTraders,
            cancellationToken);
    }

    /// <summary>Copies <paramref name="sourcePath"/> into a new, otherwise empty temp directory, so a test can see every file a call leaves there.</summary>
    /// <param name="sourcePath">The file to copy.</param>
    /// <returns>The copy's path.</returns>
    private string CopyToNewDirectory(string sourcePath)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"jdwenc_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(directory);
        this.tempDirectories.Add(directory);
        string path = Path.Combine(directory, Path.GetFileName(sourcePath));
        File.Copy(sourcePath, path);
        return path;
    }

    private async Task<string> CloneAsync(string sourcePath, string ext)
    {
        byte[] bytes = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        string temp = Path.Combine(Path.GetTempPath(), $"jdwenc_{Guid.NewGuid():N}{ext}");
        await File.WriteAllBytesAsync(temp, bytes, TestContext.Current.CancellationToken);
        this.tempFiles.Add(temp);
        return temp;
    }
}
