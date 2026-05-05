namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for encryption coverage gaps from §3 of test-coverage-gaps.md:
///   1. Office 2007 (ECMA-376) Standard AES-128 encryption detection.
///   2. LvProp / MSysDb properties survival after a password change.
/// </summary>
public sealed class EncryptionCoverageGapTests(DatabaseCache db) : IClassFixture<DatabaseCache>, IDisposable
{
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

    // ═══════════════════════════════════════════════════════════════════
    // 1. OFFICE 2007 (ECMA-376) STANDARD AES-128 ENCRYPTION DETECTION
    // ═══════════════════════════════════════════════════════════════════
    //
    // The reader must detect standard-encrypted CFB files (EncryptionInfo
    // version 3.2 or 4.2) and throw a clear NotSupportedException rather
    // than silently failing with a cryptic JET parse error.

    [Theory]
    [InlineData(3, 2)]
    [InlineData(4, 2)]
    public async Task StandardEncryption_DetectedWithClearError(ushort major, ushort minor)
    {
        byte[] cfb = BuildStandardEncryptedCfb(major, minor);

        await using var ms = new MemoryStream(cfb, writable: false);
        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("Standard encryption", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(3, 2)]
    [InlineData(4, 2)]
    public async Task StandardEncryption_WithPassword_StillThrowsNotSupported(ushort major, ushort minor)
    {
        byte[] cfb = BuildStandardEncryptedCfb(major, minor);
        var options = new AccessReaderOptions { Password = "test".AsMemory() };

        await using var ms = new MemoryStream(cfb, writable: false);
        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken));

        Assert.Contains("Standard encryption", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void IsStandardEncryptionInfo_Version3_2_ReturnsTrue()
    {
        byte[] info = new byte[16];
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(0), 3); // major
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(2), 2); // minor
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(4), 0x24); // flags (standard)

        Assert.True(OfficeCryptoAgile.IsStandardEncryptionInfo(info));
    }

    [Fact]
    public void IsStandardEncryptionInfo_Version4_2_ReturnsTrue()
    {
        byte[] info = new byte[16];
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(0), 4); // major
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(2), 2); // minor
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(4), 0x24); // flags (standard)

        Assert.True(OfficeCryptoAgile.IsStandardEncryptionInfo(info));
    }

    [Fact]
    public void IsStandardEncryptionInfo_AgileVersion4_4_ReturnsFalse()
    {
        byte[] info = new byte[16];
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(0), 4); // major
        BinaryPrimitives.WriteUInt16LittleEndian(info.AsSpan(2), 4); // minor
        BinaryPrimitives.WriteUInt32LittleEndian(info.AsSpan(4), 0x40); // AgileEncryption flag

        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo(info));
    }

    [Fact]
    public void IsStandardEncryptionInfo_NullOrShort_ReturnsFalse()
    {
        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo(null!));
        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo([]));
        Assert.False(OfficeCryptoAgile.IsStandardEncryptionInfo(new byte[4]));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. LVPROP / MSYSDB PROPERTIES ROUND-TRIP AFTER PASSWORD CHANGE
    // ═══════════════════════════════════════════════════════════════════
    //
    // Verifies that MSysObjects.LvProp (column property metadata) survives
    // Encrypt → ChangePassword → Decrypt operations. The concern is that
    // re-encryption might corrupt internal metadata blocks that contain
    // column-level property information (expression definitions, format
    // strings, etc.).

    [Fact]
    public async Task LvProp_SurvivesAgilePasswordChange()
    {
        string path = await CloneAsync(TestDatabases.NorthwindTraders, ".accdb");

        // Capture original column metadata (depends on LvProp for description/format).
        var originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        // Encrypt with Agile.
        const string Password1 = "first_pass!";
        const string Password2 = "rotated_2nd";
        await AccessWriter.EncryptAsync(path, Password1, AccessEncryptionFormat.AccdbAgile, NoLockOptions(), TestContext.Current.CancellationToken);

        // Change password.
        await AccessWriter.ChangePasswordAsync(path, Password1, Password2, NoLockOptions(), TestContext.Current.CancellationToken);

        // Decrypt.
        await AccessWriter.DecryptAsync(path, Password2, NoLockOptions(), TestContext.Current.CancellationToken);

        // Verify column metadata still matches (LvProp survived the round-trip).
        var afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.Equal(originalMeta.Count, afterMeta.Count);
        foreach (var (table, columns) in originalMeta)
        {
            Assert.True(afterMeta.ContainsKey(table), $"Table '{table}' missing after password change.");
            var afterCols = afterMeta[table];
            Assert.Equal(columns.Count, afterCols.Count);
            for (int i = 0; i < columns.Count; i++)
            {
                Assert.Equal(columns[i].Name, afterCols[i].Name);
                Assert.Equal(columns[i].ClrType, afterCols[i].ClrType);
            }
        }
    }

    [Fact]
    public async Task LvProp_SurvivesJet4Rc4PasswordChange()
    {
        string path = await CloneAsync(TestDatabases.AdventureWorks, ".mdb");

        var originalMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.NotEmpty(originalMeta);

        const string Password1 = "first!";
        const string Password2 = "second!";
        await AccessWriter.EncryptAsync(path, Password1, AccessEncryptionFormat.Jet4Rc4, NoLockOptions(), TestContext.Current.CancellationToken);
        await AccessWriter.ChangePasswordAsync(path, Password1, Password2, NoLockOptions(), TestContext.Current.CancellationToken);
        await AccessWriter.DecryptAsync(path, Password2, NoLockOptions(), TestContext.Current.CancellationToken);

        var afterMeta = await GetAllColumnMetadataAsync(path, password: null);
        Assert.Equal(originalMeta.Count, afterMeta.Count);
        foreach (var (table, columns) in originalMeta)
        {
            Assert.True(afterMeta.ContainsKey(table), $"Table '{table}' missing after password change.");
            var afterCols = afterMeta[table];
            Assert.Equal(columns.Count, afterCols.Count);
            for (int i = 0; i < columns.Count; i++)
            {
                Assert.Equal(columns[i].Name, afterCols[i].Name);
                Assert.Equal(columns[i].ClrType, afterCols[i].ClrType);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    private static AccessWriterOptions NoLockOptions() => new() { UseLockFile = false };

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

    /// <summary>
    /// Builds a synthetic CFB compound document whose EncryptionInfo stream
    /// advertises ECMA-376 Standard encryption (version 3.2 or 4.2). The file
    /// is not genuinely encrypted — it only needs to trigger detection logic.
    /// </summary>
    private static byte[] BuildStandardEncryptedCfb(ushort major, ushort minor)
    {
        // Build EncryptionInfo with standard version header.
        byte[] encryptionInfo = new byte[64];
        BinaryPrimitives.WriteUInt16LittleEndian(encryptionInfo.AsSpan(0), major);
        BinaryPrimitives.WriteUInt16LittleEndian(encryptionInfo.AsSpan(2), minor);
        BinaryPrimitives.WriteUInt32LittleEndian(encryptionInfo.AsSpan(4), 0x24); // fCryptoAPI | fAES

        // Minimal EncryptedPackage placeholder (just needs to exist).
        byte[] encryptedPackage = new byte[128];

        // Wrap into a CFB using the production writer.
        return CompoundFileWriter.Build(
        [
            new("EncryptionInfo", encryptionInfo),
            new("EncryptedPackage", encryptedPackage),
        ]);
    }

    private async Task<string> CloneAsync(string sourcePath, string ext)
    {
        byte[] bytes = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        string temp = Path.Combine(Path.GetTempPath(), $"jdwgap_{Guid.NewGuid():N}{ext}");
        await File.WriteAllBytesAsync(temp, bytes, TestContext.Current.CancellationToken);
        _tempFiles.Add(temp);
        return temp;
    }
}
