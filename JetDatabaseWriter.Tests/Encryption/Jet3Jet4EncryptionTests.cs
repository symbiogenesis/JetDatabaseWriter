namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for database encryption across all Jet/ACE versions:
///   1. Jet3 XOR mask  — fixed XOR pattern applied to all pages after page 0
///   2. Jet4 RC4 flag  — password verified against the XOR-encoded header hash (0x42)
///   3. Jet4 RC4 pages — RC4 page decryption.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class Jet3Jet4EncryptionTests(DatabaseCache db) : IClassFixture<DatabaseCache>, IDisposable
{
    private readonly List<string> tempFiles = [];

    // ═══════════════════════════════════════════════════════════════════
    // 1. JET3 XOR MASK
    // ═══════════════════════════════════════════════════════════════════
    //
    // Jet3 encryption uses a fixed 128-byte XOR mask applied cyclically
    // to every page after page 0. The reader detects the flag and removes
    // the mask transparently on open.

    [Fact]
    public async Task Encryption_Jet3Xor_DatabaseIsReadable()
    {
        // Jet3 encryption uses a simple XOR mask applied to every page.
        // Verify the reader detects and transparently decrypts XOR-masked databases.
        byte[] data = await this.CloneFileAsync(TestDatabases.Jet3Test);
        ApplyXorMask(data, EncryptionManager.Jet3PageXorMask);
        SetJet3EncryptionFlag(data);

        await using MemoryStream ms = ToStream(data);
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. JET4 RC4 PASSWORD FLAG
    // ═══════════════════════════════════════════════════════════════════
    //
    // The password is verified against the stored XOR-encoded hash in the
    // header at offset 0x42. Flag 0x01 at offset 0x62 means password-only;
    // flag 0x02 means full RC4 page encryption (covered in section 3).

    [Fact]
    public async Task Encryption_Jet4Rc4_WithCorrectPassword_DatabaseIsReadable()
    {
        // Verify that a Jet4 database with the password flag set is readable
        // when the correct password is provided.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);

        var options = new AccessReaderOptions
        {
            Password = "test".AsMemory(),
        };

        await using MemoryStream ms = ToStream(data);
        await using AccessReader reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WithoutPassword_ThrowsDescriptiveError()
    {
        // Opening an encrypted database without a password throws with a
        // message that hints at providing a password.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);

        await using MemoryStream ms = ToStream(data);
        UnauthorizedAccessException ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WithWrongPassword_ThrowsMeaningfulError()
    {
        // An incorrect password produces a clear error rather than corrupt data.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);

        var options = new AccessReaderOptions
        {
            Password = "wrong_password".AsMemory(),
        };

        await using MemoryStream ms = ToStream(data);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
            await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        });

        Assert.NotNull(ex);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WriterWithoutPassword_ThrowsDescriptiveError()
    {
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);
        string temp = this.WriteTempBytes(data, ".mdb");

        UnauthorizedAccessException ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await AccessWriter.OpenAsync(temp, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WriterWithWrongPassword_ThrowsMeaningfulError()
    {
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);
        string temp = this.WriteTempBytes(data, ".mdb");

        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            Password = "wrong_password".AsMemory(),
        };

        UnauthorizedAccessException ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken));
        Assert.Contains("incorrect", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WriterWithCorrectPassword_Opens()
    {
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);
        string temp = this.WriteTempBytes(data, ".mdb");

        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            Password = "test".AsMemory(),
        };

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
        Assert.NotNull(writer);
    }

    // ───── Write-back encryption (re-encrypt pages on flush) ─────────

    [Fact]
    public async Task Encryption_Jet4Rc4_WriterRoundTrip_InsertedRowReadsBackThroughRc4()
    {
        // After full RC4 page encryption is applied to a Jet4 file, opening
        // it with AccessWriter and inserting a row must re-encrypt the
        // mutated pages on flush. Re-opening with AccessReader (RC4 path)
        // must surface the new row as plaintext.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");
        string temp = this.WriteTempBytes(data, ".mdb");

        var writerOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            Password = "test".AsMemory(),
        };

        const string tableName = "JetWriteEncTest";
        await using (AccessWriter writer = await AccessWriter.OpenAsync(temp, writerOptions, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Label", typeof(string), maxLength: 64),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                tableName,
                [42, "encrypted-write"],
                TestContext.Current.CancellationToken);
        }

        var readerOptions = new AccessReaderOptions { Password = "test".AsMemory() };
        await using AccessReader reader = await AccessReader.OpenAsync(temp, readerOptions, TestContext.Current.CancellationToken);
        DataTable dt = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        DataRow row = Assert.Single(dt.Rows.Cast<DataRow>());
        Assert.Equal(42, row["Id"]);
        Assert.Equal("encrypted-write", row["Label"]);
    }

    [Fact]
    public async Task Encryption_Jet3Xor_WriterRoundTrip_InsertedRowReadsBackThroughXor()
    {
        // The Jet3 XOR mask is symmetric — write-back must re-mask each page
        // so a fresh reader can decrypt it.
        byte[] data = await this.CloneFileAsync(TestDatabases.Jet3Test);
        ApplyXorMask(data, EncryptionManager.Jet3PageXorMask);
        SetJet3EncryptionFlag(data);
        string temp = this.WriteTempBytes(data, ".mdb");

        var writerOptions = new AccessWriterOptions { UseLockFile = false };

        const string tableName = "Jet3WriteEncTest";
        await using (AccessWriter writer = await AccessWriter.OpenAsync(temp, writerOptions, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Label", typeof(string), maxLength: 64),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                tableName,
                [7, "jet3-xor-write"],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);
        DataTable dt = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        DataRow row = Assert.Single(dt.Rows.Cast<DataRow>());
        Assert.Equal(7, row["Id"]);
        Assert.Equal("jet3-xor-write", row["Label"]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. JET4 RC4 PAGE DECRYPTION
    // ═══════════════════════════════════════════════════════════════════
    //
    // The reader derives the RC4 key from the database key (header offset 0x3E)
    // and decrypts each page (except page 0) with a page-number-derived key.
    // These tests encrypt a known database in memory and verify the reader
    // returns the original rows when the correct password is supplied.

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_ReadTable_ReturnsDecryptedRows()
    {
        // A Jet4 database with RC4 encryption set and password provided
        // should return actual row data, not garbled bytes.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using MemoryStream ms = ToStream(data);
        await using AccessReader reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable dt = await reader.ReadDataTableAsync("Product", cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "RC4-decrypted table should contain rows");
    }

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_StreamRows_ReturnsDecryptedRows()
    {
        // Streaming should also work through RC4-encrypted pages.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using MemoryStream ms = ToStream(data);
        await using AccessReader reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int rowCount = await reader.Rows(tables[0], cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        Assert.True(rowCount > 0, "RC4-decrypted stream should yield rows");
    }

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_GetStatistics_ReturnsValidStats()
    {
        // Statistics (catalog scan, row counts) should work on encrypted databases.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using MemoryStream ms = ToStream(data);
        await using AccessReader reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TableCount > 0, "Should report tables in encrypted database");
        Assert.True(stats.TotalRows > 0, "Should report rows in encrypted database");
    }

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_ColumnMetadata_IsCorrect()
    {
        // Column metadata from TDEF pages must be decrypted correctly.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using MemoryStream ms = ToStream(data);
        await using AccessReader reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tables[0], TestContext.Current.CancellationToken);
        Assert.NotEmpty(meta);
        Assert.All(meta, col =>
        {
            Assert.False(string.IsNullOrEmpty(col.Name), "Column name should be readable after decryption");
            Assert.NotEqual(typeof(object), col.ClrType);
        });
    }

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_ReadAllTables_Succeeds()
    {
        // Bulk read of all tables should succeed on an RC4-encrypted database.
        byte[] data = await this.CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using MemoryStream ms = ToStream(data);
        await using AccessReader reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        IReadOnlyDictionary<string, DataTable> all = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotEmpty(all);
        Assert.All(all.Values, Assert.NotNull);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

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
                /* best-effort cleanup */
            }
        }
    }

    /// <summary>XOR-masks a database byte array starting at page 1 (page 0 is the header).</summary>
    /// <param name="data">The data bytes or values.</param>
    /// <param name="mask">The encryption mask or page bitmask.</param>
    internal static void ApplyXorMask(byte[] data, byte[] mask)
    {
        // Apply mask starting from page 1 (offset 2048) through the data
        for (int offset = Constants.PageSizes.Jet3; offset < data.Length; offset++)
        {
            data[offset] ^= mask[(offset - Constants.PageSizes.Jet3) % mask.Length];
        }
    }

    /// <summary>Sets the Office97 password flag (0x01) in a Jet3 database header.</summary>
    /// <param name="data">The data bytes or values.</param>
    internal static void SetJet3EncryptionFlag(byte[] data) => data[0x62] = 0x01; // Office97 password flag

    /// <summary>
    /// Encodes the native date-masked password <c>"test"</c>
    /// in a Jet4 database header. Data pages are <em>not</em> RC4-encrypted —
    /// use <see cref="Rc4EncryptDataPages"/> to also encrypt the page data.
    /// </summary>
    /// <param name="data">The data bytes or values.</param>
    private static void SetJet4PasswordFlag(byte[] data)
        => EncryptionManager.WriteNativeJet4EncryptionHeader(data, 0, "test");

    /// <summary>Encrypts data pages with the native Jet encoding key.</summary>
    /// <param name="data">The database bytes.</param>
    /// <param name="password">The database password.</param>
    private static void Rc4EncryptDataPages(byte[] data, string password)
    {
        const uint dbKey = 0x12345678;
        EncryptionManager.WriteNativeJet4EncryptionHeader(data, dbKey, password);
        const int pageSize = Constants.PageSizes.Jet4;
        for (int page = 1; page * pageSize < data.Length; page++)
        {
            byte[] key = new byte[sizeof(uint)];
            BinaryPrimitives.WriteUInt32LittleEndian(key, dbKey ^ (uint)page);
            Rc4Transform(data, page * pageSize, Math.Min(pageSize, data.Length - (page * pageSize)), key);
        }
    }
    /// <summary>In-place RC4 transform (encrypt/decrypt are the same operation).</summary>
    /// <param name="data">The data bytes or values.</param>
    /// <param name="offset">The offset.</param>
    /// <param name="length">The length.</param>
    /// <param name="key">The key bytes or index key.</param>
    private static void Rc4Transform(byte[] data, int offset, int length, byte[] key)
    {
        // RC4 key scheduling
        byte[] s = new byte[256];
        for (int i = 0; i < 256; i++)
        {
            s[i] = (byte)i;
        }

        int j = 0;
        for (int i = 0; i < 256; i++)
        {
            j = (j + s[i] + key[i % key.Length]) & 0xFF;
            (s[i], s[j]) = (s[j], s[i]);
        }

        // RC4 cipher
        int x = 0, y = 0;
        for (int k = 0; k < length; k++)
        {
            x = (x + 1) & 0xFF;
            y = (y + s[x]) & 0xFF;
            (s[x], s[y]) = (s[y], s[x]);
            data[offset + k] ^= s[(s[x] + s[y]) & 0xFF];
        }
    }

    private static MemoryStream ToStream(byte[] data)
    {
        var ms = new MemoryStream();
        ms.Write(data, 0, data.Length);
        ms.Position = 0;
        return ms;
    }

    private async Task<byte[]> CloneFileAsync(string sourcePath)
    {
        byte[] cached = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        return (byte[])cached.Clone();
    }

    private string WriteTempBytes(byte[] data, string extension)
    {
        string temp = Path.Combine(Path.GetTempPath(), $"JetEncTest_{Guid.NewGuid():N}{extension}");
        File.WriteAllBytes(temp, data);
        this.tempFiles.Add(temp);
        return temp;
    }
}
