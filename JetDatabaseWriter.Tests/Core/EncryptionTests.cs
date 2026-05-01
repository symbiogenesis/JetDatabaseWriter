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

#pragma warning disable CA5358 // ECB mode is intentional for deterministic test fixture encryption
#pragma warning disable CA5390 // Hard-coded key is intentional for deterministic test fixtures

/// <summary>
/// Tests for database encryption across all Jet/ACE versions:
///   1. Jet3 XOR mask  — fixed XOR pattern applied to all pages after page 0
///   2. Jet4 RC4 flag  — password verified against the XOR-encoded header hash (0x42)
///   3. Jet4 RC4 pages — RC4 page decryption
///   4. ACCDB AES      — detection and page decryption (OLE2 CFB magic)
///   5. ACCDB AES      — genuine AesEncrypted.accdb fixture from Access 16 CompactDatabase.
/// </summary>
public sealed class EncryptionTests(DatabaseCache db) : IClassFixture<DatabaseCache>, IDisposable
{
    private readonly List<string> _tempFiles = [];

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
        byte[] data = await CloneFileAsync(TestDatabases.Jet3Test);
        byte[] xorMask = BuildJet3XorMask();
        ApplyXorMask(data, xorMask);
        SetJet3EncryptionFlag(data);

        await using var ms = ToStream(data);
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

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
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);

        var options = new AccessReaderOptions
        {
            Password = "test".AsMemory(),
        };

        await using var ms = ToStream(data);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WithoutPassword_ThrowsDescriptiveError()
    {
        // Opening an encrypted database without a password throws with a
        // message that hints at providing a password.
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);

        await using var ms = ToStream(data);
        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WithWrongPassword_ThrowsMeaningfulError()
    {
        // An incorrect password produces a clear error rather than corrupt data.
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);

        var options = new AccessReaderOptions
        {
            Password = "wrong_password".AsMemory(),
        };

        await using var ms = ToStream(data);
        var ex = await Record.ExceptionAsync(async () =>
        {
            await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
            await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        });

        Assert.NotNull(ex);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WriterWithoutPassword_ThrowsDescriptiveError()
    {
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);
        string temp = WriteTempBytes(data, ".mdb");

        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await AccessWriter.OpenAsync(temp, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WriterWithWrongPassword_ThrowsMeaningfulError()
    {
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);
        string temp = WriteTempBytes(data, ".mdb");

        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            Password = "wrong_password".AsMemory(),
        };

        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken));
        Assert.Contains("incorrect", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Encryption_Jet4Rc4_WriterWithCorrectPassword_Opens()
    {
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(data);
        string temp = WriteTempBytes(data, ".mdb");

        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            Password = "test".AsMemory(),
        };

        await using var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
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
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");
        string temp = WriteTempBytes(data, ".mdb");

        var writerOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            Password = "test".AsMemory(),
        };

        const string TableName = "JetWriteEncTest";
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
                [42, "encrypted-write"],
                TestContext.Current.CancellationToken);
        }

        var readerOptions = new AccessReaderOptions { Password = "test".AsMemory() };
        await using var reader = await AccessReader.OpenAsync(temp, readerOptions, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.Single(dt.Rows);
        Assert.Equal(42, dt.Rows[0]["Id"]);
        Assert.Equal("encrypted-write", dt.Rows[0]["Label"]);
    }

    [Fact]
    public async Task Encryption_Jet3Xor_WriterRoundTrip_InsertedRowReadsBackThroughXor()
    {
        // The Jet3 XOR mask is symmetric — write-back must re-mask each page
        // so a fresh reader can decrypt it.
        byte[] data = await CloneFileAsync(TestDatabases.Jet3Test);
        ApplyXorMask(data, BuildJet3XorMask());
        SetJet3EncryptionFlag(data);
        string temp = WriteTempBytes(data, ".mdb");

        var writerOptions = new AccessWriterOptions { UseLockFile = false };

        const string TableName = "Jet3WriteEncTest";
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
                [7, "jet3-xor-write"],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.Single(dt.Rows);
        Assert.Equal(7, dt.Rows[0]["Id"]);
        Assert.Equal("jet3-xor-write", dt.Rows[0]["Label"]);
    }

    [Fact]
    public async Task Encryption_AccdbCfbWrapped_LegacyAes_Writer_RoundTripsRow()
    {
        // Synthetic legacy AES-128 CFB-wrapped .accdb files (CFB magic at byte 0
        // but flat per-page AES-128-ECB beneath) are now writable in place: the
        // existing PrepareEncryptedPageForWrite pipeline re-encrypts every page
        // we flush. Verify a round-trip by inserting a row and reading it back.
        const string TableName = "AesWriteRoundTrip";

        byte[] data = await CloneFileAsync(TestDatabases.NorthwindTraders);
        SetAccdbEncryptionHeader(data);
        string temp = WriteTempBytes(data, ".accdb");

        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };

        await using (var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken))
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
                [11, "legacy-aes-cfb-write"],
                TestContext.Current.CancellationToken);
        }

        var readerOptions = new AccessReaderOptions
        {
            UseLockFile = false,
            Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        };
        await using var reader = await AccessReader.OpenAsync(temp, readerOptions, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.Single(dt.Rows);
        Assert.Equal(11, dt.Rows[0]["Id"]);
        Assert.Equal("legacy-aes-cfb-write", dt.Rows[0]["Label"]);
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
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using var ms = ToStream(data);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable dt = (await reader.ReadDataTableAsync("Product", cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "RC4-decrypted table should contain rows");
    }

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_StreamRows_ReturnsDecryptedRows()
    {
        // Streaming should also work through RC4-encrypted pages.
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using var ms = ToStream(data);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int rowCount = await reader.Rows(tables[0], cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        Assert.True(rowCount > 0, "RC4-decrypted stream should yield rows");
    }

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_GetStatistics_ReturnsValidStats()
    {
        // Statistics (catalog scan, row counts) should work on encrypted databases.
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using var ms = ToStream(data);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);
        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);

        Assert.True(stats.TableCount > 0, "Should report tables in encrypted database");
        Assert.True(stats.TotalRows > 0, "Should report rows in encrypted database");
    }

    [Fact]
    public async Task Rc4Decryption_EncryptedJet4_ColumnMetadata_IsCorrect()
    {
        // Column metadata from TDEF pages must be decrypted correctly.
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using var ms = ToStream(data);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tables[0], TestContext.Current.CancellationToken);
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
        byte[] data = await CloneFileAsync(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(data, "test");

        var options = new AccessReaderOptions { Password = "test".AsMemory() };
        await using var ms = ToStream(data);
        await using var reader = await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken);

        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotEmpty(all);
        Assert.All(all.Values, dt => Assert.NotNull(dt));
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

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
                /* best-effort cleanup */
            }
        }
    }

    /// <summary>
    /// Builds the Jet3 XOR mask (128 bytes, applied cyclically across each 2048-byte page).
    /// This is the well-known fixed mask from the mdbtools specification.
    /// </summary>
    private static byte[] BuildJet3XorMask()
    {
        // The Jet3 encryption mask is a fixed 128-byte pattern applied cyclically.
        // Sourced from mdbtools HACKING.md.
        return
        [
            0xEC, 0x7B, 0x28, 0x07, 0x77, 0x26, 0x13, 0x82,
            0x75, 0x4E, 0x22, 0x04, 0x42, 0xCE, 0xB3, 0x19,
            0xA1, 0x32, 0x75, 0x46, 0xE3, 0x66, 0x27, 0x37,
            0x19, 0x9E, 0xA3, 0x56, 0x85, 0x3A, 0xD6, 0xDE,
            0xEC, 0x03, 0xE6, 0xFC, 0xF8, 0x85, 0x8F, 0xA0,
            0x1B, 0x20, 0xAD, 0xE5, 0x0E, 0x7A, 0xF7, 0x38,
            0x54, 0xFC, 0x10, 0x4E, 0x25, 0x22, 0xBD, 0xC7,
            0x5D, 0x62, 0x5E, 0x44, 0xBB, 0x6D, 0xCB, 0xB5,
            0x90, 0x14, 0xDE, 0xC5, 0xD7, 0xA5, 0x4F, 0x84,
            0xBE, 0xE5, 0x06, 0x62, 0xC5, 0xF1, 0xBB, 0xBB,
            0xE3, 0xBB, 0x4C, 0xFD, 0x38, 0x7B, 0xDA, 0x88,
            0x1F, 0x5C, 0x2E, 0x5A, 0x49, 0xEB, 0x47, 0xE2,
            0xCA, 0xAD, 0xCE, 0x73, 0xBB, 0x25, 0xF9, 0xED,
            0x47, 0x59, 0x4C, 0x42, 0xEF, 0xF0, 0xB1, 0x58,
            0x45, 0x58, 0x5D, 0xF3, 0xBC, 0x27, 0xBC, 0x60,
            0x19, 0xEB, 0xB1, 0xF9, 0x4F, 0x5D, 0xD1, 0x12,
        ];
    }

    /// <summary>XOR-masks a database byte array starting at page 1 (page 0 is the header).</summary>
    private static void ApplyXorMask(byte[] data, byte[] mask)
    {
        const int jet3PageSize = Constants.PageSizes.Jet3;

        // Apply mask starting from page 1 (offset 2048) through the data
        for (int offset = jet3PageSize; offset < data.Length; offset++)
        {
            data[offset] ^= mask[(offset - jet3PageSize) % mask.Length];
        }
    }

    /// <summary>Sets the Office97 password flag (0x01) in a Jet3 database header.</summary>
    private static void SetJet3EncryptionFlag(byte[] data)
    {
        data[0x62] = 0x01; // Office97 password flag
    }

    /// <summary>
    /// Sets the Office97 password flag (0x01) and encodes password <c>"test"</c>
    /// in a Jet4 database header. Data pages are <em>not</em> RC4-encrypted —
    /// use <see cref="Rc4EncryptDataPages"/> to also encrypt the page data.
    /// </summary>
    private static void SetJet4PasswordFlag(byte[] data)
    {
        // Jet4 password XOR mask (from mdbtools / jackcess specification)
        byte[] jet4PwdMask =
        [
            0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
            0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
            0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
            0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
            0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
        ];

        // Encode password "test" as UTF-16LE, XOR with masks, write to 0x42
        byte[] pwdUtf16 = System.Text.Encoding.Unicode.GetBytes("test");
        var encoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            byte pwdByte = i < pwdUtf16.Length ? pwdUtf16[i] : (byte)0;
            encoded[i] = (byte)(pwdByte ^ jet4PwdMask[i] ^ data[0x72 + (i % 4)]);
        }

        Buffer.BlockCopy(encoded, 0, data, 0x42, 40);

        // Set Office97 password flag (0x01): password required but pages are NOT RC4-encrypted.
        // Flag 0x02 would mean RC4 page encryption, which requires also encrypting the page
        // data — this helper only sets the header flag for password-verification tests.
        data[0x62] = 0x01;
    }

    /// <summary>
    /// Simulates RC4 encryption of data pages in a Jet4 database.
    /// Sets the encryption flag (0x02), encodes the password, and applies RC4
    /// to pages 1+ using the standard Jet4 page-key derivation.
    /// </summary>
    private static void Rc4EncryptDataPages(byte[] data, string password)
    {
        // Step 1: Write the RC4 encryption flag (0x02) and encode the password in the header
        SetJet4Rc4EncryptionFlag(data, password);

        // Step 2: Derive RC4 key from the database key at offset 0x3E and
        // encrypt each data page (pages 1+). The key for each page is:
        //   RC4Key = MD5(DatabaseKey + PageNumber)
        // where PageNumber is a 4-byte little-endian integer.
        uint dbKey = BitConverter.ToUInt32(data, 0x3E);

        int pageSize = Constants.PageSizes.Jet4;
        for (int pageNum = 1; pageNum * pageSize < data.Length; pageNum++)
        {
            int offset = pageNum * pageSize;
            int length = Math.Min(pageSize, data.Length - offset);

            byte[] rc4Key = DeriveJet4PageKey(dbKey, (uint)pageNum);
            Rc4Transform(data, offset, length, rc4Key);
        }
    }

    /// <summary>
    /// Sets the RC4 page-encryption flag (0x02) and encodes a password
    /// in a Jet4 database header.
    /// </summary>
    private static void SetJet4Rc4EncryptionFlag(byte[] data, string password)
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

        // Set RC4 page-encryption flag
        data[0x62] = 0x02;
    }

#pragma warning disable CA5351 // MD5 is required by the Jet4 RC4 key derivation spec
    /// <summary>
    /// Derives the RC4 key for a specific page using the Jet4 algorithm:
    /// key = first 4 bytes of MD5(dbKey LE bytes + pageNumber LE bytes).
    /// </summary>
    private static byte[] DeriveJet4PageKey(uint dbKey, uint pageNumber)
    {
        byte[] input = new byte[8];
        BitConverter.GetBytes(dbKey).CopyTo(input, 0);
        BitConverter.GetBytes(pageNumber).CopyTo(input, 4);

        byte[] hash = MD5.HashData(input);

        // Use first 4 bytes as the RC4 key (per Jet4 spec)
        return hash[..4];
    }
#pragma warning restore CA5351

    /// <summary>In-place RC4 transform (encrypt/decrypt are the same operation).</summary>
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

    /// <summary>
    /// Writes the OLE2 Compound File Binary (CFB) magic bytes at the start of the file,
    /// simulating a genuinely AES-encrypted Access 2007+ .accdb.
    /// The reader detects the CFB magic (D0 CF 11 E0) and throws
    /// <see cref="UnauthorizedAccessException"/> regardless of password.
    /// </summary>
    private static void SetAccdbEncryptionHeader(byte[] data)
    {
        // OLE2 CFB magic: first 8 bytes of any Compound File Binary container.
        // Access 2007+ AES-encrypts the .accdb by wrapping it in a CFB document.
        byte[] cfbMagic = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];

        // Jet4/ACCDB password XOR mask (same scheme at offset 0x42 for all Jet4+ versions)
        byte[] jet4PwdMask =
        [
            0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
            0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
            0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
            0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
            0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
        ];

        // Write CFB magic
        Buffer.BlockCopy(cfbMagic, 0, data, 0, cfbMagic.Length);

        // Encode password at offset 0x42 using the Jet4/ACCDB XOR scheme
        byte[] pwdUtf16 = System.Text.Encoding.Unicode.GetBytes(TestDatabases.AesEncryptedPassword);
        var encoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            byte pwdByte = i < pwdUtf16.Length ? pwdUtf16[i] : (byte)0;
            encoded[i] = (byte)(pwdByte ^ jet4PwdMask[i] ^ data[0x72 + (i % 4)]);
        }

        Buffer.BlockCopy(encoded, 0, data, 0x42, 40);

        // AES-encrypt all data pages (pages 1+) using the same key derivation
        // as the reader: SHA256(TestDatabases.AesEncryptedPassword)[0..16].
        byte[] aesKey = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(TestDatabases.AesEncryptedPassword))[..16];

        for (int page = 1; page * Constants.PageSizes.Jet4 < data.Length; page++)
        {
            int offset = page * Constants.PageSizes.Jet4;
            int length = Math.Min(Constants.PageSizes.Jet4, data.Length - offset);
            if (length % 16 != 0)
            {
                continue;
            }

            using var aes = Aes.Create();
            aes.Key = aesKey;
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.None;
            using var encryptor = aes.CreateEncryptor();
            byte[] block = new byte[length];
            Buffer.BlockCopy(data, offset, block, 0, length);
            byte[] encrypted = encryptor.TransformFinalBlock(block, 0, length);
            Buffer.BlockCopy(encrypted, 0, data, offset, length);
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
        _tempFiles.Add(temp);
        return temp;
    }
}
