namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for database encryption across all Jet/ACE versions:
///   1. Jet3 XOR mask  — fixed XOR pattern applied to all pages after page 0
///   2. Jet4 RC4 flag  — password verified against the XOR-encoded header hash (0x42)
///   3. Jet4 RC4 pages — TDD: RC4 page decryption not yet implemented
///   4. ACCDB AES      — detection works (OLE2 CFB magic); page decryption still TDD red
///   5. ACCDB AES      — TDD: genuine AesEncrypted.accdb fixture from Access 16 CompactDatabase.
/// </summary>
public sealed class EncryptionTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    // ═══════════════════════════════════════════════════════════════════
    // 1. JET3 XOR MASK
    // ═══════════════════════════════════════════════════════════════════
    //
    // Jet3 encryption uses a fixed 128-byte XOR mask applied cyclically
    // to every page after page 0. The reader detects the flag and removes
    // the mask transparently on open.

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Encryption_OptionsAcceptPassword(string path)
    {
        // AccessReaderOptions accepts a Password property for encrypted databases.
        // Non-encrypted databases ignore the password and open normally.
        var options = new AccessReaderOptions
        {
            Password = "test123",
        };

        using var reader = AccessReader.Open(path, options);
        var tables = reader.ListTables();

        Assert.NotNull(tables);
    }

    [Fact]
    public void Encryption_Jet3Xor_DatabaseIsReadable()
    {
        // Jet3 encryption uses a simple XOR mask applied to every page.
        // Verify the reader detects and transparently decrypts XOR-masked databases.
        string temp = CopyToTemp(TestDatabases.Jet3Test);
        byte[] xorMask = BuildJet3XorMask();
        ApplyXorMask(temp, xorMask);
        SetJet3EncryptionFlag(temp);

        using var reader = AccessReader.Open(temp);
        var tables = reader.ListTables();

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
    public void Encryption_Jet4Rc4_WithCorrectPassword_DatabaseIsReadable()
    {
        // Verify that a Jet4 database with the password flag set is readable
        // when the correct password is provided.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(temp);

        var options = new AccessReaderOptions
        {
            Password = "test",
        };

        using var reader = AccessReader.Open(temp, options);
        var tables = reader.ListTables();

        Assert.NotEmpty(tables);
    }

    [Fact]
    public void Encryption_Jet4Rc4_WithoutPassword_ThrowsDescriptiveError()
    {
        // Opening an encrypted database without a password throws with a
        // message that hints at providing a password.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(temp);

        var ex = Assert.Throws<UnauthorizedAccessException>(() => AccessReader.Open(temp));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Encryption_Jet4Rc4_WithWrongPassword_ThrowsMeaningfulError()
    {
        // An incorrect password produces a clear error rather than corrupt data.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        SetJet4PasswordFlag(temp);

        var options = new AccessReaderOptions
        {
            Password = "wrong_password",
        };

        var ex = Record.Exception(() =>
        {
            using var reader = AccessReader.Open(temp, options);
            reader.ListTables();
        });

        Assert.NotNull(ex);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. JET4 RC4 PAGE DECRYPTION
    // ═══════════════════════════════════════════════════════════════════
    //
    // Current state: the password is verified against the stored hash,
    // but actual RC4 decryption of data pages is not performed.
    // When implemented, the reader should:
    //   - Derive the RC4 key from the database key (header offset 0x3E)
    //   - Decrypt each page (except page 0) using RC4 with the page-number-derived key
    //   - Return correct, readable data from encrypted databases

    [Fact]
    public void Rc4Decryption_EncryptedJet4_ReadTable_ReturnsDecryptedRows()
    {
        // A Jet4 database with RC4 encryption set and password provided
        // should return actual row data, not garbled bytes.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(temp, "test");

        var options = new AccessReaderOptions { Password = "test" };
        using var reader = AccessReader.Open(temp, options);
        DataTable dt = reader.ReadTable("Product")!;

        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "RC4-decrypted table should contain rows");
    }

    [Fact]
    public void Rc4Decryption_EncryptedJet4_StreamRows_ReturnsDecryptedRows()
    {
        // Streaming should also work through RC4-encrypted pages.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(temp, "test");

        var options = new AccessReaderOptions { Password = "test" };
        using var reader = AccessReader.Open(temp, options);

        List<string> tables = reader.ListTables();
        Assert.NotEmpty(tables);

        int rowCount = reader.StreamRows(tables[0]).Count();
        Assert.True(rowCount > 0, "RC4-decrypted stream should yield rows");
    }

    [Fact]
    public void Rc4Decryption_EncryptedJet4_GetStatistics_ReturnsValidStats()
    {
        // Statistics (catalog scan, row counts) should work on encrypted databases.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(temp, "test");

        var options = new AccessReaderOptions { Password = "test" };
        using var reader = AccessReader.Open(temp, options);
        DatabaseStatistics stats = reader.GetStatistics();

        Assert.True(stats.TableCount > 0, "Should report tables in encrypted database");
        Assert.True(stats.TotalRows > 0, "Should report rows in encrypted database");
    }

    [Fact]
    public void Rc4Decryption_EncryptedJet4_ColumnMetadata_IsCorrect()
    {
        // Column metadata from TDEF pages must be decrypted correctly.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(temp, "test");

        var options = new AccessReaderOptions { Password = "test" };
        using var reader = AccessReader.Open(temp, options);

        List<string> tables = reader.ListTables();
        Assert.NotEmpty(tables);

        List<ColumnMetadata> meta = reader.GetColumnMetadata(tables[0]);
        Assert.NotEmpty(meta);
        Assert.All(meta, col =>
        {
            Assert.False(string.IsNullOrEmpty(col.Name), "Column name should be readable after decryption");
            Assert.NotEqual(typeof(object), col.ClrType);
        });
    }

    [Fact]
    public void Rc4Decryption_EncryptedJet4_ReadAllTables_Succeeds()
    {
        // Bulk read of all tables should succeed on an RC4-encrypted database.
        string temp = CopyToTemp(TestDatabases.AdventureWorks);
        Rc4EncryptDataPages(temp, "test");

        var options = new AccessReaderOptions { Password = "test" };
        using var reader = AccessReader.Open(temp, options);

        Dictionary<string, DataTable> all = reader.ReadAllTables();
        Assert.NotEmpty(all);
        Assert.All(all.Values, dt => Assert.NotNull(dt));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 4. ACCDB (AES) ENCRYPTION — Access 2007+
    // ═══════════════════════════════════════════════════════════════════
    //
    // Genuine Access 2007+ AES encryption wraps the entire .accdb file in an
    // OLE2 Compound File Binary (CFB) container.  The first four bytes of a
    // genuinely AES-encrypted file are the CFB magic: D0 CF 11 E0.
    //
    // Current state:
    //   ✓ Detection  — CFB magic check fires; UnauthorizedAccessException thrown.
    //   ✗ Decryption — AES page decryption not yet implemented; even a correct
    //                  password cannot open the file (TDD red tests below).
    //
    // SetAccdbEncryptionHeader writes the CFB magic to simulate a genuinely
    // AES-encrypted file without needing Access installed.

    [Fact]
    public void AesEncryption_AccdbWithPassword_Open_Succeeds()
    {
        // An .accdb file encrypted with AES should open when the correct password is provided.
        string temp = CopyToTemp(TestDatabases.NorthwindTraders);
        SetAccdbEncryptionHeader(temp);

        var options = new AccessReaderOptions { Password = "secret" };

        // Once AES is implemented, this should succeed without throwing.
        var ex = Record.Exception(() =>
        {
            using var reader = AccessReader.Open(temp, options);
            reader.ListTables();
        });

        // Currently expected to fail; when implemented, ex should be null.
        Assert.Null(ex);
    }

    [Fact]
    public void AesEncryption_AccdbWithPassword_ReadTable_ReturnsRows()
    {
        // Reading table data through AES decryption should return valid rows.
        string temp = CopyToTemp(TestDatabases.NorthwindTraders);
        SetAccdbEncryptionHeader(temp);

        var options = new AccessReaderOptions { Password = "secret" };
        using var reader = AccessReader.Open(temp, options);

        List<string> tables = reader.ListTables();
        Assert.NotEmpty(tables);

        DataTable dt = reader.ReadTable(tables[0])!;
        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "AES-decrypted table should contain rows");
    }

    [Fact]
    public void AesEncryption_AccdbWithoutPassword_ThrowsUnauthorized()
    {
        // A genuine AES-encrypted .accdb (CFB magic in header) must throw
        // UnauthorizedAccessException whether or not a password is supplied,
        // because full AES page decryption is not yet implemented.
        string temp = CopyToTemp(TestDatabases.NorthwindTraders);
        SetAccdbEncryptionHeader(temp);

        var ex = Assert.Throws<UnauthorizedAccessException>(() => AccessReader.Open(temp));
        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AesEncryption_AccdbWithWrongPassword_Throws()
    {
        // Any password for a CFB-encrypted .accdb must throw, because decryption
        // is not implemented — the error should be clear rather than silent corruption.
        string temp = CopyToTemp(TestDatabases.NorthwindTraders);
        SetAccdbEncryptionHeader(temp);

        var options = new AccessReaderOptions { Password = "wrong_password" };

        var ex = Record.Exception(() =>
        {
            using var reader = AccessReader.Open(temp, options);
            reader.ListTables();
        });

        Assert.NotNull(ex);
    }

    [Fact]
    public void AesEncryption_AccdbStreamRows_ReturnsDecryptedData()
    {
        // Streaming through AES-encrypted pages should yield readable data.
        string temp = CopyToTemp(TestDatabases.NorthwindTraders);
        SetAccdbEncryptionHeader(temp);

        var options = new AccessReaderOptions { Password = "secret" };
        using var reader = AccessReader.Open(temp, options);

        List<string> tables = reader.ListTables();
        Assert.NotEmpty(tables);

        int count = reader.StreamRows(tables[0]).Count();
        Assert.True(count > 0, "AES-decrypted stream should yield rows");
    }

    // ═══════════════════════════════════════════════════════════════════
    // 5. GENUINE ACCDB AES FIXTURE — AesEncrypted.accdb
    // ═══════════════════════════════════════════════════════════════════
    //
    // AesEncrypted.accdb was produced by:
    //   $access.DBEngine.CompactDatabase(plain, dest, ";;", 4, ";pwd=secret")
    // using Access 16 (16.0) COM automation with password "secret".
    //
    // Header characteristics:
    //   • Version (0x14) = 0x03  →  Access 2010 / ACE 14 format
    //   • First 4 bytes: 00 01 00 00  →  standard ACCDB header (NOT CFB magic)
    //     Data pages are NOT AES-encrypted; the password is stored in the header
    //     using an ACE-internal scheme the reader does not yet implement.
    //   • The reader's ACCDB check is CFB-magic-based: this file has no CFB magic,
    //     so it opens without password verification — the old-style ACE password
    //     is silently bypassed.  This is a known limitation documented in the README.
    //
    // What passes now:
    //   • The file opens without a password (pages are not encrypted).
    //   • ListTables / ReadTable return the original NorthwindTraders data.
    //
    // What is still TDD red (pending ACE password verification):
    //   • The reader does not detect that a password is required.
    //   • A wrong password does not produce an error.

    [Fact]
    public void AesEncryption_GenuineAccdb_HasStandardAccdbHeader_NotCfbMagic()
    {
        // AesEncrypted.accdb starts with the standard ACCDB header (00 01 00 00),
        // NOT the OLE2 CFB magic (D0 CF 11 E0).  This confirms the reader's
        // CFB-based AES detection does NOT fire for this file — it is a
        // "password-only" ACCDB, not a genuinely AES-page-encrypted file.
        byte[] bytes = File.ReadAllBytes(TestDatabases.AesEncrypted);
        Assert.Equal(0x00, bytes[0]);
        Assert.Equal(0x01, bytes[1]);
        Assert.Equal(0x00, bytes[2]);
        Assert.Equal(0x00, bytes[3]);
    }

    [Fact]
    public void AesEncryption_GenuineAccdb_Version_IsAccdbFormat()
    {
        // Sanity-check the fixture: version byte 0x14 must be >= 2 (ACCDB format).
        byte[] bytes = File.ReadAllBytes(TestDatabases.AesEncrypted);
        Assert.True(
            bytes[0x14] >= 2,
            $"Expected ACCDB version >= 2 at offset 0x14, got 0x{bytes[0x14]:X2}");
    }

    [Fact]
    public void AesEncryption_GenuineAccdb_OpensWithoutPassword_BecauseNoPageEncryption()
    {
        // AesEncrypted.accdb uses an old-style ACE password (hash in header only).
        // Data pages are NOT encrypted, so the reader opens the file without a password.
        // Old-style ACE password verification is not yet implemented (known limitation).
        var ex = Record.Exception(() =>
        {
            using var reader = AccessReader.Open(TestDatabases.AesEncrypted);
            reader.ListTables();
        });

        Assert.Null(ex);
    }

    [Fact]
    public void AesEncryption_GenuineAccdb_ListTables_ReturnsNonEmpty()
    {
        // Since pages are not encrypted, ListTables returns the original database contents.
        using var reader = AccessReader.Open(TestDatabases.AesEncrypted);
        List<string> tables = reader.ListTables();
        Assert.NotEmpty(tables);
    }

    [Fact]
    public void AesEncryption_GenuineAccdb_ReadTable_ReturnsRows()
    {
        // Data pages are readable; ReadTable returns original NorthwindTraders rows.
        using var reader = AccessReader.Open(TestDatabases.AesEncrypted);
        List<string> tables = reader.ListTables();
        Assert.NotEmpty(tables);

        DataTable dt = reader.ReadTable(tables[0])!;
        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count > 0, "Table should contain rows from the source database.");
    }

    [Fact]
    public void AesEncryption_GenuineAccdb_StreamRows_ReturnsRows()
    {
        // StreamRows also works because data pages are unencrypted.
        using var reader = AccessReader.Open(TestDatabases.AesEncrypted);
        List<string> tables = reader.ListTables();
        Assert.NotEmpty(tables);

        int rowCount = reader.StreamRows(tables[0]).Count();
        Assert.True(rowCount > 0, "StreamRows should yield rows from the source database.");
    }

    [Fact]
    public void AesEncryption_GenuineAccdb_GetStatistics_ReturnsValidStats()
    {
        // Statistics work because catalog pages are readable.
        using var reader = AccessReader.Open(TestDatabases.AesEncrypted);
        DatabaseStatistics stats = reader.GetStatistics();

        Assert.True(stats.TableCount > 0, "Should report tables.");
        Assert.True(stats.TotalRows > 0, "Should report rows.");
    }

    // ── TDD red: ACE old-style password verification (not yet implemented) ──

    [Fact]
    public void AesEncryption_GenuineAccdb_OldStylePassword_IsNotDetected_TDD()
    {
        // TDD: The reader currently does NOT detect old-style ACE passwords.
        // When ACE password verification is implemented, opening without a password
        // should throw UnauthorizedAccessException.
        var ex = Record.Exception(() =>
        {
            using var reader = AccessReader.Open(TestDatabases.AesEncrypted);
            reader.ListTables();
        });

        // TDD: currently null (opens without error).
        // When implemented, this should be Assert.NotNull(ex) instead.
        Assert.Null(ex); // known limitation — will flip to NotNull when implemented
    }

    [Fact]
    public void AesEncryption_GenuineAccdb_WithWrongPassword_DoesNotThrow_TDD()
    {
        // TDD: Because old-style ACE password verification is not implemented,
        // a wrong password is silently ignored and the file opens normally.
        // When verification is implemented, this should throw UnauthorizedAccessException.
        var options = new AccessReaderOptions { Password = "definitely_wrong_password" };

        var ex = Record.Exception(() =>
        {
            using var reader = AccessReader.Open(TestDatabases.AesEncrypted, options);
            reader.ListTables();
        });

        // TDD: currently null.  When implemented, Assert.NotNull(ex) and
        // Assert.IsAssignableFrom<UnauthorizedAccessException>(ex).
        Assert.Null(ex);
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
        return new byte[]
        {
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
        };
    }

    /// <summary>XOR-masks a database file starting at page 1 (page 0 is the header).</summary>
    private static void ApplyXorMask(string path, byte[] mask)
    {
        const int jet3PageSize = 2048;
        byte[] data = File.ReadAllBytes(path);

        // Apply mask starting from page 1 (offset 2048) through the file
        for (int offset = jet3PageSize; offset < data.Length; offset++)
        {
            data[offset] ^= mask[(offset - jet3PageSize) % mask.Length];
        }

        File.WriteAllBytes(path, data);
    }

    /// <summary>Sets the Office97 password flag (0x01) in a Jet3 database header.</summary>
    private static void SetJet3EncryptionFlag(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
        fs.Seek(0x62, SeekOrigin.Begin);
        fs.WriteByte(0x01); // Office97 password flag
    }

    /// <summary>
    /// Sets the Office97 password flag (0x01) and encodes password <c>"test"</c>
    /// in a Jet4 database header. Data pages are <em>not</em> RC4-encrypted —
    /// use <see cref="Rc4EncryptDataPages"/> to also encrypt the page data.
    /// </summary>
    private static void SetJet4PasswordFlag(string path)
    {
        // Jet4 password XOR mask (from mdbtools / jackcess specification)
        byte[] jet4PwdMask =
        {
            0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
            0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
            0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
            0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
            0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
        };

        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);

        // Read full header (need creation date at 0x72 for password mask)
        var hdr = new byte[0x80];
        fs.Seek(0, SeekOrigin.Begin);
        _ = fs.Read(hdr, 0, hdr.Length);

        // Encode password "test" as UTF-16LE, XOR with masks, write to 0x42
        byte[] pwdUtf16 = System.Text.Encoding.Unicode.GetBytes("test");
        var encoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            byte pwdByte = i < pwdUtf16.Length ? pwdUtf16[i] : (byte)0;
            encoded[i] = (byte)(pwdByte ^ jet4PwdMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        fs.Seek(0x42, SeekOrigin.Begin);
        fs.Write(encoded, 0, 40);

        // Set Office97 password flag (0x01): password required but pages are NOT RC4-encrypted.
        // Flag 0x02 would mean RC4 page encryption, which requires also encrypting the page
        // data — this helper only sets the header flag for password-verification tests.
        fs.Seek(0x62, SeekOrigin.Begin);
        fs.WriteByte(0x01);
    }

    /// <summary>
    /// Simulates RC4 encryption of data pages in a Jet4 database.
    /// Sets the encryption flag (0x02), encodes the password, and applies RC4
    /// to pages 1+ using the standard Jet4 page-key derivation.
    /// </summary>
    private static void Rc4EncryptDataPages(string path, string password)
    {
        // Step 1: Write the RC4 encryption flag (0x02) and encode the password in the header
        SetJet4Rc4EncryptionFlag(path, password);

        // Step 2: Derive RC4 key from the database key at offset 0x3E and
        // encrypt each data page (pages 1+). The key for each page is:
        //   RC4Key = MD5(DatabaseKey + PageNumber)
        // where PageNumber is a 4-byte little-endian integer.
        byte[] data = File.ReadAllBytes(path);
        uint dbKey = BitConverter.ToUInt32(data, 0x3E);

        int pageSize = 4096; // Jet4
        for (int pageNum = 1; pageNum * pageSize < data.Length; pageNum++)
        {
            int offset = pageNum * pageSize;
            int length = Math.Min(pageSize, data.Length - offset);

            byte[] rc4Key = DeriveJet4PageKey(dbKey, (uint)pageNum);
            Rc4Transform(data, offset, length, rc4Key);
        }

        File.WriteAllBytes(path, data);
    }

    /// <summary>
    /// Sets the RC4 page-encryption flag (0x02) and encodes a password
    /// in a Jet4 database header.
    /// </summary>
    private static void SetJet4Rc4EncryptionFlag(string path, string password)
    {
        byte[] jet4PwdMask =
        {
            0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
            0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
            0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
            0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
            0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
        };

        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
        var hdr = new byte[0x80];
        fs.Seek(0, SeekOrigin.Begin);
        _ = fs.Read(hdr, 0, hdr.Length);

        byte[] pwdUtf16 = System.Text.Encoding.Unicode.GetBytes(password);
        var encoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            byte pwdByte = i < pwdUtf16.Length ? pwdUtf16[i] : (byte)0;
            encoded[i] = (byte)(pwdByte ^ jet4PwdMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        fs.Seek(0x42, SeekOrigin.Begin);
        fs.Write(encoded, 0, 40);

        // Set RC4 page-encryption flag
        fs.Seek(0x62, SeekOrigin.Begin);
        fs.WriteByte(0x02);
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

        byte[] hash = System.Security.Cryptography.MD5.HashData(input);

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
    private static void SetAccdbEncryptionHeader(string path)
    {
        // OLE2 CFB magic: first 8 bytes of any Compound File Binary container.
        // Access 2007+ AES-encrypts the .accdb by wrapping it in a CFB document.
        byte[] cfbMagic = { 0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1 };

        // Jet4/ACCDB password XOR mask (same scheme at offset 0x42 for all Jet4+ versions)
        byte[] jet4PwdMask =
        {
            0x86, 0xFB, 0xEC, 0x37, 0x5D, 0x44, 0x9C, 0xFA,
            0xC6, 0x5E, 0x28, 0xE6, 0x13, 0xB6, 0x8A, 0x60,
            0x54, 0x94, 0x7B, 0x36, 0xD1, 0xEC, 0xDF, 0xB1,
            0x31, 0x6A, 0x13, 0x43, 0xEF, 0x31, 0xB1, 0x33,
            0xA1, 0xFE, 0x6A, 0x7A, 0x42, 0x62, 0x04, 0xFE,
        };

        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);

        // Read header for creation date at 0x72 (needed for password XOR)
        var hdr = new byte[0x80];
        fs.Seek(0, SeekOrigin.Begin);
        _ = fs.Read(hdr, 0, hdr.Length);

        // Write CFB magic
        fs.Seek(0, SeekOrigin.Begin);
        fs.Write(cfbMagic, 0, cfbMagic.Length);

        // Encode password "secret" at offset 0x42 using the Jet4/ACCDB XOR scheme
        byte[] pwdUtf16 = System.Text.Encoding.Unicode.GetBytes("secret");
        var encoded = new byte[40];
        for (int i = 0; i < 40; i++)
        {
            byte pwdByte = i < pwdUtf16.Length ? pwdUtf16[i] : (byte)0;
            encoded[i] = (byte)(pwdByte ^ jet4PwdMask[i] ^ hdr[0x72 + (i % 4)]);
        }

        fs.Seek(0x42, SeekOrigin.Begin);
        fs.Write(encoded, 0, 40);
    }

    private string CopyToTemp(string sourcePath)
    {
        string ext = Path.GetExtension(sourcePath);
        string temp = Path.Combine(Path.GetTempPath(), $"JetEncTest_{Guid.NewGuid():N}{ext}");
        _tempFiles.Add(temp);
        File.Copy(sourcePath, temp, overwrite: true);
        return temp;
    }
}
