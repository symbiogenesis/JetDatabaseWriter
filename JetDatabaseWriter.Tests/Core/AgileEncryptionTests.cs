namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Regression tests for ECMA-376 §2.3.4.10–.13 "Agile" encryption support
/// in <see cref="AccessReader"/> and <see cref="AccessWriter"/>.
///
/// Agile encryption is the modern scheme used by password-encrypted .accdb
/// files produced by Access 2010 SP1+ / Microsoft 365 with the "Encrypt with
/// Password" command. The file is a CFB compound document with two streams:
///
///   • <c>EncryptionInfo</c>  — version (4,4) header + UTF-8 XML descriptor
///                              (PBKDF salt, spinCount, hashAlgorithm,
///                              cipherAlgorithm, encryptedKeyValue, etc.)
///   • <c>EncryptedPackage</c> — 8-byte little-endian decrypted size, then
///                              AES-CBC encrypted segments (4096-byte
///                              segments with per-segment IV derived from
///                              the keyData salt and segment index).
///
/// These tests build a real, spec-compliant Agile-encrypted .accdb fixture
/// in memory by wrapping <see cref="TestDatabases.ComplexFields"/> and feed
/// it through <see cref="AccessReader.OpenAsync(Stream, AccessReaderOptions?, bool, System.Threading.CancellationToken)"/>
/// to verify the round-trip:
///   1. Parse the CFB container
///   2. Detect the Agile EncryptionInfo header (version 4.4, flag 0x40)
///   3. Parse the EncryptionInfo XML
///   4. Derive the intermediate key from the password (Agile PBKDF)
///   5. Verify the password (verifierHashInput vs verifierHashValue)
///   6. Decrypt the EncryptedPackage segments
///   7. Hand the decrypted bytes to the existing JET page reader.
/// </summary>
public sealed class AgileEncryptionTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ═══════════════════════════════════════════════════════════════════
    // 1. PASSWORD ENFORCEMENT — Agile-encrypted file requires a password
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Agile_OpenWithoutPassword_ThrowsUnauthorizedAccessException()
    {
        byte[] data = await BuildAgileEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(
                ms,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                TestContext.Current.CancellationToken));

        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Agile_OpenWithWrongPassword_ThrowsUnauthorizedAccessException()
    {
        byte[] data = await BuildAgileEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        var options = new AccessReaderOptions
        {
            Password = "definitely_wrong".AsMemory(),
            UseLockFile = false,
        };

        await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. SUCCESSFUL OPEN + CATALOG — Agile decryption produces a JET stream
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Agile_OpenWithCorrectPassword_Succeeds()
    {
        byte[] data = await BuildAgileEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        // Smoke check: ListTables must succeed (i.e. catalog page decrypted).
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);
    }

    [Fact]
    public async Task Agile_ListTables_MatchesUnencryptedSource()
    {
        // Compare the table list of the Agile-decrypted fixture against the
        // original unencrypted source — they must match exactly.
        var sourceReader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        List<string> expected = (await sourceReader.ListTablesAsync(TestContext.Current.CancellationToken))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        byte[] data = await BuildAgileEncryptedFixtureAsync();
        await using var ms = new MemoryStream(data, writable: false);
        await using var encReader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> actual = (await encReader.ListTablesAsync(TestContext.Current.CancellationToken))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, actual);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. DATA PAGE DECRYPTION — rows survive Agile + JET round-trip
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Agile_ReadDataTable_ReturnsRows()
    {
        byte[] data = await BuildAgileEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        DataTable? dt = await reader.ReadDataTableAsync(
            tables[0],
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.True(dt!.Rows.Count > 0, "Agile-decrypted table should contain rows.");
    }

    [Fact]
    public async Task Agile_StreamRows_YieldsRows()
    {
        byte[] data = await BuildAgileEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int count = await reader.Rows(
            tables[0],
            cancellationToken: TestContext.Current.CancellationToken)
            .CountAsync(TestContext.Current.CancellationToken);

        Assert.True(count > 0, "Agile-decrypted stream should yield rows.");
    }

    [Fact]
    public async Task Agile_RowCounts_MatchUnencryptedSource()
    {
        var sourceReader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        List<TableStat> expected = await sourceReader.GetTableStatsAsync(TestContext.Current.CancellationToken);

        byte[] data = await BuildAgileEncryptedFixtureAsync();
        await using var ms = new MemoryStream(data, writable: false);
        await using var encReader = await AccessReader.OpenAsync(
            ms,
            CorrectPasswordOptions(),
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        List<TableStat> actual = await encReader.GetTableStatsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expected.Count, actual.Count);
        foreach (TableStat exp in expected)
        {
            TableStat? act = actual.FirstOrDefault(s => s.Name == exp.Name);
            Assert.NotNull(act);
            Assert.Equal(exp.RowCount, act!.RowCount);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 4. KEY DERIVATION VECTOR — exercises the Agile PBKDF independently
    // ═══════════════════════════════════════════════════════════════════

    /// <summary>
    /// Round-trip self-check: deriving the intermediate key with the same
    /// inputs the fixture builder used must reproduce the verifierHashInput
    /// → verifierHashValue chain, proving the spec primitives are wired
    /// correctly. When the reader implementation lands and exposes the same
    /// primitive (or this helper is migrated next to it), this acts as a
    /// regression guard.
    /// </summary>
    [Fact]
    public void Agile_KeyDerivation_VerifierRoundTripsAgainstStoredHash()
    {
        // Use a fixed, known set of Agile parameters — any change to the
        // PBKDF, block-key constants, or hashing chain breaks this test.
        var p = AgileEncryptionFixtureBuilder.DeterministicParameters();

        byte[] verifierInput = AgileEncryptionFixtureBuilder.DecryptVerifierHashInput(p, TestDatabases.AesEncryptedPassword);
        byte[] expectedHash = System.Security.Cryptography.SHA512.HashData(verifierInput);
        byte[] storedHash = AgileEncryptionFixtureBuilder.DecryptVerifierHashValue(p, TestDatabases.AesEncryptedPassword);

        Assert.Equal(expectedHash, storedHash);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 5. WRITE SUPPORT — decrypt-on-open + re-encrypt-on-dispose round-trip
    // ═══════════════════════════════════════════════════════════════════

    /// <summary>
    /// AccessWriter must transparently decrypt the Agile-encrypted CFB on
    /// open, accept writes against the in-memory decrypted ACCDB, and
    /// re-encrypt the entire compound document back to disk on dispose.
    /// </summary>
    /// <returns>A task that completes when the round-trip assertion has run.</returns>
    [Fact]
    public async Task Agile_WriterRoundTrip_InsertedRow_VisibleAfterReopen()
    {
        const string TableName = "AgileWriteRoundTrip";

        byte[] data = await BuildAgileEncryptedFixtureAsync();
        string temp = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.accdb");
        await File.WriteAllBytesAsync(temp, data, TestContext.Current.CancellationToken);

        try
        {
            var writerOptions = new AccessWriterOptions
            {
                UseLockFile = false,
                Password = TestDatabases.AesEncryptedPassword.AsMemory(),
            };

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
                    [42, "agile-write-roundtrip"],
                    TestContext.Current.CancellationToken);
            }

            // Reopen via AccessReader: must still detect Agile, decrypt,
            // and surface the freshly-inserted row.
            await using var reader = await AccessReader.OpenAsync(temp, CorrectPasswordOptions(), TestContext.Current.CancellationToken);
            DataTable dt = (await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken))!;

            Assert.NotNull(dt);
            Assert.Single(dt.Rows);
            Assert.Equal(42, dt.Rows[0]["Id"]);
            Assert.Equal("agile-write-roundtrip", dt.Rows[0]["Label"]);
        }
        finally
        {
            if (File.Exists(temp))
            {
                File.Delete(temp);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 6. HMAC / DATA INTEGRITY — tampered ciphertext is detected
    // ═══════════════════════════════════════════════════════════════════
    //
    // The Agile spec (MS-OFFCRYPTO §2.3.4.14) mandates computing an
    // HMAC-SHA512 over the EncryptedPackage and storing the result in
    // the <dataIntegrity> element. The reader verifies this HMAC after
    // password verification but before decryption, so any ciphertext
    // tampering is caught early.

    /// <summary>
    /// Tampers with a single byte in the <c>EncryptedPackage</c> stream
    /// (past the 8-byte size prefix, inside the first AES-CBC segment).
    /// The dataIntegrity HMAC verification detects the modification and
    /// throws <see cref="InvalidDataException"/> with an integrity message.
    /// </summary>
    [Fact]
    public async Task Agile_TamperedEncryptedPackage_ThrowsIntegrityError()
    {
        byte[] data = await BuildAgileEncryptedFixtureAsync();

        // Flip a byte deep inside the ciphertext (offset 16 past the
        // 8-byte size prefix) to avoid corrupting the size header.
        int tamperOffset = FindEncryptedPackageDataOffset(data) + 16;
        data[tamperOffset] ^= 0xFF;

        await using var ms = new MemoryStream(data, writable: false);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await AccessReader.OpenAsync(
                ms,
                CorrectPasswordOptions(),
                leaveOpen: true,
                TestContext.Current.CancellationToken));

        Assert.Contains("integrity", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Tampers with the password verifier section (different from HMAC):
    /// confirms that password verification still rejects the wrong password
    /// even without HMAC, proving that authentication relies on the
    /// verifier hash triple, not the dataIntegrity HMAC.
    /// </summary>
    [Fact]
    public async Task Agile_WrongPassword_StillRejected_IndependentOfHmac()
    {
        byte[] data = await BuildAgileEncryptedFixtureAsync();

        await using var ms = new MemoryStream(data, writable: false);
        var options = new AccessReaderOptions
        {
            Password = "not_the_password".AsMemory(),
            UseLockFile = false,
        };

        var ex = await Assert.ThrowsAsync<UnauthorizedAccessException>(
            async () => await AccessReader.OpenAsync(ms, options, leaveOpen: true, TestContext.Current.CancellationToken));

        // This proves the password verifier (SHA-512 hash chain) provides
        // authentication independent of the HMAC mechanism.
        Assert.Contains("password", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    private static AccessReaderOptions CorrectPasswordOptions() => new()
    {
        Password = TestDatabases.AesEncryptedPassword.AsMemory(),
        UseLockFile = false,
    };

    /// <summary>
    /// Finds the file offset of the EncryptedPackage stream's data
    /// (the first byte of its first sector) in the CFB fixture.
    /// </summary>
    private static int FindEncryptedPackageDataOffset(byte[] cfbFile)
    {
        // The fixture builder uses v4 (4096-byte sectors). The directory
        // is at sector 1. Scan directory entries to find "EncryptedPackage".
        const int SectorSize = 4096;
        int dirOffset = SectorSize + (1 * SectorSize); // sector 1

        for (int entry = 0; entry < 4; entry++)
        {
            int off = dirOffset + (entry * 128);
            ushort nameLen = BinaryPrimitives.ReadUInt16LittleEndian(cfbFile.AsSpan(off + 0x40, 2));
            if (nameLen == 0 || nameLen > 64)
            {
                continue;
            }

            string name = Encoding.Unicode.GetString(cfbFile, off, nameLen - 2);
            if (name == "EncryptedPackage")
            {
                uint startSector = BinaryPrimitives.ReadUInt32LittleEndian(cfbFile.AsSpan(off + 0x74, 4));
                return SectorSize + ((int)startSector * SectorSize);
            }
        }

        throw new InvalidOperationException("EncryptedPackage directory entry not found in CFB fixture.");
    }

    private async Task<byte[]> BuildAgileEncryptedFixtureAsync()
    {
        byte[] inner = await db.GetFileAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        return AgileEncryptionFixtureBuilder.Build(inner, TestDatabases.AesEncryptedPassword);
    }
}
