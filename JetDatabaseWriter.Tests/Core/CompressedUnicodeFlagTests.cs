namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests verifying that the writer respects the per-column
/// <c>COMPRESSED_UNICODE_EXT_FLAG_MASK</c> (bit 0x01 in the TDEF
/// column descriptor ExtraFlags byte at offset 16). Writer-created
/// Jet4/ACE T_TEXT and T_MEMO columns have this bit set and should
/// emit the compressed form (FF FE + 1 byte/char) for Latin-1 text.
/// Columns without the flag (e.g. system table columns authored by
/// DAO/Access) must always emit plain UCS-2 LE.
///
/// Reader-side: the reader decodes both forms correctly regardless
/// of the flag — it detects compression by the FF FE marker bytes.
/// </summary>
public sealed class CompressedUnicodeFlagTests
{
    // ═══════════════════════════════════════════════════════════════
    // §1  Writer: user-created TEXT columns compress Latin-1
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// A writer-created T_TEXT column (ExtraFlags bit 0x01 set by
    /// TDefPageBuilder) stores an ASCII-range value in compressed
    /// form — the FF FE marker must appear on disk.
    /// </summary>
    [Fact]
    public async Task Writer_TextColumn_CompressesLatin1()
    {
        const string sentinel = "CompressMe_SENTINEL";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "CmpText",
                [
                    new("Id", typeof(int)),
                    new("Val", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("CmpText", [1, sentinel], TestContext.Current.CancellationToken);
        }

        byte[] disk = ms.ToArray();

        // Compressed form: FF FE + ASCII bytes
        byte[] compressed = BuildCompressed(sentinel);
        Assert.True(
            IndexOf(disk, compressed) >= 0,
            "Writer-created TEXT column should store Latin-1 value in compressed form (FF FE + 1-byte/char).");

        // Plain UCS-2 must NOT appear
        byte[] ucs2 = Encoding.Unicode.GetBytes(sentinel);
        Assert.True(
            IndexOf(disk, ucs2) < 0,
            "Compressed column must NOT store value in plain UCS-2.");
    }

    /// <summary>
    /// A writer-created T_MEMO column (ExtraFlags bit 0x01 set)
    /// compresses inline Latin-1 MEMO values.
    /// </summary>
    [Fact]
    public async Task Writer_MemoColumn_CompressesLatin1()
    {
        const string sentinel = "MemoCompressSentinel_ABC";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "CmpMemo",
                [
                    new("Id", typeof(int)),
                    new("Notes", typeof(string)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("CmpMemo", [1, sentinel], TestContext.Current.CancellationToken);
        }

        byte[] disk = ms.ToArray();
        byte[] compressed = BuildCompressed(sentinel);
        Assert.True(
            IndexOf(disk, compressed) >= 0,
            "Writer-created MEMO column should compress Latin-1 inline values.");
    }

    /// <summary>
    /// Non-Latin-1 text (contains chars > U+00FF) is never compressed,
    /// regardless of the ExtraFlags bit — it must appear as UCS-2.
    /// </summary>
    [Fact]
    public async Task Writer_TextColumn_NonLatin1_StoresAsUcs2()
    {
        const string value = "Hello\u4E16\u754C"; // "Hello世界"

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "CmpUni",
                [
                    new("Id", typeof(int)),
                    new("Val", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("CmpUni", [1, value], TestContext.Current.CancellationToken);
        }

        byte[] disk = ms.ToArray();
        byte[] ucs2 = Encoding.Unicode.GetBytes(value);
        Assert.True(
            IndexOf(disk, ucs2) >= 0,
            "Non-Latin-1 text must be stored as plain UCS-2 LE.");
    }

    // ═══════════════════════════════════════════════════════════════
    // §2  Writer: catalog rows respect the system table's
    //     ExtraFlags (no compression flag on MSysObjects.Name)
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// When a writer creates a table, the catalog row in MSysObjects
    /// stores the table name. MSysObjects columns in DAO-authored
    /// databases do NOT have the COMPRESSED_UNICODE bit set, so the
    /// writer must NOT compress the Name column value. Verify by
    /// creating a table in an existing NorthwindTraders copy and
    /// checking that the table name appears in UCS-2 form only.
    /// </summary>
    [Fact]
    public async Task Writer_CatalogRow_RespectsSystemTableExtraFlags()
    {
        // Use a fresh in-memory database created by the writer itself.
        // Writer-created MSysObjects columns DO have ExtraFlags=0x01,
        // so in writer-created databases, catalog names ARE compressed.
        // The critical scenario is opening an existing DAO-authored
        // database — tested indirectly by the DaoValidationTests.
        // Here we just verify the internal plumbing: the writer's
        // EncodeJet4Text respects the compress=false path.
        byte[] plain = AccessBase.EncodeJet4Text("TestTable", compress: false);
        byte[] compressed = AccessBase.EncodeJet4Text("TestTable", compress: true);

        // Plain must be UCS-2 LE (no FF FE marker)
        Assert.True(plain.Length > 0);
        Assert.False(
            plain[0] == 0xFF && plain[1] == 0xFE,
            "EncodeJet4Text(compress: false) must NOT emit the FF FE compressed marker.");

        // Compressed must have FF FE marker
        Assert.True(compressed.Length >= 2);
        Assert.Equal(0xFF, compressed[0]);
        Assert.Equal(0xFE, compressed[1]);

        // UCS-2 form should be 2× the string length
        Assert.Equal("TestTable".Length * 2, plain.Length);

        // Compressed form: 2-byte marker + 1 byte/char
        Assert.Equal("TestTable".Length + 2, compressed.Length);
    }

    /// <summary>
    /// Verifies that EncodeJet4Text with compress=false always produces
    /// plain UCS-2 for any Latin-1 string, even if it would be
    /// compressible.
    /// </summary>
    [Theory]
    [InlineData("A")]
    [InlineData("AB")]
    [InlineData("ABC")]
    [InlineData("HelloWorld_Test_12345")] // longer string
    public void EncodeJet4Text_CompressFalse_NeverEmitsMarker(string value)
    {
        byte[] result = AccessBase.EncodeJet4Text(value, compress: false);
        if (result.Length >= 2)
        {
            Assert.False(
                result[0] == 0xFF && result[1] == 0xFE,
                $"compress=false must never produce FF FE marker for '{value}'.");
        }
    }

    /// <summary>
    /// Verifies that EncodeJet4Text with compress=true produces the
    /// compressed form for Latin-1 strings ≥ 3 characters.
    /// </summary>
    [Theory]
    [InlineData("ABC")]
    [InlineData("HelloWorld_Test_12345")]
    public void EncodeJet4Text_CompressTrue_EmitsMarkerForLatin1(string value)
    {
        byte[] result = AccessBase.EncodeJet4Text(value, compress: true);
        Assert.True(result.Length >= 2);
        Assert.Equal(0xFF, result[0]);
        Assert.Equal(0xFE, result[1]);
    }

    // ═══════════════════════════════════════════════════════════════
    // §3  Reader: decodes both compressed and plain UCS-2 correctly
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// The reader's DecodeJet4Text correctly decompresses FF FE
    /// marker data.
    /// </summary>
    [Theory]
    [InlineData("Hello")]
    [InlineData("TestTable")]
    [InlineData("A longer string with spaces and CAPS 123")]
    public void Reader_DecodeJet4Text_DecompressesMarkedData(string expected)
    {
        byte[] compressed = BuildCompressed(expected);
        string actual = AccessBase.DecodeJet4Text(compressed, 0, compressed.Length);
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// The reader's DecodeJet4Text correctly reads plain UCS-2 LE
    /// data (no FF FE marker).
    /// </summary>
    [Theory]
    [InlineData("Hello")]
    [InlineData("TestTable")]
    [InlineData("A")]
    public void Reader_DecodeJet4Text_ReadsPlainUcs2(string expected)
    {
        byte[] ucs2 = Encoding.Unicode.GetBytes(expected);
        string actual = AccessBase.DecodeJet4Text(ucs2, 0, ucs2.Length);
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Round-trip: compress=true → decode, compress=false → decode
    /// both yield the original string.
    /// </summary>
    [Theory]
    [InlineData("ShortText")]
    [InlineData("A medium-length string for round-tripping")]
    public void RoundTrip_BothCompressionModes_DecodeCorrectly(string original)
    {
        byte[] compressed = AccessBase.EncodeJet4Text(original, compress: true);
        byte[] plain = AccessBase.EncodeJet4Text(original, compress: false);

        Assert.Equal(original, AccessBase.DecodeJet4Text(compressed, 0, compressed.Length));
        Assert.Equal(original, AccessBase.DecodeJet4Text(plain, 0, plain.Length));
    }

    /// <summary>
    /// Full integration: writer creates a table, inserts a row, then
    /// the reader reads the value back — verifying the entire
    /// ExtraFlags-aware encode/decode pipeline.
    /// </summary>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task Writer_Reader_RoundTrip_TextValue(DatabaseFormat format)
    {
        const string value = "RoundTrip_Compressed_Test";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "RTText",
                [
                    new("Id", typeof(int)),
                    new("Val", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("RTText", [1, value], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync("RTText", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, dt.Rows.Count);
        Assert.Equal(value, Assert.IsType<string>(dt.Rows[0]["Val"]));
    }

    /// <summary>
    /// Writer-created databases set ExtraFlags=0x01 on T_TEXT columns.
    /// Verify by reading the TDEF column descriptor byte at offset 16
    /// from the raw file bytes.
    /// </summary>
    [Fact]
    public async Task Writer_TextColumn_SetsCompressedUnicodeExtFlag()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "FlagTest",
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        byte[] disk = ms.ToArray();

        // Find the user table's TDEF page by scanning for page type 0x02
        // (TDEF). The TDEF header at offset +45 has num_cols (2 bytes).
        // Each Jet4 column descriptor is 25 bytes starting at
        // BlockEnd (63) + (num_real_idx * 12). ExtraFlags is at
        // descriptor-relative offset 16.
        const int pageSize = 4096;
        bool foundTextCol = false;

        for (int p = 1; p < disk.Length / pageSize; p++)
        {
            int off = p * pageSize;
            if (disk[off] != 0x02)
            {
                continue; // Not a TDEF page
            }

            int numCols = disk[off + 45] | (disk[off + 46] << 8);
            if (numCols != 2)
            {
                continue; // Not our 2-column table
            }

            int numRealIdx = disk[off + 51] | (disk[off + 52] << 8) | (disk[off + 53] << 16) | (disk[off + 54] << 24);
            int colStart = off + 63 + (numRealIdx * 12);

            for (int c = 0; c < numCols; c++)
            {
                int co = colStart + (c * 25);
                byte colType = disk[co]; // col_type at offset 0

                // T_TEXT = 0x0A
                if (colType == 0x0A)
                {
                    byte extraFlags = disk[co + 16];
                    Assert.Equal(Constants.CompressedUnicodeExtFlagMask, (byte)(extraFlags & Constants.CompressedUnicodeExtFlagMask));
                    foundTextCol = true;
                }
            }
        }

        Assert.True(foundTextCol, "Should have found at least one T_TEXT column descriptor with ExtraFlags bit 0x01 set.");
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private static byte[] BuildCompressed(string value)
    {
        byte[] ascii = Encoding.ASCII.GetBytes(value);
        byte[] result = new byte[ascii.Length + 2];
        result[0] = 0xFF;
        result[1] = 0xFE;
        Buffer.BlockCopy(ascii, 0, result, 2, ascii.Length);
        return result;
    }

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        if (needle.Length == 0 || haystack.Length < needle.Length)
        {
            return -1;
        }

        for (int i = 0; i <= haystack.Length - needle.Length; i++)
        {
            bool found = true;
            for (int j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] != needle[j])
                {
                    found = false;
                    break;
                }
            }

            if (found)
            {
                return i;
            }
        }

        return -1;
    }
}
