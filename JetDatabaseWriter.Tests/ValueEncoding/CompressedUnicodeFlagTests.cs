namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using Xunit;

/// <summary>
/// <para>
/// Tests verifying that the writer respects the per-column
/// <c>COMPRESSED_UNICODE_EXT_FLAG_MASK</c> (bit 0x01 in the TDEF
/// column descriptor ExtraFlags byte at offset 16). Writer-created
/// Jet4/ACE Text and Memo columns have this bit set and should
/// emit the compressed form (FF FE + 1 byte/char) for Latin-1 text.
/// Columns without the flag (e.g. system table columns authored by
/// DAO/Access) must always emit plain UCS-2 LE.
/// </para>
/// <para>
/// Reader-side: the reader decodes both forms correctly regardless
/// of the flag — it detects compression by the FF FE marker bytes.
/// </para>
/// </summary>
public sealed class CompressedUnicodeFlagTests
{
    /// <summary>
    /// Writer-created Text and Memo columns store Latin-1 values in compressed
    /// form rather than plain UCS-2.
    /// </summary>
    /// <param name="maxLength">The Text width, or zero for Memo storage.</param>
    /// <param name="sentinel">The value whose on-disk encoding is checked.</param>
    [Theory]
    [InlineData(100, "CompressMe_SENTINEL")]
    [InlineData(0, "MemoCompressSentinel_ABC")]
    public async Task Writer_TextAndMemoColumns_CompressLatin1(int maxLength, string sentinel)
    {
        ArgumentException.ThrowIfNullOrEmpty(sentinel);

        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Compressed",
                [
                    new("Id", typeof(int)),
                    new("Val", typeof(string), maxLength: maxLength),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("Compressed", [1, sentinel], TestContext.Current.CancellationToken);
        }

        byte[] disk = ms.ToArray();
        Assert.True(
            disk.AsSpan().IndexOf(BuildCompressed(sentinel)) >= 0,
            "Writer-created Text and Memo columns should store Latin-1 values in compressed form.");
        Assert.True(
            disk.AsSpan().IndexOf(Encoding.Unicode.GetBytes(sentinel)) < 0,
            "Compressed columns must not store values in plain UCS-2.");
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
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
            disk.AsSpan().IndexOf(ucs2) >= 0,
            "Non-Latin-1 text must be stored as plain UCS-2 LE.");
    }

    /// <summary>
    /// The compression option selects the exact byte representation, and both
    /// representations decode to the original value.
    /// </summary>
    /// <param name="value">The Latin-1 value to encode.</param>
    /// <param name="compress">Whether to use compressed storage.</param>
    [Theory]
    [InlineData("A", false)]
    [InlineData("AB", false)]
    [InlineData("ABC", false)]
    [InlineData("HelloWorld_Test_12345", false)]
    [InlineData("TestTable", false)]
    [InlineData("ABC", true)]
    [InlineData("HelloWorld_Test_12345", true)]
    [InlineData("TestTable", true)]
    [InlineData("ShortText", false)]
    [InlineData("ShortText", true)]
    [InlineData("A medium-length string for round-tripping", false)]
    [InlineData("A medium-length string for round-tripping", true)]
    public void EncodeJet4Text_CompressionMode_UsesExpectedBytesAndRoundTrips(string value, bool compress)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);

        byte[] expected = compress ? BuildCompressed(value) : Encoding.Unicode.GetBytes(value);
        byte[] actual = JetTypeInfo.EncodeJet4Text(value, compress);

        Assert.Equal(expected, actual);
        Assert.Equal(value, JetTypeInfo.DecodeJet4Text(actual, 0, actual.Length));
    }

    /// <summary>
    /// Independently constructed compressed and plain UCS-2 payloads decode
    /// without relying on the writer's encoder.
    /// </summary>
    /// <param name="expected">The decoded value.</param>
    /// <param name="compressed">Whether the input carries the compression marker.</param>
    [Theory]
    [InlineData("Hello", true)]
    [InlineData("TestTable", true)]
    [InlineData("A longer string with spaces and CAPS 123", true)]
    [InlineData("Caf\u00E9\u00FF", true)]
    [InlineData("Hello", false)]
    [InlineData("TestTable", false)]
    [InlineData("A", false)]
    public void Reader_DecodeJet4Text_ReadsCompressedAndPlainPayloads(string expected, bool compressed)
    {
        ArgumentException.ThrowIfNullOrEmpty(expected);

        byte[] bytes = compressed ? BuildCompressed(expected) : Encoding.Unicode.GetBytes(expected);
        string actual = JetTypeInfo.DecodeJet4Text(bytes, 0, bytes.Length);
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Full integration: writer creates a table, inserts a row, then
    /// the reader reads the value back — verifying the entire
    /// ExtraFlags-aware encode/decode pipeline.
    /// </summary>
    /// <param name="format">The format.</param>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task Writer_Reader_RoundTrip_TextValue(DatabaseFormat format)
    {
        const string value = "RoundTrip_Compressed_Test";

        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadTableAsync("RTText", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, dt.Rows.Count);
        Assert.Equal(value, Assert.IsType<string>(dt.Rows[0]["Val"]));
    }

    /// <summary>
    /// Writer-created databases set ExtraFlags=0x01 on Text columns.
    /// Verify by reading the TDEF column descriptor byte at offset 16
    /// from the raw file bytes.
    /// </summary>
    [Fact]
    public async Task Writer_TextColumn_SetsCompressedUnicodeExtFlag()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
        bool foundTextCol = false;

        for (int p = 1; p < disk.Length / Constants.PageSizes.Jet4; p++)
        {
            int off = p * Constants.PageSizes.Jet4;
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

                // Text = 0x0A
                if (colType == 0x0A)
                {
                    byte extraFlags = disk[co + 16];
                    Assert.Equal(Constants.CompressedUnicodeExtFlagMask, (byte)(extraFlags & Constants.CompressedUnicodeExtFlagMask));
                    foundTextCol = true;
                }
            }
        }

        Assert.True(foundTextCol, "Should have found at least one Text column descriptor with ExtraFlags bit 0x01 set.");
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private static byte[] BuildCompressed(string value)
    {
        byte[] result = new byte[value.Length + 2];
        result[0] = 0xFF;
        result[1] = 0xFE;
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (c is '\0' or > (char)0xFF)
            {
                throw new InvalidOperationException("Compressed test payload must contain only non-zero Latin-1 characters.");
            }

            result[i + 2] = (byte)c;
        }

        return result;
    }
}
