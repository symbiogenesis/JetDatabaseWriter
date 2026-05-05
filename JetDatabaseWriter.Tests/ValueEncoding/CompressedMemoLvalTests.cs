namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Round-trip tests for Memo values that use the Jet4 UCS-2 → ASCII
/// compression path. When a Memo string contains only Latin-1 characters,
/// Access stores the encoded payload with 1 byte per character (compressed
/// form) instead of 2 bytes per character (raw UCS-2 LE). This compression
/// interacts with the LVAL code path — previously only non-Latin-1 text
/// (which forces the uncompressed 2 bytes/char path) was tested over LVAL.
/// Closes §2.1 gap: "Compressed (UCS-2 → ASCII) Memo prefixes longer
/// than 1 inline page.".
/// </summary>
public sealed class CompressedMemoLvalTests
{
    /// <summary>
    /// A Latin-1 Memo that fits inline (compressed to ~50 bytes) round-trips.
    /// </summary>
    [Fact]
    public async Task Memo_ShortLatin1_RoundTripsCompressed()
    {
        string memoValue = new string('A', 50);
        await AssertMemoRoundTripAsync(memoValue);
    }

    /// <summary>
    /// A Latin-1 Memo that exceeds the inline cap when encoded as UCS-2 but
    /// compresses to fit inline (1 byte/char) still round-trips. 600 Latin-1
    /// chars → 600 compressed bytes (under 1024-byte inline cap).
    /// </summary>
    [Fact]
    public async Task Memo_MediumLatin1Compressible_FitsInlineAndRoundTrips()
    {
        // 600 Latin-1 chars: uncompressed = 1200 UCS-2 bytes (would exceed
        // inline cap), but Jet4 compression → 600 bytes (fits inline).
        string memoValue = new string('Z', 600);
        await AssertMemoRoundTripAsync(memoValue);
    }

    /// <summary>
    /// A Latin-1 Memo longer than 1024 bytes even compressed forces a
    /// single-page LVAL (bitmask 0x40) and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Memo_LargeLatin1_ForcesLvalAndRoundTrips()
    {
        // 1500 Latin-1 chars → 1500 compressed bytes > 1024-byte inline cap
        // → single-page LVAL form.
        string memoValue = new string('X', 1500);
        await AssertMemoRoundTripAsync(memoValue);
    }

    /// <summary>
    /// A very large Latin-1 Memo forces the chained LVAL path (bitmask 0x00)
    /// and round-trips correctly. This is the specific gap case: compressed
    /// Memo prefixes longer than 1 inline page.
    /// </summary>
    [Fact]
    public async Task Memo_VeryLargeLatin1_ChainedLvalRoundTrips()
    {
        // 5000 Latin-1 chars → 5000 compressed bytes > single LVAL page cap
        // → chained LVAL pages.
        string memoValue = new string('Q', 5000);
        await AssertMemoRoundTripAsync(memoValue);
    }

    /// <summary>
    /// Mixed content: starts with Latin-1 but has non-Latin-1 characters
    /// interspersed, forcing the uncompressed path at the LVAL boundary.
    /// </summary>
    [Fact]
    public async Task Memo_MixedLatin1AndUnicode_LvalRoundTrips()
    {
        // Mix Latin-1 with occasional non-Latin-1 to create a string that
        // cannot be compressed but is large enough for LVAL.
        char[] chars = new char[2000];
        for (int i = 0; i < chars.Length; i++)
        {
            chars[i] = (i % 10 == 0) ? '\u4E2D' : 'A';
        }

        string memoValue = new string(chars);
        await AssertMemoRoundTripAsync(memoValue);
    }

    private static async Task AssertMemoRoundTripAsync(string memoValue)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "MemoTest",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Content", typeof(string)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "MemoTest",
                [1, memoValue],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "MemoTest",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, dt.Rows.Count);
        string actual = Assert.IsType<string>(dt.Rows[0]["Content"]);
        Assert.Equal(memoValue.Length, actual.Length);
        Assert.Equal(memoValue, actual);
    }

    /// <summary>
    /// Verifies that ASCII-range inline text is stored in Jet4 compressed
    /// unicode form (FF FE + 1 byte per char) and NOT in raw UCS-2.
    /// </summary>
    [Fact]
    public async Task InlineText_AsciiRange_IsWrittenAsCompressedUnicode()
    {
        const string sentinel = "ZZUNIQUEWRITERSENTINELZZ";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new("Id", typeof(int)),
                    new("Txt", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, sentinel], TestContext.Current.CancellationToken);
        }

        byte[] bytes = ms.ToArray();
        byte[] ucs2 = System.Text.Encoding.Unicode.GetBytes(sentinel);
        byte[] ascii = System.Text.Encoding.ASCII.GetBytes(sentinel);

        byte[] compressed = new byte[ascii.Length + 2];
        compressed[0] = 0xFF;
        compressed[1] = 0xFE;
        Buffer.BlockCopy(ascii, 0, compressed, 2, ascii.Length);

        Assert.True(
            IndexOf(bytes, compressed) >= 0,
            "Expected the sentinel string to be stored in JET4 compressed-unicode form (FF FE + 1-byte).");

        Assert.True(
            IndexOf(bytes, ucs2) < 0,
            "Sentinel must NOT appear in plain UCS-2 form when compression is applicable.");

        // Round-trip: the reader should still see the original string.
        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        var dt = await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(dt);
        Assert.Equal(sentinel, dt!.Rows[0]["Txt"]);
    }

    /// <summary>
    /// Verifies that a Latin-1 Memo stored via LVAL (exceeds inline cap) is
    /// written in compressed unicode form on disk, not raw UCS-2.
    /// </summary>
    [Fact]
    public async Task Memo_LargeLatin1_LvalPayloadIsCompressedOnDisk()
    {
        // 1500 Latin-1 chars → compressed = 1500 bytes (exceeds inline cap),
        // so data lives in an LVAL page. The raw bytes should contain the
        // FF FE compressed-unicode marker followed by single-byte characters,
        // and must NOT contain the UCS-2 form.
        const string sentinel = "LVALCOMPRESSED";
        string memoValue = new string('K', 1480) + sentinel; // 1494 chars

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "LvalComp",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Content", typeof(string)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("LvalComp", [1, memoValue], TestContext.Current.CancellationToken);
        }

        byte[] bytes = ms.ToArray();
        byte[] ucs2Sentinel = System.Text.Encoding.Unicode.GetBytes(sentinel);
        byte[] asciiSentinel = System.Text.Encoding.ASCII.GetBytes(sentinel);

        // The compressed form must appear somewhere in the LVAL page chain.
        Assert.True(
            IndexOf(bytes, asciiSentinel) >= 0,
            "Expected Latin-1 LVAL content stored in 1-byte compressed form.");

        // The UCS-2 expansion of the sentinel must NOT appear.
        Assert.True(
            IndexOf(bytes, ucs2Sentinel) < 0,
            "LVAL payload must NOT appear in raw UCS-2 when the content is compressible.");

        // Round-trip the value.
        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync("LvalComp", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(memoValue, Assert.IsType<string>(dt.Rows[0]["Content"]));
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
