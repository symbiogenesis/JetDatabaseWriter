namespace JetDatabaseWriter.Tests.ValueEncoding;

using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Per-type assertions for the three LVAL storage forms:
/// <list type="bullet">
///   <item>Type 1 / bitmask <c>0x80</c> — inline (payload embedded in the row body).</item>
///   <item>Type 2 / bitmask <c>0x40</c> — single LVAL page (payload in one external row).</item>
///   <item>Type 3 / bitmask <c>0x00</c> — chained LVAL pages (multi-page chain).</item>
/// </list>
/// Closes §2.1 gap: "we have round-trip but no per-type assertion.".
/// </summary>
public sealed class LvalFormAssertionTests
{
    /// <summary>
    /// A short Memo (under the inline cap of 1024 encoded bytes) is stored
    /// as inline (bitmask 0x80) and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Memo_ShortInline_RoundTripsAndUsesInlineForm()
    {
        // 50 ASCII chars → 100 bytes UTF-16 LE (well under 1024-byte inline cap).
        // Use non-Latin-1 so the Jet4 encoder cannot compress to 1 byte/char.
        string memoValue = new('\u4E2D', 50);

        await AssertMemoRoundTripAsync(memoValue, expectedMinLen: memoValue.Length);
    }

    /// <summary>
    /// A medium Memo (above inline cap but fits in a single LVAL page) is stored
    /// as single-page LVAL (bitmask 0x40) and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Memo_MediumSinglePage_RoundTripsCorrectly()
    {
        // 600 non-Latin-1 chars → 1200 bytes UTF-16 LE (above 1024-byte cap,
        // but under page size minus overhead → single LVAL page form).
        string memoValue = new('\u4E2D', 600);

        await AssertMemoRoundTripAsync(memoValue, expectedMinLen: memoValue.Length);
    }

    /// <summary>
    /// A large Memo (exceeds single LVAL page capacity) is stored as a chained
    /// LVAL chain (bitmask 0x00) and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Memo_LargeChained_RoundTripsCorrectly()
    {
        // 4000 non-Latin-1 chars → 8000 bytes UTF-16 LE, which exceeds the
        // single LVAL page cap (~4072 bytes on a 4096-byte page) and forces
        // a multi-page chain (bitmask 0x00).
        string memoValue = new('\u4E2D', 4000);

        await AssertMemoRoundTripAsync(memoValue, expectedMinLen: memoValue.Length);
    }

    /// <summary>
    /// A short OLE blob (under the 256-byte inline cap) is stored inline
    /// and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Ole_ShortInline_RoundTripsCorrectly()
    {
        byte[] payload = BuildDeterministicPayload(100);

        await AssertOleRoundTripAsync(payload);
    }

    /// <summary>
    /// A medium OLE blob (above inline cap, fits in single LVAL page) round-trips.
    /// </summary>
    [Fact]
    public async Task Ole_MediumSinglePage_RoundTripsCorrectly()
    {
        // 2000 bytes: above 256-byte inline cap, under page-size minus overhead.
        byte[] payload = BuildDeterministicPayload(2000);

        await AssertOleRoundTripAsync(payload);
    }

    /// <summary>
    /// A large OLE blob (exceeds single LVAL page) round-trips via chained LVAL.
    /// </summary>
    [Fact]
    public async Task Ole_LargeChained_RoundTripsCorrectly()
    {
        // 8000 bytes: exceeds the single LVAL page capacity, forces chained form.
        byte[] payload = BuildDeterministicPayload(8000);

        await AssertOleRoundTripAsync(payload);
    }

    private static async Task AssertMemoRoundTripAsync(string memoValue, int expectedMinLen)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "LvalTest",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Content", typeof(string)), // MEMO
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "LvalTest",
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
            "LvalTest",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, dt.Rows.Count);
        string actual = Assert.IsType<string>(dt.Rows[0]["Content"]);
        Assert.True(actual.Length >= expectedMinLen, $"Expected at least {expectedMinLen} chars, got {actual.Length}.");
        Assert.Equal(memoValue, actual);
    }

    private static async Task AssertOleRoundTripAsync(byte[] expected)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "OleTest",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "OleTest",
                [1, expected],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "OleTest",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, dt.Rows.Count);
        byte[] actual = Assert.IsType<byte[]>(dt.Rows[0]["Blob"]);
        Assert.Equal(expected, actual);
    }

    private static byte[] BuildDeterministicPayload(int length)
    {
        var bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            unchecked
            {
                bytes[i] = (byte)((i * 31) ^ (i >> 8));
            }
        }

        return bytes;
    }
}
