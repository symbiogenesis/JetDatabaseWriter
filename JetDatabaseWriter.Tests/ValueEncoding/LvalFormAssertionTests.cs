namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
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
    /// A short Memo (under the inline cap of 64 encoded bytes) is stored
    /// as inline (bitmask 0x80) and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Memo_ShortInline_RoundTripsAndUsesInlineForm()
    {
        // 32 non-Latin-1 chars → 64 bytes UTF-16 LE (the native inline limit).
        // Use non-Latin-1 so the Jet4 encoder cannot compress to 1 byte/char.
        string memoValue = new('\u4E2D', 32);

        await AssertMemoRoundTripAsync(memoValue, expectedMinLen: memoValue.Length, expectedStorageMode: Constants.LongValue.InlineStorageMode);
    }

    /// <summary>
    /// A medium Memo (above inline cap but fits in a single LVAL page) is stored
    /// as single-page LVAL (bitmask 0x40) and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Memo_MediumSinglePage_RoundTripsCorrectly()
    {
        // 600 non-Latin-1 chars → 1200 bytes UTF-16 LE (above 64-byte cap,
        // but under page size minus overhead → single LVAL page form).
        string memoValue = new('\u4E2D', 600);

        await AssertMemoRoundTripAsync(memoValue, expectedMinLen: memoValue.Length, expectedStorageMode: Constants.LongValue.SinglePageStorageMode);
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

        await AssertMemoRoundTripAsync(memoValue, expectedMinLen: memoValue.Length, expectedStorageMode: Constants.LongValue.ChainedStorageMode);
    }

    /// <summary>
    /// A short OLE blob (under the 64-byte inline cap) is stored inline
    /// and round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Ole_ShortInline_RoundTripsCorrectly()
    {
        byte[] payload = BuildDeterministicPayload(64);

        await AssertOleRoundTripAsync(payload, Constants.LongValue.InlineStorageMode);
    }

    /// <summary>
    /// A medium OLE blob (above inline cap, fits in single LVAL page) round-trips.
    /// </summary>
    [Fact]
    public async Task Ole_MediumSinglePage_RoundTripsCorrectly()
    {
        // 2000 bytes: above 64-byte inline cap, under page-size minus overhead.
        byte[] payload = BuildDeterministicPayload(2000);

        await AssertOleRoundTripAsync(payload, Constants.LongValue.SinglePageStorageMode);
    }

    /// <summary>
    /// A large OLE blob (exceeds single LVAL page) round-trips via chained LVAL.
    /// </summary>
    [Fact]
    public async Task Ole_LargeChained_RoundTripsCorrectly()
    {
        // 8000 bytes: exceeds the single LVAL page capacity, forces chained form.
        byte[] payload = BuildDeterministicPayload(8000);

        await AssertOleRoundTripAsync(payload, Constants.LongValue.ChainedStorageMode);
    }

    internal static async Task AssertStorageModeAsync(MemoryStream stream, string tableName, byte expectedStorageMode)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        var entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken));
        DataPageLayout layout = harness.Database.Format.DataPage;
        var modes = new List<byte>();
        for (long pageNumber = 1; pageNumber < harness.Database.Pages.PageCount; pageNumber++)
        {
            byte[] page = await harness.ReadPageCopyAsync(pageNumber, TestContext.Current.CancellationToken);
            if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, layout.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            foreach (RowBound bound in DataPageRows.EnumerateLiveRowBounds(harness.Database.Format, page))
            {
                // These two-column test tables place one Int32 before their MEMO/OLE slot.
                int valueStart = bound.RowStart + harness.Database.Format.RowFields.NumCols + sizeof(int);
                Assert.True(LongValueDescriptor.TryRead(page.AsSpan(valueStart, Constants.LongValue.HeaderSize), out LongValueDescriptor descriptor));
                modes.Add(descriptor.StorageMode);
            }
        }

        Assert.Equal(expectedStorageMode, Assert.Single(modes));
    }

    private static async Task AssertMemoRoundTripAsync(string memoValue, int expectedMinLen, byte expectedStorageMode)
    {
        var ms = new MemoryStream();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
        await using AccessReader reader = await AccessReader.OpenAsync(
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
        await AssertStorageModeAsync(ms, "LvalTest", expectedStorageMode);
    }

    private static async Task AssertOleRoundTripAsync(byte[] expected, byte expectedStorageMode)
    {
        var ms = new MemoryStream();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
        await using AccessReader reader = await AccessReader.OpenAsync(
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
        await AssertStorageModeAsync(ms, "OleTest", expectedStorageMode);
    }

    private static byte[] BuildDeterministicPayload(int length)
    {
        byte[] bytes = new byte[length];
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
