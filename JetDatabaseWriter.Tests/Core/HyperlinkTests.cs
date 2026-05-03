namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201

/// <summary>
/// Positive coverage for Hyperlink columns — the format-flag bit
/// (<c>HYPERLINK_FLAG_MASK = 0x80</c>) plus the <c>displaytext # address #
/// subaddress # screentip</c> value layer. See
/// <c>docs/design/hyperlink-format-notes.md</c>.
/// </summary>
public sealed class HyperlinkTests
{
    private sealed class HyperlinkRow
    {
        public int Id { get; set; }

        public Hyperlink? Link { get; set; }
    }

    private sealed class StringRow
    {
        public int Id { get; set; }

        public string? Link { get; set; }
    }

    [Theory]
    [InlineData("https://example.com", "", "https://example.com", "", "")]
    [InlineData("Docs#https://example.com", "Docs", "https://example.com", "", "")]
    [InlineData("Docs#https://example.com#intro", "Docs", "https://example.com", "intro", "")]
    [InlineData("Docs#https://example.com#intro#tooltip", "Docs", "https://example.com", "intro", "tooltip")]
    [InlineData("Click##anchor#", "Click", "", "anchor", "")]
    [InlineData("##anchor", "", "", "anchor", "")]
    public void Parse_DecomposesAccessFormat(string raw, string display, string address, string sub, string tip)
    {
        Hyperlink? h = Hyperlink.Parse(raw);
        Assert.NotNull(h);
        Assert.Equal(display, h!.DisplayText);
        Assert.Equal(address, h.Address);
        Assert.Equal(sub, h.SubAddress);
        Assert.Equal(tip, h.ScreenTip);
    }

    [Fact]
    public void Parse_NullOrEmpty_ReturnsNull()
    {
        Assert.Null(Hyperlink.Parse(null));
        Assert.Null(Hyperlink.Parse(string.Empty));
    }

    [Fact]
    public void ToString_OmitsTrailingEmptyParts()
    {
        Assert.Equal("Docs#https://example.com", new Hyperlink("Docs", "https://example.com").ToString());
        Assert.Equal("Docs#https://example.com#intro", new Hyperlink("Docs", "https://example.com", "intro").ToString());
        Assert.Equal("Docs#https://example.com#intro#tip", new Hyperlink("Docs", "https://example.com", "intro", "tip").ToString());
    }

    [Fact]
    public void ToString_EscapesEmbeddedHashes()
    {
        var h = new Hyperlink("a#b", "https://x/y#z", "frag#1", "tip#2");
        Assert.Equal("a%23b#https://x/y%23z#frag%231#tip%232", h.ToString());

        Hyperlink? round = Hyperlink.Parse(h.ToString());
        Assert.Equal(h, round);
    }

    [Fact]
    public async Task Writer_TypedHyperlinkColumn_SetsFlagAndRoundTripsValue()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"HL_{Guid.NewGuid():N}".Substring(0, 16);

        var link = new Hyperlink("Docs", "https://example.com/docs", "intro", "Hover tip");

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(Hyperlink)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, link], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);

        var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        Assert.True(meta[1].IsHyperlink);
        Assert.Equal(typeof(Hyperlink), meta[1].ClrType);
        Assert.Equal("Hyperlink", meta[1].TypeName);

        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(typeof(Hyperlink), dt.Columns["Link"]!.DataType);

        var actual = Assert.IsType<Hyperlink>(dt.Rows[0]["Link"]);
        Assert.Equal(link, actual);
    }

    [Fact]
    public async Task Writer_IsHyperlinkOnStringColumn_ProducesSameOnDiskShape()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"HL_{Guid.NewGuid():N}".Substring(0, 16);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(string)) { IsHyperlink = true },
                ],
                TestContext.Current.CancellationToken);

            // Plain string value also accepted — pre-encoded hyperlink form.
            await writer.InsertRowAsync(
                tableName,
                [1, "Site#https://example.com#anchor"],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        Assert.True(meta[1].IsHyperlink);

        await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            var hl = Assert.IsType<Hyperlink>(row[1]);
            Assert.Equal("Site", hl.DisplayText);
            Assert.Equal("https://example.com", hl.Address);
            Assert.Equal("anchor", hl.SubAddress);
        }
    }

    [Fact]
    public async Task Writer_IsHyperlinkOnNonMemoColumn_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "Bad",
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(string), maxLength: 50) { IsHyperlink = true },
                ],
                TestContext.Current.CancellationToken));

        Assert.Contains("hyperlink", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Reader_PocoMapping_BindsHyperlinkAndStringPropertiesBothWays()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"HL_{Guid.NewGuid():N}".Substring(0, 16);
        var link = new Hyperlink("Site", "https://example.com", "x", "y");

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(Hyperlink)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, link], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);

        List<HyperlinkRow> typed = await reader.ReadTableAsync<HyperlinkRow>(tableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(link, typed[0].Link);

        List<StringRow> asStrings = await reader.ReadTableAsync<StringRow>(tableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(link.ToString(), asStrings[0].Link);
    }

    [Fact]
    public async Task Reader_RowsAsStrings_ReturnsRawEncodedFormForHyperlinkColumn()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"HL_{Guid.NewGuid():N}".Substring(0, 16);
        var link = new Hyperlink("Site", "https://example.com", "x");

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(Hyperlink)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, link], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        await foreach (string[] row in reader.RowsAsStrings(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(link.ToString(), row[1]);
        }
    }

    /// <summary>
    /// Round-trips a Hyperlink with all four parts populated near the
    /// per-part length Access exposes through its UI (~2048 chars/part).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Writer_HyperlinkAllFourPartsAtLargeLength_RoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"HL_{Guid.NewGuid():N}".Substring(0, 16);

        // 1024 chars per part stays well under any single-part Access limit
        // and produces a UCS-2 payload (~8 KB) that comfortably exceeds the
        // inline-memo cap, exercising the LVAL chain on a Hyperlink column.
        string display = new('D', 1024);
        string address = "https://example.com/" + new string('a', 1004);
        string sub = new('s', 1024);
        string tip = new('t', 1024);
        var link = new Hyperlink(display, address, sub, tip);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(Hyperlink)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, link], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
        var actual = Assert.IsType<Hyperlink>(dt.Rows[0]["Link"]);
        Assert.Equal(link, actual);
        Assert.Equal(display, actual.DisplayText);
        Assert.Equal(address, actual.Address);
        Assert.Equal(sub, actual.SubAddress);
        Assert.Equal(tip, actual.ScreenTip);
    }

    /// <summary>
    /// Round-trips a Hyperlink whose URL contains literal <c>#</c>
    /// characters. The on-disk encoding splits parts on <c>#</c>, so the
    /// writer must escape embedded hashes (Jackcess uses <c>%23</c>) before
    /// emit. Closes the §2.5 "embedded `#` literal in the URL" gap.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Writer_HyperlinkWithEmbeddedHashInEveryPart_RoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"HL_{Guid.NewGuid():N}".Substring(0, 16);

        var link = new Hyperlink(
            displayText: "Title # with hash",
            address: "https://example.com/path#section/q?x=1#y=2",
            subAddress: "frag#1",
            screenTip: "tip#with#hashes");

        // Self-check: the on-disk form must not contain raw '#' inside any
        // single part — only the three part separators.
        string encoded = link.ToString();
        Assert.Equal(3, encoded.Split('#').Length - 1);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(Hyperlink)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, link], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
        var actual = Assert.IsType<Hyperlink>(dt.Rows[0]["Link"]);
        Assert.Equal(link, actual);
        Assert.Equal("https://example.com/path#section/q?x=1#y=2", actual.Address);
        Assert.Equal("tip#with#hashes", actual.ScreenTip);
    }

    [Fact]
    public async Task SchemaEvolution_AddRenameColumn_PreservesHyperlinkFlag()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"HL_{Guid.NewGuid():N}".Substring(0, 16);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Other", typeof(string), maxLength: 32),
                ],
                TestContext.Current.CancellationToken);

            await writer.AddColumnAsync(
                tableName,
                new ColumnDefinition("Link", typeof(Hyperlink)),
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                tableName,
                [1, "x", new Hyperlink("D", "https://x", "s")],
                TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(tableName, "Link", "Url", TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        ColumnMetadata urlCol = meta[2];
        Assert.Equal("Url", urlCol.Name);
        Assert.True(urlCol.IsHyperlink);
        Assert.Equal(typeof(Hyperlink), urlCol.ClrType);
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            // empty
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
