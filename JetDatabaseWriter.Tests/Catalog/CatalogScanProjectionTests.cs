namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// The catalog's own scans of <c>MSysObjects</c> and <c>MSysComplexColumns</c>
/// decode only the columns they read, so they never decode, or read the LVAL
/// pages of, the <c>LvProp</c>, <c>LvModule</c> and <c>LvExtra</c> blobs.
/// </summary>
public sealed class CatalogScanProjectionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Listing linked tables reads only <c>MSysObjects</c>' data pages, not the
    /// LVAL pages that hold its larger <c>LvProp</c> blobs. NorthwindTraders has
    /// no linked table, so no LVAL page holds a value the listing reads (a link's
    /// <c>Database</c> or <c>Connect</c> memo can be on one).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ListLinkedTables_ReadsNoLongValuePages()
    {
        await using MemoryStream file = await CopyFixtureAsync(TestDatabases.NorthwindTraders);
        await using var counting = new CountingStream(file);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, cancellationToken: Ct);
        _ = await reader.ListTablesAsync(Ct);
        counting.Reset();

        _ = await reader.ListLinkedTablesAsync(Ct);

        Assert.Empty(LongValuePagesRead(file, counting));
    }

    /// <summary>
    /// Resolving a table's complex columns looks up the flat tables' names in
    /// <c>MSysObjects</c> by Id and Name only, without reading LVAL pages.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetComplexColumns_ReadsNoLongValuePages()
    {
        await using MemoryStream file = await CopyFixtureAsync(TestDatabases.NorthwindTraders);
        await using var counting = new CountingStream(file);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, cancellationToken: Ct);
        _ = await reader.ListTablesAsync(Ct);
        counting.Reset();

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync("Employees", Ct);

        Assert.NotEmpty(complex);
        Assert.Empty(LongValuePagesRead(file, counting));
    }

    /// <summary>
    /// A string scan that leaves a MEMO column out never decodes it: the wanted
    /// columns come back and the MEMO is an empty string, even where its LVAL
    /// page is damaged, with no placeholder text.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EnumerateRowsForTdef_ProjectedOutMemoColumn_IsNotDecoded(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateDatabaseWithUnreadableMemoAsync(format);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("T", Ct));
        TableDef td = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, Ct));
        var decoder = new RowDecoder(harness.Database.Format, harness.Database.OwnedPages, harness.Services.PageCache, new LongValueDecoder(harness.Database.Format, harness.Services.PageCache), strictParsing: true);

        var rows = new List<string[]>();
        await foreach (string[] row in decoder.EnumerateRowsForTdefAsync(entry.TDefPage, td, ["Id", "note"], Ct))
        {
            rows.Add(row);
        }

        Assert.Equal(["1|a|", "2|b|"], rows.Select(r => string.Join("|", r)).Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// The linked-table listing, which now decodes six <c>MSysObjects</c>
    /// columns, returns what a decode of every column gives.
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("LinkerTestV2007")]
    [InlineData("OdbcLinkerTestV2007")]
    public async Task ListLinkedTables_WithProjection_MatchesFixtureMetadata(string fixture)
    {
        string path = fixture == "LinkerTestV2007" ? TestDatabases.LinkerTestV2007 : TestDatabases.OdbcLinkerTestV2007;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(path, cancellationToken: Ct);
        TableDef msys = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(2, Ct));
        int name = msys.FindColumnIndex("Name");
        int type = msys.FindColumnIndex("Type");
        int flags = msys.FindColumnIndex("Flags");
        int database = msys.FindColumnIndex("Database");
        int foreignName = msys.FindColumnIndex("ForeignName");
        int connect = msys.FindColumnIndex("Connect");
        var expected = new Dictionary<string, string[]>(StringComparer.Ordinal);
        await foreach (string[] row in harness.Services.Catalog.EnumerateMSysObjectsRowsAsync(msys, Ct))
        {
            bool system = long.TryParse(row[flags], NumberStyles.Integer, CultureInfo.InvariantCulture, out long f) && (unchecked((uint)f) & 0x80000002U) != 0;
            if (row[type] is "4" or "6" && !system && row[name].Length > 0)
            {
                expected.Add(row[name], row);
            }
        }

        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        IReadOnlyList<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(Ct);

        Assert.NotEmpty(linked);
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), linked.Select(l => l.Name).Order(StringComparer.Ordinal));
        foreach (LinkedTableInfo link in linked)
        {
            string[] row = expected[link.Name];
            Assert.Equal(row[connect].Length == 0 ? null : row[connect], link.ConnectString);
            if (link.Kind != LinkedTableKind.Text)
            {
                Assert.Equal(row[foreignName], link.SourceObjectName);
            }

            if (link.Kind != LinkedTableKind.Odbc)
            {
                Assert.Equal(row[database].Length == 0 ? null : row[database], link.SourcePath);
            }
        }
    }

    /// <summary>
    /// Creates <c>T</c> (Id, Note, Body MEMO) with row 1's MEMO on an LVAL page
    /// and row 2's inline, then breaks the LVAL page's page-type byte.
    /// </summary>
    /// <param name="format">The database format.</param>
    private static async Task<MemoryStream> CreateDatabaseWithUnreadableMemoAsync(DatabaseFormat format)
    {
        const string marker = "UNREADABLEMEMOMARKER";
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Note", typeof(string), maxLength: 50), new("Body", typeof(string))], Ct);
            await writer.InsertRowsAsync("T", [[1, "a", marker + new string('q', 1500)], [2, "b", "short memo"]], Ct);
        }

        byte[] file = ms.ToArray();
        int offset = file.AsSpan().IndexOf(System.Text.Encoding.Unicode.GetBytes(marker));
        if (offset < 0)
        {
            offset = file.AsSpan().IndexOf(System.Text.Encoding.ASCII.GetBytes(marker));
        }

        Assert.True(offset >= 0, "The MEMO's LVAL page was not found.");
        int lvalPageStart = offset / 4096 * 4096;
        Assert.Equal(0x01, file[lvalPageStart]);
        ms.Position = lvalPageStart;
        ms.WriteByte(0x7F);
        ms.Position = 0;
        return ms;
    }

    private static List<long> LongValuePagesRead(MemoryStream file, CountingStream counting)
    {
        const int pageSize = 4096;
        byte[] bytes = file.ToArray();
        return [.. counting.PagesRead(pageSize).Where(p => (p + 1) * pageSize <= bytes.Length && IsLvalPage(bytes.AsSpan(checked((int)(p * pageSize)), pageSize))).Order()];
    }

    private static bool IsLvalPage(ReadOnlySpan<byte> page)
        => page[0] == 0x01 && page[4] == (byte)'L' && page[5] == (byte)'V' && page[6] == (byte)'A' && page[7] == (byte)'L';

    private static async Task<MemoryStream> CopyFixtureAsync(string path)
    {
        var ms = new MemoryStream();
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        await ms.WriteAsync(bytes, Ct);
        ms.Position = 0;
        return ms;
    }
}
