namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Overflow rows: when a row outgrows its page, Access moves its bytes to a slot
/// flagged deleted on another page of the same table and turns the original slot
/// into a 4-byte pointer flagged <c>0x4000</c> (Jackcess
/// <c>TableImpl.positionAtRowData</c>). Every read follows the pointer, so the row
/// is read once, at its original position.
/// </summary>
/// <param name="seeds">The database cache fixture.</param>
public sealed class OverflowRowTests(GeneratedDatabaseCache seeds) : IClassFixture<GeneratedDatabaseCache>
{
    private const string TableName = "Items";
    private const int MinimumRows = 300;

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    public static TheoryData<string, PageReadOptimizationMode> OverflowFixtures
    {
        get
        {
            var data = new TheoryData<string, PageReadOptimizationMode>();
            foreach (string path in new[]
            {
                TestDatabases.OverflowTestV1997,
                TestDatabases.OverflowTestV2000,
                TestDatabases.OverflowTestV2003,
                TestDatabases.OverflowTestV2007,
                TestDatabases.OverflowTestV2010,
            })
            {
                data.Add(path, PageReadOptimizationMode.Disabled);
                data.Add(path, PageReadOptimizationMode.Enabled);
            }

            return data;
        }
    }

    public static TheoryData<DatabaseFormat, OverflowRowLayout, PageReadOptimizationMode> ValidLayouts => Combine(
        OverflowRowLayout.CrossPage,
        OverflowRowLayout.SamePage,
        OverflowRowLayout.TwoHop);

    public static TheoryData<DatabaseFormat, OverflowRowLayout, PageReadOptimizationMode> CorruptLayouts => Combine(
        OverflowRowLayout.PointerPastEndOfFile,
        OverflowRowLayout.PointerToOtherTable,
        OverflowRowLayout.ShortPointer,
        OverflowRowLayout.Cycle);

    /// <summary>
    /// The Jackcess <c>overflowTest</c> fixtures hold 7 rows in <c>Table1</c>, rows 3
    /// and 5 overflow rows (Jackcess <c>DatabaseTest.testOverflow</c>).
    /// </summary>
    /// <param name="path">The fixture path.</param>
    /// <param name="readMode">The table-scan read-ahead mode.</param>
    [Theory]
    [MemberData(nameof(OverflowFixtures))]
    public async Task OverflowFixture_ReadsEveryRowWithJackcessValues(string path, PageReadOptimizationMode readMode)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false, PageReadOptimizationMode = readMode },
            this.ct);

        using DataTable data = await reader.ReadTableAsync("Table1", cancellationToken: this.ct);
        List<object[]> rows = await reader.Rows("Table1", cancellationToken: this.ct).ToListAsync(this.ct);
        List<string[]> stringRows = await reader.RowsAsStrings("Table1", cancellationToken: this.ct).ToListAsync(this.ct);
        using DataTable strings = await reader.ReadTableAsStringsAsync("Table1", cancellationToken: this.ct);
        long realCount = await reader.GetRealRowCountAsync("Table1", this.ct);

        Assert.Equal(7, data.Rows.Count);
        Assert.Equal(7, rows.Count);
        Assert.Equal(7, stringRows.Count);
        Assert.Equal(7, strings.Rows.Count);
        Assert.Equal(7, realCount);
        Assert.Equal<object?>([null, "row3col3", null, null, null, null, null, "row3col9", null], rows[2].Select(v => v is DBNull ? null : v));
        Assert.Equal<object?>([null, "row5col2", null, null, null, null, null, null, null], rows[4].Select(v => v is DBNull ? null : v));
        Assert.Equal("row3col9", data.Rows[2][7]);
        Assert.Equal("row5col2", strings.Rows[4][1]);
    }

    /// <summary>
    /// Index entries of an overflow row name its header slot, so a seek through
    /// any index finds every row, overflow rows included.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    /// <param name="expectedRows">The number of rows <c>Table1</c> holds.</param>
    [Theory]
    [InlineData(nameof(TestDatabases.TestOleV2007), 11)]
    [InlineData(nameof(TestDatabases.ExtDateTestV2019), 9)]
    public async Task OverflowRows_IndexSeek_FindsEveryRow(string path, int expectedRows)
    {
        path = (string)typeof(TestDatabases).GetField(path)!.GetValue(null)!;
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: this.ct);

        using DataTable data = await reader.ReadTableAsync("Table1", cancellationToken: this.ct);
        Assert.Equal(expectedRows, data.Rows.Count);

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Table1", this.ct);
        IndexMetadata primaryKey = indexes.Single(i => i.Kind == IndexKind.PrimaryKey);
        int seekableIndexes = 0;
        foreach (IndexMetadata index in indexes.Where(i => i.Columns.Count == 1 && !i.IsForeignKey))
        {
            string column = index.Columns[0].Name;
            if (data.Columns[column] is not { } dataColumn || dataColumn.DataType == typeof(byte[]))
            {
                continue;
            }

            seekableIndexes++;
            foreach (DataRow row in data.Rows)
            {
                if (row[column] is DBNull)
                {
                    continue;
                }

                List<object[]> hits = await reader.SeekRowsAsync("Table1", index.Name, [row[column]], this.ct).ToListAsync(this.ct);
                Assert.True(
                    index == primaryKey ? hits.Count == 1 : hits.Count >= 1,
                    $"Seeking {row[column]} through '{index.Name}' found {hits.Count} rows.");
            }
        }

        Assert.True(seekableIndexes >= 1, "Expected at least one seekable index.");
    }

    /// <summary>
    /// A synthetic overflow row on a writer-created table is read once, in place,
    /// through every read API.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">Where the moved row data lands.</param>
    /// <param name="readMode">The table-scan read-ahead mode.</param>
    [Theory]
    [MemberData(nameof(ValidLayouts))]
    public async Task SyntheticOverflowRow_IsReadOnce(DatabaseFormat format, OverflowRowLayout layout, PageReadOptimizationMode readMode)
    {
        byte[] bytes = await this.CopySeedAsync(format);
        List<int> expectedIds = await this.ReadIdsAsync(bytes, readMode);

        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, layout, this.ct);

        Assert.Equal(expectedIds, await this.ReadIdsAsync(bytes, readMode));
        Assert.All(await this.CountThroughReadApisAsync(bytes, readMode), pair => Assert.True(pair.Value == expectedIds.Count, $"{pair.Key} returned {pair.Value} rows; expected {expectedIds.Count}."));
    }

    /// <summary>
    /// An overflow pointer that cannot be resolved refuses strict reads before an incomplete result succeeds.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">How the pointer is corrupt.</param>
    /// <param name="readMode">The table-scan read-ahead mode.</param>
    [Theory]
    [MemberData(nameof(CorruptLayouts))]
    public async Task CorruptOverflowPointer_RefusesStrictReads(DatabaseFormat format, OverflowRowLayout layout, PageReadOptimizationMode readMode)
    {
        byte[] bytes = await this.CopySeedAsync(format);
        List<int> originalIds = await this.ReadIdsAsync(bytes, readMode);

        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, layout, this.ct);

        await this.AssertStrictPublicReadsAsync(bytes, readMode);
        Assert.True(originalIds.Count > 0);
    }

    /// <summary>Explicitly lenient reads report omissions even when verbose diagnostics are disabled.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">The corrupt pointer layout.</param>
    /// <param name="readMode">The scan read mode.</param>
    [Theory]
    [MemberData(nameof(CorruptLayouts))]
    public async Task CorruptOverflowPointer_LenientReadsAlwaysReportOmissions(DatabaseFormat format, OverflowRowLayout layout, PageReadOptimizationMode readMode)
    {
        byte[] bytes = await this.CopySeedAsync(format);
        int originalCount = (await this.ReadIdsAsync(bytes, readMode)).Count;
        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, layout, this.ct);
        await using var messages = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            for (int pass = 0; pass < 2; pass++)
            {
                bool enabled = pass != 0;
                messages.GetStringBuilder().Clear();
                await using var stream = new MemoryStream(bytes, writable: false);
                await using AccessReader reader = await AccessReader.OpenAsync(
                    stream,
                    new AccessReaderOptions { UseLockFile = false, StrictParsing = false, DiagnosticsEnabled = enabled, PageReadOptimizationMode = readMode },
                    leaveOpen: true,
                    this.ct);
                using DataTable rows = await reader.ReadTableAsync(TableName, cancellationToken: this.ct);
                Assert.Equal(originalCount - 1, rows.Rows.Count);
                Assert.Equal(1, reader.OmittedRowCount);
                listener.Flush();
                Assert.Contains("table result is incomplete", messages.ToString(), StringComparison.Ordinal);
                Assert.Equal(enabled, messages.ToString().Contains("Skipped corrupt overflow row", StringComparison.Ordinal));
                if (enabled)
                {
                    Assert.Contains("table definition page", messages.ToString(), StringComparison.Ordinal);
                    Assert.Contains("header slot", messages.ToString(), StringComparison.Ordinal);
                }
            }
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    /// <summary>Malformed trailers refuse every complete public row read and do not cache partial counts.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MalformedRowLayout_RefusesAllPublicReads(DatabaseFormat format)
    {
        byte[] bytes = await this.CopySeedAsync(format);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, OverflowRowLayout.CrossPage, this.ct);
        int pageSize = format == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int offset = checked((int)(moved.DataPage * pageSize)) + moved.DataStart;
        bytes[offset] = 0;
        if (format != DatabaseFormat.Jet3Mdb)
        {
            bytes[offset + 1] = 0;
        }

        await this.AssertStrictPublicReadsAsync(bytes, PageReadOptimizationMode.Disabled);
        await using AccessReader reader = await this.OpenAsync(bytes, PageReadOptimizationMode.Disabled);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetRealRowCountAsync(TableName, this.ct));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetRealRowCountAsync(TableName, this.ct));
        Assert.Equal(0, reader.OmittedRowCount);

        await using AccessReader lenient = await AccessReader.OpenAsync(
            new MemoryStream(bytes, writable: false),
            new AccessReaderOptions { UseLockFile = false, StrictParsing = false },
            leaveOpen: false,
            this.ct);
        Assert.NotEmpty(await lenient.Rows<ItemRow>(TableName, cancellationToken: this.ct).ToListAsync(this.ct));
        Assert.Equal(1, lenient.OmittedRowCount);
    }

    /// <summary>An exact indexed read refuses the referenced corrupt row instead of returning no matches.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CorruptOverflowPointer_RefusesIndexedReads(DatabaseFormat format)
    {
        byte[] bytes = await seeds.CopyToArrayAsync(
            $"{format}/{TableName}/{MinimumRows}/primary-key",
            () => SyntheticOverflowRows.CreateTableAsync(format, TableName, MinimumRows, primaryKey: true, CancellationToken.None).AsTask());
        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, OverflowRowLayout.PointerPastEndOfFile, this.ct);
        await using AccessReader reader = await this.OpenAsync(bytes, PageReadOptimizationMode.Disabled);
        IndexMetadata primary = (await reader.ListIndexesAsync(TableName, this.ct)).Single(index => index.Kind == IndexKind.PrimaryKey);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.SeekRowsAsync(TableName, primary.Name, [1], this.ct).ToListAsync(this.ct));
        Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
        Assert.NotNull(failure.ErrorInfo.PageNumber);
    }

    /// <summary>Lenient omissions in authorized linked sources remain observable on the root reader.</summary>
    [Fact]
    public async Task LenientLinkedReads_AggregateOmissionsOnRootReader()
    {
        byte[] source = await this.CopySeedAsync(DatabaseFormat.Jet4Mdb);
        int originalCount = (await this.ReadIdsAsync(source, PageReadOptimizationMode.Disabled)).Count;
        _ = await SyntheticOverflowRows.MoveRowAsync(source, TableName, OverflowRowLayout.PointerPastEndOfFile, this.ct);
        await using var host = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(host, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, this.ct))
        {
            await writer.CreateLinkedTableAsync("LinkedItems", "source.mdb", TableName, this.ct);
        }

        host.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            host,
            new AccessReaderOptions
            {
                UseLockFile = false,
                StrictParsing = false,
                LinkedSourceResolver = (_, _, _) => new ValueTask<Stream>(new MemoryStream(source, writable: false)),
            },
            leaveOpen: true,
            this.ct);
        Assert.Equal(originalCount - 1, (await reader.Rows("LinkedItems", cancellationToken: this.ct).ToListAsync(this.ct)).Count);
        Assert.Equal(1, reader.OmittedRowCount);
    }

    private static TheoryData<DatabaseFormat, OverflowRowLayout, PageReadOptimizationMode> Combine(params OverflowRowLayout[] layouts)
    {
        var data = new TheoryData<DatabaseFormat, OverflowRowLayout, PageReadOptimizationMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (OverflowRowLayout layout in layouts)
            {
                data.Add(format, layout, PageReadOptimizationMode.Disabled);
                data.Add(format, layout, PageReadOptimizationMode.Enabled);
            }
        }

        return data;
    }

    private async ValueTask AssertStrictPublicReadsAsync(byte[] bytes, PageReadOptimizationMode readMode)
    {
        for (int surface = 0; surface < 9; surface++)
        {
            await using AccessReader reader = await this.OpenAsync(bytes, readMode);
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(() => this.ReadSurfaceAsync(reader, surface));
            Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
            Assert.NotNull(failure.ErrorInfo.PageNumber);
            Assert.Contains("slot", failure.ErrorInfo.Reason, StringComparison.Ordinal);
            Assert.Equal(0, reader.OmittedRowCount);
        }
    }

    private async Task ReadSurfaceAsync(AccessReader reader, int surface)
    {
        switch (surface)
        {
            case 0:
                _ = await reader.GetRealRowCountAsync(TableName, this.ct);
                break;
            case 1:
                _ = await reader.Rows(TableName, cancellationToken: this.ct).ToListAsync(this.ct);
                break;
            case 2:
                _ = await reader.RowsAsStrings(TableName, cancellationToken: this.ct).ToListAsync(this.ct);
                break;
            case 3:
                using (DataTable table = await reader.ReadTableAsync(TableName, cancellationToken: this.ct))
                {
                    _ = table.Rows.Count;
                }

                break;
            case 4:
                using (DataTable table = await reader.ReadTableAsStringsAsync(TableName, cancellationToken: this.ct))
                {
                    _ = table.Rows.Count;
                }

                break;
            case 5:
                _ = await reader.ReadTableAsync<ItemRow>(TableName, cancellationToken: this.ct);
                break;
            case 6:
                _ = await reader.Query<ItemRow>(TableName).CountAsync(this.ct);
                break;
            case 7:
                _ = await reader.Rows<ItemRow>(TableName, cancellationToken: this.ct).ToListAsync(this.ct);
                break;
            case 8:
                _ = await reader.Services.Tables.RowsWithDecoder<ItemRow>(TableName, forceProjection: true, progress: null, this.ct).ToListAsync(this.ct);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(surface));
        }
    }

    private ValueTask<byte[]> CopySeedAsync(DatabaseFormat format) =>
        seeds.CopyToArrayAsync(
            $"{format}/{TableName}/{MinimumRows}/no-primary-key",
            () => SyntheticOverflowRows.CreateTableAsync(format, TableName, MinimumRows, primaryKey: false, CancellationToken.None).AsTask());

    private async ValueTask<AccessReader> OpenAsync(byte[] bytes, PageReadOptimizationMode readMode)
        => await AccessReader.OpenAsync(
            new MemoryStream(bytes, writable: false),
            new AccessReaderOptions { UseLockFile = false, PageReadOptimizationMode = readMode },
            leaveOpen: false,
            this.ct);

    private async ValueTask<List<int>> ReadIdsAsync(byte[] bytes, PageReadOptimizationMode readMode)
    {
        await using AccessReader reader = await this.OpenAsync(bytes, readMode);
        return [.. (await reader.Rows(TableName, cancellationToken: this.ct).ToListAsync(this.ct)).Select(row => (int)row[0])];
    }

    private async ValueTask<Dictionary<string, long>> CountThroughReadApisAsync(byte[] bytes, PageReadOptimizationMode readMode)
    {
        await using AccessReader reader = await this.OpenAsync(bytes, readMode);
        var counts = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            ["GetRealRowCountAsync"] = await reader.GetRealRowCountAsync(TableName, this.ct),
            ["Rows"] = (await reader.Rows(TableName, cancellationToken: this.ct).ToListAsync(this.ct)).Count,
            ["RowsAsStrings"] = (await reader.RowsAsStrings(TableName, cancellationToken: this.ct).ToListAsync(this.ct)).Count,
        };

        using (DataTable typed = await reader.ReadTableAsync(TableName, cancellationToken: this.ct))
        {
            counts["ReadTableAsync"] = typed.Rows.Count;
        }

        using (DataTable strings = await reader.ReadTableAsStringsAsync(TableName, cancellationToken: this.ct))
        {
            counts["ReadTableAsStringsAsync"] = strings.Rows.Count;
        }

        return counts;
    }

    /// <summary>A scalar mapped row that exercises the direct decoder.</summary>
#pragma warning disable CA1812 // Rows<T> and its generated decoder construct this POCO.
    private sealed class ItemRow
    {
        /// <summary>Gets or sets the row identity.</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets the row name.</summary>
        public string? Name { get; set; }
    }
#pragma warning restore CA1812
}
