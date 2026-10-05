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
public sealed class OverflowRowTests
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

        using DataTable data = await reader.ReadDataTableAsync("Table1", cancellationToken: this.ct);
        List<object[]> rows = await CollectAsync(reader.Rows("Table1", cancellationToken: this.ct));
        List<string[]> stringRows = await CollectAsync(reader.RowsAsStrings("Table1", cancellationToken: this.ct));
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

        using DataTable data = await reader.ReadDataTableAsync("Table1", cancellationToken: this.ct);
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

                List<object[]> hits = await CollectAsync(reader.SeekRowsAsync("Table1", index.Name, [row[column]], this.ct));
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
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(format, TableName, MinimumRows, primaryKey: false, this.ct);
        List<int> expectedIds = await this.ReadIdsAsync(bytes, readMode);

        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, layout, this.ct);

        Assert.Equal(expectedIds, await this.ReadIdsAsync(bytes, readMode));
        Assert.All(await this.CountThroughReadApisAsync(bytes, readMode), pair => Assert.True(pair.Value == expectedIds.Count, $"{pair.Key} returned {pair.Value} rows; expected {expectedIds.Count}."));
    }

    /// <summary>
    /// An overflow pointer that cannot be resolved (past the end of the file, into
    /// another table, too short, or a cycle) skips the row, as an undecodable row
    /// is skipped, and nothing throws.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">How the pointer is corrupt.</param>
    /// <param name="readMode">The table-scan read-ahead mode.</param>
    [Theory]
    [MemberData(nameof(CorruptLayouts))]
    public async Task CorruptOverflowPointer_SkipsTheRow(DatabaseFormat format, OverflowRowLayout layout, PageReadOptimizationMode readMode)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(format, TableName, MinimumRows, primaryKey: false, this.ct);
        List<int> originalIds = await this.ReadIdsAsync(bytes, readMode);

        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, layout, this.ct);

        List<int> ids = await this.ReadIdsAsync(bytes, readMode);
        Assert.Equal(originalIds.Skip(1), ids);
        Assert.All(await this.CountThroughReadApisAsync(bytes, readMode), pair => Assert.True(pair.Value == ids.Count, $"{pair.Key} returned {pair.Value} rows; expected {ids.Count}."));
    }

    /// <summary>Diagnostics expose a skipped overflow row only when enabled.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">The corrupt pointer layout.</param>
    /// <param name="readMode">The scan read mode.</param>
    [Theory(DisableParallelization = true)]
    [MemberData(nameof(CorruptLayouts))]
    public async Task CorruptOverflowPointer_TracesOnlyWithDiagnostics(DatabaseFormat format, OverflowRowLayout layout, PageReadOptimizationMode readMode)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(format, TableName, MinimumRows, primaryKey: false, this.ct);
        int originalCount = (await this.ReadIdsAsync(bytes, readMode)).Count;
        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, layout, this.ct);
        await using var messages = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        Trace.Listeners.Add(listener);
        try
        {
            for (int pass = 0; pass < 2; pass++)
            {
                bool enabled = pass != 0;
                messages.GetStringBuilder().Clear();
                await using var stream = new MemoryStream(bytes, writable: false);
                await using AccessReader reader = await AccessReader.OpenAsync(
                    stream,
                    new AccessReaderOptions { UseLockFile = false, DiagnosticsEnabled = enabled, PageReadOptimizationMode = readMode },
                    leaveOpen: true,
                    this.ct);
                using DataTable rows = await reader.ReadDataTableAsync(TableName, cancellationToken: this.ct);
                Assert.Equal(originalCount - 1, rows.Rows.Count);
                listener.Flush();
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
            Trace.Listeners.Remove(listener);
        }
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

    private static async ValueTask<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var items = new List<T>();
        await foreach (T item in source)
        {
            items.Add(item);
        }

        return items;
    }

    private async ValueTask<AccessReader> OpenAsync(byte[] bytes, PageReadOptimizationMode readMode)
        => await AccessReader.OpenAsync(
            new MemoryStream(bytes, writable: false),
            new AccessReaderOptions { UseLockFile = false, PageReadOptimizationMode = readMode },
            leaveOpen: false,
            this.ct);

    private async ValueTask<List<int>> ReadIdsAsync(byte[] bytes, PageReadOptimizationMode readMode)
    {
        await using AccessReader reader = await this.OpenAsync(bytes, readMode);
        return [.. (await CollectAsync(reader.Rows(TableName, cancellationToken: this.ct))).Select(row => (int)row[0])];
    }

    private async ValueTask<Dictionary<string, long>> CountThroughReadApisAsync(byte[] bytes, PageReadOptimizationMode readMode)
    {
        await using AccessReader reader = await this.OpenAsync(bytes, readMode);
        var counts = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            ["GetRealRowCountAsync"] = await reader.GetRealRowCountAsync(TableName, this.ct),
            ["Rows"] = (await CollectAsync(reader.Rows(TableName, cancellationToken: this.ct))).Count,
            ["RowsAsStrings"] = (await CollectAsync(reader.RowsAsStrings(TableName, cancellationToken: this.ct))).Count,
        };

        using (DataTable typed = await reader.ReadDataTableAsync(TableName, cancellationToken: this.ct))
        {
            counts["ReadDataTableAsync"] = typed.Rows.Count;
        }

        using (DataTable strings = await reader.ReadTableAsStringsAsync(TableName, cancellationToken: this.ct))
        {
            counts["ReadTableAsStringsAsync"] = strings.Rows.Count;
        }

        return counts;
    }
}
