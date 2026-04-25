namespace JetDatabaseWriter.Tests;

using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Round-trip tests for non-ASCII (umlaut, accented, CJK) table and column names.
/// Mirrors the upstream mdbtools test_script.sh, which deliberately runs every
/// utility against the German-named "Umsätze" table in nwind.mdb to exercise the
/// codepage-decoded name path:
/// https://github.com/mdbtools/mdbtools/blob/dev/test_script.sh.
/// </summary>
public sealed class NonAsciiNamesTests
{
    public static IEnumerable<object[]> Formats =>
    [
        [DatabaseFormat.Jet3Mdb],
        [DatabaseFormat.Jet4Mdb],
        [DatabaseFormat.AceAccdb],
    ];

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task CreateTable_WithUmlautName_RoundTrips(DatabaseFormat format)
    {
        const string TableName = "Umsätze";
        const string ColumnName = "Beträge";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                new List<ColumnDefinition>
                {
                    new("Id", typeof(int)),
                    new(ColumnName, typeof(string), maxLength: 50),
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(TableName, new object[] { 1, "Größe" }, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(TableName, new object[] { 2, "Straße" }, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(TableName, tables);

        var meta = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Contains(meta, c => c.Name == ColumnName);

        var rows = await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(rows);
        Assert.Equal(2, rows!.Rows.Count);
        Assert.Equal("Größe", rows.Rows[0][ColumnName]);
        Assert.Equal("Straße", rows.Rows[1][ColumnName]);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task CreateTable_WithAccentedName_RoundTrips(DatabaseFormat format)
    {
        const string TableName = "Café";
        const string ColumnName = "Crêpe";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                new List<ColumnDefinition>
                {
                    new("Id", typeof(int)),
                    new(ColumnName, typeof(string), maxLength: 50),
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(TableName, new object[] { 1, "Océ" }, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(TableName, tables);

        var meta = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Contains(meta, c => c.Name == ColumnName);

        var rows = await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("Océ", rows!.Rows[0][ColumnName]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_WithCjkName_RoundTrips(DatabaseFormat format)
    {
        // Jet4 / ACE store object names in UTF-16, so CJK round-trips verbatim.
        // Jet3 names are codepage-encoded and would require a CJK codepage —
        // outside scope of this regression test.
        const string TableName = "顧客";
        const string ColumnName = "氏名";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                new List<ColumnDefinition>
                {
                    new("Id", typeof(int)),
                    new(ColumnName, typeof(string), maxLength: 50),
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(TableName, new object[] { 1, "山田太郎" }, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(TableName, tables);

        var meta = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Contains(meta, c => c.Name == ColumnName);

        var rows = await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("山田太郎", rows!.Rows[0][ColumnName]);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task Streaming_NonAsciiTable_YieldsRows(DatabaseFormat format)
    {
        // Mirrors `mdb-json nwind.mdb "Umsätze"` and `mdb-count nwind.mdb "Umsätze"`
        // exit-zero smoke checks in mdbtools' test_script.sh.
        const string TableName = "Umsätze";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                new List<ColumnDefinition>
                {
                    new("Id", typeof(int)),
                    new("Wert", typeof(int)),
                },
                TestContext.Current.CancellationToken);

            for (int i = 1; i <= 5; i++)
            {
                await writer.InsertRowAsync(TableName, new object[] { i, i * 10 }, TestContext.Current.CancellationToken);
            }
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        int count = await reader.Rows(TableName, cancellationToken: TestContext.Current.CancellationToken)
            .CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(5, count);
    }
}
