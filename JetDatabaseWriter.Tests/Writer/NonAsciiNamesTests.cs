namespace JetDatabaseWriter.Tests.Writer;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
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
    public static TheoryData<DatabaseFormat> Formats =>
    [
        DatabaseFormat.Jet3Mdb,
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.AceAccdb,
    ];

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task CreateTable_WithUmlautName_RoundTrips(DatabaseFormat format)
    {
        const string tableName = "Umsätze";
        const string columnName = "Beträge";

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new(columnName, typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, "Größe"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [2, "Straße"], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(tableName, tables);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        Assert.Contains(meta, c => c.Name == columnName);

        DataTable rows = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(rows);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Equal("Größe", rows.Rows[0][columnName]);
        Assert.Equal("Straße", rows.Rows[1][columnName]);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task CreateTable_WithAccentedName_RoundTrips(DatabaseFormat format)
    {
        const string tableName = "Café";
        const string columnName = "Crêpe";

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new(columnName, typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, "Océ"], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(tableName, tables);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        Assert.Contains(meta, c => c.Name == columnName);

        DataTable rows = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("Océ", rows.Rows[0][columnName]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_WithCjkName_RoundTrips(DatabaseFormat format)
    {
        // Jet4 / ACE store object names in UTF-16, so CJK round-trips verbatim.
        // Jet3 names are codepage-encoded and would require a CJK codepage —
        // outside scope of this regression test.
        const string tableName = "顧客";
        const string columnName = "氏名";

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new(columnName, typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, "山田太郎"], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(tableName, tables);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        Assert.Contains(meta, c => c.Name == columnName);

        DataTable rows = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("山田太郎", rows.Rows[0][columnName]);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task Streaming_NonAsciiTable_YieldsRows(DatabaseFormat format)
    {
        // Mirrors `mdb-json nwind.mdb "Umsätze"` and `mdb-count nwind.mdb "Umsätze"`
        // exit-zero smoke checks in mdbtools' test_script.sh.
        const string tableName = "Umsätze";

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Wert", typeof(int)),
                ],
                TestContext.Current.CancellationToken);

            var rows = new List<object[]>(5);
            for (int i = 1; i <= 5; i++)
            {
                rows.Add([i, i * 10]);
            }

            await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        int count = await reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .CountAsync(TestContext.Current.CancellationToken);

        Assert.Equal(5, count);
    }
}
