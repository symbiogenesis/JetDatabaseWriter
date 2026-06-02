namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Integration tests for ReadDataTableAsync and ReadTableAsStringsAsync.
/// </summary>
/// <param name="db">The database input.</param>
public class AccessReaderDataTableTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── ReadDataTableAsync ────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_ReturnsNonNullWithColumns(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.True(dt.Columns.Count > 0);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_MaxRows_LimitsRowCount(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        const int max = 5;

        DataTable dt = await reader.ReadDataTableAsync(table, max, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.True(dt.Rows.Count <= max);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadDataTable_ColumnMetadata_MatchesGetColumnMetadata(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = await reader.ReadDataTableAsync(table, 1, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

        Assert.NotNull(dt);
        Assert.Equal(meta.Count, dt.Columns.Count);
        for (int i = 0; i < meta.Count; i++)
        {
            Assert.Equal(meta[i].Name, dt.Columns[i].ColumnName);
            Assert.Equal(meta[i].ClrType, dt.Columns[i].DataType);
        }
    }

    [Fact]
    public async Task ReadDataTable_BulkLoadPath_PreservesValuesAndMaxRows()
    {
        await using var stream = new MemoryStream();
        DateTime seenOn = new(2026, 5, 24, 9, 30, 0, DateTimeKind.Unspecified);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "BulkData",
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 64),
                    new("Amount", typeof(decimal)),
                    new("SeenOn", typeof(DateTime)),
                    new("Notes", typeof(string)),
                    new("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);

            var rows = new List<object[]>
            {
                new object[] { 1, "Alpha", 12m, seenOn, new string('A', 1200), new byte[] { 0x01, 0x02, 0x03 } },
                new object[] { 2, DBNull.Value, 56m, seenOn.AddDays(1), DBNull.Value, DBNull.Value },
                new object[] { 3, "Gamma", 90m, seenOn.AddDays(2), "not read", new byte[] { 0x04 } },
            };

            await writer.InsertRowsAsync("BulkData", rows, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable table = await reader.ReadDataTableAsync(
            "BulkData",
            maxRows: 2,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("BulkData", table.TableName);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal(typeof(int), table.Columns["Id"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Name"]!.DataType);
        Assert.Equal(typeof(decimal), table.Columns["Amount"]!.DataType);
        Assert.Equal(typeof(DateTime), table.Columns["SeenOn"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Notes"]!.DataType);
        Assert.Equal(typeof(byte[]), table.Columns["Blob"]!.DataType);

        DataRow first = table.Rows[0];
        Assert.Equal(DataRowState.Added, first.RowState);
        Assert.Equal(1, first["Id"]);
        Assert.Equal("Alpha", first["Name"]);
        Assert.Equal(12m, first["Amount"]);
        Assert.Equal(seenOn, first["SeenOn"]);
        Assert.Equal(new string('A', 1200), first["Notes"]);
        Assert.Equal(new byte[] { 0x01, 0x02, 0x03 }, Assert.IsType<byte[]>(first["Blob"]));

        DataRow second = table.Rows[1];
        Assert.Equal(DBNull.Value, second["Name"]);
        Assert.Equal(DBNull.Value, second["Notes"]);
        Assert.Equal(DBNull.Value, second["Blob"]);
    }

    // ── ReadTableAsStringsAsync returns DataTable — integration ──────

    [Theory]
    [MemberData(nameof(TestDatabases.All), MemberType = typeof(TestDatabases))]
    public async Task ReadTableAsStrings_AllColumnsAreStringType(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string table = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        DataTable dt = await reader.ReadTableAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(dt.Columns.Count > 0);
        foreach (DataColumn col in dt.Columns)
        {
            Assert.Equal(typeof(string), col.DataType);
        }
    }
}
