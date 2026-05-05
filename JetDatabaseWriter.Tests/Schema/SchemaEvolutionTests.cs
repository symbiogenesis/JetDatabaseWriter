namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// End-to-end tests for schema evolution operations: AddColumnAsync,
/// DropColumnAsync, RenameColumnAsync, and related validation.
/// Tests run against both Jet3 and ACE formats via <c>[Theory]</c> parameters.
/// </summary>
public sealed class SchemaEvolutionTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task AddColumnAsync_AppendsColumn_ExistingRowsBecomeNull(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        const string table = "Contacts";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, "Alice"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, "Bob"], TestContext.Current.CancellationToken);

            await writer.AddColumnAsync(
                table,
                new ColumnDefinition("Score", typeof(int)),
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [3, "Carol", 99], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(3, dt.Columns.Count);
        Assert.Equal("Score", dt.Columns[2].ColumnName);
        Assert.Equal(3, dt.Rows.Count);

        Assert.Equal("Alice", dt.Rows[0]["Name"]);
        Assert.Equal(DBNull.Value, dt.Rows[0]["Score"]);
        Assert.Equal("Bob", dt.Rows[1]["Name"]);
        Assert.Equal(DBNull.Value, dt.Rows[1]["Score"]);
        Assert.Equal("Carol", dt.Rows[2]["Name"]);
        Assert.Equal(99, dt.Rows[2]["Score"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DropColumnAsync_RemovesColumn_AndPreservesOtherData(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        const string table = "Contacts";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                    new("Score", typeof(int)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, "Alice", 90], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, "Bob", 80], TestContext.Current.CancellationToken);

            await writer.DropColumnAsync(table, "Score", TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(2, dt.Columns.Count);
        Assert.False(dt.Columns.Contains("Score"));
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(1, dt.Rows[0]["Id"]);
        Assert.Equal("Alice", dt.Rows[0]["Name"]);
        Assert.Equal("Bob", dt.Rows[1]["Name"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task RenameColumnAsync_RenamesColumn_AndPreservesAllData(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        const string table = "Contacts";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, 95], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, 88], TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(table, "Score", "Rating", TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(2, dt.Columns.Count);
        Assert.True(dt.Columns.Contains("Rating"));
        Assert.False(dt.Columns.Contains("Score"));
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(95, dt.Rows[0]["Rating"]);
        Assert.Equal(88, dt.Rows[1]["Rating"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DropColumnAsync_LastColumn_Throws(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        const string table = "Solo";

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [new("Only", typeof(int))],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.DropColumnAsync(table, "Only", TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task AddColumnAsync_DuplicateName_Throws(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);
        const string table = "Dup";

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 20)],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.AddColumnAsync(table, new ColumnDefinition("name", typeof(string), 20), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task FreshlyCreatedTable_HasNoUserDefinedIndexEntries()
    {
        await using var stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        string tableName = $"NoIdx_{Guid.NewGuid():N}"[..18];

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);

        DataTable? msysIndexes = await reader.ReadDataTableAsync("MSysIndexes", cancellationToken: TestContext.Current.CancellationToken);
        if (msysIndexes is not null)
        {
            int matches = msysIndexes.AsEnumerable()
                .Count(r => MentionsTable(r, tableName));
            Assert.Equal(0, matches);
        }

        DataTable? msysRels = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);
        if (msysRels is not null)
        {
            int matches = msysRels.AsEnumerable()
                .Count(r => MentionsTable(r, tableName));
            Assert.Equal(0, matches);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static bool MentionsTable(DataRow row, string tableName)
    {
        foreach (DataColumn col in row.Table.Columns)
        {
            object val = row[col];
            if (val is string s && string.Equals(s, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
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
