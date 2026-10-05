namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// The writer refuses a new table, column, index, relationship or linked-table
/// name that Microsoft Access does not allow (whitespace only, a leading space,
/// more than 64 characters, or one of <c>. ! ` [ ]</c> or a control character)
/// before it writes anything, and keeps working with objects that already carry
/// such a name.
/// </summary>
public sealed class ObjectNameValidationTests
{
    private static readonly DatabaseFormat[] Formats = [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    private static readonly string[] InvalidNames =
    [
        " ",
        "   ",
        " Lead",
        "a.b",
        "a!b",
        "a`b",
        "a[b",
        "a]b",
        "a\tb",
        "a\u0001b",
        "a\u007Fb",
        new('x', 65),
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every writer-created format.</summary>
    /// <returns>The formats.</returns>
    public static TheoryData<DatabaseFormat> AllFormats() => [.. Formats];

    /// <summary>Gets every writer-created format with every invalid name.</summary>
    /// <returns>The format and name pairs.</returns>
    public static TheoryData<DatabaseFormat, string> FormatsAndInvalidNames()
    {
        var data = new TheoryData<DatabaseFormat, string>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (string name in InvalidNames)
            {
                data.Add(format, name);
            }
        }

        return data;
    }

    /// <summary>Gets every writer-created format with names at the edge of the rules that Access allows.</summary>
    /// <returns>The format and name pairs.</returns>
    public static TheoryData<DatabaseFormat, string> FormatsAndBoundaryNames()
    {
        var data = new TheoryData<DatabaseFormat, string>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (string name in new[] { new string('x', 64), "=Eq", "a'b", "a\"b", "Order Details", "Trail ", "#Col", "Café" })
            {
                data.Add(format, name);
            }
        }

        return data;
    }

    /// <summary>Gets every writer-created format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> AllFormatsAndModes()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task CreateTable_InvalidTableName_ThrowsAndCreatesNothing(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync(name, [new("Id", typeof(int))], Ct));

            Assert.Equal("tableName", ex.ParamName);
            Assert.Contains("table name", ex.Message, StringComparison.Ordinal);
            Assert.Contains("1 to 64 characters", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.ListTablesAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task CreateTable_InvalidColumnName_ThrowsAndCreatesNothing(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("T", [new("Id", typeof(int)), new(name, typeof(string), maxLength: 10)], Ct));

            Assert.Equal("columns", ex.ParamName);
            Assert.Contains("column name", ex.Message, StringComparison.Ordinal);
            Assert.Contains("position 1", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.ListTablesAsync(Ct));
    }

    /// <summary>
    /// A null column name used to leak <see cref="ArgumentNullException"/> with
    /// <c>ParamName</c> "s" from <c>Encoding.GetBytes</c>, an empty one created a
    /// nameless column, and a null column or index element threw
    /// <see cref="NullReferenceException"/>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task CreateTable_MissingColumnOrIndexName_ThrowsArgumentException(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException nullName = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("T", [new("Id", typeof(int)), new(null!, typeof(string), maxLength: 10)], Ct));
            Assert.Equal("columns", nullName.ParamName);

            ArgumentException emptyName = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("T", [new(string.Empty, typeof(int))], Ct));
            Assert.Equal("columns", emptyName.ParamName);

            ArgumentException nullColumn = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("T", [new("Id", typeof(int)), null!], Ct));
            Assert.Equal("columns", nullColumn.ParamName);

            ArgumentException nullIndex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("T", [new("Id", typeof(int))], [null!], Ct));
            Assert.Equal("indexes", nullIndex.ParamName);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.ListTablesAsync(Ct));
    }

    /// <summary>
    /// Access compares column names ignoring case, so <c>Name</c> and
    /// <c>NAME</c> in one table used to give two columns no lookup could tell apart.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task CreateTable_DuplicateColumnNamesIgnoringCase_Throws(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("T", [new("Name", typeof(string), maxLength: 10), new("NAME", typeof(string), maxLength: 10)], Ct));

            Assert.Equal("columns", ex.ParamName);
            Assert.Contains("'NAME'", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.ListTablesAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task CreateTable_InvalidIndexName_ThrowsAndCreatesNothing(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync("T", [new("Id", typeof(int))], [new IndexDefinition(name, "Id")], Ct));

            Assert.Equal("indexes", ex.ParamName);
            Assert.Contains("index name", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.ListTablesAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task AddColumn_InvalidName_ThrowsAndLeavesTableUnchanged(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateTableWithRowAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.AddColumnAsync("T", new ColumnDefinition(name, typeof(int)), Ct));

            Assert.Equal("column", ex.ParamName);
            Assert.Contains("column name", ex.Message, StringComparison.Ordinal);
        }

        await AssertTableUnchangedAsync(ms);
    }

    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task RenameColumn_InvalidNewName_ThrowsAndLeavesTableUnchanged(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateTableWithRowAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.RenameColumnAsync("T", "Name", name, Ct));

            Assert.Equal("newColumnName", ex.ParamName);
            Assert.Contains("column name", ex.Message, StringComparison.Ordinal);
        }

        await AssertTableUnchangedAsync(ms);
    }

    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task CreateLinkedTable_InvalidLinkName_ThrowsAndCreatesNothing(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            byte[] cachedSchema = LinkedOdbcLvPropBuilder.Build("dbo.Orders", null, format);
            Func<ValueTask>[] creates =
            [
                () => writer.CreateLinkedTableAsync(name, @"C:\Data\Backend.accdb", "Orders", Ct),
                () => writer.CreateLinkedTextTableAsync(name, @"C:\Data", "orders.csv", "Text;HDR=YES;FMT=Delimited", Ct),
                () => writer.CreateLinkedOdbcTableAsync(name, "ODBC;DSN=Sales", "dbo.Orders", Ct),
                () => writer.CreateLinkedOdbcTableAsync(name, "ODBC;DSN=Sales", "dbo.Orders", [new ColumnDefinition("OrderId", typeof(int))], Ct),
                () => writer.CreateLinkedOdbcTableAsync(name, "ODBC;DSN=Sales", "dbo.Orders", cachedSchema, Ct),
            ];

            foreach (Func<ValueTask> create in creates)
            {
                ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () => await create());
                Assert.Equal("linkedTableName", ex.ParamName);
                Assert.Contains("table name", ex.Message, StringComparison.Ordinal);
            }
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.ListLinkedTablesAsync(Ct));
        Assert.Empty(await reader.ListTablesAsync(Ct));
    }

    /// <summary>
    /// Only the local link name follows the Access rules: an ODBC table such as
    /// <c>dbo.Orders</c> and a text file such as <c>orders.csv</c> are legal
    /// foreign names.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task CreateLinkedTable_ForeignNamesWithDots_AreAccepted(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateLinkedOdbcTableAsync("LinkedOrders", "ODBC;DSN=Sales", "dbo.Orders", Ct);
            await writer.CreateLinkedTextTableAsync("LinkedCsv", @"C:\Data", "orders.csv", "Text;HDR=YES;FMT=Delimited", Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        IReadOnlyList<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(Ct);
        Assert.Equal(["LinkedCsv", "LinkedOrders"], linked.Select(l => l.Name).Order(StringComparer.Ordinal));
        Assert.Contains(linked, l => l.SourceObjectName == "dbo.Orders");
        Assert.Contains(linked, l => l.SourceObjectName == "orders.csv");
    }

    /// <summary>
    /// The relationship name is checked before any table or
    /// <c>MSysRelationships</c> lookup, on every format.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="name">The invalid name.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task CreateRelationship_InvalidName_Throws(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateParentChildAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateRelationshipAsync(new RelationshipDefinition(name, "Parent", "Id", "Child", "ParentId"), Ct));

            Assert.Equal("relationship.Name", ex.ParamName);
            Assert.Contains("relationship name", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.ListRelationshipsAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(FormatsAndInvalidNames))]
    public async Task RenameRelationship_InvalidNewName_Throws(DatabaseFormat format, string name)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct);

        ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.RenameRelationshipAsync("ParentChild", name, Ct));

        Assert.Equal("newName", ex.ParamName);
        Assert.Contains("relationship name", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A valid relationship name still renames, on the format whose new
    /// databases carry <c>MSysRelationships</c>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task CreateAndRenameRelationship_ValidNames_Succeed()
    {
        await using MemoryStream ms = await CreateParentChildAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition("Parent Child", "Parent", "Id", "Child", "ParentId"), Ct);
            await writer.RenameRelationshipAsync("Parent Child", new string('r', 64), Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(new string('r', 64), Assert.Single(await reader.ListRelationshipsAsync(Ct)).Name);
    }

    /// <summary>
    /// Names at the edge of the rules, which Access allows, are accepted and
    /// read back exactly: as a table, column and index name in
    /// <c>CreateTableAsync</c>, as a new name in <c>RenameColumnAsync</c>, and
    /// as an added column.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="name">The name.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndBoundaryNames))]
    public async Task ValidBoundaryNames_RoundTrip(DatabaseFormat format, string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        string added = name.Length < 64 ? name + "_" : new string('y', 64);
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(name, [new("Id", typeof(int)), new(name, typeof(string), maxLength: 10)], [new IndexDefinition(name, "Id")], Ct);
            await writer.InsertRowAsync(name, [1, "v"], Ct);
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("C", typeof(int))], Ct);
            await writer.RenameColumnAsync("T", "C", name, Ct);
            await writer.AddColumnAsync("T", new ColumnDefinition(added, typeof(int)), Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(new[] { "T", name }.Order(StringComparer.Ordinal), (await reader.ListTablesAsync(Ct)).Order(StringComparer.Ordinal));
        Assert.Equal(["Id", name], (await reader.GetColumnMetadataAsync(name, Ct)).Select(c => c.Name));
        Assert.Contains(await reader.ListIndexesAsync(name, Ct), i => i.Name == name);
        Assert.Equal("v", Assert.Single((await reader.ReadDataTableAsync(name, cancellationToken: Ct)).AsEnumerable())[1]);
        Assert.Equal(["Id", name, added], (await reader.GetColumnMetadataAsync("T", Ct)).Select(c => c.Name));
    }

    /// <summary>
    /// A table whose name and column names an earlier writer, or another tool,
    /// stored against the rules can still be written, altered, read and
    /// dropped: only names a caller introduces are checked.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task LegacyInvalidNames_RemainUsable(DatabaseFormat format)
    {
        const string legacy = "   ";
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct))
        {
            await harness.Services.Schema.CreateTableAsync(
                legacy,
                [new("Id", typeof(int)), new(" Lead", typeof(string), maxLength: 10), new("a.b", typeof(int))],
                [new IndexDefinition("ix.Lead", " Lead")],
                Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.InsertRowAsync(legacy, [1, "one", 10], Ct);
            await writer.AddColumnAsync(legacy, new ColumnDefinition("Ok", typeof(int)), Ct);
            await writer.RenameColumnAsync(legacy, " Lead", "Lead", Ct);
            await writer.DropColumnAsync(legacy, "a.b", Ct);
            await writer.InsertRowAsync(legacy, [2, "two", 20], Ct);
        }

        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            Assert.Equal(legacy, Assert.Single(await reader.ListTablesAsync(Ct)));
            Assert.Equal(["Id", "Lead", "Ok"], (await reader.GetColumnMetadataAsync(legacy, Ct)).Select(c => c.Name));
            Assert.Contains(await reader.ListIndexesAsync(legacy, Ct), i => i.Name == "ix.Lead");
            Assert.Equal(
                ["1|one|", "2|two|20"],
                (await reader.ReadDataTableAsync(legacy, cancellationToken: Ct)).AsEnumerable().Select(r => $"{r["Id"]}|{r["Lead"]}|{r["Ok"]}").Order(StringComparer.Ordinal));
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.DropTableAsync(legacy, Ct);
        }

        await using AccessReader after = await OpenReaderAsync(ms);
        Assert.Empty(await after.ListTablesAsync(Ct));
    }

    /// <summary>
    /// A refused name leaves the writer, and an explicit transaction, usable;
    /// nothing of the refused call reaches the file.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the calls.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task InvalidName_LeavesWriterUsable(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync(" Bad", [new("Id", typeof(int))], Ct));
                await writer.CreateTableAsync("Good", [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 10)], Ct);
                await Assert.ThrowsAsync<ArgumentException>(async () => await writer.AddColumnAsync("Good", new ColumnDefinition("a.b", typeof(int)), Ct));
                await writer.InsertRowAsync("Good", [1, "one"], Ct);
            });
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal("Good", Assert.Single(await reader.ListTablesAsync(Ct)));
        Assert.Equal(["Id", "Name"], (await reader.GetColumnMetadataAsync("Good", Ct)).Select(c => c.Name));
        Assert.Equal("one", Assert.Single((await reader.ReadDataTableAsync("Good", cancellationToken: Ct)).AsEnumerable())["Name"]);
    }

    private static async Task AssertTableUnchangedAsync(MemoryStream ms)
    {
        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["Id", "Name"], (await reader.GetColumnMetadataAsync("T", Ct)).Select(c => c.Name));
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("T", cancellationToken: Ct)).AsEnumerable());
        Assert.Equal(1, row["Id"]);
        Assert.Equal("one", row["Name"]);
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(WriteMode.Direct), leaveOpen: true, cancellationToken: Ct))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static async Task<MemoryStream> CreateTableWithRowAsync(DatabaseFormat format)
    {
        MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 10)], Ct);
            await writer.InsertRowAsync("T", [1, "one"], Ct);
        }

        return ms;
    }

    private static async Task<MemoryStream> CreateParentChildAsync(DatabaseFormat format)
    {
        MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Parent", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("Child", [new("Id", typeof(int)), new("ParentId", typeof(int))], Ct);
        }

        return ms;
    }

    private static AccessWriterOptions WriterOptions(WriteMode mode) => new()
    {
        UseLockFile = false,
        UseTransactionalWrites = mode == WriteMode.AutoCommit,
    };

    private static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);
    }

    private static async Task<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }

    private static Task RunAsync(AccessWriter writer, WriteMode mode, Func<Task> work)
        => JetDatabaseWriter.Tests.Infrastructure.WriteModes.RunAsync(writer, mode, work, Ct);
}
