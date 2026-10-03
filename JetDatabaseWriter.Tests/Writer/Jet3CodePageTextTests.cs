namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// A Jet3 (Access 97) database stores text and object names in its ANSI code
/// page, Windows-1252 in the files <c>CreateDatabaseAsync</c> makes. .NET's
/// code-page encoder turns a character outside it into a best-fit match or
/// <c>?</c>, so the writer used to store a name or value that differed from the
/// caller's: a table named 顧客 was stored as "??" and could not be found by its
/// own name, a second such table passed the duplicate check, and an indexed
/// "Łódź" was stored as "Lódz" under an index key built from "Łódź". The writer
/// now refuses such a name or value before it writes anything.
/// </summary>
public sealed class Jet3CodePageTextTests
{
    private const string Cp1252Message = "code page 1252";

    /// <summary>A best-fit character, two characters cp1252 lacks, and a surrogate pair.</summary>
    private static readonly string[] TextsOutsideCp1252 = ["Łódź", "中文", "x😀"];

    private static readonly Dictionary<string, (string ParamName, Func<AccessWriter, string, ValueTask> Call)> NameCalls = new(StringComparer.Ordinal)
    {
        ["CreateTable table"] = ("tableName", (w, s) => w.CreateTableAsync(s, [new("Id", typeof(int))], Ct)),
        ["CreateTable column"] = ("columns", (w, s) => w.CreateTableAsync("New", [new("Id", typeof(int)), new(s, typeof(int))], Ct)),
        ["CreateTable index"] = ("indexes", (w, s) => w.CreateTableAsync("New", [new("Id", typeof(int))], [new IndexDefinition(s, "Id")], Ct)),
        ["AddColumn"] = ("column", (w, s) => w.AddColumnAsync("T", new ColumnDefinition(s, typeof(int)), Ct)),
        ["RenameColumn"] = ("newColumnName", (w, s) => w.RenameColumnAsync("T", "Name", s, Ct)),
        ["CreateRelationship name"] = ("relationship.Name", (w, s) => w.CreateRelationshipAsync(new RelationshipDefinition(s, "T", "Id", "T", "Id"), Ct)),
        ["CreateRelationship table"] = ("relationship.PrimaryTable", (w, s) => w.CreateRelationshipAsync(new RelationshipDefinition("R", s, "Id", "T", "Id"), Ct)),
        ["CreateRelationship column"] = ("relationship.ForeignColumns", (w, s) => w.CreateRelationshipAsync(new RelationshipDefinition("R", "T", "Id", "T", s), Ct)),
        ["RenameRelationship"] = ("newName", (w, s) => w.RenameRelationshipAsync("R", s, Ct)),
        ["CreateLinkedTable link"] = ("linkedTableName", (w, s) => w.CreateLinkedTableAsync(s, @"C:\Data\Backend.mdb", "Orders", Ct)),
        ["CreateLinkedTable path"] = ("sourceDatabasePath", (w, s) => w.CreateLinkedTableAsync("L", $@"C:\Data\{s}.mdb", "Orders", Ct)),
        ["CreateLinkedTable foreign"] = ("foreignTableName", (w, s) => w.CreateLinkedTableAsync("L", @"C:\Data\Backend.mdb", s, Ct)),
        ["CreateLinkedOdbcTable connection"] = ("connectionString", (w, s) => w.CreateLinkedOdbcTableAsync("L", $"ODBC;DSN={s}", "dbo.Orders", Ct)),
        ["CreateLinkedOdbcTable foreign"] = ("foreignTableName", (w, s) => w.CreateLinkedOdbcTableAsync("L", "ODBC;DSN=Sales", s, Ct)),
        ["CreateLinkedTextTable directory"] = ("sourceDirectoryPath", (w, s) => w.CreateLinkedTextTableAsync("L", $@"C:\{s}", "orders.csv", "Text;HDR=YES", Ct)),
        ["CreateLinkedTextTable file"] = ("foreignFileName", (w, s) => w.CreateLinkedTextTableAsync("L", @"C:\Data", s + ".csv", "Text;HDR=YES", Ct)),
        ["CreateLinkedTextTable connect"] = ("connectString", (w, s) => w.CreateLinkedTextTableAsync("L", @"C:\Data", "orders.csv", "Text;" + s, Ct)),
    };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every name-taking call with every text outside cp1252.</summary>
    /// <returns>The call and text pairs.</returns>
    public static TheoryData<string, string> NameCallsAndTexts()
    {
        var data = new TheoryData<string, string>();
        foreach (string call in NameCalls.Keys)
        {
            foreach (string text in TextsOutsideCp1252)
            {
                data.Add(call, text);
            }
        }

        return data;
    }

    /// <summary>Gets the Text, Memo and indexed Text columns of the test table with every text outside cp1252.</summary>
    /// <returns>The column and text pairs.</returns>
    public static TheoryData<string, string> ColumnsAndTexts()
    {
        var data = new TheoryData<string, string>();
        foreach (string column in new[] { "Name", "Notes", "Code" })
        {
            foreach (string text in TextsOutsideCp1252)
            {
                data.Add(column, text);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(NameCallsAndTexts))]
    public async Task NameOutsideCodePage_ThrowsBeforeWriting(string call, string text)
    {
        (string paramName, Func<AccessWriter, string, ValueTask> invoke) = NameCalls[call];
        await using MemoryStream ms = await CreateJet3TableAsync();
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            byte[] before = ms.ToArray();

            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () => await invoke(writer, text));

            Assert.Equal(paramName, ex.ParamName);
            Assert.Contains(Cp1252Message, ex.Message, StringComparison.Ordinal);
            Assert.Equal(before, ms.ToArray());
        }

        await AssertTableUnchangedAsync(ms);
    }

    /// <summary>
    /// A table named 顧客 used to be stored as "??", so it could not be found
    /// by its own name, and a second table named 日本 passed the duplicate
    /// check and became another "??".
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task CjkTableNames_AreRefusedInsteadOfStoredAsQuestionMarks()
    {
        await using MemoryStream ms = await CreateJet3TableAsync();
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            _ = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("顧客", [new("Id", typeof(int))], Ct));
            _ = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("日本", [new("Id", typeof(int))], Ct));
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["T"], await reader.ListTablesAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(ColumnsAndTexts))]
    public async Task InsertTextOutsideCodePage_ThrowsBeforeWriting(string column, string text)
    {
        await using MemoryStream ms = await CreateJet3TableAsync();
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            byte[] before = ms.ToArray();

            object?[] row = [2, "two", "notes two", "B2"];
            row[ColumnIndex(column)] = text;
            object?[][] batch = [[3, "three", "notes three", "C3"], row];
            await AssertRefusedAsync(column, async () => await writer.InsertRowAsync("T", row, Ct));
            await AssertRefusedAsync(column, async () => await writer.InsertRowsAsync("T", batch, Ct));
            await AssertRefusedAsync(column, async () => await writer.InsertRowAsync("T", new RowValues { ["Id"] = 4, [column] = text }, Ct));
            Assert.Equal(before, ms.ToArray());

            // The writer stays usable.
            await writer.InsertRowAsync("T", [5, "five", "notes five", "E5"], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(
            ["1|one|notes one|A1", "5|five|notes five|E5"],
            (await reader.ReadDataTableAsync("T", cancellationToken: Ct)).AsEnumerable().Select(Format).Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// A Memo value over the inline limit goes to a long-value page before
    /// the row is encoded. The refusal comes first, so no such page is written
    /// for the rejected row.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task InsertLongMemoWithTextOutsideCodePage_WritesNoLongValuePage()
    {
        await using MemoryStream ms = await CreateJet3TableAsync();
        await using AccessWriter writer = await OpenWriterAsync(ms);
        byte[] before = ms.ToArray();

        await AssertRefusedAsync("Code", async () => await writer.InsertRowAsync("T", [2, "two", new string('n', 5000), "中"], Ct));

        Assert.Equal(before, ms.ToArray());
    }

    [Theory]
    [MemberData(nameof(ColumnsAndTexts))]
    public async Task UpdateTextOutsideCodePage_ThrowsAndKeepsRow(string column, string text)
    {
        await using MemoryStream ms = await CreateJet3TableAsync();
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            byte[] before = ms.ToArray();

            await AssertRefusedAsync(column, async () => await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { [column] = text }, Ct));

            Assert.Equal(before, ms.ToArray());
        }

        await AssertTableUnchangedAsync(ms);
    }

    /// <summary>
    /// Every character Windows-1252 has is still accepted, in a value and in
    /// names, and reads back unchanged, including the five bytes cp1252 leaves
    /// undefined, which .NET maps to U+0081, U+008D, U+008F, U+0090 and U+009D.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task EveryCodePageCharacter_RoundTrips()
    {
        Encoding cp1252 = CodePagesEncodingProvider.Instance.GetEncoding(1252)!;
        string everyCharacter = cp1252.GetString([.. Enumerable.Range(0x20, 0xE0).Select(static b => (byte)b)]);
        const string tableName = "Œuvres €";
        const string columnName = "Prix ƒ ™ Ÿ";

        await using MemoryStream ms = await CreateJet3TableAsync();
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await writer.CreateTableAsync(tableName, [new("Id", typeof(int)), new(columnName, typeof(string), maxLength: 255), new("Memo", typeof(string))], [new IndexDefinition("Ix ‰", "Id")], Ct);
            await writer.InsertRowAsync(tableName, [1, everyCharacter, everyCharacter], Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync(tableName, RowCriteria.Where("Id", 1), new RowValues { ["Memo"] = everyCharacter + everyCharacter }, Ct));
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Contains(tableName, await reader.ListTablesAsync(Ct));
        DataRow row = Assert.Single((await reader.ReadDataTableAsync(tableName, cancellationToken: Ct)).AsEnumerable());
        Assert.Equal(everyCharacter, row[columnName]);
        Assert.Equal(everyCharacter + everyCharacter, row["Memo"]);
        Assert.Contains(await reader.ListIndexesAsync(tableName, Ct), i => i.Name == "Ix ‰");
    }

    /// <summary>
    /// A Jet3 file from an earlier build, whose unmasked header reads as an
    /// unknown code page, keeps storing UTF-8, which holds every string.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LegacyUtf8Jet3File_StillStoresTextOutsideCp1252()
    {
        await using MemoryStream ms = await CreateJet3TableAsync();
        ms.Position = 0x18;
        ms.Write(new byte[0x96 - 0x18]);

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            Assert.Equal(65001, writer.CodePage);
            await writer.CreateTableAsync("顧客", [new("Id", typeof(int)), new("氏名", typeof(string), maxLength: 50)], Ct);
            await writer.InsertRowAsync("顧客", [1, "Łódź 中文"], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Contains("顧客", await reader.ListTablesAsync(Ct));
        Assert.Equal("Łódź 中文", Assert.Single((await reader.ReadDataTableAsync("顧客", cancellationToken: Ct)).AsEnumerable())["氏名"]);
    }

    private static int ColumnIndex(string column) => column switch
    {
        "Name" => 1,
        "Notes" => 2,
        "Code" => 3,
        _ => throw new ArgumentOutOfRangeException(nameof(column), column, null),
    };

    private static string Format(DataRow row) => $"{row["Id"]}|{row["Name"]}|{row["Notes"]}|{row["Code"]}";

    private static async Task AssertRefusedAsync(string column, Func<Task> write)
    {
        JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(write);
        Assert.Contains($"'{column}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(Cp1252Message, ex.Message, StringComparison.Ordinal);
    }

    private static async Task AssertTableUnchangedAsync(MemoryStream ms)
    {
        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["T"], await reader.ListTablesAsync(Ct));
        Assert.Empty(await reader.ListLinkedTablesAsync(Ct));
        Assert.Equal(["Id", "Name", "Notes", "Code"], (await reader.GetColumnMetadataAsync("T", Ct)).Select(c => c.Name));
        Assert.Equal("1|one|notes one|A1", Format(Assert.Single((await reader.ReadDataTableAsync("T", cancellationToken: Ct)).AsEnumerable())));
    }

    private static async Task<MemoryStream> CreateJet3TableAsync()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 50), new("Notes", typeof(string)), new("Code", typeof(string), maxLength: 10)],
                [new IndexDefinition("IxCode", "Code")],
                Ct);
            await writer.InsertRowAsync("T", [1, "one", "notes one", "A1"], Ct);
        }

        return ms;
    }

    private static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }

    private static async Task<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }
}
