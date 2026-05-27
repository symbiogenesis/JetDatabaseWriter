namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for linked text/CSV table entries — MSysObjects type 6 with a
/// <c>Connect</c> string that identifies a text-file driver (e.g.
/// <c>"Text;HDR=YES;FMT=Delimited"</c>). This is the remaining linked-table
/// variant not exercised by <see cref="LinkedTableTests"/> or
/// <see cref="LinkedTableTypeTests"/> (which cover Access-linked and
/// ODBC-linked entries respectively).
/// </summary>
public sealed class LinkedTextTableTests : IDisposable
{
    private readonly List<string> _tempFiles = [];
    private readonly List<string> _tempDirectories = [];

    public static TheoryData<string, string, string, string> SupportedTextFormatCases => new()
    {
        { "Text;HDR=YES;FMT=Delimited", "A,B\r\n1,2\r\n", "B", "2" },
        { "Text;HDR=YES;FMT=CSVDelimited", "A,B\r\n\"1,1\",2\r\n", "A", "1,1" },
        { "Text;FMT=TabDelimited;HDR=YES", "A\tB\r\n1\t2\r\n", "B", "2" },
        { "Text;FMT=Delimited(;);HDR=YES;IMEX=2", "A;B\r\n1;2\r\n", "B", "2" },
    };

    public static TheoryData<string, string> UnsupportedTextFormatCases => new()
    {
        { "Text;HDR=YES;FMT=FixedLength", "FixedLength" },
        { "Text;HDR=YES;FMT=LotusDelimited", "LotusDelimited" },
    };

    public static TheoryData<string, Encoding, bool> LinkedTextEncodingCases => new()
    {
        { "UTF-8 with BOM", new UTF8Encoding(encoderShouldEmitUTF8Identifier: true), true },
        { "UTF-8 without BOM", new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), false },
        { "UTF-16 LE with BOM", Encoding.Unicode, true },
        { "UTF-16 BE with BOM", Encoding.BigEndianUnicode, true },
    };

    private static async ValueTask WriteEncodedTextAsync(
        string path,
        string text,
        Encoding encoding,
        bool includePreamble,
        CancellationToken cancellationToken)
    {
        byte[] preamble = includePreamble ? encoding.GetPreamble() : [];
        byte[] payload = encoding.GetBytes(text);
        byte[] bytes = new byte[preamble.Length + payload.Length];
        Buffer.BlockCopy(preamble, 0, bytes, 0, preamble.Length);
        Buffer.BlockCopy(payload, 0, bytes, preamble.Length, payload.Length);
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);
    }

    [Fact]
    public async Task LinkedTextTable_CreateViaSchemaInterface_ReturnsEntryWithConnectString()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkSchema");
        const string connect = "Text;HDR=YES;FMT=Delimited";

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken))
        {
#pragma warning disable CA1859 // Intentionally use interface type to test that the method is exposed there
            IAccessSchema schema = writer;
#pragma warning restore CA1859 // Intentionally use interface type to test that the method is exposed there
            await schema.CreateLinkedTextTableAsync(
                "LinkedCsvData",
                @"C:\Data\Exports",
                "sales.csv",
                connect,
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        LinkedTableInfo entry = Assert.Single(linked, l =>
            string.Equals(l.Name, "LinkedCsvData", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(LinkedTableKind.Text, entry.Kind);
        Assert.Equal("sales.csv", entry.SourceObjectName);
        Assert.Equal(@"C:\Data\Exports", entry.SourcePath);
        Assert.Equal(connect, entry.ConnectString);
    }

    [Fact]
    public async Task LinkedTextTable_ListLinkedTables_ReturnsEntryWithConnectString()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkFE");
        const string connect = "Text;HDR=YES;FMT=Delimited";

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedCsvData",
                @"C:\Data\Exports",
                "sales.csv",
                connect,
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        LinkedTableInfo? entry = linked.FirstOrDefault(l =>
            string.Equals(l.Name, "LinkedCsvData", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(entry);
        Assert.Equal(LinkedTableKind.Text, entry.Kind);
        Assert.Equal("sales.csv", entry.SourceObjectName);
        Assert.Equal(@"C:\Data\Exports", entry.SourcePath);
        Assert.Equal(connect, entry.ConnectString);
    }

    [Fact]
    public async Task LinkedTextTable_ListLinkedTables_DistinguishesFromAccessLinked()
    {
        string sourcePath = await CreateTempAccdbDatabaseAsync("TextLinkSrc");
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkMix");
        const string textConnect = "Text;HDR=YES;FMT=FixedLength";

        await using (var writer = await AccessWriter.OpenAsync(sourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Products",
                [new("Id", typeof(int))],
                TestContext.Current.CancellationToken);
        }

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken))
        {
            // Access-linked entry (no Connect string)
            await writer.CreateLinkedTableAsync(
                "LinkedProducts",
                sourcePath,
                "Products",
                TestContext.Current.CancellationToken);

            // Text-linked entry (has Connect string)
            await writer.CreateLinkedTextTableAsync(
                "LinkedLogFile",
                @"C:\Logs",
                "app.log",
                textConnect,
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, linked.Count);

        LinkedTableInfo accessLinked = linked.Single(l =>
            string.Equals(l.Name, "LinkedProducts", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(LinkedTableKind.Access, accessLinked.Kind);
        Assert.Null(accessLinked.ConnectString);

        LinkedTableInfo textLinked = linked.Single(l =>
            string.Equals(l.Name, "LinkedLogFile", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(LinkedTableKind.Text, textLinked.Kind);
        Assert.Equal(textConnect, textLinked.ConnectString);
        Assert.Equal("app.log", textLinked.SourceObjectName);
    }

    [Fact]
    public async Task LinkedTextTable_ListTables_ExcludesTextLinkedEntries()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkExclude");

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedCsv",
                @"C:\Data",
                "report.csv",
                "Text;HDR=YES;FMT=Delimited",
                TestContext.Current.CancellationToken);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.DoesNotContain("LinkedCsv", tables);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_ReadsDelimitedRowsThroughManagedReader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkCsvFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"orders_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(
            csvPath,
            "OrderId,Customer,Note\r\n1,\"Ada, Inc.\",\"He said \"\"hi\"\"\"\r\n2,Grace,\"line\r\nbreak\"\r\n",
            ct);

        const string connect = "Text;HDR=YES;FMT=Delimited";

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedOrdersCsv",
                sourceDirectory,
                csvFileName,
                connect,
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(ct);

        LinkedTableInfo entry = Assert.Single(linked, table =>
            string.Equals(table.Name, "LinkedOrdersCsv", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(LinkedTableKind.Text, entry.Kind);
        Assert.Equal(csvFileName, entry.SourceObjectName);
        Assert.Equal(sourceDirectory, entry.SourcePath);
        Assert.Equal(connect, entry.ConnectString);

        List<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("LinkedOrdersCsv", ct);
        Assert.Collection(
            metadata,
            column => Assert.Equal("OrderId", column.Name),
            column => Assert.Equal("Customer", column.Name),
            column => Assert.Equal("Note", column.Name));
        Assert.All(metadata, column => Assert.Equal(typeof(string), column.ClrType));

        long realRowCount = await reader.GetRealRowCountAsync("LinkedOrdersCsv", ct);
        Assert.Equal(2, realRowCount);

        DataTable table = await reader.ReadDataTableAsync("LinkedOrdersCsv", cancellationToken: ct);
        Assert.Equal(3, table.Columns.Count);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal("Ada, Inc.", table.Rows[0]["Customer"]);
        Assert.Equal("He said \"hi\"", table.Rows[0]["Note"]);
        Assert.Equal("line\r\nbreak", table.Rows[1]["Note"]);

        DataTable preview = await reader.ReadTableAsStringsAsync("LinkedOrdersCsv", maxRows: 1, cancellationToken: ct);
        Assert.Equal(1, preview.Rows.Count);
        Assert.Equal("Ada, Inc.", preview.Rows[0]["Customer"]);

        var stringRows = new List<string[]>();
        await foreach (string[] row in reader.RowsAsStrings("LinkedOrdersCsv", cancellationToken: ct))
        {
            stringRows.Add(row);
        }

        Assert.Equal(2, stringRows.Count);
        Assert.Equal("Grace", stringRows[1][1]);

        var objectRows = new List<object[]>();
        await foreach (object[] row in reader.Rows("LinkedOrdersCsv", cancellationToken: ct))
        {
            objectRows.Add(row);
        }

        Assert.Equal(2, objectRows.Count);
        Assert.Equal("Ada, Inc.", objectRows[0][1]);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_WithoutHeader_UsesGeneratedColumnNames()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkCsvNoHeaderFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"customers_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "1,Ada\r\n2,Grace\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedCustomersCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=NO;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        long realRowCount = await reader.GetRealRowCountAsync("LinkedCustomersCsv", ct);
        Assert.Equal(2, realRowCount);

        DataTable table = await reader.ReadDataTableAsync("LinkedCustomersCsv", cancellationToken: ct);

        Assert.Equal("F1", table.Columns[0].ColumnName);
        Assert.Equal("F2", table.Columns[1].ColumnName);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal("1", table.Rows[0]["F1"]);
        Assert.Equal("Ada", table.Rows[0]["F2"]);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_CustomDelimiter_ReadsDelimitedRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkCsvSemicolonFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"orders_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id;Customer;Note\r\n1;Ada;\"uses;delimiter\"\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedSemicolonCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited(;)",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        DataTable table = await reader.ReadDataTableAsync("LinkedSemicolonCsv", cancellationToken: ct);

        Assert.Equal(3, table.Columns.Count);
        Assert.Equal("Customer", table.Columns[1].ColumnName);
        Assert.Equal("Ada", table.Rows[0]["Customer"]);
        Assert.Equal("uses;delimiter", table.Rows[0]["Note"]);
    }

    [Theory]
    [MemberData(nameof(SupportedTextFormatCases))]
    public async Task LinkedTextTable_CsvFile_SupportedFormats_ReadDelimitedRows(
        string connectString,
        string sourceText,
        string expectedColumnName,
        string expectedValue)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkFormatFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"format_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, sourceText, ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedFormatCsv",
                sourceDirectory,
                csvFileName,
                connectString,
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        DataTable table = await reader.ReadDataTableAsync("LinkedFormatCsv", cancellationToken: ct);

        DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.True(table.Columns.Contains(expectedColumnName));
        Assert.Equal(expectedValue, row[expectedColumnName]);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_CarriageReturnLineEndings_ReadsDelimitedRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkCsvCrFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"orders_cr_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Customer\r1,Ada\r2,Grace", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedCarriageReturnCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        DataTable table = await reader.ReadDataTableAsync("LinkedCarriageReturnCsv", cancellationToken: ct);

        Assert.Equal(2, table.Rows.Count);
        Assert.Equal("Ada", table.Rows[0]["Customer"]);
        Assert.Equal("Grace", table.Rows[1]["Customer"]);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_SeparatorsOnlyRow_MaterializesEmptyFields()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkEmptyFieldsFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"empty_fields_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "A,B,C,D\r\n,,,\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedEmptyFieldsCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        DataTable table = await reader.ReadDataTableAsync("LinkedEmptyFieldsCsv", cancellationToken: ct);

        DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal(string.Empty, row["A"]);
        Assert.Equal(string.Empty, row["B"]);
        Assert.Equal(string.Empty, row["C"]);
        Assert.Equal(string.Empty, row["D"]);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_RaggedRows_NormalizesToHeaderWidth()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkRaggedRowsFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"ragged_rows_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "A,B,C\r\n1,2\r\n3,4,5,6\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedRaggedRowsCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);

        List<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("LinkedRaggedRowsCsv", ct);
        Assert.Equal(["A", "B", "C"], metadata.Select(column => column.Name).ToArray());

        long realRowCount = await reader.GetRealRowCountAsync("LinkedRaggedRowsCsv", ct);
        Assert.Equal(2, realRowCount);

        var stringRows = new List<string[]>();
        await foreach (string[] row in reader.RowsAsStrings("LinkedRaggedRowsCsv", cancellationToken: ct))
        {
            stringRows.Add(row);
        }

        Assert.Collection(
            stringRows,
            row => Assert.Equal(["1", "2", string.Empty], row),
            row => Assert.Equal(["3", "4", "5"], row));

        DataTable table = await reader.ReadDataTableAsync("LinkedRaggedRowsCsv", cancellationToken: ct);
        Assert.Equal(3, table.Columns.Count);
        Assert.Equal(string.Empty, table.Rows[0]["C"]);
        Assert.Equal("5", table.Rows[1]["C"]);
    }

    [Theory]
    [MemberData(nameof(LinkedTextEncodingCases))]
    public async Task LinkedTextTable_CsvFile_BomAndEncoding_ReadsNonAsciiData(
        string encodingName,
        Encoding encoding,
        bool includePreamble)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(encodingName);
        ArgumentNullException.ThrowIfNull(encoding);

        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkEncodingFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"encoding_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);

        const string firstName = "Zo\u00EB";
        const string firstCity = "M\u00FCnchen";
        const string firstNote = "quoted \u03A9 and \u4E2D";
        const string secondName = "Jalape\u00F1o";
        const string secondCity = "S\u00E3o Paulo";
        const string secondNote = "unquoted-\u00E9";
        string sourceText =
            $"Name,City,Note\r\n{firstName},\"{firstCity}\",\"{firstNote}\"\r\n{secondName},{secondCity},{secondNote}\r\n";
        await WriteEncodedTextAsync(csvPath, sourceText, encoding, includePreamble, ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedEncodingCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        List<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("LinkedEncodingCsv", ct);

        Assert.Equal(["Name", "City", "Note"], metadata.Select(column => column.Name).ToArray());

        DataTable table = await reader.ReadDataTableAsync("LinkedEncodingCsv", cancellationToken: ct);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal(firstName, table.Rows[0]["Name"]);
        Assert.Equal(firstCity, table.Rows[0]["City"]);
        Assert.Equal(firstNote, table.Rows[0]["Note"]);
        Assert.Equal(secondName, table.Rows[1]["Name"]);
        Assert.Equal(secondCity, table.Rows[1]["City"]);
        Assert.Equal(secondNote, table.Rows[1]["Note"]);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_ValueWhitespace_MatchesDaoTrimming()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkValueWhitespaceFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"value_whitespace_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(
            csvPath,
            "Id,Unquoted,Quoted,AfterQuote,LeadingSpaceBeforeQuote\r\n1,  unquoted  ,\"  quoted  \",\"closed\"  , \"not-starting-quote\" \r\n",
            ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedValueWhitespaceCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        DataTable table = await reader.ReadDataTableAsync("LinkedValueWhitespaceCsv", cancellationToken: ct);

        DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal("unquoted", row["Unquoted"]);
        Assert.Equal("  quoted", row["Quoted"]);
        Assert.Equal("closed", row["AfterQuote"]);
        Assert.Equal("not-starting-quote", row["LeadingSpaceBeforeQuote"]);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_FieldLengthBudget_ThrowsInvalidData()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkFieldBudgetFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"field_budget_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Note\r\n1,abcdef\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedFieldBudgetCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxFieldLength = 4 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadDataTableAsync("LinkedFieldBudgetCsv", cancellationToken: ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxFieldLength), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_RecordLengthBudget_ThrowsInvalidData()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkRecordBudgetFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"record_budget_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Note\r\n1,record-is-too-long\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedRecordBudgetCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxRecordLength = 10 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        InvalidDataException countException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.GetRealRowCountAsync("LinkedRecordBudgetCsv", ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxRecordLength), countException.Message, StringComparison.Ordinal);

        InvalidDataException tableException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadDataTableAsync("LinkedRecordBudgetCsv", cancellationToken: ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxRecordLength), tableException.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_MissingClosingQuote_ThrowsInvalidData()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkMissingQuoteFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"missing_quote_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Note\r\n1,\"unterminated\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedMissingQuoteCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);

        InvalidDataException countException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.GetRealRowCountAsync("LinkedMissingQuoteCsv", ct));
        Assert.Contains("closing quote", countException.Message, StringComparison.OrdinalIgnoreCase);

        InvalidDataException tableException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadDataTableAsync("LinkedMissingQuoteCsv", cancellationToken: ct));
        Assert.Contains("closing quote", tableException.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_ColumnCountBudget_ThrowsBeforeDataTableColumns()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkColumnBudgetFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"column_budget_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "A,B,C,D\r\n1,2,3,4\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedColumnBudgetCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxColumnCount = 3 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        InvalidDataException countException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.GetRealRowCountAsync("LinkedColumnBudgetCsv", ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxColumnCount), countException.Message, StringComparison.Ordinal);

        InvalidDataException tableException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadDataTableAsync("LinkedColumnBudgetCsv", cancellationToken: ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxColumnCount), tableException.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_ManyDuplicateHeaders_NormalizesLinearly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkDuplicateHeadersFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"duplicate_headers_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        string header = string.Join(",", Enumerable.Repeat("A", 64));
        string row = string.Join(",", Enumerable.Range(1, 64));
        await File.WriteAllTextAsync(csvPath, header + "\r\n" + row + "\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedDuplicateHeadersCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxColumnCount = 128 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);
        List<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("LinkedDuplicateHeadersCsv", ct);

        Assert.Equal(64, metadata.Count);
        Assert.Equal("A", metadata[0].Name);
        Assert.Equal("A2", metadata[1].Name);
        Assert.Equal("A64", metadata[63].Name);
        Assert.Equal(64, metadata.Select(column => column.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_CancellationDuringLongQuotedRecord_ThrowsOperationCanceled()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkCancelLongRecordFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"cancel_long_record_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        string longQuotedValue = new('x', 2_000_000);
        await File.WriteAllTextAsync(csvPath, "Id,Note\r\n1,\"" + longQuotedValue + "\"\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedCancelLongRecordCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions
        {
            LinkedTextMaxFieldLength = 3_000_000,
            LinkedTextMaxRecordLength = 3_000_100,
        };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(1));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await reader.GetRealRowCountAsync("LinkedCancelLongRecordCsv", timeout.Token));
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_MaxRowsPreview_DoesNotParseOversizedLaterRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkPreviewBudgetFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"preview_budget_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Note\r\n1,ok\r\n2,oversized\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedPreviewBudgetCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxFieldLength = 8 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        DataTable preview = await reader.ReadDataTableAsync("LinkedPreviewBudgetCsv", maxRows: 1, cancellationToken: ct);
        Assert.Single(preview.Rows);
        Assert.Equal("ok", preview.Rows[0]["Note"]);

        InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadDataTableAsync("LinkedPreviewBudgetCsv", cancellationToken: ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxFieldLength), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_SourceFileSizeLimit_ThrowsInvalidData()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkSourceSizeFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"source_size_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedSourceSizeCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxSourceFileBytes = 8 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        InvalidDataException countException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.GetRealRowCountAsync("LinkedSourceSizeCsv", ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxSourceFileBytes), countException.Message, StringComparison.Ordinal);

        InvalidDataException tableException = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadDataTableAsync("LinkedSourceSizeCsv", cancellationToken: ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxSourceFileBytes), tableException.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_MaterializedRowLimit_ThrowsBeforeAddingExtraRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkMaterializedRowsFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"materialized_rows_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n2,Grace\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedMaterializedRowsCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxMaterializedRows = 1 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        DataTable preview = await reader.ReadDataTableAsync("LinkedMaterializedRowsCsv", maxRows: 1, cancellationToken: ct);
        Assert.Single(preview.Rows);

        InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadDataTableAsync("LinkedMaterializedRowsCsv", cancellationToken: ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxMaterializedRows), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_MaterializedRowLimit_AppliesToTypedReads()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkTypedRowsFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"typed_rows_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n2,Grace\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedTypedRowsCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions { LinkedTextMaxMaterializedRows = 1 };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        List<LinkedTextRow> preview = await reader.ReadTableAsync<LinkedTextRow>("LinkedTypedRowsCsv", maxRows: 1, ct);
        LinkedTextRow row = Assert.Single(preview);
        Assert.Equal("Ada", row.Name);

        InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadTableAsync<LinkedTextRow>("LinkedTypedRowsCsv", cancellationToken: ct));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxMaterializedRows), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_SourceDirectoryReparsePoint_IsBlockedByDefault()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string hostDirectory = CreateTempDirectory("TextLinkReparseHost");
        string targetDirectory = CreateTempDirectory("TextLinkReparseTarget");
        string linkDirectory = Path.Combine(hostDirectory, "LinkedSource");
        if (!TryCreateDirectorySymlink(linkDirectory, targetDirectory))
        {
            Assert.Skip("Directory symbolic links are unavailable on this machine.");
        }

        string frontEndPath = await CreateTempAccdbDatabaseInDirectoryAsync("TextLinkReparseFE", hostDirectory);
        string csvFileName = $"reparse_source_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(targetDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedReparseCsv",
                linkDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);

        UnauthorizedAccessException exception = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await reader.ReadDataTableAsync("LinkedReparseCsv", cancellationToken: ct));
        Assert.Contains("reparse", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_PathValidatorMutation_DoesNotPoisonLinkedTableCache()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkValidatorMutationFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"validator_mutation_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedValidatorMutationCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        var options = new AccessReaderOptions
        {
            LinkedSourcePathValidator = (link, _) =>
            {
                link.Name = "Poisoned";
                link.SourcePath = @"C:\Blocked";
                link.SourceObjectName = "blocked.csv";
                return true;
            },
        };
        await using var reader = await AccessReader.OpenAsync(frontEndPath, options, ct);

        DataTable table = await reader.ReadDataTableAsync("LinkedValidatorMutationCsv", cancellationToken: ct);
        Assert.Single(table.Rows);

        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(ct);
        LinkedTableInfo entry = Assert.Single(linked, link =>
            string.Equals(link.Name, "LinkedValidatorMutationCsv", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(sourceDirectory, entry.SourcePath);
        Assert.Equal(csvFileName, entry.SourceObjectName);
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_RelativeForeignNameTraversal_IsBlockedByDefault()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string hostDirectory = CreateTempDirectory("TextLinkTraversalHost");
        string frontEndPath = await CreateTempAccdbDatabaseInDirectoryAsync("TextLinkTraversalFE", hostDirectory);
        string outsideFileName = $"outside_{Guid.NewGuid():N}.csv";

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedEscapedCsv",
                hostDirectory,
                Path.Combine("..", outsideFileName),
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await reader.ReadDataTableAsync("LinkedEscapedCsv", cancellationToken: ct));
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_StreamHostWithoutPathPolicy_IsBlockedByDefault()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourceDirectory = CreateTempDirectory("TextLinkStreamSource");
        string csvFileName = $"data_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n", ct);

        await using var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            ct))
        {
        }

        stream.Position = 0;
        await using (var writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedStreamCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        stream.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            ct);

        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await reader.ReadDataTableAsync("LinkedStreamCsv", cancellationToken: ct));
    }

    [Fact]
    public async Task LinkedTextTable_CsvFile_StreamHostWithAllowlistedSourceDirectory_ReadsDelimitedRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourceDirectory = CreateTempDirectory("TextLinkStreamAllowedSource");
        string csvFileName = $"data_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n", ct);

        await using var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            ct))
        {
        }

        stream.Position = 0;
        await using (var writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedAllowedCsv",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=Delimited",
                ct);
        }

        stream.Position = 0;
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            LinkedSourcePathAllowlist = [sourceDirectory],
        };
        await using var reader = await AccessReader.OpenAsync(stream, options, leaveOpen: true, ct);

        DataTable table = await reader.ReadDataTableAsync("LinkedAllowedCsv", cancellationToken: ct);

        Assert.Single(table.Rows);
        Assert.Equal("Ada", table.Rows[0]["Name"]);
    }

    [Theory]
    [MemberData(nameof(UnsupportedTextFormatCases))]
    public async Task LinkedTextTable_CsvFile_UnsupportedFormat_ThrowsNotSupported(string connectString, string expectedFormat)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkUnsupportedFormatFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"unsupported_format_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "Id,Name\r\n1,Ada\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedUnsupportedFormatText",
                sourceDirectory,
                csvFileName,
                connectString,
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);

        NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await reader.ReadDataTableAsync("LinkedUnsupportedFormatText", cancellationToken: ct));
        Assert.Contains(expectedFormat, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LinkedTextTable_CreateLinkedTextTableAsync_DuplicateLocalTableName_Throws()
    {
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkDup");

        await using var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(
            "LocalTable",
            [new("Id", typeof(int))],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.CreateLinkedTextTableAsync(
                "LocalTable",
                @"C:\Data",
                "data.csv",
                "Text;HDR=YES;FMT=Delimited",
                TestContext.Current.CancellationToken).AsTask());
    }

    public void Dispose()
    {
        foreach (string path in _tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }

        foreach (string path in _tempDirectories)
        {
            try
            {
                Directory.Delete(path, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
            catch (UnauthorizedAccessException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private string CreateTempDirectory(string prefix)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        _tempDirectories.Add(directory);
        return directory;
    }

    private bool TryCreateDirectorySymlink(string linkPath, string targetPath)
    {
        try
        {
            Directory.CreateSymbolicLink(linkPath, targetPath);
            _tempDirectories.Add(linkPath);
            return (File.GetAttributes(linkPath) & FileAttributes.ReparsePoint) != 0;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return false;
        }
    }

    private async ValueTask<string> CreateTempAccdbDatabaseAsync(string prefix)
    {
        string temp = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.accdb");
        await using (await AccessWriter.CreateDatabaseAsync(
            temp,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
        }

        _tempFiles.Add(temp);
        return temp;
    }

    private async ValueTask<string> CreateTempAccdbDatabaseInDirectoryAsync(string prefix, string directory)
    {
        string temp = Path.Combine(directory, $"{prefix}_{Guid.NewGuid():N}.accdb");
        await using (await AccessWriter.CreateDatabaseAsync(
            temp,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
        }

        _tempFiles.Add(temp);
        return temp;
    }

    private sealed class LinkedTextRow
    {
        public string Id { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;
    }
}
