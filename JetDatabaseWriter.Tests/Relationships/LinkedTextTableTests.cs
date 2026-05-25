namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
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

    [Fact]
    public async Task LinkedTextTable_CsvFile_FixedLengthFormat_ThrowsNotSupported()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkFixedLengthFE");
        string sourceDirectory = Path.GetDirectoryName(frontEndPath)!;
        string csvFileName = $"fixed_{Guid.NewGuid():N}.csv";
        string csvPath = Path.Combine(sourceDirectory, csvFileName);
        _tempFiles.Add(csvPath);
        await File.WriteAllTextAsync(csvPath, "IdName\r\n1 Ada\r\n", ct);

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedFixedLengthText",
                sourceDirectory,
                csvFileName,
                "Text;HDR=YES;FMT=FixedLength",
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);

        NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await reader.ReadDataTableAsync("LinkedFixedLengthText", cancellationToken: ct));
        Assert.Contains("FixedLength", exception.Message, StringComparison.Ordinal);
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
}
