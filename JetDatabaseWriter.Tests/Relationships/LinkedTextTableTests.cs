namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
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
            IAccessSchema schema = writer;
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
    public async Task LinkedTextTable_CsvFile_ReturnsTextMetadataAndManagedReadsAreMetadataOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string sourceDirectory = CreateTempDirectory("TextLinkCsvSource");
        string csvPath = Path.Combine(sourceDirectory, "orders.csv");
        await File.WriteAllTextAsync(csvPath, "OrderId,Customer,Total\r\n1,Ada,12.50\r\n2,Grace,18.75\r\n", ct);

        string frontEndPath = await CreateTempAccdbDatabaseAsync("TextLinkCsvFE");
        const string connect = "Text;HDR=YES;FMT=Delimited";

        await using (var writer = await AccessWriter.OpenAsync(frontEndPath, cancellationToken: ct))
        {
            await writer.CreateLinkedTextTableAsync(
                "LinkedOrdersCsv",
                sourceDirectory,
                "orders.csv",
                connect,
                ct);
        }

        await using var reader = await AccessReader.OpenAsync(frontEndPath, cancellationToken: ct);
        List<LinkedTableInfo> linked = await reader.ListLinkedTablesAsync(ct);

        LinkedTableInfo entry = Assert.Single(linked, table =>
            string.Equals(table.Name, "LinkedOrdersCsv", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(LinkedTableKind.Text, entry.Kind);
        Assert.Equal("orders.csv", entry.SourceObjectName);
        Assert.Equal(sourceDirectory, entry.SourcePath);
        Assert.Equal(connect, entry.ConnectString);

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await reader.ReadDataTableAsync("LinkedOrdersCsv", cancellationToken: ct));
        Assert.Contains("metadata-only", ex.Message, StringComparison.OrdinalIgnoreCase);
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
}
