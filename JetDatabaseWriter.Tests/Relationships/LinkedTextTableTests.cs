namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for linked text/CSV table entries — MSysObjects type 6 with a
/// <c>Connect</c> string that identifies a text-file driver (e.g.
/// <c>"Text;HDR=YES;FMT=Delimited"</c>). This is the remaining linked-table
/// variant not exercised by <see cref="LinkedTableTests"/> or
/// <see cref="LinkedTableTypeTests"/> (which cover Access-linked and
/// ODBC-linked entries respectively). Closes §4 of test-coverage-gaps.md.
/// </summary>
public sealed class LinkedTextTableTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

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
        Assert.False(entry.IsOdbc);
        Assert.Equal("sales.csv", entry.ForeignName);
        Assert.Equal(@"C:\Data\Exports", entry.SourceDatabasePath);
        Assert.Equal(connect, entry.ConnectionString);
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
        Assert.Null(accessLinked.ConnectionString);
        Assert.False(accessLinked.IsOdbc);

        LinkedTableInfo textLinked = linked.Single(l =>
            string.Equals(l.Name, "LinkedLogFile", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(textConnect, textLinked.ConnectionString);
        Assert.False(textLinked.IsOdbc);
        Assert.Equal("app.log", textLinked.ForeignName);
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
