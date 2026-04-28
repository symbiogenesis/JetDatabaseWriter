namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests — schema evolution
/// (<see cref="IAccessWriter.AddColumnAsync"/> /
/// <see cref="IAccessWriter.DropColumnAsync"/> /
/// <see cref="IAccessWriter.RenameColumnAsync"/>) on parent tables that
/// already contain Attachment / MultiValue columns. See
/// <c>docs/design/complex-columns-format-notes.md</c> §4.2.
/// </summary>
public sealed class ComplexColumnsSchemaEvolutionTests
{
    [Fact]
    public async Task AddColumnAsync_OnTableWithAttachmentColumn_PreservesAttachmentData()
    {
        var ms = await CreateDbWithAttachmentAndOneFileAsync();

        await using (var writer = await AccessWriter.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.AddColumnAsync(
                "Documents",
                new ColumnDefinition("Note", typeof(string), maxLength: 100),
                TestContext.Current.CancellationToken);
        }

        await AssertAttachmentSurvivesAsync(ms, expectedColumn: "Files", expectedFileName: "notes.txt");

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync("Documents", TestContext.Current.CancellationToken);
        Assert.Contains(meta, m => string.Equals(m.Name, "Note", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task DropColumnAsync_NonComplexColumn_OnTableWithAttachment_PreservesAttachmentData()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Title", typeof(string), maxLength: 100),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Documents",
                [1, "doc one", DBNull.Value],
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("notes.txt", Encoding.UTF8.GetBytes("hi")),
                TestContext.Current.CancellationToken);

            await writer.DropColumnAsync("Documents", "Title", TestContext.Current.CancellationToken);
        }

        await AssertAttachmentSurvivesAsync(ms, expectedColumn: "Files", expectedFileName: "notes.txt");

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync("Documents", TestContext.Current.CancellationToken);
        Assert.DoesNotContain(meta, m => string.Equals(m.Name, "Title", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(meta, m => string.Equals(m.Name, "Files", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task DropColumnAsync_TheAttachmentColumnItself_RemovesMSysComplexColumnsRow()
    {
        var ms = await CreateDbWithAttachmentAndOneFileAsync();

        long originalFlatId;
        ms.Position = 0;
        await using (var probe = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            var info = await probe.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
            originalFlatId = Assert.Single(info).FlatTableId;
            Assert.True(originalFlatId > 0);
        }

        await using (var writer = await AccessWriter.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.DropColumnAsync("Documents", "Files", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        // Parent column descriptor is gone.
        var meta = await reader.GetColumnMetadataAsync("Documents", TestContext.Current.CancellationToken);
        Assert.DoesNotContain(meta, m => string.Equals(m.Name, "Files", StringComparison.OrdinalIgnoreCase));

        // Complex column metadata is gone for the parent.
        var infoAfter = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        Assert.Empty(infoAfter);

        // MSysComplexColumns no longer has the row for this column.
        DataTable? cx = await reader.ReadDataTableAsync(
            "MSysComplexColumns",
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(cx);
        bool anyForFiles = cx!.Rows.Cast<DataRow>().Any(r =>
            string.Equals(Convert.ToString(r["ColumnName"], CultureInfo.InvariantCulture), "Files", StringComparison.OrdinalIgnoreCase));
        Assert.False(anyForFiles);

        // Hidden flat-table catalog row is gone.
        Assert.DoesNotContain(
            await reader.ListTablesAsync(TestContext.Current.CancellationToken),
            n => n.Contains("_Files", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task RenameColumnAsync_NonComplexColumn_OnTableWithAttachment_PreservesAttachmentData()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Title", typeof(string), maxLength: 100),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Documents",
                [1, "doc one", DBNull.Value],
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("notes.txt", Encoding.UTF8.GetBytes("hi")),
                TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync("Documents", "Title", "Heading", TestContext.Current.CancellationToken);
        }

        await AssertAttachmentSurvivesAsync(ms, expectedColumn: "Files", expectedFileName: "notes.txt");

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync("Documents", TestContext.Current.CancellationToken);
        Assert.Contains(meta, m => string.Equals(m.Name, "Heading", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(meta, m => string.Equals(m.Name, "Title", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task RenameColumnAsync_TheAttachmentColumnItself_UpdatesMSysComplexColumnsRow()
    {
        var ms = await CreateDbWithAttachmentAndOneFileAsync();

        await using (var writer = await AccessWriter.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.RenameColumnAsync("Documents", "Files", "Attachments", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        // Parent column descriptor reflects the new name.
        var meta = await reader.GetColumnMetadataAsync("Documents", TestContext.Current.CancellationToken);
        Assert.Contains(meta, m => string.Equals(m.Name, "Attachments", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(meta, m => string.Equals(m.Name, "Files", StringComparison.OrdinalIgnoreCase));

        // GetComplexColumnsAsync surfaces the new name.
        var info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        var only = Assert.Single(info);
        Assert.Equal("Attachments", only.ColumnName);

        // GetAttachmentsAsync still returns the original payload — joined under the new column name.
        var attachments = await reader.GetAttachmentsAsync("Documents", "Attachments", TestContext.Current.CancellationToken);
        var single = Assert.Single(attachments);
        Assert.Equal("notes.txt", single.FileName);
        Assert.Equal(Encoding.UTF8.GetBytes("hi"), single.FileData);
    }

    [Fact]
    public async Task DropColumnAsync_TheMultiValueColumnItself_RemovesMSysComplexColumnsRow()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Tags",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Labels", typeof(object))
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(int),
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Tags",
                [1, DBNull.Value],
                TestContext.Current.CancellationToken);

            await writer.AddMultiValueItemAsync(
                "Tags",
                "Labels",
                new Dictionary<string, object> { ["Id"] = 1 },
                42,
                TestContext.Current.CancellationToken);

            await writer.DropColumnAsync("Tags", "Labels", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var info = await reader.GetComplexColumnsAsync("Tags", TestContext.Current.CancellationToken);
        Assert.Empty(info);

        DataTable? cx = await reader.ReadDataTableAsync("MSysComplexColumns", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(cx);
        Assert.DoesNotContain(
            cx!.Rows.Cast<DataRow>(),
            r => string.Equals(Convert.ToString(r["ColumnName"], CultureInfo.InvariantCulture), "Labels", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task AddColumnAsync_NewAttachmentColumn_OnTableThatAlreadyHasAttachment_AllocatesFreshComplexId()
    {
        var ms = await CreateDbWithAttachmentAndOneFileAsync();

        // Capture the existing column's ComplexId so we can assert the new one differs.
        int existingComplexId;
        ms.Position = 0;
        await using (var probe = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            var info = await probe.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
            existingComplexId = Assert.Single(info).ComplexId;
            Assert.True(existingComplexId > 0);
        }

        await using (var writer = await AccessWriter.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.AddColumnAsync(
                "Documents",
                new ColumnDefinition("Backup", typeof(byte[])) { IsAttachment = true },
                TestContext.Current.CancellationToken);
        }

        // Original attachment data still readable.
        await AssertAttachmentSurvivesAsync(ms, expectedColumn: "Files", expectedFileName: "notes.txt");

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        // Both columns are complex now, with distinct ComplexIds.
        var info2 = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        Assert.Equal(2, info2.Count);
        Assert.Contains(info2, c => string.Equals(c.ColumnName, "Files", StringComparison.OrdinalIgnoreCase) && c.ComplexId == existingComplexId);
        Assert.Contains(info2, c => string.Equals(c.ColumnName, "Backup", StringComparison.OrdinalIgnoreCase) && c.ComplexId != existingComplexId);
    }

    private static async Task<MemoryStream> CreateDbWithAttachmentAndOneFileAsync()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(
            "Documents",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
            ],
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync(
            "Documents",
            [1, DBNull.Value],
            TestContext.Current.CancellationToken);

        await writer.AddAttachmentAsync(
            "Documents",
            "Files",
            new Dictionary<string, object> { ["Id"] = 1 },
            new AttachmentInput("notes.txt", Encoding.UTF8.GetBytes("hi")),
            TestContext.Current.CancellationToken);

        return ms;
    }

    private static async Task AssertAttachmentSurvivesAsync(MemoryStream ms, string expectedColumn, string expectedFileName)
    {
        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var attachments = await reader.GetAttachmentsAsync("Documents", expectedColumn, TestContext.Current.CancellationToken);
        var single = Assert.Single(attachments);
        Assert.Equal(expectedFileName, single.FileName);
        Assert.Equal(Encoding.UTF8.GetBytes("hi"), single.FileData);
    }
}
