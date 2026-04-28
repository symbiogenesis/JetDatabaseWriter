namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
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
/// When <see cref="IAccessWriter.DropTableAsync"/> drops a parent table
/// that has Attachment / MultiValue columns, the hidden flat child tables
/// and the corresponding <c>MSysComplexColumns</c> rows must be removed
/// too. See <c>docs/design/complex-columns-format-notes.md</c> §4.3.
/// </summary>
public sealed class ComplexColumnsDropTableTests
{
    [Fact]
    public async Task DropTableAsync_OnParentWithAttachments_RemovesMSysComplexColumnsRow()
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

            await writer.DropTableAsync("Documents", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        // Parent gone from user listing.
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.DoesNotContain("Documents", tables, StringComparer.OrdinalIgnoreCase);

        // The MSysComplexColumns row that joined the parent column to its flat
        // table must be gone.
        DataTable? cx = await reader.ReadDataTableAsync(
            "MSysComplexColumns",
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(cx);
        bool anyForFiles = cx!.Rows.Cast<DataRow>().Any(r =>
            string.Equals(Convert.ToString(r["ColumnName"], System.Globalization.CultureInfo.InvariantCulture), "Files", StringComparison.OrdinalIgnoreCase));
        Assert.False(anyForFiles, "MSysComplexColumns still has a row for the dropped parent's complex column.");
    }

    [Fact]
    public async Task DropTableAsync_OnParentWithComplexColumns_RemovesFlatChildTableFromCatalog()
    {
        var ms = new MemoryStream();
        long? flatTdefPage = null;

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
        }

        // Capture the flat table's TDEF page before the drop so we can verify
        // it's been removed from MSysObjects after.
        ms.Position = 0;
        await using (var probe = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            var info = await probe.GetComplexColumnsAsync("Tags", TestContext.Current.CancellationToken);
            var only = Assert.Single(info);
            flatTdefPage = only.FlatTableId;
        }

        Assert.NotNull(flatTdefPage);
        Assert.True(flatTdefPage > 0);

        ms.Position = 0;
        await using (var writer = await AccessWriter.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.DropTableAsync("Tags", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        // The flat table should no longer appear as a live row in MSysObjects.
        DataTable? msys = await reader.ReadDataTableAsync(
            "MSysObjects",
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(msys);
        long expectedId = flatTdefPage!.Value;
        bool flatStillThere = msys!.Rows.Cast<DataRow>().Any(r =>
        {
            object idObj = r["Id"];
            if (idObj is null || idObj == DBNull.Value)
            {
                return false;
            }

            long id = Convert.ToInt64(idObj, System.Globalization.CultureInfo.InvariantCulture) & 0x00FFFFFFL;
            return id == expectedId;
        });
        Assert.False(flatStillThere, "Hidden flat-table catalog row for the dropped parent's complex column was not removed.");

        DataTable? cx = await reader.ReadDataTableAsync(
            "MSysComplexColumns",
            cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(cx);
        Assert.Empty(cx!.Rows);
    }

    [Fact]
    public async Task DropTableAsync_OnTableWithoutComplexColumns_StillSucceeds()
    {
        // Regression: the cascade must be a silent no-op for ordinary tables.
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Plain",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Plain",
                [1, "abc"],
                TestContext.Current.CancellationToken);

            await writer.DropTableAsync("Plain", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.DoesNotContain("Plain", tables, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DropTableAsync_OnJet4Mdb_DoesNotThrowWhenMSysComplexColumnsAbsent()
    {
        // Jet4 .mdb files don't have MSysComplexColumns. Drop must still succeed
        // (the helper has to tolerate the missing system table).
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.Jet4Mdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Plain",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.DropTableAsync("Plain", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.DoesNotContain("Plain", tables, StringComparer.OrdinalIgnoreCase);
    }
}
