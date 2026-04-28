namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Cascade-on-delete for complex (Attachment / MultiValue) columns. When a
/// parent row is removed via <see cref="IAccessWriter.DeleteRowsAsync"/>
/// (directly or through an FK cascade), its associated rows in the hidden
/// flat child table must also be removed. See
/// <c>docs/design/complex-columns-format-notes.md</c> §4.3.
/// </summary>
public sealed class ComplexColumnsCascadeDeleteTests
{
    [Fact]
    public async Task DeleteRowsAsync_OnParentWithAttachments_RemovesFlatChildRows()
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

            await writer.InsertRowsAsync(
                "Documents",
                [
                    new object[] { 1, DBNull.Value },
                    [2, DBNull.Value],
                ],
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("doomed-a.txt", Encoding.UTF8.GetBytes("a")),
                TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("doomed-b.txt", Encoding.UTF8.GetBytes("b")),
                TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 2 },
                new AttachmentInput("survivor.txt", Encoding.UTF8.GetBytes("s")),
                TestContext.Current.CancellationToken);

            int deleted = await writer.DeleteRowsAsync(
                "Documents",
                "Id",
                1,
                TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var attachments = await reader.GetAttachmentsAsync(
            "Documents",
            "Files",
            TestContext.Current.CancellationToken);

        var single = Assert.Single(attachments);
        Assert.Equal("survivor.txt", single.FileName);
    }

    [Fact]
    public async Task DeleteRowsAsync_OnParentWithMultiValueItems_RemovesFlatChildRows()
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

            await writer.InsertRowsAsync(
                "Tags",
                [
                    new object[] { 1, DBNull.Value },
                    [2, DBNull.Value],
                ],
                TestContext.Current.CancellationToken);

            var k1 = new Dictionary<string, object> { ["Id"] = 1 };
            var k2 = new Dictionary<string, object> { ["Id"] = 2 };
            await writer.AddMultiValueItemAsync("Tags", "Labels", k1, 11, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Tags", "Labels", k1, 12, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Tags", "Labels", k2, 21, TestContext.Current.CancellationToken);

            int deleted = await writer.DeleteRowsAsync(
                "Tags",
                "Id",
                1,
                TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var items = await reader.GetMultiValueItemsAsync(
            "Tags",
            "Labels",
            TestContext.Current.CancellationToken);

        var only = Assert.Single(items);
        Assert.Equal(21, Convert.ToInt32(only.Value, System.Globalization.CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task DeleteRowsAsync_OnParentWithNullComplexSlot_DoesNothingExtra()
    {
        // Parent has a complex column but never received an attachment, so its
        // ConceptualTableID slot is null. Delete must succeed without touching
        // (or even inspecting beyond the null bit) the flat table.
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

            await writer.InsertRowsAsync(
                "Documents",
                [
                    new object[] { 1, DBNull.Value },
                    [2, DBNull.Value],
                ],
                TestContext.Current.CancellationToken);

            // Only parent #2 gets an attachment.
            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 2 },
                new AttachmentInput("keep.txt", Encoding.UTF8.GetBytes("k")),
                TestContext.Current.CancellationToken);

            int deleted = await writer.DeleteRowsAsync(
                "Documents",
                "Id",
                1,
                TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var attachments = await reader.GetAttachmentsAsync(
            "Documents",
            "Files",
            TestContext.Current.CancellationToken);

        var single = Assert.Single(attachments);
        Assert.Equal("keep.txt", single.FileName);
    }

    [Fact]
    public async Task FkCascadeDelete_AlsoCascadesComplexChildrenOnTheChildTable()
    {
        // Build: Customers (PK CustId) <- 1:N - Documents (FK CustId, complex Files)
        // Cascade-delete a Customer; the Document(s) for that customer cascade-
        // delete via the FK cascade, and their attachment rows in turn cascade-
        // delete via the complex-column cascade.
        var ms = new MemoryStream();

        // Bootstrap from an Access-authored fixture so MSysRelationships exists.
        byte[] fixture = await File.ReadAllBytesAsync(
            Path.Combine(AppContext.BaseDirectory, "Databases", "ComplexFields.accdb"),
            TestContext.Current.CancellationToken);

        // Strip the fixture's existing tables — we only need the catalog scaffolding.
        // Easiest path: just open the fixture and build our schema next to whatever
        // is already there. The fixture's own Documents table has a different
        // schema, so use distinct names.
        var fixtureMs = new MemoryStream();
        await fixtureMs.WriteAsync(fixture, TestContext.Current.CancellationToken);
        fixtureMs.Position = 0;

        await using (var writer = await AccessWriter.OpenAsync(
            fixtureMs,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "C5_Customers",
                [
                    new ColumnDefinition("CustId", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                "C5_Documents",
                [
                    new ColumnDefinition("DocId", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("CustId", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    name: "FK_C5_Documents_Customers",
                    primaryTable: "C5_Customers",
                    primaryColumn: "CustId",
                    foreignTable: "C5_Documents",
                    foreignColumn: "CustId")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "C5_Customers",
                [1, "Alice"],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "C5_Customers",
                [2, "Bob"],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "C5_Documents",
                [100, 1, DBNull.Value],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "C5_Documents",
                [200, 2, DBNull.Value],
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync(
                "C5_Documents",
                "Files",
                new Dictionary<string, object> { ["DocId"] = 100 },
                new AttachmentInput("alice.txt", Encoding.UTF8.GetBytes("alice")),
                TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync(
                "C5_Documents",
                "Files",
                new Dictionary<string, object> { ["DocId"] = 200 },
                new AttachmentInput("bob.txt", Encoding.UTF8.GetBytes("bob")),
                TestContext.Current.CancellationToken);

            // Delete Alice — cascades to her Document, which cascades to her attachment.
            int deleted = await writer.DeleteRowsAsync(
                "C5_Customers",
                "CustId",
                1,
                TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        fixtureMs.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            fixtureMs,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var attachments = await reader.GetAttachmentsAsync(
            "C5_Documents",
            "Files",
            TestContext.Current.CancellationToken);

        var single = Assert.Single(attachments);
        Assert.Equal("bob.txt", single.FileName);
    }
}
