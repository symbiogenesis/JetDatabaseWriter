namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for <see cref="IAccessReader.GetComplexColumnsAsync"/> against the
/// <c>ComplexFields.accdb</c> fixture.
/// Schema assertions are grounded in
/// <see href="docs/design/format-probe-appendix-complex.md" />.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class ComplexColumnsInfoTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task GetComplexColumns_DocumentsAttachments_ReturnsSingleAttachment()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);

        ComplexColumnInfo entry = Assert.Single(info);
        Assert.Equal("Attachments", entry.ColumnName, ignoreCase: true);
        Assert.Equal(ComplexColumnKind.Attachment, entry.Kind);

        // Per the format probe appendix, Documents.Attachments has ComplexID = 1.
        Assert.Equal(1, entry.ComplexId);

        // The hidden flat table follows the f_<32-hex>_<colName> pattern.
        Assert.StartsWith("f_", entry.FlatTableName, StringComparison.OrdinalIgnoreCase);
        Assert.EndsWith("_Attachments", entry.FlatTableName, StringComparison.OrdinalIgnoreCase);

        Assert.Equal("MSysComplexType_Attachment", entry.ComplexTypeName, ignoreCase: true);
        Assert.NotEqual(0, entry.FlatTableId);
        Assert.NotEqual(0, entry.ComplexTypeObjectId);
    }

    [Fact]
    public async Task GetComplexColumns_TableWithoutComplexColumns_ReturnsEmpty()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Tags", TestContext.Current.CancellationToken);
        Assert.Empty(info);
    }

    [Fact]
    public async Task GetComplexColumns_UnknownTable_ReturnsEmpty()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("NoSuchTable", TestContext.Current.CancellationToken);
        Assert.Empty(info);
    }

    [Fact]
    public async Task GetComplexColumns_NorthwindCategories_ResolvesAttachment()
    {
        // NorthwindTraders.accdb has multiple complex/attachment columns
        // (e.g. ProductCategories.ProductCategoryImage, Employees.Attachments).
        AccessReader reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("ProductCategories", TestContext.Current.CancellationToken);

        Assert.NotEmpty(info);
        Assert.All(info, c =>
        {
            Assert.NotEqual(0, c.ComplexId);
            Assert.False(string.IsNullOrEmpty(c.ColumnName));
            Assert.NotEqual(ComplexColumnKind.Unknown, c.Kind);
        });
    }

    [Fact]
    public async Task ComplexMetadata_WhenMSysComplexColumnsTdefIsCorrupt_FallsBackWithoutThrowing()
    {
        byte[] database = await CreateAttachmentDatabaseAsync();
        int complexColumnsTdefPage = await FindSystemTablePageAsync(database, "MSysComplexColumns");
        database[complexColumnsTdefPage * Constants.PageSizes.Jet4] = 0x00;

        await using var stream = new MemoryStream(database, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(
            "Documents",
            TestContext.Current.CancellationToken);
        ColumnMetadata files = Assert.Single(metadata, column => string.Equals(column.Name, "Files", StringComparison.Ordinal));
        Assert.Equal("Complex", files.TypeName);

        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync(
            "Documents",
            TestContext.Current.CancellationToken);
        Assert.Empty(info);
    }

    private static async ValueTask<byte[]> CreateAttachmentDatabaseAsync()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(object)) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);
        }

        return stream.ToArray();
    }

    private static async ValueTask<int> FindSystemTablePageAsync(byte[] database, string tableName)
    {
        await using var stream = new MemoryStream(database, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable objects = await reader.ReadDataTableAsync(
            "MSysObjects",
            cancellationToken: TestContext.Current.CancellationToken);
        foreach (DataRow row in objects.Rows)
        {
            string? name = Convert.ToString(row["Name"], CultureInfo.InvariantCulture);
            if (!string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            long id = Convert.ToInt64(row["Id"], CultureInfo.InvariantCulture);
            return checked((int)(id & 0x00FFFFFFL));
        }

        throw new InvalidDataException($"System table '{tableName}' was not found.");
    }
}
