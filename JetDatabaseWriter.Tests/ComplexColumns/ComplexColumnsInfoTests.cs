namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
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
    private static Dictionary<string, string> ExpectedWriterTypeNames => new(StringComparer.Ordinal)
    {
        ["Id"] = "Long Integer",
        ["Files"] = "Attachment",
        ["Tags"] = "Multi-value Long Integer",
        ["Labels"] = "Multi-value Text",
        ["Amounts"] = "Multi-value Decimal",
        ["Keys"] = "Multi-value GUID",
    };

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

    /// <summary>
    /// <see cref="ColumnMetadata.TypeName"/> names each complex column's
    /// subtype: the version-history, multi-value Text and attachment columns
    /// of complexDataTest all used to report "Attachment".
    /// </summary>
    /// <param name="path">The fixture path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task GetColumnMetadata_ComplexDataFixture_ReportsSubtypeTypeNames(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        var typeNames = (await reader.GetColumnMetadataAsync("Table1", TestContext.Current.CancellationToken))
            .ToDictionary(c => c.Name, c => c.TypeName, StringComparer.Ordinal);

        Assert.Equal("Version History", typeNames["VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880"]);
        Assert.Equal("Multi-value Text", typeNames["multi-value-data"]);
        Assert.Equal("Attachment", typeNames["attach-data"]);
        Assert.Equal("Memo", typeNames["memo-data"]);
    }

    [Fact]
    public async Task GetColumnMetadata_NorthwindAttachment_IsAttachment()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("ProductCategories", TestContext.Current.CancellationToken);

        Assert.Equal("Attachment", Assert.Single(metadata, c => c.Name == "ProductCategoryImage").TypeName);
    }

    /// <summary>
    /// Writer-created multi-value columns report their element type, keyed by
    /// ComplexID, so a renamed column keeps its name.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task GetColumnMetadata_WriterComplexColumns_ReportsSubtypeTypeNames()
    {
        byte[] database = await CreateAllKindsDatabaseAsync(rename: false);
        Assert.Equal(ExpectedWriterTypeNames, await ReadTypeNamesAsync(database));

        byte[] renamed = await CreateAllKindsDatabaseAsync(rename: true);
        Dictionary<string, string> afterRename = await ReadTypeNamesAsync(renamed);
        Assert.Equal("Multi-value Long Integer", afterRename["Counts"]);
        Assert.False(afterRename.ContainsKey("Tags"));
    }

    /// <summary>
    /// Files written before the type-template tables existed hold
    /// <c>ComplexTypeObjectID</c> 0, so the template name cannot say the kind.
    /// It then comes from the flat table's schema, for
    /// <see cref="IAccessReader.GetComplexColumnsAsync"/>, the type names and
    /// the row cells alike.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ZeroComplexTypeObjectId_KindAndTypeNameComeFromFlatTable()
    {
        byte[] database = await ClearComplexTypeObjectIdsAsync(await CreateAllKindsDatabaseAsync(rename: false));

        await using var stream = new MemoryStream(database, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync("Docs", TestContext.Current.CancellationToken);
        Assert.All(complex, c => Assert.Equal(0, c.ComplexTypeObjectId));
        Assert.Equal(ComplexColumnKind.Attachment, Assert.Single(complex, c => c.ColumnName == "Files").Kind);
        Assert.All(complex.Where(c => c.ColumnName != "Files"), c => Assert.Equal(ComplexColumnKind.MultiValue, c.Kind));

        Assert.Equal(ExpectedWriterTypeNames, await ReadTypeNamesAsync(database));

        object[] row = Assert.Single(await reader.Rows("Docs", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(["a.txt"], ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(row[1])).Select(a => a.FileName));
        Assert.Equal([7], ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(row[2])).Select(i => i.Value));
    }

    private static async ValueTask<byte[]> CreateAllKindsDatabaseAsync(bool rename)
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
                "Docs",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                    new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
                    new ColumnDefinition("Labels", typeof(object), maxLength: 50) { IsMultiValue = true, MultiValueElementType = typeof(string) },
                    new ColumnDefinition("Amounts", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal) },
                    new ColumnDefinition("Keys", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(Guid) },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Docs", new RowValues { ["Id"] = 1 }, TestContext.Current.CancellationToken);
            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            await writer.AddAttachmentAsync("Docs", "Files", key, new AttachmentInput("a.txt", [1, 2, 3]), TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Docs", "Tags", key, 7, TestContext.Current.CancellationToken);
            if (rename)
            {
                await writer.RenameColumnAsync("Docs", "Tags", "Counts", TestContext.Current.CancellationToken);
            }
        }

        return stream.ToArray();
    }

    private static async ValueTask<Dictionary<string, string>> ReadTypeNamesAsync(byte[] database)
    {
        await using var stream = new MemoryStream(database, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        return (await reader.GetColumnMetadataAsync("Docs", TestContext.Current.CancellationToken))
            .ToDictionary(c => c.Name, c => c.TypeName, StringComparer.Ordinal);
    }

    /// <summary>Sets every <c>MSysComplexColumns.ComplexTypeObjectID</c> to 0, as builds before the template tables wrote it.</summary>
    /// <param name="database">The database bytes.</param>
    /// <returns>The patched database bytes.</returns>
    private static async ValueTask<byte[]> ClearComplexTypeObjectIdsAsync(byte[] database)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(database, TestContext.Current.CancellationToken);
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            await ClearComplexTypeObjectIdsAsync(harness);
        }

        return stream.ToArray();
    }

    private static async ValueTask ClearComplexTypeObjectIdsAsync(WriterHarness harness)
    {
        DatabaseFile file = harness.Database;
        long tdefPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
        TableDef msys = Assert.IsType<TableDef>(await file.ReadTableDefAsync(tdefPage, TestContext.Current.CancellationToken));
        ColumnInfo typeObjectId = Assert.IsType<ColumnInfo>(msys.FindColumn("ComplexTypeObjectID"));
        Assert.True(typeObjectId.IsFixed);
        foreach (RowLocation location in await file.GetLiveRowLocationsAsync(tdefPage, TestContext.Current.CancellationToken))
        {
            byte[] page = await file.ReadPageCopyAsync(location.PageNumber, TestContext.Current.CancellationToken);
            page.AsSpan(location.RowStart + file.RowFields.NumCols + typeObjectId.FixedOff, 4).Clear();
            await harness.Pager.WritePageAsync(location.PageNumber, page, TestContext.Current.CancellationToken);
        }
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
