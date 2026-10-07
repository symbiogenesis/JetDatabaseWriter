namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Refuses complex-column creation when required native template metadata is missing.</summary>
public sealed class ComplexTemplateIntegrityTests
{
    /// <summary>A missing template never becomes a zero catalog reference or changes the database.</summary>
    /// <param name="mode">The write mode.</param>
    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task CreateAttachment_MissingTemplate_RefusesWithoutWriting(WriteMode mode)
    {
        await using var created = new MemoryStream();
        await using (await CreateWriterAsync(created))
        {
        }

        byte[] bytes = created.ToArray();
        await HideTemplateAsync(bytes);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
            });
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    private static async Task HideTemplateAsync(byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        DatabaseFile database = reader.Database;
        TableDef? catalog = await reader.ReadTableDefAsync(2, Ct);
        Assert.NotNull(catalog);
        var catalogRows = new CatalogRowReader(database.Format, database.TableDefs, database.OwnedPages);
        CatalogRow template = Assert.Single(await catalogRows.GetCatalogRowsAsync(catalog, Ct), row => row.Name == "MSysComplexType_Attachment");
        byte[] page = await reader.ReadPageCopyAsync(template.PageNumber, Ct);
        RowBound row = DataPageRows.EnumerateLiveRowBounds(database.Format, page).Single(bound => bound.RowIndex == template.RowIndex);
        int offset = checked((int)(template.PageNumber * database.Format.PageSize)) + row.RowStart;
        bytes.AsSpan(offset, database.Format.RowFields.NumCols).Clear();
    }
}
