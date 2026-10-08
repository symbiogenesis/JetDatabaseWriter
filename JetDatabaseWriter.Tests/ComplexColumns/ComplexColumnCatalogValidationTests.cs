namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class ComplexColumnCatalogValidationTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ComplexColumns_ReadFailure_PropagatesAndDoesNotPoisonRetry(bool strict)
    {
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        stream.FailOnRead(1);
        await Assert.ThrowsAsync<IOException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        Assert.True(stream.Faulted);
        Assert.Equal("Files", Assert.Single(await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct)).ColumnName);
    }

    [Theory]
    [InlineData(true, "ColumnName")]
    [InlineData(false, "ColumnName")]
    [InlineData(true, "ComplexID")]
    [InlineData(false, "ComplexID")]
    [InlineData(true, "FlatTableID")]
    [InlineData(false, "FlatTableID")]
    [InlineData(true, "ConceptualTableID")]
    [InlineData(false, "ConceptualTableID")]
    [InlineData(true, "ComplexTypeObjectID")]
    [InlineData(false, "ComplexTypeObjectID")]
    public async Task ComplexColumns_MissingRequiredColumn_IsCorruptionInEveryMode(bool strict, string missing)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            long pageNumber = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(pageNumber, ComplexColumnTestSupport.Ct);
            int nameOffset = page.AsSpan().IndexOf(Encoding.Unicode.GetBytes(missing));
            Assert.True(nameOffset >= 0);
            page[nameOffset] = (byte)'X';
            await harness.Pager.WritePageAsync(pageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
        Assert.Equal("MSysComplexColumns", error.ErrorInfo.TableName);
        JetCorruptDataException scanError = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            using DataTable rejected = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
        });
        Assert.Equal(JetErrorCode.CorruptCatalog, scanError.ErrorCode);
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, true)]
    [InlineData(false, false, true)]
    public async Task ComplexColumns_InvalidTemplateIdentity_DoesNotLoseValidSibling(bool strict, bool wrongKind, bool duplicate)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Broken", typeof(byte[])) { IsAttachment = true }, new ColumnDefinition("Valid", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value, DBNull.Value], ComplexColumnTestSupport.Ct);
            await writer.AddAttachmentAsync("Docs", "Broken", new System.Collections.Generic.Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("Broken.txt", [1]), ComplexColumnTestSupport.Ct);
            await writer.AddAttachmentAsync("Docs", "Valid", new System.Collections.Generic.Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("Valid.txt", [2]), ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            long pageNumber = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
            TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(pageNumber, ComplexColumnTestSupport.Ct));
            ColumnInfo column = Assert.IsType<ColumnInfo>(definition.FindColumn("ComplexTypeObjectID"));
            RowLocation location = (await harness.Database.GetLiveRowLocationsAsync(pageNumber, ComplexColumnTestSupport.Ct))[0];
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, ComplexColumnTestSupport.Ct);
            int invalidIdentity = wrongKind ? checked((int)(await harness.Services.Catalog.ResolveRequiredTableAsync("Docs", ComplexColumnTestSupport.Ct)).Entry.TDefPage) : int.MaxValue;
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + column.FixedOff, 4), invalidIdentity);
            if (duplicate)
            {
                ColumnInfo complexId = Assert.IsType<ColumnInfo>(definition.FindColumn("ComplexID"));
                BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + complexId.FixedOff, 4), 2);
            }

            await harness.Pager.WritePageAsync(location.PageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            if (strict)
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
                Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
                Assert.Equal("MSysComplexColumns", error.ErrorInfo.TableName);
                Assert.Equal("ComplexTypeObjectID", error.ErrorInfo.ColumnName);
            }
            else
            {
                ComplexColumnInfo valid = Assert.Single(await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
                Assert.Equal("Valid", valid.ColumnName);
                using DataTable table = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
                DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
                Assert.IsType<DBNull>(row["Broken"]);
                Assert.Equal("Valid.txt", Assert.Single(ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(row["Valid"]))).FileName);
            }
        }
    }
}
