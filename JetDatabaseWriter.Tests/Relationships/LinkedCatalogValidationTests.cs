namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.ComplexColumns;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class LinkedCatalogValidationTests
{
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task LinkedCatalog_AccessAndTextAliasCollision_IsRefusedWithoutCaching(bool strict, bool text)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Alias1", [new ColumnDefinition("Id", typeof(int))], ComplexColumnTestSupport.Ct);
            if (text)
            {
                await writer.CreateLinkedTextTableAsync("Alias2", @"C:\Unused", "data.csv", "Text;HDR=YES;FMT=Delimited", ComplexColumnTestSupport.Ct);
            }
            else
            {
                await writer.CreateLinkedTableAsync("Alias2", @"C:\Unused\source.accdb", "Data", ComplexColumnTestSupport.Ct);
            }
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            TableDef objects = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, ComplexColumnTestSupport.Ct));
            CatalogRow link = Assert.Single(await harness.Services.CatalogRows.GetCatalogRowsAsync(objects, ComplexColumnTestSupport.Ct), row => row.Name == "Alias2");
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(link.PageNumber, ComplexColumnTestSupport.Ct);
            int offset = page.AsSpan().IndexOf(Encoding.UTF8.GetBytes("Alias2"));
            if (offset < 0)
            {
                offset = page.AsSpan().IndexOf(Encoding.Unicode.GetBytes("Alias2"));
                Assert.True(offset >= 0);
                page[offset + 10] = (byte)'1';
            }
            else
            {
                page[offset + 5] = (byte)'1';
            }

            await harness.Pager.WritePageAsync(link.PageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ListLinkedTablesAsync(ComplexColumnTestSupport.Ct));
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ListTablesAsync(ComplexColumnTestSupport.Ct));
        }
    }

    [Theory]
    [InlineData(true, "ForeignName")]
    [InlineData(false, "ForeignName")]
    [InlineData(true, "Database")]
    [InlineData(false, "Database")]
    [InlineData(true, "Connect")]
    [InlineData(false, "Connect")]
    public async Task LinkedCatalog_MissingRequiredMetadataColumn_IsCorruption(bool strict, string missing)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateLinkedOdbcTableAsync("Remote", "ODBC;DSN=Unused", "Data", ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(2, ComplexColumnTestSupport.Ct);
            int offset = page.AsSpan().IndexOf(Encoding.Unicode.GetBytes(missing));
            Assert.True(offset >= 0);
            page[offset] = (byte)'X';
            await harness.Pager.WritePageAsync(2, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ListLinkedTablesAsync(ComplexColumnTestSupport.Ct));
            Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LinkedCatalog_ReadFailure_PropagatesAndDoesNotPoisonRetry(bool strict)
    {
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateLinkedOdbcTableAsync("Remote", "ODBC;DSN=Unused", "Data", ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        stream.FailOnRead(1);
        await Assert.ThrowsAsync<IOException>(async () => await reader.ListLinkedTablesAsync(ComplexColumnTestSupport.Ct));
        Assert.True(stream.Faulted);
        Assert.Equal("Remote", Assert.Single(await reader.ListLinkedTablesAsync(ComplexColumnTestSupport.Ct)).Name);
    }
}
