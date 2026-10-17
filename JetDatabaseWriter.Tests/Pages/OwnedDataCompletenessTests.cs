namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Missing owned content cannot become an apparently complete read or destructive write-back.</summary>
public sealed class OwnedDataCompletenessTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UnrecognizedMappedDataPage_StrictReadsRefuseBeforeReturningSubset(DatabaseFormat format)
    {
        Image image = await CreateImageAsync(format);
        image.Bytes[checked((int)(image.DamagedPage * image.PageSize))] = 0x7F;
        await using var stream = new MemoryStream(image.Bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = true }, cancellationToken: Ct);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            {
                using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
            });
            Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
            Assert.Equal(image.DamagedPage, failure.ErrorInfo.PageNumber);
            Assert.False(string.IsNullOrEmpty(failure.ErrorInfo.Reason));
        }

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetRealRowCountAsync("Items", Ct));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            using DataTable rows = await reader.ReadTableAsStringsAsync("Items", cancellationToken: Ct);
        });
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UnrecognizedMappedDataPage_LenientReadWarnsAndCompleteWalkRefuses(DatabaseFormat format)
    {
        Image image = await CreateImageAsync(format);
        image.Bytes[checked((int)(image.DamagedPage * image.PageSize))] = 0x7F;
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        using var scope = new TestTraceScope(listener);
        await using var stream = new MemoryStream(image.Bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = false, DiagnosticsEnabled = false }, cancellationToken: Ct);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            messages.GetStringBuilder().Clear();
            using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
            Assert.Equal(image.UnaffectedIds, rows.Rows.Cast<DataRow>().Select(row => Assert.IsType<int>(row["Id"])).Order());
            listener.Flush();
            Assert.Contains("incomplete", messages.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Contains(image.DamagedPage.ToString(CultureInfo.InvariantCulture), messages.ToString(), StringComparison.Ordinal);
        }

        int visited = 0;
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.Database.OwnedPages.ForEachLiveTableRowAsync(
                image.Root,
                (_, _) =>
                {
                    visited++;
                    return ValueTask.FromResult(true);
                },
                Ct,
                requireCompleteRows: true));
        Assert.Equal(0, visited);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task UnrecognizedMappedDataPage_UpdateRefusesWithoutChangingSource(DatabaseFormat format, bool unreadableMap)
    {
        Image image = await CreateImageAsync(format);
        if (unreadableMap)
        {
            int mapOffset = JetFormat.ForNewDatabase(format).TDef.UsedPages;
            image.Bytes.AsSpan(checked((int)(image.Root * image.PageSize)) + mapOffset, 4).Clear();
        }

        image.Bytes[checked((int)(image.DamagedPage * image.PageSize))] = 0x7F;
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image.Bytes, Ct);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await writer.UpdateRowsAsync("Items", "Id", image.UnaffectedIds[^1], new Dictionary<string, object?> { ["Pad"] = "updated" }, Ct));
        }

        Assert.Equal(image.Bytes, stream.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnrecognizedPageWrite_InvalidatesPreviouslyCachedOwnedPages(bool unreadableMap)
    {
        Image image = await CreateImageAsync(DatabaseFormat.AceAccdb);
        if (unreadableMap)
        {
            int mapOffset = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb).TDef.UsedPages;
            image.Bytes.AsSpan(checked((int)(image.Root * image.PageSize)) + mapOffset, 4).Clear();
        }

        await using var stream = new MemoryStream();
        await stream.WriteAsync(image.Bytes, Ct);
        stream.Position = 0;
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        Assert.NotEmpty(await writer.Database.OwnedPages.GetOwnedDataPagesAsync(image.Root, Ct));
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(image.DamagedPage, Ct);
        page[0] = 0x7F;

        await writer.Pager.WritePageAsync(image.DamagedPage, page, Ct);

        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await writer.Database.OwnedPages.GetOwnedDataPagesAsync(image.Root, Ct));
        Assert.Equal(image.DamagedPage, failure.ErrorInfo.PageNumber);

        page[0] = Constants.PageTypes.Data;
        await writer.Pager.WritePageAsync(image.DamagedPage, page, Ct);
        Assert.Contains(image.DamagedPage, await writer.Database.OwnedPages.GetOwnedDataPagesAsync(image.Root, Ct));
    }

    [Fact]
    public async Task CachedOwnedPages_PreserveCancellation()
    {
        Image image = await CreateImageAsync(DatabaseFormat.AceAccdb);
        await using var stream = new MemoryStream(image.Bytes, writable: false);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.NotEmpty(await reader.Database.OwnedPages.GetOwnedDataPagesAsync(image.Root, Ct));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await reader.Database.OwnedPages.GetOwnedDataPagesAsync(image.Root, new CancellationToken(canceled: true)));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 0U)]
    [InlineData(DatabaseFormat.Jet3Mdb, 1U)]
    [InlineData(DatabaseFormat.Jet3Mdb, 1000U)]
    [InlineData(DatabaseFormat.Jet4Mdb, 0U)]
    [InlineData(DatabaseFormat.Jet4Mdb, 1U)]
    [InlineData(DatabaseFormat.Jet4Mdb, 1000U)]
    [InlineData(DatabaseFormat.AceAccdb, 0U)]
    [InlineData(DatabaseFormat.AceAccdb, 1U)]
    [InlineData(DatabaseFormat.AceAccdb, 1000U)]
    public async Task StaleRowCountHint_DoesNotDiscardOrRefuseReadableRows(DatabaseFormat format, uint count)
    {
        Image image = await CreateImageAsync(format);
        int countOffset = format == DatabaseFormat.Jet3Mdb ? 12 : 16;
        BinaryPrimitives.WriteUInt32LittleEndian(image.Bytes.AsSpan(checked((int)(image.Root * image.PageSize)) + countOffset), count);
        await using var stream = new MemoryStream(image.Bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = true }, cancellationToken: Ct);

        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);

        Assert.Equal(Enumerable.Range(1, 100), rows.Rows.Cast<DataRow>().Select(row => Assert.IsType<int>(row["Id"])).Order());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task UnrecognizedOwnedPage_UnreadableMapFallbackRetainsIncompleteEvidence(DatabaseFormat format, bool strict)
    {
        Image image = await CreateImageAsync(format);
        image.Bytes[checked((int)(image.DamagedPage * image.PageSize))] = 0x7F;
        int mapOffset = JetFormat.ForNewDatabase(format).TDef.UsedPages;
        image.Bytes.AsSpan(checked((int)(image.Root * image.PageSize)) + mapOffset, 4).Clear();
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        using var scope = new TestTraceScope(listener);
        await using var stream = new MemoryStream(image.Bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, cancellationToken: Ct);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            if (strict)
            {
                JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                {
                    using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
                });
                Assert.Equal(image.DamagedPage, failure.ErrorInfo.PageNumber);
            }
            else
            {
                messages.GetStringBuilder().Clear();
                using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
                Assert.Equal(image.UnaffectedIds, rows.Rows.Cast<DataRow>().Select(row => Assert.IsType<int>(row["Id"])).Order());
                listener.Flush();
                Assert.Contains("incomplete", messages.ToString(), StringComparison.OrdinalIgnoreCase);
                Assert.Contains(image.DamagedPage.ToString(CultureInfo.InvariantCulture), messages.ToString(), StringComparison.Ordinal);
            }
        }

        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.Database.OwnedPages.ForEachLiveTableRowAsync(image.Root, (_, _) => ValueTask.FromResult(true), Ct, requireCompleteRows: true));
    }

    private static async Task<Image> CreateImageAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        long root;
        long damagedPage;
        int pageSize;
        int[] unaffected;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Pad", typeof(string), 180)], Ct);
            await writer.InsertRowsAsync("Items", Enumerable.Range(1, 100).Select(id => new object?[] { id, new string('p', 140) }), Ct);
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct));
            root = entry.TDefPage;
            pageSize = writer.PageSize;
            IReadOnlyList<long> owned = await writer.Database.OwnedPages.GetOwnedDataPagesAsync(root, Ct);
            Assert.True(owned.Count > 1);
            damagedPage = owned[0];
            byte[] page = await writer.Database.Pages.ReadPageCopyAsync(damagedPage, Ct);
            int[] affected = [.. DataPageRows.ComputeRowDirectory(writer.Database.Format, page).Select(row => BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(row.RowStart + writer.Database.Format.RowFields.NumCols)))];
            unaffected = [.. Enumerable.Range(1, 100).Except(affected)];
        }

        return new Image(stream.ToArray(), root, damagedPage, pageSize, unaffected);
    }

    private sealed record Image(byte[] Bytes, long Root, long DamagedPage, int PageSize, int[] UnaffectedIds);
}
