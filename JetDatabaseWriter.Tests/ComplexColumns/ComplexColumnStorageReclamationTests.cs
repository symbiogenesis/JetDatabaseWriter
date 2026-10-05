namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Checks that dropping complex artifacts releases their physical storage.</summary>
public sealed class ComplexColumnStorageReclamationTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Drop_ReleasesFlatStorageAndPreservesSibling(bool dropColumn, bool attachment)
    {
        await using var stream = new MemoryStream();
        byte[] payload = Enumerable.Range(0, 20_000).Select(index => (byte)(((index * 73) + (index / 251)) & 0xFF)).ToArray();
        await CreatePopulatedAsync(stream, attachment, payload);
        long flatPage;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            flatPage = Assert.Single(await reader.GetComplexColumnsAsync("Docs", Ct)).FlatTableId;
        }

        var pages = new HashSet<long> { flatPage };
        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            byte[] tdef = await harness.Database.Pages.ReadPageCopyAsync(flatPage, Ct);
            pages.Add(UsageMap.ReadUInt24(tdef, harness.Database.Format.TDef.UsedPagesPage));
            foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, flatPage, Ct))
            {
                if (root != 0)
                {
                    pages.UnionWith(await IndexLeafChain.ReadTreePagesAsync(harness.Database, flatPage, root, Ct));
                }
            }

            for (long pageNumber = 3; pageNumber < harness.Pager.PageCount; pageNumber++)
            {
                byte[] page = await harness.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);
                if ((page[0] == Constants.PageTypes.Data
                    && BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(4)) == flatPage)
                    || (attachment && page.AsSpan().IndexOf(payload.AsSpan(1_000, 100)) >= 0))
                {
                    pages.Add(pageNumber);
                }
            }
        }

        Assert.True(pages.Count >= 4, "The populated child must have TDEF, usage-map, data and index storage.");
        var options = new AccessWriterOptions { UseLockFile = false, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages };
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, cancellationToken: Ct))
        {
            if (dropColumn)
            {
                await writer.DropColumnAsync("Docs", "Values", Ct);
            }
            else
            {
                await writer.DropTableAsync("Docs", Ct);
            }
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            foreach (long page in pages)
            {
                Assert.True(await harness.Services.PageAllocator.IsPageFreeAsync(page, Ct), $"Flat child page {page} was not released.");
            }
        }

        Assert.True(stream.ToArray().AsSpan().IndexOf(payload.AsSpan(1_000, 100)) < 0);
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            Assert.Single(await reader.GetMultiValueItemsAsync("Keep", "Values", Ct));
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync("Replacement", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.InsertRowAsync("Replacement", [7], Ct);
            for (int index = 0; index < 8; index++)
            {
                await writer.CreateTableAsync($"Reuse{index}", [new ColumnDefinition("Id", typeof(int))], Ct);
            }
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            SortedSet<long> allocated = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct);
            Assert.Contains(pages, allocated.Contains);
        }

        await using AccessReader verify = await OpenReaderAsync(stream);
        using DataTable replacement = (await verify.ReadDataTableAsync("Replacement", cancellationToken: Ct))!;
        Assert.Single(replacement.Rows);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Drop_RollbackRestoresChildStorageAndContents(bool dropColumn, bool attachment)
    {
        await using var stream = new MemoryStream();
        byte[] payload = [1, 2, 3, 4];
        await CreatePopulatedAsync(stream, attachment, payload);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            if (dropColumn)
            {
                await writer.DropColumnAsync("Docs", "Values", Ct);
            }
            else
            {
                await writer.DropTableAsync("Docs", Ct);
            }

            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
        await using AccessReader reader = await OpenReaderAsync(stream);
        if (attachment)
        {
            Assert.Single(await reader.GetAttachmentsAsync("Docs", "Values", Ct));
        }
        else
        {
            Assert.Single(await reader.GetMultiValueItemsAsync("Docs", "Values", Ct));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Drop_CorruptFlatReferenceDoesNotReleaseSibling(bool dropColumn)
    {
        await using var stream = new MemoryStream();
        await CreatePopulatedAsync(stream, attachment: false, []);
        ComplexColumnInfo dropping;
        ComplexColumnInfo keeping;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            dropping = Assert.Single(await reader.GetComplexColumnsAsync("Docs", Ct));
            keeping = Assert.Single(await reader.GetComplexColumnsAsync("Keep", Ct));
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            long complexPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", Ct);
            TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(complexPage, "MSysComplexColumns", Ct);
            int idOffset = definition.FindColumn("ComplexID")!.FixedOff;
            int flatOffset = definition.FindColumn("FlatTableID")!.FixedOff;
            bool patched = false;
            foreach (RowLocation location in await harness.Database.GetLiveRowLocationsAsync(complexPage, Ct))
            {
                byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.DataPageNumber, Ct);

                // ACE rows begin with a two-byte column count, then the fixed area.
                if (BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(location.RowStart + 2 + idOffset)) == dropping.ComplexId)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + 2 + flatOffset), keeping.FlatTableId);
                    await harness.Pager.WritePageAsync(location.DataPageNumber, page, Ct);
                    patched = true;
                }
            }

            Assert.True(patched);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            if (dropColumn)
            {
                await writer.DropColumnAsync("Docs", "Values", Ct);
            }
            else
            {
                await writer.DropTableAsync("Docs", Ct);
            }
        }

        await using AccessReader verify = await OpenReaderAsync(stream);
        Assert.Single(await verify.GetMultiValueItemsAsync("Keep", "Values", Ct));
    }

    private static async Task CreatePopulatedAsync(MemoryStream stream, bool attachment, byte[] payload)
    {
        await using AccessWriter writer = await CreateWriterAsync(stream);
        ColumnDefinition values = attachment
            ? new ColumnDefinition("Values", typeof(byte[])) { IsAttachment = true }
            : new ColumnDefinition("Values", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) };
        await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), values], Ct);
        await writer.CreateTableAsync("Keep", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Values", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) }], Ct);
        await writer.InsertRowAsync("Docs", [1, DBNull.Value], Ct);
        await writer.InsertRowAsync("Keep", [1, DBNull.Value], Ct);
        var key = new Dictionary<string, object?> { ["Id"] = 1 };
        await writer.AddMultiValueItemAsync("Keep", "Values", key, 42, Ct);
        if (attachment)
        {
            await writer.AddAttachmentAsync("Docs", "Values", key, new AttachmentInput("private.zip", payload), Ct);
        }
        else
        {
            await writer.AddMultiValueItemAsync("Docs", "Values", key, 17, Ct);
        }
    }
}
