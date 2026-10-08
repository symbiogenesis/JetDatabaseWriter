namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Replaced trees must not accumulate pages across repeated maintenance.</summary>
/// <param name="cache">The independent Access-authored fixture cache.</param>
public sealed class IndexTreeReclamationTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Rebuild_UnmaintainedDescriptorWithSharedOrOpaqueOwnership_DoesNotFreeCandidate(int ownership)
    {
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.IndexTestV1997, this.ct);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        CatalogEntry table = (await harness.GetCatalogEntryAsync("Table1", this.ct))!;
        DatabaseFile db = harness.Database;
        TableDef definition = (await db.TableDefs.ReadTableDefAsync(table.TDefPage, this.ct))!;
        LogicalTDefChain chain = await db.TableDefs.ReadTDefChainAsync(table.TDefPage, this.ct);
        byte[] td = chain.Bytes;
        int numCols = Ru16(td, db.Format.TDef.NumCols);
        int numRealIdx = Ri32(td, db.Format.TDef.NumRealIdx);
        int start = IndexCatalogReader.LocateRealIdxDescStart(db.Format, td, numCols, numRealIdx);
        Assert.True(db.Format.Index.TryReadRealIdxSlot(td, start, 0, out RealIdxSlot maintained));
        Assert.True(db.Format.Index.TryReadRealIdxSlot(td, start, 3, out RealIdxSlot unmaintained));
        long firstRoot = (uint)Ri32(td, maintained.FirstDpOffset);
        long otherRoot = (uint)Ri32(td, unmaintained.FirstDpOffset);
        IndexPageLayout layout = db.Format.IndexPage;
        byte[] leaf = IndexPageCodec.TryBuildLeafPage(layout, db.Format.PageSize, table.TDefPage, [])!;
        long candidate = await harness.Services.PageAllocator.AllocatePageAsync(leaf, this.ct);
        byte[] root = IndexPageCodec.BuildIntermediatePage(
            layout,
            db.Format.PageSize,
            table.TDefPage,
            [new DecodedIntermediateEntry(new IndexEntry([1], 3, 0), candidate)],
            prevPage: 0,
            nextPage: 0,
            tailPage: 0,
            maxPrefixLength: 0);
        await harness.Pager.WritePageAsync(firstRoot, root, this.ct);

        // This descriptor remains in the TDEF, but its empty column map
        // excludes it from the maintained-index dictionary.
        Array.Fill(td, byte.MaxValue, unmaintained.PhysStart, 30);
        if (ownership == 0)
        {
            Assert.True(UsageMap.TryReadPointer(td, unmaintained.FirstDpOffset - 4, out UsageMapPointer pointer));
            byte[] map = await db.Pages.ReadPageCopyAsync(pointer.PageNumber, this.ct);
            Assert.True(UsageMap.TryGetRowBound(map, db.Format.DataPage, db.Format.PageSize, pointer.RowIndex, out RowBound row));
            Assert.True(UsageMap.TryWriteInlineRow(map, row.RowStart, row.RowSize, [otherRoot, candidate]));
            await harness.Pager.WritePageAsync(pointer.PageNumber, map, this.ct);
        }
        else if (ownership == 1)
        {
            byte[] partial = IndexPageCodec.BuildIntermediatePage(
                layout,
                db.Format.PageSize,
                table.TDefPage,
                [
                    new DecodedIntermediateEntry(new IndexEntry([1], 3, 0), otherRoot),
                    new DecodedIntermediateEntry(new IndexEntry([2], 3, 0), candidate),
                ],
                prevPage: 0,
                nextPage: 0,
                tailPage: otherRoot,
                maxPrefixLength: 0);
            int payloadEnd = db.Format.PageSize - Ru16(partial, 2);
            int malformedStart = payloadEnd - 4;
            int bit = malformedStart - layout.FirstEntryOffset;
            partial[layout.BitmaskOffset + (bit / 8)] |= (byte)(1 << (bit % 8));
            Assert.Single(IndexPageCodec.DecodeIntermediateEntries(layout, partial, db.Format.PageSize));
            long partialRoot = await harness.Services.PageAllocator.AllocatePageAsync(partial, this.ct);
            Wi32(td, unmaintained.FirstDpOffset, checked((int)partialRoot));
        }
        else
        {
            UsageMap.WritePointer(td, unmaintained.FirstDpOffset - 4, 0, db.Pages.PageCount + 10);
        }

        await harness.Services.TDefWriter.WriteChainInPlaceAsync(chain, this.ct);
        await harness.Services.Indexes.MaintainIndexesAsync(table.TDefPage, definition, "Table1", this.ct);
        Assert.False(await harness.Services.PageAllocator.IsPageFreeAsync(candidate, this.ct));
        Assert.Equal(leaf, await db.Pages.ReadPageCopyAsync(candidate, this.ct));
        byte[] after = (await db.TableDefs.ReadTDefBytesAsync(table.TDefPage, this.ct))!;
        Assert.NotEqual(firstRoot, (uint)Ri32(after, maintained.FirstDpOffset));
    }

    [Fact]
    public async Task Jet3NativeForeignKeyTree_TenPublicInsertBatches_DoNotAccumulateWriterOrphans()
    {
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.IndexTestV1997, this.ct);
        var template = new RowValues();
        int originalCount;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, this.ct))
        {
            using DataTable table = await reader.ReadTableAsync("Table1", cancellationToken: this.ct);
            originalCount = table.Rows.Count;
            foreach (DataColumn column in table.Columns)
            {
                template[column.ColumnName] = table.Rows[0][column];
            }
        }

        SortedSet<long>? retained = null;
        for (int i = 0; i <= 10; i++)
        {
            stream.Position = 0;
            await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, this.ct))
            {
                var rows = new List<RowValues>();
                for (int rowNumber = 0; rowNumber < (i == 0 ? 4000 : 40); rowNumber++)
                {
                    var row = new RowValues();
                    foreach ((string name, object? value) in template)
                    {
                        row[name] = value;
                    }

                    row["id"] = (i == 0 ? 10_000 : 14_000 + ((i - 1) * 40)) + rowNumber;
                    row["data"] = FormattableString.Invariant($"z{i}");
                    rows.Add(row);
                }

                await writer.InsertRowsAsync("Table1", rows, this.ct);
            }

            stream.Position = 0;
            await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
            CatalogEntry table = (await harness.GetCatalogEntryAsync("Table1", this.ct))!;
            SortedSet<long> unreachable = await PageAudit.FindUnreachableIndexPagesAsync(harness.Database, harness.Services.PageAllocator, table.TDefPage, this.ct);
            if (retained is null)
            {
                Assert.NotEmpty(unreachable);
                retained = unreachable;
            }

            Assert.Empty(unreachable.Except(retained));
            Assert.Empty(retained.Except(unreachable));
        }

        stream.Position = 0;
        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, this.ct);
        using DataTable after = await reopened.ReadTableAsync("Table1", cancellationToken: this.ct);
        Assert.Equal(originalCount + 4400, after.Rows.Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task IncrementalTreeMerge_ReclaimsUnreachablePagesAndRollsBack(DatabaseFormat format, bool rollback)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, this.ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], this.ct);
            await writer.InsertRowsAsync("T", Enumerable.Range(0, 4000).Select(static id => new object?[] { id }), this.ct);
            byte[] before = stream.ToArray();
            await using JetTransaction transaction = await writer.BeginTransactionAsync(this.ct);
            Assert.Equal(3990, await writer.DeleteRowsAsync("T", RowCriteria.Where(ColumnPredicate.GreaterThanOrEqual("Id", 10)), this.ct));
            if (rollback)
            {
                await transaction.RollbackAsync(this.ct);
                Assert.Equal(before, stream.ToArray());
            }
            else
            {
                await transaction.CommitAsync(this.ct);
            }
        }

        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        CatalogEntry table = (await harness.GetCatalogEntryAsync("T", this.ct))!;
        Assert.Empty(await PageAudit.FindUnreachableIndexPagesAsync(harness.Database, harness.Services.PageAllocator, table.TDefPage, this.ct));
        foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, table.TDefPage, this.ct))
        {
            Assert.Equal(rollback ? 4000 : 10, (await IndexLeafChain.ReadEntriesAsync(harness.Database, table.TDefPage, root, this.ct)).Count);
        }
    }
}
