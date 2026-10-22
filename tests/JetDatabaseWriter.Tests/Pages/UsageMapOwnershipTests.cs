namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Usage-map mutations distinguish shared rows, shared pages and shared bitmap storage.</summary>
public sealed class UsageMapOwnershipTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MutationBatch_MatchesIndependentProofsInOrderWithFewerPhysicalReads(DatabaseFormat format)
    {
        await using var stream = new WriteFaultStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.InsertRowAsync("M", [1], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
        int[] offsets = [writer.Database.Format.TDef.FreePages, Assert.Single(image.RealIndexes).Slot.FirstDpOffset - 4, writer.Database.Format.TDef.UsedPages, writer.Database.Format.TDef.FreePages];
        foreach (int offset in offsets)
        {
            _ = await UsageMapOwnership.InspectForMutationAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offset, Ct);
        }

        int initialReads = stream.ReadCount;
        var expected = new UsageMapOwnership.Proof[offsets.Length];
        for (int index = 0; index < offsets.Length; index++)
        {
            expected[index] = await UsageMapOwnership.InspectForMutationAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offsets[index], Ct);
        }

        int individualReads = stream.ReadCount - initialReads;
        initialReads = stream.ReadCount;
        int initialWrites = stream.WriteCount;
        UsageMapOwnership.Proof[] batch = await UsageMapOwnership.InspectForMutationBatchAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offsets, Ct);
        int batchReads = stream.ReadCount - initialReads;
        Assert.Equal(initialWrites, stream.WriteCount);
        Assert.Equal(expected.Length, batch.Length);
        for (int index = 0; index < batch.Length; index++)
        {
            AssertSameMutationProof(expected[index], batch[index]);
        }

        Assert.NotSame(batch[0], batch[3]);
        Assert.True(batchReads < individualReads, $"Batch read {batchReads} physical pages; independent proofs read {individualReads}.");
    }

    [Theory]
    [InlineData("same-row")]
    [InlineData("same-bitmap")]
    [InlineData("duplicate-bitmap")]
    public async Task MutationBatch_SharedTargetsRetainAllAliasProtections(string alias)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        JetFormat format = writer.Database.Format;
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer owned));
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.FreePages, out UsageMapPointer free));
        if (alias == "same-row")
        {
            UsageMap.WritePointer(definition, format.TDef.FreePages, owned.RowIndex, owned.PageNumber);
            await writer.Pager.WritePageAsync(root, definition, Ct);
        }
        else
        {
            byte[] bitmap = new byte[format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            long bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
            foreach (UsageMapPointer pointer in new[] { owned, free })
            {
                byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
                Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
                Array.Clear(map, row.RowStart, row.RowSize);
                map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
                JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 1, checked((int)bitmapPage));
                if (alias == "duplicate-bitmap")
                {
                    JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 5, checked((int)bitmapPage));
                }

                await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            }
        }

        int[] offsets = [format.TDef.FreePages, format.TDef.UsedPages];
        UsageMapOwnership.Proof[] batch = await UsageMapOwnership.InspectForMutationBatchAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offsets, Ct);
        for (int index = 0; index < offsets.Length; index++)
        {
            UsageMapOwnership.Proof single = await UsageMapOwnership.InspectForMutationAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offsets[index], Ct);
            AssertSameMutationProof(single, batch[index]);
            Assert.False(batch[index].HasExclusiveBitmapPages);
            Assert.Equal(alias != "same-row", batch[index].IsExclusiveRow);
        }
    }

    [Fact]
    public async Task MutationBatch_EmptyCancellationAndTargetBudgetDoNotReadOrWritePages()
    {
        await using var stream = new WriteFaultStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, Ct);
        JetFormat format = writer.Database.Format;
        int reads = stream.ReadCount;
        int writes = stream.WriteCount;
        Assert.Empty(await UsageMapOwnership.InspectForMutationBatchAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, 2, [], Ct));
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await UsageMapOwnership.InspectForMutationBatchAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, 2, [], canceled.Token));
        const int limit = (Constants.TableDefinition.MaxColumns * 2) + Constants.TableDefinition.MaxIndexes + 2;
        await Assert.ThrowsAsync<JetLimitationException>(async () => await UsageMapOwnership.InspectForMutationBatchAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, 2, new int[limit + 1], Ct));
        Assert.Equal(reads, stream.ReadCount);
        Assert.Equal(writes, stream.WriteCount);
    }

    [Fact]
    public async Task MutationBatch_AllowsAllLongColumnPointersWithinTheTargetBudget()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        int[] offsets = new int[512];
        Array.Fill(offsets, writer.Database.Format.TDef.UsedPages);
        UsageMapOwnership.Proof[] batch = await UsageMapOwnership.InspectForMutationBatchAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, 2, offsets, Ct);
        Assert.Equal(offsets.Length, batch.Length);
        Assert.All(batch, proof => Assert.Equal(batch[0].Pointer, proof.Pointer));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(12)]
    public async Task MutationBatch_CancellationDuringReadsLeavesTheSourceUnchanged(int read)
    {
        await using var stream = new WriteFaultStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        int[] offsets = [writer.Database.Format.TDef.FreePages, writer.Database.Format.TDef.UsedPages];
        byte[] before = stream.ToArray();
        int writes = stream.WriteCount;
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        stream.CancelAfterReads(read, canceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await UsageMapOwnership.InspectForMutationBatchAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offsets, canceled.Token));
        Assert.True(canceled.IsCancellationRequested);
        Assert.Equal(writes, stream.WriteCount);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    public async Task MutationBatch_InvalidTargetOffsetRefusesTheEntireBatch(int offset)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        int[] offsets = [writer.Database.Format.TDef.UsedPages, offset];
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await UsageMapOwnership.InspectForMutationBatchAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, 2, offsets, Ct));
    }

    [Fact]
    public async Task MutationBatch_AfterOpaqueAliasWriteBuildsAFreshGraph()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        int[] offsets = [writer.Database.Format.TDef.UsedPages, writer.Database.Format.TDef.FreePages];
        UsageMapOwnership.Proof[] before = await UsageMapOwnership.InspectForMutationBatchAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offsets, Ct);
        Assert.True(before[0].IsExclusiveRow);
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        UsageMap.WritePointer(definition, definition.Length - 4, before[0].Pointer.RowIndex, before[0].Pointer.PageNumber);
        await writer.Pager.WritePageAsync(otherRoot, definition, Ct);
        UsageMapOwnership.Proof[] after = await UsageMapOwnership.InspectForMutationBatchAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Pager, root, offsets, Ct);
        Assert.False(after[0].IsExclusiveRow);
        Assert.False(after[0].HasExclusiveBitmapPages);
        Assert.True(after[1].IsExclusiveRow);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MutationProof_DoesNotCollectUnrelatedMembers(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        await writer.InsertRowAsync("Other", new RowValues { ["Id"] = 2 }, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
        int offset = Assert.Single(image.RealIndexes).Slot.FirstDpOffset - 4;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        long otherMember = Assert.Single(await writer.Database.OwnedPages.GetOwnedDataPagesAsync(otherRoot, Ct));
        byte[] bitmap = new byte[writer.Database.Format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        Assert.True(UsageMap.TrySetReferencePageState(bitmap, writer.Database.Format.PageSize, otherMember, isMarked: true));
        long bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        Assert.True(UsageMap.TryReadPointer(other, writer.Database.Format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound row));
        Array.Clear(map, row.RowStart, row.RowSize);
        map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
        JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 1, checked((int)bitmapPage));
        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
        UsageMapOwnership.Proof full = await UsageMapOwnership.InspectAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, offset, Ct);

        Assert.Contains(otherMember, full.OtherMappedPages);
        await AssertMutationProofAsync(writer, root, offset, full);
    }

    [Theory]
    [InlineData("pointer")]
    [InlineData("negative-pointer")]
    [InlineData("header")]
    [InlineData("page-zero")]
    [InlineData("past-end")]
    [InlineData("far-window")]
    [InlineData("inline-negative-base")]
    [InlineData("inline-page-zero")]
    [InlineData("inline-past-end")]
    public async Task MutationProof_UnrelatedReferenceMapStillRejectsInvalidStorage(string corruption)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        JetFormat format = writer.Database.Format;
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        byte[] bitmap = new byte[format.PageSize];
        bitmap[0] = corruption == "header" ? Constants.PageTypes.Data : Constants.PageTypes.UsageMap;
        long bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
        if (corruption is "page-zero" or "past-end" or "far-window")
        {
            long member = corruption == "page-zero" ? 0 : writer.Database.Pages.PageCount;
            Assert.True(UsageMap.TrySetReferencePageState(bitmap, format.PageSize, member, isMarked: true));
            await writer.Pager.WritePageAsync(bitmapPage, bitmap, Ct);
        }

        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        Assert.True(UsageMap.TryReadPointer(other, format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
        Array.Clear(map, row.RowStart, row.RowSize);
        if (corruption is "inline-negative-base" or "inline-page-zero" or "inline-past-end")
        {
            map[row.RowStart] = Constants.UsageMap.InlineMapType;
            if (corruption == "inline-negative-base")
            {
                JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 1, -1);
            }
            else
            {
                long member = corruption == "inline-page-zero" ? 0 : writer.Database.Pages.PageCount;
                Assert.True(UsageMap.TrySetInlinePageState(map, row.RowStart, row.RowSize, member, isMarked: true));
            }
        }
        else
        {
            map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
            long reference = corruption switch
            {
                "pointer" => writer.Database.Pages.PageCount,
                "negative-pointer" => -1,
                _ => bitmapPage,
            };
            int referenceOffset = corruption == "far-window" ? 5 : 1;
            JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + referenceOffset, checked((int)reference));
        }

        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await UsageMapOwnership.InspectForMutationAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await UsageMapOwnership.InspectForMutationBatchAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, [format.TDef.UsedPages, format.TDef.FreePages], Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task IndexMap_ExclusiveRowOnSharedPage_ProtectsOtherMapMembers(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
        int offset = Assert.Single(image.RealIndexes).Slot.FirstDpOffset - 4;
        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, offset, Ct);
        await AssertMutationProofAsync(writer, root, offset, proof);
        Assert.True(proof.IsExclusiveRow);
        Assert.True(proof.HasExclusiveBitmapPages);
        Assert.False(proof.IsDedicatedPage);
        Assert.Equal(Assert.Single(image.RealIndexes).RootPage, Assert.Single(proof.Pages));
        Assert.NotEmpty(proof.OtherMappedPages);
        Assert.DoesNotContain(Assert.Single(proof.Pages), proof.OtherMappedPages);
    }

    [Theory]
    [InlineData("table")]
    [InlineData("index")]
    [InlineData("opaque")]
    public async Task IndexMap_AliasedAcrossDescriptors_ProtectsEveryOriginalMember(string alias)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
        int offset = Assert.Single(image.RealIndexes).Slot.FirstDpOffset - 4;
        byte[] definition = await writer.Database.TableDefs.ReadTDefBytesAsync(root, Ct) ?? throw new InvalidOperationException();
        Assert.True(UsageMap.TryReadPointer(definition, offset, out UsageMapPointer pointer));
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        int otherOffset = alias switch
        {
            "table" => writer.Database.Format.TDef.UsedPages,
            "index" => Assert.Single(Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(otherRoot, Ct)).RealIndexes).Slot.FirstDpOffset - 4,
            _ => other.Length - 4,
        };
        UsageMap.WritePointer(other, otherOffset, pointer.RowIndex, pointer.PageNumber);
        await writer.Pager.WritePageAsync(otherRoot, other, Ct);

        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, offset, Ct);

        await AssertMutationProofAsync(writer, root, offset, proof);
        Assert.False(proof.IsExclusiveRow);
        Assert.False(proof.HasExclusiveBitmapPages);
        Assert.False(proof.IsDedicatedPage);
        Assert.NotEmpty(proof.Pages);
        Assert.All(proof.Pages, page => Assert.Contains(page, proof.OtherMappedPages));
        UsageMapPointer copy = await UsageMapOwnership.CreatePrivateCopyAsync(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, proof, Ct);
        Assert.NotEqual(pointer.PageNumber, copy.PageNumber);
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(copy.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, copy.RowIndex, out RowBound row));
        var copied = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, row, writer.Database.Format.PageSize, writer.Database.Pages.PageCount, 1, true, writer.Database.Pages.ReadPageAsync, JetDatabaseWriter.Pages.Paging.PageBuffers.Return, copied, Ct));
        Assert.Equal(proof.Pages, copied);
        await UsageMapOwnership.ReleasePrivateCopyAsync(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, copy);
        Assert.Equal(Constants.PageTypes.Data, (await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct))[0]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MapStorage_ReleasedOnlyAfterEverySurvivingReferenceIsRemoved(bool opaqueAlias)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, writer.Database.Format.TDef.UsedPages, out UsageMapPointer pointer));
        if (opaqueAlias)
        {
            long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
            byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
            UsageMap.WritePointer(other, other.Length - 4, pointer.RowIndex, pointer.PageNumber);
            await writer.Pager.WritePageAsync(otherRoot, other, Ct);
        }

        HashSet<long> referenced = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [pointer.PageNumber], 0, Ct);
        Assert.Empty(referenced);
        HashSet<long> afterDrop = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [pointer.PageNumber], root, Ct);
        if (opaqueAlias)
        {
            Assert.Empty(afterDrop);
        }
        else
        {
            Assert.Equal(pointer.PageNumber, Assert.Single(afterDrop));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task CachedWritableUserMap_DescriptorAliasInvalidatesProofAndCopiesStorage(DatabaseFormat databaseFormat, bool sharedBitmap)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        JetFormat format = writer.Database.Format;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer original));
        Assert.True(UsageMap.TryReadPointer(other, format.TDef.UsedPages, out UsageMapPointer otherPointer));
        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        if (sharedBitmap)
        {
            byte[] bitmap = new byte[format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            bitmap[1] = 1;
            long bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
            foreach (UsageMapPointer pointer in new[] { original, otherPointer })
            {
                byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
                Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
                Array.Clear(map, row.RowStart, row.RowSize);
                map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
                JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 1, checked((int)bitmapPage));
                await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            }
        }
        else
        {
            UsageMap.WritePointer(other, format.TDef.UsedPages, original.RowIndex, original.PageNumber);
            await writer.Pager.WritePageAsync(otherRoot, other, Ct);
        }

        UsageMapOwnership.Proof aliased = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        await AssertMutationProofAsync(writer, root, format.TDef.UsedPages, aliased);
        Assert.False(sharedBitmap ? aliased.HasExclusiveBitmapPages : aliased.IsExclusiveRow);
        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        byte[] isolatedDefinition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(isolatedDefinition, format.TDef.UsedPages, out UsageMapPointer isolated));
        Assert.NotEqual(original, isolated);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        await writer.InsertRowAsync("Other", new RowValues { ["Id"] = 2 }, Ct);
        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        UsageMapOwnership.Proof otherProof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, otherRoot, format.TDef.UsedPages, Ct);
        Assert.NotEqual(Assert.Single(proof.Pages), Assert.Single(otherProof.Pages));
    }

    [Fact]
    public async Task StorageCleanup_NeverReclaimsGlobalReferenceBitmap()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        byte[] initial = await writer.Database.Pages.ReadPageCopyAsync(1, Ct);
        Assert.True(UsageMap.TryGetFirstRowBound(initial, writer.Database.Format.DataPage, writer.Database.Format.PageSize, out RowBound initialRow));
        Assert.Equal(Constants.UsageMap.InlineMapType, initial[initialRow.RowStart]);
        int inlineCapacity = (initialRow.RowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        _ = await writer.Services.PageAllocator.ReserveContiguousPagesAsync(inlineCapacity + 1, Ct);
        byte[] global = await writer.Database.Pages.ReadPageCopyAsync(1, Ct);
        Assert.True(UsageMap.TryGetFirstRowBound(global, writer.Database.Format.DataPage, writer.Database.Format.PageSize, out RowBound row));
        Assert.Equal(Constants.UsageMap.ReferenceMapType, global[row.RowStart]);
        long bitmap = JetDatabaseWriter.Schema.JetTypeInfo.Ri32(global, row.RowStart + 1);
        Assert.True(bitmap > 1);
        HashSet<long> reclaimable = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, [bitmap], 0, Ct);
        Assert.Empty(reclaimable);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ReopenedAliasedTableMaps_RecoverOwnPhysicalPagesAndExcludeForeignPages(DatabaseFormat databaseFormat, bool deletedSlot)
    {
        await using var stream = new MemoryStream();
        long root;
        long otherRoot;
        long physical;
        long foreign;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
            await writer.InsertRowAsync("Other", new RowValues { ["Id"] = 2 }, Ct);
            root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
            otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
            physical = Assert.Single(await writer.Database.OwnedPages.GetOwnedDataPagesAsync(root, Ct));
            foreign = Assert.Single(await writer.Database.OwnedPages.GetOwnedDataPagesAsync(otherRoot, Ct));
            if (deletedSlot)
            {
                JetFormat format = writer.Database.Format;
                byte[] data = await writer.Database.Pages.ReadPageCopyAsync(physical, Ct);
                JetDatabaseWriter.Schema.JetTypeInfo.Wu16(data, format.DataPage.NumRows, 2);
                JetDatabaseWriter.Schema.JetTypeInfo.Wu16(data, format.DataPage.RowsStart + 2, Constants.DataPage.NonLiveRowFlags | format.PageSize);
                await writer.Pager.WritePageAsync(physical, data, Ct);
            }

            byte[] target = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
            byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
            Assert.True(UsageMap.TryReadPointer(other, writer.Database.Format.TDef.UsedPages, out UsageMapPointer otherMap));
            UsageMap.WritePointer(target, writer.Database.Format.TDef.UsedPages, otherMap.RowIndex, otherMap.PageNumber);
            UsageMap.WritePointer(target, writer.Database.Format.TDef.FreePages, otherMap.RowIndex, otherMap.PageNumber);
            await writer.Pager.WritePageAsync(root, target, Ct);
        }

        stream.Position = 0;
        await using AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        Assert.True(await reopened.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        UsageMapOwnership.Proof owned = await UsageMapOwnership.InspectAsync(reopened.Database.Format, reopened.Database.TableDefs, reopened.Services.CatalogRows, reopened.Database.Pages, root, reopened.Database.Format.TDef.UsedPages, Ct);
        UsageMapOwnership.Proof free = await UsageMapOwnership.InspectAsync(reopened.Database.Format, reopened.Database.TableDefs, reopened.Services.CatalogRows, reopened.Database.Pages, root, reopened.Database.Format.TDef.FreePages, Ct);
        Assert.Equal(physical, Assert.Single(owned.Pages));
        Assert.Equal(physical, Assert.Single(free.Pages));
        Assert.DoesNotContain(foreign, owned.Pages);
        Assert.DoesNotContain(foreign, free.Pages);
        await reopened.InsertRowAsync("M", new RowValues { ["Id"] = 3 }, Ct);
        Assert.Equal(physical, Assert.Single(await reopened.Database.OwnedPages.GetOwnedDataPagesAsync(root, Ct)));
        Assert.Equal(foreign, Assert.Single(await reopened.Database.OwnedPages.GetOwnedDataPagesAsync(otherRoot, Ct)));
    }

    [Fact]
    public async Task GlobalBitmapAlias_DetachesAndPreservesAllocationOwnership()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        JetFormat format = writer.Database.Format;
        byte[] initial = await writer.Database.Pages.ReadPageCopyAsync(1, Ct);
        Assert.True(UsageMap.TryGetFirstRowBound(initial, format.DataPage, format.PageSize, out RowBound initialRow));
        Assert.Equal(Constants.UsageMap.InlineMapType, initial[initialRow.RowStart]);
        int inlineCapacity = (initialRow.RowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        _ = await writer.Services.PageAllocator.ReserveContiguousPagesAsync(inlineCapacity + 1, Ct);
        byte[] global = await writer.Database.Pages.ReadPageCopyAsync(1, Ct);
        Assert.True(UsageMap.TryGetFirstRowBound(global, format.DataPage, format.PageSize, out RowBound globalRow));
        Assert.Equal(Constants.UsageMap.ReferenceMapType, global[globalRow.RowStart]);
        long bitmapPage = JetDatabaseWriter.Schema.JetTypeInfo.Ri32(global, globalRow.RowStart + 1);
        byte[] bitmap = await writer.Database.Pages.ReadPageCopyAsync(bitmapPage, Ct);
        bitmap.AsSpan(Constants.UsageMap.ReferenceMapBitmapOffset).Clear();
        await writer.Pager.WritePageAsync(bitmapPage, bitmap, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
        Array.Clear(map, row.RowStart, row.RowSize);
        map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
        JetDatabaseWriter.Schema.JetTypeInfo.Wi32(map, row.RowStart + 1, checked((int)bitmapPage));
        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);

        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        await AssertMutationProofAsync(writer, root, format.TDef.UsedPages, proof);
        Assert.False(proof.HasExclusiveBitmapPages);
        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        Assert.True(UsageMap.TrySetReferencePageState(bitmap, format.PageSize, pointer.PageNumber, isMarked: true));
        Assert.Equal(bitmap, await writer.Database.Pages.ReadPageCopyAsync(bitmapPage, Ct));
    }

    [Fact]
    public async Task NonliveDirectoryAlias_RetainsOpaqueReferenceBytesAndCopiesOwnedMap()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("M", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        await writer.InsertRowAsync("M", new RowValues { ["Id"] = 1 }, Ct);
        long root = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("M", Ct)).TDefPage;
        long otherRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("Other", Ct)).TDefPage;
        JetFormat format = writer.Database.Format;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(definition, format.TDef.UsedPages, out UsageMapPointer pointer));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound owned));
        JetDatabaseWriter.Schema.JetTypeInfo.Wu16(map, format.DataPage.NumRows, 3);
        JetDatabaseWriter.Schema.JetTypeInfo.Wu16(map, format.DataPage.RowsStart + 4, Constants.DataPage.NonLiveRowFlags | owned.RowStart);
        await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
        byte[] other = await writer.Database.Pages.ReadPageCopyAsync(otherRoot, Ct);
        UsageMap.WritePointer(other, other.Length - 4, 2, pointer.PageNumber);
        await writer.Pager.WritePageAsync(otherRoot, other, Ct);

        UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectAsync(format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, format.TDef.UsedPages, Ct);
        Assert.False(proof.IsExclusiveRow);
        Assert.True(await writer.Services.OwnedMaps.CanMaintainAsync(root, Ct));
        byte[] isolated = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        Assert.True(UsageMap.TryReadPointer(isolated, format.TDef.UsedPages, out UsageMapPointer replacement));
        Assert.NotEqual(pointer, replacement);
        Assert.Equal(map, await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct));
    }

    private static async ValueTask AssertMutationProofAsync(AccessWriter writer, long root, int offset, UsageMapOwnership.Proof full)
    {
        UsageMapOwnership.Proof mutation = await UsageMapOwnership.InspectForMutationAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, offset, Ct);
        AssertSameMutationProof(full, mutation);
        UsageMapOwnership.Proof batch = Assert.Single(await UsageMapOwnership.InspectForMutationBatchAsync(writer.Database.Format, writer.Database.TableDefs, writer.Services.CatalogRows, writer.Database.Pages, root, [offset], Ct));
        AssertSameMutationProof(full, batch);
    }

    private static void AssertSameMutationProof(UsageMapOwnership.Proof expected, UsageMapOwnership.Proof actual)
    {
        Assert.Equal(expected.Pointer, actual.Pointer);
        Assert.Equal(expected.Pages, actual.Pages);
        Assert.Equal(expected.BitmapPages, actual.BitmapPages);
        Assert.Equal(expected.IsExclusiveRow, actual.IsExclusiveRow);
        Assert.Equal(expected.HasExclusiveBitmapPages, actual.HasExclusiveBitmapPages);
        Assert.Equal(expected.IsDedicatedPage, actual.IsDedicatedPage);
        Assert.Empty(actual.OtherMappedPages);
    }
}
