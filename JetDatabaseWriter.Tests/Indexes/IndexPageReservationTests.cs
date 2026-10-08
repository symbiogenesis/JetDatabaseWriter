namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Pages an index-maintenance path reserves through <see cref="PageAllocator"/>
/// but never links into a tree must go back to the global usage map when the
/// path bails, throws, or takes a fallback that reserves its own pages.
/// Before, each such reservation stayed marked used and unreachable until
/// Microsoft Access compacted the file.
/// </summary>
public sealed class IndexPageReservationTests
{
    private const string TableName = "T";
    private const int SeedRows = 4000;

    private static readonly AccessReaderOptions UncachedReaderOptions = new() { UseLockFile = false, PageCacheSize = 0 };

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TryPlaceTree_RelocatedBuildThrows_ReleasesRunAndReturnsNull(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        DatabaseFile db = harness.Database;
        PageAllocator allocator = harness.Services.PageAllocator;
        IndexPageLayout layout = JetFormat.ForNewDatabase(format).IndexPage;
        List<IndexEntry> entries = BuildLongKeyEntries(1000);
        int treePages = IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, db.Pages.PageCount).Pages.Count;
        Assert.True(treePages > 1, "The tree should span several pages.");

        // A free run the size of the tree makes the reservation reuse it, so
        // the tree is rebuilt at the run's first page; that rebuild fails.
        long freeRun = await CreateFreeRunAsync(harness, treePages, this.ct);
        SortedSet<long> allocatedBefore = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);
        long pageCountBefore = db.Pages.PageCount;

        int calls = 0;
        var runs = new ReservedPageRuns(allocator);
        IndexBTreeBuildResult? placed = await new IndexBTreeEditor(db.Format, harness.Pager, harness.Services.TDefWriter, allocator).TryPlaceTreeAsync(
            firstPage => ++calls == 2
                ? throw new IndexCapacityException(nameof(firstPage), "Injected capacity refusal.")
                : IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, firstPage),
            runs,
            this.ct);

        Assert.Null(placed);
        Assert.Equal(2, calls);
        Assert.True(runs.IsEmpty);
        for (long page = freeRun; page < freeRun + treePages; page++)
        {
            Assert.True(await allocator.IsPageFreeAsync(page, this.ct), $"Page {page} of the reused run should be free again.");
        }

        Assert.Equal(pageCountBefore, db.Pages.PageCount);
        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TryPlaceTree_InvalidRelocatedMetadata_ReleasesRunAndRethrows(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        DatabaseFile db = harness.Database;
        PageAllocator allocator = harness.Services.PageAllocator;
        IndexPageLayout layout = JetFormat.ForNewDatabase(format).IndexPage;
        List<IndexEntry> entries = BuildLongKeyEntries(1000);
        int treePages = IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, db.Pages.PageCount).Pages.Count;
        Assert.True(treePages > 1, "The tree should span several pages.");

        // A free run the size of the tree makes the reservation reuse it, so
        // the tree is rebuilt at the run's first page; that rebuild fails.
        long freeRun = await CreateFreeRunAsync(harness, treePages, this.ct);
        SortedSet<long> allocatedBefore = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);
        long pageCountBefore = db.Pages.PageCount;

        int calls = 0;
        var runs = new ReservedPageRuns(allocator);
        _ = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => new IndexBTreeEditor(db.Format, harness.Pager, harness.Services.TDefWriter, allocator).TryPlaceTreeAsync(
            firstPage => ++calls == 2
                ? throw new ArgumentOutOfRangeException(nameof(firstPage), "Injected invalid metadata.")
                : IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, firstPage),
            runs,
            this.ct).AsTask());

        Assert.Equal(2, calls);
        Assert.True(runs.IsEmpty);
        for (long page = freeRun; page < freeRun + treePages; page++)
        {
            Assert.True(await allocator.IsPageFreeAsync(page, this.ct), $"Page {page} of the reused run should be free again.");
        }

        Assert.Equal(pageCountBefore, db.Pages.PageCount);
        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TryPlaceTree_RelocatedCancellation_ReleasesRunAndRethrows(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        DatabaseFile db = harness.Database;
        PageAllocator allocator = harness.Services.PageAllocator;
        IndexPageLayout layout = JetFormat.ForNewDatabase(format).IndexPage;
        List<IndexEntry> entries = BuildLongKeyEntries(1000);
        int treePages = IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, db.Pages.PageCount).Pages.Count;
        Assert.True(treePages > 1, "The tree should span several pages.");

        // A free run the size of the tree makes the reservation reuse it, so
        // the tree is rebuilt at the run's first page; that rebuild fails.
        long freeRun = await CreateFreeRunAsync(harness, treePages, this.ct);
        SortedSet<long> allocatedBefore = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);
        long pageCountBefore = db.Pages.PageCount;

        int calls = 0;
        var runs = new ReservedPageRuns(allocator);
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => new IndexBTreeEditor(db.Format, harness.Pager, harness.Services.TDefWriter, allocator).TryPlaceTreeAsync(
            firstPage => ++calls == 2
                ? throw new OperationCanceledException(this.ct)
                : IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, firstPage),
            runs,
            this.ct).AsTask());

        Assert.Equal(2, calls);
        Assert.True(runs.IsEmpty);
        for (long page = freeRun; page < freeRun + treePages; page++)
        {
            Assert.True(await allocator.IsPageFreeAsync(page, this.ct), $"Page {page} of the reused run should be free again.");
        }

        Assert.Equal(pageCountBefore, db.Pages.PageCount);
        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TryPlaceTree_PageWriteFails_ReleasesRunAndRethrows(DatabaseFormat format)
    {
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            this.ct))
        {
        }

        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        DatabaseFile db = harness.Database;
        PageAllocator allocator = harness.Services.PageAllocator;
        IndexPageLayout layout = JetFormat.ForNewDatabase(format).IndexPage;
        List<IndexEntry> entries = BuildLongKeyEntries(1000);
        int treePages = IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, db.Pages.PageCount).Pages.Count;
        _ = await CreateFreeRunAsync(harness, treePages, this.ct);
        SortedSet<long> allocatedBefore = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);

        // The relocated build runs after the run is reserved; it arms a
        // fault on the second page write of the tree.
        long relocatedTo = -1;
        var runs = new ReservedPageRuns(allocator);
        _ = await Assert.ThrowsAsync<IOException>(() => new IndexBTreeEditor(db.Format, harness.Pager, harness.Services.TDefWriter, allocator).TryPlaceTreeAsync(
            firstPage =>
            {
                if (firstPage != db.Pages.PageCount)
                {
                    relocatedTo = firstPage;
                    stream.FailOnWrite(2);
                }

                return IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, firstPage);
            },
            runs,
            this.ct).AsTask());

        Assert.True(stream.Faulted);
        Assert.True(relocatedTo > 0, "The reservation should have reused the free run.");
        Assert.True(runs.IsEmpty);
        for (long page = relocatedTo; page < relocatedTo + treePages; page++)
        {
            Assert.True(await allocator.IsPageFreeAsync(page, this.ct), $"Page {page} of the run should be free again.");
            Assert.Equal(Constants.PageTypes.Freed, (await db.Pages.ReadPageCopyAsync(page, this.ct))[0]);
        }

        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
    }

    [Fact]
    public async Task TryPlaceTree_FailureInsideExplicitTransaction_ReleasesRunInJournal()
    {
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(DatabaseFormat.AceAccdb);
        IndexPageLayout layout = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb).IndexPage;
        List<IndexEntry> entries = BuildLongKeyEntries(1000);
        long freeRun;
        int treePages;
        SortedSet<long> allocatedBefore;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct))
        {
            DatabaseFile db = harness.Database;
            PageAllocator allocator = harness.Services.PageAllocator;
            treePages = IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, db.Pages.PageCount).Pages.Count;
            freeRun = await CreateFreeRunAsync(harness, treePages, this.ct);
            allocatedBefore = await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct);

            JetTransaction tx = await harness.Services.Transactions.BeginTransactionAsync(this.ct);
            int calls = 0;
            _ = await Assert.ThrowsAsync<IOException>(() => new IndexBTreeEditor(db.Format, harness.Pager, harness.Services.TDefWriter, allocator).TryPlaceTreeAsync(
                firstPage => ++calls == 2
                    ? throw new IOException("Injected failure after the reservation.")
                    : IndexBTreeBuilder.Build(layout, db.Format.PageSize, 2, entries, firstPage),
                new ReservedPageRuns(allocator),
                this.ct).AsTask());

            Assert.Equal(2, calls);
            Assert.Empty((await PageAudit.FindAllocatedPagesAsync(db, allocator, this.ct)).Except(allocatedBefore));
            await tx.CommitAsync(this.ct);
        }

        stream.Position = 0;
        await using WriterHarness reopened = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        for (long page = freeRun; page < freeRun + treePages; page++)
        {
            Assert.True(await reopened.Services.PageAllocator.IsPageFreeAsync(page, this.ct), $"Page {page} of the reused run should be free after commit.");
        }

        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(reopened.Database, reopened.Services.PageAllocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task IncrementalMaintenance_LaterIndexBails_ReleasesEarlierRebuiltTree(DatabaseFormat format)
    {
        // The primary key takes the incremental multi-level rebuild (a key
        // between every existing key defeats the surgical paths), then the
        // second index bails because its root is not an index page. The
        // primary key's rebuilt tree was reserved and written but never
        // linked, since the TDEF patch happens at the end of the call.
        await using MemoryStream stream = await CreateSeededDatabaseAsync(format, uniqueName: false);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        (long tdefPage, TableDef tableDef) = await ResolveTableAsync(harness, this.ct);

        IReadOnlyList<IndexMetadata> indexes = await ListIndexesAsync(harness.Database, this.ct);
        Assert.Equal(0, Assert.Single(indexes, index => index.Name == "PK").RealIndexNumber);
        IndexMetadata nameIndex = Assert.Single(indexes, index => index.Name == "IX_Name");
        Assert.Equal(1, nameIndex.RealIndexNumber);
        await StampPageTypeAsync(harness, nameIndex.FirstDp, Constants.PageTypes.Data, this.ct);

        var hints = new List<(RowLocation Loc, object[] Row)>(SeedRows);
        for (int i = 0; i < SeedRows; i++)
        {
            hints.Add((new RowLocation(10 + (i / 200), i % 200, 0, 0), [(i * 10) + 5, $"m{i:D5}"]));
        }

        SortedSet<long> allocatedBefore = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, this.ct);
        long pageCountBefore = harness.Database.Pages.PageCount;

        bool incremental = await harness.Services.Indexes.TryMaintainIndexesIncrementalAsync(tdefPage, tableDef, hints, deletedRows: null, this.ct);

        Assert.False(incremental);
        Assert.StartsWith("C6", harness.Services.Indexes.LastIncrementalBail, StringComparison.Ordinal);
        Assert.True(harness.Database.Pages.PageCount > pageCountBefore, "The primary key's rebuild should have reserved pages at the end of the file.");
        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
        Assert.Empty(allocatedBefore.Except(allocatedAfter));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BulkRebuild_LaterIndexThrows_ReleasesEarlierRebuiltTrees(DatabaseFormat format)
    {
        // The bulk rebuild builds and writes each index's tree in turn and
        // links them all at the end. A unique violation on the second index
        // throws after the first index's tree was written.
        await using MemoryStream stream = await CreateSeededDatabaseAsync(format, uniqueName: true);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        (long tdefPage, TableDef tableDef) = await ResolveTableAsync(harness, this.ct);

        var writtenRows = new List<LocatedRow>(SeedRows);
        for (int i = 0; i < SeedRows; i++)
        {
            string name = i == SeedRows - 1 ? "n00000" : $"n{i:D5}";
            writtenRows.Add(new LocatedRow(new RowLocation(10 + (i / 200), i % 200, 0, 0), [i * 10, name]));
        }

        SortedSet<long> allocatedBefore = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, this.ct);
        long pageCountBefore = harness.Database.Pages.PageCount;

        JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(
            () => harness.Services.Indexes.RebuildIndexesAsync(tdefPage, tableDef, TableName, writtenRows, this.ct).AsTask());

        Assert.Equal(JetErrorCode.UniqueViolation, ex.ErrorCode);
        Assert.Equal(TableName, ex.ErrorInfo.TableName);
        Assert.Equal("UQ_Name", ex.ErrorInfo.IndexName);
        Assert.Contains("Unique index violation", ex.Message, StringComparison.Ordinal);
        Assert.True(harness.Database.Pages.PageCount > pageCountBefore, "The first index's rebuild should have reserved pages at the end of the file.");
        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
        Assert.Empty(allocatedBefore.Except(allocatedAfter));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CatalogSplice_RootLeafSplitFallingBackToRebuild_LeavesNoReservedPageBehind(DatabaseFormat format)
    {
        // A fresh catalog's MSysObjects indexes are single root leaves, so
        // their first split has no ancestor path to rewrite and rebuilds the
        // index from its entries instead. The split's own reservation used to
        // be taken first and then abandoned, unwritten and still marked used.
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        SortedSet<long> unlinkedBefore = await PageAudit.FindUnlinkedReservedPagesAsync(harness.Database, harness.Services.PageAllocator, this.ct);
        int intermediateRootsBefore = await CountMsysObjectsIntermediateRootsAsync(harness.Database, this.ct);

        for (int i = 0; i < 180; i++)
        {
            string name = FormattableString.Invariant($"LinkedSplit_{i:D4}_Padding_For_Index_Split");
            await harness.Services.CatalogArtifacts.ExecutePlanAsync(
                new CatalogArtifactPlan(
                    [],
                    [new CatalogObjectArtifact(
                        -20_000 - i,
                        Constants.SystemObjects.TablesParentId,
                        name,
                        Constants.SystemObjects.LinkedTableType,
                        Constants.SystemObjects.LinkedTableFlags,
                        Constants.SystemObjects.DefaultOwnerBlob,
                        null)]),
                this.ct);
        }

        Assert.True(
            await CountMsysObjectsIntermediateRootsAsync(harness.Database, this.ct) > intermediateRootsBefore,
            "The inserts should have split an MSysObjects root leaf.");
        SortedSet<long> unlinkedAfter = await PageAudit.FindUnlinkedReservedPagesAsync(harness.Database, harness.Services.PageAllocator, this.ct);
        Assert.Empty(unlinkedAfter.Except(unlinkedBefore));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit)]
    public async Task CreateTables_UntilCatalogIndexSplits_LeaveNoReservedPageBehind(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        SortedSet<long> unlinkedBefore;
        int intermediateRootsBefore;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct))
        {
            unlinkedBefore = await PageAudit.FindUnlinkedReservedPagesAsync(harness.Database, harness.Services.PageAllocator, this.ct);
            intermediateRootsBefore = await CountMsysObjectsIntermediateRootsAsync(harness.Database, this.ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            WriteModes.WriterOptions(mode),
            leaveOpen: true,
            this.ct))
        {
            await using JetTransaction? tx = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(this.ct) : null;
            for (int i = 0; i < 120; i++)
            {
                await writer.CreateTableAsync(
                    FormattableString.Invariant($"Table_{i:D4}_Padding_For_The_Catalog_Index_Split"),
                    [new ColumnDefinition("Id", typeof(int))],
                    this.ct);
            }

            if (tx is not null)
            {
                await tx.CommitAsync(this.ct);
            }
        }

        stream.Position = 0;
        await using WriterHarness reopened = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        Assert.True(
            await CountMsysObjectsIntermediateRootsAsync(reopened.Database, this.ct) > intermediateRootsBefore,
            "The tables should have split an MSysObjects root leaf.");
        SortedSet<long> unlinkedAfter = await PageAudit.FindUnlinkedReservedPagesAsync(reopened.Database, reopened.Services.PageAllocator, this.ct);
        Assert.Empty(unlinkedAfter.Except(unlinkedBefore));
    }

    [Fact]
    public async Task ReservedRun_InitializerFails_ReleasesAllPages()
    {
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.Jet4Mdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            this.ct))
        {
        }

        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        PageAllocator allocator = harness.Services.PageAllocator;
        SortedSet<long> allocatedBefore = await PageAudit.FindAllocatedPagesAsync(harness.Database, allocator, this.ct);
        long pageCountBefore = harness.Database.Pages.PageCount;

        var reservations = new ReservedPageRuns(allocator);
        long firstPage = await reservations.ReserveAsync(3, this.ct);
        Assert.Equal(pageCountBefore, firstPage);
        byte[] initialized = new byte[harness.Database.Format.PageSize];
        initialized[0] = Constants.PageTypes.Data;
        stream.FailOnWrite(2);
        _ = await Assert.ThrowsAsync<IOException>(async () =>
        {
            try
            {
                for (int offset = 0; offset < 3; offset++)
                {
                    await harness.Pager.WritePageAsync(firstPage + offset, initialized, this.ct);
                }
            }
            finally
            {
                await reservations.ReleaseAsync();
            }
        });

        Assert.True(stream.Faulted);
        Assert.Equal(pageCountBefore + 3, harness.Database.Pages.PageCount);
        for (int offset = 0; offset < 3; offset++)
        {
            Assert.True(await allocator.IsPageFreeAsync(firstPage + offset, this.ct), $"Reserved page {firstPage + offset} should be free after initialization fails.");
        }

        SortedSet<long> allocatedAfter = await PageAudit.FindAllocatedPagesAsync(harness.Database, allocator, this.ct);
        Assert.Empty(allocatedAfter.Except(allocatedBefore));
        Assert.Empty(allocatedBefore.Except(allocatedAfter));
    }

    /// <summary>
    /// Builds sorted leaf entries with 40-byte keys, so a few hundred entries
    /// span several leaves and an intermediate page on every format.
    /// </summary>
    /// <param name="count">The number of entries.</param>
    private static List<IndexEntry> BuildLongKeyEntries(int count)
    {
        var entries = new List<IndexEntry>(count);
        for (int i = 0; i < count; i++)
        {
            byte[] key = new byte[40];
            key[0] = 0x7F;
            key[1] = (byte)(i >> 8);
            key[2] = (byte)(i & 0xFF);
            entries.Add(new IndexEntry(key, 10 + (i / 100), (byte)(i % 100)));
        }

        return entries;
    }

    /// <summary>
    /// Appends <paramref name="pageCount"/> pages and frees them again, so the
    /// global usage map lists a free run of exactly that length.
    /// </summary>
    /// <param name="harness">The writer harness.</param>
    /// <param name="pageCount">The run length.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The first page of the run.</returns>
    private static async Task<long> CreateFreeRunAsync(WriterHarness harness, int pageCount, CancellationToken cancellationToken)
    {
        PageAllocator allocator = harness.Services.PageAllocator;
        long first = await allocator.ReserveContiguousPagesAsync(pageCount, cancellationToken);
        for (long page = first; page < first + pageCount; page++)
        {
            await allocator.DeallocatePageAsync(page, cancellationToken);
        }

        for (long page = first; page < first + pageCount; page++)
        {
            Assert.True(await allocator.IsPageFreeAsync(page, cancellationToken), $"Setup: page {page} should be free.");
        }

        return first;
    }

    private static async Task<MemoryStream> CreateEmptyDatabaseAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
    }

    private static async Task<MemoryStream> CreateSeededDatabaseAsync(DatabaseFormat format, bool uniqueName)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken))
        {
            IndexDefinition[] indexes = uniqueName
                ? [new IndexDefinition("IX_Id", "Id"), new IndexDefinition("UQ_Name", "Name") { IsUnique = true }]
                : [new IndexDefinition("PK", "Id") { IsPrimaryKey = true }, new IndexDefinition("IX_Name", "Name")];
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                indexes,
                cancellationToken);

            var rows = new List<object?[]>(SeedRows);
            for (int i = 0; i < SeedRows; i++)
            {
                rows.Add([i * 10, $"n{i:D5}"]);
            }

            _ = await writer.InsertRowsAsync(TableName, rows, cancellationToken);
        }

        stream.Position = 0;
        return stream;
    }

    private static async Task<(long TDefPage, TableDef TableDef)> ResolveTableAsync(WriterHarness harness, CancellationToken cancellationToken)
    {
        ResolvedTable resolved = await harness.Services.Catalog.ResolveRequiredTableAsync(TableName, cancellationToken);
        return (resolved.Entry.TDefPage, resolved.Definition);
    }

    private static async Task<IReadOnlyList<IndexMetadata>> ListIndexesAsync(DatabaseFile db, CancellationToken cancellationToken)
    {
        using var services = new ReaderServices(db, UncachedReaderOptions);
        return await services.Indexes.ListIndexesAsync(TableName, cancellationToken);
    }

    private static async Task StampPageTypeAsync(WriterHarness writer, long pageNumber, byte pageType, CancellationToken cancellationToken)
    {
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);
        page[0] = pageType;
        await writer.Pager.WritePageAsync(pageNumber, page, cancellationToken);
    }

    private static async Task<int> CountMsysObjectsIntermediateRootsAsync(DatabaseFile db, CancellationToken cancellationToken)
    {
        byte[] tdef = (await db.TableDefs.ReadTDefBytesAsync(2, cancellationToken))!;
        int numCols = Ru16(tdef, db.Format.TDef.NumCols);
        int numRealIdx = Ri32(tdef, db.Format.TDef.NumRealIdx);
        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(db.Format, tdef, numCols, numRealIdx);
        int count = 0;
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            int phys = db.Format.Index.RealIdxPhysOffset(realIdxDescStart, ri);
            long root = (uint)Ri32(tdef, db.Format.Index.FirstDpAbsoluteOffset(phys));
            byte[] page = await db.Pages.ReadPageCopyAsync(root, cancellationToken);
            if (page[0] == Constants.PageTypes.IndexIntermediate)
            {
                count++;
            }
        }

        return count;
    }
}
