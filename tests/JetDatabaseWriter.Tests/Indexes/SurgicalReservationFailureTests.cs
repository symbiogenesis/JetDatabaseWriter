namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Globalization;
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
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Surgical splits release unpublished append runs and public write failures restore their original trees.</summary>
public sealed class SurgicalReservationFailureTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, true)]
    public async Task Split_FailureAfterFirstNewPage_ReleasesAppendRunAndPreservesOriginalTree(DatabaseFormat format, bool crossLeaf, bool cancel)
    {
        (byte[] baseline, long root) = await CreateEditorBaselineAsync(format);
        int freshPages;
        await using (var successful = new WriteFaultStream())
        {
            await successful.WriteAsync(baseline, Ct);
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(successful, Options(), cancellationToken: Ct);
            long before = writer.Pager.PageCount;
            Assert.True(await SplitAsync(writer, root, crossLeaf, Ct));
            freshPages = checked((int)(writer.Pager.PageCount - before));
            Assert.True(freshPages >= 2);
        }

        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(baseline, Ct);
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, Options(), cancellationToken: Ct);
        SortedSet<long> allocated = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct);
        var originalPages = new Dictionary<long, byte[]>();
        foreach (long page in await IndexLeafChain.ReadTreePagesAsync(harness.Database, 2, root, Ct))
        {
            originalPages.Add(page, await harness.Database.Pages.ReadPageCopyAsync(page, Ct));
        }

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        // The append-only reservation marks each new global-map bit before
        // writing the new tree. Stop after the first tree page, before any link.
        if (cancel)
        {
            stream.CancelAfterWrites(freshPages + 1, cancellation);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => SplitAsync(harness, root, crossLeaf, cancellation.Token).AsTask());
        }
        else
        {
            stream.FailDuringWrite(freshPages + 2);
            await Assert.ThrowsAsync<IOException>(() => SplitAsync(harness, root, crossLeaf, Ct).AsTask());
        }

        Assert.True(stream.Faulted);
        foreach ((long page, byte[] bytes) in originalPages)
        {
            Assert.Equal(bytes, await harness.Database.Pages.ReadPageCopyAsync(page, Ct));
        }

        Assert.Equal(allocated, await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct));
        Assert.Empty(await PageAudit.FindUnlinkedReservedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task PublicSplit_LatePartialWriteOrFlushFailure_RestoresBytesAndWriterState(DatabaseFormat format, bool flushFault)
    {
        byte[] baseline = await CreatePublicBaselineAsync(format);
        int writes;
        await using (var successful = new WriteFaultStream())
        {
            await successful.WriteAsync(baseline, Ct);
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(successful, Options(), cancellationToken: Ct);
            CatalogEntry? table = await writer.Services.Catalog.GetCatalogEntryAsync("T", Ct);
            Assert.NotNull(table);
            long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
            Assert.Equal(Constants.PageTypes.IndexIntermediate, (await writer.Database.Pages.ReadPageCopyAsync(root, Ct))[0]);
            int before = successful.WriteCount;
            await writer.InsertRowsAsync("T", PublicAdditions(), Ct);
            writes = successful.WriteCount - before;
            Assert.Null(writer.Services.Indexes.LastIncrementalBail);
        }

        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(baseline, Ct);
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, Options(), cancellationToken: Ct);
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync("T", Ct);
        Assert.NotNull(entry);
        byte[] original = stream.ToArray();
        SortedSet<long> allocated = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct);
        if (flushFault)
        {
            stream.FailOnFlush(1);
        }
        else
        {
            stream.FailDuringWrite(writes);
        }

        await Assert.ThrowsAsync<IOException>(() => harness.InsertRowsAsync("T", PublicAdditions(), Ct).AsTask());
        Assert.True(stream.Faulted);
        Assert.Equal(original, stream.ToArray());
        Assert.Equal(allocated, await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct));
        Assert.Empty(await PageAudit.FindUnlinkedReservedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct));
        Assert.Empty(await PageAudit.FindUnreachableIndexPagesAsync(harness.Database, harness.Services.PageAllocator, entry.TDefPage, Ct));
        await harness.InsertRowsAsync("T", PublicAdditions(), Ct);
        long currentRoot = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, entry.TDefPage, Ct));
        await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, entry.TDefPage, currentRoot, Ct);
    }

    private static AccessWriterOptions Options() => new() { UseLockFile = false, UseByteRangeLocks = false, PageCacheSize = 0, UseTransactionalWrites = true };

    private static async Task<(byte[] Image, long Root)> CreateEditorBaselineAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        long root;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, Options(), leaveOpen: true, Ct))
        {
            var editor = new IndexBTreeEditor(writer.Database.Format, writer.Pager, writer.Services.TDefWriter, writer.Services.PageAllocator);
            var runs = new ReservedPageRuns(writer.Services.PageAllocator);
            IndexBTreeBuildResult tree = Assert.IsType<IndexBTreeBuildResult>(await editor.TryPlaceTreeAsync(first => IndexBTreeBuilder.Build(writer.Database.Format.IndexPage, writer.Database.Format.PageSize, 2, OriginalEntries(), first), runs, Ct));
            runs.MarkLinked();
            root = tree.RootPageNumber;
        }

        return (stream.ToArray(), root);
    }

    private static List<IndexEntry> OriginalEntries()
    {
        var entries = new List<IndexEntry>();
        for (int index = 0; index < 200; index++)
        {
            byte[] key = new byte[80];
            key[0] = 0x7F;
            key[1] = checked((byte)(index >> 8));
            key[2] = unchecked((byte)index);
            entries.Add(new IndexEntry(key, 10 + (index / 100), checked((byte)(index % 100))));
        }

        return entries;
    }

    private static async ValueTask<bool> SplitAsync(AccessWriter writer, long root, bool crossLeaf, CancellationToken cancellationToken)
    {
        var additions = new List<IndexEntry>();
        byte[] key = OriginalEntries()[1].Key;
        for (int index = 0; index < 120; index++)
        {
            additions.Add(new IndexEntry(key, 1000 + index, 0));
        }

        var editor = new IndexBTreeEditor(writer.Database.Format, writer.Pager, writer.Services.TDefWriter, writer.Services.PageAllocator);
        if (crossLeaf)
        {
            additions.Add(new IndexEntry(OriginalEntries()[150].Key, 2000, 0));
        }

        return (await editor.TrySurgicalMultiLevelMaintainAsync(writer.Database.Format.IndexPage, 2, root, 0, additions, [], cancellationToken)).Handled;
    }

    private static async Task<byte[]> CreatePublicBaselineAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, Options(), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Key", typeof(string), maxLength: 255), new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("IX_Key", "Key")], Ct);
            await writer.InsertRowsAsync("T", Enumerable.Range(0, 40).Select(id => new object?[] { PublicKey(id * 100), id }), Ct);
        }

        return stream.ToArray();
    }

    private static IEnumerable<object?[]> PublicAdditions() => Enumerable.Range(1, 80).Select(id => new object?[] { PublicKey((id * 17) + 1), 1000 + id });

    private static string PublicKey(int id) => id.ToString("D6", CultureInfo.InvariantCulture) + new string('k', 120);
}
