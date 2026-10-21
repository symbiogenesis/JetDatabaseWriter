namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Planning must finish all fallible neighbor reads before publishing index pages.</summary>
public sealed class IndexMutationPhaseTests
{
    [Fact]
    public async Task CapturedPathLookup_ObservesCancellationWithoutReadingPages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: ct))
        {
        }

        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, PageCacheSize = 0 }, cancellationToken: ct);
        var editor = new IndexBTreeEditor(harness.Database.Format, harness.Pager, harness.Services.TDefWriter, harness.Services.PageAllocator);
        var path = new List<DescentStep> { new(2, [], [new(new IndexEntry([2], 1, 0), 3)], 0) };

        // Exercise the already-captured path directly: cancellation after a
        // descent must remain observable when later keys need no page reads.
        Type groupType = typeof(IndexBTreeEditor).GetNestedType("LeafGroup", BindingFlags.NonPublic)!;
        object previous = groupType.GetConstructors()[0].Invoke([3L, path, null, null]);
        object groups = typeof(Dictionary<,>).MakeGenericType(typeof(long), groupType).GetConstructor(Type.EmptyTypes)!.Invoke(null);
        MethodInfo lookup = typeof(IndexBTreeEditor).GetMethod("DescendOrLookupGroupAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        await cancellation.CancelAsync();
        int readsBefore = stream.ReadCount;
        object pending = lookup.Invoke(editor, [harness.Database.Format.IndexPage, 2L, new byte[] { 1 }, groups, previous, cancellation.Token])!;
        var task = (Task)pending.GetType().GetMethod("AsTask")!.Invoke(pending, null)!;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        Assert.Equal(readsBefore, stream.ReadCount);
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, false, true)]
    [InlineData(true, false, true)]
    public async Task Preparation_RefusalReadFailureOrCancellation_WritesNothing(bool crossLeaf, bool cancel, bool refuse)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, cancellationToken: ct))
        {
        }

        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, PageCacheSize = 0 }, cancellationToken: ct);
        var editor = new IndexBTreeEditor(harness.Database.Format, harness.Pager, harness.Services.TDefWriter, harness.Services.PageAllocator);
        IndexPageLayout layout = harness.Database.Format.IndexPage;
        var entries = new List<IndexEntry>();
        for (int i = 0; i < 1000; i++)
        {
            byte[] key = new byte[40];
            key[0] = 0x7F;
            key[1] = checked((byte)(i >> 8));
            key[2] = unchecked((byte)i);
            entries.Add(new IndexEntry(key, 10 + (i / 100), checked((byte)(i % 100))));
        }

        var runs = new ReservedPageRuns(harness.Services.PageAllocator);
        IndexBTreeBuildResult tree = Assert.IsType<IndexBTreeBuildResult>(await editor.TryPlaceTreeAsync(first => IndexBTreeBuilder.Build(layout, harness.Database.Format.PageSize, 2, entries, first), runs, ct));
        runs.MarkLinked();
        Assert.Equal(Constants.IndexLeafPage.PageTypeIntermediate, tree.Pages[checked((int)(tree.RootPageNumber - tree.FirstPageNumber))][0]);
        var additions = new List<IndexEntry>();
        for (int i = 0; i < 100; i++)
        {
            additions.Add(new IndexEntry(entries[1].Key, 1000 + i, 0));
        }

        if (crossLeaf)
        {
            additions.Add(new IndexEntry(entries[500].Key, 2000, 0));
        }

        // Each uncached descent reads root + leaf. The target leaf is then
        // read once for splicing; the next read stages its untouched neighbor.
        int neighborRead = crossLeaf ? 7 : 4;
        int writesBefore = stream.WriteCount;
        byte[] before = stream.ToArray();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (refuse)
        {
            byte[] oversized = new byte[harness.Database.Format.PageSize * 2];
            entries[1].Key.CopyTo(oversized, 0);
            Assert.False(await MutateAsync(editor, layout, tree.RootPageNumber, [new IndexEntry(oversized, 1000, 0)], cancellation.Token));
        }
        else if (cancel)
        {
            stream.CancelAfterReads(neighborRead, cancellation);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => MutateAsync(editor, layout, tree.RootPageNumber, additions, cancellation.Token).AsTask());
        }
        else
        {
            stream.FailOnRead(neighborRead);
            await Assert.ThrowsAsync<IOException>(() => MutateAsync(editor, layout, tree.RootPageNumber, additions, cancellation.Token).AsTask());
        }

        Assert.Equal(!refuse, stream.Faulted);
        Assert.Equal(writesBefore, stream.WriteCount);
        Assert.Equal(before, stream.ToArray());
    }

    private static async ValueTask<bool> MutateAsync(
        IndexBTreeEditor editor,
        IndexPageLayout layout,
        long root,
        List<IndexEntry> additions,
        CancellationToken ct)
        => (await editor.TrySurgicalMultiLevelMaintainAsync(layout, 2, root, 0, additions, [], ct)).Handled;
}
