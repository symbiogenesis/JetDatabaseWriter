namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Surgical index edits cannot turn a forged sibling pointer into foreign-page write authority.</summary>
/// <param name="seeds">Caches independent copies of the complete relationship fixture.</param>
public sealed class IndexNeighborOwnershipTests(GeneratedDatabaseCache seeds) : IClassFixture<GeneratedDatabaseCache>
{
    public static TheoryData<DatabaseFormat, WriteMode, bool> FormatsModesAndOwners()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode, false);
                data.Add(format, mode, true);
            }
        }

        return data;
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ForgedSibling_RefusesBeforeWritingAnotherPhysicalTree(DatabaseFormat format, bool sameOwner)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, ct);
        var editor = new IndexBTreeEditor(writer.Database.Format, writer.Pager, writer.Services.TDefWriter, writer.Services.PageAllocator);
        IndexPageLayout layout = writer.Database.Format.IndexPage;
        List<IndexEntry> entries = Entries(1000);
        IndexBTreeBuildResult tree = await PlaceAsync(editor, writer, entries, owner: 2, ct);
        IndexBTreeBuildResult other = await PlaceAsync(editor, writer, Entries(100), sameOwner ? 2 : 3, ct);
        long source = tree.FirstPageNumber;
        long foreign = other.FirstPageNumber;
        byte[] leaf = await writer.Pager.ReadPageCopyAsync(source, ct);
        byte[] foreignLeaf = await writer.Pager.ReadPageCopyAsync(foreign, ct);
        Assert.True(IndexPageCodec.ReadNextPage(layout, leaf) > 0);
        IndexPageCodec.WriteNextPage(layout, leaf, foreign);
        IndexPageCodec.WritePrevPage(layout, foreignLeaf, source);
        await writer.Pager.WritePageAsync(source, leaf, ct);
        await writer.Pager.WritePageAsync(foreign, foreignLeaf, ct);
        byte[] before = stream.ToArray();
        var additions = new List<IndexEntry>();
        for (int index = 0; index < 120; index++)
        {
            additions.Add(new(entries[1].Key, 1000 + index, 0));
        }

        Exception? failure = await Record.ExceptionAsync(async () =>
            await editor.TrySurgicalMultiLevelMaintainAsync(layout, 2, tree.RootPageNumber, 0, additions, [], ct));

        Assert.Equal(foreignLeaf, await writer.Pager.ReadPageCopyAsync(foreign, ct));
        JetCorruptDataException corruption = Assert.IsType<JetCorruptDataException>(failure);
        Assert.Equal(JetErrorCode.CorruptIndex, corruption.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(FormatsModesAndOwners))]
    public async Task ForgedSibling_RollsBackCompleteRelationshipStatement(DatabaseFormat format, WriteMode mode, bool sameOwner)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await seeds.CopyToStreamAsync(format.ToString(), () => CreateRelationshipSeedAsync(format, ct));
        var options = new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = mode == WriteMode.AutoCommit, PageCacheSize = 0 };
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, ct);
        long child = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("C", ct)).TDefPage;
        List<long> childRoots = await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, child, ct);
        Assert.Equal(3, childRoots.Count);
        long otherRoot = sameOwner
            ? childRoots[1]
            : Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("F", ct)).TDefPage, ct));
        var editor = new IndexBTreeEditor(writer.Database.Format, writer.Pager, writer.Services.TDefWriter, writer.Services.PageAllocator);
        IndexPageLayout layout = writer.Database.Format.IndexPage;
        long source = await editor.DescendToLeftmostLeafAsync(layout, childRoots[0], ct);
        long foreign = await editor.DescendToLeftmostLeafAsync(layout, otherRoot, ct);
        byte[] sourceLeaf = await writer.Pager.ReadPageCopyAsync(source, ct);
        byte[] foreignLeaf = await writer.Pager.ReadPageCopyAsync(foreign, ct);
        Assert.True(IndexPageCodec.ReadNextPage(layout, sourceLeaf) > 0);
        IndexPageCodec.WriteNextPage(layout, sourceLeaf, foreign);
        IndexPageCodec.WritePrevPage(layout, foreignLeaf, source);
        await writer.Pager.WritePageAsync(source, sourceLeaf, ct);
        await writer.Pager.WritePageAsync(foreign, foreignLeaf, ct);
        byte[] before = stream.ToArray();

        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.InsertRowAsync("C", [-1, 2, -1], ct));
                Assert.Equal(JetErrorCode.CorruptIndex, failure.ErrorCode);
                Assert.Equal(before, stream.ToArray());
                Assert.Equal(2560, (await writer.Services.Snapshots.ReadRowsAsync(child, ct)).Count);
            },
            ct);

        Assert.Equal(before, stream.ToArray());
        Assert.Equal(foreignLeaf, await writer.Pager.ReadPageCopyAsync(foreign, ct));
    }

    [Fact]
    public void LeafChainAdmission_RefusesIncompleteOrAliasedEvidence()
    {
        long[] members = [4, 7, 9];
        long[] chain = [9, 4, 7];
        long[] previous = [0, 9, 4];
        long[] next = [4, 7, 0];
        long[] owners = [3, 3, 3];
        byte[] kinds = [4, 4, 4];
        byte[] scratch = [255, 255, 255];
        Assert.True(IndexMutationKernel.ValidateLeafChain(members, chain, previous, next, owners, kinds, 3, 10, scratch));
        Assert.False(IndexMutationKernel.ValidateLeafChain(members, [9, 4, 4], previous, next, owners, kinds, 3, 10, scratch));
        Assert.False(IndexMutationKernel.ValidateLeafChain(members, [9, 4, 8], previous, next, owners, kinds, 3, 10, scratch));
        Assert.False(IndexMutationKernel.ValidateLeafChain(members, chain, [0, 9, 9], next, owners, kinds, 3, 10, scratch));
        Assert.False(IndexMutationKernel.ValidateLeafChain(members, chain, previous, [4, 7, 9], owners, kinds, 3, 10, scratch));
        Assert.False(IndexMutationKernel.ValidateLeafChain(members, chain, previous, next, [3, 2, 3], kinds, 3, 10, scratch));
        Assert.False(IndexMutationKernel.ValidateLeafChain(members, chain, previous, next, owners, [4, 3, 4], 3, 10, scratch));
        Assert.False(IndexMutationKernel.ValidateLeafChain(members, chain, previous, next, owners, kinds, 3, 10, kinds));
        Assert.Equal(new byte[] { 4, 4, 4 }, kinds);
    }

    private static async Task<byte[]> CreateRelationshipSeedAsync(DatabaseFormat format, CancellationToken ct)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("PK_P", "Id") { IsPrimaryKey = true }], ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("Alternate", typeof(int))], [new IndexDefinition("PK_C", "Id") { IsPrimaryKey = true }, new IndexDefinition("IX_Alternate", "Alternate")], ct);
            await writer.CreateTableAsync("F", [new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("PK_F", "Id") { IsPrimaryKey = true }], ct);
            await writer.InsertRowsAsync("P", [[1], [2]], ct);
            await writer.InsertRowsAsync("C", Enumerable.Range(0, 2560).Select(static id => new object[] { id, 1, id }), ct);
            await writer.InsertRowsAsync("F", Enumerable.Range(0, 100).Select(static id => new object[] { id }), ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), ct);
        }

        return stream.ToArray();
    }

    private static async Task<IndexBTreeBuildResult> PlaceAsync(IndexBTreeEditor editor, AccessWriter writer, List<IndexEntry> entries, long owner, CancellationToken ct)
    {
        var runs = new ReservedPageRuns(writer.Services.PageAllocator);
        IndexBTreeBuildResult tree = Assert.IsType<IndexBTreeBuildResult>(await editor.TryPlaceTreeAsync(
            first => IndexBTreeBuilder.Build(writer.Database.Format.IndexPage, writer.Database.Format.PageSize, owner, entries, first), runs, ct));
        runs.MarkLinked();
        return tree;
    }

    private static List<IndexEntry> Entries(int count)
    {
        var entries = new List<IndexEntry>(count);
        for (int index = 0; index < count; index++)
        {
            byte[] key = new byte[40];
            key[0] = 0x7F;
            key[1] = checked((byte)(index >> 8));
            key[2] = unchecked((byte)index);
            entries.Add(new(key, 10 + (index / 100), checked((byte)(index % 100))));
        }

        return entries;
    }
}
