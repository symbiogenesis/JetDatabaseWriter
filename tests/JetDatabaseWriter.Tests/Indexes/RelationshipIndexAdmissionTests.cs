namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Verification;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Boundary controls for the generated relationship index admission.</summary>
public sealed class RelationshipIndexAdmissionTests
{
    [Theory]
    [InlineData(1U)]
    [InlineData(2U)]
    public void ModernRelationshipKey_RequiresNativeV2000IndexFlag(uint format)
    {
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(format, 4, 1, true, true, false, 0));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(format, 4, 1, true, true, false, 0x09));
        Assert.True(IndexMutationKernel.AdmitRelationshipKey(format, 4, 1, true, true, false, 0x89));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 2U, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, 2U, false)]
    [InlineData(DatabaseFormat.AceAccdb, 2U, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, uint.MaxValue, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, uint.MaxValue, false)]
    [InlineData(DatabaseFormat.AceAccdb, uint.MaxValue, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, uint.MaxValue, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, uint.MaxValue, true)]
    [InlineData(DatabaseFormat.AceAccdb, uint.MaxValue, true)]
    public async Task AdmittedRelationship_UnreadablePostEditTreeRefusesCompletion(DatabaseFormat format, uint unreadableRoot, bool corruptTail)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("PK", "Id") { IsPrimaryKey = true }], ct);
        long tablePage = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", ct)).TDefPage;
        LogicalTDefChain chain = (await LogicalTDefChain.ReadAsync(tablePage, writer.Database.Format.PageSize, writer.Pager.ReadPageAsync, PageBuffers.Return, retainPageNumbers: true, ct))!;
        int start = IndexCatalogReader.LocateRealIdxDescStart(writer.Database.Format, chain.Bytes, Ru16(chain.Bytes, writer.Database.Format.TDef.NumCols), 1);
        Assert.True(writer.Database.Format.Index.TryReadRealIdxSlotWithKeyColumns(chain.Bytes, start, 0, out RealIdxSlot slot, out List<KeyColumn>? keys));
        var slots = new List<(int RealIdxNum, RealIdxEntry Entry)> { (0, slot.ToEntry(keys)) };
        if (corruptTail)
        {
            byte[] intermediate = IndexPageCodec.BuildIntermediatePage(writer.Database.Format.IndexPage, writer.Database.Format.PageSize, tablePage, [new(new IndexEntry([1], 3, 0), 3)], 0, 0, 0);
            Wu32(intermediate, writer.Database.Format.IndexPage.TailPageOffset, unreadableRoot);
            await writer.Pager.WritePageAsync(Ru32(chain.Bytes, slot.FirstDpOffset), intermediate, ct);
        }
        else
        {
            Wu32(chain.Bytes, slot.FirstDpOffset, unreadableRoot);
        }

        // Exercise the existing commit phase with an already admitted image;
        // publication must refuse instead of asking its caller to bulk rebuild.
        Type stateType = typeof(IndexMaintainer).GetNestedType("IncrementalMetadataState", BindingFlags.NonPublic)!;
        object state = stateType.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)[0].Invoke([chain]);
        stateType.GetProperty("RequireExactRelationshipMaps")!.SetValue(state, true);
        MethodInfo commit = typeof(IndexMaintainer).GetMethod("CommitIncrementalMetadataAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        byte[] before = stream.ToArray();
        var pending = (ValueTask<bool>)commit.Invoke(writer.Services.Indexes, [tablePage, writer.Database.Format.IndexPage, slots, 1, state, new ReservedPageRuns(writer.Services.PageAllocator), ct])!;
        JetCorruptDataException refusal = await Assert.ThrowsAsync<JetCorruptDataException>(pending.AsTask);
        Assert.Equal(JetErrorCode.CorruptIndex, refusal.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TextRelationship_RetainsConservativeRebuild(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("P", [new ColumnDefinition("Code", typeof(string), maxLength: 20)], [new IndexDefinition("PK_P", "Code") { IsPrimaryKey = true }], ct);
        await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Code", typeof(string), maxLength: 20)], [new IndexDefinition("PK_C", "Id") { IsPrimaryKey = true }], ct);
        await writer.InsertRowAsync("P", ["parent"], ct);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK", "P", "Code", "C", "Code"), ct);
        await writer.InsertRowAsync("C", [1, "parent"], ct);
        Assert.Equal("C1c unsupported foreign-key index form", writer.Services.Indexes.LastIncrementalBail);
        await writer.InsertRowAsync("C", [2, "parent"], ct);
        Assert.Equal("C1c unsupported foreign-key index form", writer.Services.Indexes.LastIncrementalBail);
        Assert.Equal(2, (await writer.Services.Snapshots.ReadRowsAsync((await writer.Services.Catalog.GetRequiredCatalogEntryAsync("C", ct)).TDefPage, ct)).Count);
    }

    [Fact]
    public void PageMembership_RefusesMissingRootStaleMapDuplicatesAndNonpositivePages()
    {
        Assert.True(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 19], [4, 12, 19]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(13, [4, 12, 19], [4, 12, 19]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 19], [4, 12, 20]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 19], [4, 12]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [4, 12, 12], [4, 12, 12]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [0, 12], [0, 12]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [12, 4], [12, 4]));
        Assert.False(IndexMutationKernel.ValidateIndexPageSet(12, [], []));
    }

    [Fact]
    public void RelationshipKey_AdmitsNativeInt32FlagsAndRefusesUnqualifiedForms()
    {
        Assert.True(IndexMutationKernel.AdmitRelationshipKey(0, 4, 1, true, true, false, 0));
        Assert.True(IndexMutationKernel.AdmitRelationshipKey(1, 4, 1, true, true, false, 0x89));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(1, 10, 1, true, false, false, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(1, 4, 2, true, true, false, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(1, 4, 1, false, true, false, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(1, 4, 1, true, true, true, 0x80));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(1, 4, 1, true, true, false, 0x84));
        Assert.False(IndexMutationKernel.AdmitRelationshipKey(1, 4, 1, true, true, false, 0x90));
    }
}
