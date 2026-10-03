namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Foreign-key logical-index entries on Jet3 (Access 97) databases. The
/// writer emits them in the shape Access 97 wrote in indexTestV1997.mdb: a
/// 20-byte entry with no cookie, a 39-byte real index with <c>flags</c> 0x00
/// and <c>used_pages</c> 0, names in a 1-byte-length ANSI record inserted in
/// case-insensitive name order, the parent side sharing its primary-key real
/// index, and the cascade flags on both sides. Each test starts from a copy
/// of that fixture, whose <c>Table2</c> (keyed on <c>id</c>) is already the
/// parent of <c>Table1</c> through 'Table2Table1'. Relationship names stay
/// short because Jet3 catalog rows are limited in length.
/// </summary>
/// <param name="cache">Caches the Access-authored fixture.</param>
public sealed class Jet3ForeignKeyIndexTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    private const string Parent = "Table2";
    private const string ParentKey = "id";
    private const string Child = "C";
    private const string Relationship = "Rel";

    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateRelationship_Jet3_EmitsLinkedForeignKeyEntriesOnBothTables()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        IReadOnlyList<IndexMetadata> parentBefore = await this.ListIndexesAsync(stream, Parent);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateChildAsync(writer, this.ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, Parent, ParentKey, Child, "ParentId") { CascadeDeletes = true }, this.ct);
        }

        long parentPage = await this.GetTDefPageAsync(stream, Parent);
        long childPage = await this.GetTDefPageAsync(stream, Child);
        IReadOnlyList<IndexMetadata> parentIndexes = await this.ListIndexesAsync(stream, Parent);
        IReadOnlyList<IndexMetadata> childIndexes = await this.ListIndexesAsync(stream, Child);

        IndexMetadata parentFk = Assert.Single(parentIndexes, i => i.Kind == IndexKind.ForeignKey && i.RelatedTablePage == childPage);
        IndexMetadata childFk = Assert.Single(childIndexes, i => i.Kind == IndexKind.ForeignKey);
        Assert.Equal(".rB", parentFk.Name);
        Assert.Equal(Relationship, childFk.Name);
        Assert.Equal(ParentKey, Assert.Single(parentFk.Columns).Name);
        Assert.Equal("ParentId", Assert.Single(childFk.Columns).Name);
        Assert.Equal(parentPage, childFk.RelatedTablePage);
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);

        // The parent shares its primary key's real index, as Access 97 did
        // for '.rC', although the non-unique 'id' index covers the same column.
        Assert.Equal(Assert.Single(parentIndexes, i => i.Kind == IndexKind.PrimaryKey).RealIndexNumber, parentFk.RealIndexNumber);

        // Cascade flags on both sides, as Access 97 wrote them.
        Assert.True(parentFk.CascadeDeletes);
        Assert.True(childFk.CascadeDeletes);
        Assert.False(parentFk.CascadeUpdates);
        Assert.False(childFk.CascadeUpdates);

        // The Access-authored relationship is untouched.
        IndexMetadata accessParentFk = Assert.Single(parentIndexes, i => i.Name == ".rC");
        Assert.Equal(Assert.Single(parentBefore, i => i.Name == ".rC"), accessParentFk, IndexMetadataComparer.Instance);
        IndexMetadata accessChildFk = Assert.Single(await this.ListIndexesAsync(stream, "Table1"), i => i.Name == "Table2Table1");
        Assert.Equal(accessChildFk.IndexNumber, accessParentFk.RelatedIndexNumber);
        Assert.Equal(accessParentFk.IndexNumber, accessChildFk.RelatedIndexNumber);
    }

    [Fact]
    public async Task CreateRelationship_Jet3_WritesTheAccessJet3EntryLayout()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            // Indexes named either side of the relationship, so its entry
            // goes in between them.
            await writer.CreateTableAsync(
                Child,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Note", typeof(string), maxLength: 20),
                ],
                [
                    new IndexDefinition("AIdx", "Note"),
                    new IndexDefinition("PrimaryKey", "Id") { IsPrimaryKey = true },
                    new IndexDefinition("ZIdx", ["Note", "Id"]),
                ],
                this.ct);
        }

        TDefIndexSection parentBefore = await this.ReadIndexSectionAsync(stream, Parent);
        TDefIndexSection childBefore = await this.ReadIndexSectionAsync(stream, Child);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, Parent, ParentKey, Child, "ParentId") { CascadeUpdates = true }, this.ct);
        }

        TDefIndexSection parent = await this.ReadIndexSectionAsync(stream, Parent);
        TDefIndexSection child = await this.ReadIndexSectionAsync(stream, Child);
        var layout = IndexLayout.For(DatabaseFormat.Jet3Mdb);

        // Names in case-insensitive order, each a 1-byte length and ANSI.
        Assert.Equal([".rB", ".rC", "id", "PrimaryKey"], parent.Names);
        Assert.Equal(["AIdx", "PrimaryKey", Relationship, "ZIdx"], child.Names);
        Assert.Equal((byte)Relationship.Length, child.Td[child.NameStarts[2]]);
        Assert.Equal("Rel"u8.ToArray(), child.Td.AsSpan(child.NameStarts[2] + 1, 3).ToArray());

        // The child entry: 20 bytes, no cookie, a new real index.
        Assert.Equal(childBefore.NumRealIdx + 1, child.NumRealIdx);
        Assert.Equal(parentBefore.NumRealIdx, parent.NumRealIdx);
        Assert.True(layout.TryReadLogicalEntry(child.Td, child.LogIdxStart, 2, out LogicalIdxEntry childEntry));
        Assert.True(layout.TryReadLogicalEntry(parent.Td, parent.LogIdxStart, 0, out LogicalIdxEntry parentEntry));
        Assert.Equal(child.LogIdxStart + (2 * 20), childEntry.FieldsOffset);
        Assert.Equal(3, childEntry.IndexNum);
        Assert.Equal(childBefore.NumRealIdx, childEntry.IndexNum2);
        Assert.Equal(Constants.TableDefinition.ChildRelationshipTableType, child.Td[childEntry.FieldsOffset + 8]);
        Assert.Equal(parentEntry.IndexNum, childEntry.RelIdxNum);
        Assert.Equal(parent.Page, childEntry.RelTblPage);
        Assert.Equal(1, childEntry.CascadeUps);
        Assert.Equal(0, childEntry.CascadeDels);
        Assert.Equal(IndexKind.ForeignKey, childEntry.IndexType);

        // The parent entry: index 3 on the primary key's real index 1.
        Assert.Equal(3, parentEntry.IndexNum);
        Assert.Equal(1, parentEntry.IndexNum2);
        Assert.Equal(Constants.TableDefinition.ParentRelationshipTableType, parent.Td[parentEntry.FieldsOffset + 8]);
        Assert.Equal(childEntry.IndexNum, parentEntry.RelIdxNum);
        Assert.Equal(child.Page, parentEntry.RelTblPage);
        Assert.Equal(1, parentEntry.CascadeUps);
        Assert.Equal(0, parentEntry.CascadeDels);

        // The existing entries keep their bytes, in their old order.
        Assert.Equal(parentBefore.EntryBytes, parent.EntryBytes.Skip(1));
        Assert.Equal(childBefore.EntryBytes, [.. child.EntryBytes.Take(2), .. child.EntryBytes.Skip(3)]);

        // The new real index: 39 bytes, col_map first, used_pages 0, flags 0x00,
        // and first_dp naming a leaf of the child; its statistics slot is zero.
        int phys = layout.RealIdxPhysOffset(child.RealIdxDescStart, childEntry.IndexNum2);
        Assert.True(layout.TryReadRealIdxSlotWithKeyColumns(child.Td, child.RealIdxDescStart, childEntry.IndexNum2, out RealIdxSlot slot, out List<KeyColumn> keyColumns));
        Assert.Equal(phys + 34, slot.FirstDpOffset);
        Assert.Equal(phys + 38, layout.FlagsAbsoluteOffset(phys));
        Assert.Equal([new KeyColumn(1, true)], keyColumns);
        Assert.Equal(0, Ri32(child.Td, phys + 30));
        Assert.Equal(0x00, slot.Flags);
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct))
        {
            byte[] leaf = await harness.ReadPageCopyAsync((uint)Ri32(child.Td, slot.FirstDpOffset), this.ct);
            Assert.Equal(Constants.IndexLeafPage.PageTypeLeaf, leaf[0]);
            Assert.Equal(child.Page, Ri32(leaf, 4));
        }

        int statistics = 43 + (childEntry.IndexNum2 * 8);
        Assert.Equal(new byte[8], child.Td.AsSpan(statistics, 8).ToArray());

        // tdef_len covers everything up to the end of the names (and, on the
        // Access-authored parent, its trailing block); 'VC' stays in place.
        Assert.Equal(child.NamesEnd - 8, Ri32(child.Td, 8));
        Assert.Equal(parentBefore.TrailingLength, parent.TrailingLength);
        Assert.Equal(parent.NamesEnd + parent.TrailingLength - 8, Ri32(parent.Td, 8));
        Assert.Equal("VC"u8.ToArray(), parent.Td.AsSpan(2, 2).ToArray());
    }

    [Fact]
    public async Task CreateRelationship_Jet3_ForeignKeyIndexesHoldEveryRowAndAreEnforced()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        List<int> parentIds = await this.ReadParentIdsAsync(stream);
        var childIds = new List<int>();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateChildAsync(writer, this.ct);
            for (int id = 1; id <= 5; id++)
            {
                await writer.InsertRowAsync(Child, [id, parentIds[id % parentIds.Count]], this.ct);
                childIds.Add(id);
            }

            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, Parent, ParentKey, Child, "ParentId") { CascadeDeletes = true }, this.ct);

            // A table with an FK entry takes the full rebuild on every insert.
            var services = (WriterServices)FacadeInternals.ReadPrivateField(writer, "services")!;
            for (int id = 6; id <= 40; id++)
            {
                await writer.InsertRowAsync(Child, [id, parentIds[id % parentIds.Count]], this.ct);
                Assert.StartsWith("C1c", services.Indexes.LastIncrementalBail, StringComparison.Ordinal);
                childIds.Add(id);
            }

            await this.AssertIndexesCoverRowsAsync(writer, Child, Parent);

            // The relationship is still enforced through MSysRelationships, and
            // a cascade delete keeps the child's indexes in step.
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync(Child, [100, parentIds.Max() + 1000], this.ct));
            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, ParentKey, parentIds[0], this.ct));
            await this.AssertIndexesCoverRowsAsync(writer, Child, Parent);
        }

        int survivors = childIds.Count(id => parentIds[id % parentIds.Count] != parentIds[0]);
        Assert.True(survivors < childIds.Count);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, ReaderOptions, leaveOpen: true, this.ct);
        Assert.Equal(survivors, (await reader.ReadDataTableAsync(Child, cancellationToken: this.ct)).Rows.Count);
    }

    [Fact]
    public async Task DropRelationship_Jet3_RemovesBothEntriesAndReclaimsTheChildRealIndex()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateChildAsync(writer, this.ct);
        }

        TDefIndexSection parentBefore = await this.ReadIndexSectionAsync(stream, Parent);
        TDefIndexSection childBefore = await this.ReadIndexSectionAsync(stream, Child);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, Parent, ParentKey, Child, "ParentId"), this.ct);
            await writer.DropRelationshipAsync(Relationship, this.ct);
        }

        TDefIndexSection parent = await this.ReadIndexSectionAsync(stream, Parent);
        TDefIndexSection child = await this.ReadIndexSectionAsync(stream, Child);
        Assert.Equal(parentBefore.Names, parent.Names);
        Assert.Equal(childBefore.Names, child.Names);
        Assert.Equal(parentBefore.NumRealIdx, parent.NumRealIdx);
        Assert.Equal(childBefore.NumRealIdx, child.NumRealIdx);
        Assert.Equal(parentBefore.EntryBytes, parent.EntryBytes);
        Assert.Equal(childBefore.EntryBytes, child.EntryBytes);
        Assert.Equal(parent.NamesEnd + parent.TrailingLength - 8, Ri32(parent.Td, 8));
        Assert.Equal(child.NamesEnd - 8, Ri32(child.Td, 8));
        Assert.DoesNotContain(await this.ListIndexesAsync(stream, Child), i => i.Kind == IndexKind.ForeignKey);
    }

    [Fact]
    public async Task DropRelationship_Jet3_AccessAuthored_RemovesItsEntries()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        int relationshipsBefore;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, ReaderOptions, leaveOpen: true, this.ct))
        {
            relationshipsBefore = (await reader.ListRelationshipsAsync(this.ct)).Count;
        }

        Assert.Contains(await this.ListIndexesAsync(stream, "Table1"), i => i.Name == "Table2Table1");
        Assert.Contains(await this.ListIndexesAsync(stream, Parent), i => i.Name == ".rC");

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.DropRelationshipAsync("Table2Table1", this.ct);
        }

        IReadOnlyList<IndexMetadata> child = await this.ListIndexesAsync(stream, "Table1");
        Assert.DoesNotContain(child, i => i.Name == "Table2Table1");
        Assert.Contains(child, i => i.Name == "Table3Table1" && i.Kind == IndexKind.ForeignKey);
        Assert.DoesNotContain(await this.ListIndexesAsync(stream, Parent), i => i.Kind == IndexKind.ForeignKey);
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, ReaderOptions, leaveOpen: true, this.ct))
        {
            Assert.Equal(relationshipsBefore - 1, (await reader.ListRelationshipsAsync(this.ct)).Count);
            Assert.Equal(4, (await reader.ReadDataTableAsync("Table1", cancellationToken: this.ct)).Rows.Count);
        }
    }

    [Fact]
    public async Task RenameRelationship_Jet3_RenamesBothEntriesAndKeepsNameOrder()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateChildAsync(writer, this.ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("Abc", Parent, ParentKey, Child, "ParentId"), this.ct);
        }

        Assert.Equal(["Abc", "PrimaryKey"], (await this.ReadIndexSectionAsync(stream, Child)).Names);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.RenameRelationshipAsync("Abc", "Zedé", this.ct);
        }

        TDefIndexSection parent = await this.ReadIndexSectionAsync(stream, Parent);
        TDefIndexSection child = await this.ReadIndexSectionAsync(stream, Child);
        Assert.Equal(["PrimaryKey", "Zedé"], child.Names);
        Assert.Equal([".rC", "id", "PrimaryKey", "Zedé"], parent.Names);
        Assert.Equal(4, child.Td[child.NameStarts[1]]);
        Assert.Equal(0xE9, child.Td[child.NameStarts[1] + 4]);
        Assert.Equal(child.NamesEnd - 8, Ri32(child.Td, 8));
        Assert.Equal(parent.NamesEnd + parent.TrailingLength - 8, Ri32(parent.Td, 8));

        IndexMetadata parentFk = Assert.Single(await this.ListIndexesAsync(stream, Parent), i => i.Name == "Zedé");
        IndexMetadata childFk = Assert.Single(await this.ListIndexesAsync(stream, Child), i => i.Kind == IndexKind.ForeignKey);
        Assert.Equal("Zedé", childFk.Name);
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);
        Assert.Equal(parent.Page, childFk.RelatedTablePage);
        Assert.Equal(child.Page, parentFk.RelatedTablePage);
    }

    [Fact]
    public async Task CreateRelationship_Jet3_SelfReferencing_LinksBothEntriesOnOneTable()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync("E", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Boss", typeof(int))], this.ct);
            await writer.InsertRowsAsync("E", [[1, null], [2, 1], [3, 1]], this.ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("EB", "E", "Id", "E", "Boss") { CascadeDeletes = true }, this.ct);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync("E", [4, 9], this.ct));
            await writer.InsertRowAsync("E", [4, 2], this.ct);
        }

        TDefIndexSection table = await this.ReadIndexSectionAsync(stream, "E");
        Assert.Equal([".rB", "EB_FK", "PrimaryKey"], table.Names);
        IReadOnlyList<IndexMetadata> indexes = await this.ListIndexesAsync(stream, "E");
        IndexMetadata parentFk = Assert.Single(indexes, i => i.Name == ".rB");
        IndexMetadata childFk = Assert.Single(indexes, i => i.Name == "EB_FK");
        Assert.Equal(table.Page, parentFk.RelatedTablePage);
        Assert.Equal(table.Page, childFk.RelatedTablePage);
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);
        Assert.Equal(Assert.Single(indexes, i => i.Kind == IndexKind.PrimaryKey).RealIndexNumber, parentFk.RealIndexNumber);
        Assert.Equal("Boss", Assert.Single(childFk.Columns).Name);
        Assert.True(parentFk.CascadeDeletes && childFk.CascadeDeletes);

        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct);
        await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, table.Page, childFk.FirstDp, this.ct);
    }

    [Fact]
    public async Task CreateRelationship_Jet3_MultiColumn_SharesTheParentKeyAndIndexesBothChildColumns()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "P2",
                [new ColumnDefinition("K1", typeof(int)), new ColumnDefinition("K2", typeof(int))],
                [new IndexDefinition("PrimaryKey", ["K1", "K2"]) { IsPrimaryKey = true }],
                this.ct);
            await writer.CreateTableAsync("C2", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("R1", typeof(int)), new ColumnDefinition("R2", typeof(int))], this.ct);
            await writer.InsertRowsAsync("P2", [[1, 1], [1, 2]], this.ct);
            await writer.InsertRowsAsync("C2", [[1, 1, 2], [2, 1, 1]], this.ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("PK2", "P2", ["K1", "K2"], "C2", ["R1", "R2"]), this.ct);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync("C2", [3, 2, 1], this.ct));
        }

        IndexMetadata parentFk = Assert.Single(await this.ListIndexesAsync(stream, "P2"), i => i.Kind == IndexKind.ForeignKey);
        IndexMetadata childFk = Assert.Single(await this.ListIndexesAsync(stream, "C2"), i => i.Kind == IndexKind.ForeignKey);
        Assert.Equal(["K1", "K2"], parentFk.Columns.Select(c => c.Name));
        Assert.Equal(["R1", "R2"], childFk.Columns.Select(c => c.Name));
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);

        long childPage = await this.GetTDefPageAsync(stream, "C2");
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct);
        await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, childPage, childFk.FirstDp, this.ct);
    }

    [Fact]
    public async Task CreateRelationship_Jet3_ChildTDefSpillsToContinuationPage()
    {
        // A child whose TDEF nearly fills its 2 KB page, so the FK entry (20
        // bytes), its real index (39 + 8) and its name push the chain onto a
        // continuation page. The extra columns are Text, so a row of nulls
        // stays short.
        await using MemoryStream stream = await this.CopyFixtureAsync();
        int parentId = (await this.ReadParentIdsAsync(stream))[0];
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int)) };
        for (int i = 0; i < 65; i++)
        {
            columns.Add(new ColumnDefinition(FormattableString.Invariant($"Column{i:D3}"), typeof(string), maxLength: 10));
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(Child, columns, this.ct);
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 1).Set("ParentId", parentId), this.ct);
        }

        int usedLength = Ri32((await this.ReadIndexSectionAsync(stream, Child)).Td, 8) + 8;
        Assert.InRange(usedLength, Constants.PageSizes.Jet3 - 70, Constants.PageSizes.Jet3);
        Assert.Equal(1, await this.CountTDefPagesAsync(stream, Child));
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, Parent, ParentKey, Child, "ParentId"), this.ct);
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 2).Set("ParentId", parentId), this.ct);
        }

        Assert.Equal(2, await this.CountTDefPagesAsync(stream, Child));
        IndexMetadata childFk = Assert.Single(await this.ListIndexesAsync(stream, Child), i => i.Kind == IndexKind.ForeignKey);
        IndexMetadata parentFk = Assert.Single(await this.ListIndexesAsync(stream, Parent), i => i.Name == ".rB");
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);

        long childPage = await this.GetTDefPageAsync(stream, Child);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct);
        await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, childPage, childFk.FirstDp, this.ct);
    }

    [Fact]
    public async Task CreateRelationship_Jet3_RolledBack_LeavesBothTDefsUnchanged()
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateChildAsync(writer, this.ct);
        }

        TDefIndexSection parentBefore = await this.ReadIndexSectionAsync(stream, Parent);
        TDefIndexSection childBefore = await this.ReadIndexSectionAsync(stream, Child);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(this.ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, Parent, ParentKey, Child, "ParentId"), this.ct);
            using (var services = new ReaderServices(FacadeInternals.Database(writer), new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }))
            {
                Assert.Single(await services.Indexes.ListIndexesAsync(Child, this.ct), i => i.Kind == IndexKind.ForeignKey);
            }

            await tx.RollbackAsync(this.ct);
        }

        Assert.Equal(parentBefore.Td, (await this.ReadIndexSectionAsync(stream, Parent)).Td);
        Assert.Equal(childBefore.Td, (await this.ReadIndexSectionAsync(stream, Child)).Td);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, ReaderOptions, leaveOpen: true, this.ct);
        Assert.DoesNotContain(await reader.ListRelationshipsAsync(this.ct), r => r.Name == Relationship);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task CreateRelationship_Jet3_InEveryWriteMode_LinksTheEntries(bool transactionalWrites, bool explicitTransaction)
    {
        await using MemoryStream stream = await this.CopyFixtureAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites))
        {
            await using JetTransaction? tx = explicitTransaction ? await writer.BeginTransactionAsync(this.ct) : null;
            await CreateChildAsync(writer, this.ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, Parent, ParentKey, Child, "ParentId"), this.ct);
            if (tx is not null)
            {
                await tx.CommitAsync(this.ct);
            }
        }

        IndexMetadata parentFk = Assert.Single(await this.ListIndexesAsync(stream, Parent), i => i.Name == ".rB");
        IndexMetadata childFk = Assert.Single(await this.ListIndexesAsync(stream, Child), i => i.Kind == IndexKind.ForeignKey);
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);
    }

    /// <summary>
    /// The child's new FK real index gets an empty leaf before either TDEF is
    /// written. When the parent's TDEF write fails, nothing links that leaf,
    /// so it goes back to the global usage map instead of staying allocated.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateRelationship_ParentTDefWriteFails_ReleasesTheChildFkLeaf(DatabaseFormat format)
    {
        string parent = format == DatabaseFormat.Jet3Mdb ? Parent : "P";
        string parentKey = format == DatabaseFormat.Jet3Mdb ? ParentKey : "Id";
        byte[] bytes;
        await using (MemoryStream seed = format == DatabaseFormat.Jet3Mdb ? await this.CopyFixtureAsync() : await CreateAccdbAsync(this.ct))
        {
            await using (AccessWriter writer = await OpenWriterAsync(seed))
            {
                if (format != DatabaseFormat.Jet3Mdb)
                {
                    await writer.CreateTableAsync(parent, [new ColumnDefinition(parentKey, typeof(int)) { IsPrimaryKey = true }], this.ct);
                }

                await CreateChildAsync(writer, this.ct);
            }

            bytes = seed.ToArray();
        }

        await using var stream = new PageWriteFaultStream();
        stream.Write(bytes);
        long parentPage = await this.GetTDefPageAsync(stream, parent);
        long childPage = await this.GetTDefPageAsync(stream, Child);
        int pageSize = format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            stream.FailOnWriteAt(parentPage * pageSize);
            await Assert.ThrowsAsync<IOException>(async () =>
                await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, parent, parentKey, Child, "ParentId"), this.ct));
            Assert.True(stream.Faulted);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: this.ct);
        Assert.DoesNotContain(await this.ListIndexesAsync(stream, Child), i => i.Kind == IndexKind.ForeignKey);
        List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, childPage, this.ct);
        var allocatedLeaves = new List<long>();
        for (long page = 3; page < harness.Database.PageCount; page++)
        {
            byte[] bytesOfPage = await harness.Database.ReadPageCopyAsync(page, this.ct);
            if (bytesOfPage[0] == Constants.IndexLeafPage.PageTypeLeaf
                && Ri32(bytesOfPage, 4) == childPage
                && !await harness.Services.PageAllocator.IsPageFreeAsync(page, this.ct))
            {
                allocatedLeaves.Add(page);
            }
        }

        Assert.Equal(roots.Order(), allocatedLeaves);
    }

    [Fact]
    public async Task CreateRelationship_Jet3WriterCreatedFile_StillThrowsNotSupported()
    {
        // A writer-created Jet3 file has no MSysRelationships table.
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.Jet3Mdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            this.ct);
        await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], this.ct);
        await CreateChildAsync(writer, this.ct);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, "P", "Id", Child, "ParentId"), this.ct));
    }

    private static async Task<MemoryStream> CreateAccdbAsync(CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken))
        {
        }

        return stream;
    }

    private static ValueTask CreateChildAsync(AccessWriter writer, CancellationToken cancellationToken)
        => writer.CreateTableAsync(
            Child,
            [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))],
            cancellationToken);

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, bool transactionalWrites = false)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = transactionalWrites },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private async Task<MemoryStream> CopyFixtureAsync()
        => await cache.CopyToStreamAsync(TestDatabases.IndexTestV1997, this.ct);

    private async Task<long> GetTDefPageAsync(MemoryStream stream, string table)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct);
        CatalogEntry entry = await harness.GetCatalogEntryAsync(table, this.ct) ?? throw new InvalidOperationException($"Table '{table}' not found.");
        return entry.TDefPage;
    }

    private async Task<IReadOnlyList<IndexMetadata>> ListIndexesAsync(MemoryStream stream, string table)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(stream, ReaderOptions, leaveOpen: true, this.ct);
        return await reader.ListIndexesAsync(table, this.ct);
    }

    private async Task<List<int>> ReadParentIdsAsync(MemoryStream stream)
    {
        var ids = new List<int>();
        await using AccessReader reader = await AccessReader.OpenAsync(stream, ReaderOptions, leaveOpen: true, this.ct);
        await foreach (object[] row in reader.Rows(Parent, cancellationToken: this.ct))
        {
            ids.Add(Convert.ToInt32(row[0], System.Globalization.CultureInfo.InvariantCulture));
        }

        Assert.NotEmpty(ids);
        return ids;
    }

    private async Task<int> CountTDefPagesAsync(MemoryStream stream, string table)
    {
        long page = await this.GetTDefPageAsync(stream, table);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct);
        int count = 0;
        while (page != 0)
        {
            byte[] bytes = await harness.ReadPageCopyAsync(page, this.ct);
            Assert.Equal(Constants.PageTypes.TableDefinition, bytes[0]);
            count++;
            page = (uint)Ri32(bytes, 4);
        }

        return count;
    }

    /// <summary>
    /// Checks every index of each of <paramref name="tables"/>, read through
    /// the writer's own pages, against the live rows of its table.
    /// </summary>
    /// <param name="writer">The open writer.</param>
    /// <param name="tables">The tables to check.</param>
    private async Task AssertIndexesCoverRowsAsync(AccessWriter writer, params string[] tables)
    {
        DatabaseFile db = FacadeInternals.Database(writer);
        using var services = new ReaderServices(db, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 });
        foreach (string table in tables)
        {
            CatalogEntry entry = (await services.TableCatalog.GetCatalogEntryAsync(table, this.ct))!;
            foreach (IndexMetadata index in await services.Indexes.ListIndexesAsync(table, this.ct))
            {
                await IndexLeafChain.AssertCoversLiveRowsAsync(db, entry.TDefPage, index.FirstDp, this.ct);
            }
        }
    }

    private async Task<TDefIndexSection> ReadIndexSectionAsync(MemoryStream stream, string table)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct);
        DatabaseFile db = harness.Database;
        Assert.Equal(DatabaseFormat.Jet3Mdb, db.Format);
        CatalogEntry entry = await harness.GetCatalogEntryAsync(table, this.ct) ?? throw new InvalidOperationException($"Table '{table}' not found.");
        byte[] td = (await db.ReadTDefBytesAsync(entry.TDefPage, this.ct))!;
        int numCols = Ru16(td, db.TDef.NumCols);
        int numIdx = Ri32(td, db.TDef.NumIdx);
        int numRealIdx = Ri32(td, db.TDef.NumRealIdx);
        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(db, td, numCols, numRealIdx);
        IndexSectionAnchors anchors = db.IndexLayoutInfo.GetIndexSection(realIdxDescStart, numRealIdx, numIdx);
        var names = new List<string>(numIdx);
        var nameStarts = new List<int>(numIdx);
        int pos = anchors.LogIdxNamesStart;
        for (int i = 0; i < numIdx; i++)
        {
            nameStarts.Add(pos);
            Assert.True(db.ReadColumnName(td, ref pos, out string name) > 0);
            names.Add(name);
        }

        var entries = new List<byte[]>(numIdx);
        for (int i = 0; i < numIdx; i++)
        {
            entries.Add(td.AsSpan(db.IndexLayoutInfo.LogicalIdxEntryOffset(anchors.LogIdxStart, i), db.IndexLayoutInfo.LogicalEntrySize).ToArray());
        }

        int end = Ri32(td, 8) + 8;
        return new TDefIndexSection(
            entry.TDefPage,
            td.AsSpan(0, Math.Max(end, pos)).ToArray(),
            numRealIdx,
            realIdxDescStart,
            anchors.LogIdxStart,
            names,
            nameStarts,
            pos,
            end - pos,
            entries);
    }

    /// <summary>The index section of one Jet3 TDEF, read back for byte-level checks.</summary>
    /// <param name="Page">The TDEF page.</param>
    /// <param name="Td">The TDEF chain bytes up to <c>tdef_len + 8</c>.</param>
    /// <param name="NumRealIdx">The number of real indexes.</param>
    /// <param name="RealIdxDescStart">The offset of the first real-idx descriptor.</param>
    /// <param name="LogIdxStart">The offset of the first logical entry.</param>
    /// <param name="Names">The logical-index names, in entry order.</param>
    /// <param name="NameStarts">The offset of each name record.</param>
    /// <param name="NamesEnd">The offset just past the last name record.</param>
    /// <param name="TrailingLength">The bytes <c>tdef_len</c> counts past the names.</param>
    /// <param name="EntryBytes">Each logical entry's bytes, in entry order.</param>
    private sealed record TDefIndexSection(
        long Page,
        byte[] Td,
        int NumRealIdx,
        int RealIdxDescStart,
        int LogIdxStart,
        List<string> Names,
        List<int> NameStarts,
        int NamesEnd,
        int TrailingLength,
        List<byte[]> EntryBytes);

    /// <summary>
    /// A <see cref="MemoryStream"/> that throws <see cref="IOException"/> once,
    /// on the first write at a chosen offset after it is armed. The failed
    /// write changes nothing.
    /// </summary>
    private sealed class PageWriteFaultStream : MemoryStream
    {
        private long faultOffset = -1;

        /// <summary>Gets a value indicating whether the armed fault has been raised.</summary>
        public bool Faulted { get; private set; }

        /// <summary>Arms the stream to fail the next write that starts at <paramref name="offset"/>.</summary>
        /// <param name="offset">The byte offset of the page whose write fails.</param>
        public void FailOnWriteAt(long offset) => this.faultOffset = offset;

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (this.faultOffset >= 0 && this.Position == this.faultOffset)
            {
                this.faultOffset = -1;
                this.Faulted = true;
                throw new IOException("Injected page write fault.");
            }

            base.Write(buffer, offset, count);
        }
    }

    /// <summary>Compares the on-disk fields of two <see cref="IndexMetadata"/> values.</summary>
    private sealed class IndexMetadataComparer : IEqualityComparer<IndexMetadata>
    {
        public static readonly IndexMetadataComparer Instance = new();

        public bool Equals(IndexMetadata? x, IndexMetadata? y)
            => x is not null && y is not null
                && x.Name == y.Name && x.IndexNumber == y.IndexNumber && x.RealIndexNumber == y.RealIndexNumber
                && x.Kind == y.Kind && x.RelatedTablePage == y.RelatedTablePage && x.RelatedIndexNumber == y.RelatedIndexNumber
                && x.CascadeUpdates == y.CascadeUpdates && x.CascadeDeletes == y.CascadeDeletes;

        public int GetHashCode(IndexMetadata obj) => obj.IndexNumber;
    }
}
