namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// <see cref="AccessWriter.DropTableAsync"/> on a table that takes part in a
/// relationship. Microsoft Access refuses such a drop (DAO and Jet SQL error
/// 3303, "currently participates in one or more relationships") whenever a
/// <c>MSysRelationships</c> row names the table, so the writer refuses too,
/// before it writes anything, and the caller drops the relationships first.
/// A table whose foreign-key index entries have no <c>MSysRelationships</c>
/// row left in a damaged catalog can still be dropped, and the drop
/// removes its partners' entries, so none is left naming the freed TDEF page.
/// Jet4 runs over a copy of the Access-authored <c>AdventureLT2008.mdb</c>
/// (fresh Jet4 files have no <c>MSysRelationships</c>), ACCDB over a fresh
/// database and Jet3 over a copy of the Access 97 <c>indexTestV1997.mdb</c>.
/// </summary>
/// <param name="db">Caches the fixture databases.</param>
public sealed class DropTableRelationshipTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Parent = "DtParent";
    private const string Child = "DtChild";
    private const string RelationshipName = "FK_DtChild_DtParent";

    private static readonly int[] ParentIds = [1, 2];

    private static readonly int[] ParentIdsWithThird = [1, 2, 3];

    private static readonly int[] ChildIds = [10, 11, 12];

    private static readonly int[] ChildIdsWithThirteenth = [10, 11, 12, 13];

    private static readonly int[] RemainingChildIds = [12, 21];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task DropTable_OfTableInRelationship_ThrowsAndLeavesFileUnchanged(DatabaseFormat format, bool dropParent)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await AssertDropRefusedAsync(writer, dropParent ? Parent : Child, RelationshipName);
        }

        Assert.Equal(before, stream.ToArray());
        await AssertRelationshipLinkedAsync(stream);
        await AssertEnforcementAndCascadeAsync(stream);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropTable_OfTableInRelationship_WithTransactionalWrites_Throws(DatabaseFormat format)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = true }))
        {
            await AssertDropRefusedAsync(writer, Parent, RelationshipName);
            await AssertDropRefusedAsync(writer, Child, RelationshipName);
        }

        Assert.Equal(before, stream.ToArray());
        await AssertRelationshipLinkedAsync(stream);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    public async Task DropTable_OfTableInRelationship_InExplicitTransaction_TransactionStaysUsable(DatabaseFormat format, bool commit)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
            await writer.InsertRowAsync(Parent, RowValues.Create().Set("Id", 3).Set("Name", "three"), Ct);
            await AssertDropRefusedAsync(writer, Parent, RelationshipName);
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 13).Set("ParentId", 3), Ct);
            if (commit)
            {
                await tx.CommitAsync(Ct);
            }
            else
            {
                await tx.RollbackAsync(Ct);
            }
        }

        Assert.Equal(commit ? ParentIdsWithThird : ParentIds, await ReadIdsAsync(stream, Parent));
        Assert.Equal(commit ? ChildIdsWithThirteenth : ChildIds, await ReadIdsAsync(stream, Child));
        await AssertRelationshipLinkedAsync(stream);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropTable_AfterCreateRelationshipInSameTransaction_Throws(DatabaseFormat format)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
            await CreateParentAndChildAsync(writer);
            await AssertDropRefusedAsync(writer, Child, RelationshipName);
            await AssertDropRefusedAsync(writer, Parent, RelationshipName);
            await tx.CommitAsync(Ct);
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            Assert.Contains(await reader.ListRelationshipsAsync(Ct), r => r.Name == RelationshipName);
        }

        await AssertRelationshipLinkedAsync(stream);
        await AssertEnforcementAndCascadeAsync(stream);
    }

    /// <summary>
    /// The supported way to drop a related table: drop its relationships, then
    /// the table. No FK entry is left naming the freed TDEF page, including
    /// after a new table takes that page.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="dropParent">Whether the parent rather than the child is dropped.</param>
    /// <param name="mode"><c>none</c>, <c>transactional</c> (<c>UseTransactionalWrites</c>) or <c>explicit</c> (one committed transaction).</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, true, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, true, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, false, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, true, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, false, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, WriteMode.ExplicitCommit)]
    public async Task DropTable_AfterDropRelationship_SucceedsAndNoEntryNamesTheFreedPage(DatabaseFormat format, bool dropParent, WriteMode mode)
    {
        string dropped = dropParent ? Parent : Child;
        string partner = dropParent ? Child : Parent;
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        long droppedPage = await GetTDefPageAsync(stream, dropped);
        var options = new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit };
        await using (AccessWriter writer = await OpenWriterAsync(stream, options))
        {
            await using JetTransaction? tx = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(Ct) : null;
            await writer.DropRelationshipAsync(RelationshipName, Ct);
            await writer.DropTableAsync(dropped, Ct);
            if (tx is not null)
            {
                await tx.CommitAsync(Ct);
            }
        }

        await AssertDroppedAndUnlinkedAsync(stream, dropped, droppedPage, partner);
        await AssertPartnerWritableAsync(stream, dropParent);
        await AssertNewTableOnFreedPageIsUnlinkedAsync(stream);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropTable_OfSelfReferencingTable_RemovesRelationship(DatabaseFormat format)
    {
        const string tree = "DtTree";
        const string relationship = "FK_DtTree";
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tree,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("ParentId", typeof(int)),
                    new("Label", typeof(string), maxLength: 20),
                ],
                Ct);
            await writer.InsertRowsAsync(tree, [[1, DBNull.Value, "root"], [2, 1, "a"], [3, 1, "b"]], Ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relationship, tree, "Id", tree, "ParentId") { CascadeDeletes = true },
                Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.DropTableAsync(tree, Ct);
        }

        ForeignKeyLinks.AssertConsistent(await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream));
        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain(tree, await reader.ListTablesAsync(Ct));
        Assert.DoesNotContain(await reader.ListRelationshipsAsync(Ct), r => r.Name == relationship);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropTable_WithRelationshipThatDoesNotEnforceIntegrity_RemovesRelationship(DatabaseFormat format)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer, enforceIntegrity: false);
        }

        long droppedPage = await GetTDefPageAsync(stream, Parent);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.DropTableAsync(Parent, Ct);
        }

        await AssertDroppedAndUnlinkedAsync(stream, Parent, droppedPage, Child);
        await AssertPartnerWritableAsync(stream, parentDropped: true);
        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain(await reader.ListRelationshipsAsync(Ct), r => r.Name == RelationshipName);
    }

    /// <summary>
    /// Access-authored relationships refuse the drop too, on every format,
    /// and the refusal names the relationships and changes no byte.
    /// </summary>
    /// <param name="fixture">The fixture file name.</param>
    /// <param name="table">A table that a relationship names.</param>
    [Theory]
    [InlineData("NorthwindTraders.accdb", "Companies")]
    [InlineData("NorthwindTraders.accdb", "CompanyTypes")]
    [InlineData("NorthwindTraders.accdb", "Contacts")]
    [InlineData("indexTestV2000.mdb", "Table1")]
    [InlineData("indexTestV2000.mdb", "Table2")]
    [InlineData("indexTestV1997.mdb", "Table2")]
    [InlineData("nwind.mdb", "Orders")]
    public async Task DropTable_OfAccessAuthoredRelatedTable_Throws(string fixture, string table)
    {
        string path = fixture switch
        {
            "NorthwindTraders.accdb" => TestDatabases.NorthwindTraders,
            "indexTestV2000.mdb" => TestDatabases.IndexTestV2000,
            "indexTestV1997.mdb" => TestDatabases.IndexTestV1997,
            _ => TestDatabases.MdbtoolsNwind,
        };
        MemoryStream stream = await db.CopyToStreamAsync(path, Ct);

        string[] names;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            Assert.Contains(table, await reader.ListTablesAsync(Ct));
            names = [.. (await reader.ListRelationshipsAsync(Ct))
                .Where(r => string.Equals(r.PrimaryTable, table, StringComparison.OrdinalIgnoreCase) || string.Equals(r.ForeignTable, table, StringComparison.OrdinalIgnoreCase))
                .Select(r => r.Name)];
        }

        Assert.NotEmpty(names);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            InvalidOperationException ex = await AssertDropRefusedAsync(writer, table, names[0]);
            Assert.All(names, name => Assert.Contains($"'{name}'", ex.Message, StringComparison.Ordinal));
        }

        Assert.Equal(before, stream.ToArray());
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            Assert.Contains(table, await reader.ListTablesAsync(Ct));
        }

        ForeignKeyLinks.AssertConsistent(await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream));
    }

    [Fact]
    public async Task DropTable_OfJet3TableInWriterCreatedRelationship_ThrowsUntilRelationshipDropped()
    {
        // Jet3 catalog rows are short, so the names are too.
        const string parent = "JP";
        const string child = "JC";
        const string relationship = "JR";
        MemoryStream stream = await db.CopyToStreamAsync(TestDatabases.IndexTestV1997, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.InsertRowsAsync(parent, [[1], [2]], Ct);
            await writer.InsertRowsAsync(child, [[10, 1], [11, 2]], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(relationship, parent, "Id", child, "ParentId"), Ct);
        }

        long parentPage = await GetTDefPageAsync(stream, parent);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await AssertDropRefusedAsync(writer, parent, relationship);
            await AssertDropRefusedAsync(writer, child, relationship);
        }

        Assert.Equal(before, stream.ToArray());
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.DropRelationshipAsync(relationship, Ct);
            await writer.DropTableAsync(parent, Ct);
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            Assert.DoesNotContain(await reader.ListRelationshipsAsync(Ct), r => r.Name == relationship);
            Assert.DoesNotContain(parent, await reader.ListTablesAsync(Ct));
            Assert.DoesNotContain(await reader.ListIndexesAsync(child, Ct), i => i.Kind == IndexKind.ForeignKey);
        }

        Dictionary<long, List<IndexMetadata>> fks = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertNoEntryNamesPage(fks, parentPage);
        ForeignKeyLinks.AssertConsistent(fks);
    }

    /// <summary>
    /// A table whose FK entries have no <c>MSysRelationships</c> row left after
    /// catalog corruption is not refused. The
    /// drop removes every partner entry that names the dropped TDEF page, and
    /// on the child gives back the FK real index those entries left trailing.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="dropParent">Whether the parent rather than the child is dropped.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task DropTable_WithForeignKeyEntriesButNoRelationshipRows_RemovesPartnerEntries(DatabaseFormat format, bool dropParent)
    {
        string dropped = dropParent ? Parent : Child;
        string partner = dropParent ? Child : Parent;
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        await RemoveRelationshipRowsAsync(stream, RelationshipName);
        long droppedPage = await GetTDefPageAsync(stream, dropped);
        int partnerRealIndexes = await ReadRealIndexCountAsync(stream, partner);
        await AssertRelationshipLinkedAsync(stream);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.DropTableAsync(dropped, Ct);
        }

        await AssertDroppedAndUnlinkedAsync(stream, dropped, droppedPage, partner);

        // The child's FK real index was its last slot; the parent side shares
        // the primary key's.
        Assert.Equal(dropParent ? partnerRealIndexes - 1 : partnerRealIndexes, await ReadRealIndexCountAsync(stream, partner));
        await AssertPartnerWritableAsync(stream, dropParent);
        await AssertNewTableOnFreedPageIsUnlinkedAsync(stream);
    }

    [Fact]
    public async Task DropTable_Jet3WithForeignKeyEntriesButNoRelationshipRows_RemovesPartnerEntries()
    {
        // In indexTestV1997.mdb, Table2 is the parent of Table1 through
        // 'Table2Table1', and Table1 is also the child of Table3.
        MemoryStream stream = await db.CopyToStreamAsync(TestDatabases.IndexTestV1997, Ct);
        await RemoveRelationshipRowsAsync(stream, "Table2Table1");
        long droppedPage = await GetTDefPageAsync(stream, "Table2");
        Assert.Contains(await ListIndexesAsync(stream, "Table1"), i => i.Name == "Table2Table1");

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.DropTableAsync("Table2", Ct);
        }

        Dictionary<long, List<IndexMetadata>> fks = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertNoEntryNamesPage(fks, droppedPage);
        ForeignKeyLinks.AssertConsistent(fks);
        IReadOnlyList<IndexMetadata> table1 = await ListIndexesAsync(stream, "Table1");
        Assert.DoesNotContain(table1, i => i.Name == "Table2Table1");
        Assert.Contains(table1, i => i.Name == "Table3Table1" && i.Kind == IndexKind.ForeignKey);

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Equal(4, (await reader.ReadTableAsync("Table1", cancellationToken: Ct)).Rows.Count);
    }

    [Fact]
    public async Task DropTable_WithForeignKeyEntriesButNoRelationshipRows_RolledBack_RestoresPartnerEntries()
    {
        MemoryStream stream = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        await RemoveRelationshipRowsAsync(stream, RelationshipName);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
            await writer.DropTableAsync(Parent, Ct);
            await tx.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
        await AssertRelationshipLinkedAsync(stream);
        Assert.Equal(ParentIds, await ReadIdsAsync(stream, Parent));
    }

    private static async ValueTask<InvalidOperationException> AssertDropRefusedAsync(AccessWriter writer, string table, string relationship)
    {
        JetOperationException ex = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropTableAsync(table, Ct));
        Assert.Contains("participates in", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"'{relationship}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"'{table}'", ex.Message, StringComparison.Ordinal);
        return ex;
    }

    private static async ValueTask CreateParentAndChildAsync(AccessWriter writer, bool enforceIntegrity = true)
    {
        await writer.CreateTableAsync(
            Parent,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 20)],
            Ct);
        await writer.CreateTableAsync(
            Child,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int)), new("Note", typeof(string), maxLength: 20)],
            Ct);
        await writer.InsertRowsAsync(Parent, [[1, "one"], [2, "two"]], Ct);
        await writer.InsertRowsAsync(Child, [[10, 1, "a"], [11, 1, "b"], [12, 2, "c"]], Ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition(RelationshipName, Parent, "Id", Child, "ParentId")
            {
                CascadeDeletes = true,
                EnforceReferentialIntegrity = enforceIntegrity,
            },
            Ct);
    }

    /// <summary>
    /// Removes the <c>MSysRelationships</c> rows of <paramref name="relationshipName"/>
    /// and nothing else, leaving orphaned FK entries in both TDEFs to exercise
    /// safe cleanup of an inconsistent catalog.
    /// </summary>
    /// <param name="stream">The database.</param>
    /// <param name="relationshipName">The relationship whose rows to remove.</param>
    private static async ValueTask RemoveRelationshipRowsAsync(MemoryStream stream, string relationshipName)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, Ct);
        TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(page, Constants.SystemTableNames.Relationships, Ct);
        var store = new RelationshipCatalogStore(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Services.Indexes, harness.Services.CatalogRows, harness.Services.Snapshots, harness.Services.Catalog);
        List<RelationshipRowSnapshot> rows = await store.CollectRowsAsync(page, definition, _ => true, Ct);
        List<object[]> keep = [.. rows.Where(r => !string.Equals(r.SzRelationship, relationshipName, StringComparison.OrdinalIgnoreCase)).Select(r => r.RowValues)];
        Assert.True(keep.Count < rows.Count, $"Relationship '{relationshipName}' has no MSysRelationships rows.");
        await store.RewriteRowsAsync(page, definition, keep, Ct);
    }

    /// <summary>
    /// Asserts that the parent and child each carry exactly one FK logical
    /// index for the relationship, and that each one's <c>rel_tbl_page</c> and
    /// <c>rel_idx_num</c> name the other.
    /// </summary>
    /// <param name="stream">The database.</param>
    private static async ValueTask AssertRelationshipLinkedAsync(MemoryStream stream)
    {
        long parentPage = await GetTDefPageAsync(stream, Parent);
        long childPage = await GetTDefPageAsync(stream, Child);

        await using AccessReader reader = await OpenReaderAsync(stream);
        IndexMetadata parentFk = Assert.Single(await reader.ListIndexesAsync(Parent, Ct), i => i.Kind == IndexKind.ForeignKey);
        IndexMetadata childFk = Assert.Single(await reader.ListIndexesAsync(Child, Ct), i => i.Kind == IndexKind.ForeignKey);

        Assert.Equal("Id", Assert.Single(parentFk.Columns).Name);
        Assert.Equal("ParentId", Assert.Single(childFk.Columns).Name);
        Assert.Equal(RelationshipName, childFk.Name);
        Assert.Equal(childPage, parentFk.RelatedTablePage);
        Assert.Equal(parentPage, childFk.RelatedTablePage);
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);
    }

    /// <summary>
    /// Rejects an orphan child, cascades a parent delete to exactly its own
    /// children, and accepts a valid child.
    /// </summary>
    /// <param name="stream">The database.</param>
    private static async ValueTask AssertEnforcementAndCascadeAsync(MemoryStream stream)
    {
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 20).Set("ParentId", 99), Ct));

            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, "Id", 1, Ct));
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 21).Set("ParentId", 2), Ct);
        }

        Assert.Equal(RemainingChildIds, await ReadIdsAsync(stream, Child));
    }

    /// <summary>
    /// Asserts that <paramref name="dropped"/> is gone, that no FK entry names
    /// its freed TDEF page, that every remaining link is reciprocal, and that
    /// <paramref name="partner"/> has no FK index left.
    /// </summary>
    /// <param name="stream">The database.</param>
    /// <param name="dropped">The dropped table.</param>
    /// <param name="droppedPage">Its TDEF page before the drop.</param>
    /// <param name="partner">The table it was related to.</param>
    private static async ValueTask AssertDroppedAndUnlinkedAsync(MemoryStream stream, string dropped, long droppedPage, string partner)
    {
        Dictionary<long, List<IndexMetadata>> fks = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertNoEntryNamesPage(fks, droppedPage);
        ForeignKeyLinks.AssertConsistent(fks);

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain(dropped, await reader.ListTablesAsync(Ct));
        Assert.DoesNotContain(await reader.ListIndexesAsync(partner, Ct), i => i.Kind == IndexKind.ForeignKey);
    }

    /// <summary>Inserts into and deletes from the table that survived the drop.</summary>
    /// <param name="stream">The database.</param>
    /// <param name="parentDropped">Whether the parent was dropped, leaving the child.</param>
    private static async ValueTask AssertPartnerWritableAsync(MemoryStream stream, bool parentDropped)
    {
        await using AccessWriter writer = await OpenWriterAsync(stream);
        if (parentDropped)
        {
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 20).Set("ParentId", 99), Ct);
            Assert.Equal(1, await writer.DeleteRowsAsync(Child, "Id", 10, Ct));
        }
        else
        {
            await writer.InsertRowAsync(Parent, RowValues.Create().Set("Id", 3).Set("Name", "three"), Ct);
            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, "Id", 1, Ct));
        }
    }

    /// <summary>
    /// Creates a table, which can take the freed TDEF page, and asserts that
    /// no FK entry names it.
    /// </summary>
    /// <param name="stream">The database.</param>
    private static async ValueTask AssertNewTableOnFreedPageIsUnlinkedAsync(MemoryStream stream)
    {
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync("DtFiller", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        }

        Dictionary<long, List<IndexMetadata>> fks = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertNoEntryNamesPage(fks, await GetTDefPageAsync(stream, "DtFiller"));
        ForeignKeyLinks.AssertConsistent(fks);
    }

    private static async ValueTask<int[]> ReadIdsAsync(MemoryStream stream, string table)
    {
        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable rows = await reader.ReadTableAsync(table, cancellationToken: Ct);
        return [.. rows.AsEnumerable().Select(r => (int)r["Id"]).Order()];
    }

    private static async ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(MemoryStream stream, string table)
    {
        await using AccessReader reader = await OpenReaderAsync(stream);
        return await reader.ListIndexesAsync(table, Ct);
    }

    private static async ValueTask<int> ReadRealIndexCountAsync(MemoryStream stream, string table)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogEntry entry = await harness.GetCatalogEntryAsync(table, Ct) ?? throw new InvalidOperationException($"Table '{table}' not found.");
        byte[] td = await harness.Database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, Ct) ?? throw new InvalidOperationException($"Table '{table}' has no TDEF.");
        return Ri32(td, harness.Database.Format.TDef.NumRealIdx);
    }

    private static async ValueTask<long> GetTDefPageAsync(MemoryStream stream, string tableName)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        return entry.TDefPage;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, AccessWriterOptions? options = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, options ?? new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
    }

    private async ValueTask<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        if (format == DatabaseFormat.Jet4Mdb)
        {
            return await db.CopyToStreamAsync(TestDatabases.AdventureWorks, Ct);
        }

        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
        }

        return stream;
    }
}
