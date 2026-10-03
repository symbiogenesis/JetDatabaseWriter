namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Foreign-key relationships across the copy-and-swap rewrite behind
/// <see cref="AccessWriter.AddColumnAsync"/>, <see cref="AccessWriter.DropColumnAsync"/>
/// and <see cref="AccessWriter.RenameColumnAsync"/>. The rewritten table moves
/// to a new TDEF page, so its FK logical-index entries must be re-emitted there
/// and the partner table's entry must point at the new page and the new
/// logical-index number. Jet4 runs over a copy of the Access-authored
/// <c>AdventureLT2008.mdb</c> (fresh Jet4 files have no <c>MSysRelationships</c>);
/// ACCDB runs over a fresh database.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class RelationshipSchemaRewriteTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Parent = "RwParent";
    private const string Child = "RwChild";
    private const string RelationshipName = "FK_RwChild_RwParent";

    private static readonly int[] RemainingChildIds = [12, 21];

    private static readonly int[] ChildIdsAfterFirstCascade = [12, 13, 14];

    private static readonly int[] ChildIdsAfterSecondCascade = [12, 13];

    private static readonly int[] ChildIdsAfterParentOneDeleted = [12];

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename", true)]
    [InlineData(DatabaseFormat.Jet4Mdb, "add", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "add", true)]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop", true)]
    [InlineData(DatabaseFormat.AceAccdb, "rename", false)]
    [InlineData(DatabaseFormat.AceAccdb, "rename", true)]
    [InlineData(DatabaseFormat.AceAccdb, "add", false)]
    [InlineData(DatabaseFormat.AceAccdb, "add", true)]
    [InlineData(DatabaseFormat.AceAccdb, "drop", false)]
    [InlineData(DatabaseFormat.AceAccdb, "drop", true)]
    public async Task SchemaRewrite_KeepsForeignKeyIndexesAndPartnerLinks(DatabaseFormat format, string operation, bool rewriteParent)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            string table = rewriteParent ? Parent : Child;
            string otherColumn = rewriteParent ? "Name" : "Note";
            switch (operation)
            {
                case "rename":
                    await writer.RenameColumnAsync(table, otherColumn, otherColumn + "2", TestContext.Current.CancellationToken);
                    break;
                case "add":
                    await writer.AddColumnAsync(table, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                default:
                    await writer.DropColumnAsync(table, otherColumn, TestContext.Current.CancellationToken);
                    break;
            }
        }

        await AssertRelationshipLinkedAsync(stream, Parent, "Id", Child, "ParentId");
        await AssertEnforcementAndCascadeAsync(stream);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameColumn_OfForeignKeyColumn_UpdatesRelationshipCatalog(DatabaseFormat format)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
            await writer.RenameColumnAsync(Child, "ParentId", "ParentRef", TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync(Parent, "Id", "ParentKey", TestContext.Current.CancellationToken);
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            RelationshipMetadata relationship = Assert.Single(
                await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken),
                r => r.Name == RelationshipName);
            Assert.Equal("ParentKey", Assert.Single(relationship.PrimaryColumns));
            Assert.Equal("ParentRef", Assert.Single(relationship.ForeignColumns));
        }

        await AssertRelationshipLinkedAsync(stream, Parent, "ParentKey", Child, "ParentRef");

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(
                    Child,
                    RowValues.Create().Set("Id", 20).Set("ParentRef", 99),
                    TestContext.Current.CancellationToken));

            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, "ParentKey", 1, TestContext.Current.CancellationToken));
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            DataTable children = await reader.ReadDataTableAsync(Child, cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(12, (int)Assert.Single(children.AsEnumerable())["Id"]);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task DropColumn_OfRelationshipKeyColumn_ThrowsAndLeavesTableUnchanged(DatabaseFormat format, bool dropParentKey)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.DropColumnAsync(
                    dropParentKey ? Parent : Child,
                    dropParentKey ? "Id" : "ParentId",
                    TestContext.Current.CancellationToken));
            Assert.Contains(RelationshipName, ex.Message, StringComparison.Ordinal);
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            Assert.Contains(
                await reader.GetColumnMetadataAsync(dropParentKey ? Parent : Child, TestContext.Current.CancellationToken),
                c => c.Name == (dropParentKey ? "Id" : "ParentId"));
        }

        await AssertRelationshipLinkedAsync(stream, Parent, "Id", Child, "ParentId");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SchemaRewrite_OfSelfReferencingTable_KeepsBothForeignKeyEntries(DatabaseFormat format)
    {
        const string tree = "RwTree";
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
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(tree, [[1, DBNull.Value, "root"], [2, 1, "a"], [3, 1, "b"], [4, DBNull.Value, "other"]], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_RwTree", tree, "Id", tree, "ParentId") { CascadeDeletes = true },
                TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(tree, "Label", "Caption", TestContext.Current.CancellationToken);
            await writer.AddColumnAsync(tree, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
        }

        long treePage = await GetTDefPageAsync(stream, tree);
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            IndexMetadata[] fks = [.. (await reader.ListIndexesAsync(tree, TestContext.Current.CancellationToken)).Where(i => i.Kind == IndexKind.ForeignKey)];
            Assert.Equal(2, fks.Length);
            IndexMetadata parentSide = Assert.Single(fks, i => i.Columns.Count == 1 && i.Columns[0].Name == "Id");
            IndexMetadata childSide = Assert.Single(fks, i => i.Columns.Count == 1 && i.Columns[0].Name == "ParentId");
            Assert.Equal(treePage, parentSide.RelatedTablePage);
            Assert.Equal(treePage, childSide.RelatedTablePage);
            Assert.Equal(childSide.IndexNumber, parentSide.RelatedIndexNumber);
            Assert.Equal(parentSide.IndexNumber, childSide.RelatedIndexNumber);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(tree, RowValues.Create().Set("Id", 9).Set("ParentId", 99), TestContext.Current.CancellationToken));
            await writer.DeleteRowsAsync(tree, "Id", 1, TestContext.Current.CancellationToken);
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            DataTable rows = await reader.ReadDataTableAsync(tree, cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(4, (int)Assert.Single(rows.AsEnumerable())["Id"]);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SchemaRewrite_OfParentWithTwoChildTables_RelinksEveryChild(DatabaseFormat format)
    {
        const string otherChild = "RwChildB";
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
            await writer.CreateTableAsync(
                otherChild,
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Owner", typeof(int))],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(otherChild, [[30, 2]], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_RwChildB_RwParent", Parent, "Id", otherChild, "Owner"),
                TestContext.Current.CancellationToken);

            await writer.AddColumnAsync(Parent, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
        }

        long parentPage = await GetTDefPageAsync(stream, Parent);
        long childPage = await GetTDefPageAsync(stream, Child);
        long otherChildPage = await GetTDefPageAsync(stream, otherChild);
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            IndexMetadata[] parentFks = [.. (await reader.ListIndexesAsync(Parent, TestContext.Current.CancellationToken)).Where(i => i.Kind == IndexKind.ForeignKey)];
            IndexMetadata childFk = Assert.Single(await reader.ListIndexesAsync(Child, TestContext.Current.CancellationToken), i => i.Kind == IndexKind.ForeignKey);
            IndexMetadata otherChildFk = Assert.Single(await reader.ListIndexesAsync(otherChild, TestContext.Current.CancellationToken), i => i.Kind == IndexKind.ForeignKey);

            Assert.Equal(2, parentFks.Length);
            IndexMetadata toChild = Assert.Single(parentFks, i => i.RelatedTablePage == childPage);
            IndexMetadata toOtherChild = Assert.Single(parentFks, i => i.RelatedTablePage == otherChildPage);

            Assert.Equal(parentPage, childFk.RelatedTablePage);
            Assert.Equal(parentPage, otherChildFk.RelatedTablePage);
            Assert.Equal(toChild.IndexNumber, childFk.RelatedIndexNumber);
            Assert.Equal(toOtherChild.IndexNumber, otherChildFk.RelatedIndexNumber);
            Assert.Equal(childFk.IndexNumber, toChild.RelatedIndexNumber);
            Assert.Equal(otherChildFk.IndexNumber, toOtherChild.RelatedIndexNumber);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            // Parent 2 still has a child in the non-cascading relationship.
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.DeleteRowsAsync(Parent, "Id", 2, TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task RenameColumn_OnTableWithAttachmentColumn_KeepsForeignKeyIndexes()
    {
        MemoryStream stream = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
            await writer.AddColumnAsync(Child, new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }, TestContext.Current.CancellationToken);
        }

        long childPageBefore = await GetTDefPageAsync(stream, Child);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            // Renaming a non-complex column keeps the complex column and so
            // transplants the rebuilt table onto its original TDEF page.
            await writer.RenameColumnAsync(Child, "Note", "Note2", TestContext.Current.CancellationToken);
        }

        Assert.Equal(childPageBefore, await GetTDefPageAsync(stream, Child));
        await AssertRelationshipLinkedAsync(stream, Parent, "Id", Child, "ParentId");
        await AssertEnforcementAndCascadeAsync(stream);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RenameColumn_InExplicitTransaction_KeepsForeignKeyIndexes(bool commit)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        long childPageBefore = await GetTDefPageAsync(stream, Child);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync(Child, "Note", "Note2", TestContext.Current.CancellationToken);
            if (commit)
            {
                await tx.CommitAsync(TestContext.Current.CancellationToken);
            }
            else
            {
                await tx.RollbackAsync(TestContext.Current.CancellationToken);
            }
        }

        long childPageAfter = await GetTDefPageAsync(stream, Child);
        Assert.Equal(commit, childPageAfter != childPageBefore);
        await AssertRelationshipLinkedAsync(stream, Parent, "Id", Child, "ParentId");
        await AssertEnforcementAndCascadeAsync(stream);
    }

    /// <summary>
    /// Rewrites one side of the relationship and then inserts into both tables
    /// inside the same explicit transaction. The inserts maintain the indexes
    /// through the bulk rebuild (the incremental path never serves a table with
    /// an FK logical index), which must key every row the transaction can see,
    /// at its real location. After commit, a child of the new parent is
    /// accepted and each cascade deletes exactly that parent's children. The
    /// child inserted in the transaction references parent 2, which was committed
    /// before the transaction began.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="operation"><c>rename</c>, <c>add</c>, <c>drop</c>, <c>renameKey</c> (renames the relationship key column) or <c>none</c> (inserts only).</param>
    /// <param name="rewriteParent">Whether the parent rather than the child is rewritten.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, "none", false)]
    [InlineData(DatabaseFormat.AceAccdb, "none", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "add", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "renameKey", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename", true)]
    [InlineData(DatabaseFormat.Jet4Mdb, "renameKey", true)]
    [InlineData(DatabaseFormat.AceAccdb, "rename", false)]
    [InlineData(DatabaseFormat.AceAccdb, "add", false)]
    [InlineData(DatabaseFormat.AceAccdb, "drop", false)]
    [InlineData(DatabaseFormat.AceAccdb, "renameKey", false)]
    [InlineData(DatabaseFormat.AceAccdb, "rename", true)]
    [InlineData(DatabaseFormat.AceAccdb, "renameKey", true)]
    public async Task SchemaRewrite_ThenInsertInSameTransaction_KeepsForeignKeyIndexesCorrect(DatabaseFormat format, string operation, bool rewriteParent)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        string table = rewriteParent ? Parent : Child;
        string keyColumn = rewriteParent ? "Id" : "ParentId";
        string parentKey = "Id";
        string childKey = "ParentId";
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            switch (operation)
            {
                case "rename":
                    await writer.RenameColumnAsync(table, rewriteParent ? "Name" : "Note", "Renamed", TestContext.Current.CancellationToken);
                    break;
                case "add":
                    await writer.AddColumnAsync(table, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "drop":
                    await writer.DropColumnAsync(table, rewriteParent ? "Name" : "Note", TestContext.Current.CancellationToken);
                    break;
                case "none":
                    break;
                default:
                    await writer.RenameColumnAsync(table, keyColumn, keyColumn + "Ref", TestContext.Current.CancellationToken);
                    parentKey = rewriteParent ? "IdRef" : parentKey;
                    childKey = rewriteParent ? childKey : "ParentIdRef";
                    break;
            }

            await writer.InsertRowAsync(Parent, RowValues.Create().Set(parentKey, 3), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 13).Set(childKey, 2), TestContext.Current.CancellationToken);
            await tx.CommitAsync(TestContext.Current.CancellationToken);
        }

        await AssertRelationshipLinkedAsync(stream, Parent, parentKey, Child, childKey);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 20).Set(childKey, 99), TestContext.Current.CancellationToken));

            // The FK check seeks parent 3, inserted inside the transaction, in
            // the parent's primary-key index.
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 14).Set(childKey, 3), TestContext.Current.CancellationToken);
            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, parentKey, 1, TestContext.Current.CancellationToken));
        }

        Assert.Equal(ChildIdsAfterFirstCascade, await ReadChildIdsAsync(stream));

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, parentKey, 3, TestContext.Current.CancellationToken));
        }

        Assert.Equal(ChildIdsAfterSecondCascade, await ReadChildIdsAsync(stream));
    }

    /// <summary>
    /// Rewrites one side of the relationship and then deletes a parent inside
    /// the same explicit transaction. The cascade seeks the children through
    /// the child's FK index on the rewritten table, so it must delete exactly
    /// the parent's children, and the indexes it rebuilds afterwards must
    /// still hold the surviving rows after commit.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="rewriteParent">Whether the parent rather than the child is rewritten.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task SchemaRewrite_ThenCascadeDeleteInSameTransaction_DeletesOnlyTheParentsChildren(DatabaseFormat format, bool rewriteParent)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.AddColumnAsync(rewriteParent ? Parent : Child, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, "Id", 1, TestContext.Current.CancellationToken));
            await tx.CommitAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(ChildIdsAfterParentOneDeleted, await ReadChildIdsAsync(stream));
        await AssertRelationshipLinkedAsync(stream, Parent, "Id", Child, "ParentId");

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 20).Set("ParentId", 1), TestContext.Current.CancellationToken));
            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, "Id", 2, TestContext.Current.CancellationToken));
        }

        Assert.Empty(await ReadChildIdsAsync(stream));
    }

    [Fact]
    public async Task RenameColumn_WithTransactionalWrites_KeepsForeignKeyIndexes()
    {
        MemoryStream stream = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = true }))
        {
            await writer.RenameColumnAsync(Child, "Note", "Note2", TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync(Parent, "Name", "Name2", TestContext.Current.CancellationToken);
        }

        await AssertRelationshipLinkedAsync(stream, Parent, "Id", Child, "ParentId");
        await AssertEnforcementAndCascadeAsync(stream);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.Jet4Rc4)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbLegacyPassword)]
    public async Task SchemaRewrite_OfEncryptedDatabase_KeepsForeignKeyIndexes(DatabaseFormat format, AccessEncryptionFormat encryption)
    {
        const string password = "RwPa$$word1";
        var options = new AccessWriterOptions { UseLockFile = false };
        MemoryStream source = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(source))
        {
            await CreateParentAndChildAsync(writer);
        }

        string path = Path.Combine(Path.GetTempPath(), $"RwEncrypted_{Guid.NewGuid():N}{(format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb")}");
        try
        {
            await File.WriteAllBytesAsync(path, source.ToArray(), TestContext.Current.CancellationToken);
            await AccessWriter.EncryptAsync(path, password.AsMemory(), encryption, options, TestContext.Current.CancellationToken);

            var encryptedOptions = new AccessWriterOptions { UseLockFile = false, Password = password.AsMemory() };
            await using (AccessWriter writer = await AccessWriter.OpenAsync(path, encryptedOptions, TestContext.Current.CancellationToken))
            {
                await writer.RenameColumnAsync(Child, "Note", "Note2", TestContext.Current.CancellationToken);
                await writer.AddColumnAsync(Parent, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
            }

            await AccessWriter.DecryptAsync(path, password.AsMemory(), options, TestContext.Current.CancellationToken);
            await using var plain = new MemoryStream();
            plain.Write(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            await AssertRelationshipLinkedAsync(plain, Parent, "Id", Child, "ParentId");
            await AssertEnforcementAndCascadeAsync(plain);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SchemaRewrite_OfEmptyTables_KeepsForeignKeyIndexes(DatabaseFormat format)
    {
        MemoryStream stream = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await CreateParentAndChildAsync(writer, insertRows: false);
            await writer.RenameColumnAsync(Child, "Note", "Note2", TestContext.Current.CancellationToken);
            await writer.AddColumnAsync(Parent, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
        }

        await AssertRelationshipLinkedAsync(stream, Parent, "Id", Child, "ParentId");

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 1).Set("ParentId", 1), TestContext.Current.CancellationToken));
            await writer.InsertRowAsync(Parent, RowValues.Create().Set("Id", 1), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 1).Set("ParentId", 1), TestContext.Current.CancellationToken);
            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, "Id", 1, TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Empty((await reader.ReadDataTableAsync(Child, cancellationToken: TestContext.Current.CancellationToken)).Rows);
    }

    /// <summary>
    /// Rewrites a table of an Access-authored database that has relationships
    /// on both sides and checks that every FK logical index in the file still
    /// names a TDEF page whose partner entry points back at it, on every
    /// format: the rewritten table's entries are re-emitted and its partners'
    /// entries re-linked to it.
    /// </summary>
    /// <param name="fixture">The fixture file name.</param>
    /// <param name="table">The table to rewrite.</param>
    /// <param name="operation"><c>add</c> or <c>rename</c> (renames the table's last column).</param>
    [Theory]
    [InlineData("NorthwindTraders.accdb", "Companies", "add")]
    [InlineData("NorthwindTraders.accdb", "Companies", "rename")]
    [InlineData("NorthwindTraders.accdb", "OrderDetails", "add")]
    [InlineData("indexTestV2000.mdb", "Table1", "add")]
    [InlineData("indexTestV2000.mdb", "Table2", "rename")]
    [InlineData("indexTestV1997.mdb", "Table2", "add")]
    [InlineData("indexTestV1997.mdb", "Table2", "rename")]
    [InlineData("nwind.mdb", "Orders", "add")]
    [InlineData("nwind.mdb", "Order Details", "rename")]
    public async Task SchemaRewrite_OfAccessAuthoredTable_KeepsEveryForeignKeyLinkConsistent(string fixture, string table, string operation)
    {
        string path = fixture switch
        {
            "NorthwindTraders.accdb" => TestDatabases.NorthwindTraders,
            "indexTestV2000.mdb" => TestDatabases.IndexTestV2000,
            "indexTestV1997.mdb" => TestDatabases.IndexTestV1997,
            _ => TestDatabases.MdbtoolsNwind,
        };
        MemoryStream stream = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);

        long pageBefore = await GetTDefPageAsync(stream, table);
        Dictionary<long, List<IndexMetadata>> fksBefore = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertConsistent(fksBefore);
        int touchingTable = fksBefore.Sum(pair => pair.Value.Count(i => pair.Key == pageBefore || i.RelatedTablePage == pageBefore));
        Assert.True(touchingTable > 0, $"Fixture table '{table}' should take part in a relationship with FK index entries.");

        int relationshipsBefore;
        int rowsBefore;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            relationshipsBefore = (await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken)).Count;
            rowsBefore = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken)).Rows.Count;
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            if (operation == "add")
            {
                await writer.AddColumnAsync(table, new ColumnDefinition("RwExtra", typeof(int)), TestContext.Current.CancellationToken);
            }
            else
            {
                await using AccessReader reader = await OpenReaderAsync(stream);
                string lastColumn = (await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken))[^1].Name;
                await writer.RenameColumnAsync(table, lastColumn, lastColumn + "Rw", TestContext.Current.CancellationToken);
            }
        }

        long pageAfter = await GetTDefPageAsync(stream, table);
        Dictionary<long, List<IndexMetadata>> fksAfter = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertConsistent(fksAfter);

        int totalBefore = fksBefore.Sum(pair => pair.Value.Count);
        int totalAfter = fksAfter.Sum(pair => pair.Value.Count);
        Assert.Equal(totalBefore, totalAfter);
        Assert.Equal(touchingTable, fksAfter.Sum(pair => pair.Value.Count(i => pair.Key == pageAfter || i.RelatedTablePage == pageAfter)));
        Assert.DoesNotContain(fksAfter.Values.SelectMany(fks => fks), i => pageBefore != pageAfter && i.RelatedTablePage == pageBefore);

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            Assert.Equal(relationshipsBefore, (await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken)).Count);
            Assert.Equal(rowsBefore, (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken)).Rows.Count);
        }
    }

    private static async ValueTask CreateParentAndChildAsync(AccessWriter writer, bool insertRows = true)
    {
        await writer.CreateTableAsync(
            Parent,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 20)],
            TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(
            Child,
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int)), new("Note", typeof(string), maxLength: 20)],
            TestContext.Current.CancellationToken);

        if (insertRows)
        {
            await writer.InsertRowsAsync(Parent, [[1, "one"], [2, "two"]], TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(Child, [[10, 1, "a"], [11, 1, "b"], [12, 2, "c"]], TestContext.Current.CancellationToken);
        }

        await writer.CreateRelationshipAsync(
            new RelationshipDefinition(RelationshipName, Parent, "Id", Child, "ParentId") { CascadeDeletes = true },
            TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Asserts that the parent and child each carry exactly one FK logical
    /// index for the relationship, keyed on the expected columns, and that
    /// each one's <c>rel_tbl_page</c> and <c>rel_idx_num</c> name the other.
    /// </summary>
    /// <param name="stream">The database.</param>
    /// <param name="parent">The parent table.</param>
    /// <param name="parentColumn">The parent key column.</param>
    /// <param name="child">The child table.</param>
    /// <param name="childColumn">The child foreign-key column.</param>
    private static async ValueTask AssertRelationshipLinkedAsync(MemoryStream stream, string parent, string parentColumn, string child, string childColumn)
    {
        long parentPage = await GetTDefPageAsync(stream, parent);
        long childPage = await GetTDefPageAsync(stream, child);

        await using AccessReader reader = await OpenReaderAsync(stream);
        IndexMetadata parentFk = Assert.Single(await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken), i => i.Kind == IndexKind.ForeignKey);
        IndexMetadata childFk = Assert.Single(await reader.ListIndexesAsync(child, TestContext.Current.CancellationToken), i => i.Kind == IndexKind.ForeignKey);

        Assert.Equal(parentColumn, Assert.Single(parentFk.Columns).Name);
        Assert.Equal(childColumn, Assert.Single(childFk.Columns).Name);
        Assert.Equal(RelationshipName, childFk.Name);
        Assert.True(childFk.CascadeDeletes);

        Assert.Equal(childPage, parentFk.RelatedTablePage);
        Assert.Equal(parentPage, childFk.RelatedTablePage);
        Assert.Equal(childFk.IndexNumber, parentFk.RelatedIndexNumber);
        Assert.Equal(parentFk.IndexNumber, childFk.RelatedIndexNumber);
    }

    /// <summary>
    /// Rejects an orphan child, cascades a parent delete to exactly its own
    /// children through the child FK index, and accepts a valid child.
    /// </summary>
    /// <param name="stream">The database.</param>
    private static async ValueTask AssertEnforcementAndCascadeAsync(MemoryStream stream)
    {
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 20).Set("ParentId", 99), TestContext.Current.CancellationToken));

            Assert.Equal(1, await writer.DeleteRowsAsync(Parent, "Id", 1, TestContext.Current.CancellationToken));
            await writer.InsertRowAsync(Child, RowValues.Create().Set("Id", 21).Set("ParentId", 2), TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable children = await reader.ReadDataTableAsync(Child, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(RemainingChildIds, children.AsEnumerable().Select(r => (int)r["Id"]).Order().ToArray());
    }

    private static async ValueTask<int[]> ReadChildIdsAsync(MemoryStream stream)
    {
        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable children = await reader.ReadDataTableAsync(Child, cancellationToken: TestContext.Current.CancellationToken);
        return [.. children.AsEnumerable().Select(r => (int)r["Id"]).Order()];
    }

    private static async ValueTask<long> GetTDefPageAsync(MemoryStream stream, string tableName)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
        Assert.NotNull(entry);
        return entry.TDefPage;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, AccessWriterOptions? options = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, options ?? new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
    }

    private async ValueTask<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        if (format == DatabaseFormat.Jet4Mdb)
        {
            return await db.CopyToStreamAsync(TestDatabases.AdventureWorks, TestContext.Current.CancellationToken);
        }

        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        return stream;
    }
}
