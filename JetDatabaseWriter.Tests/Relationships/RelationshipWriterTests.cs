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
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// relationship writer — round-trip tests for <see cref="AccessWriter.CreateRelationshipAsync"/>.
/// Each test starts from a copy of <c>NorthwindTraders.accdb</c> (which already
/// contains the <c>MSysRelationships</c> system table), creates two scratch
/// tables, declares a relationship between them, and asserts that one row per
/// FK column lands in <c>MSysRelationships</c> with the appendix-confirmed
/// column layout. The per-TDEF FK logical-index entries are not exercised here.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class RelationshipWriterTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task CreateRelationshipAsync_SingleColumn_AppendsOneMSysRelationshipsRow()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("Parent");
        string child = MakeTableName("Child");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        DataTable rels = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);

        DataRow[] matching = rels.AsEnumerable()
            .Where(r => string.Equals(SafeString(r, "szRelationship"), relName, StringComparison.Ordinal))
            .ToArray();

        Assert.Single(matching);
        DataRow row = matching[0];
        Assert.Equal(1, (int)row["ccolumn"]);
        Assert.Equal(0, (int)row["icolumn"]);
        Assert.Equal(0, (int)row["grbit"]);
        Assert.Equal(parent, SafeString(row, "szReferencedObject"));
        Assert.Equal("Id", SafeString(row, "szReferencedColumn"));
        Assert.Equal(child, SafeString(row, "szObject"));
        Assert.Equal("ParentId", SafeString(row, "szColumn"));
    }

    [Fact]
    public async Task CreateRelationshipAsync_MultiColumn_AppendsOneRowPerColumn()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("MParent");
        string child = MakeTableName("MChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                parent,
                [new("KeyA", typeof(int)), new("KeyB", typeof(int))],
                TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                child,
                [new("Id", typeof(int)), new("RefA", typeof(int)), new("RefB", typeof(int))],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, ["KeyA", "KeyB"], child, ["RefA", "RefB"]),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        DataTable rels = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);

        DataRow[] matching = rels.AsEnumerable()
            .Where(r => string.Equals(SafeString(r, "szRelationship"), relName, StringComparison.Ordinal))
            .OrderBy(r => (int)r["icolumn"])
            .ToArray();

        Assert.Equal(2, matching.Length);
        Assert.All(matching, r => Assert.Equal(2, (int)r["ccolumn"]));
        Assert.Equal(0, (int)matching[0]["icolumn"]);
        Assert.Equal(1, (int)matching[1]["icolumn"]);
        Assert.Equal("KeyA", SafeString(matching[0], "szReferencedColumn"));
        Assert.Equal("RefA", SafeString(matching[0], "szColumn"));
        Assert.Equal("KeyB", SafeString(matching[1], "szReferencedColumn"));
        Assert.Equal("RefB", SafeString(matching[1], "szColumn"));
    }

    [Fact]
    public async Task CreateRelationshipAsync_CascadeAndNoEnforce_EncodesGrbitFlags()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("CParent");
        string child = MakeTableName("CChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId")
                {
                    EnforceReferentialIntegrity = false,
                    CascadeUpdates = true,
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        DataTable rels = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);

        DataRow row = rels.AsEnumerable()
            .Single(r => string.Equals(SafeString(r, "szRelationship"), relName, StringComparison.Ordinal));

        // 0x02 (NoRefIntegrity) | 0x100 (CascadeUpdates) | 0x1000 (CascadeDeletes) = 0x1102
        Assert.Equal(0x1102, (int)row["grbit"]);
    }

    [Fact]
    public async Task CreateRelationshipAsync_DuplicateName_Throws()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("DParent");
        string child = MakeTableName("DChild");
        string relName = $"FK_{child}_{parent}";

        await using AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

        var def = new RelationshipDefinition(relName, parent, "Id", child, "ParentId");
        await writer.CreateRelationshipAsync(def, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.CreateRelationshipAsync(def, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateRelationshipAsync_UnknownTable_Throws()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("UParent");
        await using AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Nope", parent, "Id", "DoesNotExist", "Id"),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateRelationshipAsync_UnknownColumn_Throws()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("EParent");
        string child = MakeTableName("EChild");

        await using AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int))], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_BadCol", parent, "Id", child, "Missing"),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateRelationshipAsync_FreshDatabaseWithScaffoldedMSysRelationships_Succeeds()
    {
        var ms = new MemoryStream();
        await using (AccessWriter w = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            // Fresh ACCDBs scaffold the relationship catalog tables.
        }

        ms.Position = 0;
        await using AccessWriter writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("P", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("C", [new("Id", typeof(int)), new("PId", typeof(int))], TestContext.Current.CancellationToken);

        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_C_P", "P", "Id", "C", "PId"),
            TestContext.Current.CancellationToken);

        ms.Position = 0;
        await using AccessReader reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        DataTable rels = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Contains(
            rels.AsEnumerable(),
            row => string.Equals(SafeString(row, "szRelationship"), "FK_C_P", StringComparison.Ordinal));
    }

    [Fact]
    public void RelationshipDefinition_MismatchedArity_Throws() => Assert.Throws<ArgumentException>(() =>
                                                                            new RelationshipDefinition("FK", "P", ["A", "B"], "C", ["X"]));

    [Fact]
    public void RelationshipDefinition_EmptyColumns_Throws() => Assert.Throws<ArgumentException>(() =>
                                                                         new RelationshipDefinition("FK", "P", [], "C", []));

    // ════════════════════════════════════════════════════════════════
    // per-TDEF FK emission — per-TDEF FK logical-idx entry round-trip tests
    // ════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateRelationshipAsync_SingleColumn_EmitsFkLogicalIdxEntriesOnBothSides()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("BParent");
        string child = MakeTableName("BChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);

        IReadOnlyList<IndexMetadata> parentIndexes = await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> childIndexes = await reader.ListIndexesAsync(child, TestContext.Current.CancellationToken);

        IndexMetadata parentFk = Assert.Single(parentIndexes, ix => ix.Kind == IndexKind.ForeignKey);
        IndexMetadata childFk = Assert.Single(childIndexes, ix => ix.Kind == IndexKind.ForeignKey);

        Assert.True(parentFk.IsForeignKey);
        Assert.True(childFk.IsForeignKey);
        Assert.NotEqual(0, parentFk.RelatedTablePage);
        Assert.NotEqual(0, childFk.RelatedTablePage);

        // Each side's rel_tbl_page should point at the other side's TDEF.
        Assert.NotEqual(parentFk.RelatedTablePage, childFk.RelatedTablePage);

        // FK columns surface in col_map.
        Assert.Single(parentFk.Columns);
        Assert.Single(childFk.Columns);
        Assert.Equal("Id", parentFk.Columns[0].Name);
        Assert.Equal("ParentId", childFk.Columns[0].Name);
    }

    [Fact]
    public async Task CreateRelationshipAsync_CascadeFlags_OnFkSideOnly()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("CFParent");
        string child = MakeTableName("CFChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId")
                {
                    CascadeUpdates = true,
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);

        IndexMetadata parentFk = Assert.Single(
            await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken),
            ix => ix.Kind == IndexKind.ForeignKey);
        IndexMetadata childFk = Assert.Single(
            await reader.ListIndexesAsync(child, TestContext.Current.CancellationToken),
            ix => ix.Kind == IndexKind.ForeignKey);

        // Cascade flags are emitted only on the FK (child) side.
        Assert.True(childFk.CascadeUpdates);
        Assert.True(childFk.CascadeDeletes);
        Assert.False(parentFk.CascadeUpdates);
        Assert.False(parentFk.CascadeDeletes);
    }

    [Fact]
    public async Task CreateRelationshipAsync_SingleColumn_EmitsDaoCompatibleFkCrossReferences()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("XRParent");
        string child = MakeTableName("XRChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                parent,
                [new("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                child,
                [new("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }, new("ParentId", typeof(int)) { IsNullable = false }],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        CatalogEntry? parentEntry = await reader.GetCatalogEntryAsync(parent, TestContext.Current.CancellationToken);
        CatalogEntry? childEntry = await reader.GetCatalogEntryAsync(child, TestContext.Current.CancellationToken);
        Assert.NotNull(parentEntry);
        Assert.NotNull(childEntry);

        byte[] fileBytes = temp.ToArray();
        RawLogicalIdxEntry parentFk = ReadSingleFkLogicalEntry(fileBytes, parentEntry.TDefPage, childEntry.TDefPage);
        RawLogicalIdxEntry childFk = ReadSingleFkLogicalEntry(fileBytes, childEntry.TDefPage, parentEntry.TDefPage);

        Assert.Equal(0x01, parentFk.RelTblType);
        Assert.Equal(0x02, childFk.RelTblType);
        Assert.Equal(childFk.IndexNum, parentFk.RelIdxNum);
        Assert.Equal(parentFk.IndexNum, childFk.RelIdxNum);
        Assert.NotEqual(0, childFk.RealIdxUsedPages);
    }

    [Fact]
    public async Task CreateRelationshipAsync_MultiColumn_EmitsFkLogicalIdxEntriesOnBothSides()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("MBParent");
        string child = MakeTableName("MBChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                parent,
                [new("KeyA", typeof(int)), new("KeyB", typeof(int))],
                TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                child,
                [new("Id", typeof(int)), new("RefA", typeof(int)), new("RefB", typeof(int))],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, ["KeyA", "KeyB"], child, ["RefA", "RefB"]),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);

        IndexMetadata parentFk = Assert.Single(
            await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken),
            ix => ix.Kind == IndexKind.ForeignKey);
        IndexMetadata childFk = Assert.Single(
            await reader.ListIndexesAsync(child, TestContext.Current.CancellationToken),
            ix => ix.Kind == IndexKind.ForeignKey);

        Assert.Equal(2, parentFk.Columns.Count);
        Assert.Equal(2, childFk.Columns.Count);
        Assert.Equal(["KeyA", "KeyB"], parentFk.Columns.Select(c => c.Name).ToArray());
        Assert.Equal(["RefA", "RefB"], childFk.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task CreateRelationshipAsync_RealIdxSharing_ReusesExistingPkIndex()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName("SParent");
        string child = MakeTableName("SChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            // Parent declares an explicit PK on Id — the per-TDEF FK emission emitter should
            // share that real-idx slot for the PK-side FK logical-idx entry
            // instead of allocating a fresh one.
            await writer.CreateTableAsync(
                parent,
                [new("Id", typeof(int)) { IsPrimaryKey = true }],
                TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> parentIndexes = await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(parentIndexes, ix => ix.Kind == IndexKind.PrimaryKey);
        IndexMetadata parentFk = Assert.Single(parentIndexes, ix => ix.Kind == IndexKind.ForeignKey);

        // Sharing per §3.3: PK and the FK logical-idx entry share the same real-idx slot.
        Assert.Equal(pk.RealIndexNumber, parentFk.RealIndexNumber);
    }

    [Fact]
    public async Task CreateRelationshipAsync_SelfReferential_DistinctIndexNames()
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string table = MakeTableName("SelfRef");
        string relName = $"FK_{table}_{table}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                table,
                [new("Id", typeof(int)), new("ParentId", typeof(int))],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, table, "Id", table, "ParentId"),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(table, TestContext.Current.CancellationToken);

        // Two FK logical-idx entries land on the same TDEF (one per side of the
        // self-referential relationship); their names must be distinct.
        IndexMetadata[] fks = indexes.Where(ix => ix.Kind == IndexKind.ForeignKey).ToArray();
        Assert.Equal(2, fks.Length);
        Assert.NotEqual(fks[0].Name, fks[1].Name);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CreateRelationshipAsync_MultiPageEndpointTDef_EmitsFkLogicalIdxEntries(bool parentIsWide)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        string parent = MakeTableName(parentIsWide ? "WParent" : "NParent");
        string child = MakeTableName(parentIsWide ? "NChild" : "WChild");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            if (parentIsWide)
            {
                await CreateWideTDefTableAsync(
                    writer,
                    parent,
                    [new("Id", typeof(int))],
                    TestContext.Current.CancellationToken);
                await writer.CreateTableAsync(
                    child,
                    [new("Id", typeof(int)), new("ParentId", typeof(int))],
                    TestContext.Current.CancellationToken);
            }
            else
            {
                await writer.CreateTableAsync(
                    parent,
                    [new("Id", typeof(int))],
                    TestContext.Current.CancellationToken);
                await CreateWideTDefTableAsync(
                    writer,
                    child,
                    [new("Id", typeof(int)), new("ParentId", typeof(int))],
                    TestContext.Current.CancellationToken);
            }

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> parentIndexes = await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> childIndexes = await reader.ListIndexesAsync(child, TestContext.Current.CancellationToken);

        IndexMetadata parentFk = Assert.Single(parentIndexes, index => index.Kind == IndexKind.ForeignKey);
        IndexMetadata childFk = Assert.Single(childIndexes, index => index.Kind == IndexKind.ForeignKey);

        Assert.Equal("Id", Assert.Single(parentFk.Columns).Name);
        Assert.Equal("ParentId", Assert.Single(childFk.Columns).Name);
        Assert.NotEqual(0, parentFk.RelatedTablePage);
        Assert.NotEqual(0, childFk.RelatedTablePage);
    }

    private static string MakeTableName(string prefix) =>
        $"{prefix}_{Guid.NewGuid():N}"[..Math.Min(18, prefix.Length + 11)];

    private static async ValueTask CreateWideTDefTableAsync(
        AccessWriter writer,
        string tableName,
        IReadOnlyList<ColumnDefinition> leadingColumns,
        CancellationToken cancellationToken)
    {
        const int columnCount = 200;
        const int indexCount = 30;

        var columns = new List<ColumnDefinition>(columnCount);
        columns.AddRange(leadingColumns);
        for (int columnOrdinal = columns.Count; columnOrdinal < columnCount; columnOrdinal++)
        {
            columns.Add(new ColumnDefinition($"C{columnOrdinal:D3}", typeof(int)));
        }

        var indexes = new List<IndexDefinition>(indexCount);
        for (int indexOrdinal = 0; indexOrdinal < indexCount; indexOrdinal++)
        {
            int indexedColumnOrdinal = leadingColumns.Count + indexOrdinal;
            indexes.Add(new IndexDefinition($"IX_{indexOrdinal:D2}", $"C{indexedColumnOrdinal:D3}"));
        }

        await writer.CreateTableAsync(tableName, columns, indexes, cancellationToken);
    }

    private static string SafeString(DataRow row, string column)
    {
        if (!row.Table.Columns.Contains(column))
        {
            return string.Empty;
        }

        object v = row[column];
        return v == DBNull.Value ? string.Empty : v.ToString() ?? string.Empty;
    }

    private static RawLogicalIdxEntry ReadSingleFkLogicalEntry(byte[] fileBytes, long tdefPage, long relatedTdefPage)
    {
        int pageStart = checked((int)tdefPage * Constants.PageSizes.Jet4);
        int numCols = BitConverter.ToUInt16(fileBytes, pageStart + 0x2D);
        int numIdx = BitConverter.ToInt32(fileBytes, pageStart + 0x2F);
        int numRealIdx = BitConverter.ToInt32(fileBytes, pageStart + 0x33);
        int colStart = pageStart + 63 + (numRealIdx * 12);
        int namePos = colStart + (numCols * 25);
        for (int i = 0; i < numCols; i++)
        {
            int nameLen = BitConverter.ToUInt16(fileBytes, namePos);
            namePos += 2 + nameLen;
        }

        int logIdxStart = namePos + (numRealIdx * Constants.TableDefinition.Jet4.RealIdx.PhysSize);
        var entries = new List<RawLogicalIdxEntry>();
        for (int i = 0; i < numIdx; i++)
        {
            int entry = logIdxStart + (i * Constants.TableDefinition.Jet4.LogicalIdx.EntrySize);
            if (fileBytes[entry + 23] != 0x02)
            {
                continue;
            }

            if (BitConverter.ToInt32(fileBytes, entry + 17) != relatedTdefPage)
            {
                continue;
            }

            int realIdxNum = BitConverter.ToInt32(fileBytes, entry + 8);
            int realIdxStart = logIdxStart - (numRealIdx * Constants.TableDefinition.Jet4.RealIdx.PhysSize) + (realIdxNum * Constants.TableDefinition.Jet4.RealIdx.PhysSize);

            entries.Add(new RawLogicalIdxEntry(
                BitConverter.ToInt32(fileBytes, entry + 4),
                realIdxNum,
                fileBytes[entry + 12],
                BitConverter.ToInt32(fileBytes, entry + 13),
                BitConverter.ToInt32(fileBytes, realIdxStart + 34)));
        }

        return Assert.Single(entries);
    }

    private readonly record struct RawLogicalIdxEntry(
        int IndexNum,
        int RealIdxNum,
        byte RelTblType,
        int RelIdxNum,
        int RealIdxUsedPages);

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }
}
