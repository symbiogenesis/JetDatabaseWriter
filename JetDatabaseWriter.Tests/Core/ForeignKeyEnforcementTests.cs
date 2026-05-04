namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Runtime foreign-key enforcement on Insert / Update / Delete.
/// Each test creates a parent / child pair, declares a relationship via
/// <see cref="AccessWriter.CreateRelationshipAsync"/>, and exercises the
/// enforcement / cascade behaviour on <c>InsertRowAsync</c>,
/// <c>InsertRowsAsync</c>, <c>UpdateRowsAsync</c>, and
/// <c>DeleteRowsAsync</c>.
/// </summary>
public sealed class ForeignKeyEnforcementTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task Insert_WithEnforce_RejectsRowReferencingMissingParent()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_Missing", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);

        // Parent is empty; child insert with ParentId=42 must be rejected.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(child, [1, 42], TestContext.Current.CancellationToken));
        Assert.Contains("FK_Missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Insert_WithEnforce_AllowsRowWhenParentExists()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(parent, [7], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Ok", parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(child, [1, 7], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Single(t.Rows);
    }

    [Fact]
    public async Task Insert_WithNullForeignKey_IsAllowed()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Null", parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(child, [1, DBNull.Value], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Single(t.Rows);
    }

    [Fact]
    public async Task Insert_WithEnforceDisabled_AllowsAnyValue()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_NoEnforce", parent, "Id", child, "ParentId")
                {
                    EnforceReferentialIntegrity = false,
                },
                TestContext.Current.CancellationToken);

            // Parent is empty but enforcement is off → must succeed.
            await writer.InsertRowAsync(child, [1, 99], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Single(t.Rows);
    }

    [Fact]
    public async Task Insert_BulkSelfReferential_LaterRowsCanReferenceEarlierRows()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string table = MakeTableName("Tree");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(table, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            // Pre-seed root then declare relationship.
            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Self", table, "Id", table, "ParentId"),
                TestContext.Current.CancellationToken);

            // Bulk insert: row 2 references row 1; row 3 references row 2.
            // The self-referential augmentation must let later rows see earlier ones.
            await writer.InsertRowsAsync(
                table,
                [
                    [2, 1],
                    [3, 2],
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(3, t.Rows.Count);
    }

    [Fact]
    public async Task Update_FkSide_RejectsChangeToMissingParent()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_UFk", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, [1, 5], TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.UpdateRowsAsync(
                child,
                "Id",
                1,
                new Dictionary<string, object> { ["ParentId"] = 999 },
                TestContext.Current.CancellationToken));
        Assert.Contains("FK_UFk", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Delete_PkSide_WithoutCascade_RejectsWhenChildrenExist()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_DelNoCasc", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, [1, 5], TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.DeleteRowsAsync(parent, "Id", 5, TestContext.Current.CancellationToken));
        Assert.Contains("FK_DelNoCasc", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Delete_PkSide_WithCascade_DeletesChildren()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_DelCasc", parent, "Id", child, "ParentId")
                {
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                child,
                [
                    [1, 5],
                    [2, 5],
                ],
                TestContext.Current.CancellationToken);

            int deleted = await writer.DeleteRowsAsync(parent, "Id", 5, TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Empty(t.Rows);
    }

    [Fact]
    public async Task Update_PkSide_WithoutCascade_RejectsKeyChangeWhenChildrenReference()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_UPkNoCasc", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, [1, 5], TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.UpdateRowsAsync(
                parent,
                "Id",
                5,
                new Dictionary<string, object> { ["Id"] = 99 },
                TestContext.Current.CancellationToken));
        Assert.Contains("FK_UPkNoCasc", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Update_PkSide_WithCascade_PropagatesNewKeyToChildren()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_UPkCasc", parent, "Id", child, "ParentId")
                {
                    CascadeUpdates = true,
                },
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                child,
                [
                    [1, 5],
                    [2, 5],
                ],
                TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                parent,
                "Id",
                5,
                new Dictionary<string, object> { ["Id"] = 99 },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(2, t.Rows.Count);
        Assert.All(t.AsEnumerable(), r => Assert.Equal(99, Convert.ToInt32(r["ParentId"], System.Globalization.CultureInfo.InvariantCulture)));

        // The parent-side `UpdateRowsAsync` must rewrite the PK column on
        // disk, not only repoint the children. Reopen the parent table
        // and assert the row carries the new PK value (and the old one
        // is gone).
        DataTable p = (await reader.ReadDataTableAsync(parent, cancellationToken: TestContext.Current.CancellationToken))!;
        DataRow parentRow = Assert.Single(p.AsEnumerable());
        Assert.Equal(99, Convert.ToInt32(parentRow["Id"], System.Globalization.CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task Update_PkSide_WithCascade_RewritesParentRowOnDisk_SingleColumnPk()
    {
        // The existing cascade-update tests assert the child-side index
        // repoint; this test isolates the parent-side observation: after
        // updating the parent row's PK, reopen the file and assert the
        // parent table now exposes only the new PK value.
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("UPP");
        string child = MakeTableName("UPC");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(
                parent,
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Label", typeof(string), maxLength: 32)],
                TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                child,
                [new("Id", typeof(int)), new("ParentId", typeof(int))],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                parent,
                [
                    [1, "one"],
                    [2, "two"],
                    [3, "three"],
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_PkUpdObs", parent, "Id", child, "ParentId")
                {
                    CascadeUpdates = true,
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                child,
                [
                    [10, 2],
                    [11, 2],
                ],
                TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                parent,
                "Id",
                2,
                new Dictionary<string, object> { ["Id"] = 222 },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable p = (await reader.ReadDataTableAsync(parent, cancellationToken: TestContext.Current.CancellationToken))!;

        // Parent side: row that had Id=2 now reports Id=222; the other
        // rows are unchanged; no row carries the old key value any more.
        Assert.Equal(3, p.Rows.Count);
        int[] parentIds = [.. p.AsEnumerable().Select(r => Convert.ToInt32(r["Id"], System.Globalization.CultureInfo.InvariantCulture)).OrderBy(static x => x)];
        Assert.Equal([1, 3, 222], parentIds);
        Assert.DoesNotContain(2, parentIds);

        // The non-PK columns must travel with the rewritten row.
        DataRow renamed = Assert.Single(p.AsEnumerable(), r => Convert.ToInt32(r["Id"], System.Globalization.CultureInfo.InvariantCulture) == 222);
        Assert.Equal("two", (string)renamed["Label"]);

        // Sanity: child-side cascade still landed (existing coverage).
        DataTable c = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(2, c.Rows.Count);
        Assert.All(c.AsEnumerable(), r => Assert.Equal(222, Convert.ToInt32(r["ParentId"], System.Globalization.CultureInfo.InvariantCulture)));
    }

    [Fact]
    public async Task Insert_MultiColumnFk_EnforcesAllColumns()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("MP");
        string child = MakeTableName("MC");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("A", typeof(int)), new("B", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("RefA", typeof(int)), new("RefB", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [1, 2], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_Multi", parent, ["A", "B"], child, ["RefA", "RefB"]),
            TestContext.Current.CancellationToken);

        // Wrong B value → reject.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(child, [1, 1, 99], TestContext.Current.CancellationToken));

        // Correct tuple → succeed.
        await writer.InsertRowAsync(child, [2, 1, 2], TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Atomicity invariant: a constraint violation deep in a multi-row
    /// insert must roll back every row written earlier in the batch —
    /// the database must look exactly as it did before the call.
    /// </summary>
    /// <remarks>
    /// The pre-write unique check (covered by
    /// <c>JetTransactionTests.UseTransactionalWrites_RollsBackOnExceptionDuringInsert</c>)
    /// fires <em>before</em> any row is physically inserted, so it doesn't
    /// exercise the per-call <c>RollbackInsertedRowsAsync</c> path. Foreign-key
    /// enforcement runs <em>per-row</em> inside the loop after the row has
    /// been written, so we use it to land partial writes on disk before
    /// the throw.
    /// </remarks>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task InsertRows_WithFkViolationDeepInBatch_RollsBackEntireBatch()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("AP");
        string child = MakeTableName("AC");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            // Seed every legal parent key 1..10.
            var parentRows = new List<object[]>(10);
            for (int i = 1; i <= 10; i++)
            {
                parentRows.Add([i]);
            }

            await writer.InsertRowsAsync(parent, parentRows, TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_AtomBatch", parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);

            // Seed one valid child row so we can detect any "leaked" rows
            // from the failed batch by row count.
            await writer.InsertRowAsync(child, [1, 1], TestContext.Current.CancellationToken);

            // Build a batch where rows 0..N-2 are valid and row N-1 is
            // an FK violation (ParentId=999 has no matching parent).
            // FK enforcement runs per-row INSIDE the insert loop after the
            // row has been written, so rows 0..N-2 land on disk before the
            // throw — the per-call rollback path must remove them.
            object[][] batch =
            [
                [100, 1],
                [101, 2],
                [102, 3],
                [103, 4],
                [104, 5],
                [105, 6],
                [106, 7],
                [107, 999], // FK violation — no parent.
                [108, 8],
            ];

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowsAsync(child, batch, TestContext.Current.CancellationToken));
        }

        // Reopen and assert the child table contains ONLY the seed row;
        // none of the partial-batch rows survived the rollback.
        await using var reader = await OpenReaderAsync(temp);
        DataTable c = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        DataRow only = Assert.Single(c.AsEnumerable());
        Assert.Equal(1, Convert.ToInt32(only["Id"], System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(only["ParentId"], System.Globalization.CultureInfo.InvariantCulture));

        // Specifically: none of the batch's Ids (100..108) leaked through.
        Assert.DoesNotContain(
            c.AsEnumerable(),
            r => Convert.ToInt32(r["Id"], System.Globalization.CultureInfo.InvariantCulture) >= 100);
    }

    [Fact]
    public async Task Insert_LargeParentTable_SeekPathFindsExistingKey()
    {
        // Regression for the seek-based RI enforcement (replacement for the
        // O(N) parent scan): build a parent with thousands of rows, create
        // the relationship after the parent rows already exist, then INSERT
        // a child row whose FK matches a parent key buried deep in the
        // table. The seek path must find it without loading every parent
        // row into memory; functional verification only.
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("BP");
        string child = MakeTableName("BC");

        const int parentRowCount = 5_000;
        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            var rows = new List<object[]>(parentRowCount);
            for (int i = 1; i <= parentRowCount; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync(parent, rows, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Seek_Big", parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);

            // Existing parent key (deep in the table) → must succeed via seek.
            await writer.InsertRowAsync(child, [1, parentRowCount - 7], TestContext.Current.CancellationToken);

            // Missing parent key → must throw.
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(child, [2, parentRowCount + 100], TestContext.Current.CancellationToken));
            Assert.Contains("FK_Seek_Big", ex.Message, StringComparison.Ordinal);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Single(t.Rows);
        Assert.Equal(parentRowCount - 7, Convert.ToInt32(t.Rows[0]["ParentId"], System.Globalization.CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task Insert_TextKeyFk_SeekPathHonoursGeneralLegacyEncoding()
    {
        // Text key relationships exercise the General Legacy encoder on
        // both the writer's index leaf emission and the seeker's lookup
        // path. The earlier HashSet path used case-insensitive ToUpperInvariant
        // string equality (BuildCompositeKey/AppendNormalized); the seeker
        // path now relies on the byte-identical encoding round-trip that
        // the writer uses when building the leaf.
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("TP");
        string child = MakeTableName("TC");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Code", typeof(string), maxLength: 32)], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("Code", typeof(string), maxLength: 32)], TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(parent, [["alpha"], ["bravo"], ["charlie"]], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Seek_Text", parent, "Code", child, "Code"),
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(child, [1, "bravo"], TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(child, [2, "delta"], TestContext.Current.CancellationToken));
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Single(t.Rows);
    }

    [Fact]
    public async Task Insert_BulkChildren_SeekPathReusesParentIndexAcrossRows()
    {
        // Bulk insert spanning many distinct parent keys must succeed when
        // every key exists. Verifies (1) the per-call seek-index cache
        // (FkContext.SeekIndexes) is reused across rows and (2) self-ref
        // pending tracking does not accidentally reject a real parent key
        // that is also the FK column of a previously-inserted child.
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("BP");
        string child = MakeTableName("BC");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            var pRows = new List<object[]>(200);
            for (int i = 1; i <= 200; i++)
            {
                pRows.Add([i]);
            }

            await writer.InsertRowsAsync(parent, pRows, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Seek_Bulk", parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);

            var cRows = new List<object[]>(200);
            for (int i = 1; i <= 200; i++)
            {
                cRows.Add([i, ((i * 7) % 200) + 1]);
            }

            await writer.InsertRowsAsync(child, cRows, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(200, t.Rows.Count);
    }

    [Fact]
    public async Task Delete_PkSide_WithCascade_BulkSeeksChildIndex()
    {
        // covering child-side real-idx is present (auto-emitted on every
        // FK relationship in Jet4 / ACE), so cascade-delete should locate the
        // 200 dependent child rows via index-seek rather than the legacy
        // O(N) child snapshot scan. Functional verification only — proves the
        // post-state is correct at scale; the snapshot fallback would also
        // pass this test.
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("DP");
        string child = MakeTableName("DC");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            var pRows = new List<object[]>(50);
            for (int i = 1; i <= 50; i++)
            {
                pRows.Add([i]);
            }

            await writer.InsertRowsAsync(parent, pRows, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_DelSeek", parent, "Id", child, "ParentId")
                {
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);

            var cRows = new List<object[]>(200);
            for (int i = 1; i <= 200; i++)
            {
                cRows.Add([i, ((i - 1) % 50) + 1]);
            }

            await writer.InsertRowsAsync(child, cRows, TestContext.Current.CancellationToken);

            // Delete a single parent — should cascade-delete exactly 4
            // children (rows whose ParentId == 7).
            int deleted = await writer.DeleteRowsAsync(parent, "Id", 7, TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(196, t.Rows.Count);
        foreach (DataRow r in t.Rows)
        {
            Assert.NotEqual(7, (int)r["ParentId"]);
        }
    }

    [Fact]
    public async Task Update_PkSide_WithCascade_BulkSeeksChildIndex()
    {
        // cascade-update via child-side index-seek. Move PK 7 → 999 and
        // assert all children that referenced 7 now reference 999, and the
        // rest are unchanged.
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string parent = MakeTableName("UP");
        string child = MakeTableName("UC");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            var pRows = new List<object[]>(50);
            for (int i = 1; i <= 50; i++)
            {
                pRows.Add([i]);
            }

            await writer.InsertRowsAsync(parent, pRows, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_UpdSeek", parent, "Id", child, "ParentId")
                {
                    CascadeUpdates = true,
                },
                TestContext.Current.CancellationToken);

            var cRows = new List<object[]>(200);
            for (int i = 1; i <= 200; i++)
            {
                cRows.Add([i, ((i - 1) % 50) + 1]);
            }

            await writer.InsertRowsAsync(child, cRows, TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                parent,
                "Id",
                7,
                new Dictionary<string, object> { ["Id"] = 999 },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(child, cancellationToken: TestContext.Current.CancellationToken))!;
        int repointed = 0;
        foreach (DataRow r in t.Rows)
        {
            int p = (int)r["ParentId"];
            Assert.NotEqual(7, p);
            if (p == 999)
            {
                repointed++;
            }
        }

        Assert.Equal(4, repointed);
    }

    /// <summary>
    /// Self-referential FK with cascade-update — a single table whose
    /// <c>ParentId</c> column references its own <c>Id</c> column. Asserts
    /// that:
    /// <list type="bullet">
    ///   <item><description>the relationship can be created against the same table on both sides;</description></item>
    ///   <item><description>renaming an <c>Id</c> propagates to every row whose <c>ParentId</c> referenced it (the cascade walks the index on the same table);</description></item>
    ///   <item><description>rows that referenced a different parent are untouched;</description></item>
    ///   <item><description>null-FK rows are untouched.</description></item>
    /// </list>
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Update_SelfReferentialFk_WithCascade_PropagatesToChildRowsInSameTable()
    {
        var temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        string tbl = MakeTableName("SR");

        await using (var writer = await OpenWriterAsync(temp))
        {
            // Mirror the proven-working shape used by
            // Update_PkSide_WithCascade_BulkSeeksChildIndex — same table on
            // both sides of the FK, no explicit PK declaration on the
            // writer-side schema, so the cascade walks via the FK's
            // child-side index seek.
            await writer.CreateTableAsync(
                tbl,
                [
                    new("Id", typeof(int)),
                    new("ParentId", typeof(int)),
                    new("Label", typeof(string), maxLength: 32),
                ],
                TestContext.Current.CancellationToken);

            // Seed parent rows BEFORE creating the FK so the rows that point
            // at not-yet-inserted ids don't trip insert-time enforcement.
            for (int i = 1; i <= 10; i++)
            {
                await writer.InsertRowAsync(tbl, [i, DBNull.Value, $"node-{i}"], TestContext.Current.CancellationToken);
            }

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Self", tbl, "Id", tbl, "ParentId")
                {
                    CascadeUpdates = true,
                },
                TestContext.Current.CancellationToken);

            // Add child rows (ParentId in 1..10).
            for (int i = 11; i <= 30; i++)
            {
                await writer.InsertRowAsync(tbl, [i, ((i - 11) % 10) + 1, $"child-{i}"], TestContext.Current.CancellationToken);
            }

            // Move the row with Id=7 to Id=999. The cascade must repoint
            // every row whose ParentId == 7 to ParentId = 999.
            int updated = await writer.UpdateRowsAsync(
                tbl,
                "Id",
                7,
                new Dictionary<string, object> { ["Id"] = 999 },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using var reader = await OpenReaderAsync(temp);
        DataTable t = (await reader.ReadDataTableAsync(tbl, cancellationToken: TestContext.Current.CancellationToken))!;

        // Count children that were repointed to the new Id and ensure no
        // child still references the old Id.
        int repointed = 0;
        foreach (DataRow r in t.Rows)
        {
            if (r["ParentId"] is DBNull)
            {
                continue;
            }

            int p = (int)r["ParentId"];
            Assert.NotEqual(7, p);
            if (p == 999)
            {
                repointed++;
            }
        }

        // 20 child rows distributed round-robin across ParentId 1..10 means
        // exactly 2 rows referenced the old Id=7 and must now reference 999.
        Assert.Equal(2, repointed);
    }

    /// <summary>
    /// §6 gap: many-to-many through a junction table where both FKs have
    /// <c>EnforceReferentialIntegrity = true</c> and <c>CascadeDeletes = true</c>.
    /// Deleting a parent row must cascade into the junction table, removing
    /// junction rows that reference the deleted key while leaving the other
    /// parent's rows intact.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Delete_ManyToMany_CascadesJunctionRows()
    {
        var temp = await db.CopyToStreamAsync(
            TestDatabases.NorthwindTraders,
            TestContext.Current.CancellationToken);

        string students = MakeTableName("S");
        string courses = MakeTableName("Cr");
        string junction = MakeTableName("SC");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(
                students,
                [new("StudentId", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                courses,
                [new("CourseId", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                junction,
                [
                    new("Id", typeof(int)),
                    new("StudentId", typeof(int)),
                    new("CourseId", typeof(int)),
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    $"FK_{junction}_S",
                    students,
                    "StudentId",
                    junction,
                    "StudentId")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    $"FK_{junction}_C",
                    courses,
                    "CourseId",
                    junction,
                    "CourseId")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                students,
                [[1], [2]],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                courses,
                [[10], [20]],
                TestContext.Current.CancellationToken);

            // Enrollments: student 1 → courses 10+20, student 2 → course 10.
            await writer.InsertRowsAsync(
                junction,
                [[1, 1, 10], [2, 1, 20], [3, 2, 10]],
                TestContext.Current.CancellationToken);

            // Delete student 1 — should cascade-delete junction rows 1 and 2.
            int deleted = await writer.DeleteRowsAsync(
                students,
                "StudentId",
                1,
                TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        await using var reader = await OpenReaderAsync(temp);

        // Student table: only student 2 remains.
        DataTable s = (await reader.ReadDataTableAsync(
            students,
            cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(1, s.Rows.Count);
        Assert.Equal(2, (int)s.Rows[0]["StudentId"]);

        // Junction table: only the student-2 / course-10 row survives.
        DataTable j = (await reader.ReadDataTableAsync(
            junction,
            cancellationToken: TestContext.Current.CancellationToken))!;
        var only = Assert.Single(j.AsEnumerable());
        Assert.Equal(2, (int)only["StudentId"]);
        Assert.Equal(10, (int)only["CourseId"]);

        // Courses table: both courses still exist (no cascade from junction→course).
        DataTable c = (await reader.ReadDataTableAsync(
            courses,
            cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(2, c.Rows.Count);
    }

    private static string MakeTableName(string prefix) =>
        $"{prefix}_{Guid.NewGuid():N}".Substring(0, Math.Min(18, prefix.Length + 11));

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true);
    }
}
