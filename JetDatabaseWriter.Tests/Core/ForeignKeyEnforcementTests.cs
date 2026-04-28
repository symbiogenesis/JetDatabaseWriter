namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_Missing", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);

        // Parent is empty; child insert with ParentId=42 must be rejected.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(child, [1, 42], TestContext.Current.CancellationToken));
        Assert.Contains("FK_W10_Missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Insert_WithEnforce_AllowsRowWhenParentExists()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(parent, [7], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_Ok", parent, "Id", child, "ParentId"),
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_Null", parent, "Id", child, "ParentId"),
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_NoEnforce", parent, "Id", child, "ParentId")
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string table = MakeTableName("Tree");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(table, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            // Pre-seed root then declare relationship.
            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_Self", table, "Id", table, "ParentId"),
                TestContext.Current.CancellationToken);

            // Bulk insert: row 2 references row 1; row 3 references row 2.
            // The self-referential augmentation must let later rows see earlier ones.
            await writer.InsertRowsAsync(
                table,
                [
                    new object[] { 2, 1 },
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_UFk", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, [1, 5], TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.UpdateRowsAsync(
                child,
                "Id",
                1,
                new Dictionary<string, object> { ["ParentId"] = 999 },
                TestContext.Current.CancellationToken));
        Assert.Contains("FK_W10_UFk", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Delete_PkSide_WithoutCascade_RejectsWhenChildrenExist()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_DelNoCasc", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, [1, 5], TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.DeleteRowsAsync(parent, "Id", 5, TestContext.Current.CancellationToken));
        Assert.Contains("FK_W10_DelNoCasc", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Delete_PkSide_WithCascade_DeletesChildren()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_DelCasc", parent, "Id", child, "ParentId")
                {
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                child,
                [
                    new object[] { 1, 5 },
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_UPkNoCasc", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, [1, 5], TestContext.Current.CancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.UpdateRowsAsync(
                parent,
                "Id",
                5,
                new Dictionary<string, object> { ["Id"] = 99 },
                TestContext.Current.CancellationToken));
        Assert.Contains("FK_W10_UPkNoCasc", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Update_PkSide_WithCascade_PropagatesNewKeyToChildren()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("P");
        string child = MakeTableName("C");

        await using (var writer = await OpenWriterAsync(temp))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(parent, [5], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_UPkCasc", parent, "Id", child, "ParentId")
                {
                    CascadeUpdates = true,
                },
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                child,
                [
                    new object[] { 1, 5 },
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
    }

    [Fact]
    public async Task Insert_MultiColumnFk_EnforcesAllColumns()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
        string parent = MakeTableName("MP");
        string child = MakeTableName("MC");

        await using var writer = await OpenWriterAsync(temp);
        await writer.CreateTableAsync(parent, [new("A", typeof(int)), new("B", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("RefA", typeof(int)), new("RefB", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(parent, [1, 2], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_Multi", parent, ["A", "B"], child, ["RefA", "RefB"]),
            TestContext.Current.CancellationToken);

        // Wrong B value → reject.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(child, [1, 1, 99], TestContext.Current.CancellationToken));

        // Correct tuple → succeed.
        await writer.InsertRowAsync(child, [2, 1, 2], TestContext.Current.CancellationToken);
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
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
                new RelationshipDefinition("FK_W22_DelSeek", parent, "Id", child, "ParentId")
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
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);
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
                new RelationshipDefinition("FK_W22_UpdSeek", parent, "Id", child, "ParentId")
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

    private async ValueTask<MemoryStream> CopyToStreamAsync(string sourcePath)
    {
        byte[] bytes = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }
}
