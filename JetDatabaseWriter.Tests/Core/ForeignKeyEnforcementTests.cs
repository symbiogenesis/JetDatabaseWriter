namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// W10 — runtime foreign-key enforcement on Insert / Update / Delete.
/// Each test creates a parent / child pair, declares a relationship via
/// <see cref="AccessWriter.CreateRelationshipAsync"/>, and exercises the
/// enforcement / cascade behaviour the W10 phase added to
/// <c>InsertRowAsync</c>, <c>InsertRowsAsync</c>, <c>UpdateRowsAsync</c>,
/// and <c>DeleteRowsAsync</c>.
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
            await writer.InsertRowAsync(child, new object[] { 1, 42 }, TestContext.Current.CancellationToken));
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
            await writer.InsertRowAsync(parent, new object[] { 7 }, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_Ok", parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(child, new object[] { 1, 7 }, TestContext.Current.CancellationToken);
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
            await writer.InsertRowAsync(child, new object[] { 1, DBNull.Value }, TestContext.Current.CancellationToken);
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
            await writer.InsertRowAsync(child, new object[] { 1, 99 }, TestContext.Current.CancellationToken);
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
            await writer.InsertRowAsync(table, new object[] { 1, DBNull.Value }, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_Self", table, "Id", table, "ParentId"),
                TestContext.Current.CancellationToken);

            // Bulk insert: row 2 references row 1; row 3 references row 2.
            // The self-referential augmentation must let later rows see earlier ones.
            await writer.InsertRowsAsync(
                table,
                new[]
                {
                    new object[] { 2, 1 },
                    new object[] { 3, 2 },
                },
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
        await writer.InsertRowAsync(parent, new object[] { 5 }, TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_UFk", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, new object[] { 1, 5 }, TestContext.Current.CancellationToken);

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
        await writer.InsertRowAsync(parent, new object[] { 5 }, TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_DelNoCasc", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, new object[] { 1, 5 }, TestContext.Current.CancellationToken);

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
            await writer.InsertRowAsync(parent, new object[] { 5 }, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_DelCasc", parent, "Id", child, "ParentId")
                {
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                child,
                new[]
                {
                    new object[] { 1, 5 },
                    new object[] { 2, 5 },
                },
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
        await writer.InsertRowAsync(parent, new object[] { 5 }, TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_UPkNoCasc", parent, "Id", child, "ParentId"),
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(child, new object[] { 1, 5 }, TestContext.Current.CancellationToken);

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
            await writer.InsertRowAsync(parent, new object[] { 5 }, TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_W10_UPkCasc", parent, "Id", child, "ParentId")
                {
                    CascadeUpdates = true,
                },
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                child,
                new[]
                {
                    new object[] { 1, 5 },
                    new object[] { 2, 5 },
                },
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
        await writer.InsertRowAsync(parent, new object[] { 1, 2 }, TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_W10_Multi", parent, ["A", "B"], child, ["RefA", "RefB"]),
            TestContext.Current.CancellationToken);

        // Wrong B value → reject.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(child, new object[] { 1, 1, 99 }, TestContext.Current.CancellationToken));

        // Correct tuple → succeed.
        await writer.InsertRowAsync(child, new object[] { 2, 1, 2 }, TestContext.Current.CancellationToken);
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
