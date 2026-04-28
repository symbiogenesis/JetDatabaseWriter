namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Round-trip tests for <see cref="AccessWriter.DropRelationshipAsync"/>
/// and <see cref="AccessWriter.RenameRelationshipAsync"/>. Each test starts
/// from a copy of <c>NorthwindTraders.accdb</c> (which has the
/// <c>MSysRelationships</c> system table), creates two scratch tables and a
/// relationship, and then exercises the mutation API.
/// </summary>
public sealed class RelationshipMutationTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task DropRelationshipAsync_RemovesAllCatalogRows()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        string parent = MakeTableName("DPar");
        string child = MakeTableName("DChi");
        string relName = $"FK_{child}_{parent}";

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("KeyA", typeof(int)), new("KeyB", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("RefA", typeof(int)), new("RefB", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, ["KeyA", "KeyB"], child, ["RefA", "RefB"]),
                TestContext.Current.CancellationToken);

            await writer.DropRelationshipAsync(relName, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        DataTable rels = (await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken))!;
        DataRow[] matching = rels.AsEnumerable()
            .Where(r => string.Equals(SafeString(r, "szRelationship"), relName, StringComparison.Ordinal))
            .ToArray();
        Assert.Empty(matching);
    }

    [Fact]
    public async Task DropRelationshipAsync_RemovesFkLogicalIdxEntriesFromBothSides()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        string parent = MakeTableName("DXP");
        string child = MakeTableName("DXC");
        string relName = $"FK_{child}_{parent}";

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);

            // Sanity: FK entries should exist before the drop.
            await using (var preReader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
            {
                Assert.Single(await preReader.ListIndexesAsync(parent, TestContext.Current.CancellationToken), ix => ix.Kind == IndexKind.ForeignKey);
                Assert.Single(await preReader.ListIndexesAsync(child, TestContext.Current.CancellationToken), ix => ix.Kind == IndexKind.ForeignKey);
            }

            await writer.DropRelationshipAsync(relName, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        var parentIdx = await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken);
        var childIdx = await reader.ListIndexesAsync(child, TestContext.Current.CancellationToken);

        Assert.DoesNotContain(parentIdx, ix => ix.Kind == IndexKind.ForeignKey);
        Assert.DoesNotContain(childIdx, ix => ix.Kind == IndexKind.ForeignKey);
    }

    [Fact]
    public async Task DropRelationshipAsync_NotFound_Throws()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.DropRelationshipAsync("FK_Definitely_Not_Present_" + Guid.NewGuid().ToString("N"), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DropRelationshipAsync_EmptyName_Throws()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.DropRelationshipAsync(string.Empty, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RenameRelationshipAsync_UpdatesEveryCatalogRow()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        string parent = MakeTableName("RPar");
        string child = MakeTableName("RChi");
        string oldName = $"FK_{child}_{parent}";
        string newName = $"FK2_{child}_{parent}";

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("KeyA", typeof(int)), new("KeyB", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("RefA", typeof(int)), new("RefB", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(oldName, parent, ["KeyA", "KeyB"], child, ["RefA", "RefB"]),
                TestContext.Current.CancellationToken);

            await writer.RenameRelationshipAsync(oldName, newName, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        DataTable rels = (await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.DoesNotContain(rels.AsEnumerable(), r => string.Equals(SafeString(r, "szRelationship"), oldName, StringComparison.Ordinal));

        DataRow[] renamed = rels.AsEnumerable()
            .Where(r => string.Equals(SafeString(r, "szRelationship"), newName, StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(2, renamed.Length);
        Assert.Equal(parent, SafeString(renamed[0], "szReferencedObject"));
        Assert.Equal(child, SafeString(renamed[0], "szObject"));
    }

    [Fact]
    public async Task RenameRelationshipAsync_DuplicateNewName_Throws()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        string parent = MakeTableName("RDP");
        string child = MakeTableName("RDC");
        string nameA = $"FKA_{child}_{parent}";
        string nameB = $"FKB_{child}_{parent}";

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("PA", typeof(int)), new("PB", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(nameA, parent, "Id", child, "PA"),
                TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(nameB, parent, "Id", child, "PB"),
                TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.RenameRelationshipAsync(nameA, nameB, TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task RenameRelationshipAsync_NotFound_Throws()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.RenameRelationshipAsync("FK_Missing_" + Guid.NewGuid().ToString("N"), "FK_New", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RenameRelationshipAsync_SameName_IsNoOp()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        // Should not throw even if the name does not exist, because the early
        // exit short-circuits before the lookup.
        await writer.RenameRelationshipAsync("AnyName", "anyname", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task RenameRelationshipAsync_UpdatesTDefLogicalIdxNameCookies()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        string parent = MakeTableName("RNP");
        string child = MakeTableName("RNC");
        string oldName = $"FK_{child}_{parent}";
        string newName = $"FK2_{child}_{parent}";

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(oldName, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);

            await writer.RenameRelationshipAsync(oldName, newName, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        var parentIdx = await reader.ListIndexesAsync(parent, TestContext.Current.CancellationToken);
        var childIdx = await reader.ListIndexesAsync(child, TestContext.Current.CancellationToken);

        // The TDEF logical-idx name cookie should now reflect the new name on
        // both sides — neither the old name nor the auto-renamed catalog row
        // should leave a stale cookie behind.
        Assert.Single(parentIdx, ix => ix.Kind == IndexKind.ForeignKey && ix.Name == newName);
        Assert.Single(childIdx, ix => ix.Kind == IndexKind.ForeignKey && ix.Name == newName);
        Assert.DoesNotContain(parentIdx, ix => ix.Name == oldName);
        Assert.DoesNotContain(childIdx, ix => ix.Name == oldName);
    }

    [Fact]
    public async Task DropRelationshipAsync_ReclaimsTrailingRealIdxSlot()
    {
        var temp = await CopyToStreamAsync(TestDatabases.NorthwindTraders);

        string parent = MakeTableName("DRP");
        string child = MakeTableName("DRC");
        string firstRel = $"FK1_{child}_{parent}";
        string secondRel = $"FK2_{child}_{parent}";

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(firstRel, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);

            int childRealIdxBefore;
            await using (var preReader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
            {
                IndexMetadata fkBefore = (await preReader.ListIndexesAsync(child, TestContext.Current.CancellationToken))
                    .Single(ix => ix.Kind == IndexKind.ForeignKey);
                childRealIdxBefore = fkBefore.RealIndexNumber;
            }

            await writer.DropRelationshipAsync(firstRel, TestContext.Current.CancellationToken);

            // Re-add a different FK on the same column. With trailing-slot
            // reclaim it should land on the same real-idx number the dropped
            // relationship freed; without reclaim it would advance to a
            // higher slot.
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(secondRel, parent, "Id", child, "ParentId"),
                TestContext.Current.CancellationToken);

            await using var postReader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
            IndexMetadata fkAfter = (await postReader.ListIndexesAsync(child, TestContext.Current.CancellationToken))
                .Single(ix => ix.Kind == IndexKind.ForeignKey);
            Assert.Equal(childRealIdxBefore, fkAfter.RealIndexNumber);
        }
    }

    private static string MakeTableName(string prefix) =>
        $"{prefix}_{Guid.NewGuid():N}".Substring(0, Math.Min(18, prefix.Length + 11));

    private static string SafeString(DataRow row, string column)
    {
        if (!row.Table.Columns.Contains(column))
        {
            return string.Empty;
        }

        object v = row[column];
        return v == DBNull.Value ? string.Empty : v.ToString() ?? string.Empty;
    }

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

    private async ValueTask<MemoryStream> CopyToStreamAsync(string sourcePath)
    {
        byte[] bytes = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }
}
