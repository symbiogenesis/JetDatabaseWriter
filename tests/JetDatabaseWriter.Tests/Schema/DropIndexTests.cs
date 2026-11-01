namespace JetDatabaseWriter.Tests.Schema;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Drops ordinary and primary indexes while preserving rows and dependent keys.</summary>
public sealed class DropIndexTests
{
    /// <summary>Gets every supported format and statement mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task PopulatedTable_DropsOnlyNamedIndexAndReleasesUniqueConstraint(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Value", typeof(int))], [new("PrimaryKey", "Id") { IsPrimaryKey = true }, new("UniqueValue", "Value") { IsUnique = true }, new("ByValueAndId", ["Value", "Id"]) { DescendingColumns = ["Id"] }], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1, 7], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [2, 8], TestContext.Current.CancellationToken);
#pragma warning disable CA1859 // This call verifies the public schema interface as well as the writer facade.
                IAccessSchema schema = writer;
#pragma warning restore CA1859
                await schema.DropIndexAsync("items", "uniquevalue", TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [3, 7], TestContext.Current.CancellationToken);
            },
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(["ByValueAndId", "PrimaryKey"], indexes.Select(index => index.Name).Order());
        IndexMetadata composite = Assert.Single(indexes, index => index.Name == "ByValueAndId");
        Assert.Equal([true, false], composite.Columns.Select(column => column.IsAscending));
        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal([1, 2, 3], rows.Rows.Cast<DataRow>().Select(row => (int)row["Id"]).Order());
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task PrimaryKeyWithoutRelationships_CanBeDropped(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
                await writer.DropIndexAsync("Items", "PrimaryKey", TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
                await writer.CreateIndexAsync("Items", new("ById", "Id"), TestContext.Current.CancellationToken);
            },
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("ById", Assert.Single(await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken)).Name);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RelationshipIndexes_RefuseBeforeChangingEitherTable()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new() { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Parents", [new("Id", typeof(int))], [new("PrimaryKey", "Id") { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Children", [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(new("FK_Children_Parents", "Parents", "Id", "Children", "ParentId"), TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Parents", [1], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Children", [1, 1], TestContext.Current.CancellationToken);
        await writer.CreateIndexAsync("Children", new("ById", "Id"), TestContext.Current.CancellationToken);
        await writer.DropIndexAsync("Children", "ById", TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        JetOperationException primary = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropIndexAsync("Parents", "PrimaryKey", TestContext.Current.CancellationToken));
        JetOperationException foreign = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropIndexAsync("Children", "FK_Children_Parents", TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.IndexInRelationship, primary.ErrorCode);
        Assert.Equal(JetErrorCode.IndexInRelationship, foreign.ErrorCode);
        Assert.Equal(before, stream.ToArray());
        await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Children", [2, 999], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExplicitRollback_RestoresDroppedIndexAndItsConstraint()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new() { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], [new("UniqueId", "Id") { IsUnique = true }], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken))
        {
            await writer.DropIndexAsync("Items", "UniqueId", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
            await transaction.RollbackAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(before, stream.ToArray());
        await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken));
        JetObjectNotFoundException missing = await Assert.ThrowsAsync<JetObjectNotFoundException>(async () => await writer.DropIndexAsync("Items", "Absent", TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.IndexNotFound, missing.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task IndexProperties_RemoveOnlyDroppedTargetAndRetainOpaqueNameMap()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new() { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], [new("Remove", "Id"), new("Keep", "Id")], TestContext.Current.CancellationToken);
        ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("Items", TestContext.Current.CancellationToken);
        ColumnPropertyBlock? properties = await writer.Services.Snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, TestContext.Current.CancellationToken);
        ColumnPropertyBlockBuilder builder = properties is null ? new ColumnPropertyBlockBuilder() : ColumnPropertyBlockBuilder.FromBlock(properties);
        builder.GetOrAddTableTarget().Entries.Add(new() { Name = "NameMap", DataType = ColumnType.OleType, Value = [0x00, 0xFF, 0x80, 0x42] });
        foreach (string name in new[] { "Remove", "Keep" })
        {
            var target = new ColumnPropertyTargetBuilder { Name = name, ChunkType = ColumnPropertyChunkType.PropertyBlockAlt2 };
            target.Entries.Add(new() { Name = "Opaque", DataType = ColumnType.OleType, Value = [0xAB, 0xCD] });
            builder.Targets.Add(target);
        }

        await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("Items", table.Entry.TDefPage, builder.ToBytes(writer.Database.Format), cancellationToken: TestContext.Current.CancellationToken);
        await writer.DropIndexAsync("Items", "Remove", TestContext.Current.CancellationToken);
        ColumnPropertyBlock stored = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, TestContext.Current.CancellationToken));
        Assert.DoesNotContain(stored.Targets, target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2 && target.Name == "Remove");
        ColumnPropertyTarget kept = Assert.Single(stored.Targets, target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2 && target.Name == "Keep");
        Assert.Equal([0xAB, 0xCD], Assert.Single(kept.Entries).Value);
        Assert.Equal([0x00, 0xFF, 0x80, 0x42], Assert.Single(Assert.IsType<ColumnPropertyTarget>(stored.FindTableTarget()).Entries, entry => entry.Name == "NameMap").Value);
    }

    [Fact]
    public async Task ComplexReferenceIndex_RefusesBeforeRemovingRequiredIdentityConstraint()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new() { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Values", typeof(string)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await writer.Services.Snapshots.ReadIndexMetadataSnapshotAsync("Items", TestContext.Current.CancellationToken);
        IndexMetadata index = Assert.Single(indexes, index => index.Columns.Count == 1 && index.Columns[0].Name == "Values");
        byte[] before = stream.ToArray();
        JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropIndexAsync("Items", index.Name, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.IndexRequiredByColumn, failure.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }
}
