namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Columns, indexes and object property maps retain their distinct native identities.</summary>
public sealed class PropertyTargetIdentityTests
{
    private static readonly JetFormat Format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets each format and write mode for public schema projection.</summary>
    /// <returns>The format and write mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes()
        => WriteModes.Combine([DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb]);

    [Fact]
    public void ColumnLookup_IgnoresSameNameIndexAndObjectMaps()
    {
        ColumnPropertyBlock block = SameNameBlock();
        Assert.Equal("column", block.FindTarget("Score")?.GetTextValue("Description", Format));
        Assert.True(block.FindPropertyTarget("Score", "Required")?.GetBooleanValue("Required"));
        ConstraintRegistry.ValidatePersistedConstraintProperties("T", block);
    }

    [Fact]
    public void Builder_ColumnOperationsLeaveSameNameIndexMapUnchanged()
    {
        var builder = ColumnPropertyBlockBuilder.FromBlock(SameNameBlock());
        Assert.Equal(ColumnPropertyChunkType.PropertyBlockAlt1, builder.GetOrAddTarget("Score").ChunkType);
        builder.RenameTarget("Score", "Amount");
        Assert.Equal("index", Parse(builder).Targets.Single(target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2).GetTextValue("Description", Format));
        Assert.Equal("column", Parse(builder).FindTarget("Amount")?.GetTextValue("Description", Format));
        Assert.True(builder.RemoveTarget("Amount"));
        Assert.Contains(builder.Targets, target => target.Name == "Score" && target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Projector_ColumnRenameOrDropPreservesOtherSubtypes(bool drop)
    {
        ColumnDefinition source = new("Score", typeof(int));
        ColumnPropertyBlock block = SameNameBlock();
        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            block,
            [source],
            drop ? [] : [source.WithName("Amount")],
            _ => drop ? null : "Amount",
            Format);
        foreach (ColumnPropertyTarget original in block.Targets.Where(target => target.ChunkType != ColumnPropertyChunkType.PropertyBlockAlt1))
        {
            ColumnPropertyTarget retained = Assert.Single(projected.Targets, target => target.ChunkType == original.ChunkType);
            Assert.Equal(original.Name, retained.Name);
            Assert.Equal(original.Entries.Select(entry => entry.Name), retained.Entries.Select(entry => entry.Name));
            for (int index = 0; index < original.Entries.Count; index++)
            {
                Assert.Equal(original.Entries[index].Value, retained.Entries[index].Value);
            }
        }

        Assert.Equal(drop ? 0 : 1, projected.Targets.Count(target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt1));
    }

    [Fact]
    public void Projector_RemovesOnlyMapsOfIndexesThatDoNotSurvive()
    {
        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            SameNameBlock(),
            [new("Score", typeof(int))],
            [new("Score", typeof(int))],
            static name => name,
            Format,
            droppedIndexNames: ["score"]);
        Assert.DoesNotContain(projected.Targets, target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2);
        Assert.Equal("column", projected.FindTarget("Score")?.GetTextValue("Description", Format));
    }

    [Fact]
    public void Projector_UnrelatedIndexDropPreservesOrphanIndexMap()
    {
        ColumnPropertyBlock original = SameNameBlock();
        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            original,
            [new("Score", typeof(int))],
            [new("Score", typeof(int))],
            static name => name,
            Format,
            droppedIndexNames: ["OtherIndex"]);
        ColumnPropertyTarget retained = Assert.Single(projected.Targets, target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2);
        Assert.Equal("Score", retained.Name);
        Assert.Equal("index", retained.GetTextValue("Description", Format));
    }

    [Fact]
    public void TableLookup_IgnoresEmptyColumnAndIndexMaps()
    {
        var builder = new ColumnPropertyBlockBuilder();
        builder.Targets.Add(new ColumnPropertyTargetBuilder { Name = string.Empty, ChunkType = ColumnPropertyChunkType.PropertyBlockAlt2 });
        builder.Targets.Add(new ColumnPropertyTargetBuilder { Name = string.Empty, ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1 });
        Assert.Null(Parse(builder).FindTableTarget());
        Assert.Equal(ColumnPropertyChunkType.PropertyBlock, builder.GetOrAddTableTarget().ChunkType);
        Assert.Equal(ColumnPropertyChunkType.PropertyBlock, Parse(builder).FindTableTarget()?.ChunkType);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task PublicMetadataAndConstraints_UseColumnMapWhenIndexMapComesFirst(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Score", typeof(int)) { Description = "column", IsNullable = false }], [new IndexDefinition("Score", "Id")], Ct);
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
            ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, Ct));
            var builder = new ColumnPropertyBlockBuilder();
            ColumnPropertyTargetBuilder index = IndexTarget(writer.Database.Format);
            builder.Targets.Add(index);
            builder.Targets.AddRange(properties.Targets.Select(ColumnPropertyBlockBuilder.FromTarget));
            byte[] bytes = Assert.IsType<byte[]>(builder.ToBytes(writer.Database.Format));
            Assert.Equal(ColumnPropertyChunkType.PropertyBlockAlt2, ColumnPropertyBlock.Parse(bytes, writer.Database.Format)?.Targets[0].ChunkType);
            await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("T", table.Entry.TDefPage, bytes, cancellationToken: Ct);
        }

        stream.Position = 0;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            ColumnMetadata score = Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), column => column.Name == "Score");
            Assert.Equal("column", score.Description);
            Assert.False(score.IsNullable);
        }

        stream.Position = 0;
        await using AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetConstraintException>(async () => await reopened.InsertRowAsync("T", [1, DBNull.Value], Ct));
        Assert.Equal(before, stream.ToArray());
        await reopened.InsertRowAsync("T", [1, 2], Ct);
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task DropColumn_RemovesDroppedIndexMapAndPreservesSurvivorAndOrphanMaps(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new("Id", typeof(int)), new("Score", typeof(int)) { Description = "column" }, new("Keep", typeof(int))],
                [new IndexDefinition("Score", "Id"), new IndexDefinition("Gone", "Score")],
                Ct);
            await writer.InsertRowAsync("T", [1, 2, 3], Ct);
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
            ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, Ct));
            var builder = ColumnPropertyBlockBuilder.FromBlock(properties);
            builder.Targets.Add(IndexTarget(writer.Database.Format));
            builder.GetOrAddTarget("Gone", ColumnPropertyChunkType.PropertyBlockAlt2).AddText("SharedCustom", "dropped index", writer.Database.Format);
            builder.GetOrAddTarget("Orphan", ColumnPropertyChunkType.PropertyBlockAlt2).AddText("SharedCustom", "orphan index", writer.Database.Format);
            builder.GetOrAddTarget("Score", ColumnPropertyChunkType.PropertyBlock).AddText("SharedCustom", "opaque object", writer.Database.Format);
            await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("T", table.Entry.TDefPage, builder.ToBytes(writer.Database.Format), cancellationToken: Ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => writer.DropColumnAsync("T", "Score", Ct).AsTask(), Ct);
        }

        stream.Position = 0;
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            ResolvedTable rewritten = await harness.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
            ColumnPropertyBlock after = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(rewritten.Entry.TDefPage, Ct));
            Assert.Null(after.FindTarget("Score"));
            Assert.Null(after.FindTarget("Gone", ColumnPropertyChunkType.PropertyBlockAlt2));
            Assert.Equal("index", after.FindTarget("Score", ColumnPropertyChunkType.PropertyBlockAlt2)?.GetTextValue("Description", harness.Database.Format));
            Assert.Equal("orphan index", after.FindTarget("Orphan", ColumnPropertyChunkType.PropertyBlockAlt2)?.GetTextValue("SharedCustom", harness.Database.Format));
            Assert.Equal("opaque object", after.FindTarget("Score", ColumnPropertyChunkType.PropertyBlock)?.GetTextValue("SharedCustom", harness.Database.Format));
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal("T", Assert.Single(await reader.ListTablesAsync(Ct)));
        IndexMetadata retainedIndex = Assert.Single(await reader.ListIndexesAsync("T", Ct));
        Assert.Equal("Score", retainedIndex.Name);
        Assert.Equal("Id", Assert.Single(retainedIndex.Columns).Name);
        using System.Data.DataTable rows = await reader.ReadTableAsync("T", cancellationToken: Ct);
        Assert.Equal(3, Assert.Single(rows.Rows.Cast<System.Data.DataRow>())["Keep"]);
    }

    private static ColumnPropertyBlock SameNameBlock()
    {
        var builder = new ColumnPropertyBlockBuilder();
        builder.Targets.Add(IndexTarget(Format));
        var objectMap = new ColumnPropertyTargetBuilder { Name = "Score", ChunkType = ColumnPropertyChunkType.PropertyBlock };
        objectMap.AddText("Description", "object", Format);
        builder.Targets.Add(objectMap);
        var column = new ColumnPropertyTargetBuilder { Name = "Score", ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1 };
        column.AddText("Description", "column", Format);
        column.AddBoolean("Required", true);
        builder.Targets.Add(column);
        return Parse(builder);
    }

    private static ColumnPropertyTargetBuilder IndexTarget(JetFormat format)
    {
        var index = new ColumnPropertyTargetBuilder { Name = "Score", ChunkType = ColumnPropertyChunkType.PropertyBlockAlt2 };
        index.AddText("Description", "index", format);
        index.AddBoolean("Required", false);
        return index;
    }

    private static ColumnPropertyBlock Parse(ColumnPropertyBlockBuilder builder)
        => Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse(builder.ToBytes(Format), Format));
}
