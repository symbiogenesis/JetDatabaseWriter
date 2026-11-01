namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Data;
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

/// <summary>Index renames retain tree, table and relationship identities.</summary>
public sealed class RenameIndexTests
{
    /// <summary>Gets each native format and statement mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Rename_ReordersLogicalNamesWithoutChangingStoredRowsOrTrees(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Value", typeof(int))], [new("Zulu", "Value") { IsUnique = true }], Ct);
            await writer.InsertRowAsync("Items", [1, 7], Ct);
            await writer.InsertRowAsync("Items", [2, 8], Ct);
            ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", Ct);
            Assert.NotNull(table);
            long root = table.Entry.TDefPage;
            TDefImage before = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
            byte[] bytes = stream.ToArray();
            LogicalIndexSnapshot[] entries = [.. before.LogicalIndexes.Select(index => new LogicalIndexSnapshot(index.Entry.IndexNum, index.RawDescriptor.ToArray()))];
            await WriteModes.RunAsync(writer, mode, async () => await writer.RenameIndexAsync("Items", "zulu", "Alpha", Ct), Ct);
            TDefImage after = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(root, Ct));
            Assert.Equal(before.RealIndexes.Select(index => index.RootPage), after.RealIndexes.Select(index => index.RootPage));
            Assert.Equal(["Alpha", "PrimaryKey"], after.LogicalIndexes.Select(index => index.Name));
            foreach (LogicalIndexSnapshot entry in entries)
            {
                Assert.Equal(entry.Bytes, Assert.Single(after.LogicalIndexes, index => index.Entry.IndexNum == entry.Number).RawDescriptor);
            }

            // A short name change needs no new pages and changes only the TDEF.
            byte[] changed = stream.ToArray();
            Assert.Equal(bytes.Length, changed.Length);
            int pageSize = writer.Database.Format.PageSize;
            for (int offset = 0; offset < bytes.Length; offset += pageSize)
            {
                if (offset / pageSize != root)
                {
                    Assert.Equal(bytes.AsSpan(offset, pageSize).ToArray(), changed.AsSpan(offset, pageSize).ToArray());
                }
            }

            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Items", [3, 7], Ct));
            await writer.InsertRowAsync("Items", [3, 9], Ct);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IndexMetadata renamed = Assert.Single(await reader.ListIndexesAsync("Items", Ct), index => index.Name == "Alpha");
        Assert.True(renamed.EnforcesUniqueness);
        Assert.Equal("Value", Assert.Single(renamed.Columns).Name);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenamePrimaryKey_KeepsForeignKeyPartnersAndRollback(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("Parents", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Children", [new("ParentId", typeof(int))], Ct);
        await writer.CreateRelationshipAsync(new("FK_Children_Parents", "Parents", "Id", "Children", "ParentId"), Ct);
        await writer.InsertRowAsync("Parents", [1], Ct);
        byte[] original = stream.ToArray();
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
        {
            await writer.RenameIndexAsync("Parents", "PrimaryKey", new string('Z', 64), Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(original, stream.ToArray());
        await writer.RenameIndexAsync("Parents", "PrimaryKey", "ParentIdentity", Ct);
        await writer.InsertRowAsync("Children", [1], Ct);
        await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Children", [2], Ct));
        await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Parents", [1], Ct));
        await writer.RenameIndexAsync("Parents", "ParentIdentity", "parentidentity", Ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Parents", Ct);
        Assert.NotNull(table);
        TDefImage image = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(table.Entry.TDefPage, Ct));
        Assert.Contains(image.LogicalIndexes, index => index.Name == "parentidentity");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MissingCollidingAndRelationshipNames_RefuseWithoutWrites(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("Parents", [new("Id", typeof(int)) { IsPrimaryKey = true }], [new("Other", "Id")], Ct);
        await writer.CreateTableAsync("Children", [new("ParentId", typeof(int))], Ct);
        await writer.CreateRelationshipAsync(new("FK_Children_Parents", "Parents", "Id", "Children", "ParentId"), Ct);
        byte[] original = stream.ToArray();
        JetOperationException missing = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.RenameIndexAsync("Parents", "Absent", "NewName", Ct));
        Assert.Equal(JetErrorCode.IndexNotFound, missing.ErrorCode);
        await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.RenameIndexAsync("Parents", "Other", "primarykey", Ct));
        await Assert.ThrowsAsync<JetArgumentException>(async () => await writer.RenameIndexAsync("Parents", "Other", "Bad.Name", Ct));
        await Assert.ThrowsAsync<JetOperationException>(async () => await writer.RenameIndexAsync("Children", "FK_Children_Parents", "Renamed", Ct));
        Assert.Equal(original, stream.ToArray());
        await writer.RenameIndexAsync("Parents", "Other", "Other", Ct);
        Assert.Equal(original, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task WideDefinition_RetainsLongValueMapsAndDistinctPropertyTargets(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await SeedPropertiesAsync(stream, format);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", Ct);
            Assert.NotNull(table);
            long root = table.Entry.TDefPage;
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
            Assert.True(chain.PageNumbers.Count > 1);
            ColumnPropertyBlock original = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(root, Ct));
            var expected = ColumnPropertyBlockBuilder.FromBlock(original);
            Assert.Single(expected.Targets, target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2).Name = new string('Z', 64);
            await writer.RenameIndexAsync("Items", "Value", new string('Z', 64), Ct);
            ColumnPropertyBlock renamed = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(root, Ct));
            Assert.Equal(expected.ToBytes(writer.Database.Format), ColumnPropertyBlockBuilder.FromBlock(renamed).ToBytes(writer.Database.Format));
            await writer.RenameIndexAsync("Items", new string('Z', 64), "A", Ct);
            Assert.Single(expected.Targets, target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2).Name = "A";
            ColumnPropertyBlock shortened = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(root, Ct));
            Assert.Equal(expected.ToBytes(writer.Database.Format), ColumnPropertyBlockBuilder.FromBlock(shortened).ToBytes(writer.Database.Format));
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(new string('n', 5000), Assert.Single(rows.Rows.Cast<DataRow>())["Notes"]);
        Assert.Contains(await reader.ListIndexesAsync("Items", Ct), index => index.Name == "A");
    }

    [Fact]
    public async Task NameGrowthBeyondSchemaBudget_RefusesBeforeWritingContinuation()
    {
        await using var stream = new MemoryStream();
        await SeedPropertiesAsync(stream, DatabaseFormat.AceAccdb);
        int capacity;
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", Ct);
            Assert.NotNull(table);
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(table.Entry.TDefPage, Ct);
            capacity = chain.Bytes.Length;
            int usedLength = BinaryPrimitives.ReadInt32LittleEndian(chain.Bytes.AsSpan(8)) + 8;
            Assert.True(usedLength < capacity - 4);
            chain.Bytes.AsSpan(usedLength, capacity - 4 - usedLength).Fill(0xA7);
            BinaryPrimitives.WriteInt32LittleEndian(chain.Bytes.AsSpan(8), capacity - 12);
            await chain.WriteAsync(chain.Bytes, capacity - 4, writer.Services.PageAllocator.AllocatePageAsync, writer.Pager.WritePageAsync, writer.Services.PageAllocator.DeallocatePageAsync, writer.Database.Format.WritesTDefFreeSpace, Ct);
        }

        byte[] original = stream.ToArray();
        stream.Position = 0;
        var options = new AccessWriterOptions { UseLockFile = false, MaxSchemaTraversalBytes = capacity };
        await using AccessWriter limited = await TestDatabase.OpenWriterAsync(stream, options, cancellationToken: Ct);
        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () => await limited.RenameIndexAsync("Items", "Value", new string('Z', 64), Ct));
        Assert.Equal(JetErrorCode.SchemaTraversalBudgetExceeded, failure.ErrorCode);
        Assert.Equal(original, stream.ToArray());
    }

    [Fact]
    public async Task OrphanDestinationIndexProperties_RefusesWithoutRebindingMetadata()
    {
        await using var stream = new MemoryStream();
        await SeedPropertiesAsync(stream, DatabaseFormat.AceAccdb);
        stream.Position = 0;
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", Ct);
        Assert.NotNull(table);
        ColumnPropertyBlock original = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, Ct));
        var properties = ColumnPropertyBlockBuilder.FromBlock(original);
        var orphan = new ColumnPropertyTargetBuilder { Name = "Unused", ChunkType = ColumnPropertyChunkType.PropertyBlockAlt2 };
        orphan.AddText("Description", "Unrelated orphan metadata", writer.Database.Format);
        properties.Targets.Add(orphan);
        await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("Items", table.Entry.TDefPage, properties.ToBytes(writer.Database.Format), cancellationToken: Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetOperationException>(async () => await writer.RenameIndexAsync("Items", "Value", "unused", Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task MetadataAndPropertyWrites_EachPartialFailureRestoresBytesAndAllowsRetry()
    {
        await using var seeded = new MemoryStream();
        await SeedPropertiesAsync(seeded, DatabaseFormat.AceAccdb);
        byte[] original = seeded.ToArray();
        int writes;
        await using (WriteFaultStream baseline = Copy(original))
        {
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(baseline, cancellationToken: Ct);
            int initial = baseline.WriteCount;
            await writer.RenameIndexAsync("Items", "Value", new string('Z', 64), Ct);
            writes = baseline.WriteCount - initial;
        }

        Assert.True(writes > 1);
        for (int point = 1; point <= writes; point++)
        {
            await using WriteFaultStream stream = Copy(original);
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
            stream.FailDuringWrite(point);
            await Assert.ThrowsAnyAsync<IOException>(async () => await writer.RenameIndexAsync("Items", "Value", new string('Z', 64), Ct));
            Assert.True(stream.Faulted);
            Assert.Equal(original, stream.ToArray());
            await writer.RenameIndexAsync("Items", "Value", new string('Z', 64), Ct);
        }
    }

    private static WriteFaultStream Copy(byte[] bytes)
    {
        var stream = new WriteFaultStream();
        stream.Write(bytes);
        stream.Position = 0;
        return stream;
    }

    private static async Task SeedPropertiesAsync(MemoryStream stream, DatabaseFormat format)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages }, leaveOpen: true, Ct);
        ColumnDefinition[] columns = [new("Value", typeof(int)), new("Notes", typeof(string), 10000), .. Enumerable.Range(0, 90).Select(index => new ColumnDefinition("Column" + index.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('c', 40), typeof(int)))];
        await writer.CreateTableAsync("Items", columns, [new("Value", "Value") { IsUnique = true }], Ct);
        object[] values = [7, new string('n', 5000), .. Enumerable.Repeat<object>(DBNull.Value, 90)];
        await writer.InsertRowAsync("Items", values, Ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", Ct);
        Assert.NotNull(table);
        var properties = new ColumnPropertyBlockBuilder();
        properties.GetOrAddTableTarget().AddMemoText("Description", new string('d', 5000), writer.Database.Format);
        properties.GetOrAddTarget("Value").AddText("Caption", "Column metadata", writer.Database.Format);
        var indexTarget = new ColumnPropertyTargetBuilder { Name = "Value", ChunkType = ColumnPropertyChunkType.PropertyBlockAlt2 };
        indexTarget.AddText("Description", "Index metadata", writer.Database.Format);
        properties.Targets.Add(indexTarget);
        await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("Items", table.Entry.TDefPage, properties.ToBytes(writer.Database.Format), cancellationToken: Ct);
    }

    private sealed record LogicalIndexSnapshot(int Number, byte[] Bytes);
}
