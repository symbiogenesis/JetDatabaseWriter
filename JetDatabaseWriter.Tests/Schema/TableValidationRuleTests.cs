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
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Stored table rules remain effective through reopen and schema changes.</summary>
/// <param name="db">The database fixture cache.</param>
public sealed class TableValidationRuleTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every database format and write mode.</summary>
    /// <returns>The format and mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>Equivalent duplicate entries and targets remain equivalent through replacement and removal.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task RepeatedTableRules_PreserveEveryEntryAndTarget(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Rules", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 0", "positive"), Ct);
        }

        ColumnPropertyBlock before = await PlantRepeatedRulesAsync(stream, conflicting: false);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 1", "greater than one"), Ct));
        }

        ColumnPropertyBlock after = await ReadRulePropertiesAsync(stream);
        ConstraintRegistry.ValidatePersistedConstraintProperties("Rules", after);
        Assert.Equal(3, after.Targets.Count);
        Assert.Equal(before.Targets.Select(target => target.ChunkType), after.Targets.Select(target => target.ChunkType));
        Assert.Equal(before.Targets.Select(target => target.SourceHeader), after.Targets.Select(target => target.SourceHeader));
        for (int targetIndex = 0; targetIndex < before.Targets.Count; targetIndex++)
        {
            ColumnPropertyTarget original = before.Targets[targetIndex];
            ColumnPropertyTarget edited = after.Targets[targetIndex];
            Assert.Equal(original.Entries.Select(entry => entry.Name), edited.Entries.Take(original.Entries.Count).Select(entry => entry.Name));
            for (int entryIndex = 0; entryIndex < original.Entries.Count; entryIndex++)
            {
                ColumnPropertyEntry oldEntry = original.Entries[entryIndex];
                ColumnPropertyEntry newEntry = edited.Entries[entryIndex];
                Assert.Equal(oldEntry.DataType, newEntry.DataType);
                Assert.Equal(oldEntry.DdlFlag, newEntry.DdlFlag);
                Assert.Equal(oldEntry.Padding, newEntry.Padding);
                Assert.Equal(oldEntry.SourceNameIndex, newEntry.SourceNameIndex);
            }

            Assert.All(edited.Entries.Where(entry => string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationRule, StringComparison.OrdinalIgnoreCase)), entry =>
                Assert.Equal(after.Format.PropertyTextEncoding.GetBytes("[Id] > 1"), entry.Value));
            Assert.All(edited.Entries.Where(entry => string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationText, StringComparison.OrdinalIgnoreCase)), entry =>
                Assert.Equal(after.Format.PropertyTextEncoding.GetBytes("greater than one"), entry.Value));
        }

        Assert.All(after.Targets, target => Assert.NotNull(target.Find(Constants.ColumnPropertyNames.ValidationRule)));
        Assert.All(after.Targets, target => Assert.NotNull(target.Find(Constants.ColumnPropertyNames.ValidationText)));
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("Rules", [1], Ct));
                await writer.InsertRowAsync("Rules", [10], Ct);
                await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 2"), Ct);
            });
        }

        ColumnPropertyBlock withoutText = await ReadRulePropertiesAsync(stream);
        ConstraintRegistry.ValidatePersistedConstraintProperties("Rules", withoutText);
        Assert.All(withoutText.Targets, target => Assert.Null(target.Find(Constants.ColumnPropertyNames.ValidationText)));
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                await writer.SetTableValidationRuleAsync("Rules", null, Ct));
        }

        ColumnPropertyBlock removed = await ReadRulePropertiesAsync(stream);
        Assert.All(removed.Targets, target => Assert.Null(target.Find(Constants.ColumnPropertyNames.ValidationRule)));
        Assert.All(removed.Targets, target => Assert.Null(target.Find(Constants.ColumnPropertyNames.ValidationText)));
        Assert.All(removed.Targets, target => Assert.Equal("unchanged", target.GetTextValue(Constants.ColumnPropertyNames.Description, removed.Format)));
        await using AccessWriter final = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode);
        await ForeignKeyTestDatabase.RunAsync(final, mode, async () => await final.InsertRowAsync("Rules", [-1], Ct));
    }

    /// <summary>Contradictory duplicate properties are refused before replacement can change bytes.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task ConflictingRepeatedTableRules_RefuseBeforeMutation(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Rules", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 0", "positive"), Ct);
        }

        _ = await PlantRepeatedRulesAsync(stream, conflicting: true);
        byte[] before = stream.ToArray();
        await using AccessWriter edit = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(() => ForeignKeyTestDatabase.RunAsync(edit, mode, async () =>
            await edit.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 1", "replacement"), Ct)));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, failure.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }

    /// <summary>Replacing table properties retains the original entry order, padding and name indices.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task TableRuleReplacement_PreservesStoredEntryStructure(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Rules", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 0", "positive"), Ct);
        }

        stream.Position = 0;
        ColumnPropertyTarget before;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("Rules", Ct));
            ColumnPropertyBlock stored = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
            var builder = ColumnPropertyBlockBuilder.FromBlock(stored);
            ColumnPropertyTargetBuilder target = builder.GetOrAddTableTarget();
            target.Entries[0].Padding = [0xA1, 0xB2, 0xC3];
            target.Entries[1].Padding = [0xD4, 0xE5];
            target.AddText(Constants.ColumnPropertyNames.Description, "unchanged", harness.Database.Format);
            byte[] bytes = Assert.IsType<byte[]>(builder.ToBytes(harness.Database.Format));
            before = Assert.IsType<ColumnPropertyTarget>(ColumnPropertyBlock.Parse(bytes, harness.Database.Format)?.FindTableTarget());
            await harness.Services.CatalogArtifacts.ExecutePlanAsync(
                new CatalogArtifactPlan([], [])
                {
                    CatalogReplacements = [new UserTableCatalogReplacementArtifact("Rules", "Rules", entry.TDefPage, bytes)],
                },
                Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 1", "greater than one"), Ct));
        }

        stream.Position = 0;
        await using WriterHarness final = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        long page = (await final.Services.Catalog.ResolveRequiredTableAsync("Rules", Ct)).Entry.TDefPage;
        ColumnPropertyTarget after = Assert.IsType<ColumnPropertyTarget>((await final.Services.Snapshots.ReadLvPropBlockAsync(page, Ct))?.FindTableTarget());
        Assert.Equal(before.Entries.Select(entry => entry.Name), after.Entries.Select(entry => entry.Name));
        for (int index = 0; index < before.Entries.Count; index++)
        {
            Assert.Equal(before.Entries[index].DataType, after.Entries[index].DataType);
            Assert.Equal(before.Entries[index].DdlFlag, after.Entries[index].DdlFlag);
            Assert.Equal(before.Entries[index].SourceNameIndex, after.Entries[index].SourceNameIndex);
            Assert.Equal(before.Entries[index].Padding, after.Entries[index].Padding);
        }
    }

    /// <summary>Rules persist, reject invalid rows, follow renames and protect dependencies.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task TableRule_PersistsAndFollowsSchemaChanges(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Rules", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Other", typeof(int))], Ct);
            Assert.Equal(1, await writer.InsertRowsAsync("Rules", [[1, 0]], Ct));
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 0", "positive identifier"), Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            long tablePage = (await harness.Services.Catalog.ResolveRequiredTableAsync("Rules", Ct)).Entry.TDefPage;
            ColumnPropertyBlock? properties = await harness.Services.Snapshots.ReadLvPropBlockAsync(tablePage, Ct);
            Assert.NotNull(properties);
            ColumnPropertyTarget? target = properties.FindTableTarget();
            Assert.NotNull(target);
            ColumnPropertyEntry? rule = target.Find(Constants.ColumnPropertyNames.ValidationRule);
            ColumnPropertyEntry? validationText = target.Find(Constants.ColumnPropertyNames.ValidationText);
            Assert.NotNull(rule);
            Assert.NotNull(validationText);
            Assert.Equal(ColumnType.MemoType, rule.DataType);
            Assert.Equal(1, rule.DdlFlag);
            Assert.Equal(ColumnType.TextType, validationText.DataType);
            Assert.Equal(1, validationText.DdlFlag);
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(new TableValidationRule("[Id] > 0", "positive identifier"), await reader.GetTableValidationRuleAsync("Rules", Ct));
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowsAsync("Rules", [[-1, 0]], Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("Rules", RowCriteria.Where("Id", 1), new RowValues { ["Other"] = 2 }, Ct));
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.UpdateRowsAsync("Rules", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = -1 }, Ct));
                await writer.RenameColumnAsync("Rules", "Id", "Value", Ct);
                _ = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("Rules", "Value", Ct));
            });
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(new TableValidationRule("[Value] > 0", "positive identifier"), await reader.GetTableValidationRuleAsync("Rules", Ct));
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Value] < 0"), Ct)));
        }

        Assert.Equal(before, stream.ToArray());
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await writer.SetTableValidationRuleAsync("Rules", null, Ct);
                Assert.Equal(1, await writer.InsertRowsAsync("Rules", [[-1, 0]], Ct));
            });
        }

        stream.Position = 0;
        await using AccessReader finalReader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        Assert.Null(await finalReader.GetTableValidationRuleAsync("Rules", Ct));
    }

    private static async Task<ColumnPropertyBlock> PlantRepeatedRulesAsync(MemoryStream stream, bool conflicting)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("Rules", Ct));
        ColumnPropertyBlock stored = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
        var builder = ColumnPropertyBlockBuilder.FromBlock(stored);
        ColumnPropertyTargetBuilder target = builder.GetOrAddTableTarget();
        target.Entries[0].DataType = ColumnType.TextType;
        target.Entries[0].Padding = [0xA1, 0xB2, 0xC3];
        target.Entries[1].Padding = [0xD4, 0xE5];
        target.AddText(Constants.ColumnPropertyNames.Description, "unchanged", harness.Database.Format);
        target.AddText("validationrule", conflicting ? "[Id] > 5" : "[Id] > 0", harness.Database.Format);
        target.Entries[^1].Padding = [0xF6];
        target.AddText("validationtext", "positive", harness.Database.Format);
        target.Entries[^1].Padding = [0x12, 0x34];
        ColumnPropertyBlock duplicated = Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse(builder.ToBytes(harness.Database.Format), harness.Database.Format));
        ColumnPropertyTargetBuilder repeated = ColumnPropertyBlockBuilder.FromTarget(Assert.IsType<ColumnPropertyTarget>(duplicated.FindTableTarget()));
        repeated.SourceChunkIndex = -1;
        repeated.SourceHeader = 0xABCD;
        repeated.SourceHeaderIsNameLength = false;
        repeated.Entries[0].DdlFlag = 0;
        repeated.Entries[0].Padding = [0x56, 0x78];
        builder.Targets.Add(repeated);
        var empty = new ColumnPropertyTargetBuilder { Name = string.Empty, ChunkType = ColumnPropertyChunkType.PropertyBlockAlt2 };
        empty.AddText(Constants.ColumnPropertyNames.Description, "unchanged", harness.Database.Format);
        builder.Targets.Add(empty);
        byte[] bytes = Assert.IsType<byte[]>(builder.ToBytes(harness.Database.Format));
        await harness.Services.CatalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan([], [])
            {
                CatalogReplacements = [new UserTableCatalogReplacementArtifact("Rules", "Rules", entry.TDefPage, bytes)],
            },
            Ct);
        return Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse(bytes, harness.Database.Format));
    }

    private static async Task<ColumnPropertyBlock> ReadRulePropertiesAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        long page = (await harness.Services.Catalog.ResolveRequiredTableAsync("Rules", Ct)).Entry.TDefPage;
        return Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(page, Ct));
    }
}
