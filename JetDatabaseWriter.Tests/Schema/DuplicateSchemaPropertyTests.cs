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

/// <summary>Schema rewrites preserve and reconcile every repeated property entry and target.</summary>
public sealed class DuplicateSchemaPropertyTests
{
    private static readonly JetFormat Format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every schema rewrite, duplicate layout and write mode.</summary>
    /// <returns>The focused schema cases.</returns>
    public static TheoryData<string, bool, WriteMode> SchemaCases()
    {
        var data = new TheoryData<string, bool, WriteMode>();
        foreach (string operation in new[] { "add", "drop", "rename" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(operation, false, mode);
                data.Add(operation, true, mode);
            }
        }

        return data;
    }

    /// <summary>Gets contradictory properties across schema rewrites and write modes.</summary>
    /// <returns>The focused refusal cases.</returns>
    public static TheoryData<string, string, WriteMode> ContradictoryCases()
    {
        var data = new TheoryData<string, string, WriteMode>();
        foreach (string property in new[] { "ValidationRule", "DefaultValue", "Expression", "ResultType", "Required" })
        {
            foreach (string operation in new[] { "add", "drop", "rename" })
            {
                foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                {
                    data.Add(property, operation, mode);
                }
            }
        }

        return data;
    }

    [Theory]
    [InlineData("ValidationRule")]
    [InlineData("DefaultValue")]
    [InlineData("Expression")]
    public void Projector_ChangedPropertyUpdatesEveryEntryWithoutReplacingMetadata(string property)
    {
        ColumnDefinition before = Definition(property, "[Source] + 1");
        ColumnDefinition after = Definition(property, "[Renamed] + 1");
        ColumnPropertyBlock original = DuplicateBlock(property, "[Source] + 1");

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(original, [before], [after], name => name, Format);

        Assert.Equal(original.Targets.Count, projected.Targets.Count);
        for (int targetIndex = 0; targetIndex < original.Targets.Count; targetIndex++)
        {
            ColumnPropertyTarget source = original.Targets[targetIndex];
            ColumnPropertyTarget target = projected.Targets[targetIndex];
            Assert.Equal(source.SourceHeader, target.SourceHeader);
            Assert.Equal(source.ChunkType, target.ChunkType);
            Assert.Equal(source.Entries.Count, target.Entries.Count);
            for (int entryIndex = 0; entryIndex < source.Entries.Count; entryIndex++)
            {
                ColumnPropertyEntry expected = source.Entries[entryIndex];
                ColumnPropertyEntry actual = target.Entries[entryIndex];
                AssertMetadata(expected, actual);
                Assert.Equal(expected.Name == property ? Format.PropertyTextEncoding.GetBytes("[Renamed] + 1") : expected.Value, actual.Value);
            }
        }
    }

    [Theory]
    [InlineData("ValidationRule")]
    [InlineData("DefaultValue")]
    [InlineData("Expression")]
    public void Projector_RemovedPropertyDisappearsFromEveryTarget(string property)
    {
        ColumnDefinition before = Definition(property, "[Source] + 1");
        ColumnPropertyBlock original = DuplicateBlock(property, "[Source] + 1");

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(original, [before], [new ColumnDefinition("Keep", typeof(int))], name => name, Format);

        Assert.Equal(original.Targets.Count, projected.Targets.Count);
        Assert.All(projected.Targets, target => Assert.DoesNotContain(target.Entries, entry => entry.Name == property));
        Assert.All(projected.Targets, target => Assert.Equal("Caption", Assert.Single(target.Entries).Name));
    }

    [Theory]
    [MemberData(nameof(SchemaCases))]
    public async Task SchemaRewrite_ReconcilesDuplicateAndSplitTargets(string operation, bool splitTargets, WriteMode mode)
    {
        await using MemoryStream stream = await CreateDatabaseAsync();
        await PlantDuplicatesAsync(stream, splitTargets);
        ColumnPropertyBlock before = await ReadBlockAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => RewriteAsync(writer, operation).AsTask(), Ct);
        }

        ColumnPropertyBlock after = await ReadBlockAsync(stream);
        string expectedSource = operation == "rename" ? "Renamed" : "Source";
        foreach (ColumnPropertyTarget target in after.Targets)
        {
            foreach (ColumnPropertyEntry entry in target.Entries)
            {
                if (entry.Name is "ValidationRule" or "DefaultValue" or "Expression")
                {
                    string expression = (target.TextEncoding ?? Format.PropertyTextEncoding).GetString(entry.Value);
                    Assert.Contains($"[{expectedSource}]", expression, StringComparison.Ordinal);
                    Assert.DoesNotContain("[Source]", operation == "rename" ? expression : string.Empty, StringComparison.Ordinal);
                }
            }
        }

        foreach (ColumnPropertyTarget source in before.Targets.Where(target => target.Name is "Keep" or "Calc" or ""))
        {
            ColumnPropertyTarget target = after.Targets[source.SourceChunkIndex - 1];
            Assert.Equal(source.Name, target.Name);
            Assert.Equal(source.SourceHeader, target.SourceHeader);
            Assert.Equal(source.Entries.Count, target.Entries.Count);
            for (int index = 0; index < source.Entries.Count; index++)
            {
                AssertMetadata(source.Entries[index], target.Entries[index]);
            }
        }

        ConstraintRegistry.ValidatePersistedConstraintProperties("T", after);
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            ColumnMetadata keep = Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), column => column.Name == "Keep");
            Assert.False(keep.IsNullable);
            Assert.Equal($"[{expectedSource}] + 1", keep.DefaultValueExpression);
            Assert.Equal($"[{expectedSource}] > 0", keep.ValidationRuleExpression);
            Assert.Equal($"[{expectedSource}] * 2", Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), column => column.Name == "Calc").CalculationExpression);
            Assert.Equal($"[{expectedSource}] > 0", Assert.IsType<TableValidationRule>(await reader.GetTableValidationRuleAsync("T", Ct)).Expression);
        }

        stream.Position = 0;
        await using (AccessWriter reopened = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            byte[] unchanged = stream.ToArray();
            await Assert.ThrowsAsync<JetConstraintException>(async () => await reopened.InsertRowsAsync("T", (RowValues[])[new() { [expectedSource] = 4, ["Keep"] = null }], Ct));
            Assert.Equal(unchanged, stream.ToArray());
            await Assert.ThrowsAsync<JetValidationRuleException>(async () => await reopened.InsertRowsAsync("T", (RowValues[])[new() { [expectedSource] = -1, ["Keep"] = 5 }], Ct));
            Assert.Equal(unchanged, stream.ToArray());
            Assert.Equal(1, await reopened.InsertRowsAsync("T", (RowValues[])[new() { [expectedSource] = 4 }], Ct));
        }

        stream.Position = 0;
        await using AccessReader final = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        using System.Data.DataTable storedRows = await final.ReadTableAsync("T", cancellationToken: Ct);
        System.Data.DataRow inserted = Assert.Single(storedRows.Rows.Cast<System.Data.DataRow>(), row => (int)row[expectedSource] == 4);
        Assert.Equal(5, inserted["Keep"]);
        Assert.Equal(8, inserted["Calc"]);
    }

    [Theory]
    [MemberData(nameof(ContradictoryCases))]
    public async Task SchemaRewrite_ContradictoryRepeatedPropertiesRefuseBeforeMutation(string property, string operation, WriteMode mode)
    {
        await using MemoryStream stream = await CreateDatabaseAsync();
        await PlantDuplicatesAsync(stream, splitTargets: false, contradictoryProperty: property);
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct);
        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await RewriteAsync(writer, operation));
                Assert.Equal(JetErrorCode.CorruptTableDefinition, error.ErrorCode);
            },
            Ct);
        Assert.Equal(before, stream.ToArray());
    }

    private static ColumnDefinition Definition(string property, string expression) => property switch
    {
        "ValidationRule" => new ColumnDefinition("Keep", typeof(int)) { ValidationRuleExpression = expression },
        "DefaultValue" => new ColumnDefinition("Keep", typeof(int)) { DefaultValueExpression = expression },
        _ => new ColumnDefinition("Keep", typeof(int)) { IsCalculated = true, CalculationExpression = expression },
    };

    private static ColumnPropertyBlock DuplicateBlock(string property, string expression)
    {
        var builder = new ColumnPropertyBlockBuilder();
        for (int targetIndex = 0; targetIndex < 2; targetIndex++)
        {
            var target = new ColumnPropertyTargetBuilder { Name = "Keep", ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1, SourceHeader = 0xAABBCCDDu + (uint)targetIndex };
            target.AddText("Caption", "Keep caption", Format);
            for (int entryIndex = 0; entryIndex < 2; entryIndex++)
            {
                target.AddMemoText(property, expression, Format);
                target.Entries[^1].DdlFlag = checked((byte)entryIndex);
                target.Entries[^1].Padding = [checked((byte)(targetIndex + entryIndex)), 0xA5];
            }

            builder.Targets.Add(target);
        }

        return Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse(builder.ToBytes(Format), Format));
    }

    private static void AssertMetadata(ColumnPropertyEntry expected, ColumnPropertyEntry actual)
    {
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.DataType, actual.DataType);
        Assert.Equal(expected.DdlFlag, actual.DdlFlag);
        Assert.Equal(expected.Padding, actual.Padding);
        Assert.Equal(expected.SourceNameIndex, actual.SourceNameIndex);
    }

    private static async Task<MemoryStream> CreateDatabaseAsync()
    {
        var stream = new MemoryStream();
        try
        {
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
            await writer.CreateTableAsync("T", [new("Source", typeof(int)), new("Keep", typeof(int)) { IsNullable = false, ValidationRuleExpression = "[Source] > 0", DefaultValueExpression = "[Source] + 1" }, new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = "[Source] * 2" }, new("Spare", typeof(int))], Ct);
            await writer.InsertRowAsync("T", [3, 5, 6, 0], Ct);
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    private static async Task PlantDuplicatesAsync(MemoryStream stream, bool splitTargets, string? contradictoryProperty = null)
    {
        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        CatalogEntry entry = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), value => value.Name == "T");
        ColumnPropertyBlock original = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
        var builder = ColumnPropertyBlockBuilder.FromBlock(original);
        ColumnPropertyTargetBuilder table = builder.GetOrAddTableTarget();
        table.AddText("ValidationRule", "[Source] > 0", Format);
        table.AddText("Caption", "Keep table", Format);
        ColumnPropertyBlock seeded = Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse(builder.ToBytes(Format), Format));
        builder = ColumnPropertyBlockBuilder.FromBlock(seeded);
        foreach (ColumnPropertyTarget target in seeded.Targets)
        {
            ColumnPropertyTargetBuilder duplicate = ColumnPropertyBlockBuilder.FromTarget(target);
            duplicate.SourceChunkIndex = -1;
            duplicate.SourceHeader = 0xAABBCCDD;
            duplicate.SourceHeaderIsNameLength = false;
            foreach (ColumnPropertyEntryBuilder property in duplicate.Entries)
            {
                property.DdlFlag ^= 1;
                property.Padding = [0xDA, 0x7A];
                if (property.Name == contradictoryProperty)
                {
                    property.Value = property.Name switch { "ResultType" => [(byte)ColumnType.DoubleType], "Required" => [0], _ => Format.PropertyTextEncoding.GetBytes("[Source] + 9") };
                }
            }

            builder.Targets.Add(duplicate);
            ColumnPropertyTargetBuilder first = builder.Targets.Find(value => value.Name == target.Name)!;
            if (splitTargets)
            {
                first.Entries.Clear();
                first.AddText("Caption", "Opaque first target", Format);
            }
            else
            {
                foreach (ColumnPropertyEntry property in target.Entries)
                {
                    var repeated = new ColumnPropertyEntryBuilder { Name = property.Name, DataType = property.DataType, DdlFlag = 1, Value = property.Value, Padding = [0x45, 0x67], SourceNameIndex = property.SourceNameIndex };
                    first.Entries.Add(repeated);
                }
            }
        }

        await harness.Services.CatalogWriter.UpdateTablePropertiesAsync("T", entry.TDefPage, builder.ToBytes(Format), cancellationToken: Ct);
    }

    private static async Task<ColumnPropertyBlock> ReadBlockAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        CatalogEntry entry = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), value => value.Name == "T");
        return Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
    }

    private static ValueTask RewriteAsync(AccessWriter writer, string operation) => operation switch
    {
        "add" => writer.AddColumnAsync("T", new ColumnDefinition("Added", typeof(int)), Ct),
        "drop" => writer.DropColumnAsync("T", "Spare", Ct),
        _ => writer.RenameColumnAsync("T", "Source", "Renamed", Ct),
    };
}
