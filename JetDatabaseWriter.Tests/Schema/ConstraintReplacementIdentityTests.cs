namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Constraint lifetimes do not depend on a reused physical location changing bytes.</summary>
public sealed class ConstraintReplacementIdentityTests
{
    /// <summary>Explicit replacement forgets old state even when the physical descriptors are identical.</summary>
    /// <param name="registerReplacement">Whether the replacement is declared by this writer or hydrated.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdenticalPhysicalReplacement_UsesNewConstraintsAndSnapshotRestoresOriginals(bool registerReplacement)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        ColumnPropertyBlock? properties = BuildProperties("7", ">= 0", "[Score] <= 10");
        long highWater = 10;
        int seedReads = 0;
        var registry = new ConstraintRegistry(
            (_, _) => ValueTask.FromResult<ColumnPropertyBlock?>(properties),
            (_, _, _, _) =>
            {
                seedReads++;
                return ValueTask.FromResult(highWater);
            });
        registry.Register(
            "T",
            [
                new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true },
                new ColumnDefinition("Score", typeof(int))
                {
                    DefaultValue = 7,
                    IsNullable = false,
                    ValidationRule = value => value is int score && score != 9,
                    ValidationRuleExpression = ">= 0",
                },
            ],
            format,
            tdefPageNumber: 10);
        TableDef original = CreateDefinition(includeAutoNumber: true);
        object[] first = [DBNull.Value, DbDefault.Value];
        _ = await registry.ApplyAsync("T", original, first, TestContext.Current.CancellationToken);
        Assert.Equal<object>([11, 7], first);
        ConstraintRegistrySnapshot snapshot = registry.CaptureSnapshot();

        registry.Unregister("T");
        properties = BuildProperties("99", ">= 50", "[Score] < 100");
        highWater = 30;
        if (registerReplacement)
        {
            registry.Register(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true },
                    new ColumnDefinition("Score", typeof(int))
                    {
                        DefaultValue = 99,
                        ValidationRuleExpression = ">= 50",
                    },
                ],
                format,
                tdefPageNumber: 10);
        }

        TableDef replacement = CreateDefinition(includeAutoNumber: true);
        Assert.NotSame(original, replacement);
        Assert.Equal(original.TDefPageNumber, replacement.TDefPageNumber);
        Assert.Equal(original.Columns[1].RawDescriptor, replacement.Columns[1].RawDescriptor);
        object[] second = [DBNull.Value, DbDefault.Value];
        _ = await registry.ApplyAsync("T", replacement, second, TestContext.Current.CancellationToken);
        Assert.Equal<object>([31, 99], second);

        // The new column rule and table rule each refuse, and neither consumes an ID.
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
            await registry.ApplyAsync("T", replacement, [DBNull.Value, 5], TestContext.Current.CancellationToken));
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
            await registry.ApplyAsync("T", replacement, [DBNull.Value, 101], TestContext.Current.CancellationToken));
        object[] afterRefusal = [DBNull.Value, DbDefault.Value];
        _ = await registry.ApplyAsync("T", replacement, afterRefusal, TestContext.Current.CancellationToken);
        Assert.Equal<object>([32, 99], afterRefusal);

        registry.Restore(snapshot);
        object[] restored = [DBNull.Value, DbDefault.Value];
        _ = await registry.ApplyAsync("T", original, restored, TestContext.Current.CancellationToken);
        Assert.Equal<object>([12, 7], restored);
        Assert.Equal(2, seedReads);
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
            await registry.ApplyUpdateAsync("T", original, [12, 9], [1], TestContext.Current.CancellationToken));
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
            await registry.ApplyUpdateAsync("T", original, [12, 15], [1], TestContext.Current.CancellationToken));
        _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
            await registry.ApplyUpdateAsync("T", original, [12, DBNull.Value], [1], TestContext.Current.CancellationToken));
    }

    /// <summary>An unconstrained declaration at a reused location must clear all earlier constraints.</summary>
    /// <param name="registerReplacement">Whether the replacement is declared by this writer or hydrated.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdenticalPhysicalReplacement_WithoutConstraints_DropsOldDefaultsAndRules(bool registerReplacement)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        ColumnPropertyBlock? properties = BuildProperties("7", ">= 0", "[Score] <= 10");
        var registry = new ConstraintRegistry((_, _) => ValueTask.FromResult<ColumnPropertyBlock?>(properties));
        registry.Register(
            "T",
            [
                new ColumnDefinition("Score", typeof(int))
                {
                    DefaultValue = 7,
                    IsNullable = false,
                    ValidationRule = value => value is int score && score <= 8,
                    ValidationRuleExpression = ">= 0",
                },
            ],
            format,
            tdefPageNumber: 10);
        TableDef original = CreateDefinition(includeAutoNumber: false);
        object[] first = [DbDefault.Value];
        _ = await registry.ApplyAsync("T", original, first, TestContext.Current.CancellationToken);
        Assert.Equal(7, first[0]);
        ConstraintRegistrySnapshot snapshot = registry.CaptureSnapshot();

        registry.Unregister("T");
        properties = null;
        if (registerReplacement)
        {
            registry.Register("T", [new ColumnDefinition("Score", typeof(int))], format, tdefPageNumber: 10);
        }

        TableDef replacement = CreateDefinition(includeAutoNumber: false);
        object[] missing = [DbDefault.Value];
        _ = await registry.ApplyAsync("T", replacement, missing, TestContext.Current.CancellationToken);
        Assert.Equal(DBNull.Value, missing[0]);
        _ = await registry.ApplyAsync("T", replacement, [-1], TestContext.Current.CancellationToken);
        await registry.ApplyUpdateAsync("T", replacement, [99], [0], TestContext.Current.CancellationToken);

        registry.Restore(snapshot);
        object[] restored = [DbDefault.Value];
        _ = await registry.ApplyAsync("T", original, restored, TestContext.Current.CancellationToken);
        Assert.Equal(7, restored[0]);
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
            await registry.ApplyUpdateAsync("T", original, [-1], [0], TestContext.Current.CancellationToken));
    }

    private static TableDef CreateDefinition(bool includeAutoNumber)
    {
        // These registry fixtures deliberately recreate the same location and descriptor bytes.
        ColumnInfo score = new(new byte[25])
        {
            Name = "Score",
            Type = ColumnType.LongIntegerType,
            ColNum = includeAutoNumber ? 1 : 0,
            FixedOff = includeAutoNumber ? 4 : 0,
            Size = 4,
        };
        return new TableDef
        {
            TDefPageNumber = 10,
            Columns = includeAutoNumber
                ? [new ColumnInfo(new byte[25]) { Name = "Id", Type = ColumnType.LongIntegerType, Flags = Constants.ColumnDescriptorFlags.AutoNumber, Size = 4 }, score]
                : [score],
        };
    }

    private static ColumnPropertyBlock BuildProperties(string defaultValue, string columnRule, string tableRule)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        var builder = new ColumnPropertyBlockBuilder();
        ColumnPropertyTargetBuilder column = builder.GetOrAddTarget("Score");
        column.AddText(Constants.ColumnPropertyNames.DefaultValue, defaultValue, format);
        column.AddText(Constants.ColumnPropertyNames.ValidationRule, columnRule, format);
        builder.GetOrAddTableTarget().AddText(Constants.ColumnPropertyNames.ValidationRule, tableRule, format);
        return ColumnPropertyBlock.Parse(builder.ToBytes(format), format)!;
    }
}
