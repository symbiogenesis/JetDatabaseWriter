namespace JetDatabaseWriter.Tests.Schema;

using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class ParserRefusalConstraintTests
{
    /// <summary>The cached unsupported plan refuses repeatedly without consuming a seeded identity.</summary>
    /// <returns>The asynchronous test.</returns>
    [Fact]
    public async Task ApplyAsync_UnsupportedDefault_KeepsCachedPlanAndAutoNumber()
    {
        int seedReads = 0;
        var registry = new ConstraintRegistry(
            readUsedAutoNumberHighWater: (_, _, _, _) =>
            {
                seedReads++;
                return ValueTask.FromResult(7L);
            });
        registry.Register(
            "T",
            [
                new("Id", typeof(int)) { IsAutoIncrement = true },
                new("Score", typeof(int)) { DefaultValueExpression = "1 +" },
            ],
            JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb),
            tdefPageNumber: 10);
        TableDef tableDef = new()
        {
            TDefPageNumber = 10,
            Columns =
            [
                new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType, Flags = Constants.ColumnDescriptorFlags.AutoNumber, ColNum = 0 },
                new ColumnInfo { Name = "Score", Type = ColumnType.LongIntegerType, ColNum = 1 },
            ],
        };

        object[] first = [DbDefault.Value, 1];
        _ = await registry.ApplyAsync("T", tableDef, first, TestContext.Current.CancellationToken);
        Assert.Equal(8, first[0]);
        ConstraintRegistrySnapshot before = registry.CaptureSnapshot();
        ColumnConstraint counter = before.Tables["T"][0];
        ColumnConstraint defaultColumn = before.Tables["T"][1];
        Assert.Equal(9L, counter.NextAutoValue);
        Assert.Null(defaultColumn.DefaultValuePlan);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
                await registry.ApplyAsync("T", tableDef, [DbDefault.Value, DbDefault.Value], TestContext.Current.CancellationToken));
            ConstraintRegistrySnapshot refused = registry.CaptureSnapshot();
            Assert.Same(counter, refused.Tables["T"][0]);
            Assert.Same(defaultColumn, refused.Tables["T"][1]);
            Assert.Same(ColumnDefaultValue.Unsupported, defaultColumn.DefaultValuePlan);
            Assert.Equal(9L, counter.NextAutoValue);
            Assert.Equal(1, seedReads);
        }

        object[] next = [DbDefault.Value, 1];
        _ = await registry.ApplyAsync("T", tableDef, next, TestContext.Current.CancellationToken);
        Assert.Equal(9, next[0]);
        Assert.Equal(10L, counter.NextAutoValue);
        Assert.Equal(1, seedReads);
        Assert.Same(ColumnDefaultValue.Unsupported, defaultColumn.DefaultValuePlan);
    }

    /// <summary>A parser-unsupported stored rule preserves a previously seeded cache and its physical identity.</summary>
    /// <returns>The asynchronous test.</returns>
    [Fact]
    public async Task ApplyAsync_UnsupportedStoredRule_KeepsSeededCounterAndCachedPlan()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        var propertiesBuilder = new ColumnPropertyBlockBuilder();
        propertiesBuilder.GetOrAddTarget("Score").AddText(Constants.ColumnPropertyNames.ValidationRule, ">= And", format);
        ColumnPropertyBlock properties = ColumnPropertyBlock.Parse(propertiesBuilder.ToBytes(format), format)!;
        int seedReads = 0;
        var registry = new ConstraintRegistry(
            readLvPropForTable: (_, _) => ValueTask.FromResult<ColumnPropertyBlock?>(properties),
            readUsedAutoNumberHighWater: (_, _, _, _) =>
            {
                seedReads++;
                return ValueTask.FromResult(7L);
            });
        TableDef tableDef = new()
        {
            TDefPageNumber = 10,
            Columns =
            [
                new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType, Flags = Constants.ColumnDescriptorFlags.AutoNumber, ColNum = 0 },
                new ColumnInfo { Name = "Score", Type = ColumnType.LongIntegerType, ColNum = 1 },
            ],
        };

        // An Id-only update binds the cached metadata without evaluating the unassigned rule.
        await registry.ApplyUpdateAsync("T", tableDef, [8, 1], [0], TestContext.Current.CancellationToken);
        ConstraintRegistrySnapshot bound = registry.CaptureSnapshot();
        ColumnConstraint counter = bound.Tables["T"][0];
        ColumnConstraint ruleColumn = bound.Tables["T"][1];

        // No insert can bypass this rule. Seed the cache fixture directly, keeping the schema unchanged.
        counter.NextAutoValue = 9;
        ConstraintRegistrySnapshot before = registry.CaptureSnapshot();
        Assert.Null(ruleColumn.ValidationRulePlan);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            object[] values = [DbDefault.Value, 1];
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
                await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken));
            Assert.Equal(9, values[0]);
            ConstraintRegistrySnapshot refused = registry.CaptureSnapshot();
            Assert.Same(counter, refused.Tables["T"][0]);
            Assert.Same(ruleColumn, refused.Tables["T"][1]);
            Assert.Same(ColumnValidationRule.Unsupported, ruleColumn.ValidationRulePlan);
            Assert.Equal(9L, counter.NextAutoValue);
            Assert.Equal(0, seedReads);

            await registry.ApplyUpdateAsync("T", tableDef, [8, 1], [0], TestContext.Current.CancellationToken);
            Assert.Same(counter, registry.CaptureSnapshot().Tables["T"][0]);
        }

        registry.Restore(before);
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
            await registry.ApplyAsync("T", tableDef, [DbDefault.Value, 1], TestContext.Current.CancellationToken));
        Assert.Same(counter, registry.CaptureSnapshot().Tables["T"][0]);
        Assert.Same(ruleColumn, registry.CaptureSnapshot().Tables["T"][1]);
        Assert.Equal(9L, counter.NextAutoValue);
        Assert.Equal(0, seedReads);
    }
}
