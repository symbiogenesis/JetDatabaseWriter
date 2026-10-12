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
}
