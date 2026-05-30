namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class ConstraintRegistryTests
{
    [Fact]
    public async Task ApplyCalculatedAsync_HydratedCalculatedColumn_UsesPersistedResultTypeClrProjection()
    {
        var tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Score", Type = ColumnType.LongIntegerType },
                new ColumnInfo
                {
                    Name = "IsHigh",
                    Type = ColumnType.LongIntegerType,
                    ExtraFlags = Constants.CalculatedColumn.ExtFlagMask,
                },
            ],
        };

        ColumnPropertyBlock properties = BuildCalculatedColumnProperties(
            "IsHigh",
            ColumnType.BooleanType,
            "[Score] >= 10");
        var registry = new ConstraintRegistry(
            static (_, _) => ValueTask.FromResult(new DataTable()),
            (_, _) => ValueTask.FromResult<ColumnPropertyBlock?>(properties));
        object[] values = [12, DBNull.Value];

        await registry.ApplyCalculatedAsync("Calc", tableDef, values, force: false, TestContext.Current.CancellationToken);

        bool isHigh = Assert.IsType<bool>(values[1]);
        Assert.True(isHigh);
    }

    private static ColumnPropertyBlock BuildCalculatedColumnProperties(
        string columnName,
        ColumnType resultType,
        string expression)
    {
        var builder = new ColumnPropertyBlockBuilder();
        ColumnPropertyBlockBuilder.TargetBuilder target = builder.GetOrAddTarget(columnName);
        target.AddMemoText(Constants.ColumnPropertyNames.Expression, expression, DatabaseFormat.AceAccdb);
        target.AddByte(Constants.ColumnPropertyNames.ResultType, (byte)resultType);

        return ColumnPropertyBlock.Parse(builder.ToBytes(DatabaseFormat.AceAccdb), DatabaseFormat.AceAccdb)!;
    }
}
