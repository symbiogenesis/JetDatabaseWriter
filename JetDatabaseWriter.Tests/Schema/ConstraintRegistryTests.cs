namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.Globalization;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class ConstraintRegistryTests
{
    /// <summary>
    /// Date-column rule cases. <c>Date()</c> reads the local clock; dates two days either
    /// side of today's UTC date are on the same side of the local date.
    /// </summary>
    /// <returns>The rule, the value, and whether the rule accepts it.</returns>
    public static TheoryData<string, DateTime, bool> DateValidationRuleCases()
    {
        DateTime today = DateTime.UtcNow.Date;
        return new TheoryData<string, DateTime, bool>
        {
            { "<=Date()", today.AddDays(-2), true },
            { "<=Date()", today.AddDays(2), false },
            { ">=#2000-01-01#", new DateTime(1999, 12, 31), false },
            { ">=#2000-01-01#", new DateTime(2000, 1, 1), true },
        };
    }

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

    [Theory]
    [InlineData(">=0 And <=100", 50, true)]
    [InlineData(">=0 And <=100", -1, false)]
    [InlineData(">=0 And <=100", 101, false)]
    [InlineData(">=0 And <=100", null, true)]
    [InlineData("Is Not Null", null, false)]
    [InlineData("Is Not Null", 5, true)]
    [InlineData("Is Null Or >5", null, true)]
    [InlineData("Is Null Or >5", 3, false)]
    [InlineData("Between 1 And 10", 10, true)]
    [InlineData("Between 1 And 10", 11, false)]
    [InlineData("Between 1 And 10 Or 99", 99, true)]
    [InlineData("Not Between 1 And 10", 5, false)]
    [InlineData("In (1,2,3)", 2, true)]
    [InlineData("In (1,2,3)", 4, false)]
    [InlineData("Not In (1,2)", 1, false)]
    [InlineData("<>0", 0, false)]
    [InlineData("<>0", 7, true)]
    [InlineData("0 Or >100", 0, true)]
    [InlineData("0 Or >100", 50, false)]
    [InlineData("0 Or >100", 101, true)]
    [InlineData("Not 5", 5, false)]
    [InlineData("Not 5", 6, true)]
    [InlineData("(>=0 And <=10) Or 99", 99, true)]
    [InlineData("(>=0 And <=10) Or 99", 50, false)]
    [InlineData(">=0 and <=100", 101, false)]
    [InlineData("[Score] >= 0", -1, false)]
    public async Task ApplyAsync_HydratedValidationRule_NumberColumn_SuppliesTheImplicitOperand(string rule, int? value, bool accepted)
        => await AssertRuleAsync(rule, ColumnType.LongIntegerType, value, accepted);

    [Theory]
    [InlineData("Like \"A*\"", "Apple", true)]
    [InlineData("Like \"A*\"", "Banana", false)]
    [InlineData("Not Like \"A*\"", "Apple", false)]
    [InlineData("Like 'A*'", "Banana", false)]
    [InlineData("\"M\" Or \"F\"", "f", true)]
    [InlineData("\"M\" Or \"F\"", "X", false)]
    [InlineData("<>\"\"", "", false)]
    [InlineData("Len([Score]) = 3", "abcd", false)]
    [InlineData("Len([Score]) = 3", "abc", true)]
    public async Task ApplyAsync_HydratedValidationRule_TextColumn_SuppliesTheImplicitOperand(string rule, string? value, bool accepted)
        => await AssertRuleAsync(rule, ColumnType.TextType, value, accepted);

    [Theory]
    [MemberData(nameof(DateValidationRuleCases))]
    public async Task ApplyAsync_HydratedValidationRule_DateColumn_SuppliesTheImplicitOperand(string rule, DateTime value, bool accepted)
        => await AssertRuleAsync(rule, ColumnType.DateTimeType, value, accepted);

    [Theory]
    [InlineData("MyFunction([Score]) > 0")]
    [InlineData("DLookUp(\"Id\",\"Other\") > 0")]
    [InlineData(">= And")]
    [InlineData("((>0)")]
    public async Task ApplyAsync_ValidationRuleThisLibraryCannotEvaluate_IsNotEnforced(string rule)
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.ValidationRule, rule)));
        object[] values = [-1];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(-1, values[0]);
    }

    /// <summary>
    /// A persisted default is evaluated and converted to the column's CLR type. The
    /// expected value is given as its type and invariant text so the data serializes.
    /// </summary>
    /// <param name="expression">The persisted DefaultValue expression.</param>
    /// <param name="type">The column type.</param>
    /// <param name="expectedType">The CLR type the default must have.</param>
    /// <param name="expectedText">The default's invariant text.</param>
    [Theory]
    [InlineData("7", ColumnType.LongIntegerType, typeof(int), "7")]
    [InlineData("=7", ColumnType.LongIntegerType, typeof(int), "7")]
    [InlineData("-3", ColumnType.IntegerType, typeof(short), "-3")]
    [InlineData("1+2", ColumnType.LongIntegerType, typeof(int), "3")]
    [InlineData("12.5", ColumnType.DoubleType, typeof(double), "12.5")]
    [InlineData("1234567890.123456789", ColumnType.NumericType, typeof(decimal), "1234567890.123456789")]
    [InlineData("0", ColumnType.MoneyType, typeof(decimal), "0")]
    [InlineData("\"hi\"", ColumnType.TextType, typeof(string), "hi")]
    [InlineData("\"say \"\"hi\"\"\"", ColumnType.TextType, typeof(string), "say \"hi\"")]
    [InlineData("True", ColumnType.BooleanType, typeof(bool), "True")]
    [InlineData("No", ColumnType.BooleanType, typeof(bool), "False")]
    [InlineData("=No", ColumnType.BooleanType, typeof(bool), "False")]
    [InlineData("#2020-01-31#", ColumnType.DateTimeType, typeof(DateTime), "2020-01-31 00:00:00")]
    [InlineData("#2026-04-24 13:05:30#", ColumnType.DateTimeType, typeof(DateTime), "2026-04-24 13:05:30")]
    [InlineData("{guid 12345678-1234-1234-1234-1234567890ab}", ColumnType.GuidType, typeof(Guid), "12345678-1234-1234-1234-1234567890ab")]
    [InlineData("{guid {12345678-1234-1234-1234-1234567890AB}}", ColumnType.GuidType, typeof(Guid), "12345678-1234-1234-1234-1234567890ab")]
    public async Task ApplyAsync_HydratedDefaultValue_FillsNull(string expression, ColumnType type, Type expectedType, string expectedText)
    {
        TableDef tableDef = SingleColumnTable(type);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, expression)));
        object[] values = [DBNull.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.IsType(expectedType, values[0]);
        string actualText = values[0] switch
        {
            DateTime dateTime => dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Guid guid => guid.ToString("D", CultureInfo.InvariantCulture),
            _ => Convert.ToString(values[0], CultureInfo.InvariantCulture)!,
        };
        Assert.Equal(expectedText, actualText);
    }

    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_DateFunctionsUseTheClock()
    {
        var tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Today", Type = ColumnType.DateTimeType },
                new ColumnInfo { Name = "Stamp", Type = ColumnType.DateTimeType },
            ],
        };
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties(
            ("Today", Constants.ColumnPropertyNames.DefaultValue, "Date()"),
            ("Stamp", Constants.ColumnPropertyNames.DefaultValue, "=Now()")));
        object[] values = [DBNull.Value, DBNull.Value];

        // Access Date() and Now() read the local clock.
        DateTime before = DateTimeOffset.Now.DateTime;
        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);
        DateTime after = DateTimeOffset.Now.DateTime;

        Assert.Equal(before.Date, Assert.IsType<DateTime>(values[0]).Date);
        DateTime stamp = Assert.IsType<DateTime>(values[1]);
        Assert.InRange(stamp, before.AddSeconds(-1), after.AddSeconds(1));
    }

    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_DoesNotReplaceSuppliedValue()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, "7")));
        object[] values = [3];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(3, values[0]);
    }

    [Theory]
    [InlineData("=CurrentUser()")]
    [InlineData("GenUniqueID()")]
    [InlineData("\"unterminated")]
    [InlineData("\"text\"")]
    public async Task ApplyAsync_DefaultValueThisLibraryCannotEvaluate_LeavesNull(string expression)
    {
        // "text" cannot become a Long Integer, so it is skipped like an unknown function.
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, expression)));
        object[] values = [DBNull.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(DBNull.Value, values[0]);
    }

    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_IsCheckedByTheValidationRule()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties(
            ("Score", Constants.ColumnPropertyNames.DefaultValue, "-1"),
            ("Score", Constants.ColumnPropertyNames.ValidationRule, ">=0")));

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await registry.ApplyAsync("T", tableDef, [DBNull.Value], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ApplyUpdateAsync_HydratedValidationRule_ChecksOnlyAssignedColumns()
    {
        var tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "A", Type = ColumnType.LongIntegerType },
                new ColumnInfo { Name = "B", Type = ColumnType.LongIntegerType },
            ],
        };
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties(
            ("A", Constants.ColumnPropertyNames.ValidationRule, ">=0"),
            ("B", Constants.ColumnPropertyNames.ValidationRule, ">=0")));

        // B already violates its rule; an update that assigns only A is still checked only on A.
        await registry.ApplyUpdateAsync("T", tableDef, [5, -1], [0], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await registry.ApplyUpdateAsync("T", tableDef, [-5, 1], [0], TestContext.Current.CancellationToken));
    }

    private static async Task AssertRuleAsync(string rule, ColumnType type, object? value, bool accepted)
    {
        TableDef tableDef = SingleColumnTable(type);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.ValidationRule, rule)));
        object[] values = [value ?? DBNull.Value];

        if (accepted)
        {
            _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);
        }
        else
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken));
            Assert.Contains(rule, ex.Message, StringComparison.Ordinal);
        }
    }

    private static TableDef SingleColumnTable(ColumnType type) => new()
    {
        Columns = [new ColumnInfo { Name = "Score", Type = type }],
    };

    private static ConstraintRegistry RegistryWithProperties(ColumnPropertyBlock properties) => new(
        static (_, _) => ValueTask.FromResult(new DataTable()),
        (_, _) => ValueTask.FromResult<ColumnPropertyBlock?>(properties));

    private static ColumnPropertyBlock BuildColumnProperties(string columnName, (string Name, string Value) property)
        => BuildColumnProperties((columnName, property.Name, property.Value));

    private static ColumnPropertyBlock BuildColumnProperties(params (string Column, string Name, string Value)[] properties)
    {
        var builder = new ColumnPropertyBlockBuilder();
        foreach ((string column, string name, string value) in properties)
        {
            builder.GetOrAddTarget(column).AddText(name, value, DatabaseFormat.AceAccdb);
        }

        return ColumnPropertyBlock.Parse(builder.ToBytes(DatabaseFormat.AceAccdb), DatabaseFormat.AceAccdb)!;
    }

    private static ColumnPropertyBlock BuildCalculatedColumnProperties(
        string columnName,
        ColumnType resultType,
        string expression)
    {
        var builder = new ColumnPropertyBlockBuilder();
        ColumnPropertyTargetBuilder target = builder.GetOrAddTarget(columnName);
        target.AddMemoText(Constants.ColumnPropertyNames.Expression, expression, DatabaseFormat.AceAccdb);
        target.AddByte(Constants.ColumnPropertyNames.ResultType, (byte)resultType);

        return ColumnPropertyBlock.Parse(builder.ToBytes(DatabaseFormat.AceAccdb), DatabaseFormat.AceAccdb)!;
    }
}
