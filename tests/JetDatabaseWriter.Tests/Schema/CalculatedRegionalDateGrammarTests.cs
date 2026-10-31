namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Globalization;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Matches independent native month-name and date-unit controls in CalculatedRegionalProfiles.</summary>
public sealed class CalculatedRegionalDateGrammarTests
{
    [Theory]
    [InlineData("en-US", 4d, 2020d, null, null, null)]
    [InlineData("fr-FR", 4d, 2020d, null, null, null)]
    [InlineData("de-DE", 4d, 2020d, 3d, null, null)]
    [InlineData("tr-TR", null, null, 3d, null, null)]
    [InlineData("ja-JP", 4d, 2020d, null, null, 4d)]
    [InlineData("ar-SA", null, null, null, null, null)]
    public void NativeDateGrammar_UsesTheRegionalMonthAliasesAndDateUnits(string cultureName, double? januaryDay, double? septemberYear, double? dotDay, double? japaneseMonthDay, double? japaneseUnitDay)
    {
        string[] names = ["January", "September", "Dot", "JapaneseMonth", "JapaneseUnits"];
        string[] expressions = ["Day(\"Jan. 4, 2020\")", "Year(\"4 Sept. 2020\")", "Day(\"03.04.2020\")", "Day(\"4 3月 2020\")", "Day(\"2020年 3月4日\")"];
        double?[] expected = [januaryDay, septemberYear, dotDay, japaneseMonthDay, japaneseUnitDay];
        var table = new TableDef { Columns = [.. names.Select(static name => new ColumnInfo { Name = name, Type = ColumnType.DoubleType })] };
        ColumnConstraint[] constraints =
        [
            .. expressions.Select((expression, index) => new ColumnConstraint
            {
                Name = names[index],
                ClrType = typeof(double),
                IsCalculated = true,
                CalculatedResultType = ColumnType.DoubleType,
                CalculationExpression = expression,
            }),
        ];
        CultureInfo culture = CalculatedExpressionRegionalProfile.Snapshot(CultureInfo.GetCultureInfo(cultureName));
        var context = new CalculatedExpressionEvaluationContext(table, constraints, Enumerable.Repeat<object>(DBNull.Value, names.Length).ToArray(), calculatedCulture: culture);
        for (int index = 0; index < expected.Length; index++)
        {
            object actual = context.EvaluateColumn(index);
            if (expected[index] is double value)
            {
                Assert.Equal(value, Assert.IsType<double>(actual));
            }
            else
            {
                Assert.Equal(13, Assert.IsType<CalculatedError>(actual).NativeCode);
            }
        }
    }
}
