namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Native string functions use English Boolean words while concatenation follows regional settings.</summary>
public sealed class CalculatedBooleanFunctionTextTests
{
    /// <summary>Gets function controls independently captured by the native regional profile workflow.</summary>
    public static TheoryData<string, string, string> NativeFunctionText { get; } = CreateNativeFunctionText();

    [Theory]
    [MemberData(nameof(NativeFunctionText))]
    public void StringFunctions_RetainEnglishBooleanWords(string cultureName, string expression, string expected)
        => Assert.Equal(expected, Evaluate(expression, cultureName));

    [Theory]
    [InlineData("en-US", "True", "False")]
    [InlineData("fr-FR", "Vrai", "Faux")]
    [InlineData("de-DE", "Wahr", "Falsch")]
    [InlineData("tr-TR", "Doğru", "Yanlış")]
    [InlineData("ja-JP", "True", "False")]
    [InlineData("ar-SA", "True", "False")]
    public void Concatenation_RetainsIndependentRegionalBooleanWords(string cultureName, string trueText, string falseText)
    {
        Assert.Equal(trueText, Evaluate("IsNull(Null)&\"\"", cultureName));
        Assert.Equal(falseText, Evaluate("IsEmpty(1)&\"\"", cultureName));
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("fr-FR")]
    [InlineData("de-DE")]
    [InlineData("tr-TR")]
    [InlineData("ja-JP")]
    [InlineData("ar-SA")]
    public async Task NativeCaptures_FunctionCachesAndRegionalConcatenationRemainDistinct(string cultureName)
    {
        foreach (string fixture in new[] { "native-profile.accdb", "native-profile-compacted.accdb" })
        {
            string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedRegionalProfiles", cultureName, fixture);
            byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            object[] expected;
            await using (AccessReader native = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                expected = Assert.Single(await native.Rows("RegionalBooleanComparisons", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
                object[] concatenated = Assert.Single(await native.Rows("RegionalNumbers", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
                IReadOnlyList<ColumnMetadata> columns = await native.GetColumnMetadataAsync("RegionalNumbers", TestContext.Current.CancellationToken);
                int trueOrdinal = Enumerable.Range(0, columns.Count).Single(index => columns[index].Name == "IntrinsicTrueText");
                int falseOrdinal = Enumerable.Range(0, columns.Count).Single(index => columns[index].Name == "IntrinsicFalseText");
                Assert.Equal(concatenated[trueOrdinal], Evaluate("IsNull(Null)&\"\"", cultureName));
                Assert.Equal(concatenated[falseOrdinal], Evaluate("IsEmpty(1)&\"\"", cultureName));
            }

            await using var stream = new MemoryStream();
            await stream.WriteAsync(bytes, TestContext.Current.CancellationToken);
            await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = CultureInfo.GetCultureInfo(cultureName) }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
            {
                object?[] inserted = new object?[expected.Length];
                Array.Copy(expected, inserted, 4);
                await writer.InsertRowAsync("RegionalBooleanComparisons", inserted, TestContext.Current.CancellationToken);
                Assert.Equal(2, await writer.UpdateRowsAsync("RegionalBooleanComparisons", "Id", expected[0], new Dictionary<string, object?> { ["Id"] = expected[0] }, TestContext.Current.CancellationToken));
            }

            await using AccessReader actual = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            object[][] rows = await actual.Rows("RegionalBooleanComparisons", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
            Assert.Equal(2, rows.Length);
            Assert.All(rows, row => Assert.Equal(expected, row));
        }
    }

    private static TheoryData<string, string, string> CreateNativeFunctionText()
    {
        (string Expression, string Expected)[] cases =
        [
            ("IsNull(Null) Like \"True\"", "-1"),
            ("IsNull(Null) Like \"-1\"", "0"),
            ("Left(IsNull(Null),1)", "T"),
            ("Len(IsNull(Null))", "4"),
            ("Str(IsNull(Null))", "True"),
            ("Str(IsEmpty(1))", "False"),
        ];
        var data = new TheoryData<string, string, string>();
        foreach (string culture in new[] { "en-US", "fr-FR", "de-DE", "tr-TR", "ja-JP", "ar-SA" })
        {
            foreach ((string expression, string expected) in cases)
            {
                data.Add(culture, expression, expected);
            }
        }

        return data;
    }

    private static object Evaluate(string expression, string cultureName)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "V", Type = ColumnType.TextType, ExtraFlags = 0xC0, CalculatedResultType = ColumnType.TextType }] };
        ColumnConstraint[] constraints = [new() { Name = "V", IsCalculated = true, ClrType = typeof(string), CalculatedResultType = ColumnType.TextType, CalculationExpression = expression }];
        return new CalculatedExpressionEvaluationContext(table, constraints, [DBNull.Value], calculatedCulture: CultureInfo.GetCultureInfo(cultureName)).EvaluateColumn(0);
    }
}
