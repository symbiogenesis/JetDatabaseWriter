namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class CalculatedColumnWriteTests
{
    [Fact]
    public async Task CreateTable_CalculatedColumns_RoundTripsMetadataAndCachedValues()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        DateTime eventDate = new(2025, 2, 3, 4, 5, 6);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcRoundTrip",
                [
                    new("Score", typeof(int)),
                    new("Label", typeof(string), maxLength: 40),
                    new("CalcLabel", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Label] & \" #\" & [Score]",
                    },
                    new("IsHigh", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Score] >= 10",
                    },
                    new("NextScore", typeof(int))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Score] + 1",
                    },
                    new("Weighted", typeof(decimal))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Score] * 1.25",
                    },
                    new("EventDate", typeof(DateTime))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Date()",
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcRoundTrip",
                [9, "Alpha", "Alpha #9", false, 10, 11.25m, eventDate],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("CalcRoundTrip", TestContext.Current.CancellationToken);

        ColumnMetadata calcLabel = Assert.Single(metadata, c => c.Name == "CalcLabel");
        Assert.True(calcLabel.IsCalculated);
        Assert.Equal("[Label] & \" #\" & [Score]", calcLabel.CalculationExpression);
        Assert.Equal(0x0A, calcLabel.CalculatedResultType);
        Assert.Equal(160, calcLabel.MaxLength);

        Assert.Equal(0x01, Assert.Single(metadata, c => c.Name == "IsHigh").CalculatedResultType);
        Assert.Equal(0x04, Assert.Single(metadata, c => c.Name == "NextScore").CalculatedResultType);
        Assert.Equal(0x10, Assert.Single(metadata, c => c.Name == "Weighted").CalculatedResultType);

        DataTable table = await reader.ReadDataTableAsync("CalcRoundTrip", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, table.Rows.Count);

        DataRow row = table.Rows[0];
        Assert.Equal("Alpha #9", row["CalcLabel"]);
        Assert.False(Convert.ToBoolean(row["IsHigh"], CultureInfo.InvariantCulture));
        Assert.Equal(10, Convert.ToInt32(row["NextScore"], CultureInfo.InvariantCulture));
        Assert.Equal(11.25m, Convert.ToDecimal(row["Weighted"], CultureInfo.InvariantCulture));
        Assert.Equal(eventDate, Convert.ToDateTime(row["EventDate"], CultureInfo.InvariantCulture));

        IReadOnlyList<CalculatedProjection> typed = await reader.ReadTableAsync<CalculatedProjection>(
            "CalcRoundTrip",
            maxRows: 10,
            TestContext.Current.CancellationToken);
        CalculatedProjection item = Assert.Single(typed);
        Assert.Equal("Alpha #9", item.CalcLabel);
        Assert.False(item.IsHigh);
        Assert.Equal(10, item.NextScore);
        Assert.Equal(11.25m, item.Weighted);
    }

    [Fact]
    public async Task CreateTable_CalculatedMemoOverInlineLimit_RoundTripsCachedValue()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        string memo = new('A', 1200);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcMemo",
                [
                    new("Id", typeof(int)),
                    new("ComputedMemo", typeof(string))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Id] & \" memo\"",
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("CalcMemo", [1, memo], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcMemo", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, table.Rows.Count);
        Assert.Equal(memo, table.Rows[0]["ComputedMemo"]);
    }

    [Fact]
    public async Task InsertRow_CalculatedColumns_EvaluatesMissingCachedValues()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcEval",
                [
                    new("Score", typeof(int)),
                    new("Label", typeof(string), maxLength: 40),
                    new("SafeLabel", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Nz([Label], \"missing\")",
                    },
                    new("CalcLabel", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "[SafeLabel] & \" #\" & [Score]",
                    },
                    new("IsHigh", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "IIf([Score] >= 10, TRUE, FALSE)",
                    },
                    new("NextScore", typeof(int))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Score] + 1",
                    },
                    new("Weighted", typeof(decimal))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Round([Score] * 1.25, 2)",
                    },
                    new("LabelCode", typeof(string), maxLength: 20)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Left([Label], 2) & CStr(Len([Label]))",
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcEval",
                [12, "Alpha", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcEval", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(table.AsEnumerable());

        Assert.Equal("Alpha", row["SafeLabel"]);
        Assert.Equal("Alpha #12", row["CalcLabel"]);
        Assert.True(Convert.ToBoolean(row["IsHigh"], CultureInfo.InvariantCulture));
        Assert.Equal(13, Convert.ToInt32(row["NextScore"], CultureInfo.InvariantCulture));
        Assert.Equal(15.00m, Convert.ToDecimal(row["Weighted"], CultureInfo.InvariantCulture));
        Assert.Equal("Al5", row["LabelCode"]);
    }

    [Fact]
    public async Task UpdateRows_RecomputesCalculatedColumns()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcUpdate",
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)),
                    new("Label", typeof(string), maxLength: 40),
                    new("CalcLabel", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Label] & \" #\" & [Score]",
                    },
                    new("IsHigh", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Score] >= 10",
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcUpdate",
                [1, 12, "Alpha", DBNull.Value, DBNull.Value],
                TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                "CalcUpdate",
                "Id",
                1,
                new Dictionary<string, object?>
                {
                    ["Score"] = 3,
                    ["Label"] = "Beta",
                },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcUpdate", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(table.AsEnumerable());

        Assert.Equal("Beta #3", row["CalcLabel"]);
        Assert.False(Convert.ToBoolean(row["IsHigh"], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task InsertRowPoco_CalculatedColumnsCanBeOmitted()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcPoco",
                [
                    new("Score", typeof(int)),
                    new("Label", typeof(string), maxLength: 40),
                    new("CalcLabel", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Label] & \" #\" & [Score]",
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcPoco",
                new SourceProjection { Score = 7, Label = "Gamma" },
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcPoco", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(table.AsEnumerable());

        Assert.Equal("Gamma #7", row["CalcLabel"]);
    }

    [Fact]
    public async Task InsertRow_CalculatedColumns_EvaluatesAccessOperatorsAndBuiltins()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcAccessSyntax",
                [
                    new("Score", typeof(int)),
                    new("Label", typeof(string), maxLength: 40),
                    new("Code", typeof(string), maxLength: 20),
                    new("IsEven", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Score] Mod 2 = 0",
                    },
                    new("MatchesLabel", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Label] Like \"Al*\"",
                    },
                    new("ScoreBand", typeof(string), maxLength: 20)
                    {
                        IsCalculated = true,
                        CalculationExpression = "IIf([Score] Between 10 And 20, \"Mid\", \"Other\")",
                    },
                    new("IsKnown", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Label] In (\"Alpha\", \"Beta\")",
                    },
                    new("LogicGate", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Not ([Score] < 10) And [Label] Like \"A*\"",
                    },
                    new("NullState", typeof(string), maxLength: 20)
                    {
                        IsCalculated = true,
                        CalculationExpression = "IIf([Code] Is Null, \"missing\", [Code])",
                    },
                    new("IntDiv", typeof(int))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Score] \\ 5",
                    },
                    new("FunctionText", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Replace(UCase([Label]), \"A\", \"@\") & \"-\" & CStr(DatePart(\"yyyy\", DateSerial(2025, 2, 3)))",
                    },
                    new("AtnValue", typeof(double))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Atn(1)",
                    },
                    new("AliasDate", typeof(DateTime))
                    {
                        IsCalculated = true,
                        CalculationExpression = "CVDate(\"2025-02-03\")",
                    },
                    new("TypeInfo", typeof(string), maxLength: 40)
                    {
                        IsCalculated = true,
                        CalculationExpression = "TypeName([Label]) & \":\" & CStr(VarType([Score]))",
                    },
                    new("StringAliases", typeof(string), maxLength: 40)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Left$([Label], 2) & \"-\" & UCase$(Right$([Label], 2))",
                    },
                    new("CaseConv", typeof(string), maxLength: 40)
                    {
                        IsCalculated = true,
                        CalculationExpression = "StrConv([Label], vbUpperCase)",
                    },
                    new("CompareConstant", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "StrComp([Label], \"alpha\", vbTextCompare) = 0",
                    },
                    new("RandomValue", typeof(double))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Rnd()",
                    },
                    new("SeededRandom", typeof(double))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Rnd(-7)",
                    },
                    new("RepeatedRandom", typeof(double))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Rnd(0)",
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcAccessSyntax",
                [
                    12,
                    "Alpha",
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcAccessSyntax", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());

        Assert.True(Convert.ToBoolean(row["IsEven"], CultureInfo.InvariantCulture));
        Assert.True(Convert.ToBoolean(row["MatchesLabel"], CultureInfo.InvariantCulture));
        Assert.Equal("Mid", row["ScoreBand"]);
        Assert.True(Convert.ToBoolean(row["IsKnown"], CultureInfo.InvariantCulture));
        Assert.True(Convert.ToBoolean(row["LogicGate"], CultureInfo.InvariantCulture));
        Assert.Equal("missing", row["NullState"]);
        Assert.Equal(2, Convert.ToInt32(row["IntDiv"], CultureInfo.InvariantCulture));
        Assert.Equal("@LPH@-2025", row["FunctionText"]);
        Assert.InRange(Math.Abs(Math.Atan(1d) - Convert.ToDouble(row["AtnValue"], CultureInfo.InvariantCulture)), 0d, 0.000000000001d);
        Assert.Equal(new DateTime(2025, 2, 3), Convert.ToDateTime(row["AliasDate"], CultureInfo.InvariantCulture));
        Assert.Equal("String:3", row["TypeInfo"]);
        Assert.Equal("Al-HA", row["StringAliases"]);
        Assert.Equal("ALPHA", row["CaseConv"]);
        Assert.True(Convert.ToBoolean(row["CompareConstant"], CultureInfo.InvariantCulture));
        Assert.InRange(Convert.ToDouble(row["RandomValue"], CultureInfo.InvariantCulture), 0d, 1d);
        Assert.InRange(Convert.ToDouble(row["SeededRandom"], CultureInfo.InvariantCulture), 0d, 1d);
        Assert.Equal(
            Convert.ToDouble(row["SeededRandom"], CultureInfo.InvariantCulture),
            Convert.ToDouble(row["RepeatedRandom"], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task InsertRow_CalculatedColumns_EvaluatesFunctionRegistryGoldenCases()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcFunctionRegistry",
                [
                    new("Seed", typeof(int)),
                    new("LogicalEdge", typeof(bool))
                    {
                        IsCalculated = true,
                        CalculationExpression = "Not (True Xor False Xor True) And (False Imp False)",
                    },
                    new("ChoiceEdge", typeof(string), maxLength: 40)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Choose(2, \"first\", \"second\")",
                    },
                    new("NullEdge", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Nz(Null, \"fallback\") & \":\" & CStr(IsNull(Null)) & \":\" & CStr(IsNumeric(\"12.5\")) & \":\" & CStr(IsDate(\"2025-02-03\"))",
                    },
                    new("TextEdge", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Mid(\"Access\", 2, 3) & \":\" & CStr(InStr(\"alphabet\", \"ph\")) & \":\" & CStr(InStrRev(\"banana\", \"na\")) & \":\" & String(3, \"xy\") & \":\" & StrReverse(\"stressed\")",
                    },
                    new("DateEdge", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "CStr(Year(DateAdd(\"m\", 1, DateSerial(2025, 12, 31)))) & \":\" & CStr(DateDiff(\"d\", DateSerial(2025, 1, 1), DateSerial(2025, 1, 31))) & \":\" & WeekdayName(2, True, 1) & \":\" & MonthName(2)",
                    },
                    new("NumericEdge", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "CStr(Int(-1.2)) & \":\" & CStr(Fix(-1.2)) & \":\" & CStr(Round(2.5, 0)) & \":\" & Hex(255) & \":\" & Oct(8) & \":\" & CStr(Val(\" 12 apples\"))",
                    },
                    new("FormattingEdge", typeof(string), maxLength: 80)
                    {
                        IsCalculated = true,
                        CalculationExpression = "FormatNumber(1234.5, 1) & \"|\" & FormatPercent(0.125, 1) & \"|\" & FormatDateTime(DateSerial(2025, 2, 3), 2)",
                    },
                    new("FinancialEdge", typeof(string), maxLength: 40)
                    {
                        IsCalculated = true,
                        CalculationExpression = "FormatNumber(Pmt(0, 10, 1000), 2)",
                    },
                    new("MetadataEdge", typeof(string), maxLength: 40)
                    {
                        IsCalculated = true,
                        CalculationExpression = "TypeName(Null) & \":\" & CStr(VarType(Null)) & \":\" & CStr(CVar(7))",
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcFunctionRegistry",
                [
                    1,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                    DBNull.Value,
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcFunctionRegistry", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());

        Assert.True(Convert.ToBoolean(row["LogicalEdge"], CultureInfo.InvariantCulture));
        Assert.Equal("second", row["ChoiceEdge"]);
        Assert.Equal("fallback:-1:-1:-1", row["NullEdge"]);
        Assert.Equal("cce:3:5:xxx:desserts", row["TextEdge"]);
        Assert.Equal("2026:30:Mon:February", row["DateEdge"]);
        Assert.Equal("-2:-1:2:FF:10:12", row["NumericEdge"]);
        Assert.Equal("1,234.5|12.5%|02/03/2025", row["FormattingEdge"]);
        Assert.Equal("-100.00", row["FinancialEdge"]);
        Assert.Equal("Null:1:7", row["MetadataEdge"]);
    }

    [Fact]
    public async Task CreateTable_InvalidCalculatedExpressionSyntax_ThrowsArgumentException()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync(
                    "CalcBadSyntax",
                    [
                        new("Score", typeof(int)),
                        new("BadCalc", typeof(int))
                        {
                            IsCalculated = true,
                            CalculationExpression = "[Score] +",
                        },
                    ],
                    TestContext.Current.CancellationToken));

            Assert.Contains("'BadCalc'", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain("CalcBadSyntax", await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateTable_OverNestedCalculatedExpression_ThrowsArgumentException()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        string expression = new string('(', 129) + "1" + new string(')', 129);

        await using AccessWriter writer = await OpenWriterAsync(stream);
        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "CalcDeepExpression",
                [
                    new("DeepCalc", typeof(int))
                    {
                        IsCalculated = true,
                        CalculationExpression = expression,
                    },
                ],
                TestContext.Current.CancellationToken));

        Assert.Contains("nesting depth", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InsertRow_CalculatedExpressionGeneratedTextTooLarge_ThrowsArgumentException()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "CalcHugeText",
            [
                new("HugeText", typeof(string), maxLength: 20)
                {
                    IsCalculated = true,
                    CalculationExpression = "Space(32769)",
                },
            ],
            TestContext.Current.CancellationToken);

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(
                "CalcHugeText",
                [DBNull.Value],
                TestContext.Current.CancellationToken));

        Assert.Contains("generated text length", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("DLookup(\"Name\", \"People\")")]
    [InlineData("DCount(\"*\", \"People\")")]
    [InlineData("DSum(\"Score\", \"People\")")]
    [InlineData("DAvg(\"Score\", \"People\")")]
    [InlineData("DMin(\"Score\", \"People\")")]
    [InlineData("DMax(\"Score\", \"People\")")]
    public async Task InsertRow_AccessRejectedDomainAggregateCalculatedExpression_ThrowsNotSupportedException(string expression)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "CalcDomain",
            [
                new("LookupValue", typeof(string), maxLength: 40)
                {
                    IsCalculated = true,
                    CalculationExpression = expression,
                },
            ],
            TestContext.Current.CancellationToken);

        NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.InsertRowAsync(
                "CalcDomain",
                [DBNull.Value],
                TestContext.Current.CancellationToken));

        Assert.Contains("Access table calculated columns reject domain aggregate function", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("SUM(1, 2)")]
    [InlineData("A1 + 1")]
    public async Task InsertRow_SpreadsheetOnlyCalculatedExpression_ThrowsNotSupportedException(string expression)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "CalcSpreadsheetOnly",
            [
                new("SpreadsheetValue", typeof(int))
                {
                    IsCalculated = true,
                    CalculationExpression = expression,
                },
            ],
            TestContext.Current.CancellationToken);

        NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.InsertRowAsync(
                "CalcSpreadsheetOnly",
                [DBNull.Value],
                TestContext.Current.CancellationToken));

        Assert.Contains("Calculated-column", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InsertRow_CalculatedExpression_UsesAccessOperatorPrecedence()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcPrecedence",
                [
                    new("Id", typeof(int)),
                    new("NegPow", typeof(double)) { IsCalculated = true, CalculationExpression = "-2^2" },
                    new("NegPowMod", typeof(double)) { IsCalculated = true, CalculationExpression = "-2^2 + (0 Mod 5)" },
                    new("NegFieldPow", typeof(double)) { IsCalculated = true, CalculationExpression = "-[Id]^2" },
                    new("PowChain", typeof(double)) { IsCalculated = true, CalculationExpression = "2^3^2" },
                    new("DivChain", typeof(int)) { IsCalculated = true, CalculationExpression = "7 \\ 2 * 2" },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcPrecedence",
                [3, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcPrecedence", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());

        Assert.Equal(-4d, Convert.ToDouble(row["NegPow"], CultureInfo.InvariantCulture));
        Assert.Equal(-4d, Convert.ToDouble(row["NegPowMod"], CultureInfo.InvariantCulture));
        Assert.Equal(-9d, Convert.ToDouble(row["NegFieldPow"], CultureInfo.InvariantCulture));
        Assert.Equal(64d, Convert.ToDouble(row["PowChain"], CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(row["DivChain"], CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("5%")]
    [InlineData("[Rate]%")]
    [InlineData("[Rate] * 5% + 1")]
    public async Task CreateTable_PercentCalculatedExpression_ThrowsArgumentException(string expression)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync(
                    "CalcPercent",
                    [
                        new("Rate", typeof(double)),
                        new("Pct", typeof(double)) { IsCalculated = true, CalculationExpression = expression },
                    ],
                    TestContext.Current.CancellationToken));

            Assert.Contains(expression, exception.Message, StringComparison.Ordinal);
            Assert.Contains("'Pct'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("'%'", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain("CalcPercent", await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AddColumn_PercentCalculatedExpression_ThrowsArgumentException()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcAddPercent",
                [new("Rate", typeof(double))],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcAddPercent", [2.5d], TestContext.Current.CancellationToken);

            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.AddColumnAsync(
                    "CalcAddPercent",
                    new("Pct", typeof(double)) { IsCalculated = true, CalculationExpression = "[Rate]%" },
                    TestContext.Current.CancellationToken));

            Assert.Contains("[Rate]%", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("CalcAddPercent", TestContext.Current.CancellationToken);
        Assert.Equal("Rate", Assert.Single(metadata).Name);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcAddPercent", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal(2.5d, Convert.ToDouble(row["Rate"], CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("[Score] 2")]
    [InlineData("[Score] + 1 )")]
    [InlineData("{1, 2}")]
    public async Task CreateTableAndAddColumn_UnparseableCalculatedExpression_ThrowsArgumentException(string expression)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            ArgumentException createException = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.CreateTableAsync(
                    "CalcBadSyntax",
                    [
                        new("Score", typeof(int)),
                        new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = expression },
                    ],
                    TestContext.Current.CancellationToken));
            Assert.Contains("'Calc'", createException.Message, StringComparison.Ordinal);
            Assert.Contains(expression, createException.Message, StringComparison.Ordinal);

            await writer.CreateTableAsync("CalcAddBadSyntax", [new("Score", typeof(int))], TestContext.Current.CancellationToken);
            ArgumentException addException = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.AddColumnAsync(
                    "CalcAddBadSyntax",
                    new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = expression },
                    TestContext.Current.CancellationToken));
            Assert.Contains(expression, addException.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain("CalcBadSyntax", await reader.ListTablesAsync(TestContext.Current.CancellationToken));
        Assert.Equal("Score", Assert.Single(await reader.GetColumnMetadataAsync("CalcAddBadSyntax", TestContext.Current.CancellationToken)).Name);
    }

    [Fact]
    public async Task StoredPercentExpression_ReadsButRejectsWritesThatReevaluateIt()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        // Only an earlier version of this library could have stored '%'; plant it
        // through the internal schema service, which skips the definition check.
        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            await harness.Services.Schema.CreateTableAsync(
                "CalcLegacyPercent",
                [
                    new("Id", typeof(int)),
                    new("R", typeof(int)),
                    new("C", typeof(double)) { IsCalculated = true, CalculationExpression = "[R]%" },
                ],
                [],
                TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("CalcLegacyPercent", [1, 50, 0.5d], TestContext.Current.CancellationToken);

            ArgumentException insert = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync("CalcLegacyPercent", [2, 60, DBNull.Value], TestContext.Current.CancellationToken));
            Assert.Contains("[R]%", insert.Message, StringComparison.Ordinal);

            ArgumentException update = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.UpdateRowsAsync(
                    "CalcLegacyPercent",
                    "Id",
                    1,
                    new Dictionary<string, object?> { ["R"] = 70 },
                    TestContext.Current.CancellationToken));
            Assert.Contains("[R]%", update.Message, StringComparison.Ordinal);

            await writer.AddColumnAsync("CalcLegacyPercent", new("Note", typeof(string), maxLength: 10), TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcLegacyPercent", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal(50, Convert.ToInt32(row["R"], CultureInfo.InvariantCulture));
        Assert.Equal(0.5d, Convert.ToDouble(row["C"], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task CreateTable_PercentInsideStringOrFieldName_IsAccepted()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcPercentText",
                [
                    new("Rate%", typeof(int)),
                    new("Label", typeof(string), maxLength: 20) { IsCalculated = true, CalculationExpression = "[Rate%] & \"%\"" },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("CalcPercentText", [15, DBNull.Value], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcPercentText", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal("15%", row["Label"]);
    }

    [Fact]
    public async Task StoredCalculatedExpressionTheEngineCannotParse_StillReadsAndAcceptsSuppliedValues()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        // The public CreateTableAsync now refuses this expression, so plant it
        // through the internal schema service, the way an older writer (or a
        // newer Access syntax this engine lacks) would have left it in the file.
        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            await harness.Services.Schema.CreateTableAsync(
                "CalcUnparsed",
                [
                    new("Score", typeof(int)),
                    new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = "[Score] 2" },
                ],
                [],
                TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("CalcUnparsed", [1, 42], TestContext.Current.CancellationToken);

            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync("CalcUnparsed", [2, DBNull.Value], TestContext.Current.CancellationToken));
            Assert.Contains("[Score] 2", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        ColumnMetadata calc = Assert.Single(await reader.GetColumnMetadataAsync("CalcUnparsed", TestContext.Current.CancellationToken), c => c.Name == "Calc");
        Assert.Equal("[Score] 2", calc.CalculationExpression);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcUnparsed", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal(42, Convert.ToInt32(row["Calc"], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task InsertAndUpdate_TextInputsForNumericAndDateColumns_EvaluateByColumnType()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcInputTypes",
                [
                    new("Id", typeof(int)),
                    new("A", typeof(int)),
                    new("B", typeof(int)),
                    new("D1", typeof(DateTime)),
                    new("D2", typeof(DateTime)),
                    new("Sum", typeof(int)) { IsCalculated = true, CalculationExpression = "[A] + [B]" },
                    new("Less", typeof(bool)) { IsCalculated = true, CalculationExpression = "[A] < [B]" },
                    new("Earlier", typeof(bool)) { IsCalculated = true, CalculationExpression = "[D1] < [D2]" },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcInputTypes",
                [1, "10", "9", "10/1/2020", "2/1/2020", DBNull.Value, DBNull.Value, DBNull.Value],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "CalcInputTypes",
                [2, 1, 2, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value],
                TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                "CalcInputTypes",
                "Id",
                2,
                new Dictionary<string, object?> { ["A"] = "100", ["B"] = "25", ["D1"] = "1/5/2021", ["D2"] = "12/1/2020" },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow[] rows = [.. (await reader.ReadDataTableAsync("CalcInputTypes", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable()
            .OrderBy(r => Convert.ToInt32(r["Id"], CultureInfo.InvariantCulture))];
        Assert.Equal(2, rows.Length);

        Assert.Equal(19, Convert.ToInt32(rows[0]["Sum"], CultureInfo.InvariantCulture));
        Assert.False((bool)rows[0]["Less"]);
        Assert.False((bool)rows[0]["Earlier"]);

        Assert.Equal(125, Convert.ToInt32(rows[1]["Sum"], CultureInfo.InvariantCulture));
        Assert.False((bool)rows[1]["Less"]);
        Assert.False((bool)rows[1]["Earlier"]);
    }

    [Fact]
    public async Task InsertRow_NumericInputsForTextColumns_ConcatenateAsText()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcTextInputs",
                [
                    new("T1", typeof(string), maxLength: 10),
                    new("T2", typeof(string), maxLength: 10),
                    new("Joined", typeof(string), maxLength: 20) { IsCalculated = true, CalculationExpression = "[T1] + [T2]" },
                    new("Less", typeof(bool)) { IsCalculated = true, CalculationExpression = "[T1] < [T2]" },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "CalcTextInputs",
                [10, 9, DBNull.Value, DBNull.Value],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcTextInputs", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal("10", row["T1"]);
        Assert.Equal("109", row["Joined"]);
        Assert.True((bool)row["Less"]);
    }

    [Fact]
    public async Task InsertRow_CircularCalculatedDependency_ThrowsInvalidOperationException()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "CalcCycle",
            [
                new("A", typeof(int))
                {
                    IsCalculated = true,
                    CalculationExpression = "[B] + 1",
                },
                new("B", typeof(int))
                {
                    IsCalculated = true,
                    CalculationExpression = "[A] + 1",
                },
            ],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(
                "CalcCycle",
                [DBNull.Value, DBNull.Value],
                TestContext.Current.CancellationToken));
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private sealed class CalculatedProjection
    {
        public string? CalcLabel { get; set; }

        public bool IsHigh { get; set; }

        public int NextScore { get; set; }

        public decimal Weighted { get; set; }
    }

    private sealed class SourceProjection
    {
        public int Score { get; set; }

        public string? Label { get; set; }
    }
}
