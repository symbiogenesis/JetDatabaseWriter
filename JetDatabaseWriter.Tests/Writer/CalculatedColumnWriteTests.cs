namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
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
            Assert.Equal("columns", exception.ParamName);
            Assert.DoesNotContain("Parameter 'expression'", exception.Message, StringComparison.Ordinal);
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
            Assert.Contains("'Pct'", exception.Message, StringComparison.Ordinal);
            Assert.Equal("column", exception.ParamName);
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
            Assert.Equal("columns", createException.ParamName);

            await writer.CreateTableAsync("CalcAddBadSyntax", [new("Score", typeof(int))], TestContext.Current.CancellationToken);
            ArgumentException addException = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.AddColumnAsync(
                    "CalcAddBadSyntax",
                    new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = expression },
                    TestContext.Current.CancellationToken));
            Assert.Contains(expression, addException.Message, StringComparison.Ordinal);
            Assert.Equal("column", addException.ParamName);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain("CalcBadSyntax", await reader.ListTablesAsync(TestContext.Current.CancellationToken));
        Assert.Equal("Score", Assert.Single(await reader.GetColumnMetadataAsync("CalcAddBadSyntax", TestContext.Current.CancellationToken)).Name);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "5%")]
    [InlineData(DatabaseFormat.Jet3Mdb, "[A] +")]
    [InlineData(DatabaseFormat.Jet3Mdb, "[A]*2")]
    [InlineData(DatabaseFormat.Jet4Mdb, "5%")]
    [InlineData(DatabaseFormat.Jet4Mdb, "[A] +")]
    [InlineData(DatabaseFormat.Jet4Mdb, "[A]*2")]
    public async Task CreateTable_CalculatedColumnOnMdb_ThrowsNotSupportedBeforeCheckingExpression(DatabaseFormat format, string expression)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await writer.CreateTableAsync(
                    "CalcMdb",
                    [
                        new("A", typeof(int)),
                        new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = expression },
                    ],
                    TestContext.Current.CancellationToken));

            Assert.Contains("only supported in ACCDB", exception.Message, StringComparison.Ordinal);
            Assert.Contains("'Calc'", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain("CalcMdb", await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "5%")]
    [InlineData(DatabaseFormat.Jet3Mdb, "[A] +")]
    [InlineData(DatabaseFormat.Jet3Mdb, "[A]*2")]
    [InlineData(DatabaseFormat.Jet4Mdb, "5%")]
    [InlineData(DatabaseFormat.Jet4Mdb, "[A] +")]
    [InlineData(DatabaseFormat.Jet4Mdb, "[A]*2")]
    public async Task AddColumn_CalculatedColumnOnMdb_ThrowsNotSupportedBeforeCheckingExpression(DatabaseFormat format, string expression)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync("CalcAddMdb", [new("A", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcAddMdb", [5], TestContext.Current.CancellationToken);

            NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await writer.AddColumnAsync(
                    "CalcAddMdb",
                    new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = expression },
                    TestContext.Current.CancellationToken));

            Assert.Contains("only supported in ACCDB", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Equal("A", Assert.Single(await reader.GetColumnMetadataAsync("CalcAddMdb", TestContext.Current.CancellationToken)).Name);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcAddMdb", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal(5, Convert.ToInt32(row["A"], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A column the format cannot hold is refused before the table is even
    /// looked up, so the error names the column rather than the missing table.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="kind">The kind of ACCDB-only column.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "calculated")]
    [InlineData(DatabaseFormat.Jet4Mdb, "calculated")]
    [InlineData(DatabaseFormat.Jet4Mdb, "largeNumber")]
    [InlineData(DatabaseFormat.Jet4Mdb, "extendedDate")]
    public async Task AddColumn_AccdbOnlyColumnOnMdb_ThrowsNotSupportedBeforeReadingTable(DatabaseFormat format, string kind)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        ColumnDefinition column = kind switch
        {
            "calculated" => new("Calc", typeof(int)) { IsCalculated = true, CalculationExpression = "[A]*2" },
            "largeNumber" => new("Big", typeof(long)),
            _ => new("Stamp", typeof(DateTime)) { IsDateTimeExtended = true },
        };

        await using AccessWriter writer = await OpenWriterAsync(stream);
        NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.AddColumnAsync("NoSuchTable", column, TestContext.Current.CancellationToken));

        Assert.Contains("only supported in ACCDB", exception.Message, StringComparison.Ordinal);
        Assert.Contains($"'{column.Name}'", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task CreateTable_InvalidTableName_IsReportedBeforeCalculatedExpression(string? tableName)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        ArgumentException exception = await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                tableName!,
                [
                    new("Rate", typeof(double)),
                    new("Pct", typeof(double)) { IsCalculated = true, CalculationExpression = "5%" },
                ],
                TestContext.Current.CancellationToken));

        Assert.Equal("tableName", exception.ParamName);
        Assert.Equal(tableName is null ? typeof(ArgumentNullException) : typeof(ArgumentException), exception.GetType());
    }

    [Fact]
    public async Task CreateTable_CalculatedAutoNumber_ThrowsNotSupportedBeforeCheckingExpression()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            NotSupportedException exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await writer.CreateTableAsync(
                    "CalcAuto",
                    [
                        new("Rate", typeof(double)),
                        new("Counter", typeof(int)) { IsCalculated = true, IsAutoIncrement = true, CalculationExpression = "5%" },
                    ],
                    TestContext.Current.CancellationToken));

            Assert.Contains("cannot be AutoNumber", exception.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.DoesNotContain("CalcAuto", await reader.ListTablesAsync(TestContext.Current.CancellationToken));
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

    /// <summary>
    /// calcFieldTestV2010's Table1 has calculated columns whose descriptor type
    /// differs from their <c>ResultType</c> (AllNames: Text/Memo, MonthlySalary:
    /// Double/Currency, WeeklySalary: Double/Decimal, IsRich and BoolTest:
    /// Integer/Boolean, FloatTest: Decimal/Single). Access stores the cached value
    /// by the result type, so an insert and an update must too: a default
    /// (strict) reader then reads every row, and every cached value equals its
    /// expression re-evaluated against the row.
    /// </summary>
    /// <param name="mode">"none", "transactional" (UseTransactionalWrites) or "explicit" (a committed transaction).</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("none")]
    [InlineData("transactional")]
    [InlineData("explicit")]
    public async Task AccessAuthoredCalculatedColumns_InsertAndUpdate_StoreEachValueAsItsResultType(string mode)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.CalcFieldTestV2010);

        await WriteInModeAsync(stream, mode, async writer =>
        {
            await writer.InsertRowAsync(
                "Table1",
                new RowValues { ["FirstName"] = "Ann", ["LastName"] = "Lee", ["City"] = "X", ["Salary"] = 120000m, ["Popularity"] = 3.5m },
                TestContext.Current.CancellationToken);
            int updated = await writer.UpdateRowsAsync(
                "Table1",
                RowCriteria.Where("FirstName", "Bruce"),
                new RowValues { ["City"] = "Gotham2" },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        });

        DataTable table = await AssertCalculatedValuesMatchExpressionsAsync(stream, "Table1");
        Assert.Equal(5, table.Rows.Count);

        DataRow ann = Assert.Single(table.AsEnumerable(), r => (string)r["FirstName"] == "Ann");
        Assert.Equal("Lee, Ann=Lee, Ann", ann["AllNames"]);
        Assert.Equal(10000m, ann["MonthlySalary"]);
        Assert.Equal(true, ann["IsRich"]);
        Assert.Equal(true, ann["BoolTest"]);

        DataRow bruce = Assert.Single(table.AsEnumerable(), r => (string)r["FirstName"] == "Bruce");
        Assert.Equal("Gotham2", bruce["City"]);
        Assert.Equal("Wayne, Bruce=Wayne, Bruce", bruce["AllNames"]);
        Assert.Equal(83333.3333m, bruce["MonthlySalary"]);
    }

    /// <summary>
    /// The inserted row's cached values use Access's payload layout for each
    /// result type: a 1-byte 0xFF Boolean, an 8-byte Currency, and a long-value
    /// header (inline, single-page or chained) in front of the wrapped Memo text,
    /// even though the descriptors say Integer, Double and Text.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessAuthoredCalculatedColumns_InsertedRow_UsesAccessPayloadLayout()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.CalcFieldTestV2010);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(
                "Table1",
                new RowValues { ["FirstName"] = "Ann", ["LastName"] = "Lee", ["City"] = "X", ["Salary"] = 120000m, ["Popularity"] = 3.5m },
                TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Table1", TestContext.Current.CancellationToken));
        TableDef tableDef = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, TestContext.Current.CancellationToken));
        DatabaseFile db = harness.Database;
        ColumnInfo firstName = tableDef.Columns.Single(c => c.Name == "FirstName");
        ColumnInfo[] calculated = [.. tableDef.Columns.Where(c => c.Name is "IsRich" or "MonthlySalary" or "AllNames")];
        Assert.Equal(ColumnType.IntegerType, calculated.Single(c => c.Name == "IsRich").Type);
        Assert.Equal(ColumnType.DoubleType, calculated.Single(c => c.Name == "MonthlySalary").Type);
        Assert.Equal(ColumnType.TextType, calculated.Single(c => c.Name == "AllNames").Type);

        Dictionary<string, byte[]>? slots = null;
        await db.ForEachLiveTableRowAsync(
            entry.TDefPage,
            (row, _) =>
            {
                byte[] page = row.Page;
                int rowStart = row.Location.RowStart;
                int rowSize = row.Location.RowSize;
                Assert.True(db.TryParseRowLayout(page, rowStart, rowSize, hasVarColumns: true, out RowLayout layout));
                ColumnSlice nameSlice = db.ResolveColumnSlice(page, rowStart, rowSize, layout, firstName);
                if (db.DecodeTextForFormat(page, rowStart + nameSlice.DataStart, nameSlice.DataLen) == "Ann")
                {
                    slots = calculated.ToDictionary(
                        c => c.Name,
                        c =>
                        {
                            ColumnSlice slice = db.ResolveColumnSlice(page, rowStart, rowSize, layout, c);
                            return page.AsSpan(rowStart + slice.DataStart, slice.DataLen).ToArray();
                        });
                }

                return new ValueTask<bool>(true);
            },
            TestContext.Current.CancellationToken);

        Assert.NotNull(slots);

        byte[] isRich = CalculatedColumnUtil.Unwrap(slots["IsRich"]);
        Assert.Equal([0xFF], isRich);

        byte[] monthlySalary = CalculatedColumnUtil.Unwrap(slots["MonthlySalary"]);
        Assert.Equal(8, monthlySalary.Length);
        Assert.Equal(10000m, decimal.FromOACurrency(BinaryPrimitives.ReadInt64LittleEndian(monthlySalary)));

        byte[] allNames = slots["AllNames"];
        Assert.True(allNames.Length >= Constants.LongValue.HeaderSize, $"AllNames slot is {allNames.Length} bytes, shorter than a long-value header.");
        Assert.Contains(allNames[3], new byte[] { 0x80, 0x40, 0x00 });
        var longValues = new LongValueDecoder(db, harness.Services.PageCache);
        byte[] lval = await longValues.ReadLongValueRawBytesAsync(allNames, 0, allNames.Length, TestContext.Current.CancellationToken);
        byte[] text = CalculatedColumnUtil.Unwrap(lval);
        Assert.Equal("Lee, Ann=Lee, Ann", db.DecodeTextForFormat(text, 0, text.Length));
    }

    /// <summary>
    /// A Memo result over the 1,024-byte inline limit spills to LVAL pages and
    /// reads back exactly, although the column descriptor says Text.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessAuthoredCalculatedMemo_OverInlineLimit_SpillsToLval()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.CalcFieldTestV2010);
        string first = new('f', 200);
        string last = new('l', 200);
        string lastFirst = last + ", " + first;
        string expected = lastFirst + "=" + lastFirst;

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(
                "Table1",
                new RowValues { ["FirstName"] = first, ["LastName"] = last, ["Salary"] = 1m, ["Popularity"] = 1m },
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(table.AsEnumerable(), r => (string)r["FirstName"] == first);
        Assert.Equal(expected, row["AllNames"]);

        DataTable strings = await reader.ReadTableAsStringsAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Contains(expected, strings.AsEnumerable().Select(r => (string)r["AllNames"]));
    }

    /// <summary>
    /// A schema rewrite of the Access-authored table carries each calculated
    /// column by its result type: the cached values still match their
    /// expressions, and AllNames's rebuilt descriptor is Memo.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessAuthoredCalculatedTable_AddColumn_KeepsCalculatedValues()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.CalcFieldTestV2010);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.AddColumnAsync("Table1", new("Note", typeof(string), maxLength: 20), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "Table1",
                new RowValues { ["FirstName"] = "Ann", ["LastName"] = "Lee", ["Salary"] = 120000m, ["Popularity"] = 3.5m, ["Note"] = "n" },
                TestContext.Current.CancellationToken);
        }

        DataTable table = await AssertCalculatedValuesMatchExpressionsAsync(stream, "Table1");
        Assert.Equal(
            ["Doe, John=Doe, John", "Lee, Ann=Lee, Ann", "Simpson, Bart=Simpson, Bart", "User, Test=User, Test", "Wayne, Bruce=Wayne, Bruce"],
            table.AsEnumerable().Select(r => (string)r["AllNames"]).Order(StringComparer.Ordinal));

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("Table1", TestContext.Current.CancellationToken);
            Assert.Equal(0x0C, Assert.Single(metadata, c => c.Name == "AllNames").CalculatedResultType);
            Assert.Equal(typeof(decimal), Assert.Single(metadata, c => c.Name == "MonthlySalary").ClrType);
        }

        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Table1", TestContext.Current.CancellationToken));
        TableDef tableDef = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, TestContext.Current.CancellationToken));
        Assert.Equal(ColumnType.MemoType, tableDef.Columns.Single(c => c.Name == "AllNames").Type);
    }

    /// <summary>
    /// A Boolean expression in a Byte calculated column stores 255 for True and 0
    /// for False (the OLE Automation conversion), on insert and when an update
    /// recomputes it.
    /// </summary>
    /// <param name="mode">"none", "transactional" (UseTransactionalWrites) or "explicit" (a committed transaction).</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("none")]
    [InlineData("transactional")]
    [InlineData("explicit")]
    public async Task InsertAndUpdate_BooleanExpressionInByteColumn_Stores255(string mode)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcByteFlag",
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)),
                    new("Flag", typeof(byte)) { IsCalculated = true, CalculationExpression = "[Score] > 1" },
                ],
                TestContext.Current.CancellationToken);
        }

        await WriteInModeAsync(stream, mode, async writer =>
        {
            await writer.InsertRowAsync("CalcByteFlag", [1, 3, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcByteFlag", [2, 0, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcByteFlag", [3, 5, DBNull.Value], TestContext.Current.CancellationToken);
            int updated = await writer.UpdateRowsAsync("CalcByteFlag", "Id", 2, new Dictionary<string, object?> { ["Score"] = 9 }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
            updated = await writer.UpdateRowsAsync("CalcByteFlag", "Id", 3, new Dictionary<string, object?> { ["Score"] = 1 }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        });

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcByteFlag", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal((byte)255, Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 1)["Flag"]);
        Assert.Equal((byte)255, Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 2)["Flag"]);
        Assert.Equal((byte)0, Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 3)["Flag"]);
    }

    /// <summary>
    /// A calculated value the result type cannot hold throws an
    /// <see cref="OverflowException"/> that names the table, column, expression
    /// and value, and the row is not written.
    /// </summary>
    /// <param name="explicitTransaction">Whether the insert runs in a transaction that is then committed.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InsertRow_CalculatedResultOverflow_ThrowsOverflowNamingColumnAndLeavesTableUnchanged(bool explicitTransaction)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcOverflow",
                [
                    new("Id", typeof(int)),
                    new("A", typeof(int)),
                    new("Hundreds", typeof(byte)) { IsCalculated = true, CalculationExpression = "[A] * 100" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcOverflow", [1, 2, DBNull.Value], TestContext.Current.CancellationToken);
        }

        await WriteInModeAsync(stream, explicitTransaction ? "explicit" : "none", async writer =>
        {
            OverflowException exception = await Assert.ThrowsAsync<OverflowException>(async () =>
                await writer.InsertRowAsync("CalcOverflow", [2, 5, DBNull.Value], TestContext.Current.CancellationToken));
            Assert.Contains("'Hundreds'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("'CalcOverflow'", exception.Message, StringComparison.Ordinal);
            Assert.Contains("[A] * 100", exception.Message, StringComparison.Ordinal);
            Assert.Contains("500", exception.Message, StringComparison.Ordinal);
            Assert.Contains("Byte", exception.Message, StringComparison.Ordinal);
        });

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcOverflow", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal((byte)200, row["Hundreds"]);
    }

    /// <summary>
    /// <c>And</c> on a number is bitwise, so a flags test stores the masked bits in an
    /// Integer column and a Boolean comparison on them; a Null flags value stores Null
    /// in both, because <c>Null And 4</c> is Null.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task InsertRow_BitwiseFlagsExpression_StoresAccessResult()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcFlags",
                [
                    new("Id", typeof(int)),
                    new("Flags", typeof(int)),
                    new("Bit2", typeof(short)) { IsCalculated = true, CalculationExpression = "[Flags] And 4" },
                    new("HasBit2", typeof(bool)) { IsCalculated = true, CalculationExpression = "([Flags] And 4) <> 0" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcFlags", [1, 12, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcFlags", [2, 8, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcFlags", [3, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcFlags", cancellationToken: TestContext.Current.CancellationToken);
        DataRow twelve = Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 1);
        DataRow eight = Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 2);
        DataRow none = Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 3);
        Assert.Equal((short)4, twelve["Bit2"]);
        Assert.Equal(true, twelve["HasBit2"]);
        Assert.Equal((short)0, eight["Bit2"]);
        Assert.Equal(false, eight["HasBit2"]);
        Assert.Equal(DBNull.Value, none["Bit2"]);
        Assert.Equal(DBNull.Value, none["HasBit2"]);
    }

    /// <summary>
    /// Single-quoted strings and <c>&amp;H</c> literals are accepted at
    /// <see cref="AccessWriter.CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, System.Threading.CancellationToken)"/>,
    /// evaluated on insert, and persisted exactly as written.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task CreateTable_SingleQuotedAndHexExpressions_AreAcceptedAndEvaluated()
    {
        const string fullName = "[First] & ' ' & [Last]";
        const string lowBits = "[Flags] And &H0F";
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcLiterals",
                [
                    new("First", typeof(string), maxLength: 20),
                    new("Last", typeof(string), maxLength: 20),
                    new("Flags", typeof(int)),
                    new("FullName", typeof(string), maxLength: 50) { IsCalculated = true, CalculationExpression = fullName },
                    new("LowBits", typeof(int)) { IsCalculated = true, CalculationExpression = lowBits },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("CalcLiterals", ["Ann", "O'Lee", 0x5A, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("CalcLiterals", TestContext.Current.CancellationToken);
        Assert.Equal(fullName, Assert.Single(metadata, c => c.Name == "FullName").CalculationExpression);
        Assert.Equal(lowBits, Assert.Single(metadata, c => c.Name == "LowBits").CalculationExpression);

        DataRow row = Assert.Single((await reader.ReadDataTableAsync("CalcLiterals", cancellationToken: TestContext.Current.CancellationToken)).AsEnumerable());
        Assert.Equal("Ann O'Lee", row["FullName"]);
        Assert.Equal(0x0A, row["LowBits"]);
    }

    /// <summary>
    /// A Text calculated column that joins a date stores the date in VBA's en-US
    /// General Date form, on insert and when an update recomputes it, whatever the
    /// current culture.
    /// </summary>
    /// <param name="mode">"none", "transactional" (UseTransactionalWrites) or "explicit" (a committed transaction).</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("none")]
    [InlineData("transactional")]
    [InlineData("explicit")]
    public async Task InsertAndUpdate_DateInTextExpression_StoresGeneralDate(string mode)
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CalcDueText",
                [
                    new("Id", typeof(int)),
                    new("Due", typeof(DateTime)),
                    new("Label", typeof(string), maxLength: 60) { IsCalculated = true, CalculationExpression = "\"Due \" & [Due]" },
                ],
                TestContext.Current.CancellationToken);
        }

        CultureInfo previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
            await WriteInModeAsync(stream, mode, async writer =>
            {
                await writer.InsertRowAsync("CalcDueText", [1, new DateTime(2020, 1, 31, 18, 30, 5), DBNull.Value], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("CalcDueText", [2, new DateTime(2020, 1, 31), DBNull.Value], TestContext.Current.CancellationToken);
                int updated = await writer.UpdateRowsAsync("CalcDueText", "Id", 2, new Dictionary<string, object?> { ["Due"] = new DateTime(2021, 12, 9, 7, 5, 0) }, TestContext.Current.CancellationToken);
                Assert.Equal(1, updated);
            });
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcDueText", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("Due 1/31/2020 6:30:05 PM", Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 1)["Label"]);
        Assert.Equal("Due 12/9/2021 7:05:00 AM", Assert.Single(table.AsEnumerable(), r => (int)r["Id"] == 2)["Label"]);
    }

    private static async Task WriteInModeAsync(MemoryStream stream, string mode, Func<AccessWriter, Task> work)
    {
        await using AccessWriter writer = await OpenWriterAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == "transactional" });
        if (mode == "explicit")
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await work(writer);
            await transaction.CommitAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            await work(writer);
        }
    }

    /// <summary>
    /// Reads <paramref name="tableName"/> with a default (strict) reader and checks
    /// that every calculated column of every row equals its expression re-evaluated
    /// against the row's other values.
    /// </summary>
    /// <param name="stream">The database.</param>
    /// <param name="tableName">The table to check.</param>
    /// <returns>The table as read.</returns>
    private static async Task<DataTable> AssertCalculatedValuesMatchExpressionsAsync(MemoryStream stream, string tableName)
    {
        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        var tableDef = new TableDef();
        var constraints = new List<ColumnConstraint>(meta.Count);
        foreach (ColumnMetadata column in meta)
        {
            tableDef.Columns.Add(new ColumnInfo { Name = column.Name });
            constraints.Add(new ColumnConstraint
            {
                Name = column.Name,
                ClrType = column.ClrType,
                IsCalculated = column.IsCalculated,
                CalculationExpression = column.CalculationExpression,
            });
        }

        DataTable table = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);
        var mismatches = new List<string>();
        foreach (DataRow row in table.Rows)
        {
            object[] values = new object[meta.Count];
            for (int i = 0; i < meta.Count; i++)
            {
                values[i] = meta[i].IsCalculated ? DBNull.Value : row[meta[i].Name];
            }

            CalculatedExpressionEvaluator.Apply(tableDef, constraints, values, force: false);
            for (int i = 0; i < meta.Count; i++)
            {
                if (!meta[i].IsCalculated)
                {
                    continue;
                }

                object stored = row[meta[i].Name];
                object computed = values[i];
                string label = $"{meta[i].Name} = {meta[i].CalculationExpression}";
                if (stored is DBNull || computed is DBNull)
                {
                    if (!(stored is DBNull && computed is DBNull))
                    {
                        mismatches.Add($"{label}: stored {stored}, computed {computed}");
                    }
                }
                else if (stored is float or double or decimal)
                {
                    double expected = Convert.ToDouble(stored, CultureInfo.InvariantCulture);
                    double actual = Convert.ToDouble(computed, CultureInfo.InvariantCulture);
                    if (Math.Abs(expected - actual) > Math.Max(1e-4, Math.Abs(expected) * 1e-6))
                    {
                        mismatches.Add($"{label}: stored {expected}, computed {actual}");
                    }
                }
                else if (!Equals(stored, computed))
                {
                    mismatches.Add($"{label}: stored {stored} ({stored.GetType().Name}), computed {computed}");
                }
            }
        }

        Assert.True(mismatches.Count == 0, string.Join(Environment.NewLine, mismatches));
        return table;
    }

    private static async ValueTask<MemoryStream> CopyFixtureAsync(string path)
    {
        Assert.True(File.Exists(path), $"Fixture not found: {path}");
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        var stream = new MemoryStream();
        await stream.WriteAsync(bytes, TestContext.Current.CancellationToken);
        stream.Position = 0;
        return stream;
    }

    private static ValueTask<MemoryStream> CreateFreshAccdbStreamAsync() => CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, AccessWriterOptions? options = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            options ?? new AccessWriterOptions { UseLockFile = false },
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
