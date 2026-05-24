namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

public sealed class CalculatedColumnWriteTests
{
    [Fact]
    public async Task CreateTable_CalculatedColumns_RoundTripsMetadataAndCachedValues()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        DateTime eventDate = new(2025, 2, 3, 4, 5, 6);

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        var metadata = await reader.GetColumnMetadataAsync("CalcRoundTrip", TestContext.Current.CancellationToken);

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

        var typed = await reader.ReadTableAsync<CalculatedProjection>(
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
        await using var stream = await CreateFreshAccdbStreamAsync();
        string memo = new('A', 1200);

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcMemo", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, table.Rows.Count);
        Assert.Equal(memo, table.Rows[0]["ComputedMemo"]);
    }

    [Fact]
    public async Task InsertRow_CalculatedColumns_EvaluatesMissingCachedValues()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
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
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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
                new Dictionary<string, object>
                {
                    ["Score"] = 3,
                    ["Label"] = "Beta",
                },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcUpdate", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(table.AsEnumerable());

        Assert.Equal("Beta #3", row["CalcLabel"]);
        Assert.False(Convert.ToBoolean(row["IsHigh"], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task InsertRowPoco_CalculatedColumnsCanBeOmitted()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadDataTableAsync("CalcPoco", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(table.AsEnumerable());

        Assert.Equal("Gamma #7", row["CalcLabel"]);
    }

    [Fact]
    public async Task InsertRow_CalculatedColumns_EvaluatesAccessOperatorsAndBuiltins()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
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
    public async Task InsertRow_InvalidCalculatedExpressionSyntax_ThrowsArgumentException()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using var writer = await OpenWriterAsync(stream);
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
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(
                "CalcBadSyntax",
                [1, DBNull.Value],
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task InsertRow_OverNestedCalculatedExpression_ThrowsArgumentException()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        string expression = new string('(', 129) + "1" + new string(')', 129);

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "CalcDeepExpression",
            [
                new("DeepCalc", typeof(int))
                {
                    IsCalculated = true,
                    CalculationExpression = expression,
                },
            ],
            TestContext.Current.CancellationToken);

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(
                "CalcDeepExpression",
                [DBNull.Value],
                TestContext.Current.CancellationToken));

        Assert.Contains("nesting depth", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InsertRow_CalculatedExpressionGeneratedTextTooLarge_ThrowsArgumentException()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using var writer = await OpenWriterAsync(stream);
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
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using var writer = await OpenWriterAsync(stream);
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

    [Fact]
    public async Task InsertRow_CircularCalculatedDependency_ThrowsInvalidOperationException()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using var writer = await OpenWriterAsync(stream);
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
