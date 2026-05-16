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
