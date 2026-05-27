namespace JetDatabaseWriter.Benchmarks.DelimitedText;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.DelimitedText;

/// <summary>
/// Isolated benchmarks for the internal linked text parser.
/// </summary>
[MemoryDiagnoser]
public class DelimitedTextReaderBenchmarks
{
    private const string PlainScenario = "Plain";
    private const string QuotedMultilineScenario = "QuotedMultiline";
    private const string EmptyFieldsScenario = "EmptyFields";
    private const string RepetitiveValuesScenario = "RepetitiveValues";
    private const int RowCount = 25_000;
    private const int ColumnCount = 10;

    private static readonly DelimitedTextFormat Format = new(hasHeaderRow: false, delimiter: ',');
    private static readonly DelimitedTextLimits Limits = new(
        MaxRecordLength: 4 * 1024 * 1024,
        MaxFieldLength: 4 * 1024 * 1024,
        MaxColumnCount: 255,
        MaxRecordLengthOptionName: "MaxRecordLength",
        MaxFieldLengthOptionName: "MaxFieldLength",
        MaxColumnCountOptionName: "MaxColumnCount");

    private string _source = string.Empty;

    /// <summary>Gets or sets the generated text shape to parse.</summary>
    [Params(PlainScenario, QuotedMultilineScenario, EmptyFieldsScenario, RepetitiveValuesScenario)]
    public string Scenario { get; set; } = PlainScenario;

    /// <summary>Generates the source text for the selected scenario.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _source = Scenario switch
        {
            PlainScenario => BuildPlainSource(),
            QuotedMultilineScenario => BuildQuotedMultilineSource(),
            EmptyFieldsScenario => BuildEmptyFieldsSource(),
            RepetitiveValuesScenario => BuildRepetitiveValuesSource(),
            _ => throw new InvalidOperationException("Unknown delimited text benchmark scenario."),
        };
    }

    /// <summary>Parses every row and discards field values after each record is produced.</summary>
    /// <returns>The number of parsed rows.</returns>
    [Benchmark]
    public async Task<int> RowOnlyScan()
    {
        using var stringReader = new StringReader(_source);
        using var reader = new DelimitedTextReader(stringReader, Format, Limits);
        int rowCount = 0;

        while (true)
        {
            var record = await reader.ReadRecordAsync(CancellationToken.None).ConfigureAwait(false);
            if (!record.HasValue)
            {
                return rowCount;
            }

            rowCount++;
        }
    }

    /// <summary>Counts records without materializing fields.</summary>
    /// <returns>The number of counted rows.</returns>
    [Benchmark]
    public async Task<long> CountRecords()
    {
        using var stringReader = new StringReader(_source);
        using var reader = new DelimitedTextReader(stringReader, Format, Limits);
        return await reader.CountRecordsAsync(skipFirstRecord: false, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Parses every row and keeps the materialized field arrays.</summary>
    /// <returns>The number of materialized rows.</returns>
    [Benchmark]
    public async Task<int> MaterializeRows()
    {
        using var stringReader = new StringReader(_source);
        using var reader = new DelimitedTextReader(stringReader, Format, Limits);
        var rows = new List<string[]>(RowCount);

        while (true)
        {
            var record = await reader.ReadRecordAsync(CancellationToken.None).ConfigureAwait(false);
            if (record is not { } current)
            {
                return rows.Count;
            }

            rows.Add(current.Fields);
        }
    }

    private static string BuildPlainSource()
    {
        var builder = new StringBuilder(RowCount * ColumnCount * 8);
        for (int rowIndex = 0; rowIndex < RowCount; rowIndex++)
        {
            for (int columnIndex = 0; columnIndex < ColumnCount; columnIndex++)
            {
                if (columnIndex > 0)
                {
                    builder.Append(',');
                }

                builder.Append('R');
                builder.Append(rowIndex.ToString(CultureInfo.InvariantCulture));
                builder.Append('C');
                builder.Append(columnIndex.ToString(CultureInfo.InvariantCulture));
            }

            builder.Append('\n');
        }

        return builder.ToString();
    }

    private static string BuildQuotedMultilineSource()
    {
        var builder = new StringBuilder(RowCount * 96);
        for (int rowIndex = 0; rowIndex < RowCount; rowIndex++)
        {
            builder.Append(rowIndex.ToString(CultureInfo.InvariantCulture));
            builder.Append(",\"first line ");
            builder.Append(rowIndex.ToString(CultureInfo.InvariantCulture));
            builder.Append("\r\nsecond line with \"\"quote\"\" and comma,\",");
            builder.Append("tail,");
            builder.Append((rowIndex % 17).ToString(CultureInfo.InvariantCulture));
            builder.Append(",done\r\n");
        }

        return builder.ToString();
    }

    private static string BuildEmptyFieldsSource()
    {
        var builder = new StringBuilder(RowCount * (ColumnCount + 1));
        string row = new string(',', ColumnCount - 1) + "\n";
        for (int rowIndex = 0; rowIndex < RowCount; rowIndex++)
        {
            builder.Append(row);
        }

        return builder.ToString();
    }

    private static string BuildRepetitiveValuesSource()
    {
        var builder = new StringBuilder(RowCount * ColumnCount * 8);
        for (int rowIndex = 0; rowIndex < RowCount; rowIndex++)
        {
            for (int columnIndex = 0; columnIndex < ColumnCount; columnIndex++)
            {
                if (columnIndex > 0)
                {
                    builder.Append(',');
                }

                builder.Append(columnIndex switch
                {
                    0 => "Active",
                    1 => "North",
                    2 => "Retail",
                    3 => "Standard",
                    4 => (rowIndex % 100).ToString(CultureInfo.InvariantCulture),
                    5 => "Pending",
                    6 => "Small",
                    7 => "Online",
                    8 => (rowIndex % 7).ToString(CultureInfo.InvariantCulture),
                    _ => "Complete",
                });
            }

            builder.Append('\n');
        }

        return builder.ToString();
    }
}
