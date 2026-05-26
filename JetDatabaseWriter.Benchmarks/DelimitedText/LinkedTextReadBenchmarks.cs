namespace JetDatabaseWriter.Benchmarks.DelimitedText;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Enums;

/// <summary>
/// End-to-end linked text table benchmarks over headered and headerless CSV sources.
/// </summary>
[MemoryDiagnoser]
public class LinkedTextReadBenchmarks
{
    private const string HeaderedKind = "Headered";
    private const string HeaderlessKind = "Headerless";
    private const string HeaderedTable = "LinkedHeaderedCsv";
    private const string HeaderlessTable = "LinkedHeaderlessCsv";
    private const int RowCount = 25_000;
    private const int ColumnCount = 5;

    private AccessReader _reader = null!;
    private string _rootDirectory = string.Empty;

    /// <summary>Gets or sets whether the benchmark uses a linked CSV source with a header row.</summary>
    [Params(HeaderedKind, HeaderlessKind)]
    public string TableKind { get; set; } = HeaderedKind;

    /// <summary>Gets the linked table name selected by <see cref="TableKind"/>.</summary>
    /// <value>The linked table name selected by <see cref="TableKind"/>.</value>
    private string CurrentTableName => TableKind == HeaderedKind ? HeaderedTable : HeaderlessTable;

    /// <summary>Creates a temporary front-end database and linked CSV sources.</summary>
    /// <returns>A task representing setup work.</returns>
    [GlobalSetup]
    public async Task Setup()
    {
        _rootDirectory = Path.Combine(Path.GetTempPath(), "JetBench", "LinkedText_" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
        Directory.CreateDirectory(_rootDirectory);

        string databasePath = Path.Combine(_rootDirectory, "LinkedText.accdb");
        string headeredFileName = "headered.csv";
        string headerlessFileName = "headerless.csv";

        await File.WriteAllTextAsync(
            Path.Combine(_rootDirectory, headeredFileName),
            BuildCsvSource(hasHeader: true),
            Encoding.UTF8).ConfigureAwait(false);
        await File.WriteAllTextAsync(
            Path.Combine(_rootDirectory, headerlessFileName),
            BuildCsvSource(hasHeader: false),
            Encoding.UTF8).ConfigureAwait(false);

        var writerOptions = new AccessWriterOptions { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(databasePath, DatabaseFormat.AceAccdb, writerOptions).ConfigureAwait(false))
        {
            await writer.CreateLinkedTextTableAsync(
                HeaderedTable,
                _rootDirectory,
                headeredFileName,
                "Text;HDR=YES;FMT=Delimited").ConfigureAwait(false);
            await writer.CreateLinkedTextTableAsync(
                HeaderlessTable,
                _rootDirectory,
                headerlessFileName,
                "Text;HDR=NO;FMT=Delimited").ConfigureAwait(false);
        }

        _reader = await AccessReader.OpenAsync(
            databasePath,
            new AccessReaderOptions { UseLockFile = false }).ConfigureAwait(false);
    }

    /// <summary>Disposes the open reader and removes temporary benchmark files.</summary>
    /// <returns>A task representing cleanup work.</returns>
    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_reader is not null)
        {
            await _reader.DisposeAsync().ConfigureAwait(false);
        }

        if (Directory.Exists(_rootDirectory))
        {
            try
            {
                Directory.Delete(_rootDirectory, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    /// <summary>Measures row counting through the linked text reader path.</summary>
    /// <returns>The linked text row count.</returns>
    [Benchmark]
    public async Task<long> GetRealRowCount()
        => await _reader.GetRealRowCountAsync(CurrentTableName).ConfigureAwait(false);

    /// <summary>Measures streaming linked text rows as strings.</summary>
    /// <returns>The streamed row count.</returns>
    [Benchmark]
    public async Task<int> RowsAsStrings_Streaming()
    {
        int rowCount = 0;
        await foreach (string[] row in _reader.RowsAsStrings(CurrentTableName).ConfigureAwait(false))
        {
            _ = row;
            rowCount++;
        }

        return rowCount;
    }

    /// <summary>Measures full linked text materialization into a <see cref="DataTable"/>.</summary>
    /// <returns>The materialized row count.</returns>
    [Benchmark]
    public async Task<int> ReadDataTable()
    {
        using DataTable table = await _reader.ReadDataTableAsync(CurrentTableName).ConfigureAwait(false);
        return table.Rows.Count;
    }

    private static string BuildCsvSource(bool hasHeader)
    {
        var builder = new StringBuilder(RowCount * ColumnCount * 16);
        if (hasHeader)
        {
            builder.Append("Id,Customer,Status,Note,Amount\r\n");
        }

        for (int rowIndex = 0; rowIndex < RowCount; rowIndex++)
        {
            builder.Append(rowIndex.ToString(CultureInfo.InvariantCulture));
            builder.Append(",Customer");
            builder.Append((rowIndex % 100).ToString(CultureInfo.InvariantCulture));
            builder.Append(",Status");
            builder.Append((rowIndex % 5).ToString(CultureInfo.InvariantCulture));
            builder.Append(",\"note with comma, and \"\"quote\"\" ");
            builder.Append((rowIndex % 17).ToString(CultureInfo.InvariantCulture));
            builder.Append("\",");
            builder.Append((rowIndex % 1_000).ToString(CultureInfo.InvariantCulture));
            builder.Append("\r\n");
        }

        return builder.ToString();
    }
}
