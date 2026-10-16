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
/// End-to-end linked text table benchmarks over a headered CSV source.
/// </summary>
[MemoryDiagnoser]
public class LinkedTextReadBenchmarks
{
    private const string HeaderedTable = "LinkedHeaderedCsv";
    private const int RowCount = 25_000;
    private const int ColumnCount = 5;

    private AccessReader reader = null!;
    private string rootDirectory = string.Empty;

    /// <summary>Creates a temporary front-end database and linked CSV source.</summary>
    /// <returns>A task representing setup work.</returns>
    [GlobalSetup]
    public async Task Setup()
    {
        this.rootDirectory = Path.Combine(Path.GetTempPath(), "JetBench", "LinkedText_" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
        Directory.CreateDirectory(this.rootDirectory);

        string databasePath = Path.Combine(this.rootDirectory, "LinkedText.accdb");
        const string headeredFileName = "headered.csv";

        string sourcePath = Path.Combine(this.rootDirectory, headeredFileName);
        await File.WriteAllTextAsync(
            sourcePath,
            BuildCsvSource(),
            Encoding.UTF8).ConfigureAwait(false);

        var writerOptions = new AccessWriterOptions { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(databasePath, DatabaseFormat.AceAccdb, writerOptions).ConfigureAwait(false))
        {
            await writer.CreateLinkedTextTableAsync(
                HeaderedTable,
                this.rootDirectory,
                headeredFileName,
                "Text;HDR=YES;FMT=Delimited").ConfigureAwait(false);
        }

        this.reader = await AccessReader.OpenAsync(
            databasePath,
            new AccessReaderOptions
            {
                UseLockFile = false,
                LinkedSourceResolver = (link, _, token) =>
                {
                    token.ThrowIfCancellationRequested();
                    if (link.Name != HeaderedTable)
                    {
                        throw new UnauthorizedAccessException("Only the fixture source is registered.");
                    }

                    return new ValueTask<Stream>(new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read));
                },
            }).ConfigureAwait(false);
    }

    /// <summary>Disposes the open reader and removes temporary benchmark files.</summary>
    /// <returns>A task representing cleanup work.</returns>
    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }

        if (Directory.Exists(this.rootDirectory))
        {
            try
            {
                Directory.Delete(this.rootDirectory, recursive: true);
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
        => await this.reader.GetRealRowCountAsync(HeaderedTable).ConfigureAwait(false);

    /// <summary>Measures streaming linked text rows as strings.</summary>
    /// <returns>The streamed row count.</returns>
    [Benchmark]
    public async Task<int> RowsAsStrings_Streaming()
    {
        int rowCount = 0;
        await foreach (string[] row in this.reader.RowsAsStrings(HeaderedTable).ConfigureAwait(false))
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
        using DataTable table = await this.reader.ReadTableAsync(HeaderedTable).ConfigureAwait(false);
        return table.Rows.Count;
    }

    private static string BuildCsvSource()
    {
        var builder = new StringBuilder(RowCount * ColumnCount * 16);
        builder.Append("Id,Customer,Status,Note,Amount\r\n");

        for (int rowIndex = 0; rowIndex < RowCount; rowIndex++)
        {
            builder.Append(rowIndex.ToString(CultureInfo.InvariantCulture))
                .Append(",Customer")
                .Append((rowIndex % 100).ToString(CultureInfo.InvariantCulture))
                .Append(",Status")
                .Append((rowIndex % 5).ToString(CultureInfo.InvariantCulture))
                .Append(",\"note with comma, and \"\"quote\"\" ")
                .Append((rowIndex % 17).ToString(CultureInfo.InvariantCulture))
                .Append("\",")
                .Append((rowIndex % 1_000).ToString(CultureInfo.InvariantCulture))
                .Append("\r\n");
        }

        return builder.ToString();
    }
}
