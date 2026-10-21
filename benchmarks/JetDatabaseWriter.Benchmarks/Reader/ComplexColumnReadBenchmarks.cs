namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Benchmarks.Models;
using JetDatabaseWriter.Models;

public enum ComplexColumnBenchmarkShape
{
    /// <summary>The synthetic <c>Documents</c> table: 150 rows with one 16 KB attachment each.</summary>
    Synthetic = 0,

    /// <summary>The Access-authored <c>ProductCategories.ProductCategoryImage</c> column in NorthwindTraders.accdb: 16 images.</summary>
    Northwind = 1,
}

/// <summary>
/// Reads an Attachment column three ways: <c>GetAttachmentsAsync</c> over the
/// hidden flat table, a <c>Rows()</c> scan whose attachment cells the reader
/// resolves per row, and a <c>Rows&lt;T&gt;()</c> scan whose cells are decoded
/// with <see cref="ComplexCellValue.ReadAttachments(byte[])"/>. The synthetic
/// table is writer-authored; the Northwind column is Access-authored. Setup runs
/// each benchmark once and throws unless it returns every attachment.
/// </summary>
[MemoryDiagnoser]
public class ComplexColumnReadBenchmarks
{
    private static readonly string NorthwindPath =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");

    private AccessReader? reader;
    private string tableName = null!;
    private string columnName = null!;
    private int expectedAttachments;

    [Params(ComplexColumnBenchmarkShape.Synthetic, ComplexColumnBenchmarkShape.Northwind)]
    public ComplexColumnBenchmarkShape Shape { get; set; }

    private AccessReader Reader => this.reader ?? throw new InvalidOperationException("The reader was not initialized.");

    [GlobalSetup]
    public async Task Setup()
    {
        string path;
        if (this.Shape == ComplexColumnBenchmarkShape.Synthetic)
        {
            await SyntheticDatabases.EnsureAttachmentsAsync().ConfigureAwait(false);
            path = SyntheticDatabases.AttachmentDbPath;
            (this.tableName, this.columnName) = (SyntheticDatabases.DocumentsTable, SyntheticDatabases.DocumentsAttachmentColumn);
            this.expectedAttachments = SyntheticDatabases.AttachmentDocuments;
        }
        else
        {
            if (!File.Exists(NorthwindPath))
            {
                throw new FileNotFoundException($"Benchmark database not found at '{NorthwindPath}'.", NorthwindPath);
            }

            path = NorthwindPath;
            (this.tableName, this.columnName) = ("ProductCategories", "ProductCategoryImage");
            this.expectedAttachments = 16;
        }

        this.reader = await AccessReader.OpenAsync(path).ConfigureAwait(false);

        this.Expect(nameof(this.GetAttachmentsAsync), await this.GetAttachmentsAsync().ConfigureAwait(false));
        this.Expect(nameof(this.RowsTyped_ReadAttachments), await this.RowsTyped_ReadAttachments().ConfigureAwait(false));

        // Rows() returns the raw cells; decode them once here to check they hold every attachment.
        int ordinal = await this.GetColumnOrdinalAsync().ConfigureAwait(false);
        int fromRows = 0;
        await foreach (object[] row in this.Reader.Rows(this.tableName).ConfigureAwait(false))
        {
            if (row[ordinal] is byte[] cell)
            {
                fromRows += ComplexCellValue.ReadAttachments(cell).Count;
            }
        }

        this.Expect(nameof(this.Rows_FullScan), fromRows);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Reads every attachment of the column through the hidden flat table.</summary>
    /// <returns>The attachments read.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> GetAttachmentsAsync()
    {
        IReadOnlyList<AttachmentRecord> attachments = await this.Reader.GetAttachmentsAsync(this.tableName, this.columnName).ConfigureAwait(false);
        return attachments.Count;
    }

    /// <summary>Scans the parent table with <c>Rows()</c>, which resolves each row's attachment cell.</summary>
    /// <returns>The total length of the attachment cells read.</returns>
    [Benchmark]
    public async Task<long> Rows_FullScan()
    {
        long cellBytes = 0;
        await foreach (object[] row in this.Reader.Rows(this.tableName).ConfigureAwait(false))
        {
            foreach (object value in row)
            {
                if (value is byte[] cell)
                {
                    cellBytes += cell.Length;
                }
            }
        }

        return cellBytes;
    }

    /// <summary>Scans the parent table with <c>Rows&lt;T&gt;()</c> and decodes each attachment cell.</summary>
    /// <returns>The attachments decoded.</returns>
    [Benchmark]
    public Task<int> RowsTyped_ReadAttachments() => this.Shape == ComplexColumnBenchmarkShape.Synthetic
        ? this.CountTypedAttachmentsAsync<Document>(static d => d.Files)
        : this.CountTypedAttachmentsAsync<ProductCategory>(static p => p.ProductCategoryImage);

    private async Task<int> CountTypedAttachmentsAsync<T>(Func<T, byte[]?> cell)
        where T : class, new()
    {
        int count = 0;
        await foreach (T row in this.Reader.Rows<T>(this.tableName).ConfigureAwait(false))
        {
            if (cell(row) is byte[] value)
            {
                count += ComplexCellValue.ReadAttachments(value).Count;
            }
        }

        return count;
    }

    private async Task<int> GetColumnOrdinalAsync()
    {
        IReadOnlyList<ColumnMetadata> columns = await this.Reader.GetColumnMetadataAsync(this.tableName).ConfigureAwait(false);
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, this.columnName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new InvalidOperationException($"Column '{this.columnName}' was not found on '{this.tableName}'.");
    }

    private void Expect(string benchmark, int actual)
    {
        if (actual != this.expectedAttachments)
        {
            throw new InvalidOperationException(
                $"{benchmark} returned {actual} attachments from {this.tableName}.{this.columnName}; expected {this.expectedAttachments}.");
        }
    }
}
