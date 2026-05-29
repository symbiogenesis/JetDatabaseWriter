namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Models;

/// <summary>
/// Compares DataTable insertion strategies against the public reader path.
/// The manual variants consume <c>AccessReader.Rows</c> so they focus on
/// DataTable materialization cost after typed row decode has already happened.
/// </summary>
[MemoryDiagnoser]
public class DataTableMaterializationBenchmarks
{
    private AccessReader _numericReader = null!;
    private AccessReader _textReader = null!;
    private List<ColumnMetadata> _numericMetadata = null!;
    private List<ColumnMetadata> _textMetadata = null!;
    private int _numericRows;
    private int _textRows;

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureAllAsync().ConfigureAwait(false);
        _numericReader = await AccessReader.OpenAsync(SyntheticDatabases.NumericDbPath).ConfigureAwait(false);
        _textReader = await AccessReader.OpenAsync(SyntheticDatabases.TextDbPath).ConfigureAwait(false);
        _numericMetadata = await _numericReader.GetColumnMetadataAsync(SyntheticDatabases.NumericTable).ConfigureAwait(false);
        _textMetadata = await _textReader.GetColumnMetadataAsync(SyntheticDatabases.TextTable).ConfigureAwait(false);
        _numericRows = checked((int)await _numericReader.GetRealRowCountAsync(SyntheticDatabases.NumericTable).ConfigureAwait(false));
        _textRows = checked((int)await _textReader.GetRealRowCountAsync(SyntheticDatabases.TextTable).ConfigureAwait(false));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _numericReader.DisposeAsync().ConfigureAwait(false);
        await _textReader.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<int> Numeric_PublicReadDataTable()
    {
        using DataTable table = await _numericReader.ReadDataTableAsync(SyntheticDatabases.NumericTable).ConfigureAwait(false);
        return table.Rows.Count;
    }

    [Benchmark]
    public async Task<int> Numeric_NewRow()
        => await MaterializeWithNewRowAsync(
            _numericReader,
            SyntheticDatabases.NumericTable,
            _numericMetadata,
            beginLoadData: false,
            minimumCapacity: 0).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Numeric_NewRow_BeginLoadData()
        => await MaterializeWithNewRowAsync(
            _numericReader,
            SyntheticDatabases.NumericTable,
            _numericMetadata,
            beginLoadData: true,
            minimumCapacity: 0).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Numeric_NewRow_BeginLoadData_MinimumCapacity()
        => await MaterializeWithNewRowAsync(
            _numericReader,
            SyntheticDatabases.NumericTable,
            _numericMetadata,
            beginLoadData: true,
            _numericRows).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Numeric_RowsAddObjectArray_BeginLoadData_MinimumCapacity()
        => await MaterializeWithRowsAddAsync(
            _numericReader,
            SyntheticDatabases.NumericTable,
            _numericMetadata,
            _numericRows).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Numeric_LoadDataRow_BeginLoadData_MinimumCapacity()
        => await MaterializeWithLoadDataRowAsync(
            _numericReader,
            SyntheticDatabases.NumericTable,
            _numericMetadata,
            _numericRows).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Text_PublicReadDataTable()
    {
        using DataTable table = await _textReader.ReadDataTableAsync(SyntheticDatabases.TextTable).ConfigureAwait(false);
        return table.Rows.Count;
    }

    [Benchmark]
    public async Task<int> Text_NewRow_BeginLoadData_MinimumCapacity()
        => await MaterializeWithNewRowAsync(
            _textReader,
            SyntheticDatabases.TextTable,
            _textMetadata,
            beginLoadData: true,
            _textRows).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Text_RowsAddObjectArray_BeginLoadData_MinimumCapacity()
        => await MaterializeWithRowsAddAsync(
            _textReader,
            SyntheticDatabases.TextTable,
            _textMetadata,
            _textRows).ConfigureAwait(false);

    [Benchmark]
    public async Task<int> Text_LoadDataRow_BeginLoadData_MinimumCapacity()
        => await MaterializeWithLoadDataRowAsync(
            _textReader,
            SyntheticDatabases.TextTable,
            _textMetadata,
            _textRows).ConfigureAwait(false);

    private static async Task<int> MaterializeWithNewRowAsync(
        AccessReader reader,
        string tableName,
        IReadOnlyList<ColumnMetadata> metadata,
        bool beginLoadData,
        int minimumCapacity)
    {
        using DataTable table = CreateDataTable(tableName, metadata, minimumCapacity);
        if (beginLoadData)
        {
            table.BeginLoadData();
        }

        await foreach (object[] sourceRow in reader.Rows(tableName).ConfigureAwait(false))
        {
            DataRow targetRow = table.NewRow();
            for (int columnIndex = 0; columnIndex < sourceRow.Length; columnIndex++)
            {
                targetRow[columnIndex] = sourceRow[columnIndex] ?? DBNull.Value;
            }

            table.Rows.Add(targetRow);
        }

        if (beginLoadData)
        {
            table.EndLoadData();
        }

        return table.Rows.Count;
    }

    private static async Task<int> MaterializeWithRowsAddAsync(
        AccessReader reader,
        string tableName,
        IReadOnlyList<ColumnMetadata> metadata,
        int minimumCapacity)
    {
        using DataTable table = CreateDataTable(tableName, metadata, minimumCapacity);
        table.BeginLoadData();

        await foreach (object[] sourceRow in reader.Rows(tableName).ConfigureAwait(false))
        {
            table.Rows.Add(sourceRow);
        }

        table.EndLoadData();
        return table.Rows.Count;
    }

    private static async Task<int> MaterializeWithLoadDataRowAsync(
        AccessReader reader,
        string tableName,
        IReadOnlyList<ColumnMetadata> metadata,
        int minimumCapacity)
    {
        using DataTable table = CreateDataTable(tableName, metadata, minimumCapacity);
        table.BeginLoadData();

        await foreach (object[] sourceRow in reader.Rows(tableName).ConfigureAwait(false))
        {
            _ = table.LoadDataRow(sourceRow, fAcceptChanges: false);
        }

        table.EndLoadData();
        return table.Rows.Count;
    }

    private static DataTable CreateDataTable(string tableName, IReadOnlyList<ColumnMetadata> metadata, int minimumCapacity)
    {
        var table = new DataTable(tableName);
        if (minimumCapacity > 0)
        {
            table.MinimumCapacity = minimumCapacity;
        }

        foreach (ColumnMetadata column in metadata)
        {
            _ = table.Columns.Add(column.Name, column.ClrType);
        }

        return table;
    }
}
