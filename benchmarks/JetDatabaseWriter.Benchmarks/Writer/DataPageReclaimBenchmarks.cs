namespace JetDatabaseWriter.Benchmarks.Writer;

using System;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Compares verified row reclamation with its previous complete production algorithm.</summary>
[MemoryDiagnoser]
public class DataPageReclaimBenchmarks
{
    private JetFormat format = null!;
    private byte[] source = [];
    private byte[] generated = [];
    private byte[] control = [];
    private int selected;

    /// <summary>Gets or sets the native logical page size.</summary>
    [Params(2048, 4096)]
    public int PageSize { get; set; }

    /// <summary>Gets or sets the complete directory size, including the largest supported count.</summary>
    [Params(1, 16, 255)]
    public int Rows { get; set; }

    /// <summary>Gets or sets a value indicating whether reclaimed free bytes are securely cleared.</summary>
    [Params(false, true)]
    public bool ClearFreedSpace { get; set; }

    /// <summary>Builds stable inputs and independently compares the complete resulting images.</summary>
    /// <exception cref="InvalidOperationException">Generated reclamation differs from the previous algorithm.</exception>
    [GlobalSetup]
    public void Setup()
    {
        this.format = JetFormat.ForNewDatabase(this.PageSize == 2048 ? DatabaseFormat.Jet3Mdb : DatabaseFormat.AceAccdb);
        this.source = new byte[this.PageSize];
        this.generated = new byte[this.PageSize];
        this.control = new byte[this.PageSize];
        this.selected = this.Rows / 2;
        for (int index = 0; index < this.source.Length; index++)
        {
            this.source[index] = (byte)((index * 37) & 255);
        }

        Wu16(this.source, this.format.DataPage.NumRows, this.Rows);
        int directoryEnd = this.format.DataPage.RowsStart + (this.Rows * 2);
        int width = (this.PageSize - directoryEnd) / (this.Rows + 1);
        for (int row = 0; row < this.Rows; row++)
        {
            Wu16(this.source, this.format.DataPage.RowsStart + (row * 2), (this.PageSize - ((row + 1) * width)) | ((row % 8) << 13));
        }

        _ = this.Verified();
        _ = this.OriginalControl();
        if (!this.generated.AsSpan().SequenceEqual(this.control))
        {
            throw new InvalidOperationException("Verified and original data-page reclamation produce different images.");
        }
    }

    /// <summary>Measures generated admission and directory mutation with unchanged CLR bulk byte effects.</summary>
    /// <returns>The resulting free-space header.</returns>
    [Benchmark]
    public int Verified()
    {
        Buffer.BlockCopy(this.source, 0, this.generated, 0, this.source.Length);
        DataPageRows.ReclaimDeletedRowSpace(this.format, this.generated, this.selected, this.ClearFreedSpace);
        return Ru16(this.generated, 2);
    }

    /// <summary>Measures the complete previous implementation using the same fresh-buffer copy and erase policy.</summary>
    /// <returns>The resulting free-space header.</returns>
    /// <exception cref="InvalidOperationException">The benchmark input has invalid row bounds.</exception>
    [Benchmark(Baseline = true)]
    public int OriginalControl()
    {
        byte[] page = this.control;
        Buffer.BlockCopy(this.source, 0, page, 0, this.source.Length);
        int rows = Ru16(page, this.format.DataPage.NumRows);
        int directoryEnd = this.format.DataPage.RowsStart + (rows * 2);
        if (rows > Constants.DataPage.MaxRowsPerPage || directoryEnd > this.format.PageSize
            || !DataPageRows.TryGetSlotBound(this.format, page, this.selected, out RowBound removed))
        {
            throw new InvalidOperationException("The deleted row has invalid page bounds.");
        }

        int first = this.format.PageSize;
        for (int index = 0; index < rows; index++)
        {
            int start = Ru16(page, this.format.DataPage.RowsStart + (index * 2)) & Constants.DataPage.RowOffsetMask;
            if (start > 0 && start < first)
            {
                first = start;
            }
        }

        if (first < directoryEnd)
        {
            throw new InvalidOperationException("The data page row directory overlaps its payload.");
        }

        int end = removed.RowStart + removed.RowSize;
        Buffer.BlockCopy(page, first, page, first + removed.RowSize, removed.RowStart - first);
        if (this.ClearFreedSpace)
        {
            Array.Clear(page, first, removed.RowSize);
        }

        for (int index = 0; index < rows; index++)
        {
            int position = this.format.DataPage.RowsStart + (index * 2);
            int raw = Ru16(page, position);
            int start = raw & Constants.DataPage.RowOffsetMask;
            if (index == this.selected)
            {
                Wu16(page, position, Constants.DataPage.NonLiveRowFlags | end);
            }
            else if (start >= first && start <= removed.RowStart)
            {
                Wu16(page, position, (raw & ~Constants.DataPage.RowOffsetMask) | (start + removed.RowSize));
            }
        }

        Wu16(page, 2, first + removed.RowSize - directoryEnd);
        return Ru16(page, 2);
    }
}
