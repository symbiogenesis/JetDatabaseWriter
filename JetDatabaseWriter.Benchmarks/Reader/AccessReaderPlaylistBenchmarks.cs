namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.TestSupport;

/// <summary>
/// Retains the consumed metadata and exact OLE arrays in one typed scan. Notes
/// are intentionally unbound. Setup checks every row and byte and reports cold
/// physical page I/O separately, without instrumenting the timed file reader.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderPlaylistBenchmarks
{
    private const int RowCount = 20_017;
    private const string TableName = "Playlists";
    private AccessReader? reader;
    private string databasePath = string.Empty;

    /// <summary>Gets or sets the definition distribution.</summary>
    [Params(PlaylistDefinitionShape.Sparse, PlaylistDefinitionShape.LargeSortOnly)]
    public PlaylistDefinitionShape Shape { get; set; }

    /// <summary>Creates the fixture and verifies the complete consumer contract.</summary>
    /// <returns>A task that completes after fixture validation.</returns>
    [GlobalSetup]
    public async Task Setup()
    {
        this.databasePath = Path.Combine(Path.GetTempPath(), $"jdw-playlists-v3-{this.Shape}.accdb");
        await this.EnsureDatabaseAsync().ConfigureAwait(false);
        this.reader = await AccessReader.OpenAsync(this.databasePath).ConfigureAwait(false);
        List<SnapshotPlaylistRow> snapshot = await this.TypedScan().ConfigureAwait(false);
        this.Verify(snapshot);

        await this.ReportIoAsync(hybrid: false).ConfigureAwait(false);
        await this.ReportIoAsync(hybrid: true).ConfigureAwait(false);
    }

    /// <summary>Disposes the primed reader.</summary>
    /// <returns>A task that completes after disposing the reader.</returns>
    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Retains one complete metadata/definition snapshot from the primed reader.</summary>
    /// <returns>Every playlist in stored order, with exact definition bytes.</returns>
    /// <exception cref="InvalidOperationException">Setup has not opened the reader.</exception>
    [Benchmark]
    public Task<List<SnapshotPlaylistRow>> TypedScan() => ScanAsync(this.reader ?? throw new InvalidOperationException("Setup has not opened the reader."), hybrid: true);

    private static async Task<List<SnapshotPlaylistRow>> ScanAsync(AccessReader source, bool hybrid)
    {
        var rows = new List<SnapshotPlaylistRow>(RowCount);
        await foreach (SnapshotPlaylistRow row in (hybrid ? source.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<SnapshotPlaylistRow>(TableName, enableHybridOle: true, forceProjection: false, progress: null, cancellationToken: default) : source.Rows<SnapshotPlaylistRow>(TableName)).ConfigureAwait(false))
        {
            rows.Add(row);
        }

        return rows;
    }

    private static byte[] Payload(int id, int length)
    {
        byte[] bytes = new byte[length];
        for (int index = 0; index < bytes.Length; index++)
        {
            bytes[index] = (byte)((id + (index * 17)) & 255);
        }

        return bytes;
    }

    private static string Name(int id) => "Playlist " + id.ToString(CultureInfo.InvariantCulture);

    private static bool Equal(byte[]? actual, byte[]? expected) => actual is null
        ? expected is null
        : expected is not null && actual.AsSpan().SequenceEqual(expected);

    private (byte[]? Filter, byte[]? SortOrder) Definitions(int id)
    {
        if (this.Shape == PlaylistDefinitionShape.LargeSortOnly)
        {
            return (id % 7 == 0 ? Payload(id, 37) : null, Payload(id, 4096));
        }

        byte[]? filter = id % 7 == 0 ? Payload(id, 37) : null;
        byte[]? sort = id % 10 == 0 ? Payload(id + 1, 120) : null;
        return (filter, sort);
    }

    private void Verify(List<SnapshotPlaylistRow> rows)
    {
        if (rows.Count != RowCount)
        {
            throw new InvalidOperationException($"Expected {RowCount} playlists; got {rows.Count}.");
        }

        for (int id = 0; id < RowCount; id++)
        {
            SnapshotPlaylistRow row = rows[id];
            (byte[]? filter, byte[]? sort) = this.Definitions(id);
            if (row.Id != id || row.Name != Name(id) || row.ParentId != id / 100 || row.Position != id % 100
                || row.IsDynamic != (filter is not null || sort is not null)
                || !Equal(row.Filter, filter) || !Equal(row.SortOrder, sort))
            {
                throw new InvalidOperationException($"Playlist metadata or exact definition bytes differ at row {id} ({this.Shape}).");
            }
        }
    }

    private async Task ReportIoAsync(bool hybrid)
    {
        await using FileStream file = File.OpenRead(this.databasePath);
        await using var trace = new PageTraceStream(file);
        await using AccessReader measured = await AccessReader.OpenAsync(trace, leaveOpen: true).ConfigureAwait(false);
        trace.Reset();
        List<SnapshotPlaylistRow> cold = await ScanAsync(measured, hybrid).ConfigureAwait(false);
        this.Verify(cold);
        long reads = trace.ReadCounts(4096).Values.Sum();
        long retained = cold.Sum(static row => (long)(row.Filter?.Length ?? 0) + (row.SortOrder?.Length ?? 0));
        Console.WriteLine($"Playlist IO: shape={this.Shape}; hybrid={hybrid}; rows={cold.Count}; retainedDefinitionBytes={retained}; pageReads={reads}; bytesRead={trace.BytesRead}; uniquePages={trace.PagesRead(4096).Count}; notesBound=false");
    }

    private async Task EnsureDatabaseAsync()
    {
        if (File.Exists(this.databasePath))
        {
            return;
        }

        string building = this.databasePath + "." + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture) + ".building";
        try
        {
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(building, DatabaseFormat.AceAccdb).ConfigureAwait(false))
            {
                await writer.CreateTableAsync(
                    TableName,
                    [
                        new("Id", typeof(int)),
                        new("Name", typeof(string), 100),
                        new("ParentId", typeof(int)),
                        new("Position", typeof(int)),
                        new("IsDynamic", typeof(bool)),
                        new("Filter", typeof(byte[])),
                        new("SortOrder", typeof(byte[])),
                        new("Notes", typeof(string)),
                    ]).ConfigureAwait(false);
                var rows = new List<object[]>(RowCount);
                string notes = new('N', 4096);
                for (int id = 0; id < RowCount; id++)
                {
                    (byte[]? filter, byte[]? sort) = this.Definitions(id);
                    rows.Add([id, Name(id), id / 100, id % 100, filter is not null || sort is not null, filter!, sort!, notes]);
                }

                await writer.InsertRowsAsync(TableName, rows).ConfigureAwait(false);
            }

            File.Move(building, this.databasePath, overwrite: true);
        }
        finally
        {
            File.Delete(building);
        }
    }
}
