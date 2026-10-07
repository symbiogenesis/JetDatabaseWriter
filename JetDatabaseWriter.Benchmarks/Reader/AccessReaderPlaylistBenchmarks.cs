namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
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
        this.databasePath = Path.Combine(Path.GetTempPath(), $"jdw-playlists-v4-{this.Shape}.accdb");
        await this.EnsureDatabaseAsync().ConfigureAwait(false);
        this.reader = await AccessReader.OpenAsync(this.databasePath).ConfigureAwait(false);
        await VerifySchemaAsync(this.reader).ConfigureAwait(false);
        List<SnapshotPlaylistRow> snapshot = await this.TypedScan().ConfigureAwait(false);
        this.Verify(snapshot);

        List<SnapshotPlaylistRow> fallback = await ScanAsync(this.reader, hybrid: false).ConfigureAwait(false);
        List<SnapshotPlaylistRow> candidate = await ScanAsync(this.reader, hybrid: true).ConfigureAwait(false);
        this.Verify(fallback);
        this.Verify(candidate);
        VerifyOrder(fallback, candidate);

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

    private static async Task VerifySchemaAsync(AccessReader source)
    {
        IReadOnlyList<ColumnMetadata> columns = await source.GetColumnMetadataAsync(TableName).ConfigureAwait(false);
        foreach (ColumnMetadata column in columns)
        {
            string expectedType = column.Name switch
            {
                "CreationDate" or "ArtistSort" or "Title" => "Text",
                "Notes" => "Memo",
                "Filter" or "SortOrder" => "OLE Object",
                _ => column.TypeName,
            };
            if (column.TypeName != expectedType)
            {
                throw new InvalidOperationException($"Fixture column {column.Name}: actual type={column.TypeName}, expected type={expectedType}.");
            }
        }
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

    private static SnapshotPlaylistRow Metadata(int id) => new()
    {
        PlaylistID = id,
        CreationDate = id % 13 == 0 ? null : "2026-09-" + ((id % 28) + 1).ToString("D2", CultureInfo.InvariantCulture),
        Favorite = id % 5 == 0,
        ArtistSort = id % 11 == 0 ? null : "Artist " + (id % 347).ToString(CultureInfo.InvariantCulture),
        Title = "Playlist " + id.ToString(CultureInfo.InvariantCulture),
        Random = id % 3 == 0,
        Max = id % 17 == 0 ? null : id % 500,
        Shuffle = id % 4 == 0,
        Reload = id % 6 == 0,
        PlayingTime = id % 19 == 0 ? null : id * 123,
        NumberOfTracks = id % 23 == 0 ? null : (short)(id % 1000),
    };

    private static void VerifyOrder(List<SnapshotPlaylistRow> fallback, List<SnapshotPlaylistRow> candidate)
    {
        for (int position = 0; position < fallback.Count; position++)
        {
            if (fallback[position].PlaylistID != candidate[position].PlaylistID)
            {
                throw new InvalidOperationException($"Stored row order differs at position {position}: fallback PlaylistID={fallback[position].PlaylistID}, hybrid PlaylistID={candidate[position].PlaylistID}.");
            }
        }
    }

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
            throw new InvalidOperationException($"Expected {RowCount} playlists; got {rows.Count} ({this.Shape}).");
        }

        var seen = new HashSet<int>();
        for (int position = 0; position < rows.Count; position++)
        {
            SnapshotPlaylistRow actual = rows[position];
            int id = actual.PlaylistID;
            if (id < 0 || id >= RowCount || !seen.Add(id))
            {
                throw new InvalidOperationException($"Invalid or duplicate PlaylistID {id} at stored position {position} ({this.Shape}).");
            }

            SnapshotPlaylistRow expected = Metadata(id);
            (byte[]? filter, byte[]? sort) = this.Definitions(id);
            var differences = new List<string>();
            Check(nameof(actual.CreationDate), actual.CreationDate, expected.CreationDate);
            Check(nameof(actual.Favorite), actual.Favorite, expected.Favorite);
            Check(nameof(actual.ArtistSort), actual.ArtistSort, expected.ArtistSort);
            Check(nameof(actual.Title), actual.Title, expected.Title);
            Check(nameof(actual.Random), actual.Random, expected.Random);
            Check(nameof(actual.Max), actual.Max, expected.Max);
            Check(nameof(actual.Shuffle), actual.Shuffle, expected.Shuffle);
            Check(nameof(actual.Reload), actual.Reload, expected.Reload);
            Check(nameof(actual.PlayingTime), actual.PlayingTime, expected.PlayingTime);
            Check(nameof(actual.NumberOfTracks), actual.NumberOfTracks, expected.NumberOfTracks);
            CheckBytes(nameof(actual.Filter), actual.Filter, filter);
            CheckBytes(nameof(actual.SortOrder), actual.SortOrder, sort);
            if (differences.Count != 0)
            {
                throw new InvalidOperationException($"PlaylistID={id}, stored position={position}, shape={this.Shape}: {string.Join("; ", differences)}");
            }

            void Check<T>(string field, T value, T wanted)
            {
                if (!EqualityComparer<T>.Default.Equals(value, wanted))
                {
                    differences.Add($"{field}: actual={((object?)value ?? "<null>")}, expected={((object?)wanted ?? "<null>")}");
                }
            }

            void CheckBytes(string field, byte[]? value, byte[]? wanted)
            {
                if (!Equal(value, wanted))
                {
                    differences.Add($"{field}: actualLength={value?.Length.ToString(CultureInfo.InvariantCulture) ?? "null"}, expectedLength={wanted?.Length.ToString(CultureInfo.InvariantCulture) ?? "null"}, exactBytes=false");
                }
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
                        new("PlaylistID", typeof(int)),
                        new("CreationDate", typeof(string), 40),
                        new("Favorite", typeof(bool)),
                        new("ArtistSort", typeof(string), 100),
                        new("Title", typeof(string), 100),
                        new("Filter", typeof(byte[])),
                        new("SortOrder", typeof(byte[])),
                        new("Random", typeof(bool)),
                        new("Max", typeof(int)),
                        new("Shuffle", typeof(bool)),
                        new("Reload", typeof(bool)),
                        new("PlayingTime", typeof(int)),
                        new("NumberOfTracks", typeof(short)),
                        new("Notes", typeof(string)),
                    ]).ConfigureAwait(false);
                var rows = new List<object[]>(RowCount);
                string notes = new('N', 4096);
                for (int id = 0; id < RowCount; id++)
                {
                    (byte[]? filter, byte[]? sort) = this.Definitions(id);
                    SnapshotPlaylistRow row = Metadata(id);
                    rows.Add([id, row.CreationDate!, row.Favorite!, row.ArtistSort!, row.Title!, filter!, sort!, row.Random!, row.Max!, row.Shuffle!, row.Reload!, row.PlayingTime!, row.NumberOfTracks!, notes]);
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
