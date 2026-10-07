namespace JetDatabaseWriter.Benchmarks.Reader;

/// <summary>The metadata and opaque definitions retained by MediaLibrary's snapshot consumer.</summary>
public sealed class SnapshotPlaylistRow
{
    public int PlaylistID { get; set; }

    public string? CreationDate { get; set; }

    public bool? Favorite { get; set; }

    public string? ArtistSort { get; set; }

    public string? Title { get; set; }

#pragma warning disable CA1819 // Rows<T> maps exact opaque OLE arrays retained by the snapshot consumer.
    public byte[]? Filter { get; set; }

    public byte[]? SortOrder { get; set; }
#pragma warning restore CA1819

    public bool? Random { get; set; }

    public int? Max { get; set; }

    public bool? Shuffle { get; set; }

    public bool? Reload { get; set; }

    public int? PlayingTime { get; set; }

    public short? NumberOfTracks { get; set; }
}
