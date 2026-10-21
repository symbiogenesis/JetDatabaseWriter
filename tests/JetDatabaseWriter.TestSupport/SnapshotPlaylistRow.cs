namespace JetDatabaseWriter.TestSupport;

#pragma warning disable CA1515 // BenchmarkDotNet exposes the retained snapshot type from public benchmark methods.

/// <summary>The metadata and opaque definitions retained by the playlist snapshot consumer.</summary>
public sealed class SnapshotPlaylistRow
{
    /// <summary>Gets or sets the stored playlist identity.</summary>
    public int PlaylistID { get; set; }

    /// <summary>Gets or sets the creation date text.</summary>
    public string? CreationDate { get; set; }

    /// <summary>Gets or sets the favorite flag.</summary>
    public bool? Favorite { get; set; }

    /// <summary>Gets or sets the artist sort text.</summary>
    public string? ArtistSort { get; set; }

    /// <summary>Gets or sets the title.</summary>
    public string? Title { get; set; }

#pragma warning disable CA1819 // Rows<T> maps exact opaque OLE arrays retained by the snapshot consumer.
    /// <summary>Gets or sets the exact filter definition bytes.</summary>
    public byte[]? Filter { get; set; }

    /// <summary>Gets or sets the exact sort definition bytes.</summary>
    public byte[]? SortOrder { get; set; }
#pragma warning restore CA1819

    /// <summary>Gets or sets the random flag.</summary>
    public bool? Random { get; set; }

    /// <summary>Gets or sets the maximum track count.</summary>
    public int? Max { get; set; }

    /// <summary>Gets or sets the shuffle flag.</summary>
    public bool? Shuffle { get; set; }

    /// <summary>Gets or sets the reload flag.</summary>
    public bool? Reload { get; set; }

    /// <summary>Gets or sets the playing time.</summary>
    public int? PlayingTime { get; set; }

    /// <summary>Gets or sets the number of tracks.</summary>
    public short? NumberOfTracks { get; set; }
}
