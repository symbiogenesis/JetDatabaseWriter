namespace JetDatabaseWriter.Benchmarks.Reader;

/// <summary>The exact metadata and opaque definitions retained by the snapshot consumer.</summary>
public sealed class SnapshotPlaylistRow
{
    public int Id { get; set; }

    public string? Name { get; set; }

    public int ParentId { get; set; }

    public int Position { get; set; }

    public bool IsDynamic { get; set; }

#pragma warning disable CA1819 // Rows<T> maps the exact OLE payload to a byte[] property.
    public byte[]? Filter { get; set; }

    public byte[]? SortOrder { get; set; }
#pragma warning restore CA1819
}
