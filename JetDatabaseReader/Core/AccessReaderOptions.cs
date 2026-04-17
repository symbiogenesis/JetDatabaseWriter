namespace JetDatabaseReader
{
    using System.IO;

    /// <summary>
    /// Configuration options for opening a JET database with <see cref="AccessReader"/>.
    /// </summary>
    public sealed class AccessReaderOptions
    {
        /// <summary>Gets or sets the maximum number of pages to keep in cache. 0 = unlimited, -1 = disabled. Default: 256 (1 MB for 4K pages).</summary>
        public int PageCacheSize { get; set; } = 256;

        /// <summary>Gets or sets a value indicating whether verbose diagnostic information is logged. Default: false.</summary>
        public bool DiagnosticsEnabled { get; set; }

        /// <summary>Gets or sets a value indicating whether parallel processing is used for reading multiple pages. Can improve performance for large tables. Default: false.</summary>
        public bool ParallelPageReadsEnabled { get; set; }

        /// <summary>Gets or sets a value indicating whether the database format is validated on open. Default: true.</summary>
        public bool ValidateOnOpen { get; set; } = true;

        /// <summary>Gets or sets the file access mode. Default: Read.</summary>
        public FileAccess FileAccess { get; set; } = FileAccess.Read;

        /// <summary>
        /// Gets or sets the file sharing mode. Default: Read (other processes may read but not write while the database is open).
        /// Set to <see cref="FileShare.ReadWrite"/> when another application (e.g. Microsoft Access) holds a write lock on the file.
        /// </summary>
        public FileShare FileShare { get; set; } = FileShare.ReadWrite;
    }
}
