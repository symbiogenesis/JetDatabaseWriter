namespace JetDatabaseReader
{
    using System;

    /// <summary>
    /// Detailed progress information for long-running read operations.
    /// </summary>
    public sealed class ReadProgress
    {
        /// <summary>Gets or sets the number of pages read so far.</summary>
        public long PagesRead { get; set; }

        /// <summary>Gets or sets the total number of pages to read.</summary>
        public long TotalPages { get; set; }

        /// <summary>Gets or sets the number of rows processed so far.</summary>
        public int RowsProcessed { get; set; }

        /// <summary>Gets or sets the time elapsed since the operation started.</summary>
        public TimeSpan Elapsed { get; set; }

        /// <summary>Gets the percentage of pages read (0-100).</summary>
        public double PercentComplete => TotalPages > 0 ? (PagesRead * 100.0 / TotalPages) : 0;

        /// <summary>Gets or sets the estimated number of rows remaining (based on average rows per page).</summary>
        public int EstimatedRowsRemaining { get; set; }

        /// <summary>Gets or sets the current table being processed.</summary>
        public string TableName { get; set; }
    }
}
