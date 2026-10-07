namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.IO;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

/// <summary>
/// Configuration options for opening a JET database with <see cref="AccessReader"/>.
/// </summary>
public sealed class AccessReaderOptions : AccessOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReaderOptions"/> class.
    /// </summary>
    public AccessReaderOptions()
        : base(useByteRangeLocks: false)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReaderOptions"/> class using a plain-text password.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text password. Null means no password.</param>
    public AccessReaderOptions(string? plainTextPassword)
        : base(plainTextPassword, useByteRangeLocks: false)
    {
    }

    /// <summary>
    /// Gets the maximum stored byte length read for one MEMO or OLE value.
    /// Default: 16,777,215, the native 24-bit long-value descriptor limit.
    /// Set a smaller positive value to limit memory used by individual values.
    /// </summary>
    public int MaxLongValueBytes { get; init; } = 0xFFFFFF;

    /// <summary>
    /// Gets the maximum uncompressed attachment content length, including its
    /// extension header. Default: 64 MiB. Set a positive value appropriate for
    /// the largest attachment the application permits before opening the file.
    /// This limit applies before decompression, including when parsing is lenient.
    /// </summary>
    public int MaxAttachmentContentBytes { get; init; } = 64 * 1024 * 1024;
    /// <summary>Gets the maximum number of pages to keep in cache. Positive values enable caching; 0 or negative disables it. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; init; } = 256;

    /// <summary>Gets a value indicating whether verbose diagnostic information is logged. Default: false.</summary>
    public bool DiagnosticsEnabled { get; init; }

    /// <summary>Gets the page-I/O optimization mode. Default: <see cref="PageReadOptimizationMode.Auto" />.</summary>
    public PageReadOptimizationMode PageReadOptimizationMode { get; init; } = PageReadOptimizationMode.Auto;

    /// <summary>Gets a value indicating whether the database format is validated on open. Default: true.</summary>
    public bool ValidateOnOpen { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether strict value parsing is enforced when converting raw column
    /// strings to their CLR types. When <see langword="true"/> (the default), values that cannot be parsed as
    /// the target type cause a <see cref="FormatException"/> to be thrown. When <see langword="false"/>,
    /// unparseable values are coerced to <see cref="DBNull.Value"/>. Unreadable
    /// MEMO/OLE values produce a trace diagnostic and a missing value: DBNull
    /// in typed rows, or null in string rows. Underlying I/O failures and resource
    /// budget refusals always propagate.
    /// </summary>
    public bool StrictParsing { get; init; } = true;

    /// <summary>Gets the file access mode. Default: Read.</summary>
    public FileAccess FileAccess { get; init; } = FileAccess.Read;

    /// <summary>
    /// Gets the file sharing mode. Default: ReadWrite (other processes may read or write while the database is open).
    /// Set to <see cref="FileShare.Read"/> to block other writers while this reader has the file open.
    /// </summary>
    public FileShare FileShare { get; init; } = FileShare.ReadWrite;

    /// <summary>
    /// Gets an optional allowlist of directories that linked-table source paths must stay under.
    /// Paths may be absolute or relative (relative entries are resolved from the opened database directory).
    /// Leave empty to allow only source files under the opened database directory, unless
    /// <see cref="LinkedSourcePathValidator"/> explicitly approves a resolved path.
    /// </summary>
    public IReadOnlyList<string> LinkedSourcePathAllowlist { get; init; } = [];

    /// <summary>
    /// Gets an optional callback to approve linked-table source paths.
    /// The callback receives linked-table metadata and the resolved absolute source path.
    /// Return true to allow opening the source; false to block it.
    /// </summary>
    public Func<LinkedTableInfo, string, bool>? LinkedSourcePathValidator { get; init; }

    /// <summary>
    /// Gets the maximum number of characters accepted in a single linked text/CSV record.
    /// Default: <c>1048576</c> characters.
    /// </summary>
    public int LinkedTextMaxRecordLength { get; init; } = 1_048_576;

    /// <summary>
    /// Gets the maximum number of decoded characters accepted in a single linked text/CSV field.
    /// Default: <c>1048576</c> characters.
    /// </summary>
    public int LinkedTextMaxFieldLength { get; init; } = 1_048_576;

    /// <summary>
    /// Gets the maximum number of columns accepted in a linked text/CSV record.
    /// Default: <c>255</c>, matching the Access table column limit.
    /// </summary>
    public int LinkedTextMaxColumnCount { get; init; } = 255;

    /// <summary>
    /// Gets an optional maximum source-file size, in bytes, for linked text/CSV read-through.
    /// Leave <see langword="null"/> to allow files of any size while still enforcing record, field, and column limits.
    /// </summary>
    public long? LinkedTextMaxSourceFileBytes { get; init; }

    /// <summary>
    /// Gets an optional maximum number of linked text/CSV rows that fully materializing APIs may load.
    /// Streaming row APIs and row-count scans are not capped by this option.
    /// </summary>
    public uint? LinkedTextMaxMaterializedRows { get; init; }

    /// <summary>
    /// Returns whether a reader opened with these options reads its pages
    /// through positional <c>RandomAccess</c> reads, which bypass the I/O gate
    /// and the shared stream position: only when the reader opened the file
    /// by path itself, so it owns a <see cref="FileStream"/> and its handle,
    /// <see cref="PageReadOptimizationMode"/> is not
    /// <see cref="PageReadOptimizationMode.Disabled"/>, and the build has
    /// <c>RandomAccess</c> (.NET 6 or later). A caller's stream, even a
    /// <see cref="FileStream"/>, keeps seek-and-read under the gate, because
    /// the caller may also use its position.
    /// <see cref="AccessReader.OpenAsync(string, AccessReaderOptions?, System.Threading.CancellationToken)"/>
    /// applies this rule, and the test harness opens files by the same rule.
    /// </summary>
    /// <param name="openedFromPath">Whether the reader opened the file by path.</param>
    /// <returns><see langword="true"/> when page reads are positional.</returns>
    internal bool UsesPositionalPageReads(bool openedFromPath)
    {
#if NET6_0_OR_GREATER
        return openedFromPath && this.PageReadOptimizationMode != PageReadOptimizationMode.Disabled;
#else
        // The netstandard2.1 build has no RandomAccess: every page is read through the stream.
        _ = openedFromPath;
        _ = this.PageReadOptimizationMode;
        return false;
#endif
    }
}
