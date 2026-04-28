namespace JetDatabaseWriter.Core;

using System;
using System.Collections.Generic;
using System.IO;
using JetDatabaseWriter.Models;

/// <summary>
/// Configuration options for opening a JET database with <see cref="AccessReader"/>.
/// </summary>
public sealed class AccessReaderOptions : IAccessOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReaderOptions"/> class.
    /// </summary>
    public AccessReaderOptions()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReaderOptions"/> class using a plain-text password.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text password. Null means no password.</param>
    public AccessReaderOptions(string? plainTextPassword)
    {
        Password = plainTextPassword.AsMemory();
    }

    /// <summary>Gets the maximum number of pages to keep in cache. 0 = unlimited, -1 = disabled. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; init; } = 256;

    /// <summary>Gets a value indicating whether verbose diagnostic information is logged. Default: false.</summary>
    public bool DiagnosticsEnabled { get; init; }

    /// <summary>Gets a value indicating whether parallel processing is used for reading multiple pages. Can improve performance for large tables. Default: false.</summary>
    public bool ParallelPageReadsEnabled { get; init; }

    /// <summary>Gets a value indicating whether the database format is validated on open. Default: true.</summary>
    public bool ValidateOnOpen { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether strict value parsing is enforced when converting raw column
    /// strings to their CLR types. When <see langword="true"/> (the default), values that cannot be parsed as
    /// the target type cause a <see cref="FormatException"/> to be thrown. When <see langword="false"/>,
    /// unparseable values are silently coerced to <see cref="DBNull.Value"/>.
    /// </summary>
    public bool StrictParsing { get; init; } = true;

    /// <summary>Gets the file access mode. Default: Read.</summary>
    public FileAccess FileAccess { get; init; } = FileAccess.Read;

    /// <summary>
    /// Gets the file sharing mode. Default: Read (other processes may read but not write while the database is open).
    /// Set to <see cref="FileShare.ReadWrite"/> when another application (e.g. Microsoft Access) holds a write lock on the file.
    /// </summary>
    public FileShare FileShare { get; init; } = FileShare.ReadWrite;

    /// <summary>
    /// Gets the password for opening encrypted databases.
    /// Empty (the default) means no decryption is attempted.
    /// Supports Jet3 (XOR), Jet4 (RC4), and ACCDB (AES) encryption.
    /// </summary>
    public ReadOnlyMemory<char> Password { get; init; }

    /// <summary>
    /// Gets a value indicating whether a lockfile (.ldb / .laccdb) is created
    /// alongside the database while it is open, and deleted on dispose.
    /// Default: true.
    /// </summary>
    public bool UseLockFile { get; init; } = true;

    /// <summary>
    /// Gets an optional allowlist of directories that linked-table source paths must stay under.
    /// Paths may be absolute or relative (relative entries are resolved from the opened database directory).
    /// Leave empty to allow any directory.
    /// </summary>
    public IReadOnlyList<string> LinkedSourcePathAllowlist { get; init; } = [];

    /// <summary>
    /// Gets an optional callback to approve linked-table source paths.
    /// The callback receives linked-table metadata and the resolved absolute source path.
    /// Return true to allow opening the source; false to block it.
    /// </summary>
    public Func<LinkedTableInfo, string, bool>? LinkedSourcePathValidator { get; init; }
}
