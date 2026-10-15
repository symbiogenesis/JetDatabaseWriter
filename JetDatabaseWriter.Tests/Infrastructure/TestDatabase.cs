namespace JetDatabaseWriter.Tests.Infrastructure;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Opens production facades with test defaults and exposes raw read helpers for format assertions.</summary>
internal static class TestDatabase
{
    /// <summary>Opens a reader through the production path, without a lock file unless requested.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="options">Optional reader options.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static ValueTask<AccessReader> OpenReaderAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
        => AccessReader.OpenAsync(path, options ?? new AccessReaderOptions { UseLockFile = false }, cancellationToken);

    /// <summary>Opens a reader through the production path, retaining the caller's stream by default.</summary>
    /// <param name="stream">The database stream.</param>
    /// <param name="options">Optional reader options.</param>
    /// <param name="leaveOpen">Whether the caller retains ownership of the stream.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static ValueTask<AccessReader> OpenReaderAsync(Stream stream, AccessReaderOptions? options = null, bool leaveOpen = true, CancellationToken cancellationToken = default)
        => AccessReader.OpenAsync(stream, options ?? new AccessReaderOptions { UseLockFile = false }, leaveOpen, cancellationToken);

    /// <summary>Opens a writer through the production path, without a lock file unless requested.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="options">Optional writer options.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static ValueTask<AccessWriter> OpenWriterAsync(string path, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
        => AccessWriter.OpenAsync(path, options ?? new AccessWriterOptions { UseLockFile = false }, cancellationToken);

    /// <summary>Opens a writer through the production path, retaining the caller's stream by default.</summary>
    /// <param name="stream">The database stream.</param>
    /// <param name="options">Optional writer options.</param>
    /// <param name="leaveOpen">Whether the caller retains ownership of the stream.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static ValueTask<AccessWriter> OpenWriterAsync(Stream stream, AccessWriterOptions? options = null, bool leaveOpen = true, CancellationToken cancellationToken = default)
        => AccessWriter.OpenAsync(stream, options ?? new AccessWriterOptions { UseLockFile = false }, leaveOpen, cancellationToken);

    /// <summary>Finds the catalog entry used by the open reader.</summary>
    /// <param name="reader">The open reader.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the lookup.</param>
    public static ValueTask<CatalogEntry?> GetCatalogEntryAsync(this AccessReader reader, string tableName, CancellationToken cancellationToken = default)
        => reader.Services.TableCatalog.GetCatalogEntryAsync(tableName, cancellationToken);

    /// <summary>Reads a table definition through the open reader.</summary>
    /// <param name="reader">The open reader.</param>
    /// <param name="tdefPage">The root table-definition page.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public static ValueTask<TableDef?> ReadTableDefAsync(this AccessReader reader, long tdefPage, CancellationToken cancellationToken = default)
        => reader.Database.TableDefs.ReadTableDefAsync(tdefPage, cancellationToken);

    /// <summary>Reads a caller-owned copy of a decrypted page through the open reader.</summary>
    /// <param name="reader">The open reader.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public static ValueTask<byte[]> ReadPageCopyAsync(this AccessReader reader, long pageNumber, CancellationToken cancellationToken = default)
        => reader.Database.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);
}
