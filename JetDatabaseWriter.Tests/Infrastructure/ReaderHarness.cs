namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;

/// <summary>
/// Opens a database through the library's internal read layers — the
/// <see cref="DatabaseFile"/> and a <see cref="ReaderServices"/> graph built
/// over it — for tests that inspect raw pages, TDEF bytes, or catalog entries.
/// Nothing goes through <see cref="AccessReader"/>, so the facade carries no
/// test-only members. No lock-file slot is taken and Agile-encrypted
/// containers are not unwrapped.
/// </summary>
internal sealed class ReaderHarness : IAsyncDisposable
{
    private ReaderHarness(DatabaseFile database, ReaderServices services)
    {
        this.Database = database;
        this.Services = services;
    }

    /// <summary>Gets the open database file.</summary>
    public DatabaseFile Database { get; }

    /// <summary>Gets the reader service graph built over <see cref="Database"/>.</summary>
    public ReaderServices Services { get; }

    /// <summary>Opens <paramref name="path"/> read-only, sharing it with any open writer.</summary>
    /// <param name="path">The database file path.</param>
    /// <param name="options">Optional reader options (password, cache size, parsing mode).</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static async ValueTask<ReaderHarness> OpenAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
    {
        FileStream stream = DatabaseFile.OpenFileStream(path, FileAccess.Read, FileShare.ReadWrite, FileOptions.Asynchronous | FileOptions.RandomAccess);
        try
        {
            return await OpenAsync(stream, options, leaveOpen: false, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Opens the database held in <paramref name="stream"/>.</summary>
    /// <param name="stream">A readable, seekable stream holding the database bytes.</param>
    /// <param name="options">Optional reader options (password, cache size, parsing mode).</param>
    /// <param name="leaveOpen">Whether <paramref name="stream"/> stays open after the harness is disposed.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static async ValueTask<ReaderHarness> OpenAsync(Stream stream, AccessReaderOptions? options = null, bool leaveOpen = true, CancellationToken cancellationToken = default)
    {
        options ??= new AccessReaderOptions { UseLockFile = false };
        string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
        byte[] header = await DatabaseFile.ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);
        var database = new DatabaseFile(stream, header, options.Password, path, leaveOpen, typeof(AccessReader), writable: false);
        try
        {
            return new ReaderHarness(database, new ReaderServices(database, options));
        }
        catch
        {
            await database.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Finds a user table's catalog entry by name (case-insensitive).</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the lookup.</param>
    public ValueTask<CatalogEntry?> GetCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
        => this.Services.TableCatalog.GetCatalogEntryAsync(tableName, cancellationToken);

    /// <summary>Reads and parses the table definition rooted at <paramref name="tdefPage"/>.</summary>
    /// <param name="tdefPage">The TDEF page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken = default)
        => this.Database.ReadTableDefAsync(tdefPage, cancellationToken);

    /// <summary>Returns a caller-owned copy of a decrypted page.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public ValueTask<byte[]> ReadPageCopyAsync(long pageNumber, CancellationToken cancellationToken = default)
        => this.Database.ReadPageCopyAsync(pageNumber, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        this.Services.Dispose();
        await this.Database.DisposeAsync().ConfigureAwait(false);
    }
}
