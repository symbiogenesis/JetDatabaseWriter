namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;

/// <summary>
/// The format probe's view of a database: the library's internal page and
/// catalog layers (<see cref="DatabaseFile"/> plus a <see cref="ReaderServices"/>
/// graph) opened directly, with the raw-page and raw-TDEF accessors the probes
/// dump alongside the handful of ordinary reads they compare against. The
/// diagnostic accessors live here, in the tool, so the public
/// <see cref="AccessReader"/> facade carries none of them. No lock-file slot is
/// taken and Agile-encrypted containers are not unwrapped.
/// </summary>
internal sealed class ProbeDatabase : IAsyncDisposable
{
    private readonly DatabaseFile db;
    private readonly ReaderServices services;

    private ProbeDatabase(DatabaseFile db, ReaderServices services)
    {
        this.db = db;
        this.services = services;
    }

    internal JetFormat Format => this.db.Format;

    public DatabaseFormat DatabaseFormat => this.db.Format.Kind;

    public int PageSize => this.db.Format.PageSize;

    public int CodePage => this.db.Format.CodePage;

    /// <summary>Gets the path the database was opened from.</summary>
    public string HostDatabasePath => this.db.DatabasePath;

    /// <summary>Gets the data-page header offsets for this format.</summary>
    public DataPageLayout DataPage => this.db.Format.DataPage;

    /// <summary>Gets the row-trailer field sizes for this format.</summary>
    public RowFieldSizes RowFields => this.db.Format.RowFields;

    /// <summary>Opens <paramref name="path"/> read-only, sharing it with any other reader or writer.</summary>
    /// <param name="path">The database file path.</param>
    /// <param name="options">Optional reader options (password, cache size, parsing mode).</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static async Task<ProbeDatabase> OpenAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new AccessReaderOptions { UseLockFile = false };
#pragma warning disable CA2000 // The database file owns the stream once it is open, and the catch disposes the stream when the open throws first.
        FileStream stream = PageFile.OpenFileStream(path, FileAccess.Read, FileShare.ReadWrite, FileOptions.Asynchronous | FileOptions.RandomAccess);
#pragma warning restore CA2000 // The database file owns the stream once it is open, and the catch disposes the stream when the open throws first.
        DatabaseFile? db = null;
        try
        {
            byte[] header = await PageFile.ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);
            db = DatabaseFile.ForReader(stream, header, options.Password, path, leaveOpen: false);
            return new ProbeDatabase(db, new ReaderServices(db, options));
        }
        catch
        {
            if (db is not null)
            {
                await db.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <summary>Returns a caller-owned copy of a decrypted page.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public ValueTask<byte[]> GetRawPageBytesAsync(long pageNumber, CancellationToken cancellationToken = default)
        => this.db.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);

    /// <summary>Returns a caller-owned copy of a decrypted page.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        => this.db.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);

    /// <summary>
    /// Returns the concatenated TDEF page-chain bytes for <paramref name="tdefPage"/>
    /// (header kept on the first page, stripped from continuations), or
    /// <see langword="null"/> when the page is not a valid TDEF root.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public ValueTask<byte[]?> GetRawTDefBytesAsync(long tdefPage, CancellationToken cancellationToken = default)
        => this.db.TableDefs.ReadTDefBytesAsync(tdefPage, cancellationToken);

    /// <summary>Loads the <c>MSysObjects</c> table definition (page 2).</summary>
    /// <param name="cancellationToken">A token used to cancel the read.</param>
    public ValueTask<TableDef?> GetMSysObjectsTableDefAsync(CancellationToken cancellationToken = default)
        => this.services.Catalog.GetMSysObjectsTableDefAsync(cancellationToken);

    /// <summary>Enumerates every <c>MSysObjects</c> row decoded as strings.</summary>
    /// <param name="msys">The <c>MSysObjects</c> table definition.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    public IAsyncEnumerable<string[]> EnumerateMSysObjectsRowsAsync(TableDef msys, CancellationToken cancellationToken = default)
        => this.services.Catalog.EnumerateMSysObjectsRowsAsync(msys, cancellationToken);

    /// <summary>Finds a user table's catalog entry by name (case-insensitive).</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the lookup.</param>
    public ValueTask<CatalogEntry?> GetCatalogEntryAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.TableCatalog.GetCatalogEntryAsync(tableName, cancellationToken);

    /// <summary>Yields the live rows of a data page paired with its page number.</summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="page">The page bytes.</param>
    public IEnumerable<RowLocation> EnumerateLiveRowLocations(long pageNumber, byte[] page)
        => DataPageRows.EnumerateLiveRowLocations(this.db.Format, pageNumber, page);

    public ValueTask<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default)
        => this.services.Schema.ListTablesAsync(cancellationToken);

    public ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Indexes.ListIndexesAsync(tableName, cancellationToken);

    public ValueTask<IReadOnlyList<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Schema.GetColumnMetadataAsync(tableName, cancellationToken);

    public ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default)
        => this.services.Tables.GetRealRowCountAsync(tableName, cancellationToken);

    public ValueTask<DataTable> ReadTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.services.Tables.ReadTableAsync(tableName, maxRows, progress, cancellationToken);

    public IAsyncEnumerable<object[]> Rows(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.services.Tables.Rows(tableName, progress, cancellationToken);

    public IAsyncEnumerable<string[]> RowsAsStrings(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        => this.services.Tables.RowsAsStrings(tableName, progress, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        this.services.Dispose();
        await this.db.DisposeAsync().ConfigureAwait(false);
    }
}
