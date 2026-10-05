namespace JetDatabaseWriter;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Encryption.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;

/// <summary>
/// Owns one open database file's immutable format, page I/O, table-definition
/// reader and owned-page discovery. Composition roots pass those parts to
/// their services. Only the writer factory hands out a writable pager.
/// </summary>
internal sealed class DatabaseFile : IAsyncDisposable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DatabaseFile"/> class
    /// from a pre-read database file header.
    /// </summary>
    /// <param name="stream">An open, seekable <see cref="Stream"/> for the database file.</param>
    /// <param name="header">Header bytes read from page 0.</param>
    /// <param name="password">The database password.</param>
    /// <param name="path">Path to the database file, or empty when opened from a stream.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller retains ownership of <paramref name="stream"/> and it will not be disposed.</param>
    /// <param name="writable">
    /// <see langword="true"/> for the writer's file, whose page I/O is a
    /// <see cref="Pager"/> and whose disposal errors name <see cref="AccessWriter"/>;
    /// <see langword="false"/> for a reader's, whose page I/O is a read-only
    /// <see cref="PageFile"/> and whose errors name <see cref="AccessReader"/>.
    /// </param>
    private DatabaseFile(
        Stream stream,
        byte[] header,
        ReadOnlyMemory<char> password,
        string path,
        bool leaveOpen,
        bool writable)
    {
        this.DatabasePath = path ?? string.Empty;

        // The profile detects the format and decodes the code page; neither
        // step can throw, so a password error below is still the first error
        // an encrypted file reports, as when the code page was decoded after
        // the page keys.
        this.Format = JetFormat.FromHeader(header);
        bool isLegacyAesCfb = EncryptionManager.IsCompoundFileEncrypted(header);
        Type ownerType = writable ? typeof(AccessWriter) : typeof(AccessReader);
        string passwordOptionName = writable
            ? EncryptionManager.WriterPasswordOption
            : EncryptionManager.ReaderPasswordOption;
        PageDecryptionKeys pageKeys = EncryptionManager.CreatePageDecryptionKeys(header, this.Format.Kind, isLegacyAesCfb, password, passwordOptionName);
        this.Pages = writable
            ? new Pager(stream, this.Format.PageSize, pageKeys, leaveOpen, ownerType)
            : new PageFile(stream, this.Format.PageSize, pageKeys, leaveOpen, ownerType);
        this.TableDefs = new TableDefReader(this.Pages, this.Format, cacheResults: !writable);
        this.OwnedPages = new OwnedDataPages(this.Pages, this.Format, cacheResults: !writable);
    }

    /// <summary>Gets the immutable format, byte layouts and text codecs.</summary>
    internal JetFormat Format { get; }

    /// <summary>Gets the file's read-only page interface; the writer receives its pager separately.</summary>
    internal PageFile Pages { get; }

    /// <summary>Gets the table-definition reader, cached only for read-only files.</summary>
    internal TableDefReader TableDefs { get; }

    /// <summary>Gets owned-page discovery, cached only for read-only files.</summary>
    internal OwnedDataPages OwnedPages { get; }

    /// <summary>Gets the path, or an empty string for a caller-owned stream.</summary>
    internal string DatabasePath { get; }

    /// <summary>
    /// Opens a reader's file over a read-only <see cref="PageFile"/>. Each
    /// table's TDEF bytes and owned data pages are memoized until the file is
    /// disposed: nothing writes through it, and the reader assumes that no
    /// other process changes the file while it is open, so a change made
    /// after a table was read is not seen.
    /// </summary>
    /// <param name="stream">An open, seekable <see cref="Stream"/> for the database file.</param>
    /// <param name="header">Header bytes read from page 0.</param>
    /// <param name="password">The database password; a missing-password error names <see cref="AccessReaderOptions"/>.</param>
    /// <param name="path">Path to the database file, or empty when opened from a stream.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller retains ownership of <paramref name="stream"/> and it will not be disposed.</param>
    /// <returns>The read-only file.</returns>
    internal static DatabaseFile ForReader(Stream stream, byte[] header, ReadOnlyMemory<char> password, string path, bool leaveOpen)
        => new(stream, header, password, path, leaveOpen, writable: false);

    /// <summary>
    /// Opens the writer's file over a <see cref="Pager"/>, which writes and
    /// journals pages, and hands that pager to the caller, the writer's
    /// composition root, which gives it to the services that write. The file
    /// itself exposes the pager only as the <see cref="PageFile"/> it reads,
    /// and nothing is memoized, because its pages change.
    /// </summary>
    /// <param name="stream">An open, readable, writable, seekable <see cref="Stream"/> for the database file.</param>
    /// <param name="header">Header bytes read from page 0.</param>
    /// <param name="password">The database password; a missing-password error names <see cref="AccessWriterOptions"/>.</param>
    /// <param name="path">Path to the database file, or empty when opened from a stream.</param>
    /// <param name="leaveOpen">When <see langword="true"/>, the caller retains ownership of <paramref name="stream"/> and it will not be disposed.</param>
    /// <param name="pager">Receives the file's pager; the file owns and disposes it.</param>
    /// <returns>The writer's file.</returns>
    internal static DatabaseFile ForWriter(Stream stream, byte[] header, ReadOnlyMemory<char> password, string path, bool leaveOpen, out Pager pager)
    {
        var file = new DatabaseFile(stream, header, password, path, leaveOpen, writable: true);
        pager = (Pager)file.Pages;
        return file;
    }

    /// <summary>
    /// Disposes the page file (marking it disposed, then the stream unless the
    /// caller kept it, then the I/O gate and the page cipher), then the TDEF
    /// and owned-page caches.
    /// </summary>
    /// <returns>A task that completes when the file is disposed.</returns>
    public async ValueTask DisposeAsync()
    {
        if (this.Pages.IsDisposed)
        {
            return;
        }

        try
        {
            await this.Pages.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            this.TableDefs.Dispose();
            this.OwnedPages.Dispose();
        }
    }

    /// <summary>
    /// Disposes owned managed resources other than the backing stream. Used by
    /// the owning reader or writer when its construction fails before an
    /// instance can be returned to the caller and disposed normally.
    /// </summary>
    internal void DisposeManagedResources()
    {
        this.Pages.DisposeManagedResources();
        this.TableDefs.Dispose();
        this.OwnedPages.Dispose();
    }
}
