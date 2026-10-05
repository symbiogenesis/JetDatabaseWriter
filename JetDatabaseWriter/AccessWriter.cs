namespace JetDatabaseWriter;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Pure-managed writer for Microsoft Access JET databases (.mdb / .accdb).
/// Supports creating tables, inserting, updating, and deleting rows.
/// </summary>
public sealed class AccessWriter : AccessBase, IAccessWriter, IAccessSchema
{
    private readonly LockFileCoordinator lockFileCoordinator;
    private readonly AccessWriterOptions options;
    private readonly WriterServices services;

    /// <summary>
    /// Office Crypto re-encryption context. When non-null, the underlying _stream is an
    /// in-memory MemoryStream containing the *decrypted* inner ACCDB; on
    /// DisposeAsync the bytes are re-encrypted with the original Office Crypto format
    /// and written back to _outerEncryptedStream (which holds the original CFB).
    /// </summary>
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via RewrapAndCloseOuterEncryptedStreamAsync, invoked by LockFileCoordinator.DisposeAfterAsync.")]
    private readonly Stream? outerEncryptedStream;
    private readonly bool outerEncryptedLeaveOpen;
    private readonly AccessEncryptionFormat outerEncryptedFormat;
    private readonly bool isAgileEncryptedRewrap;

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "AccessBase takes ownership of the database file; DisposeAsync disposes it as the last LockFileCoordinator.DisposeAfterAsync step.")]
    private AccessWriter(
        string path,
        Stream stream,
        byte[] header,
        AccessWriterOptions options,
        Stream? outerEncryptedStream = null,
        bool outerEncryptedLeaveOpen = false,
        AccessEncryptionFormat outerEncryptedFormat = AccessEncryptionFormat.None,
        bool leaveOpen = false)
        : base(DatabaseFile.ForWriter(stream, header, options.Password, path, leaveOpen, out Pager pager, options.PageCacheSize))
    {
        this.options = options;
        this.lockFileCoordinator = LockFileCoordinator.ForWriter(path, options);
        this.outerEncryptedStream = outerEncryptedStream;
        this.outerEncryptedLeaveOpen = outerEncryptedLeaveOpen;
        this.outerEncryptedFormat = outerEncryptedFormat;
        this.isAgileEncryptedRewrap = outerEncryptedFormat != AccessEncryptionFormat.None;

        this.lockFileCoordinator.Acquire();
        try
        {
            pager.ByteRangeLock = options.CreateByteRangeLock(stream);
            this.services = new WriterServices(this.Database, pager, options, pager.ByteRangeLock);
        }
        catch
        {
            this.lockFileCoordinator.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Asynchronously opens a JET database file for writing and returns a new <see cref="AccessWriter"/> instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the specified database.</returns>
    /// <remarks>
    /// The file is opened once, for reading and writing, and page 0 is read
    /// once: the flat Agile check and the password check both use it, before
    /// the lock-file slot is taken or anything is written. A file that cannot
    /// be opened for writing (read-only, or locked by another process) reports
    /// that error before any encryption or password error.
    /// </remarks>
    /// <exception cref="NotSupportedException">Thrown when the file uses Access-native flat Agile encryption (<see cref="AccessEncryptionFormat.AccdbAgile"/>), which the writer cannot edit in place. The file is not modified.</exception>
    /// <exception cref="UnauthorizedAccessException">Thrown when the database needs a password and the options' <see cref="AccessOptions.Password"/> is missing or wrong, or when the file cannot be opened for writing. The file is not modified.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="options"/> has a <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> of zero or less. The file is not opened.</exception>
    public static async ValueTask<AccessWriter> OpenAsync(string path, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        options ??= new AccessWriterOptions();
        options.Validate();

        FileStream fs = CreateStream(path);
        return await OpenAsync(fs, options, leaveOpen: false, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously opens a JET database from a caller-supplied <see cref="Stream"/> and returns a new <see cref="AccessWriter"/> instance.
    /// The stream must be readable, writable, and seekable. The caller retains ownership unless <paramref name="leaveOpen"/> is false (the default),
    /// in which case the stream will be disposed when the writer is disposed.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the database bytes.</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="leaveOpen">If <c>true</c>, the stream is not disposed when the writer is disposed. Default is <c>false</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the open operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the database.</returns>
    /// <exception cref="NotSupportedException">Thrown when the stream holds an Access-native flat Agile database (<see cref="AccessEncryptionFormat.AccdbAgile"/>), which the writer cannot edit in place. Nothing is written to the stream.</exception>
    /// <exception cref="UnauthorizedAccessException">Thrown when the database needs a password and the options' <see cref="AccessOptions.Password"/> is missing or wrong. Nothing is written to the stream.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="options"/> has a <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> of zero or less. The stream is not read, written or disposed.</exception>
    public static async ValueTask<AccessWriter> OpenAsync(Stream stream, AccessWriterOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
    {
        Guard.RequireReadWriteSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        options ??= new AccessWriterOptions();
        options.Validate();
        try
        {
            string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;

            // One read of page 0 serves the header and the flat Agile probe.
            byte[] headerPage = await EncryptionManager.ReadOpenHeaderPageAsync(stream, cancellationToken).ConfigureAwait(false);
            byte[] header = headerPage.AsSpan(0, Constants.DatabaseHeader.Length).ToArray();

            // Access-native flat Agile encrypts every page with AES-CBC keyed
            // from the descriptor in page 0. The page cipher has no writer
            // path, so refuse before anything is written.
            if (!EncryptionManager.IsCompoundFileEncrypted(header) && OfficeCryptoAgile.IsFlatAgileEncrypted(headerPage))
            {
                throw FlatAgileNotSupported();
            }

            // Office Crypto API ("Agile") encrypted .accdb files are real OLE
            // compound documents (CFB) wrapping an EncryptedPackage stream.
            // We can't edit them in place: writes are buffered into an
            // in-memory MemoryStream containing the *decrypted* inner ACCDB,
            // and the whole CFB is re-emitted on DisposeAsync.
            if (EncryptionManager.IsCompoundFileEncrypted(header))
            {
                _ = stream.Seek(0, SeekOrigin.Begin);
                (byte[]? decryptedPackage, AccessEncryptionFormat outerFormat) = await EncryptionManager
                    .TryDecryptCompoundFileWithFormatAsync(stream, header, options.Password, EncryptionManager.WriterPasswordOption, cancellationToken)
                    .ConfigureAwait(false);

                if (decryptedPackage != null)
                {
                    var inner = new MemoryStream();
                    await inner.WriteAsync(decryptedPackage.AsMemory(), cancellationToken).ConfigureAwait(false);
                    inner.Position = 0;
                    byte[] innerHeader = await PageFile.ReadHeaderAsync(inner, cancellationToken).ConfigureAwait(false);

                    return new AccessWriter(
                        path,
                        inner,
                        innerHeader,
                        options,
                        outerEncryptedStream: stream,
                        outerEncryptedLeaveOpen: leaveOpen,
                        outerEncryptedFormat: outerFormat);
                }

                // CFB magic but not a real Agile compound document: treat as
                // the synthetic legacy AES-128 layout (flat per-page AES-ECB
                // beneath a CFB-magic header byte). The constructor sets up
                // the page key and writes are re-encrypted on every flush.
                _ = stream.Seek(0, SeekOrigin.Begin);
            }

            return new AccessWriter(
                path,
                stream,
                header,
                options,
                leaveOpen: leaveOpen);
        }
        catch
        {
            if (!leaveOpen)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <summary>
    /// Asynchronously creates a new, empty JET database file at the specified path
    /// and returns a new <see cref="AccessWriter"/> ready for table creation and data insertion.
    /// The file must not already exist.
    /// </summary>
    /// <param name="path">Path where the new .mdb or .accdb file will be created.</param>
    /// <param name="format">The database format to use (Jet4 .mdb or ACE .accdb).</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the new database.</returns>
    /// <exception cref="IOException">Thrown when a database file already exists at <paramref name="path"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="options"/> has a <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> of zero or less. No file is created.</exception>
    public static async ValueTask<AccessWriter> CreateDatabaseAsync(string path, DatabaseFormat format, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();
        options ??= new AccessWriterOptions();
        options.Validate();

        if (File.Exists(path))
        {
            throw new JetIOException(JetErrorCode.DatabaseFileExists, $"Database file already exists: {path}");
        }

        byte[] dbBytes = TDefPageBuilder.BuildEmptyDatabase(format, options.WriteFullCatalogSchema);

        await using (FileStream fs = FileStreamFactory.Open(
            path,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            FileOptions.Asynchronous,
            preallocationSize: dbBytes.Length))
        {
            await fs.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            AccessWriter writer = await OpenAsync(path, options, cancellationToken).ConfigureAwait(false);
            await writer.InitializeFreshDatabaseAsync(options.WriteFullCatalogSchema, cancellationToken).ConfigureAwait(false);
            return writer;
        }
        catch
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup of the partially-created file.
            }
            catch (UnauthorizedAccessException)
            {
                // Best-effort cleanup if we lack permission.
            }

            throw;
        }
    }

    /// <summary>
    /// Asynchronously writes a new, empty JET database into the specified stream
    /// and returns a new <see cref="AccessWriter"/> ready for table creation and data insertion.
    /// The stream must be readable, writable, and seekable.
    /// </summary>
    /// <param name="stream">A writable, seekable stream to write the new database into.</param>
    /// <param name="format">The database format to use (Jet4 .mdb or ACE .accdb).</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="leaveOpen">If <c>true</c>, the stream is not disposed when the writer is disposed. Default is <c>false</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the new database.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="options"/> has a <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> of zero or less. Nothing is written to the stream.</exception>
    public static async ValueTask<AccessWriter> CreateDatabaseAsync(Stream stream, DatabaseFormat format, AccessWriterOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
    {
        Guard.RequireReadWriteSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();
        options ??= new AccessWriterOptions();
        options.Validate();

        byte[] dbBytes = TDefPageBuilder.BuildEmptyDatabase(format, options.WriteFullCatalogSchema);
        await stream.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        stream.Position = 0;

        AccessWriter writer = await OpenAsync(stream, options, leaveOpen, cancellationToken).ConfigureAwait(false);
        try
        {
            await writer.InitializeFreshDatabaseAsync(options.WriteFullCatalogSchema, cancellationToken).ConfigureAwait(false);
            return writer;
        }
        catch
        {
            await writer.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Encryption mutation: change password / encrypt / decrypt
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Detects the on-disk encryption format of the database at
    /// <paramref name="path"/>. Returns <see cref="AccessEncryptionFormat.None"/>
    /// when the file is unencrypted. The file is read but not modified.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        string path,
        CancellationToken cancellationToken = default)
        => EncryptionManager.DetectEncryptionFormatAsync(path, cancellationToken);

    /// <summary>
    /// Detects the on-disk encryption format of the database in <paramref name="stream"/>
    /// without modifying it. The stream must be seekable.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
        => EncryptionManager.DetectEncryptionFormatAsync(stream, cancellationToken);

    /// <summary>
    /// Changes the password of an already-encrypted JET / ACE database,
    /// preserving the existing on-disk encryption format. Use
    /// <see cref="EncryptAsync(string, ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, CancellationToken)"/>
    /// to add encryption to an unencrypted database, or
    /// <see cref="DecryptAsync(string, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>
    /// to remove it.
    /// </summary>
    /// <param name="path">Path to an existing encrypted .mdb or .accdb file.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="newPassword">The new password (must be non-empty). Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring; the password fields are ignored.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="UnauthorizedAccessException">The supplied <paramref name="oldPassword"/> is wrong, or the database is unencrypted.</exception>
    /// <exception cref="ArgumentException"><paramref name="newPassword"/> is empty.</exception>
    /// <remarks>
    /// Writes and flushes a temporary file named <c>&lt;path&gt;.reenc-&lt;guid&gt;.tmp</c>,
    /// then replaces the database by renaming that file over it.
    /// </remarks>
    /// <exception cref="IOException">
    /// The temporary file cannot be written or the database cannot be replaced,
    /// for example while a reader holds it open. The original is unchanged unless
    /// <see cref="Exception.Data"/> contains <c>JetDatabaseWriter.ReplacementFile</c>;
    /// that entry names the retained temporary file holding the new contents
    /// when replacement removed the original but could not install the new file.
    /// </exception>
    public static ValueTask ChangePasswordAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.ChangePasswordAsync(path, oldPassword, newPassword, options, cancellationToken);

    /// <summary>
    /// Encrypts a currently-unencrypted JET / ACE database, applying
    /// <paramref name="targetFormat"/> when supplied or the best supported
    /// password encryption for the database format when omitted.
    /// </summary>
    /// <param name="path">Path to an existing unencrypted .mdb or .accdb file.</param>
    /// <param name="newPassword">The password to apply (must be non-empty). Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="targetFormat">The encryption format to use. When <see langword="null"/>, Jet4 <c>.mdb</c> files use <see cref="AccessEncryptionFormat.Jet4Rc4"/> and ACE <c>.accdb</c> files use <see cref="AccessEncryptionFormat.AccdbAgile"/>, which <c>OpenAsync</c> cannot open for writing; choose <see cref="AccessEncryptionFormat.AccdbAgileCfb"/> when the file must stay writable.</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="newPassword"/> is empty,
    /// <paramref name="targetFormat"/> is <see cref="AccessEncryptionFormat.None"/>,
    /// or the format is not valid for the underlying file kind.
    /// </exception>
    /// <exception cref="InvalidOperationException">The file is already encrypted.</exception>
    /// <remarks>
    /// Writes and flushes a temporary file named <c>&lt;path&gt;.reenc-&lt;guid&gt;.tmp</c>,
    /// then replaces the database by renaming that file over it.
    /// </remarks>
    /// <exception cref="IOException">
    /// The temporary file cannot be written or the database cannot be replaced,
    /// for example while a reader holds it open. The original is unchanged unless
    /// <see cref="Exception.Data"/> contains <c>JetDatabaseWriter.ReplacementFile</c>;
    /// that entry names the retained temporary file holding the new contents
    /// when replacement removed the original but could not install the new file.
    /// </exception>
    public static ValueTask EncryptAsync(
        string path,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.EncryptAsync(path, newPassword, targetFormat, options, cancellationToken);

    /// <summary>
    /// Removes encryption from a JET / ACE database, leaving an
    /// unencrypted file with no header password residue.
    /// </summary>
    /// <param name="path">Path to an existing encrypted .mdb or .accdb file.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="options">Optional configuration. Used only for lockfile honouring.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    /// <exception cref="UnauthorizedAccessException">The supplied <paramref name="oldPassword"/> is wrong.</exception>
    /// <exception cref="InvalidOperationException">The file is already unencrypted.</exception>
    /// <remarks>
    /// Writes and flushes a temporary file named <c>&lt;path&gt;.reenc-&lt;guid&gt;.tmp</c>,
    /// then replaces the database by renaming that file over it.
    /// </remarks>
    /// <exception cref="IOException">
    /// The temporary file cannot be written or the database cannot be replaced,
    /// for example while a reader holds it open. The original is unchanged unless
    /// <see cref="Exception.Data"/> contains <c>JetDatabaseWriter.ReplacementFile</c>;
    /// that entry names the retained temporary file holding the new contents
    /// when replacement removed the original but could not install the new file.
    /// </exception>
    public static ValueTask DecryptAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.DecryptAsync(path, oldPassword, options, cancellationToken);

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="ChangePasswordAsync(string, ReadOnlyMemory{char}, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>.
    /// The stream must be readable, writable, and seekable; it is rewritten
    /// in place (length may change for Agile transitions).
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the database bytes.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="newPassword">The new password (must be non-empty). Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask ChangePasswordAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        CancellationToken cancellationToken = default)
        => EncryptionManager.ChangePasswordAsync(stream, oldPassword, newPassword, cancellationToken);

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="EncryptAsync(string, ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the unencrypted database bytes.</param>
    /// <param name="newPassword">The password to apply. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="targetFormat">The encryption format to use. When <see langword="null"/>, Jet4 <c>.mdb</c> files use <see cref="AccessEncryptionFormat.Jet4Rc4"/> and ACE <c>.accdb</c> files use <see cref="AccessEncryptionFormat.AccdbAgile"/>, which <c>OpenAsync</c> cannot open for writing; choose <see cref="AccessEncryptionFormat.AccdbAgileCfb"/> when the file must stay writable.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask EncryptAsync(
        Stream stream,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        CancellationToken cancellationToken = default)
        => EncryptionManager.EncryptAsync(stream, newPassword, targetFormat, cancellationToken);

    /// <summary>
    /// Stream-based equivalent of
    /// <see cref="DecryptAsync(string, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">A readable, writable, seekable stream containing the encrypted database bytes.</param>
    /// <param name="oldPassword">The current password. Mutable backing memory must remain unchanged until the returned task completes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public static ValueTask DecryptAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
        CancellationToken cancellationToken = default)
        => EncryptionManager.DecryptAsync(stream, oldPassword, cancellationToken);

    /// <inheritdoc/>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, CancellationToken cancellationToken = default)
        => this.CreateTableAsync(tableName, columns, indexes: [], cancellationToken);

    /// <inheritdoc/>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Schema.CreateDeclaredTableAsync(tableName, columns, indexes, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Schema.DropTableAsync(tableName, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Schema.AddColumnAsync(tableName, column, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask DropColumnAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Schema.DropColumnAsync(tableName, columnName, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Schema.RenameColumnAsync(tableName, oldColumnName, newColumnName, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask InsertRowAsync(string tableName, object?[] values, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Data.InsertRowAsync(tableName, values, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object?[]> rows, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Data.InsertRowsAsync(tableName, rows, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask InsertRowAsync<T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new()
        => this.RunAutoCommitAsync(_ => this.services.Data.InsertItemAsync(tableName, item, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new()
        => this.RunAutoCommitAsync(_ => this.services.Data.InsertItemsAsync(tableName, items, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask InsertRowAsync(string tableName, RowValues row, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Data.InsertNamedRowAsync(tableName, row, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<RowValues> rows, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Data.InsertNamedRowsAsync(tableName, rows, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> UpdateRowsAsync(string tableName, RowCriteria criteria, RowValues updatedValues, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Data.UpdateRowsAsync(tableName, criteria, updatedValues, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object?> updatedValues, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        return this.RunAutoCommitAsync(
            _ => this.services.Data.UpdateRowsAsync(
                tableName,
                RowCriteria.Where(predicateColumn, predicateValue),
                new RowValues(updatedValues),
                cancellationToken),
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<int> DeleteRowsAsync(string tableName, RowCriteria criteria, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Data.DeleteRowsAsync(tableName, criteria, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        return this.RunAutoCommitAsync(
            _ => this.services.Data.DeleteRowsAsync(tableName, RowCriteria.Where(predicateColumn, predicateValue), cancellationToken),
            cancellationToken);
    }

    /// <summary>
    /// Asynchronously creates a linked-table entry (MSysObjects type 6) that references
    /// a table in another Access database. No row data is stored locally; readers follow
    /// the entry to <paramref name="sourceDatabasePath"/> on demand.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="sourceDatabasePath">Path to the source Access database file (.mdb / .accdb).</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedTableAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => LinkedTableManager.CreateLinkedTableAsync(this.Database.Format, this.Database.Pages, this.services.CatalogArtifacts, linkedTableName, sourceDatabasePath, foreignTableName, cancellationToken), cancellationToken);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) that references
    /// a table accessible via an ODBC connection. No row data is stored locally; managed
    /// readers expose the catalog metadata but do not open the ODBC source. Because this
    /// overload receives no source columns, it writes a table-level <c>LvProp</c>
    /// property block but cannot cache the remote column schema. Use the source-column
    /// overload for generated column-level metadata, or the <c>cachedSchemaLvProp</c>
    /// overload when byte-for-byte Access/DAO-authored metadata is required.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedOdbcTableAsync(string linkedTableName, string connectionString, string foreignTableName, CancellationToken cancellationToken = default)
        => this.CreateLinkedOdbcTableCoreAsync(linkedTableName, connectionString, foreignTableName, cachedSchemaLvProp: null, sourceColumns: null, cancellationToken);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) and
    /// generates a cached-schema <c>MSysObjects.LvProp</c> property block from
    /// the supplied remote column definitions.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="sourceColumns">Column definitions for the remote source table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedOdbcTableAsync(
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        IReadOnlyList<ColumnDefinition> sourceColumns,
        CancellationToken cancellationToken = default)
        => this.CreateLinkedOdbcTableCoreAsync(linkedTableName, connectionString, foreignTableName, cachedSchemaLvProp: null, sourceColumns, cancellationToken);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) using
    /// a caller-supplied Access/DAO cached-schema payload for <c>MSysObjects.LvProp</c>.
    /// The payload must come from an Access-compatible ODBC link to the same source
    /// schema; the writer validates that it is a non-empty <c>MR2\0</c> / <c>KKD\0</c>
    /// property block and stores it verbatim.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cachedSchemaLvProp">Access/DAO-authored cached linked-schema payload for <c>MSysObjects.LvProp</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedOdbcTableAsync(
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        ReadOnlyMemory<byte> cachedSchemaLvProp,
        CancellationToken cancellationToken = default)
    {
        byte[] validatedLvProp = LinkedTableManager.CopyValidatedCachedSchemaLvProp(this.Database.Format.Kind, cachedSchemaLvProp, nameof(cachedSchemaLvProp));
        return this.CreateLinkedOdbcTableCoreAsync(linkedTableName, connectionString, foreignTableName, validatedLvProp, sourceColumns: null, cancellationToken);
    }

    /// <summary>
    /// Asynchronously creates a linked-text/CSV table entry (MSysObjects type 6) that references
    /// a text or CSV file in a directory. The entry stores both a <c>Database</c> path (the
    /// directory containing the file) and a <c>Connect</c> string (e.g.
    /// <c>"Text;HDR=YES;FMT=Delimited"</c>). No row data is stored locally; managed
    /// readers parse supported delimited text sources on demand through the
    /// linked-source path policy and expose fields as strings.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="sourceDirectoryPath">Path to the directory containing the text/CSV source file.</param>
    /// <param name="foreignFileName">The filename of the text/CSV source (e.g. <c>"data.csv"</c>).</param>
    /// <param name="connectString">The text-driver connect string (e.g. <c>"Text;HDR=YES;FMT=Delimited"</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedTextTableAsync(string linkedTableName, string sourceDirectoryPath, string foreignFileName, string connectString, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => LinkedTableManager.CreateLinkedTextTableAsync(this.Database.Format, this.Database.Pages, this.services.CatalogArtifacts, linkedTableName, sourceDirectoryPath, foreignFileName, connectString, cancellationToken), cancellationToken);

    // ════════════════════════════════════════════════════════════════
    // Foreign-key relationships — thin forwarders to RelationshipManager
    // ════════════════════════════════════════════════════════════════

    /// <inheritdoc/>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Relationships.CreateRelationshipAsync(relationship, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Relationships.DropRelationshipAsync(relationshipName, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Relationships.RenameRelationshipAsync(oldName, newName, cancellationToken), cancellationToken);

    // ── Row-level APIs for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §2.1 / §2.4 / §3.

    /// <inheritdoc/>
    public ValueTask AddAttachmentAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?> parentRowKey,
        AttachmentInput attachment,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        Guard.NotNull(attachment, nameof(attachment));
        return this.RunAutoCommitAsync(
            _ => this.services.ComplexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, attachment, expectAttachment: true, cancellationToken),
            cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask AddMultiValueItemAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?> parentRowKey,
        object? value,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        return this.RunAutoCommitAsync(
            _ => this.services.ComplexColumns.AddComplexItemCoreAsync(tableName, columnName, parentRowKey, value, expectAttachment: false, cancellationToken),
            cancellationToken);
    }

    /// <summary>
    /// Overwrites every page currently marked free in the Access global page
    /// allocation map. This is a maintenance operation; it does not move live
    /// pages or change table contents.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of free pages scrubbed.</returns>
    public async ValueTask<int> ScrubFreePagesAsync(CancellationToken cancellationToken = default)
    {
        this.Database.Pages.ThrowIfDisposedOrCancelled(cancellationToken);
        return await this.RunAutoCommitAsync(token => this.services.PageAllocator.ScrubFreePagesAsync(token), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Truncates globally-free pages from the physical end of the database file.
    /// This is a tail shrinker, not a full Microsoft Access Compact &amp; Repair
    /// rebuild: live pages keep their existing page numbers.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of pages removed from the end of the file.</returns>
    public async ValueTask<long> ShrinkDatabaseAsync(CancellationToken cancellationToken = default)
    {
        this.Database.Pages.ThrowIfDisposedOrCancelled(cancellationToken);
        return await this.services.Transactions.RunMutationAsync(async () =>
        {
            await using WriteScope scope = this.services.Transactions.BeginWriteScope();
            return await this.services.PageAllocator.ShrinkDatabaseAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Begins an explicit page-buffered transaction against this writer. While
    /// the returned <see cref="JetTransaction"/> is active, every page-write
    /// performed by this writer is journaled in memory instead of flushed to
    /// the database file. <see cref="JetTransaction.CommitAsync"/> writes the
    /// journaled pages over the file in place; this is not crash-atomic, so a
    /// crash or I/O error partway through leaves part of the transaction in
    /// the file. <see cref="JetTransaction.RollbackAsync"/> (and
    /// <see cref="JetTransaction.DisposeAsync"/> on an uncommitted transaction)
    /// discards the journal, leaving the file in its pre-transaction state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The newly-started transaction.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a transaction is already active on this writer (only one
    /// concurrent transaction is supported per <see cref="AccessWriter"/>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown when the writer has been disposed.</exception>
    public ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        => this.services.Transactions.BeginTransactionAsync(cancellationToken);

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        if (this.Database.Pages.IsDisposed)
        {
            return;
        }

        // The coordinator drains every step in order, aggregates failures,
        // and unconditionally releases the .ldb / .laccdb slot last.
        // Lock-file release runs after the agile re-wrap so the lock-file
        // accurately reflects "database still in use" while we re-encrypt.
        await this.services.Transactions.RunDisposalAsync(() => this.lockFileCoordinator.DisposeAfterAsync(
            this.services.Transactions.DisposeActiveTransactionAsync,
            this.services.Transactions.FlushPendingWritesAsync,
            this.RewrapAndCloseOuterEncryptedStreamAsync,
            this.Database.DisposeAsync)).ConfigureAwait(false);
    }

    private static FileStream CreateStream(string path) =>
        PageFile.OpenFileStream(path, FileAccess.ReadWrite, FileShare.Read, FileOptions.Asynchronous | FileOptions.RandomAccess);

    private static NotSupportedException FlatAgileNotSupported() => new(
        "This .accdb file uses Access-native Agile encryption (AccessEncryptionFormat.AccdbAgile), " +
        "which AccessWriter cannot edit in place. Open it with AccessReader to read it. To write to it, " +
        "remove the encryption with AccessWriter.DecryptAsync, write to the decrypted file, then encrypt it " +
        "again with AccessWriter.EncryptAsync. Files encrypted as AccessEncryptionFormat.AccdbAgileCfb can be " +
        "opened for writing.");

    /// <summary>
    /// If <see cref="AccessWriterOptions.UseTransactionalWrites"/> is enabled
    /// and no explicit transaction is currently active, wraps
    /// <paramref name="work"/> in a private <see cref="JetTransaction"/> so an
    /// exception before commit leaves the database in its pre-call state (a
    /// crash during the commit's page replay can still leave part of the call
    /// in the file). Otherwise invokes <paramref name="work"/> directly using
    /// the flush-per-page path.
    /// </summary>
    /// <param name="work">The work to execute.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private ValueTask RunAutoCommitAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
        => this.services.Transactions.RunAutoCommitAsync(work, cancellationToken);

    /// <summary>
    /// Generic-result variant of <see cref="RunAutoCommitAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="TResult">The result type produced by <paramref name="work"/>.</typeparam>
    /// <param name="work">The work to execute.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private ValueTask<TResult> RunAutoCommitAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> work, CancellationToken cancellationToken)
        => this.services.Transactions.RunAutoCommitAsync(work, cancellationToken);

    /// <summary>
    /// Completes a freshly written empty database: reserves the core ACCDB
    /// system-table TDEF slots, bootstraps the <c>MSysObjects</c> indexes, and
    /// scaffolds the remaining full-catalog system tables.
    /// </summary>
    /// <param name="fullCatalogSchema">Whether the full 17-column catalog schema is in use.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask InitializeFreshDatabaseAsync(bool fullCatalogSchema, CancellationToken cancellationToken)
    {
        await using WriteScope scope = this.services.Transactions.BeginWriteScope();
        long coreSystemTableStartPage = await this.services.CatalogArtifacts.ReserveFreshCoreSystemTablePagesAsync(fullCatalogSchema, cancellationToken).ConfigureAwait(false);
        await this.services.CatalogArtifacts.InitializeFreshCatalogIndexesAsync(fullCatalogSchema, cancellationToken).ConfigureAwait(false);
        await this.services.ComplexColumns.ScaffoldSystemTablesAsync(fullCatalogSchema, coreSystemTableStartPage, cancellationToken).ConfigureAwait(false);
    }

    private ValueTask CreateLinkedOdbcTableCoreAsync(
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        byte[]? cachedSchemaLvProp,
        IReadOnlyList<ColumnDefinition>? sourceColumns,
        CancellationToken cancellationToken)
        => this.RunAutoCommitAsync(
            _ => LinkedTableManager.CreateLinkedOdbcTableAsync(
                this.Database.Format,
                this.Database.Pages,
                this.services.CatalogArtifacts,
                linkedTableName,
                connectionString,
                foreignTableName,
                cachedSchemaLvProp,
                sourceColumns,
                cancellationToken),
            cancellationToken);

    private async ValueTask RewrapAndCloseOuterEncryptedStreamAsync()
    {
        // For Agile-encrypted databases the underlying _stream is an in-memory
        // copy of the *decrypted* ACCDB. Re-encrypt it before tearing down so
        // the user's outer encrypted stream/file ends up with all writes.
        if (!this.isAgileEncryptedRewrap || this.outerEncryptedStream is null || this.options.Password.IsEmpty)
        {
            return;
        }

        try
        {
            await EncryptionManager.RewrapDecryptedCompoundFileAsync(
                this.Database.Pages.Stream,
                this.outerEncryptedStream,
                this.outerEncryptedFormat,
                this.options.Password).ConfigureAwait(false);
        }
        finally
        {
            if (!this.outerEncryptedLeaveOpen)
            {
                await this.outerEncryptedStream.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
