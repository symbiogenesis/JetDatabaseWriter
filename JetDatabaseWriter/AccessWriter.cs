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
    private readonly WriterServices services;

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "AccessBase takes ownership of the database file; DisposeAsync disposes it as the last LockFileCoordinator.DisposeAfterAsync step.")]
    private AccessWriter(
        string path,
        Stream stream,
        byte[] header,
        AccessWriterOptions options,
        bool leaveOpen = false,
        CancellationToken cancellationToken = default)
        : base(DatabaseFile.ForWriter(stream, header, options.Password, path, leaveOpen, out Pager pager, options.PageCacheSize, options, cancellationToken))
    {
        this.lockFileCoordinator = LockFileCoordinator.ForWriter(path, options);

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
    /// the lock-file slot is taken. Pending recovery runs before this header read.
    /// A file that cannot
    /// be opened for writing (read-only, or locked by another process) reports
    /// that error before any encryption or password error.
    /// </remarks>
    /// <exception cref="UnauthorizedAccessException">Thrown when the database needs a password and the options' <see cref="AccessOptions.Password"/> is missing or wrong, or when the file cannot be opened for writing. A pending transaction journal is recovered before password validation.</exception>
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
    /// <exception cref="UnauthorizedAccessException">Thrown when the database needs a password and the options' <see cref="AccessOptions.Password"/> is missing or wrong. A pending file-backed transaction journal is recovered before password validation.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="options"/> has a <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> of zero or less. The stream is not read, written or disposed.</exception>
    /// <exception cref="NotSupportedException">Office compound packages are not native Microsoft Access database inputs.</exception>
    public static async ValueTask<AccessWriter> OpenAsync(Stream stream, AccessWriterOptions? options = null, bool leaveOpen = false, CancellationToken cancellationToken = default)
    {
        Guard.RequireReadWriteSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        options ??= new AccessWriterOptions();
        options.Validate();
        try
        {
            string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;

            if (stream is FileStream recoveryFile)
            {
                // Retain the durable recovery decision until lock-file cleanup succeeds.
                PersistentRollbackJournal.Recover(
                    recoveryFile,
                    () => RemoveAbandonedLockFile(recoveryFile.Name));
            }

            // One read of page 0 serves the header and the flat Agile probe.
            byte[] headerPage = await EncryptionManager.ReadOpenHeaderPageAsync(stream, cancellationToken).ConfigureAwait(false);
            byte[] header = headerPage.AsSpan(0, Constants.DatabaseHeader.Length).ToArray();

            if (EncryptionManager.IsCompoundFileEncrypted(header))
            {
                throw new NotSupportedException("Office compound packages are not native Microsoft Access databases.");
            }

            return new AccessWriter(
                path,
                stream,
                headerPage,
                options,
                leaveOpen: leaveOpen,
                cancellationToken: cancellationToken);
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
    /// The file must not already exist. Initialization completes in a private adjacent staging file
    /// whose contents are durably flushed before non-overwriting publication. If reopening the
    /// published database fails, the complete database remains at the destination. Directory
    /// entries are not durably flushed; power-loss persistence is not guaranteed. Protect the
    /// containing directory from untrusted access, especially on Unix netstandard2.1.
    /// </summary>
    /// <param name="path">Path where the new .mdb or .accdb file will be created.</param>
    /// <param name="format">The database format to use (Jet4 .mdb or ACE .accdb).</param>
    /// <param name="options">Optional configuration options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> that yields an <see cref="AccessWriter"/> for the new database.</returns>
    /// <exception cref="IOException">Thrown when a database file already exists at <paramref name="path"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="options"/> has a <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> of zero or less. No file is created.</exception>
    /// <exception cref="JetIOException">The destination database file already exists.</exception>
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

        path = Path.GetFullPath(path);
        await DatabaseCreationPublication.PublishAsync(
            path,
            async (staging, token) =>
            {
                await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(staging, format, options, leaveOpen: true, token).ConfigureAwait(false);
            },
            observer: null,
            cancellationToken).ConfigureAwait(false);

        // Publication is committed. Reopen failure must retain the complete database.
        return await OpenAsync(path, options, cancellationToken).ConfigureAwait(false);
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

        byte[] dbBytes = await AccessDatabaseEncryption.PrepareCreatedDatabaseAsync(TDefPageBuilder.BuildEmptyDatabase(format), format, options, cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(dbBytes.AsMemory(), cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        stream.Position = 0;

        AccessWriter writer = await OpenAsync(stream, options, leaveOpen, cancellationToken).ConfigureAwait(false);
        try
        {
            await writer.InitializeFreshDatabaseAsync(cancellationToken).ConfigureAwait(false);
            return writer;
        }
        catch
        {
            await writer.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Sets or removes a table validation rule after checking every existing row.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rule">The new rule, or null to remove it and its validation message.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    public ValueTask SetTableValidationRuleAsync(string tableName, TableValidationRule? rule, CancellationToken cancellationToken = default)
        => this.RunAutoCommitAsync(_ => this.services.Schema.SetTableValidationRuleAsync(tableName, rule, cancellationToken), cancellationToken);

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
    public ValueTask InsertRowAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new()
        => this.RunAutoCommitAsync(_ => this.services.Data.InsertItemAsync(tableName, item, cancellationToken), cancellationToken);

    /// <inheritdoc/>
    public ValueTask<int> InsertRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
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
        byte[] validatedLvProp = LinkedTableManager.CopyValidatedCachedSchemaLvProp(this.Database.Format, cachedSchemaLvProp, nameof(cachedSchemaLvProp));
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
        return await this.RunAutoCommitAsync(this.services.PageAllocator.ScrubFreePagesAsync, cancellationToken).ConfigureAwait(false);
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
        return await this.services.Transactions.RunMutationAsync(
            async () =>
            {
                await using WriteScope scope = this.services.Transactions.BeginWriteScope();
                return await this.services.PageAllocator.ShrinkDatabaseAsync(cancellationToken).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Begins a transaction with bounded page buffering. Changed pages may
    /// spill to the database before commit; before-images are retained in
    /// memory or a temporary undo file. File-backed transactions also persist raw
    /// recovery images beside the database. <see cref="JetTransaction.CommitAsync"/>
    /// writes the remaining pages in place. Opening a file-backed writer recovers
    /// interrupted spills or commits before parsing the database. Caller-supplied
    /// streams other than FileStream cannot recover after a process crash.
    /// <see cref="JetTransaction.RollbackAsync"/> (and disposal of an uncommitted
    /// transaction) restores early writes and cached state. An I/O failure also
    /// attempts restoration; if undo fails, the writer rejects further mutations.
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
        await this.services.Transactions.RunDisposalAsync(this.DisposeCoreAsync).ConfigureAwait(false);
        this.lockFileCoordinator.Dispose();
    }

    private static void RemoveAbandonedLockFile(string databasePath)
    {
        string lockPath = LockFileSlotWriter.GetLockFilePath(databasePath);
        if (!File.Exists(lockPath))
        {
            return;
        }

        // Successful recovery establishes journal ownership. Exclusive opening
        // additionally proves the old lock file has no surviving holders.
        using FileStream abandoned = new(lockPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose);
    }

    private static FileStream CreateStream(string path) =>
        PageFile.OpenFileStream(path, FileAccess.ReadWrite, FileShare.None, FileOptions.Asynchronous | FileOptions.RandomAccess);

    private ValueTask DisposeCoreAsync()
        => this.services.Transactions.IsFaulted
            ? this.lockFileCoordinator.DisposeAfterAsync(
                this.Database.DisposeAsync)
            : this.lockFileCoordinator.DisposeAfterAsync(
                this.services.Transactions.DisposeActiveTransactionAsync,
                this.services.Transactions.FlushPendingWritesAsync,
                this.Database.DisposeAsync);

    /// <summary>
    /// Runs the work in a private statement transaction when no explicit
    /// transaction is active, or in a savepoint of the explicit transaction.
    /// Failed work discards its pages and writer state. Physical write-back
    /// restores the original image after an I/O failure. File-backed recovery restores
    /// interrupted physical writes when the database is next opened for writing.
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
    /// Completes a freshly written empty database: reserves the core Jet4/ACE
    /// system-table TDEF slots, bootstraps the <c>MSysObjects</c> indexes, and
    /// scaffolds native catalog system tables and security metadata.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask InitializeFreshDatabaseAsync(CancellationToken cancellationToken)
    {
        await using WriteScope scope = this.services.Transactions.BeginWriteScope();
        long coreSystemTableStartPage = await this.services.CatalogArtifacts.ReserveFreshCoreSystemTablePagesAsync(cancellationToken).ConfigureAwait(false);
        await this.services.CatalogArtifacts.InitializeFreshCatalogIndexesAsync(cancellationToken).ConfigureAwait(false);
        await this.services.SystemCatalog.CreateCoreSystemTablesAsync(coreSystemTableStartPage, cancellationToken).ConfigureAwait(false);
        await this.services.ComplexColumns.ScaffoldComplexSystemTablesAsync(cancellationToken).ConfigureAwait(false);
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
}
