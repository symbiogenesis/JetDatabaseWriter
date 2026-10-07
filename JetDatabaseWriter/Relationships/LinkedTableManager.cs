namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.DelimitedText;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Centralises the linked-table (MSysObjects type 4 / 6) format and path
/// policy: the MSysObjects scan that produces <see cref="LinkedTableInfo"/>
/// entries, source-path resolution under a <see cref="LinkedSourcePolicy"/>,
/// the delimited-text read-through used by <see cref="LinkedTableReader"/>, and
/// the writer-side creation of Access-file, ODBC, and text/CSV link catalog
/// entries.
/// </summary>
internal static class LinkedTableManager
{
    private const int MaxLinkedTableMetadataRows = 4096;
    private static readonly char[] PathSeparators = [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar];

    /// <summary>The <c>MSysObjects</c> columns <see cref="GetLinkedTablesAsync"/> reads; the LvProp, LvModule and LvExtra blobs are not decoded.</summary>
    private static readonly string[] LinkedTableCatalogColumns = ["Name", "Type", "Flags", "Database", "ForeignName", "Connect"];

    /// <summary>
    /// Normalises the caller-supplied allowlist of directories that linked-table
    /// source paths must reside under. Relative entries are resolved against the
    /// directory containing <paramref name="hostDatabasePath"/>.
    /// </summary>
    /// <param name="allowlist">Directory allowlist entries supplied by the caller.</param>
    /// <param name="hostDatabasePath">Path to the database that owns the linked-table definitions.</param>
    internal static string[] NormalizeAllowlist(IReadOnlyList<string> allowlist, string hostDatabasePath)
    {
        if (allowlist == null || allowlist.Count == 0)
        {
            return [];
        }

        string baseDirectory = Path.GetDirectoryName(hostDatabasePath) ?? Directory.GetCurrentDirectory();
        var normalized = new List<string>(allowlist.Count);

        foreach (string path in allowlist)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            string fullPath = ResolvePath(path.Trim(), baseDirectory, "linked-source allowlist");
            normalized.Add(fullPath);
        }

        return normalized.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    /// <summary>
    /// Builds a derivative <see cref="AccessReaderOptions"/> instance suitable for
    /// re-opening the source database referenced by a linked table. The allowlist
    /// is normalised against the host database directory and the validator is
    /// forwarded so transitively linked databases inherit the same security policy.
    /// </summary>
    /// <param name="options">The options.</param>
    /// <param name="hostDatabasePath">The host database path.</param>
    /// <param name="traversal">The ancestry of a linked read.</param>
    /// <param name="password">The explicitly resolved linked-source password.</param>
    /// <exception cref="ArgumentOutOfRangeException">The depth limit is negative.</exception>
    internal static AccessReaderOptions CreateLinkedSourceOpenOptions(
        AccessReaderOptions options,
        string hostDatabasePath,
        LinkedSourceTraversal? traversal = null,
        ReadOnlyMemory<char> password = default)
    {
        if (options.LinkedSourceMaxDepth < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Linked-source depth cannot be negative.");
        }

        return new()
        {
            PageCacheSize = options.PageCacheSize,
            DiagnosticsEnabled = options.DiagnosticsEnabled,
            PageReadOptimizationMode = options.PageReadOptimizationMode,
            ValidateOnOpen = options.ValidateOnOpen,
            StrictParsing = options.StrictParsing,
            MaxEncryptionSpinCount = options.MaxEncryptionSpinCount,
            MaxEncryptionInfoBytes = options.MaxEncryptionInfoBytes,
            MaxLongValueBytes = options.MaxLongValueBytes,
            MaxAttachmentContentBytes = options.MaxAttachmentContentBytes,
            FileAccess = options.FileAccess,
            FileShare = options.FileShare,
            Password = password,
            UseLockFile = options.UseLockFile,
            LockFileUserName = options.LockFileUserName,
            LockFileMachineName = options.LockFileMachineName,
            UseByteRangeLocks = options.UseByteRangeLocks,
            LockTimeoutMilliseconds = options.LockTimeoutMilliseconds,
            LinkedSourcePathAllowlist = NormalizeAllowlist(options.LinkedSourcePathAllowlist, hostDatabasePath),
            LinkedSourcePathValidator = options.LinkedSourcePathValidator,
            LinkedSourceMaxDepth = options.LinkedSourceMaxDepth,
            LinkedSourcePasswordResolver = options.LinkedSourcePasswordResolver,
            LinkedSourceTraversal = traversal ?? options.LinkedSourceTraversal,
            LinkedTextMaxRecordLength = options.LinkedTextMaxRecordLength,
            LinkedTextMaxFieldLength = options.LinkedTextMaxFieldLength,
            LinkedTextMaxColumnCount = options.LinkedTextMaxColumnCount,
            LinkedTextMaxSourceFileBytes = options.LinkedTextMaxSourceFileBytes,
            LinkedTextMaxMaterializedRows = options.LinkedTextMaxMaterializedRows,
        };
    }

    /// <summary>
    /// Enumerates every linked table (Access-file, ODBC, or text) defined in
    /// MSysObjects.
    /// </summary>
    /// <param name="catalog">Reads the <c>MSysObjects</c> rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">Thrown when linked-table metadata exceeds the per-reader row limit.</exception>
    internal static async ValueTask<List<LinkedTableInfo>> GetLinkedTablesAsync(CatalogReader catalog, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await catalog.GetMSysObjectsTableDefAsync(cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return [];
        }

        int idxName = msys.FindColumnIndex("Name");
        int idxType = msys.FindColumnIndex("Type");
        int idxFlags = msys.FindColumnIndex("Flags");
        int idxDatabase = msys.FindColumnIndex("Database");
        int idxForeignName = msys.FindColumnIndex("ForeignName");
        int idxConnect = msys.FindColumnIndex("Connect");

        if (idxName < 0 || idxType < 0)
        {
            return [];
        }

        var result = new List<LinkedTableInfo>();

        await foreach (string[] row in catalog.EnumerateMSysObjectsRowsAsync(msys, LinkedTableCatalogColumns, cancellationToken).ConfigureAwait(false))
        {
            if (!CatalogValueReader.TryParseInt32(row, idxType, out int objType))
            {
                continue;
            }

            if (objType is not Constants.SystemObjects.LinkedTableType and not Constants.SystemObjects.LinkedOdbcType)
            {
                continue;
            }

            string nameStr = CatalogValueReader.GetStringOrEmpty(row, idxName);
            if (string.IsNullOrEmpty(nameStr))
            {
                continue;
            }

            if (CatalogValueReader.TryParseInt64(row, idxFlags, out long flagsLong) &&
                (unchecked((uint)flagsLong) & Constants.SystemObjects.SystemTableMask) != 0)
            {
                continue;
            }

            string connectStr = CatalogValueReader.GetStringOrEmpty(row, idxConnect);
            string foreignName = CatalogValueReader.GetStringOrEmpty(row, idxForeignName);
            string sourcePath = CatalogValueReader.GetStringOrEmpty(row, idxDatabase);
            LinkedTableKind kind = objType switch
            {
                Constants.SystemObjects.LinkedOdbcType => LinkedTableKind.Odbc,
                Constants.SystemObjects.LinkedTableType when !string.IsNullOrEmpty(connectStr) => LinkedTableKind.Text,
                Constants.SystemObjects.LinkedTableType => LinkedTableKind.Access,
                _ => throw new InvalidDataException($"Unsupported linked-table object type: {objType}."),
            };

            if (result.Count >= MaxLinkedTableMetadataRows)
            {
                throw new InvalidDataException(
                    $"Linked-table metadata exceeds the per-reader limit of {MaxLinkedTableMetadataRows} entries.");
            }

            result.Add(new LinkedTableInfo
            {
                Name = nameStr,
                Kind = kind,
                SourceObjectName = kind == LinkedTableKind.Text ? DecodeTextForeignName(foreignName) : foreignName,
                SourcePath = kind == LinkedTableKind.Odbc || string.IsNullOrEmpty(sourcePath) ? null : sourcePath,
                ConnectString = string.IsNullOrEmpty(connectStr) ? null : connectStr,
            });
        }

        return result;
    }

    /// <summary>
    /// Opens the source database referenced by <paramref name="link"/> as a
    /// separate reader, applying the host's allowlist and validator and its
    /// linked-source open options.
    /// </summary>
    /// <param name="policy">The host's linked-source policy.</param>
    /// <param name="link">The linked-table metadata.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="FileNotFoundException">Thrown when the linked source database cannot be found.</exception>
    internal static async ValueTask<AccessReader> OpenLinkedSourceAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        ThrowIfUnsupportedLinkedRead(link);

        string resolvedPath = ResolveLinkedSourcePath(policy, link);
        LinkedSourceTraversal traversal = ExtendLinkedTraversal(policy, link, resolvedPath);
        ReadOnlyMemory<char> password = policy.OpenOptions.LinkedSourcePasswordResolver?.Invoke(link with { }, resolvedPath) ?? default;
        AccessReaderOptions linkedOptions = CreateLinkedSourceOpenOptions(policy.OpenOptions, policy.HostDatabasePath, traversal, password);

        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException(
                $"Source database for linked table '{link.Name}' not found: {resolvedPath}",
                resolvedPath);
        }

        return await AccessReader.OpenAsync(resolvedPath, linkedOptions, cancellationToken).ConfigureAwait(false);
    }

    internal static async ValueTask<long> CountLinkedTextRowsAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextDataSource source = GetLinkedTextDataSource(policy, link);
        using var records = new LinkedTextRecordReader(source);
        return await records.DelimitedReader.CountRecordsAsync(source.Format.HasHeaderRow, cancellationToken).ConfigureAwait(false);
    }

    internal static async IAsyncEnumerable<string[]> RowsLinkedTextAsStringsAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(policy, link, cancellationToken).ConfigureAwait(false);
        long rowCount = 0;

        while (await rows.ReadRowAsync(cancellationToken).ConfigureAwait(false) is { } row)
        {
            cancellationToken.ThrowIfCancellationRequested();
            rowCount++;
            progress?.Report(rowCount);
            yield return row;
        }
    }

    internal static async IAsyncEnumerable<T> RowsLinkedTextMappedAsync<T>(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        IProgress<long>? progress,
        Func<IReadOnlyList<ColumnMetadata>, Func<object?[], T>> mapperFactory,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(policy, link, cancellationToken).ConfigureAwait(false);
        Func<object?[], T> map = mapperFactory(CreateLinkedTextColumnMetadata(rows.ColumnNames));
        long rowCount = 0;

        while (await rows.ReadRowAsync(cancellationToken).ConfigureAwait(false) is { } row)
        {
            cancellationToken.ThrowIfCancellationRequested();
            rowCount++;
            progress?.Report(rowCount);
            yield return map(row);
        }
    }

    internal static async ValueTask<IReadOnlyList<ColumnMetadata>> GetLinkedTextColumnMetadataAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(policy, link, cancellationToken).ConfigureAwait(false);
        return CreateLinkedTextColumnMetadata(rows.ColumnNames);
    }

    internal static async ValueTask<DataTable> ReadLinkedTextDataTableAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        uint? maxRows,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(policy, link, cancellationToken).ConfigureAwait(false);
        DataTable? table = null;
        try
        {
            table = new DataTable(link.Name);
            foreach (string columnName in rows.ColumnNames)
            {
                _ = table.Columns.Add(columnName, typeof(string));
            }

            // The limit is checked before each read, so maxRows: 0 returns the
            // header-only table without reading a record.
            long rowCount = 0;
            while ((!maxRows.HasValue || rowCount < maxRows.Value)
                && await rows.ReadRowAsync(cancellationToken).ConfigureAwait(false) is { } row)
            {
                ThrowIfLinkedTextMaterializedRowLimitExceeded(link.Name, rowCount, rows.MaxMaterializedRows);
                _ = table.Rows.Add(row);
                rowCount++;
                progress?.Report(rowCount);
            }

            DataTable final = table;
            table = null;
            return final;
        }
        finally
        {
            table?.Dispose();
        }
    }

    internal static async ValueTask<IReadOnlyList<T>> ReadLinkedTextMappedRowsAsync<T>(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        uint? maxRows,
        Func<IReadOnlyList<ColumnMetadata>, Func<object?[], T>> mapperFactory,
        CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(policy, link, cancellationToken).ConfigureAwait(false);
        Func<object?[], T> map = mapperFactory(CreateLinkedTextColumnMetadata(rows.ColumnNames));
        var items = new List<T>();

        while ((!maxRows.HasValue || items.Count < maxRows.Value)
            && await rows.ReadRowAsync(cancellationToken).ConfigureAwait(false) is { } row)
        {
            ThrowIfLinkedTextMaterializedRowLimitExceeded(link.Name, items.Count, rows.MaxMaterializedRows);
            items.Add(map(row));
        }

        return items;
    }

    internal static void ThrowIfLinkedTextMaterializedRowLimitExceeded(
        string tableName,
        long rowCount,
        uint? maxMaterializedRows)
    {
        if (maxMaterializedRows.HasValue && rowCount >= maxMaterializedRows.Value)
        {
            throw new InvalidDataException(
                $"Linked text table '{tableName}' exceeds AccessReaderOptions.{nameof(AccessReaderOptions.LinkedTextMaxMaterializedRows)} ({maxMaterializedRows.Value}).");
        }
    }

    // ════════════════════════════════════════════════════════════════
    // Linked-table creation (writer side). AccessWriter exposes thin
    // public-API forwarders and owns the auto-commit scope; the
    // MSysObjects type 4 / 6 catalog rows are emitted here through the
    // shared catalog-artifact plan.
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Creates a linked-table entry (MSysObjects type 6) that references a table
    /// in another Access database. No row data is stored locally.
    /// </summary>
    /// <param name="format">The database's immutable format profile.</param>
    /// <param name="pageSource">The database's page source.</param>
    /// <param name="catalogArtifacts">Emits the catalog object row.</param>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDatabasePath">Path to the source Access database file (.mdb / .accdb).</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static async ValueTask CreateLinkedTableAsync(
        JetFormat format,
        IPageSource pageSource,
        CatalogArtifactWriter catalogArtifacts,
        string linkedTableName,
        string sourceDatabasePath,
        string foreignTableName,
        CancellationToken cancellationToken)
    {
        AccessObjectName.ThrowIfInvalid(linkedTableName, nameof(linkedTableName), "table");
        Guard.NotNullOrEmpty(sourceDatabasePath, nameof(sourceDatabasePath));
        Guard.NotNullOrEmpty(foreignTableName, nameof(foreignTableName));
        AccessObjectName.ThrowIfNotStorable(format, linkedTableName, nameof(linkedTableName), "table");
        ThrowIfNotStorable(format, sourceDatabasePath, nameof(sourceDatabasePath));
        ThrowIfNotStorable(format, foreignTableName, nameof(foreignTableName));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        await catalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan(
                [],
                [CatalogObjectArtifact.LinkedTable(
                    linkedTableName,
                    sourceDatabasePath,
                    foreignTableName,
                    connectString: null,
                    objectType: Constants.SystemObjects.LinkedTableType)]),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a linked-ODBC table entry (MSysObjects type 4). When
    /// <paramref name="cachedSchemaLvProp"/> is supplied it is stored verbatim;
    /// otherwise a cached-schema <c>LvProp</c> block is generated, column-level
    /// when <paramref name="sourceColumns"/> is supplied and table-level otherwise.
    /// </summary>
    /// <param name="format">The database's immutable format profile.</param>
    /// <param name="pageSource">The database's page source.</param>
    /// <param name="catalogArtifacts">Emits the catalog object row.</param>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cachedSchemaLvProp">A payload already validated by <see cref="CopyValidatedCachedSchemaLvProp"/>, or <see langword="null"/>.</param>
    /// <param name="sourceColumns">Optional column definitions for the remote source table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static async ValueTask CreateLinkedOdbcTableAsync(
        JetFormat format,
        IPageSource pageSource,
        CatalogArtifactWriter catalogArtifacts,
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        byte[]? cachedSchemaLvProp,
        IReadOnlyList<ColumnDefinition>? sourceColumns,
        CancellationToken cancellationToken)
    {
        AccessObjectName.ThrowIfInvalid(linkedTableName, nameof(linkedTableName), "table");
        Guard.NotNullOrEmpty(connectionString, nameof(connectionString));
        Guard.NotNullOrEmpty(foreignTableName, nameof(foreignTableName));
        AccessObjectName.ThrowIfNotStorable(format, linkedTableName, nameof(linkedTableName), "table");
        ThrowIfNotStorable(format, connectionString, nameof(connectionString));
        ThrowIfNotStorable(format, foreignTableName, nameof(foreignTableName));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        string normalizedConnect = connectionString.StartsWith("ODBC;", StringComparison.OrdinalIgnoreCase)
            ? connectionString
            : "ODBC;" + connectionString;

        if (sourceColumns is not null)
        {
            LinkedOdbcLvPropBuilder.ValidateSourceColumns(sourceColumns, nameof(sourceColumns));
        }

        byte[] lvProp = cachedSchemaLvProp ?? LinkedOdbcLvPropBuilder.Build(foreignTableName, sourceColumns, format);

        await catalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan(
                [],
                [CatalogObjectArtifact.LinkedTable(
                    linkedTableName,
                    sourceDatabasePath: null,
                    foreignName: foreignTableName,
                    connectString: normalizedConnect,
                    objectType: Constants.SystemObjects.LinkedOdbcType,
                    cachedSchemaLvProp: lvProp)]),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a linked-text/CSV table entry (MSysObjects type 6) that references a
    /// text or CSV file in a directory.
    /// </summary>
    /// <param name="format">The database's immutable format profile.</param>
    /// <param name="pageSource">The database's page source.</param>
    /// <param name="catalogArtifacts">Emits the catalog object row.</param>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDirectoryPath">Path to the directory containing the text/CSV source file.</param>
    /// <param name="foreignFileName">The filename of the text/CSV source (e.g. <c>"data.csv"</c>).</param>
    /// <param name="connectString">The text-driver connect string (e.g. <c>"Text;HDR=YES;FMT=Delimited"</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static async ValueTask CreateLinkedTextTableAsync(
        JetFormat format,
        IPageSource pageSource,
        CatalogArtifactWriter catalogArtifacts,
        string linkedTableName,
        string sourceDirectoryPath,
        string foreignFileName,
        string connectString,
        CancellationToken cancellationToken)
    {
        AccessObjectName.ThrowIfInvalid(linkedTableName, nameof(linkedTableName), "table");
        Guard.NotNullOrEmpty(sourceDirectoryPath, nameof(sourceDirectoryPath));
        Guard.NotNullOrEmpty(foreignFileName, nameof(foreignFileName));
        Guard.NotNullOrEmpty(connectString, nameof(connectString));
        AccessObjectName.ThrowIfNotStorable(format, linkedTableName, nameof(linkedTableName), "table");
        ThrowIfNotStorable(format, sourceDirectoryPath, nameof(sourceDirectoryPath));
        ThrowIfNotStorable(format, foreignFileName, nameof(foreignFileName));
        ThrowIfNotStorable(format, connectString, nameof(connectString));
        pageSource.ThrowIfDisposedOrCancelled(cancellationToken);

        await catalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan(
                [],
                [CatalogObjectArtifact.LinkedTable(
                    linkedTableName,
                    sourceDirectoryPath,
                    foreignFileName,
                    connectString,
                    Constants.SystemObjects.LinkedTableType)]),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Validates and copies a caller-supplied Access/DAO cached-schema payload
    /// for <c>MSysObjects.LvProp</c>. Runs before any catalog mutation begins.
    /// </summary>
    /// <param name="format">The database format, which selects the expected property-block magic.</param>
    /// <param name="cachedSchemaLvProp">The caller-supplied payload.</param>
    /// <param name="paramName">The public parameter name, for <see cref="ArgumentException"/>.</param>
    /// <returns>A private copy of the validated payload.</returns>
    /// <exception cref="ArgumentException">Thrown when the payload is empty or not a property block for <paramref name="format"/>.</exception>
    internal static byte[] CopyValidatedCachedSchemaLvProp(JetFormat format, ReadOnlyMemory<byte> cachedSchemaLvProp, string paramName)
    {
        if (cachedSchemaLvProp.IsEmpty)
        {
            throw new ArgumentException("Cached schema LvProp cannot be empty.", paramName);
        }

        byte[] copy = cachedSchemaLvProp.ToArray();
        uint expectedMagic = JetFormat.PropertyBlockMagicOf(format.Kind);
        if (copy.Length < sizeof(uint) || JetTypeInfo.Ru32(copy, 0) != expectedMagic)
        {
            throw new ArgumentException("Cached schema LvProp must use the property-block magic for this database format.", paramName);
        }

        var block = ColumnPropertyBlock.Parse(copy, format);
        if (block is null || block.Targets.Count == 0)
        {
            throw new ArgumentException("Cached schema LvProp must contain at least one property target.", paramName);
        }

        return copy;
    }

    /// <summary>Refuses cycles and excessive recursion before touching the next source.</summary>
    /// <param name="policy">The inherited linked-source policy.</param>
    /// <param name="link">The next linked table.</param>
    /// <param name="resolvedPath">The authorized source path.</param>
    /// <returns>The new immutable ancestry.</returns>
    /// <exception cref="InvalidDataException">The read contains a cycle or exceeds its depth limit.</exception>
    private static LinkedSourceTraversal ExtendLinkedTraversal(LinkedSourcePolicy policy, LinkedTableInfo link, string resolvedPath)
    {
        LinkedSourceTraversal? previous = policy.OpenOptions.LinkedSourceTraversal;
        int depth = previous?.Depth ?? 0;
        if (depth >= policy.OpenOptions.LinkedSourceMaxDepth)
        {
            throw new InvalidDataException($"Linked table '{link.Name}' exceeds the linked-source depth limit ({policy.OpenOptions.LinkedSourceMaxDepth}).");
        }

        List<KeyValuePair<string, string>> ancestors = previous is null ? [] : [.. previous.Ancestors];
        if (!string.IsNullOrEmpty(policy.HostDatabasePath))
        {
            ancestors.Add(new KeyValuePair<string, string>(Path.GetFullPath(policy.HostDatabasePath), link.Name));
        }

        StringComparison pathComparison = Path.DirectorySeparatorChar == '\\' ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        foreach (KeyValuePair<string, string> ancestor in ancestors)
        {
            if (string.Equals(ancestor.Key, resolvedPath, pathComparison)
                && string.Equals(ancestor.Value, link.SourceObjectName, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException($"Linked table '{link.Name}' contains a linked-source cycle at '{resolvedPath}' table '{link.SourceObjectName}'.");
            }
        }

        ancestors.Add(new KeyValuePair<string, string>(resolvedPath, link.SourceObjectName));
        return new LinkedSourceTraversal(depth + 1, ancestors);
    }

    /// <summary>
    /// Refuses a link argument that the database would not store as given: a
    /// Jet3 <c>MSysObjects</c> row holds the foreign name, path and connect
    /// string in the database's code page (<see cref="JetFormat.DescribeUnstorableCharacter"/>).
    /// </summary>
    /// <param name="format">The database's immutable format profile.</param>
    /// <param name="value">The argument.</param>
    /// <param name="paramName">The public parameter that carries it.</param>
    /// <exception cref="ArgumentException"><paramref name="value"/> holds a character the database's code page does not have.</exception>
    private static void ThrowIfNotStorable(JetFormat format, string value, string paramName)
    {
        if (format.DescribeUnstorableCharacter(value) is { } character)
        {
            throw new ArgumentException(format.UnstorableTextMessage($"The {paramName} '{value}'", character), paramName);
        }
    }

    private static LinkedTextLimits CreateLinkedTextLimits(AccessReaderOptions options)
    {
        ValidatePositiveLimit(
            options.LinkedTextMaxRecordLength,
            nameof(AccessReaderOptions.LinkedTextMaxRecordLength));
        ValidatePositiveLimit(
            options.LinkedTextMaxFieldLength,
            nameof(AccessReaderOptions.LinkedTextMaxFieldLength));
        ValidatePositiveLimit(
            options.LinkedTextMaxColumnCount,
            nameof(AccessReaderOptions.LinkedTextMaxColumnCount));

        if (options.LinkedTextMaxSourceFileBytes.HasValue)
        {
            ValidatePositiveLimit(
                options.LinkedTextMaxSourceFileBytes.Value,
                nameof(AccessReaderOptions.LinkedTextMaxSourceFileBytes));
        }

        if (options.LinkedTextMaxMaterializedRows == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.LinkedTextMaxMaterializedRows.Value,
                $"{nameof(AccessReaderOptions.LinkedTextMaxMaterializedRows)} must be positive when set.");
        }

        var delimitedLimits = new DelimitedTextLimits(
            options.LinkedTextMaxRecordLength,
            options.LinkedTextMaxFieldLength,
            options.LinkedTextMaxColumnCount,
            $"{nameof(AccessReaderOptions)}.{nameof(AccessReaderOptions.LinkedTextMaxRecordLength)}",
            $"{nameof(AccessReaderOptions)}.{nameof(AccessReaderOptions.LinkedTextMaxFieldLength)}",
            $"{nameof(AccessReaderOptions)}.{nameof(AccessReaderOptions.LinkedTextMaxColumnCount)}");

        return new LinkedTextLimits(
            delimitedLimits,
            options.LinkedTextMaxSourceFileBytes,
            options.LinkedTextMaxMaterializedRows);
    }

    private static void ValidatePositiveLimit(int value, string optionName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(optionName, value, $"{optionName} must be positive.");
        }
    }

    private static void ValidatePositiveLimit(long value, string optionName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(optionName, value, $"{optionName} must be positive.");
        }
    }

    private static void ValidateLinkedTextSourceFileSize(string filePath, LinkedTextLimits limits, string tableName)
    {
        if (!limits.MaxSourceFileBytes.HasValue)
        {
            return;
        }

        long length = new FileInfo(filePath).Length;
        if (length > limits.MaxSourceFileBytes.Value)
        {
            throw new InvalidDataException(
                $"Linked text table '{tableName}' source file exceeds AccessReaderOptions.{nameof(AccessReaderOptions.LinkedTextMaxSourceFileBytes)} ({limits.MaxSourceFileBytes.Value}).");
        }
    }

    private static string ResolveLinkedSourcePath(
        LinkedTableInfo link,
        string hostDatabasePath,
        IReadOnlyList<string> linkedSourcePathAllowlist,
        Func<LinkedTableInfo, string, bool>? linkedSourcePathValidator)
    {
        if (string.IsNullOrWhiteSpace(link.SourcePath))
        {
            throw new FileNotFoundException(
                $"Source path for linked table '{link.Name}' not found: {link.SourcePath}",
                link.SourcePath);
        }

        string rawPath = link.SourcePath.Trim();
        bool hasHostDatabasePath = !string.IsNullOrWhiteSpace(hostDatabasePath);
        string baseDirectory = hasHostDatabasePath
            ? Path.GetDirectoryName(hostDatabasePath) ?? Directory.GetCurrentDirectory()
            : Directory.GetCurrentDirectory();
        string resolvedPath = ResolvePath(rawPath, baseDirectory, $"linked table '{link.Name}'");
        bool isWithinHostDatabaseDirectory = hasHostDatabasePath && IsPathWithinDirectory(resolvedPath, baseDirectory);
        bool callbackApproved = linkedSourcePathValidator?.Invoke(link with { }, resolvedPath) ?? false;
        string? allowlistRoot = linkedSourcePathAllowlist.FirstOrDefault(root => IsPathWithinDirectory(resolvedPath, root));

        if (!hasHostDatabasePath && linkedSourcePathAllowlist.Count == 0 && !callbackApproved)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{link.SourcePath}' cannot be resolved safely because the host database was opened from a stream. " +
                "Use AccessReaderOptions.LinkedSourcePathAllowlist or LinkedSourcePathValidator to explicitly allow trusted paths.");
        }

        if (!isWithinHostDatabaseDirectory && linkedSourcePathAllowlist.Count == 0 && !callbackApproved)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{link.SourcePath}' is outside the host database directory. " +
                "Use AccessReaderOptions.LinkedSourcePathValidator to explicitly allow trusted paths.");
        }

        if (linkedSourcePathAllowlist.Count > 0 &&
            allowlistRoot == null)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{resolvedPath}' is not permitted by AccessReaderOptions.LinkedSourcePathAllowlist.");
        }

        if (linkedSourcePathValidator != null && !callbackApproved)
        {
            throw new UnauthorizedAccessException(
                $"Linked table '{link.Name}' source path '{resolvedPath}' was rejected by AccessReaderOptions.LinkedSourcePathValidator.");
        }

        string trustedDirectory = allowlistRoot
            ?? (isWithinHostDatabaseDirectory ? baseDirectory : Path.GetDirectoryName(resolvedPath) ?? resolvedPath);
        EnsurePathDoesNotCrossReparsePoint(
            resolvedPath,
            trustedDirectory,
            targetIsDirectory: link.Kind == LinkedTableKind.Text,
            context: $"linked table '{link.Name}' source path");

        return resolvedPath;
    }

    private static string ResolveLinkedTextSourceFilePath(LinkedSourcePolicy policy, LinkedTableInfo link)
    {
        if (link.Kind != LinkedTableKind.Text)
        {
            ThrowIfUnsupportedLinkedRead(link);
        }

        string resolvedDirectory = ResolveLinkedSourcePath(policy, link);

        if (string.IsNullOrWhiteSpace(link.SourceObjectName))
        {
            throw new FileNotFoundException(
                $"Text source for linked table '{link.Name}' not found: {link.SourceObjectName}",
                link.SourceObjectName);
        }

        string resolvedFilePath = ResolvePath(
            link.SourceObjectName.Trim(),
            resolvedDirectory,
            $"linked text table '{link.Name}'");
        if (!IsPathWithinDirectory(resolvedFilePath, resolvedDirectory))
        {
            throw new UnauthorizedAccessException(
                $"Linked text table '{link.Name}' source file '{link.SourceObjectName}' is outside its source directory.");
        }

        EnsurePathDoesNotCrossReparsePoint(
            resolvedFilePath,
            resolvedDirectory,
            targetIsDirectory: false,
            context: $"linked text table '{link.Name}' source file");

        return resolvedFilePath;
    }

    private static string ResolveLinkedSourcePath(LinkedSourcePolicy policy, LinkedTableInfo link)
    {
        AccessReaderOptions linkedOptions = policy.OpenOptions;
        return ResolveLinkedSourcePath(
            link,
            policy.HostDatabasePath,
            linkedOptions.LinkedSourcePathAllowlist,
            linkedOptions.LinkedSourcePathValidator);
    }

    private static async ValueTask<LinkedTextRowReader> OpenLinkedTextRowsAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextDataSource source = GetLinkedTextDataSource(policy, link);
        return await LinkedTextRowReader.OpenAsync(source, cancellationToken).ConfigureAwait(false);
    }

    private static LinkedTextDataSource GetLinkedTextDataSource(LinkedSourcePolicy policy, LinkedTableInfo link)
    {
        LinkedTextLimits limits = CreateLinkedTextLimits(policy.OpenOptions);
        string resolvedPath = ResolveLinkedTextSourceFilePath(policy, link);
        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException(
                $"Text source for linked table '{link.Name}' not found: {resolvedPath}",
                resolvedPath);
        }

        ValidateLinkedTextSourceFileSize(resolvedPath, limits, link.Name);
        return new LinkedTextDataSource(resolvedPath, ParseTextLinkFormat(link.ConnectString), limits);
    }

    private static List<ColumnMetadata> CreateLinkedTextColumnMetadata(string[] columnNames)
    {
        var metadata = new List<ColumnMetadata>(columnNames.Length);
        for (int i = 0; i < columnNames.Length; i++)
        {
            metadata.Add(new ColumnMetadata
            {
                Name = columnNames[i],
                TypeName = "Text",
                ClrType = typeof(string),
                IsNullable = true,
                IsFixedLength = false,
                Ordinal = i,
                Size = ColumnSize.Variable,
            });
        }

        return metadata;
    }

    private static void ThrowIfUnsupportedLinkedRead(LinkedTableInfo link)
    {
        if (link.Kind == LinkedTableKind.Access)
        {
            return;
        }

        string kindDescription = link.Kind switch
        {
            LinkedTableKind.Access => "Access-file",
            LinkedTableKind.Odbc => "ODBC",
            LinkedTableKind.Text => "text",
            _ => "non-Access",
        };

        throw new NotSupportedException(
            $"Linked {kindDescription} table '{link.Name}' is metadata-only; JetDatabaseWriter opens Access-file linked tables and reads delimited text links.");
    }

    private static DelimitedTextFormat ParseTextLinkFormat(string? connectString)
    {
        bool hasHeaderRow = false;
        char delimiter = ',';
        string? format = null;

        if (!string.IsNullOrWhiteSpace(connectString))
        {
            foreach (string rawPart in SplitConnectStringParts(connectString))
            {
                string part = rawPart.Trim();
                int separator = part.IndexOf('=', StringComparison.Ordinal);
                if (separator < 0)
                {
                    continue;
                }

                string key = part[..separator].Trim();
                string value = part[(separator + 1)..].Trim();
                if (key.Equals("HDR", StringComparison.OrdinalIgnoreCase))
                {
                    hasHeaderRow = value.Equals("YES", StringComparison.OrdinalIgnoreCase)
                        || value.Equals("TRUE", StringComparison.OrdinalIgnoreCase)
                        || value == "1";
                }
                else if (key.Equals("FMT", StringComparison.OrdinalIgnoreCase))
                {
                    format = value;
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(format))
        {
            if (format.Equals("FixedLength", StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException("Linked text tables with FMT=FixedLength are not supported by the managed CSV reader.");
            }

            if (format.Equals("TabDelimited", StringComparison.OrdinalIgnoreCase))
            {
                delimiter = '\t';
            }
            else if (format.StartsWith("Delimited(", StringComparison.OrdinalIgnoreCase))
            {
                int start = format.IndexOf('(', StringComparison.Ordinal) + 1;
                int end = format.IndexOf(')', start);
                if (end > start)
                {
                    delimiter = format[start];
                }
            }
            else if (!format.Equals("Delimited", StringComparison.OrdinalIgnoreCase)
                && !format.Equals("CSVDelimited", StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException($"Linked text tables with FMT={format} are not supported by the managed CSV reader.");
            }
        }

        return new DelimitedTextFormat(hasHeaderRow, delimiter, trimValues: true);
    }

    private static IEnumerable<string> SplitConnectStringParts(string connectString)
    {
        int start = 0;
        int parenthesisDepth = 0;
        for (int i = 0; i < connectString.Length; i++)
        {
            char ch = connectString[i];
            if (ch == '(')
            {
                parenthesisDepth++;
            }
            else if (ch == ')' && parenthesisDepth > 0)
            {
                parenthesisDepth--;
            }
            else if (ch == ';' && parenthesisDepth == 0)
            {
                yield return connectString[start..i];
                start = i + 1;
            }
        }

        yield return connectString[start..];
    }

    private static string[] NormalizeStringRow(string[] row, int columnCount)
    {
        if (row.Length == columnCount)
        {
            return row;
        }

        string[] normalized = new string[columnCount];
        int copyCount = Math.Min(row.Length, columnCount);
        for (int i = 0; i < copyCount; i++)
        {
            normalized[i] = row[i];
        }

        for (int i = copyCount; i < normalized.Length; i++)
        {
            normalized[i] = string.Empty;
        }

        return normalized;
    }

    private static string ResolvePath(string path, string baseDirectory, string context)
    {
        ThrowIfUnsafeWindowsPath(path, context);
        try
        {
            string fullBaseDirectory = Path.GetFullPath(baseDirectory);
            string fullPath = Path.GetFullPath(path, fullBaseDirectory);
            ThrowIfUnsafeWindowsPath(fullPath, context);
            return fullPath;
        }
        catch (Exception ex) when (
            ex is ArgumentException or
            NotSupportedException or
            PathTooLongException)
        {
            throw new UnauthorizedAccessException(
                $"Invalid path in {context}: '{path}'.",
                ex);
        }
    }

    /// <summary>Rejects Windows device namespaces and ambiguous drive-relative sources.</summary>
    /// <param name="path">The path supplied by database metadata or caller policy.</param>
    /// <param name="context">The failure context.</param>
    /// <exception cref="UnauthorizedAccessException">The path can address a device or has ambiguous Windows semantics.</exception>
    private static void ThrowIfUnsafeWindowsPath(string path, string context)
    {
        string devicePath = path.Replace('/', '\\');
        if (devicePath.StartsWith("\\\\?\\", StringComparison.Ordinal)
            || devicePath.StartsWith("\\\\.\\", StringComparison.Ordinal)
            || devicePath.StartsWith("\\??\\", StringComparison.Ordinal)
            || (path.Length >= 2 && path[1] == ':' && (path.Length == 2 || (path[2] != '\\' && path[2] != '/'))))
        {
            throw new UnauthorizedAccessException($"Invalid device or drive-relative path in {context}: '{path}'.");
        }

        if (Path.DirectorySeparatorChar != '\\')
        {
            return;
        }

        string withoutDrive = path.Length >= 2 && path[1] == ':' ? path[2..] : path;
        foreach (string segment in withoutDrive.Split(PathSeparators, StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment is "." or "..")
            {
                continue;
            }

            string stem = segment.Split('.')[0];
            bool reserved = stem.Equals("CON", StringComparison.OrdinalIgnoreCase)
                || stem.Equals("PRN", StringComparison.OrdinalIgnoreCase)
                || stem.Equals("AUX", StringComparison.OrdinalIgnoreCase)
                || stem.Equals("NUL", StringComparison.OrdinalIgnoreCase)
                || stem.Equals("CONIN$", StringComparison.OrdinalIgnoreCase)
                || stem.Equals("CONOUT$", StringComparison.OrdinalIgnoreCase)
                || (stem.Length == 4 && (stem[3] is (>= '1' and <= '9') or '¹' or '²' or '³')
                    && (stem.StartsWith("COM", StringComparison.OrdinalIgnoreCase) || stem.StartsWith("LPT", StringComparison.OrdinalIgnoreCase)));
            if (reserved || segment.Contains(':', StringComparison.Ordinal) || segment.EndsWith('.') || segment.EndsWith(' '))
            {
                throw new UnauthorizedAccessException($"Invalid device, alternate stream or aliased path in {context}: '{path}'.");
            }
        }
    }

    private static void EnsurePathDoesNotCrossReparsePoint(
        string path,
        string trustedDirectory,
        bool targetIsDirectory,
        string context)
    {
        string fullTrustedDirectory = Path.GetFullPath(trustedDirectory);
        string fullPath = Path.GetFullPath(path, fullTrustedDirectory);
        if (!IsPathWithinDirectory(fullPath, fullTrustedDirectory))
        {
            throw new UnauthorizedAccessException(
                $"{context} '{path}' is outside trusted directory '{trustedDirectory}'.");
        }

        string directoryToCheck = targetIsDirectory ? fullPath : Path.GetDirectoryName(fullPath) ?? fullTrustedDirectory;
        for (string? ancestor = fullTrustedDirectory; ancestor != null; ancestor = Path.GetDirectoryName(ancestor))
        {
            CheckExistingDirectoryForReparsePoint(ancestor, context);
        }

        string relativeDirectory = Path.GetRelativePath(fullTrustedDirectory, directoryToCheck);
        if (!string.Equals(relativeDirectory, ".", StringComparison.Ordinal))
        {
            string current = fullTrustedDirectory;
            string[] segments = relativeDirectory.Split(
                PathSeparators,
                StringSplitOptions.RemoveEmptyEntries);
            foreach (string segment in segments)
            {
                current = Path.Combine(current, segment);
                CheckExistingDirectoryForReparsePoint(current, context);
            }
        }

        if (!targetIsDirectory && File.Exists(fullPath))
        {
            CheckExistingFileForReparsePoint(fullPath, context);
        }
    }

    private static void CheckExistingDirectoryForReparsePoint(string directoryPath, string context)
    {
        if (!Directory.Exists(directoryPath))
        {
            return;
        }

        CheckExistingPathForReparsePoint(directoryPath, context);
    }

    private static void CheckExistingFileForReparsePoint(string filePath, string context)
    {
        if (!File.Exists(filePath))
        {
            return;
        }

        CheckExistingPathForReparsePoint(filePath, context);
    }

    private static void CheckExistingPathForReparsePoint(string path, string context)
    {
        try
        {
            if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
            {
                throw new UnauthorizedAccessException(
                    $"{context} '{path}' crosses a filesystem reparse point.");
            }
        }
        catch (UnauthorizedAccessException)
        {
            throw;
        }
        catch (Exception ex) when (
            ex is IOException or
            ArgumentException or
            NotSupportedException or
            PathTooLongException)
        {
            throw new UnauthorizedAccessException(
                $"Unable to verify {context} '{path}' for filesystem reparse points.",
                ex);
        }
    }

    private static bool IsPathWithinDirectory(string path, string directory)
    {
        string fullDirectory = Path.GetFullPath(directory);
        string fullPath = Path.GetFullPath(path, fullDirectory);
        string relativePath = Path.GetRelativePath(fullDirectory, fullPath);
        return relativePath.Length == 0
            || string.Equals(relativePath, ".", StringComparison.Ordinal)
            || (!Path.IsPathRooted(relativePath) && !StartsWithParentDirectoryTraversal(relativePath));
    }

    private static bool StartsWithParentDirectoryTraversal(string relativePath)
    {
        if (relativePath.Equals("..", StringComparison.Ordinal))
        {
            return true;
        }

        if (relativePath.Length < 3 || relativePath[0] != '.' || relativePath[1] != '.')
        {
            return false;
        }

        char separator = relativePath[2];
        return separator == Path.DirectorySeparatorChar || separator == Path.AltDirectorySeparatorChar;
    }

    private static string DecodeTextForeignName(string foreignName) =>
        foreignName.Replace('#', '.');

    private readonly record struct LinkedTextDataSource(string FilePath, DelimitedTextFormat Format, LinkedTextLimits Limits);

    private readonly record struct LinkedTextLimits(
        DelimitedTextLimits Delimited,
        long? MaxSourceFileBytes,
        uint? MaxMaterializedRows);

    private sealed class LinkedTextRecordReader : IDisposable
    {
        private readonly StreamReader textReader;

        internal LinkedTextRecordReader(LinkedTextDataSource source)
        {
            this.textReader = new StreamReader(source.FilePath, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            this.DelimitedReader = new DelimitedTextReader(this.textReader, source.Format, source.Limits.Delimited);
        }

        internal DelimitedTextReader DelimitedReader { get; }

        public void Dispose()
        {
            this.DelimitedReader.Dispose();
            this.textReader.Dispose();
        }
    }

    private sealed class LinkedTextRowReader : IDisposable
    {
        private readonly LinkedTextRecordReader records;
        private readonly int columnCount;
        private string[]? firstDataRow;
        private bool hasFirstDataRow;

        private LinkedTextRowReader(
            LinkedTextRecordReader records,
            string[] columnNames,
            string[]? firstDataRow,
            bool hasFirstDataRow,
            uint? maxMaterializedRows)
        {
            this.records = records;
            this.ColumnNames = columnNames;
            this.columnCount = columnNames.Length;
            this.firstDataRow = firstDataRow;
            this.hasFirstDataRow = hasFirstDataRow;
            this.MaxMaterializedRows = maxMaterializedRows;
        }

        internal string[] ColumnNames { get; }

        internal uint? MaxMaterializedRows { get; }

        internal static async ValueTask<LinkedTextRowReader> OpenAsync(
            LinkedTextDataSource source,
            CancellationToken cancellationToken)
        {
            LinkedTextRecordReader? records = null;
            try
            {
                records = new LinkedTextRecordReader(source);
                DelimitedTextRecord? firstRecord = await records.DelimitedReader.ReadRecordAsync(cancellationToken).ConfigureAwait(false);
                string[] columnNames;
                string[]? firstDataRow = null;
                bool hasFirstDataRow = false;

                if (firstRecord is not { } record)
                {
                    columnNames = [];
                }
                else if (source.Format.HasHeaderRow)
                {
                    columnNames = DelimitedTextColumnNames.Normalize(record.Fields);
                }
                else
                {
                    columnNames = DelimitedTextColumnNames.CreateGenerated(record.FieldCount);
                    firstDataRow = NormalizeStringRow(record.Fields, columnNames.Length);
                    hasFirstDataRow = true;
                }

                LinkedTextRowReader result = new(
                    records,
                    columnNames,
                    firstDataRow,
                    hasFirstDataRow,
                    source.Limits.MaxMaterializedRows);
                records = null;
                return result;
            }
            finally
            {
                records?.Dispose();
            }
        }

        internal async ValueTask<string[]?> ReadRowAsync(CancellationToken cancellationToken)
        {
            if (this.hasFirstDataRow)
            {
                this.hasFirstDataRow = false;
                string[] row = this.firstDataRow!;
                this.firstDataRow = null;
                return row;
            }

            DelimitedTextRecord? record = await this.records.DelimitedReader.ReadRecordAsync(cancellationToken).ConfigureAwait(false);
            return record is { } current
                ? NormalizeStringRow(current.Fields, this.columnCount)
                : null;
        }

        public void Dispose() => this.records.Dispose();
    }
}
