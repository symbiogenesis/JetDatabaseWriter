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
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Centralises all linked-table (MSysObjects type 4 / 6) logic: the reader-side
/// discovery, resolution, and opening of links referenced by an
/// <see cref="AccessReader"/>, and the writer-side creation of Access-file, ODBC,
/// and text/CSV link catalog entries for an <see cref="AccessWriter"/>. Pure
/// path-handling helpers and the MSysObjects scan that produces
/// <see cref="LinkedTableInfo"/> entries live here so <see cref="AccessReader"/>
/// and <see cref="AccessWriter"/> keep only thin forwarders that delegate to this
/// manager.
/// </summary>
internal static class LinkedTableManager
{
    private const int MaxLinkedTableMetadataRows = 4096;
    private static readonly char[] PathSeparators = [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar];

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
    internal static AccessReaderOptions CreateLinkedSourceOpenOptions(
        AccessReaderOptions options,
        string hostDatabasePath) => new()
        {
            PageCacheSize = options.PageCacheSize,
            DiagnosticsEnabled = options.DiagnosticsEnabled,
            PageReadOptimizationMode = options.PageReadOptimizationMode,
            ValidateOnOpen = options.ValidateOnOpen,
            StrictParsing = options.StrictParsing,
            FileAccess = options.FileAccess,
            FileShare = options.FileShare,
            Password = options.Password,
            UseLockFile = options.UseLockFile,
            LockFileUserName = options.LockFileUserName,
            LockFileMachineName = options.LockFileMachineName,
            UseByteRangeLocks = options.UseByteRangeLocks,
            LockTimeoutMilliseconds = options.LockTimeoutMilliseconds,
            LinkedSourcePathAllowlist = NormalizeAllowlist(options.LinkedSourcePathAllowlist, hostDatabasePath),
            LinkedSourcePathValidator = options.LinkedSourcePathValidator,
            LinkedTextMaxRecordLength = options.LinkedTextMaxRecordLength,
            LinkedTextMaxFieldLength = options.LinkedTextMaxFieldLength,
            LinkedTextMaxColumnCount = options.LinkedTextMaxColumnCount,
            LinkedTextMaxSourceFileBytes = options.LinkedTextMaxSourceFileBytes,
            LinkedTextMaxMaterializedRows = options.LinkedTextMaxMaterializedRows,
        };

    /// <summary>
    /// Enumerates every linked table (Access-file, ODBC, or text) defined in
    /// MSysObjects on the given <paramref name="reader"/>.
    /// </summary>
    /// <param name="reader">The reader.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">Thrown when linked-table metadata exceeds the per-reader row limit.</exception>
    internal static async ValueTask<List<LinkedTableInfo>> GetLinkedTablesAsync(AccessReader reader, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        TableDef? msys = await reader.GetMSysObjectsTableDefAsync(cancellationToken).ConfigureAwait(false);
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

        await foreach (string[] row in reader.EnumerateMSysObjectsRowsAsync(msys, cancellationToken).ConfigureAwait(false))
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
    /// Locates the linked-table entry matching <paramref name="tableName"/>
    /// (case-insensitive) or returns <see langword="null"/> when the name does
    /// not refer to a linked table.
    /// </summary>
    /// <param name="reader">The reader.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal static async ValueTask<LinkedTableInfo?> FindLinkedTableAsync(AccessReader reader, string tableName, CancellationToken cancellationToken)
    {
        List<LinkedTableInfo> links = await reader.GetLinkedTablesCachedAsync(cancellationToken).ConfigureAwait(false);
        LinkedTableInfo? link = links.Find(l => string.Equals(l.Name, tableName, StringComparison.OrdinalIgnoreCase));
        return link is null ? null : link with { };
    }

    /// <summary>
    /// Opens the source database referenced by <paramref name="link"/>, applying
    /// the host reader's allowlist and validator and reusing its cached
    /// linked-source open options.
    /// </summary>
    /// <param name="reader">The reader.</param>
    /// <param name="link">The linked-table metadata.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="FileNotFoundException">Thrown when the linked source database cannot be found.</exception>
    internal static async ValueTask<AccessReader> OpenLinkedSourceAsync(
        AccessReader reader,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        ThrowIfUnsupportedLinkedRead(link);

        AccessReaderOptions linkedOptions = reader.LinkedSourceOpenOptions;
        string resolvedPath = ResolveLinkedSourcePath(reader, link);

        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException(
                $"Source database for linked table '{link.Name}' not found: {resolvedPath}",
                resolvedPath);
        }

        return await AccessReader.OpenAsync(resolvedPath, linkedOptions, cancellationToken).ConfigureAwait(false);
    }

    internal static async ValueTask<long> CountLinkedTextRowsAsync(
        AccessReader reader,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextDataSource source = GetLinkedTextDataSource(reader, link);
        using var records = new LinkedTextRecordReader(source);
        return await records.DelimitedReader.CountRecordsAsync(source.Format.HasHeaderRow, cancellationToken).ConfigureAwait(false);
    }

    internal static async IAsyncEnumerable<string[]> RowsLinkedTextAsStringsAsync(
        AccessReader reader,
        LinkedTableInfo link,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(reader, link, cancellationToken).ConfigureAwait(false);
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
        AccessReader reader,
        LinkedTableInfo link,
        IProgress<long>? progress,
        Func<IReadOnlyList<ColumnMetadata>, Func<object?[], T>> mapperFactory,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(reader, link, cancellationToken).ConfigureAwait(false);
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
        AccessReader reader,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(reader, link, cancellationToken).ConfigureAwait(false);
        return CreateLinkedTextColumnMetadata(rows.ColumnNames);
    }

    internal static async ValueTask<DataTable> ReadLinkedTextDataTableAsync(
        AccessReader reader,
        LinkedTableInfo link,
        uint? maxRows,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(reader, link, cancellationToken).ConfigureAwait(false);
        DataTable? table = null;
        try
        {
            table = new DataTable(link.Name);
            foreach (string columnName in rows.ColumnNames)
            {
                _ = table.Columns.Add(columnName, typeof(string));
            }

            long rowCount = 0;
            while (await rows.ReadRowAsync(cancellationToken).ConfigureAwait(false) is { } row)
            {
                ThrowIfLinkedTextMaterializedRowLimitExceeded(link.Name, rowCount, rows.MaxMaterializedRows);
                _ = table.Rows.Add(row);
                rowCount++;
                progress?.Report(rowCount);
                if (maxRows.HasValue && rowCount >= maxRows.Value)
                {
                    DataTable result = table;
                    table = null;
                    return result;
                }
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
        AccessReader reader,
        LinkedTableInfo link,
        uint? maxRows,
        Func<IReadOnlyList<ColumnMetadata>, Func<object?[], T>> mapperFactory,
        CancellationToken cancellationToken)
    {
        using LinkedTextRowReader rows = await OpenLinkedTextRowsAsync(reader, link, cancellationToken).ConfigureAwait(false);
        Func<object?[], T> map = mapperFactory(CreateLinkedTextColumnMetadata(rows.ColumnNames));
        var items = new List<T>();

        while (await rows.ReadRowAsync(cancellationToken).ConfigureAwait(false) is { } row)
        {
            ThrowIfLinkedTextMaterializedRowLimitExceeded(link.Name, items.Count, rows.MaxMaterializedRows);
            items.Add(map(row));
            if (maxRows.HasValue && items.Count >= maxRows.Value)
            {
                break;
            }
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
    // public-API forwarders; the MSysObjects type 4 / 6 catalog rows are
    // emitted here through the shared catalog-artifact plan.
    // ════════════════════════════════════════════════════════════════

    /// <summary>
    /// Creates a linked-table entry (MSysObjects type 6) that references a table
    /// in another Access database. No row data is stored locally.
    /// </summary>
    /// <param name="writer">The owning writer.</param>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDatabasePath">Path to the source Access database file (.mdb / .accdb).</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static ValueTask CreateLinkedTableAsync(
        AccessWriter writer,
        string linkedTableName,
        string sourceDatabasePath,
        string foreignTableName,
        CancellationToken cancellationToken)
        => writer.RunAutoCommitAsync(
            _ => CreateLinkedTableCoreAsync(writer, linkedTableName, sourceDatabasePath, foreignTableName, cancellationToken),
            cancellationToken);

    /// <summary>
    /// Creates a linked-ODBC table entry (MSysObjects type 4). When
    /// <paramref name="sourceColumns"/> is supplied a column-level cached-schema
    /// <c>LvProp</c> block is generated; otherwise a table-level block is written.
    /// </summary>
    /// <param name="writer">The owning writer.</param>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="sourceColumns">Optional column definitions for the remote source table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static ValueTask CreateLinkedOdbcTableAsync(
        AccessWriter writer,
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        IReadOnlyList<ColumnDefinition>? sourceColumns,
        CancellationToken cancellationToken)
        => writer.RunAutoCommitAsync(
            _ => CreateLinkedOdbcTableCoreAsync(
                writer,
                linkedTableName,
                connectionString,
                foreignTableName,
                cachedSchemaLvProp: null,
                sourceColumns,
                cancellationToken),
            cancellationToken);

    /// <summary>
    /// Creates a linked-ODBC table entry (MSysObjects type 4) using a
    /// caller-supplied Access/DAO cached-schema payload for <c>MSysObjects.LvProp</c>.
    /// The payload is validated synchronously before any catalog mutation begins.
    /// </summary>
    /// <param name="writer">The owning writer.</param>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cachedSchemaLvProp">Access/DAO-authored cached linked-schema payload for <c>MSysObjects.LvProp</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static ValueTask CreateLinkedOdbcTableAsync(
        AccessWriter writer,
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        ReadOnlyMemory<byte> cachedSchemaLvProp,
        CancellationToken cancellationToken)
    {
        byte[] validatedLvProp = CopyValidatedCachedSchemaLvProp(writer, cachedSchemaLvProp, nameof(cachedSchemaLvProp));
        return writer.RunAutoCommitAsync(
            _ => CreateLinkedOdbcTableCoreAsync(
                writer,
                linkedTableName,
                connectionString,
                foreignTableName,
                cachedSchemaLvProp: validatedLvProp,
                sourceColumns: null,
                cancellationToken),
            cancellationToken);
    }

    /// <summary>
    /// Creates a linked-text/CSV table entry (MSysObjects type 6) that references a
    /// text or CSV file in a directory.
    /// </summary>
    /// <param name="writer">The owning writer.</param>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDirectoryPath">Path to the directory containing the text/CSV source file.</param>
    /// <param name="foreignFileName">The filename of the text/CSV source (e.g. <c>"data.csv"</c>).</param>
    /// <param name="connectString">The text-driver connect string (e.g. <c>"Text;HDR=YES;FMT=Delimited"</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static ValueTask CreateLinkedTextTableAsync(
        AccessWriter writer,
        string linkedTableName,
        string sourceDirectoryPath,
        string foreignFileName,
        string connectString,
        CancellationToken cancellationToken)
        => writer.RunAutoCommitAsync(
            _ => CreateLinkedTextTableCoreAsync(writer, linkedTableName, sourceDirectoryPath, foreignFileName, connectString, cancellationToken),
            cancellationToken);

    private static async ValueTask CreateLinkedTableCoreAsync(
        AccessWriter writer,
        string linkedTableName,
        string sourceDatabasePath,
        string foreignTableName,
        CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(linkedTableName, nameof(linkedTableName));
        Guard.NotNullOrEmpty(sourceDatabasePath, nameof(sourceDatabasePath));
        Guard.NotNullOrEmpty(foreignTableName, nameof(foreignTableName));
        writer.ThrowIfDisposedOrCancelled(cancellationToken);

        await writer.ExecuteCatalogArtifactPlanAsync(
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

    private static async ValueTask CreateLinkedOdbcTableCoreAsync(
        AccessWriter writer,
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        byte[]? cachedSchemaLvProp,
        IReadOnlyList<ColumnDefinition>? sourceColumns,
        CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(linkedTableName, nameof(linkedTableName));
        Guard.NotNullOrEmpty(connectionString, nameof(connectionString));
        Guard.NotNullOrEmpty(foreignTableName, nameof(foreignTableName));
        writer.ThrowIfDisposedOrCancelled(cancellationToken);

        string normalizedConnect = connectionString.StartsWith("ODBC;", StringComparison.OrdinalIgnoreCase)
            ? connectionString
            : "ODBC;" + connectionString;

        if (sourceColumns is not null)
        {
            LinkedOdbcLvPropBuilder.ValidateSourceColumns(sourceColumns, nameof(sourceColumns));
        }

        byte[] lvProp = cachedSchemaLvProp ?? LinkedOdbcLvPropBuilder.Build(foreignTableName, sourceColumns, writer.Format);

        await writer.ExecuteCatalogArtifactPlanAsync(
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

    private static async ValueTask CreateLinkedTextTableCoreAsync(
        AccessWriter writer,
        string linkedTableName,
        string sourceDirectoryPath,
        string foreignFileName,
        string connectString,
        CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(linkedTableName, nameof(linkedTableName));
        Guard.NotNullOrEmpty(sourceDirectoryPath, nameof(sourceDirectoryPath));
        Guard.NotNullOrEmpty(foreignFileName, nameof(foreignFileName));
        Guard.NotNullOrEmpty(connectString, nameof(connectString));
        writer.ThrowIfDisposedOrCancelled(cancellationToken);

        await writer.ExecuteCatalogArtifactPlanAsync(
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

    private static byte[] CopyValidatedCachedSchemaLvProp(AccessWriter writer, ReadOnlyMemory<byte> cachedSchemaLvProp, string paramName)
    {
        if (cachedSchemaLvProp.IsEmpty)
        {
            throw new ArgumentException("Cached schema LvProp cannot be empty.", paramName);
        }

        byte[] copy = cachedSchemaLvProp.ToArray();
        if (copy.AsSpan().SequenceEqual(Constants.SystemObjects.DefaultLvPropPlaceholder))
        {
            throw new ArgumentException("Cached schema LvProp cannot be the default placeholder.", paramName);
        }

        uint expectedMagic = writer.Format == DatabaseFormat.Jet3Mdb ? 0x00444B4BU : 0x0032524DU;
        if (copy.Length < sizeof(uint) || JetTypeInfo.Ru32(copy, 0) != expectedMagic)
        {
            throw new ArgumentException("Cached schema LvProp must use the property-block magic for this database format.", paramName);
        }

        var block = ColumnPropertyBlock.Parse(copy, writer.Format);
        if (block is null || block.Targets.Count == 0)
        {
            throw new ArgumentException("Cached schema LvProp must contain at least one property target.", paramName);
        }

        return copy;
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

    private static string ResolveLinkedTextSourceFilePath(AccessReader reader, LinkedTableInfo link)
    {
        if (link.Kind != LinkedTableKind.Text)
        {
            ThrowIfUnsupportedLinkedRead(link);
        }

        string resolvedDirectory = ResolveLinkedSourcePath(reader, link);

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

    private static string ResolveLinkedSourcePath(AccessReader reader, LinkedTableInfo link)
    {
        AccessReaderOptions linkedOptions = reader.LinkedSourceOpenOptions;
        return ResolveLinkedSourcePath(
            link,
            reader.HostDatabasePath,
            linkedOptions.LinkedSourcePathAllowlist,
            linkedOptions.LinkedSourcePathValidator);
    }

    private static async ValueTask<LinkedTextRowReader> OpenLinkedTextRowsAsync(
        AccessReader reader,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextDataSource source = GetLinkedTextDataSource(reader, link);
        return await LinkedTextRowReader.OpenAsync(source, cancellationToken).ConfigureAwait(false);
    }

    private static LinkedTextDataSource GetLinkedTextDataSource(AccessReader reader, LinkedTableInfo link)
    {
        LinkedTextLimits limits = CreateLinkedTextLimits(reader.LinkedSourceOpenOptions);
        string resolvedPath = ResolveLinkedTextSourceFilePath(reader, link);
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
        try
        {
            string fullBaseDirectory = Path.GetFullPath(baseDirectory);
            return Path.GetFullPath(path, fullBaseDirectory);
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
        CheckExistingDirectoryForReparsePoint(fullTrustedDirectory, context);

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
