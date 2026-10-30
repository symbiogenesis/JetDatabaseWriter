namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.DelimitedText;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Centralises the linked-table (MSysObjects type 4 / 6) format and source
/// policy: the MSysObjects scan that produces <see cref="LinkedTableInfo"/>
/// entries, authorized-stream resolution under a <see cref="LinkedSourcePolicy"/>,
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
    /// Enumerates every linked table (Access-file, ODBC, or text) defined in
    /// MSysObjects.
    /// </summary>
    /// <param name="catalog">Reads the <c>MSysObjects</c> rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetCorruptDataException">Thrown when linked-table metadata exceeds the per-reader row limit.</exception>
    /// <exception cref="JetCorruptDataException">Required linked catalog structure is missing or invalid.</exception>
    /// <exception cref="JetLimitationException">Linked-table metadata exceeds its entry limit.</exception>
    internal static async ValueTask<LinkedCatalog> GetLinkedTablesAsync(CatalogReader catalog, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        _ = await catalog.ReadValidatedObjectsAsync(cancellationToken).ConfigureAwait(false);
        TableDef msys = await catalog.GetMSysObjectsTableDefAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The MSysObjects catalog table definition could not be read.");

        int idxName = msys.FindColumnIndex("Name");
        int idxType = msys.FindColumnIndex("Type");
        int idxFlags = msys.FindColumnIndex("Flags");
        int idxDatabase = msys.FindColumnIndex("Database");
        int idxForeignName = msys.FindColumnIndex("ForeignName");
        int idxConnect = msys.FindColumnIndex("Connect");

        if (idxName < 0 || idxType < 0 || idxFlags < 0 || idxDatabase < 0 || idxForeignName < 0 || idxConnect < 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects is missing a required linked-table metadata column.");
        }

        var result = new List<LinkedTableInfo>();
        var visible = new List<LinkedTableInfo>();

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

            bool isSystem = CatalogValueReader.TryParseInt64(row, idxFlags, out long flagsLong)
                && (unchecked((uint)flagsLong) & Constants.SystemObjects.SystemTableMask) != 0;

            string connectStr = CatalogValueReader.GetStringOrEmpty(row, idxConnect);
            string foreignName = CatalogValueReader.GetStringOrEmpty(row, idxForeignName);
            string sourcePath = CatalogValueReader.GetStringOrEmpty(row, idxDatabase);
            LinkedTableKind kind = objType switch
            {
                Constants.SystemObjects.LinkedOdbcType => LinkedTableKind.Odbc,
                Constants.SystemObjects.LinkedTableType when !string.IsNullOrEmpty(connectStr) => LinkedTableKind.Text,
                Constants.SystemObjects.LinkedTableType => LinkedTableKind.Access,
                _ => throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"Unsupported linked-table object type: {objType}."),
            };

            // Native ODBC links may expose only cached LvProp schema, with no source connection metadata.
            bool valid = kind == LinkedTableKind.Odbc
                || (!string.IsNullOrWhiteSpace(foreignName) && !string.IsNullOrWhiteSpace(sourcePath));
            if (!valid)
            {
                if (catalog.StrictParsing)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "A linked table has incomplete source metadata.", new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, ObjectName = nameStr });
                }

                continue;
            }

            if (result.Count >= MaxLinkedTableMetadataRows)
            {
                throw new JetLimitationException(JetErrorCode.LinkedSourceBudgetExceeded, $"Linked-table metadata exceeds the per-reader limit of {MaxLinkedTableMetadataRows} entries.");
            }

            var linkedTable = new LinkedTableInfo
            {
                Name = nameStr,
                Kind = kind,
                SourceObjectName = kind == LinkedTableKind.Text ? DecodeTextForeignName(foreignName) : foreignName,
                SourcePath = kind == LinkedTableKind.Odbc || string.IsNullOrEmpty(sourcePath) ? null : sourcePath,
                ConnectString = string.IsNullOrEmpty(connectStr) ? null : connectStr,
            };
            result.Add(linkedTable);
            if (!isSystem)
            {
                visible.Add(linkedTable);
            }
        }

        return new LinkedCatalog(visible, result);
    }

    /// <summary>
    /// Opens the source database referenced by <paramref name="link"/> as a
    /// separate reader, applying the host's stream resolver and its
    /// explicit linked-source credentials and shared runtime context.
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
        cancellationToken.ThrowIfCancellationRequested();

        string resolvedPath = ResolveLinkedSourcePath(policy, link);
        LinkedSourceTraversal traversal = ExtendLinkedTraversal(policy, link, resolvedPath);
        Stream source = await OpenSourceStreamAsync(policy, link, resolvedPath, cancellationToken).ConfigureAwait(false);
        ReaderOpenContext context;
        try
        {
            ReadOnlyMemory<char> password = policy.Options.LinkedSourcePasswordResolver?.Invoke(link with { }, resolvedPath) ?? default;
            context = policy.Context.ForLinkedSource(resolvedPath, traversal, password);
        }
        catch
        {
            await source.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return await AccessReader.OpenWithContextAsync(source, context, leaveOpen: false, cancellationToken).ConfigureAwait(false);
    }

    internal static async ValueTask<long> CountLinkedTextRowsAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextDataSource source = await GetLinkedTextDataSourceAsync(policy, link, cancellationToken).ConfigureAwait(false);
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
            throw new JetLimitationException(JetErrorCode.LinkedSourceBudgetExceeded, $"Linked text table '{tableName}' exceeds AccessReaderOptions.{nameof(AccessReaderOptions.LinkedTextMaxMaterializedRows)} ({maxMaterializedRows.Value}).", new JetErrorInfo { TableName = tableName });
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
                [
                    CatalogObjectArtifact.LinkedTable(
                        linkedTableName,
                        sourceDatabasePath,
                        foreignTableName,
                        connectString: null,
                        objectType: Constants.SystemObjects.LinkedTableType),
                ]),
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
                [
                    CatalogObjectArtifact.LinkedTable(
                        linkedTableName,
                        sourceDatabasePath: null,
                        foreignName: foreignTableName,
                        connectString: normalizedConnect,
                        objectType: Constants.SystemObjects.LinkedOdbcType,
                        cachedSchemaLvProp: lvProp),
                ]),
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
                [
                    CatalogObjectArtifact.LinkedTable(
                        linkedTableName,
                        sourceDirectoryPath,
                        foreignFileName,
                        connectString,
                        Constants.SystemObjects.LinkedTableType),
                ]),
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
    /// <param name="resolvedPath">The untrusted logical source path hint.</param>
    /// <returns>The new immutable ancestry.</returns>
    /// <exception cref="JetCorruptDataException">The read contains a cycle.</exception>
    /// <exception cref="JetLimitationException">The read exceeds its depth limit.</exception>
    private static LinkedSourceTraversal ExtendLinkedTraversal(LinkedSourcePolicy policy, LinkedTableInfo link, string resolvedPath)
    {
        LinkedSourceTraversal? previous = policy.Context.Traversal;
        int depth = previous?.Depth ?? 0;
        if (depth >= policy.Options.LinkedSourceMaxDepth)
        {
            throw new JetLimitationException(JetErrorCode.LinkedSourceBudgetExceeded, $"Linked table '{link.Name}' exceeds the linked-source depth limit ({policy.Options.LinkedSourceMaxDepth}).", new JetErrorInfo { TableName = link.Name });
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
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"Linked table '{link.Name}' contains a linked-source cycle at '{resolvedPath}' table '{link.SourceObjectName}'.", new JetErrorInfo { TableName = link.Name, ObjectName = link.SourceObjectName });
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

    private static void ValidateLinkedTextSourceFileSize(Stream source, LinkedTextLimits limits, string tableName)
    {
        if (!limits.MaxSourceFileBytes.HasValue)
        {
            return;
        }

        long length = source.Length;
        if (length > limits.MaxSourceFileBytes.Value)
        {
            throw new JetLimitationException(JetErrorCode.LinkedSourceBudgetExceeded, $"Linked text table '{tableName}' source file exceeds AccessReaderOptions.{nameof(AccessReaderOptions.LinkedTextMaxSourceFileBytes)} ({limits.MaxSourceFileBytes.Value}).", new JetErrorInfo { TableName = tableName });
        }
    }

    private static string ResolveLinkedSourcePath(LinkedSourcePolicy policy, LinkedTableInfo link)
    {
        if (string.IsNullOrWhiteSpace(link.SourcePath))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"Source path for linked table '{link.Name}' not found: {link.SourcePath}", new JetErrorInfo { TableName = link.Name });
        }

        string rawPath = link.SourcePath.Trim();
        ThrowIfUnsafeWindowsPath(rawPath, $"linked table '{link.Name}'");
        rawPath = rawPath.Replace('\\', Path.DirectorySeparatorChar);
        string baseDirectory = string.IsNullOrWhiteSpace(policy.HostDatabasePath)
            ? Directory.GetCurrentDirectory()
            : Path.GetDirectoryName(policy.HostDatabasePath) ?? Directory.GetCurrentDirectory();
        return ResolvePath(rawPath, baseDirectory, $"linked table '{link.Name}'");
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
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, $"Text source for linked table '{link.Name}' not found: {link.SourceObjectName}", new JetErrorInfo { TableName = link.Name });
        }

        string rawFilePath = link.SourceObjectName.Trim();
        ThrowIfUnsafeWindowsPath(rawFilePath, $"linked text table '{link.Name}'");
        rawFilePath = rawFilePath.Replace('\\', Path.DirectorySeparatorChar);
        string resolvedFilePath = ResolvePath(rawFilePath, resolvedDirectory, $"linked text table '{link.Name}'");
        if (!IsPathWithinDirectory(resolvedFilePath, resolvedDirectory))
        {
            throw new JetAccessDeniedException(JetErrorCode.LinkedSourceNotPermitted, $"Linked text table '{link.Name}' source file '{link.SourceObjectName}' is outside its source directory.", new JetErrorInfo { TableName = link.Name });
        }

        return resolvedFilePath;
    }

    private static async ValueTask<Stream> OpenSourceStreamAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        string resolvedPath,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Func<LinkedTableInfo, string, CancellationToken, ValueTask<Stream>> resolver = policy.Options.LinkedSourceResolver
            ?? throw new JetAccessDeniedException(JetErrorCode.LinkedSourceNotPermitted, $"Linked table '{link.Name}' requires AccessReaderOptions.LinkedSourceResolver to provide an authorized source stream.", new JetErrorInfo { TableName = link.Name });
        Stream source = await resolver(link with { }, resolvedPath, cancellationToken).ConfigureAwait(false)
            ?? throw new JetAccessDeniedException(JetErrorCode.LinkedSourceNotPermitted, $"Linked table '{link.Name}' source was not authorized by AccessReaderOptions.LinkedSourceResolver.", new JetErrorInfo { TableName = link.Name });

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!source.CanRead || !source.CanSeek)
            {
                throw new ArgumentException("The linked-source resolver must return a readable, seekable stream.", nameof(policy));
            }

            source.Position = 0;
            return source;
        }
        catch
        {
            await source.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static async ValueTask<LinkedTextRowReader> OpenLinkedTextRowsAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextDataSource source = await GetLinkedTextDataSourceAsync(policy, link, cancellationToken).ConfigureAwait(false);
        return await LinkedTextRowReader.OpenAsync(source, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<LinkedTextDataSource> GetLinkedTextDataSourceAsync(
        LinkedSourcePolicy policy,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextLimits limits = CreateLinkedTextLimits(policy.Options);
        string resolvedPath = ResolveLinkedTextSourceFilePath(policy, link);
        DelimitedTextFormat format = ParseTextLinkFormat(link.ConnectString);
        Stream source = await OpenSourceStreamAsync(policy, link, resolvedPath, cancellationToken).ConfigureAwait(false);
        return new LinkedTextDataSource(source, format, limits, link.Name);
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

        throw new JetNotSupportedException(JetErrorCode.OdbcRowsNotAvailable, $"Linked {kindDescription} table '{link.Name}' is metadata-only; JetDatabaseWriter opens Access-file linked tables and reads delimited text links.", new JetErrorInfo { TableName = link.Name });
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
                throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, "Linked text tables with FMT=FixedLength are not supported by the managed CSV reader.");
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
                throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, $"Linked text tables with FMT={format} are not supported by the managed CSV reader.");
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
            throw new JetAccessDeniedException(
                JetErrorCode.LinkedSourceNotPermitted,
                $"Invalid path in {context}: '{path}'.",
                new JetErrorInfo { Reason = context },
                ex);
        }
    }

    /// <summary>Rejects Windows device namespaces and ambiguous drive-relative sources.</summary>
    /// <param name="path">The path supplied by database metadata or caller policy.</param>
    /// <param name="context">The failure context.</param>
    /// <exception cref="JetAccessDeniedException">The path can address a device or has ambiguous Windows semantics.</exception>
    private static void ThrowIfUnsafeWindowsPath(string path, string context)
    {
        string devicePath = path.Replace('/', '\\');
        if (devicePath.StartsWith("\\\\?\\", StringComparison.Ordinal)
            || devicePath.StartsWith("\\\\.\\", StringComparison.Ordinal)
            || devicePath.StartsWith("\\??\\", StringComparison.Ordinal)
            || (path.Length >= 2 && path[1] == ':' && (path.Length == 2 || (path[2] != '\\' && path[2] != '/'))))
        {
            throw new JetAccessDeniedException(JetErrorCode.LinkedSourceNotPermitted, $"Invalid device or drive-relative path in {context}: '{path}'.");
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
                throw new JetAccessDeniedException(JetErrorCode.LinkedSourceNotPermitted, $"Invalid device, alternate stream or aliased path in {context}: '{path}'.");
            }
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

    /// <summary>One fully scanned catalog image shared by listings and execution policy.</summary>
    /// <param name="VisibleLinks">Non-system links exposed by public listings.</param>
    /// <param name="ExecutionLinks">All links, including system identities needed for authorization.</param>
    internal sealed record LinkedCatalog(List<LinkedTableInfo> VisibleLinks, List<LinkedTableInfo> ExecutionLinks);

    private readonly record struct LinkedTextDataSource(Stream Source, DelimitedTextFormat Format, LinkedTextLimits Limits, string TableName);

    private readonly record struct LinkedTextLimits(
        DelimitedTextLimits Delimited,
        long? MaxSourceFileBytes,
        uint? MaxMaterializedRows);

    private sealed class LinkedTextRecordReader : IDisposable
    {
        private readonly Stream sourceStream;
        private readonly StreamReader textReader;

        internal LinkedTextRecordReader(LinkedTextDataSource source)
        {
            this.sourceStream = source.Source;
            StreamReader? reader = null;
            try
            {
                ValidateLinkedTextSourceFileSize(this.sourceStream, source.Limits, source.TableName);
                reader = new StreamReader(
                    new LinkedTextSourceStream(this.sourceStream, source.Limits.MaxSourceFileBytes, source.TableName),
                    Encoding.UTF8,
                    detectEncodingFromByteOrderMarks: true);
                this.DelimitedReader = new DelimitedTextReader(reader, source.Format, source.Limits.Delimited);
                this.textReader = reader;
            }
            catch
            {
                try
                {
                    reader?.Dispose();
                }
                finally
                {
                    this.sourceStream.Dispose();
                }

                throw;
            }
        }

        internal DelimitedTextReader DelimitedReader { get; }

        public void Dispose()
        {
            try
            {
                this.DelimitedReader.Dispose();
            }
            finally
            {
                try
                {
                    this.textReader.Dispose();
                }
                finally
                {
                    this.sourceStream.Dispose();
                }
            }
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
