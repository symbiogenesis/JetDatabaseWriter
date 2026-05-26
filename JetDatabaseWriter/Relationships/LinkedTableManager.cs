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
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

/// <summary>
/// Centralises all logic for discovering, resolving, and opening linked tables
/// (MSysObjects type 4 / 6) referenced by an <see cref="AccessReader"/>. Pure
/// path-handling helpers and the MSysObjects scan that produces
/// <see cref="LinkedTableInfo"/> entries live here so <see cref="AccessReader"/>
/// keeps only the wiring needed to delegate to this manager.
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
    internal static AccessReaderOptions CreateLinkedSourceOpenOptions(
        AccessReaderOptions options,
        string hostDatabasePath)
    {
        return new AccessReaderOptions
        {
            PageCacheSize = options.PageCacheSize,
            DiagnosticsEnabled = options.DiagnosticsEnabled,
            ParallelPageReadsEnabled = options.ParallelPageReadsEnabled,
            ValidateOnOpen = options.ValidateOnOpen,
            StrictParsing = options.StrictParsing,
            FileAccess = options.FileAccess,
            FileShare = options.FileShare,
            Password = options.Password,
            UseLockFile = options.UseLockFile,
            LockFileUserName = options.LockFileUserName,
            LockFileMachineName = options.LockFileMachineName,
            LinkedSourcePathAllowlist = NormalizeAllowlist(options.LinkedSourcePathAllowlist, hostDatabasePath),
            LinkedSourcePathValidator = options.LinkedSourcePathValidator,
            LinkedTextMaxRecordLength = options.LinkedTextMaxRecordLength,
            LinkedTextMaxFieldLength = options.LinkedTextMaxFieldLength,
            LinkedTextMaxColumnCount = options.LinkedTextMaxColumnCount,
            LinkedTextMaxSourceFileBytes = options.LinkedTextMaxSourceFileBytes,
            LinkedTextMaxMaterializedRows = options.LinkedTextMaxMaterializedRows,
        };
    }

    /// <summary>
    /// Enumerates every linked table (Access-file, ODBC, or text) defined in
    /// MSysObjects on the given <paramref name="reader"/>.
    /// </summary>
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
            string typeStr = SafeGet(row, idxType);
            if (!int.TryParse(typeStr, out int objType))
            {
                continue;
            }

            if (objType != Constants.SystemObjects.LinkedTableType && objType != Constants.SystemObjects.LinkedOdbcType)
            {
                continue;
            }

            string nameStr = SafeGet(row, idxName);
            if (string.IsNullOrEmpty(nameStr))
            {
                continue;
            }

            string flagsStr = SafeGet(row, idxFlags);
            if (long.TryParse(flagsStr, out long flagsLong) &&
                (unchecked((uint)flagsLong) & Constants.SystemObjects.SystemTableMask) != 0)
            {
                continue;
            }

            bool isOdbc = objType == Constants.SystemObjects.LinkedOdbcType;
            string connectStr = SafeGet(row, idxConnect);
            string foreignName = SafeGet(row, idxForeignName);
            bool isText = !isOdbc && !string.IsNullOrEmpty(connectStr);
            string sourcePath = SafeGet(row, idxDatabase);
            LinkedTableKind kind = isOdbc
                ? LinkedTableKind.Odbc
                : isText ? LinkedTableKind.Text : LinkedTableKind.Access;

            if (result.Count >= MaxLinkedTableMetadataRows)
            {
                throw new InvalidDataException(
                    $"Linked-table metadata exceeds the per-reader limit of {MaxLinkedTableMetadataRows} entries.");
            }

            result.Add(new LinkedTableInfo
            {
                Name = nameStr,
                Kind = kind,
                SourceObjectName = isText ? DecodeTextForeignName(foreignName) : foreignName,
                SourcePath = isOdbc || string.IsNullOrEmpty(sourcePath) ? null : sourcePath,
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
        long count = 0;
        await foreach (string[] row in EnumerateTextDataRowsAsync(source.FilePath, source.Format, source.Limits, cancellationToken).ConfigureAwait(false))
        {
            _ = row;
            count++;
        }

        return count;
    }

    internal static async IAsyncEnumerable<string[]> RowsLinkedTextAsStringsAsync(
        AccessReader reader,
        LinkedTableInfo link,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        LinkedTextSource source = await GetLinkedTextSourceAsync(reader, link, cancellationToken).ConfigureAwait(false);
        long rowCount = 0;

        await foreach (string[] row in EnumerateTextDataRowsAsync(source.FilePath, source.Format, source.Limits, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            rowCount++;
            progress?.Report(rowCount);
            yield return NormalizeStringRow(row, source.ColumnNames.Length);
        }
    }

    internal static async ValueTask<List<ColumnMetadata>> GetLinkedTextColumnMetadataAsync(
        AccessReader reader,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextSource source = await GetLinkedTextSourceAsync(reader, link, cancellationToken).ConfigureAwait(false);
        var metadata = new List<ColumnMetadata>(source.ColumnNames.Length);
        for (int i = 0; i < source.ColumnNames.Length; i++)
        {
            metadata.Add(new ColumnMetadata
            {
                Name = source.ColumnNames[i],
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

    internal static async ValueTask<DataTable> ReadLinkedTextDataTableAsync(
        AccessReader reader,
        LinkedTableInfo link,
        uint? maxRows,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
    {
        LinkedTextSource source = await GetLinkedTextSourceAsync(reader, link, cancellationToken).ConfigureAwait(false);
        DataTable? table = null;
        try
        {
            table = new DataTable(link.Name);
            foreach (string columnName in source.ColumnNames)
            {
                _ = table.Columns.Add(columnName, typeof(string));
            }

            long rowCount = 0;
            await foreach (string[] row in EnumerateTextDataRowsAsync(source.FilePath, source.Format, source.Limits, cancellationToken).ConfigureAwait(false))
            {
                ThrowIfLinkedTextMaterializedRowLimitExceeded(link.Name, rowCount, source.Limits.MaxMaterializedRows);
                _ = table.Rows.Add(NormalizeStringRow(row, source.ColumnNames.Length));
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

    internal static async ValueTask<uint?> GetLinkedTextMaterializedRowLimitAsync(
        AccessReader reader,
        string tableName,
        CancellationToken cancellationToken)
    {
        LinkedTableInfo? link = await FindLinkedTableAsync(reader, tableName, cancellationToken).ConfigureAwait(false);
        if (link?.Kind != LinkedTableKind.Text)
        {
            return null;
        }

        return CreateLinkedTextLimits(reader.LinkedSourceOpenOptions).MaxMaterializedRows;
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

        if (options.LinkedTextMaxMaterializedRows.HasValue && options.LinkedTextMaxMaterializedRows.Value == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.LinkedTextMaxMaterializedRows.Value,
                $"{nameof(AccessReaderOptions.LinkedTextMaxMaterializedRows)} must be positive when set.");
        }

        return new LinkedTextLimits(
            options.LinkedTextMaxRecordLength,
            options.LinkedTextMaxFieldLength,
            options.LinkedTextMaxColumnCount,
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

    private static async ValueTask<LinkedTextSource> GetLinkedTextSourceAsync(
        AccessReader reader,
        LinkedTableInfo link,
        CancellationToken cancellationToken)
    {
        LinkedTextDataSource source = GetLinkedTextDataSource(reader, link);
        string[] columnNames = await ReadLinkedTextColumnNamesAsync(source.FilePath, source.Format, source.Limits, cancellationToken).ConfigureAwait(false);
        return new LinkedTextSource(source.FilePath, source.Format, source.Limits, columnNames);
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

    private static void ThrowIfUnsupportedLinkedRead(LinkedTableInfo link)
    {
        if (link.Kind == LinkedTableKind.Access)
        {
            return;
        }

        string kindDescription = link.Kind switch
        {
            LinkedTableKind.Odbc => "ODBC",
            LinkedTableKind.Text => "text",
            _ => "non-Access",
        };

        throw new NotSupportedException(
            $"Linked {kindDescription} table '{link.Name}' is metadata-only; JetDatabaseWriter opens Access-file linked tables and reads delimited text links.");
    }

    private static TextLinkFormat ParseTextLinkFormat(string? connectString)
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

                string key = part.Substring(0, separator).Trim();
                string value = part.Substring(separator + 1).Trim();
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

        return new TextLinkFormat(hasHeaderRow, delimiter);
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
                yield return connectString.Substring(start, i - start);
                start = i + 1;
            }
        }

        yield return connectString.Substring(start);
    }

    private static async ValueTask<string[]> ReadLinkedTextColumnNamesAsync(
        string filePath,
        TextLinkFormat format,
        LinkedTextLimits limits,
        CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(filePath, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        string[]? firstRecord = await ReadDelimitedRecordAsync(reader, format.Delimiter, limits, cancellationToken).ConfigureAwait(false);
        if (firstRecord is null)
        {
            return [];
        }

        return format.HasHeaderRow
            ? NormalizeColumnNames(firstRecord)
            : CreateGeneratedColumnNames(firstRecord.Length);
    }

    private static async IAsyncEnumerable<string[]> EnumerateTextDataRowsAsync(
        string filePath,
        TextLinkFormat format,
        LinkedTextLimits limits,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(filePath, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        bool isFirstRecord = true;
        while (true)
        {
            string[]? record = await ReadDelimitedRecordAsync(reader, format.Delimiter, limits, cancellationToken).ConfigureAwait(false);
            if (record is null)
            {
                yield break;
            }

            if (isFirstRecord)
            {
                isFirstRecord = false;
                if (format.HasHeaderRow)
                {
                    continue;
                }
            }

            yield return record;
        }
    }

    private static async ValueTask<string[]?> ReadDelimitedRecordAsync(
        StreamReader reader,
        char delimiter,
        LinkedTextLimits limits,
        CancellationToken cancellationToken)
    {
        var fields = new List<string>();
        var field = new StringBuilder();
        var singleChar = new char[1];
        int recordLength = 0;
        int fieldLength = 0;
        bool inQuotes = false;
        bool atFieldStart = true;
        bool sawAnyCharacter = false;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int value = await ReadCharAsync(reader, singleChar, cancellationToken).ConfigureAwait(false);
            if (value < 0)
            {
                if (!sawAnyCharacter && fields.Count == 0 && field.Length == 0)
                {
                    return null;
                }

                if (inQuotes)
                {
                    throw new InvalidDataException("Linked text source contains a quoted field without a closing quote.");
                }

                AddDelimitedField(fields, field, limits);
                return fields.ToArray();
            }

            recordLength = IncrementLinkedTextLength(
                recordLength,
                limits.MaxRecordLength,
                nameof(AccessReaderOptions.LinkedTextMaxRecordLength));
            char ch = (char)value;
            sawAnyCharacter = true;

            if (inQuotes)
            {
                if (ch == '"')
                {
                    if (reader.Peek() == '"')
                    {
                        _ = await ReadCharAsync(reader, singleChar, cancellationToken).ConfigureAwait(false);
                        recordLength = IncrementLinkedTextLength(
                            recordLength,
                            limits.MaxRecordLength,
                            nameof(AccessReaderOptions.LinkedTextMaxRecordLength));
                        fieldLength = AppendDelimitedFieldCharacter(field, '"', fieldLength, limits);
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    fieldLength = AppendDelimitedFieldCharacter(field, ch, fieldLength, limits);
                }

                continue;
            }

            if (atFieldStart && ch == '"')
            {
                inQuotes = true;
                atFieldStart = false;
                continue;
            }

            if (ch == delimiter)
            {
                AddDelimitedField(fields, field, limits);
                _ = field.Clear();
                fieldLength = 0;
                atFieldStart = true;
                continue;
            }

            if (ch == '\r')
            {
                if (reader.Peek() == '\n')
                {
                    _ = await ReadCharAsync(reader, singleChar, cancellationToken).ConfigureAwait(false);
                    recordLength = IncrementLinkedTextLength(
                        recordLength,
                        limits.MaxRecordLength,
                        nameof(AccessReaderOptions.LinkedTextMaxRecordLength));
                }

                AddDelimitedField(fields, field, limits);
                return fields.ToArray();
            }

            if (ch == '\n')
            {
                AddDelimitedField(fields, field, limits);
                return fields.ToArray();
            }

            fieldLength = AppendDelimitedFieldCharacter(field, ch, fieldLength, limits);
            atFieldStart = false;
        }
    }

    private static async ValueTask<int> ReadCharAsync(StreamReader reader, char[] buffer, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        int read = await reader.ReadAsync(buffer, 0, 1).ConfigureAwait(false);
        return read == 0 ? -1 : buffer[0];
    }

    private static int AppendDelimitedFieldCharacter(
        StringBuilder field,
        char ch,
        int fieldLength,
        LinkedTextLimits limits)
    {
        int newLength = IncrementLinkedTextLength(
            fieldLength,
            limits.MaxFieldLength,
            nameof(AccessReaderOptions.LinkedTextMaxFieldLength));
        field.Append(ch);
        return newLength;
    }

    private static int IncrementLinkedTextLength(int currentLength, int maxLength, string optionName)
    {
        if (currentLength >= maxLength)
        {
            throw new InvalidDataException(
                $"Linked text source exceeds AccessReaderOptions.{optionName} ({maxLength}).");
        }

        return currentLength + 1;
    }

    private static void AddDelimitedField(List<string> fields, StringBuilder field, LinkedTextLimits limits)
    {
        if (fields.Count >= limits.MaxColumnCount)
        {
            throw new InvalidDataException(
                $"Linked text source exceeds AccessReaderOptions.{nameof(AccessReaderOptions.LinkedTextMaxColumnCount)} ({limits.MaxColumnCount}).");
        }

        fields.Add(field.ToString());
    }

    private static string[] NormalizeColumnNames(string[] rawColumnNames)
    {
        var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var nextSuffixByBaseName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var columnNames = new string[rawColumnNames.Length];
        for (int i = 0; i < rawColumnNames.Length; i++)
        {
            string baseName = string.IsNullOrWhiteSpace(rawColumnNames[i]) ? $"F{i + 1}" : rawColumnNames[i].Trim();

            if (usedNames.Add(baseName))
            {
                columnNames[i] = baseName;
                continue;
            }

            int suffix = nextSuffixByBaseName.TryGetValue(baseName, out int nextSuffix) ? nextSuffix : 2;
            string candidate;
            do
            {
                candidate = baseName + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);
                suffix++;
            }
            while (!usedNames.Add(candidate));

            nextSuffixByBaseName[baseName] = suffix;
            columnNames[i] = candidate;
        }

        return columnNames;
    }

    private static string[] CreateGeneratedColumnNames(int columnCount)
    {
        var columnNames = new string[columnCount];
        for (int i = 0; i < columnNames.Length; i++)
        {
            columnNames[i] = $"F{i + 1}";
        }

        return columnNames;
    }

    private static string[] NormalizeStringRow(string[] row, int columnCount)
    {
        if (row.Length == columnCount)
        {
            return row;
        }

        var normalized = new string[columnCount];
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
            ex is ArgumentException ||
            ex is NotSupportedException ||
            ex is PathTooLongException)
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
            ex is IOException ||
            ex is ArgumentException ||
            ex is NotSupportedException ||
            ex is PathTooLongException)
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

    private static string SafeGet(string[] row, int idx) =>
        (idx >= 0 && idx < row.Length) ? row[idx] : string.Empty;

    private static string DecodeTextForeignName(string foreignName) =>
        foreignName.Replace('#', '.');

    private readonly record struct LinkedTextDataSource(string FilePath, TextLinkFormat Format, LinkedTextLimits Limits);

    private readonly record struct LinkedTextSource(string FilePath, TextLinkFormat Format, LinkedTextLimits Limits, string[] ColumnNames);

    private readonly record struct LinkedTextLimits(
        int MaxRecordLength,
        int MaxFieldLength,
        int MaxColumnCount,
        long? MaxSourceFileBytes,
        uint? MaxMaterializedRows);

    private readonly record struct TextLinkFormat(bool HasHeaderRow, char Delimiter);
}
