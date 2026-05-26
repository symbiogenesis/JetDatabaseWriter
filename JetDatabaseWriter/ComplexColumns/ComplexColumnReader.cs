namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

internal sealed class ComplexColumnReader(AccessReader reader)
{
    private readonly AccessReader _reader = reader;

    internal static void ResolveColumns(object?[] typedRow, List<ColumnInfo> columns, Dictionary<int, Dictionary<int, byte[]>>? complexData)
    {
        int parentId = -1;
        int limit = Math.Min(columns.Count, typedRow.Length);
        for (int i = 0; i < limit; i++)
        {
            ColumnInfo col = columns[i];
            if (col.Type != T_COMPLEX && col.Type != T_ATTACHMENT)
            {
                continue;
            }

            if (complexData != null &&
                complexData.TryGetValue(i, out Dictionary<int, byte[]>? colData))
            {
                int complexId = typedRow[i] is ComplexIdRef cir ? cir.Id : 0;
                if (complexId <= 0)
                {
                    if (parentId < 0)
                    {
                        parentId = ExtractParentIdTyped(typedRow, columns);
                    }

                    complexId = parentId;
                }

                if (complexId > 0 && colData.TryGetValue(complexId, out byte[]? attachBytes) &&
                    attachBytes != null && attachBytes.Length > 0)
                {
                    typedRow[i] = attachBytes;
                    continue;
                }
            }

            typedRow[i] = DBNull.Value;
        }
    }

    internal async ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken)
    {
        if (_reader._format == DatabaseFormat.Jet3Mdb)
        {
            return [];
        }

        var resolved = await _reader.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        byte[]? td = await _reader.GetRawTDefBytesAsync(resolved.Value.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null)
        {
            return [];
        }

        int numCols = AccessBase.Ru16(td, _reader._tdef.NumCols);
        int numRealIdx = AccessBase.Ri32(td, _reader._tdef.NumRealIdx);
        if (numRealIdx < 0 || numRealIdx > 1000)
        {
            numRealIdx = 0;
        }

        int colStart = _reader._tdef.BlockEnd + (numRealIdx * _reader._tdef.RealIdxEntrySz);

        var byComplexId = new Dictionary<int, (string Name, byte Type)>();
        for (int i = 0; i < numCols; i++)
        {
            int offset = colStart + (i * _reader._colDesc.Size);
            if (offset + _reader._colDesc.Size > td.Length)
            {
                break;
            }

            byte type = td[offset + _reader._colDesc.TypeOff];
            if (type != T_COMPLEX && type != T_ATTACHMENT)
            {
                continue;
            }

            int complexId = AccessBase.Ri32(td, offset + _reader._colDesc.MiscOff);
            if (complexId <= 0)
            {
                continue;
            }

            int colNum = AccessBase.Ru16(td, offset + _reader._colDesc.NumOff);
            ColumnInfo? info = resolved.Value.Td.Columns.Find(c => c.ColNum == colNum);
            string name = info?.Name ?? string.Empty;
            byComplexId[complexId] = (name, type);
        }

        return byComplexId.Count == 0
            ? []
            : await JoinComplexColumnsAsync(byComplexId, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        ComplexColumnInfo? info = await FindComplexColumnAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
        if (info == null || string.IsNullOrEmpty(info.FlatTableName))
        {
            return [];
        }

        DataTable flat = await _reader.ReadDataTableAsync(info.FlatTableName, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (flat.Rows.Count == 0)
        {
            return [];
        }

        int idxFk = FindFlatLongFkIndex(flat);
        int idxFileUrl = flat.Columns.IndexOf("FileURL");
        int idxFileName = flat.Columns.IndexOf("FileName");
        int idxFileType = flat.Columns.IndexOf("FileType");
        int idxFileTime = flat.Columns.IndexOf("FileTimeStamp");
        int idxFileData = flat.Columns.IndexOf("FileData");

        var result = new List<AttachmentRecord>(flat.Rows.Count);
        foreach (DataRow row in flat.Rows)
        {
            int fk = idxFk >= 0 && row[idxFk] is not DBNull ? Convert.ToInt32(row[idxFk], CultureInfo.InvariantCulture) : 0;
            byte[] rawData = ExtractOleBytesBestEffort(idxFileData >= 0 ? row[idxFileData] : null);
            byte[] decoded = rawData;
            string ext = idxFileType >= 0 && row[idxFileType] is not DBNull ? Convert.ToString(row[idxFileType], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty;
            if (rawData.Length > 0 && AttachmentWrapper.TryDecode(rawData, out string decodedExt, out byte[] payload))
            {
                decoded = payload;
                if (string.IsNullOrEmpty(ext))
                {
                    ext = decodedExt;
                }
            }

            result.Add(new AttachmentRecord
            {
                ConceptualTableId = fk,
                FileName = idxFileName >= 0 && row[idxFileName] is not DBNull ? Convert.ToString(row[idxFileName], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty,
                FileType = ext,
                FileURL = idxFileUrl >= 0 && row[idxFileUrl] is not DBNull ? Convert.ToString(row[idxFileUrl], CultureInfo.InvariantCulture) : null,
                FileTimeStamp = idxFileTime >= 0 && row[idxFileTime] is DateTime dt ? dt : null,
                FileData = decoded,
            });
        }

        return result;
    }

    internal async ValueTask<IReadOnlyList<(int ConceptualTableId, object? Value)>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        ComplexColumnInfo? info = await FindComplexColumnAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
        if (info == null || string.IsNullOrEmpty(info.FlatTableName))
        {
            return [];
        }

        DataTable flat = await _reader.ReadDataTableAsync(info.FlatTableName, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (flat.Rows.Count == 0)
        {
            return [];
        }

        int idxFk = FindFlatLongFkIndex(flat);
        int idxValue = flat.Columns.IndexOf("value");
        if (idxValue < 0)
        {
            for (int i = 0; i < flat.Columns.Count; i++)
            {
                if (i != idxFk)
                {
                    idxValue = i;
                    break;
                }
            }
        }

        var result = new List<(int, object?)>(flat.Rows.Count);
        foreach (DataRow row in flat.Rows)
        {
            int fk = idxFk >= 0 && row[idxFk] is not DBNull ? Convert.ToInt32(row[idxFk], CultureInfo.InvariantCulture) : 0;
            object? value = idxValue >= 0 && row[idxValue] is not DBNull ? row[idxValue] : null;
            result.Add((fk, value));
        }

        return result;
    }

    internal async ValueTask<Dictionary<string, string>> ReadColumnSubtypesAsync(string tableName, CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            long tdefPage = await _reader.FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
            if (tdefPage <= 0)
            {
                return result;
            }

            TableDef? td = await _reader.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            if (td == null)
            {
                return result;
            }

            int idxCol = td.FindColumnIndex("ColumnName");
            int idxConceptualTable = td.Columns.FindIndex(c =>
                string.Equals(c.Name, "ConceptualTableID", StringComparison.OrdinalIgnoreCase)
                || string.Equals(c.Name, "TableName", StringComparison.OrdinalIgnoreCase));

            if (idxCol < 0)
            {
                return result;
            }

            var resolved = await _reader.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            long targetTdefPage = resolved is { } resolvedValue ? resolvedValue.Entry.TDefPage : 0;

            await foreach (string[] row in _reader.EnumerateRowsForTdefAsync(tdefPage, td, cancellationToken).ConfigureAwait(false))
            {
                if (idxConceptualTable >= 0 &&
                    !ConceptualTableMatches(CatalogValueReader.GetStringOrEmpty(row, idxConceptualTable), targetTdefPage, tableName))
                {
                    continue;
                }

                string colName = CatalogValueReader.GetStringOrEmpty(row, idxCol);
                result[colName] = "Attachment";
            }
        }
        catch (InvalidDataException ex)
        {
            TraceBestEffortFallback(nameof(ReadColumnSubtypesAsync), ex);
        }
        catch (IndexOutOfRangeException ex)
        {
            TraceBestEffortFallback(nameof(ReadColumnSubtypesAsync), ex);
        }

        return result;
    }

    internal async ValueTask<Dictionary<int, Dictionary<int, byte[]>>?> BuildColumnDataAsync(
        string tableName,
        List<ColumnInfo> columns,
        CancellationToken cancellationToken)
    {
        Dictionary<int, Dictionary<int, byte[]>>? result = null;

        for (int i = 0; i < columns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = columns[i];
            if (col.Type != T_COMPLEX && col.Type != T_ATTACHMENT)
            {
                continue;
            }

            Dictionary<int, byte[]>? colData = await LoadAttachmentDataAsync(tableName, col.Name, cancellationToken).ConfigureAwait(false);
            if (colData != null && colData.Count > 0)
            {
                result ??= [];
                result[i] = colData;
            }
        }

        return result;
    }

    private static bool ConceptualTableMatches(string tableIdStr, long targetTdefPage, string? tableName)
    {
        if (targetTdefPage <= 0)
        {
            return true;
        }

        if (CatalogValueReader.TryParseInt64(tableIdStr, out long tableId))
        {
            return (tableId & 0x00FFFFFFL) == targetTdefPage;
        }

        return tableName != null && string.Equals(tableIdStr, tableName, StringComparison.OrdinalIgnoreCase);
    }

    private static ComplexColumnKind ClassifyComplexKind(string complexTypeName)
    {
        if (string.IsNullOrEmpty(complexTypeName))
        {
            return ComplexColumnKind.Unknown;
        }

        if (complexTypeName.Equals(Constants.ComplexTypeNames.Attachment, StringComparison.OrdinalIgnoreCase))
        {
            return ComplexColumnKind.Attachment;
        }

        if (complexTypeName.StartsWith(Constants.ComplexTypeNames.VersionHistoryPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return ComplexColumnKind.VersionHistory;
        }

        if (complexTypeName.StartsWith(Constants.ComplexTypeNames.Prefix, StringComparison.OrdinalIgnoreCase))
        {
            return ComplexColumnKind.MultiValue;
        }

        return ComplexColumnKind.Unknown;
    }

    private static int ExtractParentIdTyped(object?[] typedRow, List<ColumnInfo> columns)
    {
        int limit = Math.Min(columns.Count, typedRow.Length);
        for (int i = 0; i < limit; i++)
        {
            if (columns[i].Type == T_LONG && typedRow[i] is int id)
            {
                return id;
            }
        }

        return 0;
    }

    private static byte[] ExtractOleBytesBestEffort(object? cell)
    {
        if (cell is null or DBNull)
        {
            return [];
        }

        if (cell is byte[] b)
        {
            return b;
        }

        if (cell is string s && s.StartsWith("data:", StringComparison.Ordinal))
        {
            int comma = s.IndexOf(',', StringComparison.Ordinal);
            if (comma >= 0 && comma + 1 < s.Length)
            {
                return BinaryStringParser.TryDecodeBase64(s.AsSpan(comma + 1), out byte[] bytes) ? bytes : [];
            }
        }

        return [];
    }

    private static int FindFlatLongFkIndex(DataTable flat)
    {
        for (int i = 0; i < flat.Columns.Count; i++)
        {
            DataColumn c = flat.Columns[i];
            if (c.DataType == typeof(int) && c.ColumnName.StartsWith('_'))
            {
                return i;
            }
        }

        for (int i = 0; i < flat.Columns.Count; i++)
        {
            if (flat.Columns[i].DataType == typeof(int))
            {
                return i;
            }
        }

        return -1;
    }

    private static byte[] DecodeAttachmentFileData(byte[] raw) => raw.Length <= 1 ? raw : raw[0] switch
    {
        0x01 => DecompressAttachmentData(raw, 1),
        0x00 => raw.AsSpan(1).ToArray(),
        _ => raw,
    };

    internal static byte[] DecompressAttachmentData(byte[] data, int offset)
    {
        try
        {
            int zlibPos = FindZlibHeader(data, offset);
            if (zlibPos < 0 || zlibPos + 2 >= data.Length)
            {
                return data.AsSpan(offset).ToArray();
            }

            return InflateZlibPayload(data, zlibPos);
        }
        catch (InvalidDataException)
        {
            return data.AsSpan(offset).ToArray();
        }
    }

    private static int FindZlibHeader(byte[] data, int offset)
    {
        for (int i = Math.Max(0, offset); i + 1 < data.Length; i++)
        {
            if (data[i] == 0x78 && IsZlibHeaderSuffix(data[i + 1]))
            {
                return i;
            }
        }

        return -1;
    }

    private static bool IsZlibHeaderSuffix(byte value)
        => value is 0x01 or 0x5E or 0x9C or 0xDA;

    private static byte[] InflateZlibPayload(byte[] data, int zlibPos)
    {
        using var output = new MemoryStream();

#if NET8_0_OR_GREATER
        using var input = new MemoryStream(data, zlibPos, data.Length - zlibPos);
        using var zlib = new System.IO.Compression.ZLibStream(input, System.IO.Compression.CompressionMode.Decompress);
        zlib.CopyTo(output);
#else
        int deflateStart = zlibPos + 2;
        using var input = new MemoryStream(data, deflateStart, data.Length - deflateStart);
        using var deflate = new System.IO.Compression.DeflateStream(input, System.IO.Compression.CompressionMode.Decompress);
        deflate.CopyTo(output);
#endif

        return output.ToArray();
    }

    private static byte[] DecodeColumnBytes(string value, byte colType)
    {
        if (string.IsNullOrEmpty(value))
        {
            return [];
        }

        if (value.StartsWith("data:", StringComparison.Ordinal))
        {
            int commaIdx = value.IndexOf(',', StringComparison.Ordinal);
            if (commaIdx >= 0)
            {
                return BinaryStringParser.TryDecodeBase64(value.AsSpan(commaIdx + 1), out byte[] bytes) ? bytes : [];
            }

            return [];
        }

        if (colType == T_BINARY && value.AsSpan().IndexOf('-') >= 0)
        {
            return BinaryStringParser.TryParseHexString(value.AsSpan(), out byte[] bytes) ? bytes : [];
        }

        return Encoding.UTF8.GetBytes(value);
    }

    private async ValueTask<ComplexColumnInfo?> FindComplexColumnAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        IReadOnlyList<ComplexColumnInfo> complex = await GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
        foreach (ComplexColumnInfo column in complex)
        {
            if (string.Equals(column.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return null;
    }

    private async ValueTask<IReadOnlyList<ComplexColumnInfo>> JoinComplexColumnsAsync(
        Dictionary<int, (string Name, byte Type)> byComplexId,
        CancellationToken cancellationToken)
    {
        long msysTdef = await _reader.FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysTdef <= 0)
        {
            return [];
        }

        TableDef? msys = await _reader.ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return [];
        }

        int idxColumnName = msys.FindColumnIndex("ColumnName");
        int idxComplexId = msys.FindColumnIndex("ComplexID");
        int idxFlatTable = msys.FindColumnIndex("FlatTableID");
        int idxConceptualTable = msys.FindColumnIndex("ConceptualTableID");
        int idxComplexType = msys.FindColumnIndex("ComplexTypeObjectID");

        if (idxComplexId < 0)
        {
            return [];
        }

        Dictionary<long, string> objectNamesById = await BuildObjectNameLookupAsync(cancellationToken).ConfigureAwait(false);

        var result = new List<ComplexColumnInfo>(byComplexId.Count);
        await foreach (string[] row in _reader.EnumerateRowsForTdefAsync(msysTdef, msys, cancellationToken).ConfigureAwait(false))
        {
            if (!CatalogValueReader.TryParseInt32(row, idxComplexId, out int complexId))
            {
                continue;
            }

            if (!byComplexId.TryGetValue(complexId, out var parent))
            {
                continue;
            }

            int flatId = CatalogValueReader.ParseInt32OrZero(row, idxFlatTable);
            int conceptualId = CatalogValueReader.ParseInt32OrZero(row, idxConceptualTable);
            int typeObjectId = CatalogValueReader.ParseInt32OrZero(row, idxComplexType);

            string columnName = CatalogValueReader.GetStringOrDefault(row, idxColumnName, parent.Name);
            string flatName = flatId != 0 && objectNamesById.TryGetValue(flatId, out string? fn) ? fn : string.Empty;
            string typeName = typeObjectId != 0 && objectNamesById.TryGetValue(typeObjectId, out string? tn) ? tn : string.Empty;

            result.Add(new ComplexColumnInfo
            {
                ColumnName = string.IsNullOrEmpty(columnName) ? parent.Name : columnName,
                ComplexId = complexId,
                Kind = ClassifyComplexKind(typeName),
                FlatTableName = flatName,
                FlatTableId = flatId,
                ConceptualTableId = conceptualId,
                ComplexTypeObjectId = typeObjectId,
                ComplexTypeName = typeName,
            });
        }

        return result;
    }

    private async ValueTask<Dictionary<long, string>> BuildObjectNameLookupAsync(CancellationToken cancellationToken)
    {
        var map = new Dictionary<long, string>();

        TableDef? msys = await _reader.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return map;
        }

        int idxId = msys.FindColumnIndex("Id");
        int idxName = msys.FindColumnIndex("Name");
        if (idxId < 0 || idxName < 0)
        {
            return map;
        }

        await foreach (string[] row in _reader.EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
        {
            if (CatalogValueReader.TryParseInt64(row, idxId, out long id))
            {
                map[id] = CatalogValueReader.GetStringOrEmpty(row, idxName);
            }
        }

        return map;
    }

    private async ValueTask<long> GetComplexFlatTablePageAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            long msysTdef = await _reader.FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
            if (msysTdef <= 0)
            {
                return 0;
            }

            TableDef? td = await _reader.ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false);
            if (td == null)
            {
                return 0;
            }

            int idxCol = td.FindColumnIndex("ColumnName");
            int idxConceptualTable = td.FindColumnIndex("ConceptualTableID");
            int idxFlatTable = td.FindColumnIndex("FlatTableID");

            if (idxCol < 0 || idxFlatTable < 0)
            {
                return 0;
            }

            var resolved = await _reader.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            long targetTdefPage = resolved?.Entry.TDefPage ?? 0;

            await foreach (string[] row in _reader.EnumerateRowsForTdefAsync(msysTdef, td, cancellationToken).ConfigureAwait(false))
            {
                string colName = CatalogValueReader.GetStringOrEmpty(row, idxCol);
                if (!string.Equals(colName, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (idxConceptualTable >= 0 &&
                    !ConceptualTableMatches(CatalogValueReader.GetStringOrEmpty(row, idxConceptualTable), targetTdefPage, tableName: null))
                {
                    continue;
                }

                if (CatalogValueReader.TryParseInt64(row, idxFlatTable, out long flatId))
                {
                    long flatTdef = flatId & 0x00FFFFFFL;
                    if (flatTdef > 0)
                    {
                        return flatTdef;
                    }
                }
            }
        }
        catch (InvalidDataException ex)
        {
            TraceBestEffortFallback(nameof(GetComplexFlatTablePageAsync), ex);
        }

        return 0;
    }

    private async ValueTask<Dictionary<int, byte[]>?> LoadAttachmentDataAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            long tdefPage = await GetComplexFlatTablePageAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
            if (tdefPage <= 0)
            {
                tdefPage = await FindSystemTablePageBySuffixAsync($"_{columnName}", cancellationToken).ConfigureAwait(false);
            }

            TableDef? td = tdefPage > 0 ? await _reader.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false) : null;
            if (td == null)
            {
                return null;
            }

            string fkColName = $"{tableName}_{columnName}";
            int idxFk = td.FindColumnIndex(fkColName);
            if (idxFk < 0)
            {
                idxFk = td.Columns.FindIndex(c => c.Type == T_LONG && !c.Name.StartsWith("Idx", StringComparison.OrdinalIgnoreCase));
            }

            if (idxFk < 0)
            {
                return null;
            }

            int idxFileName = td.FindColumnIndex("FileName");
            int idxFileData = td.FindColumnIndex("FileData");

            var result = new Dictionary<int, byte[]>(capacity: 32);

            await foreach (string[] row in _reader.EnumerateRowsForTdefAsync(tdefPage, td, cancellationToken).ConfigureAwait(false))
            {
                if (!CatalogValueReader.TryParseInt32(row, idxFk, out int parentId))
                {
                    continue;
                }

                byte[] fileNameBytes = idxFileName >= 0 && CatalogValueReader.GetStringOrEmpty(row, idxFileName) is { Length: > 0 } fileName
                    ? Encoding.Unicode.GetBytes(fileName)
                    : [];

                byte[] fileDataBytes = idxFileData >= 0
                    ? DecodeAttachmentFileData(DecodeColumnBytes(CatalogValueReader.GetStringOrEmpty(row, idxFileData), td.Columns[idxFileData].Type))
                    : [];

                if (fileNameBytes.Length == 0 && fileDataBytes.Length == 0)
                {
                    continue;
                }

                var serialized = new byte[2 + fileNameBytes.Length + fileDataBytes.Length];
                BinaryPrimitives.WriteUInt16LittleEndian(serialized, (ushort)fileNameBytes.Length);
                Buffer.BlockCopy(fileNameBytes, 0, serialized, 2, fileNameBytes.Length);
                Buffer.BlockCopy(fileDataBytes, 0, serialized, 2 + fileNameBytes.Length, fileDataBytes.Length);

                result[parentId] = serialized;
            }

            return result.Count > 0 ? result : null;
        }
        catch (InvalidDataException ex)
        {
            TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
        catch (IndexOutOfRangeException ex)
        {
            TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
        catch (IOException ex)
        {
            TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
        catch (OverflowException ex)
        {
            TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
    }

    private ValueTask<long> FindSystemTablePageBySuffixAsync(string nameSuffix, CancellationToken cancellationToken)
        => _reader.FindSystemTablePageAsync(
            name => name.EndsWith(nameSuffix, StringComparison.OrdinalIgnoreCase),
            cancellationToken);

    private void TraceBestEffortFallback(string operation, Exception exception)
    {
        if (_reader.DiagnosticsEnabled)
        {
            Trace.WriteLine($"[AccessReader] Best-effort fallback in ComplexColumnReader.{operation}: suppressed {exception.GetType().Name} while reading MSysComplexColumns.");
        }
    }
}
