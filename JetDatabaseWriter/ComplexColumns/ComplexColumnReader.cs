namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Reads Access 2007+ complex-column (Attachment / Multi-value) metadata:
/// joins a table's complex column descriptors with <c>MSysComplexColumns</c>,
/// resolves column subtypes, and loads the attachment payloads that table
/// scans substitute for each row's complex reference.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="catalog">Resolves tables and locates <c>MSysComplexColumns</c> and the flat child tables.</param>
/// <param name="rows">Decodes system-table and flat-table rows as strings.</param>
/// <param name="diagnosticsEnabled">Whether suppressed best-effort failures are traced.</param>
internal sealed class ComplexColumnReader(DatabaseFile db, CatalogReader catalog, RowDecoder rows, bool diagnosticsEnabled)
{
    internal static void ResolveColumns(object?[] typedRow, List<ColumnInfo> columns, Dictionary<int, Dictionary<int, byte[]>>? complexData)
    {
        int parentId = -1;
        int limit = Math.Min(columns.Count, typedRow.Length);
        for (int i = 0; i < limit; i++)
        {
            ColumnInfo col = columns[i];
            if (col.Type is not ComplexType and not AttachmentType)
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
                    attachBytes?.Length > 0)
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
        if (db.Format == DatabaseFormat.Jet3Mdb)
        {
            return [];
        }

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        byte[]? td = await db.ReadTDefBytesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null)
        {
            return [];
        }

        int numCols = Ru16(td, db.TDef.NumCols);
        int numRealIdx = Ri32(td, db.TDef.NumRealIdx);
        if (numRealIdx is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            numRealIdx = 0;
        }

        int colStart = db.TDef.BlockEnd + (numRealIdx * db.TDef.RealIdxEntrySz);

        var byComplexId = new Dictionary<int, (string Name, ColumnType Type)>();
        for (int i = 0; i < numCols; i++)
        {
            int offset = colStart + (i * db.ColumnDescriptor.Size);
            if (offset + db.ColumnDescriptor.Size > td.Length)
            {
                break;
            }

            var type = (ColumnType)td[offset + db.ColumnDescriptor.TypeOff];
            if (type is not ComplexType and not AttachmentType)
            {
                continue;
            }

            int complexId = Ri32(td, offset + db.ColumnDescriptor.MiscOff);
            if (complexId <= 0)
            {
                continue;
            }

            int colNum = Ru16(td, offset + db.ColumnDescriptor.NumOff);

            ColumnInfo? info = resolved.Definition.Columns.Find(c => c.ColNum == colNum);
            string name = info?.Name ?? string.Empty;
            byComplexId[complexId] = (name, type);
        }

        return byComplexId.Count == 0
            ? []
            : await this.JoinComplexColumnsAsync(byComplexId, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask<Dictionary<string, string>> ReadColumnSubtypesAsync(string tableName, CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            long tdefPage = await catalog.FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
            if (tdefPage <= 0)
            {
                return result;
            }

            TableDef? td = await db.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
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

            ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            long targetTdefPage = resolved?.Entry.TDefPage ?? 0;

            await foreach (string[] row in rows.EnumerateRowsForTdefAsync(tdefPage, td, cancellationToken).ConfigureAwait(false))
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
            this.TraceBestEffortFallback(nameof(ReadColumnSubtypesAsync), ex);
        }
        catch (IndexOutOfRangeException ex)
        {
            this.TraceBestEffortFallback(nameof(ReadColumnSubtypesAsync), ex);
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
            if (col.Type is not ComplexType and not AttachmentType)
            {
                continue;
            }

            Dictionary<int, byte[]>? colData = await this.LoadAttachmentDataAsync(tableName, col.Name, cancellationToken).ConfigureAwait(false);
            if (colData?.Count > 0)
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
            return CatalogValueReader.TdefPageFromId(tableId) == targetTdefPage;
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
            if (columns[i].Type == LongIntegerType && typedRow[i] is int id)
            {
                return id;
            }
        }

        return 0;
    }

    private static byte[] DecodeAttachmentFileData(byte[] raw) => raw.Length <= 1 ? raw : raw[0] switch
    {
        0x01 => DecompressAttachmentData(raw, 1),
        0x00 => BinaryBuffer.CopyTail(raw, 1),
        _ => raw,
    };

    internal static byte[] DecompressAttachmentData(byte[] data, int offset)
    {
        try
        {
            int zlibPos = FindZlibHeader(data, offset);
            if (zlibPos < 0 || zlibPos + 2 >= data.Length)
            {
                return BinaryBuffer.CopyTail(data, offset);
            }

            return InflateZlibPayload(data, zlibPos);
        }
        catch (InvalidDataException)
        {
            return BinaryBuffer.CopyTail(data, offset);
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

    private static byte[] DecodeColumnBytes(string value, ColumnType colType)
    {
        if (string.IsNullOrEmpty(value))
        {
            return [];
        }

        if (value.StartsWith("data:", StringComparison.Ordinal))
        {
            return BinaryStringParser.TryDecodeBase64DataUri(value, out byte[] bytes) ? bytes : [];
        }

        if (colType == BinaryType && value.AsSpan().IndexOf('-') >= 0)
        {
            return BinaryStringParser.TryParseHexString(value.AsSpan(), out byte[] bytes) ? bytes : [];
        }

        return Encoding.UTF8.GetBytes(value);
    }

    private async ValueTask<IReadOnlyList<ComplexColumnInfo>> JoinComplexColumnsAsync(
        Dictionary<int, (string Name, ColumnType Type)> byComplexId,
        CancellationToken cancellationToken)
    {
        long msysTdef = await catalog.FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysTdef <= 0)
        {
            return [];
        }

        TableDef? msys = await db.ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false);
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

        Dictionary<long, string> objectNamesById = await this.BuildObjectNameLookupAsync(cancellationToken).ConfigureAwait(false);

        var result = new List<ComplexColumnInfo>(byComplexId.Count);
        await foreach (string[] row in rows.EnumerateRowsForTdefAsync(msysTdef, msys, cancellationToken).ConfigureAwait(false))
        {
            if (!CatalogValueReader.TryParseInt32(row, idxComplexId, out int complexId))
            {
                continue;
            }

            if (!byComplexId.TryGetValue(complexId, out (string Name, ColumnType Type) parent))
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

        TableDef? msys = await db.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
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

        await foreach (string[] row in rows.EnumerateRowsForTdefAsync(2, msys, cancellationToken).ConfigureAwait(false))
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
            long msysTdef = await catalog.FindSystemTablePageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
            if (msysTdef <= 0)
            {
                return 0;
            }

            TableDef? td = await db.ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false);
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

            ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            long targetTdefPage = resolved?.Entry.TDefPage ?? 0;

            await foreach (string[] row in rows.EnumerateRowsForTdefAsync(msysTdef, td, cancellationToken).ConfigureAwait(false))
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
                    long flatTdef = CatalogValueReader.TdefPageFromId(flatId);
                    if (flatTdef > 0)
                    {
                        return flatTdef;
                    }
                }
            }
        }
        catch (InvalidDataException ex)
        {
            this.TraceBestEffortFallback(nameof(GetComplexFlatTablePageAsync), ex);
        }

        return 0;
    }

    private async ValueTask<Dictionary<int, byte[]>?> LoadAttachmentDataAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            long tdefPage = await this.GetComplexFlatTablePageAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
            if (tdefPage <= 0)
            {
                tdefPage = await this.FindSystemTablePageBySuffixAsync($"_{columnName}", cancellationToken).ConfigureAwait(false);
            }

            TableDef? td = tdefPage > 0 ? await db.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false) : null;
            if (td == null)
            {
                return null;
            }

            string fkColName = $"{tableName}_{columnName}";
            int idxFk = td.FindColumnIndex(fkColName);
            if (idxFk < 0)
            {
                idxFk = td.Columns.FindIndex(c => c.Type == LongIntegerType && !c.Name.StartsWith("Idx", StringComparison.OrdinalIgnoreCase));
            }

            if (idxFk < 0)
            {
                return null;
            }

            int idxFileName = td.FindColumnIndex("FileName");
            int idxFileData = td.FindColumnIndex("FileData");

            var result = new Dictionary<int, byte[]>(capacity: 32);

            await foreach (string[] row in rows.EnumerateRowsForTdefAsync(tdefPage, td, cancellationToken).ConfigureAwait(false))
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

                byte[] serialized = new byte[2 + fileNameBytes.Length + fileDataBytes.Length];
                BinaryPrimitives.WriteUInt16LittleEndian(serialized, (ushort)fileNameBytes.Length);
                Buffer.BlockCopy(fileNameBytes, 0, serialized, 2, fileNameBytes.Length);
                Buffer.BlockCopy(fileDataBytes, 0, serialized, 2 + fileNameBytes.Length, fileDataBytes.Length);

                result[parentId] = serialized;
            }

            return result.Count > 0 ? result : null;
        }
        catch (InvalidDataException ex)
        {
            this.TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
        catch (IndexOutOfRangeException ex)
        {
            this.TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
        catch (IOException ex)
        {
            this.TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
        catch (OverflowException ex)
        {
            this.TraceBestEffortFallback(nameof(LoadAttachmentDataAsync), ex);
            return null;
        }
    }

    private ValueTask<long> FindSystemTablePageBySuffixAsync(string nameSuffix, CancellationToken cancellationToken)
        => catalog.FindSystemTablePageAsync(
            name => name.EndsWith(nameSuffix, StringComparison.OrdinalIgnoreCase),
            cancellationToken);

    private void TraceBestEffortFallback(string operation, Exception exception)
    {
        if (diagnosticsEnabled)
        {
            Trace.WriteLine($"[AccessReader] Best-effort fallback in ComplexColumnReader.{operation}: suppressed {exception.GetType().Name} while reading MSysComplexColumns.");
        }
    }
}
