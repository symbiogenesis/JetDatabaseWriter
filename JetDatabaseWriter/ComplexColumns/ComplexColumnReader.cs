namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Reads Access 2007+ complex-column (Attachment / Multi-value) data:
/// joins a table's complex column descriptors with <c>MSysComplexColumns</c>,
/// resolves column subtypes, reads the attachments and values stored in the
/// hidden flat child tables, and builds the <see cref="ComplexCellValue"/>
/// cells that table scans substitute for each row's complex reference.
/// </summary>
/// <param name="format">The file's format profile: the TDEF and column-descriptor layouts.</param>
/// <param name="tableDefs">Reads table definitions and TDEF bytes.</param>
/// <param name="catalog">Resolves tables and locates <c>MSysComplexColumns</c> and the flat child tables.</param>
/// <param name="rows">Decodes system-table rows as strings and flat-table rows as typed values.</param>
/// <param name="diagnosticsEnabled">Whether suppressed best-effort failures are traced.</param>
/// <param name="maxAttachmentContentBytes">The maximum uncompressed attachment content size.</param>
/// <param name="maxComplexDiscoveryEntries">The maximum aggregate metadata entries inspected during discovery.</param>
internal sealed class ComplexColumnReader(JetFormat format, TableDefReader tableDefs, CatalogReader catalog, RowDecoder rows, bool diagnosticsEnabled, int maxAttachmentContentBytes = 64 * 1024 * 1024, int maxComplexDiscoveryEntries = 65536)
{
    /// <summary>The <c>MSysComplexColumns</c> columns the descriptor join reads.</summary>
    private static readonly string[] ComplexColumnJoinColumns = ["ColumnName", "ComplexID", "FlatTableID", "ConceptualTableID", "ComplexTypeObjectID"];

    /// <summary>The <c>MSysComplexColumns</c> columns the flat-table lookup by column name reads.</summary>
    private static readonly string[] FlatTableLookupColumns = ["ColumnName", "ConceptualTableID", "FlatTableID", "ComplexTypeObjectID"];

    /// <summary>
    /// Replaces each complex column's <see cref="ComplexIdRef"/> in a typed row
    /// with the <see cref="ComplexCellValue"/> cell holding every item stored
    /// for that reference, or <see cref="DBNull"/> when the row has none.
    /// </summary>
    /// <param name="typedRow">The decoded row.</param>
    /// <param name="columns">The table's columns.</param>
    /// <param name="complexData">Cells by column index and complex reference, from <see cref="BuildColumnDataAsync"/>.</param>
    internal static void ResolveColumns(object?[] typedRow, IReadOnlyList<ColumnInfo> columns, Dictionary<int, Dictionary<int, byte[]>>? complexData)
    {
        int limit = Math.Min(columns.Count, typedRow.Length);
        for (int i = 0; i < limit; i++)
        {
            if (columns[i].Type is ComplexType)
            {
                typedRow[i] = typedRow[i] is ComplexIdRef reference && TryGetCell(complexData, i, reference.Id, out byte[] cell)
                    ? cell
                    : DBNull.Value;
            }
        }
    }

    /// <summary>
    /// String-row counterpart of <see cref="ResolveColumns"/>: replaces each
    /// complex column's reference with its cell as a base64 <c>data:</c> URI,
    /// or an empty string when the row has no items.
    /// </summary>
    /// <param name="row">The decoded string row.</param>
    /// <param name="columns">The table's columns.</param>
    /// <param name="complexData">Cells by column index and complex reference, from <see cref="BuildColumnDataAsync"/>.</param>
    internal static void ResolveStringColumns(string[] row, IReadOnlyList<ColumnInfo> columns, Dictionary<int, Dictionary<int, byte[]>>? complexData)
    {
        const string prefix = "__CX:";
        const string suffix = "__";

        int limit = Math.Min(columns.Count, row.Length);
        for (int i = 0; i < limit; i++)
        {
            if (columns[i].Type is not ComplexType)
            {
                continue;
            }

            string value = row[i] ?? string.Empty;
            bool isReference = value.Length > prefix.Length + suffix.Length
                && value.StartsWith(prefix, StringComparison.Ordinal)
                && value.EndsWith(suffix, StringComparison.Ordinal);
            row[i] = isReference
                && int.TryParse(value.AsSpan(prefix.Length, value.Length - prefix.Length - suffix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int complexId)
                && TryGetCell(complexData, i, complexId, out byte[] cell)
                    ? "data:application/octet-stream;base64," + Convert.ToBase64String(cell)
                    : string.Empty;
        }
    }

    internal async ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken)
    {
        if (!format.SupportsComplexColumns)
        {
            return [];
        }

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        byte[]? td = await tableDefs.ReadTDefBytesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null)
        {
            return [];
        }

        List<ColumnInfo>? columns = TDefCodec.ReadColumns(format, td, out _, out _, out _, out _);
        if (columns is null)
        {
            return [];
        }

        var byComplexId = new Dictionary<int, (string Name, ColumnType Type)>();
        foreach (ColumnInfo column in columns)
        {
            if (column.Type is ComplexType && (column.Misc <= 0 || !byComplexId.TryAdd(column.Misc, (column.Name, column.Type))))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "A required complex parent descriptor identity is missing or ambiguous.", new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = resolved.Entry.TDefPage });
            }
        }

        return byComplexId.Count == 0
            ? []
            : await this.JoinComplexColumnsAsync(byComplexId, resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the <see cref="ColumnMetadata.TypeName"/> of each complex column
    /// of <paramref name="tableName"/>, keyed by its <c>ComplexID</c> (the
    /// descriptor's <see cref="ColumnInfo.Misc"/>), so a renamed column keeps
    /// its name: "Attachment", "Version History", or "Multi-value" and the
    /// element type's display name ("Multi-value Text", "Multi-value Long
    /// Integer"). The element type comes from the flat table's value column,
    /// else from the <c>MSysComplexType_*</c> template name. A column whose
    /// kind or element type cannot be resolved is left out, and the caller
    /// reports it as "Complex". Lenient reads omit invalid descriptors; missing
    /// required catalog structure and real I/O failures remain errors.
    /// </summary>
    /// <param name="tableName">The parent table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<Dictionary<int, string>> ReadColumnTypeNamesAsync(string tableName, CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, string>();
        foreach (ComplexColumnInfo column in await this.TryGetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false))
        {
            string? typeName = column.Kind switch
            {
                ComplexColumnKind.Attachment => "Attachment",
                ComplexColumnKind.VersionHistory => "Version History",
                ComplexColumnKind.MultiValue => await this.DescribeMultiValueTypeAsync(column, cancellationToken).ConfigureAwait(false),
                ComplexColumnKind.Unknown => null,
                _ => null,
            };

            if (typeName != null)
            {
                result[column.ComplexId] = typeName;
            }
        }

        return result;
    }

    /// <summary>
    /// Loads the complex columns of <paramref name="tableName"/> into
    /// <see cref="ComplexCellValue"/> cells: one cell per parent complex
    /// reference, holding every attachment or value stored for it. Only the
    /// columns <paramref name="wantedColumns"/> selects are loaded, so a read
    /// that maps a few columns never reads the flat tables or long values of
    /// the complex columns it leaves out.
    /// </summary>
    /// <param name="tableName">The parent table name.</param>
    /// <param name="columns">The parent table's columns.</param>
    /// <param name="wantedColumns">The columns to load, by column index, or <see langword="null"/> for every complex column.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>Cells by column index and complex reference, or <see langword="null"/> when no loaded complex column has items.</returns>
    internal async ValueTask<Dictionary<int, Dictionary<int, byte[]>>?> BuildColumnDataAsync(
        string tableName,
        IReadOnlyList<ColumnInfo> columns,
        bool[]? wantedColumns,
        CancellationToken cancellationToken)
    {
        Dictionary<int, Dictionary<int, byte[]>>? result = null;
        IReadOnlyList<ComplexColumnInfo>? complexColumns = null;
        var discovery = new DiscoveryBudget(maxComplexDiscoveryEntries);

        for (int i = 0; i < columns.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo col = columns[i];
            if (col.Type is not ComplexType
                || (wantedColumns is not null && (i >= wantedColumns.Length || !wantedColumns[i])))
            {
                continue;
            }

            complexColumns ??= await this.TryGetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
            Dictionary<int, byte[]>? colData = await this.LoadColumnCellsAsync(tableName, col.Name, complexColumns, discovery, cancellationToken).ConfigureAwait(false);
            if (colData?.Count > 0)
            {
                result ??= [];
                result[i] = colData;
            }
        }

        return result;
    }

    /// <summary>
    /// Reads every attachment in the flat table behind <paramref name="column"/>,
    /// decoding each <c>FileData</c> wrapper from its stored bytes.
    /// </summary>
    /// <param name="column">The complex column.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<AttachmentRecord>> ReadAttachmentsAsync(ComplexColumnInfo column, CancellationToken cancellationToken)
    {
        FlatTable? flat = await this.ResolveFlatTableAsync(column, cancellationToken).ConfigureAwait(false);
        return flat == null ? [] : await this.ReadAttachmentsAsync(flat, column.ColumnName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads every value in the flat table behind <paramref name="column"/>,
    /// with each version's timestamp for a version-history column.
    /// </summary>
    /// <param name="column">The complex column.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<MultiValueItem>> ReadMultiValueItemsAsync(ComplexColumnInfo column, CancellationToken cancellationToken)
    {
        FlatTable? flat = await this.ResolveFlatTableAsync(column, cancellationToken).ConfigureAwait(false);
        return flat == null ? [] : await this.ReadMultiValueItemsAsync(flat, column.ColumnName, column.Kind, cancellationToken).ConfigureAwait(false);
    }

    private static bool ConceptualTableMatches(string tableIdStr, long targetTdefPage, string? tableName)
    {
        if (targetTdefPage <= 0)
        {
            return false;
        }

        if (CatalogValueReader.TryParseInt64(tableIdStr, out long tableId))
        {
            return CatalogValueReader.TdefPageFromId(tableId) == targetTdefPage;
        }

        return tableName != null && string.Equals(tableIdStr, tableName, StringComparison.OrdinalIgnoreCase);
    }

    private static void ValidateRequiredComplexColumns(TableDef definition, long tdefPage)
    {
        foreach (string columnName in ComplexColumnJoinColumns)
        {
            if (definition.FindColumnIndex(columnName) < 0)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptCatalog,
                    "MSysComplexColumns is missing a required metadata column.",
                    new JetErrorInfo { TableName = Constants.SystemTableNames.ComplexColumns, ColumnName = columnName, PageNumber = tdefPage });
            }
        }
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

    /// <summary>
    /// Classifies a complex column from its flat table's schema, for a column
    /// with no type template (<c>ComplexTypeObjectID</c> 0, as files written
    /// before the template tables existed hold): a <c>FileData</c> column marks
    /// an attachment; leaving out the foreign key and the AutoNumber key, one
    /// value column marks a multi-value column, and a Memo plus a Date/Time
    /// column a version history.
    /// </summary>
    /// <param name="flat">The flat table's definition.</param>
    /// <param name="columnName">The parent's complex column name.</param>
    private static ComplexColumnKind ClassifyFlatTable(TableDef flat, string columnName)
    {
        if (flat.FindColumnIndex("FileData") >= 0)
        {
            return ComplexColumnKind.Attachment;
        }

        int fkIndex = FindForeignKeyIndex(flat, columnName);
        var valueTypes = new List<ColumnType>(2);
        for (int i = 0; i < flat.Columns.Count; i++)
        {
            if (i != fkIndex && (flat.Columns[i].Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0)
            {
                valueTypes.Add(flat.Columns[i].Type);
            }
        }

        return valueTypes.Count switch
        {
            1 => ComplexColumnKind.MultiValue,
            2 when valueTypes.Contains(MemoType) && valueTypes.Contains(DateTimeType) => ComplexColumnKind.VersionHistory,
            _ => ComplexColumnKind.Unknown,
        };
    }

    /// <summary>
    /// Returns the value column type a multi-value <c>MSysComplexType_*</c>
    /// template declares, or <see langword="null"/> for any other name.
    /// </summary>
    /// <param name="templateName">The template table name.</param>
    private static ColumnType? TemplateElementType(string templateName) => templateName.ToUpperInvariant() switch
    {
        "MSYSCOMPLEXTYPE_UNSIGNEDBYTE" => ByteType,
        "MSYSCOMPLEXTYPE_SHORT" => IntegerType,
        "MSYSCOMPLEXTYPE_LONG" => LongIntegerType,
        "MSYSCOMPLEXTYPE_IEEESINGLE" => FloatType,
        "MSYSCOMPLEXTYPE_IEEEDOUBLE" => DoubleType,
        "MSYSCOMPLEXTYPE_GUID" => GuidType,
        "MSYSCOMPLEXTYPE_DECIMAL" => NumericType,
        "MSYSCOMPLEXTYPE_TEXT" => TextType,
        _ => null,
    };

    private static bool TryGetCell(Dictionary<int, Dictionary<int, byte[]>>? complexData, int columnIndex, int complexId, out byte[] cell)
    {
        if (complexId > 0
            && complexData != null
            && complexData.TryGetValue(columnIndex, out Dictionary<int, byte[]>? cells)
            && cells.TryGetValue(complexId, out byte[]? found))
        {
            cell = found;
            return true;
        }

        cell = [];
        return false;
    }

    private static Dictionary<int, byte[]> GroupCells<T>(
        IReadOnlyList<T> items,
        Func<T, int> conceptualTableId,
        Func<int, IReadOnlyList<T>, byte[]> encode)
    {
        var byParent = new Dictionary<int, List<T>>();
        foreach (T item in items)
        {
            int parentId = conceptualTableId(item);
            if (!byParent.TryGetValue(parentId, out List<T>? group))
            {
                group = [];
                byParent[parentId] = group;
            }

            group.Add(item);
        }

        var cells = new Dictionary<int, byte[]>(byParent.Count);
        foreach (KeyValuePair<int, List<T>> pair in byParent)
        {
            cells[pair.Key] = encode(pair.Key, pair.Value);
        }

        return cells;
    }

    /// <summary>
    /// Finds a multi-value flat table's value column: the column named
    /// <c>value</c>, else the first column that is neither the foreign key
    /// nor the flat table's AutoNumber key.
    /// </summary>
    /// <param name="flat">The flat table's definition.</param>
    /// <param name="fkIndex">The foreign key's index.</param>
    private static int FindValueColumnIndex(TableDef flat, int fkIndex)
    {
        int index = flat.FindColumnIndex("value");
        if (index >= 0)
        {
            return index;
        }

        for (int i = 0; i < flat.Columns.Count; i++)
        {
            if (i != fkIndex && (flat.Columns[i].Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0)
            {
                return i;
            }
        }

        return -1;
    }

    private static int ReadInt32OrZero(object?[] row, int index)
        => index >= 0 && row[index] is int value ? value : 0;

    private static string? ReadStringOrNull(object?[] row, int index)
        => index >= 0 && row[index] is string value ? value : null;

    /// <summary>
    /// Finds the flat table's <c>_&lt;column&gt;</c> back-reference to the parent's
    /// complex slot. Prefers the exact name, then a <c>_</c>-prefixed Long that is
    /// not the flat table's own AutoNumber (<c>&lt;table&gt;_&lt;column&gt;</c>, which
    /// also starts with <c>_</c> when the parent table's name does), and only
    /// then any <c>_</c>-prefixed or plain Long column.
    /// </summary>
    /// <param name="flat">The flat table's definition.</param>
    /// <param name="columnName">The parent's complex column name.</param>
    private static int FindForeignKeyIndex(TableDef flat, string columnName)
    {
        string foreignKeyName = "_" + columnName;
        int index = flat.FindColumnIndex(c => c.Type == LongIntegerType && string.Equals(c.Name, foreignKeyName, StringComparison.OrdinalIgnoreCase));
        if (index < 0)
        {
            index = flat.FindColumnIndex(c => c.Type == LongIntegerType
                && c.Name.StartsWith('_')
                && (c.Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0
                && !c.Name.EndsWith(foreignKeyName, StringComparison.OrdinalIgnoreCase));
        }

        if (index < 0)
        {
            index = flat.FindColumnIndex(c => c.Type == LongIntegerType && c.Name.StartsWith('_'));
        }

        return index >= 0 ? index : flat.FindColumnIndex(c => c.Type == LongIntegerType);
    }

    private static ComplexColumnInfo? FindComplexColumn(IReadOnlyList<ComplexColumnInfo> complexColumns, string columnName)
    {
        foreach (ComplexColumnInfo column in complexColumns)
        {
            if (string.Equals(column.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return null;
    }

    private static bool IsAttachmentTemplate(TableDef template)
        => template.Columns.Count == 6 && HasAttachmentPayloadSchema(template);

    private static bool HasAttachmentPayloadSchema(TableDef template)
        => template.FindColumn("FileData")?.Type == OleType
            && template.FindColumn("FileFlags")?.Type == LongIntegerType
            && template.FindColumn("FileName")?.Type == TextType
            && template.FindColumn("FileTimeStamp")?.Type == DateTimeType
            && template.FindColumn("FileType")?.Type == TextType
            && template.FindColumn("FileURL")?.Type == MemoType;

    private static long FindComplexCatalogPage(Dictionary<long, CatalogRow> objects)
    {
        foreach (CatalogRow entry in objects.Values)
        {
            if (entry.ObjectType == Constants.SystemObjects.UserTableType && string.Equals(entry.Name, Constants.SystemTableNames.ComplexColumns, StringComparison.OrdinalIgnoreCase))
            {
                return entry.TDefPage;
            }
        }

        return 0;
    }

    private async ValueTask<IReadOnlyList<ComplexColumnInfo>> JoinComplexColumnsAsync(
        Dictionary<int, (string Name, ColumnType Type)> byComplexId,
        long parentTdefPage,
        CancellationToken cancellationToken)
    {
        Dictionary<long, CatalogRow> objectNamesById = await this.BuildObjectNameLookupAsync(cancellationToken).ConfigureAwait(false);
        long msysTdef = FindComplexCatalogPage(objectNamesById);
        if (msysTdef <= 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The required MSysComplexColumns catalog table is missing.");
        }

        TableDef msys = await tableDefs.ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false)
            ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The required MSysComplexColumns definition could not be read.");

        int idxColumnName = msys.FindColumnIndex("ColumnName");
        int idxComplexId = msys.FindColumnIndex("ComplexID");
        int idxFlatTable = msys.FindColumnIndex("FlatTableID");
        int idxConceptualTable = msys.FindColumnIndex("ConceptualTableID");
        int idxComplexType = msys.FindColumnIndex("ComplexTypeObjectID");

        ValidateRequiredComplexColumns(msys, msysTdef);

        var result = new List<ComplexColumnInfo>(byComplexId.Count);
        var referencedIds = new HashSet<int>();
        var expectedNames = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<int, (string Name, ColumnType Type)> expected in byComplexId)
        {
            expectedNames.Add(expected.Value.Name, expected.Key);
        }

        var seenIds = new HashSet<int>();
        var seenFlats = new HashSet<int>();
        int discoveryWork = objectNamesById.Count;
        await foreach (string[] row in rows.EnumerateRowsForTdefAsync(msysTdef, msys, ComplexColumnJoinColumns, cancellationToken).ConfigureAwait(false))
        {
            if (discoveryWork >= maxComplexDiscoveryEntries)
            {
                throw new JetLimitationException(JetErrorCode.ComplexDiscoveryBudgetExceeded, "Complex catalog discovery exceeds MaxComplexDiscoveryEntries.");
            }

            discoveryWork++;

            if (ConceptualTableMatches(CatalogValueReader.GetStringOrEmpty(row, idxConceptualTable), parentTdefPage, tableName: null)
                && expectedNames.TryGetValue(CatalogValueReader.GetStringOrEmpty(row, idxColumnName), out int mappedId))
            {
                referencedIds.Add(mappedId);
            }

            if (!CatalogValueReader.TryParseInt32(row, idxComplexId, out int complexId))
            {
                continue;
            }

            if (!byComplexId.TryGetValue(complexId, out (string Name, ColumnType Type) parent))
            {
                continue;
            }

            referencedIds.Add(complexId);
            if (!ConceptualTableMatches(CatalogValueReader.GetStringOrEmpty(row, idxConceptualTable), parentTdefPage, tableName: null))
            {
                if (rows.StrictParsing)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The complex catalog parent identity disagrees with its descriptor.");
                }

                continue;
            }

            bool hasTypeObjectId = CatalogValueReader.TryParseInt32(row, idxComplexType, out int typeObjectId);
            string typeName = typeObjectId != 0 && objectNamesById.TryGetValue(typeObjectId, out CatalogRow? tn) ? tn.Name : string.Empty;
            if (!this.AcceptComplexTypeReference(hasTypeObjectId, typeObjectId, typeName, msysTdef))
            {
                continue;
            }

            if (!seenIds.Add(complexId))
            {
                if (rows.StrictParsing)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The complex catalog descriptor identity is ambiguous.");
                }

                result.RemoveAll(column => column.ComplexId == complexId);
                continue;
            }

            int flatId = CatalogValueReader.ParseInt32OrZero(row, idxFlatTable);
            int conceptualId = CatalogValueReader.ParseInt32OrZero(row, idxConceptualTable);
            if (!seenFlats.Add(flatId))
            {
                if (rows.StrictParsing)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The complex catalog flat table identity is ambiguous.");
                }

                result.RemoveAll(column => column.FlatTableId == flatId);
                continue;
            }

            string columnName = CatalogValueReader.GetStringOrEmpty(row, idxColumnName);
            if (!string.Equals(columnName, parent.Name, StringComparison.OrdinalIgnoreCase))
            {
                if (rows.StrictParsing)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The complex catalog column identity disagrees with its parent descriptor.");
                }

                continue;
            }

            string flatName = flatId != 0 && objectNamesById.TryGetValue(flatId, out CatalogRow? fn) ? fn.Name : string.Empty;

            var info = new ComplexColumnInfo
            {
                ColumnName = string.IsNullOrEmpty(columnName) ? parent.Name : columnName,
                ComplexId = complexId,
                Kind = ClassifyComplexKind(typeName),
                FlatTableName = flatName,
                FlatTableId = flatId,
                ConceptualTableId = conceptualId,
                ComplexTypeObjectId = typeObjectId,
                ComplexTypeName = typeName,
            };

            if (info.Kind == ComplexColumnKind.Unknown)
            {
                info = info with { Kind = await this.ClassifyFromFlatTableAsync(info, cancellationToken).ConfigureAwait(false) };
            }

            if (await this.AcceptComplexSchemaAsync(info, objectNamesById, cancellationToken).ConfigureAwait(false))
            {
                result.Add(info);
            }
        }

        if (referencedIds.Count != byComplexId.Count)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "A required MSysComplexColumns entry is missing.", new JetErrorInfo { TableName = Constants.SystemTableNames.ComplexColumns, PageNumber = msysTdef });
        }

        return result;
    }

    /// <summary>
    /// Classifies <paramref name="column"/> from its flat table's schema
    /// (<see cref="ClassifyFlatTable"/>), or returns
    /// <see cref="ComplexColumnKind.Unknown"/> when the flat table cannot be read.
    /// </summary>
    /// <param name="column">A column its template name did not classify.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<ComplexColumnKind> ClassifyFromFlatTableAsync(ComplexColumnInfo column, CancellationToken cancellationToken)
    {
        try
        {
            FlatTable? flat = await this.ResolveFlatTableAsync(column, cancellationToken).ConfigureAwait(false);
            return flat == null ? ComplexColumnKind.Unknown : ClassifyFlatTable(flat.Definition, column.ColumnName);
        }
        catch (InvalidDataException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(ClassifyFromFlatTableAsync), ex);
            return ComplexColumnKind.Unknown;
        }
    }

    /// <summary>
    /// Returns "Multi-value" and the display name of <paramref name="column"/>'s
    /// element type: the type of its flat table's value column, else the one
    /// its type template declares; <see langword="null"/> when neither resolves.
    /// </summary>
    /// <param name="column">A multi-value column.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<string?> DescribeMultiValueTypeAsync(ComplexColumnInfo column, CancellationToken cancellationToken)
    {
        ColumnType? elementType = null;
        try
        {
            FlatTable? flat = await this.ResolveFlatTableAsync(column, cancellationToken).ConfigureAwait(false);
            if (flat != null)
            {
                int valueIndex = FindValueColumnIndex(flat.Definition, FindForeignKeyIndex(flat.Definition, column.ColumnName));
                elementType = valueIndex >= 0 ? flat.Definition.Columns[valueIndex].Type : null;
            }
        }
        catch (InvalidDataException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(DescribeMultiValueTypeAsync), ex);
        }

        elementType ??= TemplateElementType(column.ComplexTypeName);
        return elementType is ColumnType type ? "Multi-value " + GetTypeDisplayName(type) : null;
    }

    private async ValueTask<Dictionary<long, CatalogRow>> BuildObjectNameLookupAsync(CancellationToken cancellationToken, int? maxEntries = null)
    {
        var map = new Dictionary<long, CatalogRow>();
        foreach (CatalogRow entry in await catalog.ReadValidatedObjectsAsync(cancellationToken, maxEntries ?? maxComplexDiscoveryEntries).ConfigureAwait(false))
        {
            if (!map.TryAdd(entry.Id, entry))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects contains duplicate object identities.");
            }
        }

        return map;
    }

    private async ValueTask<bool> AcceptComplexSchemaAsync(ComplexColumnInfo info, Dictionary<long, CatalogRow> objects, CancellationToken cancellationToken)
    {
        bool valid = objects.TryGetValue(info.FlatTableId, out CatalogRow? flatObject)
            && flatObject.ObjectType == Constants.SystemObjects.UserTableType
            && (flatObject.Flags & Constants.SystemObjects.ComplexFlatTableFlags) == Constants.SystemObjects.ComplexFlatTableFlags;
        TableDef? flat = valid ? await tableDefs.ReadTableDefAsync(flatObject!.TDefPage, cancellationToken).ConfigureAwait(false) : null;
        int foreignKey = flat is null ? -1 : FindForeignKeyIndex(flat, info.ColumnName);
        valid &= foreignKey >= 0 && !flat!.Columns[foreignKey].IsAutoNumber && flat.Columns[foreignKey].Name.StartsWith('_');
        if (valid && info.Kind == ComplexColumnKind.Unknown)
        {
            info = info with { Kind = ClassifyFlatTable(flat!, info.ColumnName) };
            valid = info.Kind != ComplexColumnKind.Unknown;
        }

        if (valid && info.Kind == ComplexColumnKind.Attachment)
        {
            valid = HasAttachmentPayloadSchema(flat!);
        }

        if (valid && info.ComplexTypeObjectId != 0)
        {
            valid = objects.TryGetValue(info.ComplexTypeObjectId, out CatalogRow? templateObject)
                && templateObject.ObjectType == Constants.SystemObjects.UserTableType;
            TableDef? template = valid ? await tableDefs.ReadTableDefAsync(templateObject!.TDefPage, cancellationToken).ConfigureAwait(false) : null;
            valid &= template is { Columns.Count: > 0 };
            if (valid)
            {
                int fk = FindForeignKeyIndex(flat!, info.ColumnName);
                var payload = new List<ColumnType>();
                for (int i = 0; i < flat!.Columns.Count; i++)
                {
                    if (i != fk && !flat.Columns[i].IsAutoNumber)
                    {
                        payload.Add(flat.Columns[i].Type);
                    }
                }

                valid = payload.Count == template!.Columns.Count;
                foreach (ColumnInfo column in template.Columns)
                {
                    valid &= !column.IsAutoNumber && !column.IsCalculated && payload.Remove(column.Type);
                }

                ColumnType? expected = TemplateElementType(info.ComplexTypeName);
                valid &= info.Kind != ComplexColumnKind.MultiValue
                    || (expected is not null && template.Columns.Count == 1 && template.Columns[0].Type == expected && string.Equals(template.Columns[0].Name, "Value", StringComparison.OrdinalIgnoreCase));
                valid &= info.Kind != ComplexColumnKind.Attachment || (IsAttachmentTemplate(template) && HasAttachmentPayloadSchema(flat));
                valid &= info.Kind != ComplexColumnKind.VersionHistory || ClassifyFlatTable(flat, info.ColumnName) == ComplexColumnKind.VersionHistory;
            }
        }

        if (!valid && rows.StrictParsing)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysComplexColumns contains an invalid flat table or incompatible type template.", new JetErrorInfo { TableName = Constants.SystemTableNames.ComplexColumns, ColumnName = "FlatTableID" });
        }

        return valid;
    }

    private bool AcceptComplexTypeReference(bool hasTypeObjectId, int typeObjectId, string typeName, long tdefPage)
    {
        if (hasTypeObjectId && (typeObjectId == 0 || ClassifyComplexKind(typeName) != ComplexColumnKind.Unknown))
        {
            return true;
        }

        var error = new JetCorruptDataException(
            JetErrorCode.CorruptCatalog,
            "MSysComplexColumns contains an invalid complex type template reference.",
            new JetErrorInfo { TableName = Constants.SystemTableNames.ComplexColumns, ColumnName = "ComplexTypeObjectID", PageNumber = tdefPage });
        if (rows.StrictParsing)
        {
            throw error;
        }

        this.TraceBestEffortFallback(nameof(AcceptComplexTypeReference), error);
        return false;
    }

    private async ValueTask<long> GetComplexFlatTablePageAsync(string tableName, string columnName, DiscoveryBudget discovery, CancellationToken cancellationToken)
    {
        try
        {
            Dictionary<long, CatalogRow> objectNamesById = await this.BuildObjectNameLookupAsync(cancellationToken, discovery.Remaining).ConfigureAwait(false);
            discovery.Consume(objectNamesById.Count);
            long msysTdef = FindComplexCatalogPage(objectNamesById);
            if (msysTdef <= 0)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The required MSysComplexColumns catalog table is missing.");
            }

            TableDef td = await tableDefs.ReadTableDefAsync(msysTdef, cancellationToken).ConfigureAwait(false)
                ?? throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The required MSysComplexColumns definition could not be read.");

            int idxCol = td.FindColumnIndex("ColumnName");
            int idxConceptualTable = td.FindColumnIndex("ConceptualTableID");
            int idxFlatTable = td.FindColumnIndex("FlatTableID");

            ValidateRequiredComplexColumns(td, msysTdef);

            ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            long targetTdefPage = resolved?.Entry.TDefPage ?? 0;
            if (targetTdefPage <= 0)
            {
                return 0;
            }

            long matchedPage = 0;
            long matchedId = 0;
            var seenFlatIds = new HashSet<long>();
            var ambiguousFlatIds = new HashSet<long>();
            int idxComplexType = td.FindColumnIndex("ComplexTypeObjectID");

            await foreach (string[] row in rows.EnumerateRowsForTdefAsync(msysTdef, td, FlatTableLookupColumns, cancellationToken).ConfigureAwait(false))
            {
                discovery.Consume(1);
                if (CatalogValueReader.TryParseInt64(row, idxFlatTable, out long discoveredFlatId) && !seenFlatIds.Add(discoveredFlatId))
                {
                    ambiguousFlatIds.Add(discoveredFlatId);
                }

                string colName = CatalogValueReader.GetStringOrEmpty(row, idxCol);
                if (!string.Equals(colName, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!ConceptualTableMatches(CatalogValueReader.GetStringOrEmpty(row, idxConceptualTable), targetTdefPage, tableName: null))
                {
                    continue;
                }

                bool hasTypeObjectId = CatalogValueReader.TryParseInt32(row, idxComplexType, out int typeObjectId);
                string typeName = typeObjectId != 0 && objectNamesById.TryGetValue(typeObjectId, out CatalogRow? templateName) ? templateName.Name : string.Empty;
                if (!this.AcceptComplexTypeReference(hasTypeObjectId, typeObjectId, typeName, msysTdef))
                {
                    continue;
                }

                if (CatalogValueReader.TryParseInt64(row, idxFlatTable, out long flatId))
                {
                    long flatTdef = CatalogValueReader.TdefPageFromId(flatId);
                    if (flatTdef > 0)
                    {
                        if (matchedPage != 0)
                        {
                            return 0;
                        }

                        var info = new ComplexColumnInfo
                        {
                            ColumnName = columnName,
                            FlatTableId = checked((int)flatId),
                            ComplexTypeObjectId = typeObjectId,
                            ComplexTypeName = typeName,
                            Kind = ClassifyComplexKind(typeName),
                        };
                        if (await this.AcceptComplexSchemaAsync(info, objectNamesById, cancellationToken).ConfigureAwait(false))
                        {
                            matchedPage = flatTdef;
                            matchedId = flatId;
                        }
                    }
                }
            }

            if (ambiguousFlatIds.Contains(matchedId))
            {
                if (rows.StrictParsing)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "The complex fallback flat table identity is ambiguous.");
                }

                return 0;
            }

            return matchedPage;
        }
        catch (InvalidDataException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(GetComplexFlatTablePageAsync), ex);
        }

        return 0;
    }

    /// <summary>
    /// Best-effort <see cref="GetComplexColumnsAsync"/> for table scans: a damaged
    /// parent TDEF, <c>MSysComplexColumns</c> or <c>MSysObjects</c> is traced and
    /// yields no descriptors, so <see cref="LoadColumnCellsAsync"/> falls back to
    /// finding each flat table through its parent/column catalog mapping instead of failing the whole read.
    /// </summary>
    /// <param name="tableName">The parent table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<IReadOnlyList<ComplexColumnInfo>> TryGetComplexColumnsAsync(string tableName, CancellationToken cancellationToken)
    {
        try
        {
            return await this.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(TryGetComplexColumnsAsync), ex);
        }
        catch (IndexOutOfRangeException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(TryGetComplexColumnsAsync), ex);
        }
        catch (OverflowException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(TryGetComplexColumnsAsync), ex);
        }

        return [];
    }

    private async ValueTask<Dictionary<int, byte[]>?> LoadColumnCellsAsync(
        string tableName,
        string columnName,
        IReadOnlyList<ComplexColumnInfo> complexColumns,
        DiscoveryBudget discovery,
        CancellationToken cancellationToken)
    {
        try
        {
            ComplexColumnInfo? info = FindComplexColumn(complexColumns, columnName);
            FlatTable? flat = info == null
                ? null
                : await this.ResolveFlatTableAsync(info, cancellationToken).ConfigureAwait(false);
            flat ??= await this.FindFlatTableFallbackAsync(tableName, columnName, discovery, cancellationToken).ConfigureAwait(false);
            if (flat == null)
            {
                return null;
            }

            ComplexColumnKind kind = info?.Kind ?? ComplexColumnKind.Unknown;
            if (kind == ComplexColumnKind.Unknown)
            {
                kind = ClassifyFlatTable(flat.Definition, columnName);
            }

            Dictionary<int, byte[]> cells;
            if (kind == ComplexColumnKind.Attachment)
            {
                cells = GroupCells(
                    await this.ReadAttachmentsAsync(flat, columnName, cancellationToken).ConfigureAwait(false),
                    static attachment => attachment.ConceptualTableId,
                    ComplexCellValue.EncodeAttachments);
            }
            else
            {
                IReadOnlyList<MultiValueItem> items = await this.ReadMultiValueItemsAsync(flat, columnName, kind, cancellationToken).ConfigureAwait(false);
                cells = kind == ComplexColumnKind.VersionHistory
                    ? GroupCells(items, static item => item.ConceptualTableId, ComplexCellValue.EncodeVersionHistoryItems)
                    : GroupCells(items, static item => item.ConceptualTableId, ComplexCellValue.EncodeMultiValueItems);
            }

            return cells.Count > 0 ? cells : null;
        }
        catch (InvalidDataException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(LoadColumnCellsAsync), ex);
            return null;
        }
        catch (IndexOutOfRangeException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(LoadColumnCellsAsync), ex);
            return null;
        }
        catch (OverflowException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(LoadColumnCellsAsync), ex);
            return null;
        }
    }

    private async ValueTask<FlatTable?> ResolveFlatTableAsync(ComplexColumnInfo column, CancellationToken cancellationToken)
    {
        long tdefPage = column.FlatTableId > 0 ? CatalogValueReader.TdefPageFromId(column.FlatTableId) : 0;
        TableDef? td = tdefPage > 0 ? await tableDefs.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false) : null;
        if (td?.Columns.Count > 0)
        {
            return new FlatTable(tdefPage, td);
        }

        if (string.IsNullOrEmpty(column.FlatTableName))
        {
            return null;
        }

        ResolvedTable? resolved = await catalog.ResolveTableAsync(column.FlatTableName, cancellationToken).ConfigureAwait(false);
        return resolved == null ? null : new FlatTable(resolved.Entry.TDefPage, resolved.Definition);
    }

    private async ValueTask<FlatTable?> FindFlatTableFallbackAsync(string tableName, string columnName, DiscoveryBudget discovery, CancellationToken cancellationToken)
    {
        long tdefPage = await this.GetComplexFlatTablePageAsync(tableName, columnName, discovery, cancellationToken).ConfigureAwait(false);
        TableDef? td = tdefPage > 0 ? await tableDefs.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false) : null;
        return td == null ? null : new FlatTable(tdefPage, td);
    }

    private async ValueTask<IReadOnlyList<AttachmentRecord>> ReadAttachmentsAsync(FlatTable flat, string columnName, CancellationToken cancellationToken)
    {
        TableDef td = flat.Definition;
        int idxFk = FindForeignKeyIndex(td, columnName);
        int idxFileUrl = td.FindColumnIndex("FileURL");
        int idxFileName = td.FindColumnIndex("FileName");
        int idxFileType = td.FindColumnIndex("FileType");
        int idxFileTime = td.FindColumnIndex("FileTimeStamp");
        int idxFileData = td.FindColumnIndex("FileData");

        var result = new List<AttachmentRecord>();
        await foreach (object?[] row in rows.EnumerateTypedRowsForTdefAsync(flat.TDefPage, td, cancellationToken).ConfigureAwait(false))
        {
            byte[] fileData = idxFileData >= 0 && row[idxFileData] is byte[] stored ? stored : [];
            string fileType = ReadStringOrNull(row, idxFileType) ?? string.Empty;
            if (fileData.Length > 0)
            {
                if (!AttachmentWrapper.TryDecode(fileData, out string wrappedType, out byte[] payload, maxAttachmentContentBytes))
                {
                    throw new InvalidDataException($"The attachment wrapper for complex column '{columnName}' is malformed.");
                }

                fileData = payload;
                if (fileType.Length == 0)
                {
                    fileType = wrappedType;
                }
            }

            result.Add(new AttachmentRecord
            {
                ConceptualTableId = ReadInt32OrZero(row, idxFk),
                FileName = ReadStringOrNull(row, idxFileName) ?? string.Empty,
                FileType = fileType,
                FileURL = ReadStringOrNull(row, idxFileUrl),
                FileTimeStamp = idxFileTime >= 0 && row[idxFileTime] is DateTime timeStamp ? timeStamp : null,
                FileData = fileData,
            });
        }

        return result;
    }

    /// <summary>
    /// Reads every row of a multi-value or version-history flat table, in
    /// flat-table order. For a version history, as in Jackcess
    /// <c>VersionHistoryColumnInfoImpl</c>, the value is the first Memo column
    /// that is neither the foreign key nor the AutoNumber key, and
    /// <see cref="MultiValueItem.Modified"/> is the first Date/Time column
    /// (<c>Modified_&lt;GUID&gt;</c>). Jackcess sorts versions newest first;
    /// this keeps the stored order.
    /// </summary>
    /// <param name="flat">The flat table.</param>
    /// <param name="columnName">The parent's complex column name.</param>
    /// <param name="kind">The column's kind.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<IReadOnlyList<MultiValueItem>> ReadMultiValueItemsAsync(FlatTable flat, string columnName, ComplexColumnKind kind, CancellationToken cancellationToken)
    {
        TableDef td = flat.Definition;
        int idxFk = FindForeignKeyIndex(td, columnName);
        int idxValue = -1;
        int idxModified = -1;
        if (kind == ComplexColumnKind.VersionHistory)
        {
            for (int i = 0; i < td.Columns.Count; i++)
            {
                ColumnInfo column = td.Columns[i];
                if (idxValue < 0 && column.Type == MemoType && i != idxFk && (column.Flags & Constants.ColumnDescriptorFlags.AutoNumber) == 0)
                {
                    idxValue = i;
                }
                else if (idxModified < 0 && column.Type == DateTimeType)
                {
                    idxModified = i;
                }
            }
        }

        if (idxValue < 0)
        {
            idxValue = FindValueColumnIndex(td, idxFk);
        }

        var result = new List<MultiValueItem>();
        await foreach (object?[] row in rows.EnumerateTypedRowsForTdefAsync(flat.TDefPage, td, cancellationToken).ConfigureAwait(false))
        {
            result.Add(new MultiValueItem
            {
                ConceptualTableId = ReadInt32OrZero(row, idxFk),
                Value = idxValue >= 0 && row[idxValue] is not (null or DBNull) ? row[idxValue] : null,
                Modified = idxModified >= 0 && row[idxModified] is DateTime modified ? modified : null,
            });
        }

        return result;
    }

    private void TraceBestEffortFallback(string operation, Exception exception)
    {
        if (diagnosticsEnabled)
        {
            Trace.WriteLine($"[AccessReader] Best-effort fallback in ComplexColumnReader.{operation}: suppressed {exception.GetType().Name} while reading MSysComplexColumns.");
        }
    }

    /// <summary>Bounds aggregate catalog work across all fallback columns of a table scan.</summary>
    /// <param name="limit">The maximum number of entries to inspect.</param>
    private sealed class DiscoveryBudget(int limit)
    {
        /// <summary>Gets the remaining entries permitted before schema loading.</summary>
        internal int Remaining { get; private set; } = limit;

        /// <summary>Charges inspected metadata entries to this scan.</summary>
        /// <param name="count">The number of entries inspected.</param>
        /// <exception cref="JetLimitationException">The aggregate metadata limit is exceeded.</exception>
        internal void Consume(int count)
        {
            this.Remaining -= count;
            if (this.Remaining < 0)
            {
                throw new JetLimitationException(JetErrorCode.ComplexDiscoveryBudgetExceeded, "Complex fallback discovery exceeds MaxComplexDiscoveryEntries.");
            }
        }
    }

    /// <summary>A complex column's hidden flat child table.</summary>
    /// <param name="TDefPage">The flat table's TDEF page.</param>
    /// <param name="Definition">The flat table's definition.</param>
    private sealed record FlatTable(long TDefPage, TableDef Definition);
}
