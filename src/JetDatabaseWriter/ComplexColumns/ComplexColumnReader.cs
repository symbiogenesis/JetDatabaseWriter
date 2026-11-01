namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
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
/// <param name="maxAttachmentContentBytesPerOperation">The maximum aggregate attachment content length for one attachment read or table scan.</param>
/// <param name="maxAttachmentDecodeWorkBytesPerOperation">The maximum aggregate attachment byte-processing work.</param>
/// <param name="attachmentReads">The operation budget context shared by nested and linked reads.</param>
/// <param name="maxComplexItemIdentityEntriesPerOperation">The aggregate scalar identity inspection allowance.</param>
/// <param name="complexItemReads">The identity allowance shared by nested and linked operations.</param>
internal sealed partial class ComplexColumnReader(JetFormat format, TableDefReader tableDefs, CatalogReader catalog, RowDecoder rows, bool diagnosticsEnabled, int maxAttachmentContentBytes = 64 * 1024 * 1024, int maxComplexDiscoveryEntries = 65536, long maxAttachmentContentBytesPerOperation = 256 * 1024 * 1024, long maxAttachmentDecodeWorkBytesPerOperation = 2L * 1024 * 1024 * 1024, AttachmentReadContext? attachmentReads = null, int maxComplexItemIdentityEntriesPerOperation = 1_048_576, ComplexItemIdentityReadContext? complexItemReads = null)
{
    private const int DescriptorCacheCapacity = 64;
    private const int ObjectNameCacheCapacity = 4096;

    /// <summary>The <c>MSysComplexColumns</c> columns the descriptor join reads.</summary>
    private static readonly string[] ComplexColumnJoinColumns = ["ColumnName", "ComplexID", "FlatTableID", "ConceptualTableID", "ComplexTypeObjectID"];

    /// <summary>The <c>MSysComplexColumns</c> columns the flat-table lookup by column name reads.</summary>
    private static readonly string[] FlatTableLookupColumns = ["ColumnName", "ConceptualTableID", "FlatTableID", "ComplexTypeObjectID"];

#if NET9_0_OR_GREATER
    private readonly Lock metadataGate = new();
#else
    private readonly object metadataGate = new();
#endif
    private readonly Dictionary<string, DescriptorSnapshot> descriptors = new(StringComparer.OrdinalIgnoreCase);
    private Dictionary<long, CatalogRow>? objectNames;
    private int metadataGeneration = -1;
    private long metadataWriteEpoch = -1;

    internal async ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!format.SupportsComplexColumns)
        {
            return [];
        }

        (int Generation, long WriteEpoch) revision = this.ReadMetadataRevision();
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return [];
        }

        lock (this.metadataGate)
        {
            // Auditing must also revisit the catalog, flat and template images.
            if (!tableDefs.VerifyOnHit && this.MatchesMetadataRevision(revision)
                && this.descriptors.TryGetValue(tableName, out DescriptorSnapshot? cached)
                && cached.ParentRoot == resolved.Entry.TDefPage
                && ReferenceEquals(cached.ParentImage, resolved.Schema.Image))
            {
                return cached.Columns;
            }
        }

        var byComplexId = new Dictionary<int, (string Name, ColumnType Type)>();
        foreach (ColumnInfo column in resolved.Definition.Columns)
        {
            if (column.Type is ComplexType && (column.Misc <= 0 || !byComplexId.TryAdd(column.Misc, (column.Name, column.Type))))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "A required complex parent descriptor identity is missing or ambiguous.", new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = resolved.Entry.TDefPage });
            }
        }

        IReadOnlyList<ComplexColumnInfo> result = byComplexId.Count == 0
            ? []
            : await this.JoinComplexColumnsAsync(byComplexId, resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);

        // A lenient join can omit damaged entries. Retrying it must retain its
        // validation and diagnostics instead of turning the omission into a hit.
        if (result.Count == byComplexId.Count)
        {
            var retained = new ComplexColumnInfo[result.Count];
            for (int index = 0; index < result.Count; index++)
            {
                retained[index] = result[index];
            }

            result = Array.AsReadOnly(retained);
            lock (this.metadataGate)
            {
                if (this.MatchesMetadataRevision(revision))
                {
                    if (this.descriptors.Count >= DescriptorCacheCapacity)
                    {
                        this.descriptors.Clear();
                    }

                    this.descriptors[tableName] = new DescriptorSnapshot(resolved.Entry.TDefPage, resolved.Schema.Image, result);
                }
            }
        }

        return result;
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
    /// Creates scan-local resolution of selected complex columns. Metadata is
    /// retained per column; attachment payloads and cells belong only to the
    /// parent row currently being resolved.
    /// </summary>
    /// <param name="tableName">The parent table name.</param>
    /// <param name="columns">The parent table's columns.</param>
    /// <param name="wantedColumns">The columns to resolve, or null for all columns.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The demand-driven resolver.</returns>
    internal async ValueTask<RowResolver> CreateRowResolverAsync(
        string tableName,
        IReadOnlyList<ColumnInfo> columns,
        bool[]? wantedColumns,
        CancellationToken cancellationToken)
    {
        var resolver = new RowResolver(this, tableName, columns, wantedColumns, maxComplexDiscoveryEntries, this.GetAttachmentBudget());
        await resolver.PrepareAsync(cancellationToken).ConfigureAwait(false);
        return resolver;
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
        return flat == null ? [] : await this.ReadAttachmentsAsync(flat, column.ColumnName, this.GetAttachmentBudget(), cancellationToken).ConfigureAwait(false);
    }

    private AttachmentWrapper.ContentBudget GetAttachmentBudget()
        => attachmentReads?.Budget ?? new(maxAttachmentContentBytesPerOperation, maxAttachmentDecodeWorkBytesPerOperation);

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

    private static bool HasMatchingHistoryTemplate(TableDef flat, TableDef template, string columnName)
    {
        if (ClassifyFlatTable(flat, columnName) != ComplexColumnKind.VersionHistory)
        {
            return false;
        }

        foreach (ColumnInfo column in template.Columns)
        {
            ColumnInfo? payload = flat.FindColumn(column.Name);
            if (payload is null || payload.Type != column.Type || payload.IsAutoNumber || payload.IsCalculated)
            {
                return false;
            }
        }

        return true;
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
    /// value column marks a multi-value column, and one or more Memo columns
    /// plus one Date/Time column mark a version history.
    /// </summary>
    /// <param name="flat">The flat table's definition.</param>
    /// <param name="columnName">The parent's complex column name.</param>
    private static ComplexColumnKind ClassifyFlatTable(TableDef flat, string columnName)
    {
        if (flat.FindColumn("FileData")?.Type == OleType)
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

        if (valueTypes.Count == 1)
        {
            return ComplexColumnKind.MultiValue;
        }

        return valueTypes.Count >= 2 && valueTypes.Remove(DateTimeType) && valueTypes.TrueForAll(static type => type == MemoType)
            ? ComplexColumnKind.VersionHistory
            : ComplexColumnKind.Unknown;
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
        catch (JetCorruptDataException ex) when (!rows.StrictParsing && ex.ErrorCode != JetErrorCode.CorruptCatalog)
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
        catch (JetCorruptDataException ex) when (!rows.StrictParsing && ex.ErrorCode != JetErrorCode.CorruptCatalog)
        {
            this.TraceBestEffortFallback(nameof(DescribeMultiValueTypeAsync), ex);
        }

        elementType ??= TemplateElementType(column.ComplexTypeName);
        return elementType is ColumnType type ? "Multi-value " + GetTypeDisplayName(type) : null;
    }

    private (int Generation, long WriteEpoch) ReadMetadataRevision()
    {
        lock (this.metadataGate)
        {
            int generation = catalog.Generation;
            long writeEpoch = tableDefs.WriteEpoch;
            if (this.metadataGeneration != generation || this.metadataWriteEpoch != writeEpoch)
            {
                this.descriptors.Clear();
                this.objectNames = null;
                this.metadataGeneration = generation;
                this.metadataWriteEpoch = writeEpoch;
            }

            return (generation, writeEpoch);
        }
    }

    private bool MatchesMetadataRevision((int Generation, long WriteEpoch) revision)
        => catalog.Generation == revision.Generation && tableDefs.WriteEpoch == revision.WriteEpoch
            && this.metadataGeneration == revision.Generation && this.metadataWriteEpoch == revision.WriteEpoch;

    private async ValueTask<Dictionary<long, CatalogRow>> BuildObjectNameLookupAsync(CancellationToken cancellationToken, int? maxEntries = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        (int Generation, long WriteEpoch) revision = this.ReadMetadataRevision();
        int limit = maxEntries ?? maxComplexDiscoveryEntries;
        lock (this.metadataGate)
        {
            if (!tableDefs.VerifyOnHit && this.MatchesMetadataRevision(revision) && this.objectNames is { } cached)
            {
                if (cached.Count > limit)
                {
                    throw new JetLimitationException(JetErrorCode.ComplexDiscoveryBudgetExceeded, "Catalog discovery exceeds MaxComplexDiscoveryEntries.");
                }

                return cached;
            }
        }

        var map = new Dictionary<long, CatalogRow>();
        foreach (CatalogRow entry in await catalog.ReadValidatedObjectsAsync(cancellationToken, limit).ConfigureAwait(false))
        {
            if (!map.TryAdd(entry.Id, entry))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptCatalog, "MSysObjects contains duplicate object identities.");
            }
        }

        lock (this.metadataGate)
        {
            if (map.Count <= ObjectNameCacheCapacity && this.MatchesMetadataRevision(revision))
            {
                this.objectNames = map;
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
                ColumnInfo? value = flat.FindColumn("Value");

                // Matching unknown scalar discriminators remain opaque in metadata.
                // Unsupported value codecs still follow the existing decoding refusal.
                bool opaqueScalar = value is { IsAutoNumber: false, IsCalculated: false }
                    && template.Columns.Count == 1
                    && value.Type != default
                    && value.Type == template.Columns[0].Type
                    && GetClrType(value.Type) is null;
                valid &= info.Kind != ComplexColumnKind.MultiValue
                    || (expected is not null && template.Columns.Count == 1 && (template.Columns[0].Type == expected || opaqueScalar) && string.Equals(template.Columns[0].Name, "Value", StringComparison.OrdinalIgnoreCase));
                valid &= info.Kind != ComplexColumnKind.Attachment || (IsAttachmentTemplate(template) && HasAttachmentPayloadSchema(flat));
                valid &= info.Kind != ComplexColumnKind.VersionHistory || HasMatchingHistoryTemplate(flat, template, info.ColumnName);
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
        catch (JetCorruptDataException ex) when (!rows.StrictParsing && ex.ErrorCode != JetErrorCode.CorruptCatalog)
        {
            this.TraceBestEffortFallback(nameof(GetComplexFlatTablePageAsync), ex);
        }

        return 0;
    }

    /// <summary>
    /// Best-effort <see cref="GetComplexColumnsAsync"/> for table scans: recoverable damage to a
    /// parent TDEF is traced and
    /// yields no descriptors, so <see cref="PrepareColumnSourceAsync"/> falls back to
    /// finding each flat table through its parent/column catalog mapping instead of failing the whole read.
    /// Missing or ambiguous required catalog structure remains a corruption failure in every mode.
    /// </summary>
    /// <param name="tableName">The parent table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<IReadOnlyList<ComplexColumnInfo>> TryGetComplexColumnsAsync(string tableName, CancellationToken cancellationToken)
    {
        try
        {
            return await this.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
        }
        catch (JetCorruptDataException ex) when (!rows.StrictParsing && ex.ErrorCode != JetErrorCode.CorruptCatalog)
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

    private async ValueTask<ColumnSource?> PrepareColumnSourceAsync(
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

            int foreignKey = FindForeignKeyIndex(flat.Definition, columnName);
            IndexMetadata? selectedIndex = null;
            TDefImage? image = await tableDefs.ReadImageAsync(flat.TDefPage, cancellationToken).ConfigureAwait(false);
            if (foreignKey >= 0 && image is not null)
            {
                foreach (IndexMetadata index in IndexCatalogReader.ReadMetadata(format, image.CopyBytes(), image.Columns))
                {
                    if (index.FirstDp > 0 && index.Columns.Count > 0
                        && index.Columns[0].ColumnNumber == flat.Definition.Columns[foreignKey].ColNum
                        && index.Columns[0].IsAscending)
                    {
                        selectedIndex = index;
                        break;
                    }
                }
            }

            return new ColumnSource(flat, kind, selectedIndex);
        }
        catch (JetCorruptDataException ex) when (!rows.StrictParsing && ex.ErrorCode != JetErrorCode.CorruptCatalog)
        {
            this.TraceBestEffortFallback(nameof(PrepareColumnSourceAsync), ex);
            return null;
        }
        catch (IndexOutOfRangeException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(PrepareColumnSourceAsync), ex);
            return null;
        }
        catch (OverflowException ex) when (!rows.StrictParsing)
        {
            this.TraceBestEffortFallback(nameof(PrepareColumnSourceAsync), ex);
            return null;
        }
    }

    private async ValueTask<byte[]?> LoadCellAsync(ColumnSource source, string columnName, int parentReference, AttachmentWrapper.ContentBudget attachmentContent, CancellationToken cancellationToken)
    {
        try
        {
            if (source.Kind == ComplexColumnKind.Attachment)
            {
                IReadOnlyList<AttachmentRecord> items = await this.ReadAttachmentsAsync(source.Flat, columnName, attachmentContent, parentReference, source.Index, source.Inventory, cancellationToken).ConfigureAwait(false);
                return items.Count == 0 ? null : ComplexCellValue.EncodeAttachments(parentReference, items);
            }

            IReadOnlyList<MultiValueItem> values = await this.ReadMultiValueItemsAsync(source.Flat, columnName, source.Kind, parentReference, source.Index, source.Inventory, cancellationToken).ConfigureAwait(false);
            if (values.Count == 0)
            {
                return null;
            }

            return source.Kind == ComplexColumnKind.VersionHistory
                ? ComplexCellValue.EncodeVersionHistoryItems(parentReference, values)
                : ComplexCellValue.EncodeMultiValueItems(parentReference, values);
        }
        catch (JetCorruptDataException ex) when (!rows.StrictParsing && ex.ErrorCode is not (JetErrorCode.CorruptCatalog or JetErrorCode.CorruptIndex))
        {
            this.TraceBestEffortFallback(nameof(LoadCellAsync), ex);
            return null;
        }
    }

    private async ValueTask<FlatTable?> ResolveFlatTableAsync(ComplexColumnInfo column, CancellationToken cancellationToken)
    {
        long tdefPage = column.FlatTableId > 0 ? CatalogValueReader.TdefPageFromId(column.FlatTableId) : 0;
        TableDef? td = tdefPage > 0 ? await tableDefs.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false) : null;
        if (td?.Columns.Count > 0)
        {
            return new FlatTable(tdefPage, td, column.FlatTableName);
        }

        if (string.IsNullOrEmpty(column.FlatTableName))
        {
            return null;
        }

        ResolvedTable? resolved = await catalog.ResolveTableAsync(column.FlatTableName, cancellationToken).ConfigureAwait(false);
        return resolved == null ? null : new FlatTable(resolved.Entry.TDefPage, resolved.Definition, column.FlatTableName);
    }

    private async ValueTask<FlatTable?> FindFlatTableFallbackAsync(string tableName, string columnName, DiscoveryBudget discovery, CancellationToken cancellationToken)
    {
        long tdefPage = await this.GetComplexFlatTablePageAsync(tableName, columnName, discovery, cancellationToken).ConfigureAwait(false);
        TableDef? td = tdefPage > 0 ? await tableDefs.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false) : null;
        return td == null ? null : new FlatTable(tdefPage, td, string.Empty);
    }

    private async ValueTask<IReadOnlyList<AttachmentRecord>> ReadAttachmentsAsync(FlatTable flat, string columnName, AttachmentWrapper.ContentBudget attachmentContent, CancellationToken cancellationToken)
        => await this.ReadAttachmentsAsync(flat, columnName, attachmentContent, parentReference: null, index: null, inventory: null, cancellationToken).ConfigureAwait(false);

    private async ValueTask<IReadOnlyList<AttachmentRecord>> ReadAttachmentsAsync(FlatTable flat, string columnName, AttachmentWrapper.ContentBudget attachmentContent, int? parentReference, IndexMetadata? index, ItemInventory? inventory, CancellationToken cancellationToken)
    {
        TableDef td = flat.Definition;
        int idxId = this.FindItemIdentityIndex(flat, columnName);
        if (idxId < 0)
        {
            return [];
        }

        int idxFk = FindForeignKeyIndex(td, columnName);
        int idxFileUrl = td.FindColumnIndex("FileURL");
        int idxFileName = td.FindColumnIndex("FileName");
        int idxFileType = td.FindColumnIndex("FileType");
        int idxFileTime = td.FindColumnIndex("FileTimeStamp");
        int idxFileData = td.FindColumnIndex("FileData");

        var result = new List<AttachmentRecord>();
        await foreach (object?[] row in this.ReadReferenceRowsAsync(flat, columnName, parentReference, index, inventory, cancellationToken).ConfigureAwait(false))
        {
            int id = (int)row[idxId]!;

            byte[] fileData = idxFileData >= 0 && row[idxFileData] is byte[] stored ? stored : [];
            string fileType = ReadStringOrNull(row, idxFileType) ?? string.Empty;
            if (fileData.Length > 0)
            {
                if (!AttachmentWrapper.TryDecode(fileData, out string wrappedType, out byte[] payload, maxAttachmentContentBytes, attachmentContent, cancellationToken))
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptComplexColumn, $"The attachment wrapper for complex column '{columnName}' is malformed.");
                }

                fileData = payload;
                if (fileType.Length == 0)
                {
                    fileType = wrappedType;
                }
            }

            result.Add(new AttachmentRecord
            {
                Id = id,
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
    /// flat-table order. Each version-history row assigns one Memo payload;
    /// its name identifies the source parent Memo when several columns share
    /// the history table. The Date/Time column (<c>Modified_&lt;GUID&gt;</c>) supplies
    /// <see cref="MultiValueItem.Modified"/>. Rows left without a payload after
    /// a native Memo drop are omitted. Versions retain their stored order.
    /// </summary>
    /// <param name="flat">The flat table.</param>
    /// <param name="columnName">The parent's complex column name.</param>
    /// <param name="kind">The column's kind.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<IReadOnlyList<MultiValueItem>> ReadMultiValueItemsAsync(FlatTable flat, string columnName, ComplexColumnKind kind, CancellationToken cancellationToken)
        => await this.ReadMultiValueItemsAsync(flat, columnName, kind, parentReference: null, index: null, inventory: null, cancellationToken).ConfigureAwait(false);

    private async ValueTask<IReadOnlyList<MultiValueItem>> ReadMultiValueItemsAsync(FlatTable flat, string columnName, ComplexColumnKind kind, int? parentReference, IndexMetadata? index, ItemInventory? inventory, CancellationToken cancellationToken)
    {
        TableDef td = flat.Definition;
        int idxId = this.FindItemIdentityIndex(flat, columnName);
        if (idxId < 0)
        {
            return [];
        }

        int idxFk = FindForeignKeyIndex(td, columnName);
        int idxValue = -1;
        int idxModified = -1;
        var historyValues = new List<int>();
        if (kind == ComplexColumnKind.VersionHistory)
        {
            for (int i = 0; i < td.Columns.Count; i++)
            {
                ColumnInfo column = td.Columns[i];
                if (column.Type == MemoType && i != idxFk && !column.IsAutoNumber)
                {
                    historyValues.Add(i);
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
        await foreach (object?[] row in this.ReadReferenceRowsAsync(flat, columnName, parentReference, index, inventory, cancellationToken).ConfigureAwait(false))
        {
            int id = (int)row[idxId]!;

            int valueIndex = kind == ComplexColumnKind.VersionHistory ? this.FindHistoryValueIndex(flat, row, historyValues) : idxValue;
            if (kind == ComplexColumnKind.VersionHistory && valueIndex < 0)
            {
                // Native DROP of a Memo removes its payload column but retains
                // that column's historical rows, now with every payload null.
                continue;
            }

            result.Add(new MultiValueItem
            {
                Id = id,
                ConceptualTableId = ReadInt32OrZero(row, idxFk),
                Value = valueIndex >= 0 && row[valueIndex] is not (null or DBNull) ? row[valueIndex] : null,
                MemoColumnName = kind == ComplexColumnKind.VersionHistory ? td.Columns[valueIndex].Name : null,
                Modified = idxModified >= 0 && row[idxModified] is DateTime modified ? modified : null,
            });
        }

        return result;
    }

    private int FindHistoryValueIndex(FlatTable flat, object?[] row, List<int> payloadIndexes)
    {
        int selected = -1;
        foreach (int index in payloadIndexes)
        {
            if (index >= row.Length || row[index] is null or DBNull)
            {
                continue;
            }

            if (selected >= 0)
            {
                var error = new JetCorruptDataException(
                    JetErrorCode.CorruptComplexColumn,
                    "A version-history item assigns more than one Memo payload.",
                    new JetErrorInfo { TableName = flat.Name, ColumnName = flat.Definition.Columns[index].Name, PageNumber = flat.TDefPage });
                if (rows.StrictParsing)
                {
                    throw error;
                }

                this.TraceBestEffortFallback(nameof(FindHistoryValueIndex), error);
                return -1;
            }

            selected = index;
        }

        return selected;
    }

    private int FindItemIdentityIndex(FlatTable flat, string columnName)
    {
        int identity = -1;
        for (int index = 0; index < flat.Definition.Columns.Count; index++)
        {
            ColumnInfo column = flat.Definition.Columns[index];
            if (!column.IsAutoNumber)
            {
                continue;
            }

            if (identity >= 0 || column.Type != LongIntegerType)
            {
                identity = -1;
                break;
            }

            identity = index;
        }

        if (identity < 0)
        {
            this.RejectItemIdentity(flat, columnName, "The complex flat table has no unambiguous Long Integer AutoNumber item identity.");
        }

        return identity;
    }

    private bool AcceptItemIdentity(FlatTable flat, object?[] row, int index, ref LongValueVisitedSet seen, HashSet<int> ambiguous, out int identity)
    {
        identity = index < row.Length && row[index] is int value ? value : 0;
        if (identity <= 0)
        {
            this.RejectItemIdentity(flat, flat.Definition.Columns[index].Name, "The complex item has an invalid AutoNumber identity.");
            return false;
        }

        if (!seen.Add((uint)identity))
        {
            ambiguous.Add(identity);
            this.RejectItemIdentity(flat, flat.Definition.Columns[index].Name, "The complex flat table contains duplicate item identities.");
            return false;
        }

        return true;
    }

    private void RejectItemIdentity(FlatTable flat, string columnName, string message)
    {
        var error = new JetCorruptDataException(
            JetErrorCode.CorruptComplexColumn,
            message,
            new JetErrorInfo { TableName = flat.Name, ColumnName = columnName, PageNumber = flat.TDefPage });
        if (rows.StrictParsing)
        {
            throw error;
        }

        this.TraceBestEffortFallback(nameof(AcceptItemIdentity), error);
    }

    private void TraceBestEffortFallback(string operation, Exception exception)
    {
        if (diagnosticsEnabled)
        {
            DiagnosticTrace.WriteLine($"[AccessReader] Best-effort fallback in ComplexColumnReader.{operation}: suppressed {exception.GetType().Name} while reading MSysComplexColumns.");
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

    /// <summary>Verified descriptors tied to their current parent image and identity.</summary>
    /// <param name="ParentRoot">The parent table's root page.</param>
    /// <param name="ParentImage">The immutable parent layout.</param>
    /// <param name="Columns">The verified complex descriptors.</param>
    private sealed record DescriptorSnapshot(long ParentRoot, TDefImage ParentImage, IReadOnlyList<ComplexColumnInfo> Columns);

    private sealed record ColumnSource(FlatTable Flat, ComplexColumnKind Kind, IndexMetadata? Index, ItemInventory? Inventory = null);

    /// <summary>Resolves one parent at a time without retaining another parent's payloads.</summary>
    internal sealed class RowResolver
    {
        private readonly ComplexColumnReader owner;
        private readonly string tableName;
        private readonly IReadOnlyList<ColumnInfo> columns;
        private readonly bool[] selected;
        private readonly ColumnSource?[] sources;
        private readonly bool[] prepared;
        private readonly DiscoveryBudget discovery;
        private readonly AttachmentWrapper.ContentBudget attachmentContent;
        private readonly ComplexItemIdentityBudget identityBudget;
        private readonly Dictionary<long, ItemInventory> inventories = [];
        private IReadOnlyList<ComplexColumnInfo>? complexColumns;

        /// <summary>Initializes a new instance of the <see cref="RowResolver"/> class.</summary>
        /// <param name="owner">The complex reader.</param>
        /// <param name="tableName">The parent table.</param>
        /// <param name="columns">The parent columns.</param>
        /// <param name="wantedColumns">The selected columns.</param>
        /// <param name="maxDiscoveryEntries">The shared fallback discovery allowance.</param>
        /// <param name="attachmentContent">The shared attachment allowance.</param>
        internal RowResolver(ComplexColumnReader owner, string tableName, IReadOnlyList<ColumnInfo> columns, bool[]? wantedColumns, int maxDiscoveryEntries, AttachmentWrapper.ContentBudget attachmentContent)
        {
            this.owner = owner;
            this.tableName = tableName;
            this.columns = columns;
            this.discovery = new DiscoveryBudget(maxDiscoveryEntries);
            this.attachmentContent = attachmentContent;
            this.identityBudget = owner.GetIdentityBudget();
            this.selected = new bool[columns.Count];
            this.sources = new ColumnSource?[columns.Count];
            this.prepared = new bool[columns.Count];
            for (int index = 0; index < columns.Count; index++)
            {
                this.selected[index] = columns[index].Type == ComplexType
                    && (wantedColumns is null || (index < wantedColumns.Length && wantedColumns[index]));
            }
        }

        /// <summary>Substitutes cells only for this typed parent's selected references.</summary>
        /// <param name="row">The decoded parent row.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        internal async ValueTask ResolveAsync(object?[] row, CancellationToken cancellationToken)
        {
            for (int index = 0; index < Math.Min(this.columns.Count, row.Length); index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (this.selected[index])
                {
                    byte[]? cell = row[index] is ComplexIdRef reference && reference.Id > 0
                        ? await this.ResolveCellAsync(index, reference.Id, cancellationToken).ConfigureAwait(false)
                        : null;
                    row[index] = (object?)cell ?? DBNull.Value;
                }
            }
        }

        /// <summary>Validates selected metadata even when the table or its cells are empty.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        internal async ValueTask PrepareAsync(CancellationToken cancellationToken)
        {
            for (int index = 0; index < this.columns.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (this.selected[index])
                {
                    await this.PrepareColumnAsync(index, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>Substitutes data URIs only for this string parent's references.</summary>
        /// <param name="row">The decoded string row.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        internal async ValueTask ResolveAsync(string[] row, CancellationToken cancellationToken)
        {
            const string prefix = "__CX:";
            const string suffix = "__";
            for (int index = 0; index < Math.Min(this.columns.Count, row.Length); index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!this.selected[index])
                {
                    continue;
                }

                string value = row[index] ?? string.Empty;
                bool isReference = value.Length > prefix.Length + suffix.Length
                    && value.StartsWith(prefix, StringComparison.Ordinal)
                    && value.EndsWith(suffix, StringComparison.Ordinal);
                byte[]? cell = isReference && int.TryParse(value.AsSpan(prefix.Length, value.Length - prefix.Length - suffix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int reference) && reference > 0
                    ? await this.ResolveCellAsync(index, reference, cancellationToken).ConfigureAwait(false)
                    : null;
                row[index] = cell is null ? string.Empty : "data:application/octet-stream;base64," + Convert.ToBase64String(cell);
            }
        }

        private async ValueTask<byte[]?> ResolveCellAsync(int columnIndex, int reference, CancellationToken cancellationToken)
        {
            await this.PrepareColumnAsync(columnIndex, cancellationToken).ConfigureAwait(false);
            return this.sources[columnIndex] is ColumnSource retained
                ? await this.owner.LoadCellAsync(retained, this.columns[columnIndex].Name, reference, this.attachmentContent, cancellationToken).ConfigureAwait(false)
                : null;
        }

        private async ValueTask PrepareColumnAsync(int columnIndex, CancellationToken cancellationToken)
        {
            if (!this.prepared[columnIndex])
            {
                this.complexColumns ??= await this.owner.TryGetComplexColumnsAsync(this.tableName, cancellationToken).ConfigureAwait(false);
                ColumnSource? source = await this.owner.PrepareColumnSourceAsync(this.tableName, this.columns[columnIndex].Name, this.complexColumns, this.discovery, cancellationToken).ConfigureAwait(false);
                if (source is not null)
                {
                    if (!this.inventories.TryGetValue(source.Flat.TDefPage, out ItemInventory? inventory))
                    {
                        inventory = await this.owner.ReadItemInventoryAsync(source.Flat, this.columns[columnIndex].Name, this.identityBudget, cancellationToken).ConfigureAwait(false);
                        this.inventories.Add(source.Flat.TDefPage, inventory);
                    }

                    if (FindForeignKeyIndex(source.Flat.Definition, this.columns[columnIndex].Name) != inventory.ParentColumn)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptComplexColumn, "Shared complex storage has inconsistent parent-reference metadata.", new JetErrorInfo { TableName = source.Flat.Name, ColumnName = this.columns[columnIndex].Name, PageNumber = source.Flat.TDefPage });
                    }

                    source = source with { Inventory = inventory };
                }

                this.sources[columnIndex] = source;
                this.prepared[columnIndex] = true;
            }
        }
    }

    /// <summary>A complex column's hidden flat child table.</summary>
    /// <param name="TDefPage">The flat table's TDEF page.</param>
    /// <param name="Definition">The flat table's definition.</param>
    /// <param name="Name">The catalog name, when available.</param>
    private sealed record FlatTable(long TDefPage, TableDef Definition, string Name);
}
