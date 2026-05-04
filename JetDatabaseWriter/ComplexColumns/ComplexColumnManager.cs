namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

#pragma warning disable CA1822 // Mark members as static
#pragma warning disable SA1202
#pragma warning disable SA1204

/// <summary>
/// Owns the Attachment / MultiValue (complex column) subsystem for
/// <see cref="AccessWriter"/>: ACCDB system-table scaffolding
/// (<c>MSysComplexColumns</c>, <c>MSysComplexType_*</c>), per-table
/// allocation of <c>ComplexID</c> / <c>ConceptualTableID</c>, hidden
/// flat-child-table emission, the row-level Add* APIs that backfill
/// flat tables, and cascade / drop / rename plumbing for the artifacts
/// when the parent column or table changes shape. See
/// <c>docs/design/complex-columns-format-notes.md</c>.
/// </summary>
internal sealed class ComplexColumnManager(AccessWriter writer)
{
    private readonly AccessWriter _writer = writer;

    /// <summary>
    /// scaffold mandatory ACCDB system tables (currently
    /// <c>MSysComplexColumns</c> and the per-kind <c>MSysComplexType_*</c>
    /// templates) into a freshly-created database. ACCDB only — complex
    /// columns are an Access 2007+ feature absent from Jet3/Jet4
    /// <c>.mdb</c>. Skipped on the slim 9-column legacy catalog schema
    /// because that mode targets backward-compatible byte hashing and
    /// must not introduce additional pages.
    /// </summary>
    public async ValueTask ScaffoldSystemTablesAsync(DatabaseFormat format, bool fullCatalogSchema, CancellationToken cancellationToken)
    {
        if (format != DatabaseFormat.AceAccdb || !fullCatalogSchema)
        {
            return;
        }

        await CreateMSysComplexColumnsAsync(cancellationToken).ConfigureAwait(false);
        await CreateMSysComplexTypeTemplatesAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// per-kind <c>MSysComplexType_*</c> template tables. Each entry maps
    /// the canonical Access template name to the column schema Access emits for that
    /// template (verified against <c>ComplexFields.accdb</c> in
    /// <c>docs/design/format-probe-appendix-complex.md</c> §<c>MSysComplexType_*</c>).
    /// All templates are zero-row, zero-index tables; their <c>MSysObjects.Id</c>
    /// (= TDEF page) is what <c>MSysComplexColumns.ComplexTypeObjectID</c> points at.
    /// </summary>
    private static readonly (string Name, ColumnDefinition[] Columns)[] _complexTypeTemplates =
    [
        (Constants.ComplexTypeNames.UnsignedByte, new[] { new ColumnDefinition("Value", typeof(byte)) }),
        (Constants.ComplexTypeNames.Short,        [new ColumnDefinition("Value", typeof(short))]),
        (Constants.ComplexTypeNames.Long,         [new ColumnDefinition("Value", typeof(int))]),
        (Constants.ComplexTypeNames.IEEESingle,   [new ColumnDefinition("Value", typeof(float))]),
        (Constants.ComplexTypeNames.IEEEDouble,   [new ColumnDefinition("Value", typeof(double))]),
        (Constants.ComplexTypeNames.GUID,         [new ColumnDefinition("Value", typeof(Guid))]),
        (Constants.ComplexTypeNames.Decimal,      [new ColumnDefinition("Value", typeof(decimal))]),
        (Constants.ComplexTypeNames.Text,         [new ColumnDefinition("Value", typeof(string), maxLength: 255)]),
        (Constants.ComplexTypeNames.Attachment,
        [
            new ColumnDefinition("FileData",      typeof(byte[])),
            new ColumnDefinition("FileFlags",     typeof(int)),
            new ColumnDefinition("FileName",      typeof(string), maxLength: 255),
            new ColumnDefinition("FileTimeStamp", typeof(DateTime)),
            new ColumnDefinition("FileType",      typeof(string), maxLength: 255),
            new ColumnDefinition("FileURL",       typeof(string)),
        ]),
    ];

    /// <summary>
    /// scaffolds the nine <c>MSysComplexType_*</c> template tables
    /// (<c>UnsignedByte</c>, <c>Short</c>, <c>Long</c>, <c>IEEESingle</c>,
    /// <c>IEEEDouble</c>, <c>GUID</c>, <c>Decimal</c>, <c>Text</c>, <c>Attachment</c>)
    /// into a freshly-created ACCDB so subsequent <see cref="EmitComplexColumnArtifactsAsync"/>
    /// calls can populate <c>MSysComplexColumns.ComplexTypeObjectID</c> with a real
    /// catalog id instead of the placeholder <c>0</c>. Each catalog row carries
    /// <c>MSysObjects.Flags = 0x80030000</c> (system + the 0x30000 marker Access uses
    /// for type-template tables) so the templates are excluded from
    /// <c>ListTablesAsync</c>. Schema verified against <c>ComplexFields.accdb</c> —
    /// see <c>docs/design/format-probe-appendix-complex.md</c>.
    /// </summary>
    private async ValueTask CreateMSysComplexTypeTemplatesAsync(CancellationToken cancellationToken)
    {
        foreach ((string name, ColumnDefinition[] cols) in _complexTypeTemplates)
        {
            TableDef tableDef = AccessWriter.BuildTableDefinition(cols, _writer._format);
            (byte[] tdefPage, _) = _writer.BuildTDefPageWithIndexOffsets(tableDef, []);
            long tdefPageNumber = await _writer.AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

            await _writer.InsertCatalogEntryAsync(
                name,
                tdefPageNumber,
                lvProp: null,
                catalogFlags: 0x80030000U,
                cancellationToken).ConfigureAwait(false);
        }

        _writer.InvalidateCatalogCache();
    }

    /// <summary>
    /// Maps a user-declared complex column to the canonical
    /// <c>MSysComplexType_*</c> template name. Returns <see langword="null"/>
    /// when the column is not complex or its element type has no matching template.
    /// </summary>
    private static string? ResolveComplexTypeTemplateName(ColumnDefinition col)
    {
        if (col.IsAttachment)
        {
            return Constants.ComplexTypeNames.Attachment;
        }

        if (!col.IsMultiValue)
        {
            return null;
        }

        Type? t = col.MultiValueElementType;
        if (t is null)
        {
            return null;
        }

        if (t == typeof(byte))
        {
            return Constants.ComplexTypeNames.UnsignedByte;
        }

        if (t == typeof(short))
        {
            return Constants.ComplexTypeNames.Short;
        }

        if (t == typeof(int))
        {
            return Constants.ComplexTypeNames.Long;
        }

        if (t == typeof(float))
        {
            return Constants.ComplexTypeNames.IEEESingle;
        }

        if (t == typeof(double))
        {
            return Constants.ComplexTypeNames.IEEEDouble;
        }

        if (t == typeof(Guid))
        {
            return Constants.ComplexTypeNames.GUID;
        }

        if (t == typeof(decimal))
        {
            return Constants.ComplexTypeNames.Decimal;
        }

        if (t == typeof(string))
        {
            return Constants.ComplexTypeNames.Text;
        }

        return null;
    }

    /// <summary>
    /// Creates the empty <c>MSysComplexColumns</c> system table.
    /// Schema verified against <c>ComplexFields.accdb</c> (see
    /// <c>docs/design/format-probe-appendix-complex.md</c> and
    /// <c>docs/design/complex-columns-format-notes.md</c> §2.2): four
    /// <c>T_LONG</c> columns (<c>ComplexTypeObjectID</c>, <c>FlatTableID</c>,
    /// <c>ConceptualTableID</c>, <c>ComplexID</c>) plus a <c>ColumnName</c>
    /// <c>T_TEXT(510)</c> variable column. The catalog row carries flag
    /// <c>0x80000000</c> (system / hidden) so the table is excluded from
    /// <c>GetUserTablesAsync</c>.
    /// </summary>
    private async ValueTask CreateMSysComplexColumnsAsync(CancellationToken cancellationToken)
    {
        var columns = new[]
        {
            new ColumnDefinition("ColumnName", typeof(string), maxLength: 255),
            new ColumnDefinition("ComplexTypeObjectID", typeof(int)),
            new ColumnDefinition("FlatTableID", typeof(int)),
            new ColumnDefinition("ConceptualTableID", typeof(int)),
            new ColumnDefinition("ComplexID", typeof(int)),
        };

        TableDef tableDef = AccessWriter.BuildTableDefinition(columns, _writer._format);
        (byte[] tdefPage, _) = _writer.BuildTDefPageWithIndexOffsets(tableDef, []);
        long tdefPageNumber = await _writer.AppendPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        await _writer.InsertCatalogEntryAsync(
            Constants.SystemTableNames.ComplexColumns,
            tdefPageNumber,
            lvProp: null,
            catalogFlags: 0x80000000U,
            cancellationToken).ConfigureAwait(false);

        _writer.InvalidateCatalogCache();
    }

    /// <summary>
    /// Per-column scratch state captured by <see cref="PrepareComplexColumnAllocationsAsync"/>
    /// and consumed by <see cref="EmitComplexColumnArtifactsAsync"/>.
    /// </summary>
    internal readonly record struct ComplexColumnAllocation(int ColumnIndex, int ComplexId, int ConceptualTableId);

    /// <summary>
    /// pre-flight for table creation. Walks <paramref name="columns"/> for
    /// user-declared complex columns
    /// (<see cref="ColumnDefinition.IsAttachment"/> / <see cref="ColumnDefinition.IsMultiValue"/>
    /// where <c>ComplexId == 0</c>), validates the format, and allocates a
    /// fresh per-database <c>ComplexID</c> + <c>ConceptualTableID</c> for each.
    /// Returns <see langword="null"/> when no allocation is needed.
    /// </summary>
    public async ValueTask<IReadOnlyList<ComplexColumnAllocation>?> PrepareComplexColumnAllocationsAsync(
        IReadOnlyList<ColumnDefinition> columns,
        CancellationToken cancellationToken)
    {
        List<int>? indices = null;
        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition def = columns[i];
            if ((def.IsAttachment || def.IsMultiValue) && def.ComplexId == 0)
            {
                indices ??= new List<int>(2);
                indices.Add(i);

                if (def.IsMultiValue && def.MultiValueElementType is null)
                {
                    throw new ArgumentException(
                        $"Column '{def.Name}': MultiValue columns require MultiValueElementType to be set.",
                        nameof(columns));
                }
            }
        }

        if (indices is null)
        {
            return null;
        }

        if (_writer._format != DatabaseFormat.AceAccdb)
        {
            throw new NotSupportedException(
                "Attachment and MultiValue columns are an Access 2007+ ACE feature; declare them only on .accdb databases.");
        }

        long msysComplexPg = await _writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysComplexPg == 0)
        {
            throw new NotSupportedException(
                "The database does not contain a 'MSysComplexColumns' table. Create the database via " +
                "AccessWriter.CreateDatabaseAsync (which scaffolds it automatically) before declaring complex columns, " +
                "or open an Access-authored .accdb that already contains the catalog.");
        }

        int nextId = await GetNextComplexIdAsync(msysComplexPg, cancellationToken).ConfigureAwait(false);
        var allocations = new ComplexColumnAllocation[indices.Count];
        for (int i = 0; i < indices.Count; i++)
        {
            int id = nextId++;
            allocations[i] = new ComplexColumnAllocation(indices[i], id, id);
        }

        return allocations;
    }

    /// <summary>
    /// Returns one greater than the largest <c>ComplexID</c> currently stored in
    /// <c>MSysComplexColumns</c>, or <c>1</c> when the table is empty. ConceptualTableIDs
    /// are allocated from the same monotonically-increasing pool to keep the per-database
    /// space simple — Access uses two independent counters but neither value is exposed
    /// through any external API and there is no documented scenario where they must differ.
    /// </summary>
    private async ValueTask<int> GetNextComplexIdAsync(long msysComplexPg, CancellationToken cancellationToken)
    {
        TableDef msysComplex = await _writer.ReadRequiredTableDefAsync(msysComplexPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? idCol = msysComplex.FindColumn("ComplexID");
        ColumnInfo? ctIdCol = msysComplex.FindColumn("ConceptualTableID");

        int maxId = 0;
        long total = _writer._stream.Length / _writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != msysComplexPg)
                {
                    continue;
                }

                foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    if (idCol != null)
                    {
                        string idText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, idCol);
                        if (int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) && v > maxId)
                        {
                            maxId = v;
                        }
                    }

                    if (ctIdCol != null)
                    {
                        string ctText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, ctIdCol);
                        if (int.TryParse(ctText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int cv) && cv > maxId)
                        {
                            maxId = cv;
                        }
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return maxId + 1;
    }

    /// <summary>
    /// post-flight: for each user-declared complex column on the parent
    /// table, build a hidden flat child table per <c>docs/design/complex-columns-format-notes.md</c>
    /// §2.3 / §2.4 and append the corresponding <c>MSysComplexColumns</c> row so
    /// readers can join parent rows to their child values.
    /// </summary>
    /// <remarks>
    /// MVP scope: emits the bare flat-table schema (FK + per-kind value columns) with no
    /// PK, no autoincrement, and no indexes. This is sufficient for round-trip through
    /// this library's reader; Microsoft Access compatibility requires a Compact &amp; Repair
    /// pass to rebuild the missing PK / FK back-reference indexes (validation gap noted in
    /// the design doc §5). <c>ComplexTypeObjectID</c> is set to <c>0</c> because the
    /// <c>MSysComplexType_*</c> template tables are not yet scaffolded; the reader's
    /// classifier falls back to <see cref="ComplexColumnKind.Unknown"/> in that case.
    /// </remarks>
    public async ValueTask EmitComplexColumnArtifactsAsync(
        string parentTableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<ComplexColumnAllocation> allocations,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < allocations.Count; i++)
        {
            ComplexColumnAllocation alloc = allocations[i];
            ColumnDefinition col = columns[alloc.ColumnIndex];

            string flatTableName = BuildFlatTableName(col.Name);
            (ColumnDefinition[] flatCols, IndexDefinition[] flatIndexes) =
                BuildFlatTableSchema(parentTableName, col);

            long flatTdefPage = await _writer.CreateTableInternalAsync(
                flatTableName,
                flatCols,
                indexes: flatIndexes,
                catalogFlags: 0x800A0000U,
                cancellationToken).ConfigureAwait(false);

            // resolve the matching MSysComplexType_* template id so the
            // MSysComplexColumns row points at the canonical type-template table
            // instead of carrying the placeholder 0. Templates are scaffolded by
            // CreateDatabaseAsync and always present in Access-authored files; the
            // lookup only falls back to 0 for slim-catalog ACCDBs
            // (WriteFullCatalogSchema = false), which intentionally skip system
            // tables for byte-hash backward compatibility.
            string? templateName = ResolveComplexTypeTemplateName(col);
            int templateId = templateName is null
                ? 0
                : (int)await _writer.Relationships.FindSystemTableTdefPageAsync(templateName, cancellationToken).ConfigureAwait(false);

            await InsertMSysComplexColumnsRowAsync(
                col.Name,
                complexId: alloc.ComplexId,
                conceptualTableId: alloc.ConceptualTableId,
                flatTableId: (int)flatTdefPage,
                complexTypeObjectId: templateId,
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Generates the canonical hidden-flat-table name <c>f_&lt;32-hex-uppercase&gt;_&lt;userColumnName&gt;</c>
    /// per the design doc §2.3 / format-probe-appendix-complex.md observations.
    /// </summary>
    private static string BuildFlatTableName(string userColumnName)
    {
        // The 32 hex chars are a GUID without dashes — Access uses a fresh GUID per
        // flat table; we do the same so the name is unique even when two columns
        // share a name across tables.
        string guid = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture).ToUpperInvariant();
        return $"f_{guid}_{userColumnName}";
    }

    /// <summary>
    /// Builds the flat-table column list and the system-managed indexes per
    /// the per-kind schemas in the design doc §2.4 / §4.2.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two LONG columns participate in the back-reference plumbing:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><c>_&lt;userColumnName&gt;</c> — FK back-reference holding the parent row's <c>ConceptualTableID</c>.</description></item>
    ///   <item><description><c>&lt;parentTable&gt;_&lt;userColumnName&gt;</c> — autoincrement scalar PK used by Access internally.</description></item>
    /// </list>
    /// <para>
    /// Naming and column ordering match
    /// <c>format-probe-appendix-complex.md</c> for the attachment case
    /// (<c>f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments</c>): the
    /// kind-specific value columns first, then the autoincrement scalar PK,
    /// then the FK back-reference. Three indexes ship with the attachment
    /// table — primary key on the scalar (<c>MSysComplexPKIndex</c>), a
    /// normal index on the FK back-reference (named after the FK column),
    /// and a normal composite index on (FK, FileName) called
    /// <c>IdxFKPrimaryScalar</c> per the appendix.
    /// </para>
    /// <para>
    /// The multi-value variant has no empirical fixture; the conservative
    /// schema mirrors the attachment pattern minus the composite
    /// <c>IdxFKPrimaryScalar</c> (the value column may be a non-indexable
    /// type such as MEMO).
    /// </para>
    /// </remarks>
    private static (ColumnDefinition[] Columns, IndexDefinition[] Indexes) BuildFlatTableSchema(
        string parentTableName,
        ColumnDefinition parentColumn)
    {
        string fkName = $"_{parentColumn.Name}";
        string scalarName = $"{parentTableName}_{parentColumn.Name}";
        var fk = new ColumnDefinition(fkName, typeof(int));
        var scalar = new ColumnDefinition(scalarName, typeof(int)) { IsAutoIncrement = true };

        if (parentColumn.IsAttachment)
        {
            ColumnDefinition[] cols =
            [
                new ColumnDefinition("FileData", typeof(byte[])),
                new ColumnDefinition("FileFlags", typeof(int)),
                new ColumnDefinition("FileName", typeof(string), maxLength: 255),
                new ColumnDefinition("FileTimeStamp", typeof(DateTime)),
                new ColumnDefinition("FileType", typeof(string), maxLength: 255),
                new ColumnDefinition("FileURL", typeof(string)) /* MEMO via no maxLength */,
                scalar,
                fk,
            ];

            IndexDefinition[] indexes =
            [
                new IndexDefinition("MSysComplexPKIndex", scalarName) { IsPrimaryKey = true },
                new IndexDefinition(fkName, fkName),
                new IndexDefinition("IdxFKPrimaryScalar", [fkName, "FileName"]),
            ];

            return (cols, indexes);
        }

        // MultiValue: a single `value` column whose CLR type is the user-declared element type.
        Type elementType = parentColumn.MultiValueElementType
            ?? throw new InvalidOperationException("MultiValueElementType must be set on a multi-value column.");
        var valueCol = new ColumnDefinition("value", elementType, maxLength: parentColumn.MaxLength);
        ColumnDefinition[] mvCols = [valueCol, scalar, fk];
        IndexDefinition[] mvIndexes =
        [
            new IndexDefinition("MSysComplexPKIndex", scalarName) { IsPrimaryKey = true },
            new IndexDefinition(fkName, fkName),
        ];
        return (mvCols, mvIndexes);
    }

    /// <summary>
    /// Inserts one row into <c>MSysComplexColumns</c> linking a parent column's
    /// <see cref="ComplexColumnAllocation.ComplexId"/> to its hidden flat-table TDEF
    /// page. Schema verified in <c>format-probe-appendix-complex.md</c>.
    /// </summary>
    private async ValueTask InsertMSysComplexColumnsRowAsync(
        string parentColumnName,
        int complexId,
        int conceptualTableId,
        int flatTableId,
        int complexTypeObjectId,
        CancellationToken cancellationToken)
    {
        long pg = await _writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (pg == 0)
        {
            throw new InvalidOperationException("MSysComplexColumns table is missing.");
        }

        TableDef msysComplex = await _writer.ReadRequiredTableDefAsync(pg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        object[] values = msysComplex.CreateNullValueRow();

        msysComplex.SetValueByName(values, "ColumnName", parentColumnName);
        msysComplex.SetValueByName(values, "ComplexTypeObjectID", complexTypeObjectId);
        msysComplex.SetValueByName(values, "FlatTableID", flatTableId);
        msysComplex.SetValueByName(values, "ConceptualTableID", conceptualTableId);
        msysComplex.SetValueByName(values, "ComplexID", complexId);

        await _writer.InsertSystemRowAndMaintainAsync(pg, msysComplex, Constants.SystemTableNames.ComplexColumns, values, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    // ── Row-level APIs for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §2.1 / §2.4 / §3.

    public async ValueTask AddComplexItemCoreAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object> parentRowKey,
        object payload,
        bool expectAttachment,
        CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        Guard.ThrowIfDisposed(_writer._disposed, _writer);
        cancellationToken.ThrowIfCancellationRequested();

        if (parentRowKey.Count == 0)
        {
            throw new ArgumentException("At least one key column is required.", nameof(parentRowKey));
        }

        if (_writer._format != DatabaseFormat.AceAccdb)
        {
            throw new NotSupportedException(
                "Complex (Attachment / MultiValue) columns are an Access 2007+ ACE feature; only .accdb databases are supported.");
        }

        // Resolve parent table + complex column.
        CatalogEntry parentEntry = await _writer.GetRequiredCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        TableDef parentDef = await _writer.ReadRequiredTableDefAsync(parentEntry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        ColumnInfo complexCol = parentDef.FindColumn(columnName)
            ?? throw new ArgumentException($"Column '{columnName}' was not found in table '{tableName}'.", nameof(columnName));

        bool isAttachmentCol = complexCol.Type == T_ATTACHMENT;
        bool isMultiValueCol = complexCol.Type == T_COMPLEX;
        if (!isAttachmentCol && !isMultiValueCol)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is not a complex (Attachment / MultiValue) column (type=0x{complexCol.Type:X2}).");
        }

        if (expectAttachment && !isAttachmentCol)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is a MultiValue column; call AddMultiValueItemAsync instead.");
        }

        if (!expectAttachment && !isMultiValueCol)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is an Attachment column; call AddAttachmentAsync instead.");
        }

        // Resolve the hidden flat child table via MSysComplexColumns.
        long flatTdefPage = await ResolveFlatTableTdefPageAsync(columnName, complexCol.Misc, cancellationToken).ConfigureAwait(false);
        if (flatTdefPage <= 0)
        {
            throw new InvalidOperationException(
                $"No MSysComplexColumns row was found for column '{tableName}.{columnName}'.");
        }

        TableDef flatDef = await _writer.ReadRequiredTableDefAsync(flatTdefPage, "<flat>", cancellationToken).ConfigureAwait(false);

        // Resolve predicate column ordinals + decode parent key (string-form for comparison).
        var predIndexes = new int[parentRowKey.Count];
        var predValues = new string[parentRowKey.Count];
        int pi = 0;
        foreach (KeyValuePair<string, object> kvp in parentRowKey)
        {
            int idx = parentDef.FindColumnIndex(kvp.Key);
            if (idx < 0)
            {
                throw new ArgumentException($"Column '{kvp.Key}' was not found in table '{tableName}'.", nameof(parentRowKey));
            }

            predIndexes[pi] = idx;
            predValues[pi] = kvp.Value is null or DBNull
                ? string.Empty
                : Convert.ToString(kvp.Value, CultureInfo.InvariantCulture) ?? string.Empty;
            pi++;
        }

        // Locate the unique parent row.
        (long parentPageNumber, int parentRowIndex, int parentRowStart, int parentRowSize) =
            await FindUniqueParentRowAsync(parentEntry.TDefPage, parentDef, predIndexes, predValues, tableName, cancellationToken)
                .ConfigureAwait(false);

        // Read the existing ConceptualTableID from the parent row's complex slot;
        // allocate a fresh one when the slot is null.
        int conceptualTableId = await ReadOrAllocateConceptualTableIdAsync(
            parentPageNumber,
            parentRowStart,
            parentRowSize,
            parentRowIndex,
            complexCol,
            flatTdefPage,
            flatDef,
            cancellationToken).ConfigureAwait(false);

        // Build the flat-table row values.
        object[] flatValues = expectAttachment
            ? BuildAttachmentFlatRow(flatDef, conceptualTableId, (AttachmentInput)payload)
            : BuildMultiValueFlatRow(flatDef, conceptualTableId, payload);

        // The flat table carries an autoincrement scalar PK column.
        // ApplyConstraintsAsync hydrates the constraint registry from the
        // persisted FLAG_AUTO_LONG bit and seeds the next value from the
        // existing rows so AddAttachmentAsync / AddMultiValueItemAsync stay
        // a single-call surface.
        string flatTableName = await ResolveFlatTableNameAsync(flatTdefPage, cancellationToken).ConfigureAwait(false);
        await _writer.ApplyConstraintsForComplexAsync(flatTableName, flatDef, flatValues, cancellationToken).ConfigureAwait(false);

        await _writer.InsertRowDataAsync(flatTdefPage, flatDef, flatValues, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves a hidden flat-child-table TDEF page back to its
    /// <c>MSysObjects.Name</c>. Used by <see cref="AddComplexItemCoreAsync"/>
    /// to drive <c>ApplyConstraintsAsync</c> for the autoincrement
    /// scalar PK column emitted by the complex-column scaffold.
    /// </summary>
    private async ValueTask<string> ResolveFlatTableNameAsync(long flatTdefPage, CancellationToken cancellationToken)
    {
        TableDef? msys = await _writer.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            throw new InvalidOperationException("MSysObjects catalog table is missing.");
        }

        List<CatalogRow> rows = await _writer.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage == flatTdefPage)
            {
                return row.Name;
            }
        }

        throw new InvalidOperationException(
            $"No MSysObjects row was found for flat-child TDEF page {flatTdefPage}.");
    }

    /// <summary>
    /// Looks up <c>MSysComplexColumns</c> for a row matching both
    /// <paramref name="columnName"/> and <paramref name="complexId"/> and returns
    /// the lower-24-bit TDEF page number of the hidden flat child table.
    /// </summary>
    private async ValueTask<long> ResolveFlatTableTdefPageAsync(string columnName, int complexId, CancellationToken cancellationToken)
    {
        long msysPg = await _writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysPg == 0)
        {
            return 0;
        }

        TableDef msys = await _writer.ReadRequiredTableDefAsync(msysPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msys.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msys.FindColumn("FlatTableID");
        ColumnInfo? complexIdCol = msys.FindColumn("ComplexID");
        if (nameCol == null || flatIdCol == null || complexIdCol == null)
        {
            return 0;
        }

        long total = _writer._stream.Length / _writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != msysPg)
                {
                    continue;
                }

                foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, complexIdCol);
                    if (complexId != 0 && (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId))
                    {
                        continue;
                    }

                    string flatText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
                    if (long.TryParse(flatText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long flatId))
                    {
                        return flatId & 0x00FFFFFFL;
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return 0;
    }

    private async ValueTask<(long PageNumber, int RowIndex, int RowStart, int RowSize)> FindUniqueParentRowAsync(
        long parentTdefPage,
        TableDef parentDef,
        int[] predIndexes,
        string[] predValues,
        string tableName,
        CancellationToken cancellationToken)
    {
        (long, int, int, int) match = default;
        bool found = false;

        long total = _writer._stream.Length / _writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != parentTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    bool ok = true;
                    for (int p = 0; p < predIndexes.Length; p++)
                    {
                        ColumnInfo c = parentDef.Columns[predIndexes[p]];
                        string actual = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, c);
                        if (!string.Equals(actual, predValues[p], StringComparison.OrdinalIgnoreCase))
                        {
                            ok = false;
                            break;
                        }
                    }

                    if (!ok)
                    {
                        continue;
                    }

                    if (found)
                    {
                        throw new InvalidOperationException(
                            $"Parent row key matches more than one row in '{tableName}'.");
                    }

                    match = (row.PageNumber, row.RowIndex, row.RowStart, row.RowSize);
                    found = true;
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        if (!found)
        {
            throw new InvalidOperationException($"No row in '{tableName}' matches the supplied parent row key.");
        }

        return match;
    }

    private async ValueTask<int> ReadOrAllocateConceptualTableIdAsync(
        long parentPageNumber,
        int parentRowStart,
        int parentRowSize,
        int parentRowIndex,
        ColumnInfo complexCol,
        long flatTdefPage,
        TableDef flatDef,
        CancellationToken cancellationToken)
    {
        // Re-read parent page to inspect the complex slot null bit + 4 bytes.
        byte[] page = await _writer.ReadPageAsync(parentPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            int numCols = _writer._format != DatabaseFormat.Jet3Mdb ? AccessBase.Ru16(page, parentRowStart) : page[parentRowStart];
            int nullMaskSz = (numCols + 7) / 8;
            int nullMaskPos = parentRowSize - nullMaskSz;
            int byteOff = nullMaskPos + (complexCol.ColNum / 8);
            int bitOff = complexCol.ColNum % 8;
            bool slotSet = byteOff < parentRowSize && (page[parentRowStart + byteOff] & (1 << bitOff)) != 0;

            int slotOff = parentRowStart + _writer._rowSz.NumCols + complexCol.FixedOff;
            if (slotSet && slotOff + 4 <= parentRowStart + parentRowSize)
            {
                int existing = AccessBase.Ri32(page, slotOff);
                if (existing > 0)
                {
                    return existing;
                }
            }
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }

        // Allocate a fresh ConceptualTableID by scanning the flat table for max(FK)+1.
        int allocated = await GetNextConceptualTableIdForFlatAsync(flatTdefPage, flatDef, cancellationToken).ConfigureAwait(false);

        // Patch the parent row's complex slot in place: 4 bytes + null-mask bit.
        await PatchParentComplexSlotAsync(parentPageNumber, parentRowStart, parentRowSize, complexCol, allocated, cancellationToken).ConfigureAwait(false);
        return allocated;
    }

    private async ValueTask PatchParentComplexSlotAsync(
        long pageNumber,
        int rowStart,
        int rowSize,
        ColumnInfo complexCol,
        int conceptualTableId,
        CancellationToken cancellationToken)
    {
        byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            int numCols = _writer._format != DatabaseFormat.Jet3Mdb ? AccessBase.Ru16(page, rowStart) : page[rowStart];
            int nullMaskSz = (numCols + 7) / 8;
            int nullMaskPos = rowSize - nullMaskSz;
            int slotOff = rowStart + _writer._rowSz.NumCols + complexCol.FixedOff;
            if (slotOff + 4 > rowStart + rowSize)
            {
                throw new InvalidDataException("Complex column slot is out of row bounds.");
            }

            AccessBase.Wi32(page, slotOff, conceptualTableId);
            int byteOff = nullMaskPos + (complexCol.ColNum / 8);
            int bitOff = complexCol.ColNum % 8;
            if (byteOff < rowSize)
            {
                page[rowStart + byteOff] |= (byte)(1 << bitOff);
            }

            await _writer.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }
    }

    /// <summary>
    /// Returns one greater than the largest FK value currently stored in the
    /// flat table, or <c>1</c> when the table is empty. The FK column is the
    /// single <c>T_LONG</c> column whose name starts with <c>"_"</c> per
    /// <c>BuildFlatTableSchema</c>.
    /// </summary>
    private async ValueTask<int> GetNextConceptualTableIdForFlatAsync(long flatTdefPage, TableDef flatDef, CancellationToken cancellationToken)
    {
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();

        int maxId = 0;
        long total = _writer._stream.Length / _writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != flatTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string text = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, fkCol);
                    if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) && v > maxId)
                    {
                        maxId = v;
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return maxId + 1;
    }

    private static object[] BuildAttachmentFlatRow(TableDef flatDef, int conceptualTableId, AttachmentInput input)
    {
        object[] values = flatDef.CreateNullValueRow();

        // FK back-ref: the single T_LONG column starting with "_".
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();
        values[flatDef.Columns.IndexOf(fkCol)] = conceptualTableId;

        string ext = input.FileType ?? DeriveExtension(input.FileName);
        byte[] wrapped = AttachmentWrapper.Encode(ext, input.FileData);

        flatDef.SetValueByName(values, "FileURL", (object?)input.FileURL ?? DBNull.Value);
        flatDef.SetValueByName(values, "FileName", input.FileName);
        flatDef.SetValueByName(values, "FileType", ext);
        flatDef.SetValueByName(values, "FileFlags", DBNull.Value);
        flatDef.SetValueByName(values, "FileTimeStamp", input.FileTimeStamp ?? DateTime.UtcNow);
        flatDef.SetValueByName(values, "FileData", wrapped);
        return values;
    }

    private static object[] BuildMultiValueFlatRow(TableDef flatDef, int conceptualTableId, object value)
    {
        object[] values = flatDef.CreateNullValueRow();
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();
        values[flatDef.Columns.IndexOf(fkCol)] = conceptualTableId;
        flatDef.SetValueByName(values, "value", value ?? DBNull.Value);
        return values;
    }

    [SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase", Justification = "Attachment FileType is intentionally stored in lowercase to match the existing attachment contract and Access conventions.")]
    private static string DeriveExtension(string fileName)
    {
        if (string.IsNullOrEmpty(fileName))
        {
            return string.Empty;
        }

        int dot = fileName.LastIndexOf('.');
        if (dot < 0 || dot == fileName.Length - 1)
        {
            return string.Empty;
        }

        return fileName.Substring(dot + 1).ToLowerInvariant();
    }

    // ── Cascade-on-delete for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §4.3.
    //
    // Whenever a parent row containing a complex column slot is deleted, the
    // associated rows in the hidden flat child table (joined via the parent's
    // 4-byte ConceptualTableID slot) must also be deleted. Without this pass
    // the flat table accumulates orphaned rows, breaks referential integrity
    // expected by Microsoft Access, and may cause Compact &amp; Repair to flag
    // the file.

    /// <summary>
    /// Cascades a pending delete of <paramref name="deletedParentLocations"/>
    /// rows in <paramref name="parentDef"/> to the hidden flat child tables
    /// of every Attachment / MultiValue column on the parent. Must be called
    /// BEFORE the parent rows are marked deleted, since the per-row
    /// <c>ConceptualTableID</c> slot value is needed to identify which flat
    /// rows to delete.
    /// </summary>
    /// <remarks>
    /// Per-flat-table cost is O(P) where P is the database page count
    /// (full sequential scan, no index seek). Multiple complex columns on
    /// the same parent perform one scan each. This matches the existing
    /// cascade-delete cost profile and the ConceptualTableID allocator
    /// used by the row-add path.
    /// </remarks>
    public async ValueTask CascadeDeleteComplexChildrenAsync(
        TableDef parentDef,
        List<RowLocation> deletedParentLocations,
        CancellationToken cancellationToken)
    {
        if (deletedParentLocations.Count == 0)
        {
            return;
        }

        // Identify complex columns on the parent.
        var complexCols = new List<ColumnInfo>();
        foreach (ColumnInfo col in parentDef.Columns)
        {
            if (col.Type == T_ATTACHMENT || col.Type == T_COMPLEX)
            {
                complexCols.Add(col);
            }
        }

        if (complexCols.Count == 0)
        {
            return;
        }

        // Resolve each complex column to its flat-table TDEF page (skip
        // any column whose MSysComplexColumns row is missing — same
        // tolerance as the row-add path).
        var flatPagesByCol = new Dictionary<int, long>(complexCols.Count);
        foreach (ColumnInfo col in complexCols)
        {
            long flatPg = await ResolveFlatTableTdefPageAsync(col.Name, col.Misc, cancellationToken).ConfigureAwait(false);
            if (flatPg > 0)
            {
                flatPagesByCol[col.ColNum] = flatPg;
            }
        }

        if (flatPagesByCol.Count == 0)
        {
            return;
        }

        // Read each parent row to collect the live ConceptualTableID per
        // complex column. Rows whose complex slot is null contribute
        // nothing to cascade.
        var idsByCol = new Dictionary<int, HashSet<int>>(complexCols.Count);
        foreach (ColumnInfo col in complexCols)
        {
            if (flatPagesByCol.ContainsKey(col.ColNum))
            {
                idsByCol[col.ColNum] = [];
            }
        }

        foreach (RowLocation loc in deletedParentLocations)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(loc.PageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                int numCols = _writer._format != DatabaseFormat.Jet3Mdb ? AccessBase.Ru16(page, loc.RowStart) : page[loc.RowStart];
                int nullMaskSz = (numCols + 7) / 8;
                int nullMaskPos = loc.RowSize - nullMaskSz;

                foreach (ColumnInfo col in complexCols)
                {
                    if (!idsByCol.TryGetValue(col.ColNum, out HashSet<int>? ids))
                    {
                        continue;
                    }

                    int byteOff = nullMaskPos + (col.ColNum / 8);
                    int bitOff = col.ColNum % 8;
                    bool slotSet = byteOff < loc.RowSize
                        && (page[loc.RowStart + byteOff] & (1 << bitOff)) != 0;
                    if (!slotSet)
                    {
                        continue;
                    }

                    int slotOff = loc.RowStart + _writer._rowSz.NumCols + col.FixedOff;
                    if (slotOff + 4 > loc.RowStart + loc.RowSize)
                    {
                        continue;
                    }

                    int ctid = AccessBase.Ri32(page, slotOff);
                    if (ctid > 0)
                    {
                        _ = ids.Add(ctid);
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        // For each complex column with collected IDs, scan the flat
        // child table once and delete every row whose FK back-reference
        // is in the set. Adjust the flat TDEF row count once.
        foreach (ColumnInfo col in complexCols)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!flatPagesByCol.TryGetValue(col.ColNum, out long flatTdefPage))
            {
                continue;
            }

            HashSet<int> ids = idsByCol[col.ColNum];
            if (ids.Count == 0)
            {
                continue;
            }

            TableDef flatDef = await _writer.ReadRequiredTableDefAsync(flatTdefPage, "<flat>", cancellationToken).ConfigureAwait(false);
            ColumnInfo? fkCol = flatDef.Columns.Find(c => c.Type == T_LONG && c.Name.StartsWith('_'))
                ?? flatDef.Columns.Find(c => c.Type == T_LONG);
            if (fkCol == null)
            {
                continue;
            }

            int deletedFromFlat = 0;
            long total = _writer._stream.Length / _writer._pgSz;
            for (long pageNumber = 3; pageNumber < total; pageNumber++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var rowsToDelete = new List<int>();
                byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (page[0] != 0x01)
                    {
                        continue;
                    }

                    if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != flatTdefPage)
                    {
                        continue;
                    }

                    foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                    {
                        string fkText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, fkCol);
                        if (int.TryParse(fkText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int fk)
                            && ids.Contains(fk))
                        {
                            rowsToDelete.Add(row.RowIndex);
                        }
                    }
                }
                finally
                {
                    AccessBase.ReturnPage(page);
                }

                foreach (int rowIdx in rowsToDelete)
                {
                    await _writer.MarkRowDeletedAsync(pageNumber, rowIdx, cancellationToken).ConfigureAwait(false);
                    deletedFromFlat++;
                }
            }

            if (deletedFromFlat > 0)
            {
                await _writer.AdjustTDefRowCountAsync(flatTdefPage, -deletedFromFlat, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// surgically drops a single complex column's flat child table
    /// and its <c>MSysComplexColumns</c> row, identified by
    /// <paramref name="columnName"/> + <paramref name="complexId"/>. Used by the
    /// rewrite path when the user calls <c>DropColumnAsync</c> on an
    /// attachment / multi-value column. Returns silently if no matching row is
    /// found (idempotent).
    /// </summary>
    public async ValueTask DropSingleComplexChildAsync(string columnName, int complexId, CancellationToken cancellationToken)
    {
        long msysCxPg = await _writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await _writer.ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msysCxDef.FindColumn("FlatTableID");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (nameCol == null || flatIdCol == null || cxIdCol == null)
        {
            return;
        }

        long flatTdefPage = 0;
        var deletedRows = new List<(long PageNumber, int RowIndex)>();

        long total = _writer._stream.Length / _writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != msysCxPg)
                {
                    continue;
                }

                foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId)
                    {
                        continue;
                    }

                    string flatText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
                    if (long.TryParse(flatText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long fid))
                    {
                        flatTdefPage = fid & 0x00FFFFFFL;
                    }

                    deletedRows.Add((row.PageNumber, row.RowIndex));
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        foreach ((long pg, int ri) in deletedRows)
        {
            await _writer.MarkRowDeletedAsync(pg, ri, cancellationToken).ConfigureAwait(false);
        }

        if (deletedRows.Count > 0)
        {
            await _writer.AdjustTDefRowCountAsync(msysCxPg, -deletedRows.Count, cancellationToken).ConfigureAwait(false);
        }

        if (flatTdefPage <= 0)
        {
            return;
        }

        // Drop the hidden flat-table catalog row. Same model as
        // DropComplexChildrenForTableAsync — orphaned data pages are reclaimed
        // by Access on the next Compact &amp; Repair pass.
        TableDef? msys = await _writer.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return;
        }

        List<CatalogRow> catalog = await _writer.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in catalog)
        {
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if (row.TDefPage == flatTdefPage)
            {
                await _writer.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// when the user renames a complex column, rewrite the
    /// matching <c>MSysComplexColumns</c> row's <c>ColumnName</c> field. The
    /// hidden flat child table's catalog name (<c>f_&lt;hex&gt;_&lt;oldName&gt;</c>)
    /// is left unchanged because it is opaque to readers — they resolve the
    /// flat name via <c>FlatTableID</c> → <c>MSysObjects</c>. This mirrors the
    /// <c>RenameRelationshipAsync</c> trade-off that leaves TDEF logical-idx
    /// name cookies stale until Compact &amp; Repair.
    /// </summary>
    public async ValueTask RenameComplexColumnArtifactsAsync(string oldColumnName, string newColumnName, int complexId, CancellationToken cancellationToken)
    {
        long msysCxPg = await _writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await _writer.ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (nameCol == null || cxIdCol == null)
        {
            return;
        }

        var matched = new List<(long PageNumber, int RowIndex, object[] Values)>();
        long total = _writer._stream.Length / _writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != msysCxPg)
                {
                    continue;
                }

                foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.Equals(rowName, oldColumnName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    string idText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid) || rid != complexId)
                    {
                        continue;
                    }

                    var values = new object[msysCxDef.Columns.Count];
                    for (int i = 0; i < values.Length; i++)
                    {
                        string text = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, msysCxDef.Columns[i]);
                        values[i] = string.IsNullOrEmpty(text) ? DBNull.Value : text;
                    }

                    msysCxDef.SetValueByName(values, "ColumnName", newColumnName);
                    matched.Add((row.PageNumber, row.RowIndex, values));
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        foreach ((long pg, int ri, object[] _) in matched)
        {
            await _writer.MarkRowDeletedAsync(pg, ri, cancellationToken).ConfigureAwait(false);
        }

        foreach ((long _, int _, object[] values) in matched)
        {
            _ = await _writer.InsertRowDataLocAsync(msysCxPg, msysCxDef, values, updateTDefRowCount: false, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// when dropping a parent table, also drop the hidden flat
    /// child tables backing each Attachment / MultiValue column on the
    /// parent and remove the corresponding rows from
    /// <c>MSysComplexColumns</c>. Tolerates missing
    /// <c>MSysComplexColumns</c> (Jet3 / Jet4 / fresh writer-created
    /// ACCDB without the system table) and missing catalog rows for a
    /// flat table (already removed) by silently skipping.
    /// </summary>
    public async ValueTask DropComplexChildrenForTableAsync(long parentTdefPage, CancellationToken cancellationToken)
    {
        TableDef? parentDef = await _writer.ReadTableDefAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
        if (parentDef == null)
        {
            return;
        }

        var complexCols = new List<ColumnInfo>();
        foreach (ColumnInfo col in parentDef.Columns)
        {
            if (col.Type == T_ATTACHMENT || col.Type == T_COMPLEX)
            {
                complexCols.Add(col);
            }
        }

        if (complexCols.Count == 0)
        {
            return;
        }

        long msysCxPg = await _writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await _writer.ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msysCxDef.FindColumn("FlatTableID");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (nameCol == null || flatIdCol == null || cxIdCol == null)
        {
            return;
        }

        var lookup = new Dictionary<string, HashSet<int>>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnInfo col in complexCols)
        {
            if (!lookup.TryGetValue(col.Name, out HashSet<int>? ids))
            {
                ids = [];
                lookup[col.Name] = ids;
            }

            _ = ids.Add(col.Misc);
        }

        var flatTdefPages = new HashSet<long>();
        var cxRowsToDelete = new List<(long PageNumber, int RowIndex)>();

        long total = _writer._stream.Length / _writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await _writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != 0x01)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, _writer._dataPage.TDefOff) != msysCxPg)
                {
                    continue;
                }

                foreach (RowLocation row in _writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string rowName = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    string idText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, cxIdCol);
                    if (!int.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out int rid))
                    {
                        continue;
                    }

                    if (!lookup.TryGetValue(rowName, out HashSet<int>? expectedIds) || !expectedIds.Contains(rid))
                    {
                        continue;
                    }

                    string flatText = _writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, flatIdCol);
                    if (long.TryParse(flatText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long flatId))
                    {
                        _ = flatTdefPages.Add(flatId & 0x00FFFFFFL);
                    }

                    cxRowsToDelete.Add((row.PageNumber, row.RowIndex));
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        foreach ((long pg, int ri) in cxRowsToDelete)
        {
            await _writer.MarkRowDeletedAsync(pg, ri, cancellationToken).ConfigureAwait(false);
        }

        if (cxRowsToDelete.Count > 0)
        {
            await _writer.AdjustTDefRowCountAsync(msysCxPg, -cxRowsToDelete.Count, cancellationToken).ConfigureAwait(false);
        }

        if (flatTdefPages.Count == 0)
        {
            return;
        }

        // Drop the hidden flat-table catalog rows (system-flag tables —
        // public DropTableAsync would skip them).
        TableDef? msys = await _writer.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return;
        }

        List<CatalogRow> catalog = await _writer.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in catalog)
        {
            if (row.ObjectType != Constants.SystemObjects.UserTableType)
            {
                continue;
            }

            if (flatTdefPages.Contains(row.TDefPage))
            {
                await _writer.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
