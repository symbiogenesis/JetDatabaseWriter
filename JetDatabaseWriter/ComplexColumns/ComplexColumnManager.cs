namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

#pragma warning disable SA1204

/// <summary>
/// Owns the Attachment / MultiValue (complex column) subsystem for
/// <see cref="AccessWriter"/>: ACCDB system-table scaffolding
/// (<c>MSysACEs</c>, <c>MSysQueries</c>, <c>MSysRelationships</c>,
/// <c>MSysComplexColumns</c>, <c>MSysComplexType_*</c>), per-table
/// allocation of per-column <c>ComplexID</c> values and per-row complex references, hidden
/// flat-child-table emission, the row-level Add* APIs that backfill
/// flat tables, and cascade / drop / rename plumbing for the artifacts
/// when the parent column or table changes shape. See
/// <see href="docs/design/complex-columns-format-notes.md" />.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="ownedPages">The database's owned-page discovery and row walks.</param>
/// <param name="pager">The writer's page file, through which the header page and patched parent rows are written.</param>
/// <param name="catalog">Resolves parent tables by name.</param>
/// <param name="tableRows">Writes and tombstones flat-table and <c>MSysComplexColumns</c> rows.</param>
/// <param name="indexes">Keeps system-table indexes current.</param>
/// <param name="catalogArtifacts">Creates the hidden flat child tables and system-table templates.</param>
/// <param name="catalogRows">Scans <c>MSysObjects</c> rows and locates system tables.</param>
/// <param name="constraints">Applies flat-table column constraints on row-level inserts.</param>
/// <param name="autoNumbers">Advances a flat table's persisted AutoNumber high-water value after a row-level insert, a parent table's complex AutoNumber after a reference is allocated, and <c>MSysComplexColumns</c>' ComplexID counter.</param>
/// <param name="seeds">Resolves flat tables and reads the per-row complex references a parent table already uses.</param>
/// <param name="snapshots">Streams strict typed projected parent rows.</param>
internal sealed class ComplexColumnManager(
    JetFormat format,
    TableDefReader tableDefs,
    OwnedDataPages ownedPages,
    Pager pager,
    TableCatalog catalog,
    TableRowStore tableRows,
    IndexMaintainer indexes,
    CatalogArtifactWriter catalogArtifacts,
    CatalogRowReader catalogRows,
    ConstraintRegistry constraints,
    AutoNumberMaintainer autoNumbers,
    ComplexReferenceSeedReader seeds,
    TableSnapshotReader snapshots)
{
    private const int ComplexTypeTemplateTextLength = 255;

    private readonly JetFormat format = format;
    private readonly TableDefReader tableDefs = tableDefs;
    private readonly OwnedDataPages ownedPages = ownedPages;
    private readonly Pager pager = pager;
    private readonly TableCatalog catalog = catalog;

    /// <summary>
    /// Scaffolds mandatory ACCDB system tables: the core
    /// <c>MSysACEs</c>, <c>MSysQueries</c>, and <c>MSysRelationships</c>
    /// tables, plus <c>MSysComplexColumns</c> and the per-kind
    /// <c>MSysComplexType_*</c> templates. ACCDB only
    /// (<see cref="JetFormat.SupportsComplexColumns"/>) — Jet3/Jet4
    /// <c>.mdb</c> scaffolds skip these tables.
    /// </summary>
    /// <param name="coreSystemTableStartPage">The core system table start page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask ScaffoldSystemTablesAsync(long coreSystemTableStartPage, CancellationToken cancellationToken)
    {
        if (!this.format.SupportsComplexColumns)
        {
            return;
        }

        await this.CreateCoreSystemTablesAsync(coreSystemTableStartPage, cancellationToken).ConfigureAwait(false);
        await this.CreateMSysComplexColumnsAsync(cancellationToken).ConfigureAwait(false);
        await this.CreateMSysComplexTypeTemplatesAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask CreateCoreSystemTablesAsync(long coreSystemTableStartPage, CancellationToken cancellationToken)
    {
        const uint systemTableFlags = Constants.SystemObjects.SystemTableMask & Constants.SystemObjects.SystemObjectFlag;
        var plan = new CatalogArtifactPlan(
            [
                new CatalogTableArtifact(
                    Constants.SystemTableNames.Aces,
                    [
                        new ColumnDefinition("ObjectId", typeof(int)),
                        new ColumnDefinition("SID", typeof(byte[]), maxLength: 255),
                        new ColumnDefinition("ACM", typeof(int)),
                        new ColumnDefinition("FInheritable", typeof(bool)),
                    ],
                    [new IndexDefinition("ObjectId", "ObjectId") { IsRequired = true }],
                    systemTableFlags,
                    ReservedTdefPageNumber: coreSystemTableStartPage,
                    EmitLvProp: false),
                new CatalogTableArtifact(
                    Constants.SystemTableNames.Queries,
                    [
                        new ColumnDefinition("ObjectId", typeof(int)),
                        new ColumnDefinition("Attribute", typeof(byte)),
                        new ColumnDefinition("Order", typeof(byte[]), maxLength: 255),
                        new ColumnDefinition("Name1", typeof(string), maxLength: 255),
                        new ColumnDefinition("Name2", typeof(string), maxLength: 255),
                        new ColumnDefinition("Expression", typeof(string)),
                        new ColumnDefinition("Flag", typeof(short)),
                        new ColumnDefinition("LvExtra", typeof(int)),
                    ],
                    [new IndexDefinition("ObjectIdAttribute", ["ObjectId", "Attribute", "Order"]) { IsPrimaryKey = true }],
                    systemTableFlags,
                    ReservedTdefPageNumber: coreSystemTableStartPage > 0 ? coreSystemTableStartPage + 1 : 0,
                    EmitLvProp: false),
                new CatalogTableArtifact(
                    Constants.SystemTableNames.Relationships,
                    [
                        new ColumnDefinition("szRelationship", typeof(string), maxLength: 255),
                        new ColumnDefinition("grbit", typeof(int)),
                        new ColumnDefinition("ccolumn", typeof(int)),
                        new ColumnDefinition("icolumn", typeof(int)),
                        new ColumnDefinition("szObject", typeof(string), maxLength: 255),
                        new ColumnDefinition("szColumn", typeof(string), maxLength: 255),
                        new ColumnDefinition("szReferencedObject", typeof(string), maxLength: 255),
                        new ColumnDefinition("szReferencedColumn", typeof(string), maxLength: 255),
                    ],
                    [
                        new IndexDefinition("szRelationship", "szRelationship") { IgnoreNulls = true },
                        new IndexDefinition("szObject", "szObject") { IgnoreNulls = true },
                        new IndexDefinition("szReferencedObject", "szReferencedObject") { IgnoreNulls = true },
                    ],
                    systemTableFlags,
                    ReservedTdefPageNumber: coreSystemTableStartPage > 0 ? coreSystemTableStartPage + 2 : 0,
                    EmitLvProp: false),
            ],
            BuildCoreCatalogObjectArtifacts());

        long[] tablePages = await catalogArtifacts.ExecutePlanAsync(plan, cancellationToken).ConfigureAwait(false);
        long acesTdefPage = tablePages[0];
        long queriesTdefPage = tablePages[1];
        long relationshipsTdefPage = tablePages[2];
        await this.PatchHeaderSystemTablePagesAsync(acesTdefPage, queriesTdefPage, relationshipsTdefPage, cancellationToken).ConfigureAwait(false);
    }

    private static CatalogObjectArtifact[] BuildCoreCatalogObjectArtifacts()
        =>
        [
            new CatalogObjectArtifact(
                2,
                Constants.SystemObjects.TablesParentId,
                Constants.SystemTableNames.Objects,
                Constants.SystemObjects.UserTableType,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.TablesParentId,
                Constants.SystemObjects.RootParentId,
                "Tables",
                3,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.DatabasesParentId,
                Constants.SystemObjects.RootParentId,
                "Databases",
                3,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.RelationshipsParentId,
                Constants.SystemObjects.RootParentId,
                "Relationships",
                3,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.DatabaseObjectId,
                Constants.SystemObjects.DatabasesParentId,
                "MSysDb",
                2,
                Constants.SystemObjects.SystemObjectFlag),
        ];

    private async ValueTask PatchHeaderSystemTablePagesAsync(
        long acesTdefPage,
        long queriesTdefPage,
        long relationshipsTdefPage,
        CancellationToken cancellationToken)
    {
        byte[] header = await this.pager.ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
        try
        {
            EncryptionManager.TransformHeaderMask(header);
            Wi32(header, 0x20, 2);
            Wi32(header, 0x24, checked((int)acesTdefPage));
            Wi32(header, 0x28, checked((int)queriesTdefPage));
            Wi32(header, 0x2C, checked((int)relationshipsTdefPage));
            EncryptionManager.TransformHeaderMask(header);
            await this.pager.WritePageAsync(0, header, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(header);
        }
    }

    /// <summary>
    /// Defines the canonical per-kind <c>MSysComplexType_*</c> template tables.
    /// Each entry maps the Access template name to the column schema Access emits
    /// for that template (verified against <c>ComplexFields.accdb</c> in
    /// <see href="docs/format-probe/format-probe-appendix-complex.md" /> §<c>MSysComplexType_*</c>).
    /// All templates are zero-row, zero-index tables; their <c>MSysObjects.Id</c>
    /// (= TDEF page) is what <c>MSysComplexColumns.ComplexTypeObjectID</c> points at.
    /// </summary>
    private static readonly (string Name, ColumnDefinition[] Columns)[] ComplexTypeTemplates =
    [
        ValueTemplate(Constants.ComplexTypeNames.UnsignedByte, typeof(byte)),
        ValueTemplate(Constants.ComplexTypeNames.Short, typeof(short)),
        ValueTemplate(Constants.ComplexTypeNames.Long, typeof(int)),
        ValueTemplate(Constants.ComplexTypeNames.IEEESingle, typeof(float)),
        ValueTemplate(Constants.ComplexTypeNames.IEEEDouble, typeof(double)),
        ValueTemplate(Constants.ComplexTypeNames.GUID, typeof(Guid)),
        ValueTemplate(Constants.ComplexTypeNames.Decimal, typeof(decimal)),
        ValueTemplate(Constants.ComplexTypeNames.Text, typeof(string), ComplexTypeTemplateTextLength),
        AttachmentTemplate(Constants.ComplexTypeNames.Attachment),
    ];

    private static (string Name, ColumnDefinition[] Columns) ValueTemplate(
        string name,
        Type valueType,
        int maxLength = 0)
        => (name, [Column("Value", valueType, maxLength)]);

    private static (string Name, ColumnDefinition[] Columns) AttachmentTemplate(string name)
        =>
        (
            name,
            [
                Column("FileData", typeof(byte[])),
                Column("FileFlags", typeof(int)),
                TextColumn("FileName"),
                Column("FileTimeStamp", typeof(DateTime)),
                TextColumn("FileType"),
                Column("FileURL", typeof(string)),
            ]);

    private static ColumnDefinition TextColumn(string name)
        => Column(name, typeof(string), ComplexTypeTemplateTextLength);

    private static ColumnDefinition Column(string name, Type clrType, int maxLength = 0)
        => new(name, clrType, maxLength);

    /// <summary>
    /// Maps a user-declared complex column to the canonical
    /// <c>MSysComplexType_*</c> template name. Returns <see langword="null"/>
    /// when the column is not complex or its element type has no matching template.
    /// </summary>
    /// <param name="col">The column descriptor.</param>
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
    /// scaffolds the nine <c>MSysComplexType_*</c> template tables
    /// (<c>UnsignedByte</c>, <c>Short</c>, <c>Long</c>, <c>IEEESingle</c>,
    /// <c>IEEEDouble</c>, <c>GUID</c>, <c>Decimal</c>, <c>Text</c>, <c>Attachment</c>)
    /// into a freshly-created ACCDB so subsequent <see cref="EmitComplexColumnArtifactsAsync"/>
    /// calls can populate <c>MSysComplexColumns.ComplexTypeObjectID</c> with a real
    /// catalog id instead of the placeholder <c>0</c>. Each catalog row carries
    /// <c>MSysObjects.Flags = 0x80030000</c> (system + the 0x30000 marker Access uses
    /// for type-template tables) so the templates are excluded from
    /// <c>ListTablesAsync</c>. Schema verified against <c>ComplexFields.accdb</c> —
    /// see <see href="docs/format-probe/format-probe-appendix-complex.md" />.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask CreateMSysComplexTypeTemplatesAsync(CancellationToken cancellationToken)
    {
        var tableArtifacts = new List<CatalogTableArtifact>(ComplexTypeTemplates.Length);
        foreach ((string name, ColumnDefinition[] cols) in ComplexTypeTemplates)
        {
            tableArtifacts.Add(new CatalogTableArtifact(
                name,
                cols,
                [],
                Constants.SystemObjects.ComplexTypeTemplateFlags,
                EmitLvProp: false,
                EmitUsageMap: false,
                MarkSystemTableTdef: false,
                EmitAceRows: false,
                RegisterConstraints: false));
        }

        await catalogArtifacts.ExecutePlanAsync(new CatalogArtifactPlan(tableArtifacts, []), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the empty <c>MSysComplexColumns</c> system table.
    /// Schema verified against <c>ComplexFields.accdb</c> (see
    /// <see href="docs/format-probe/format-probe-appendix-complex.md" /> and
    /// <see href="docs/design/complex-columns-format-notes.md" /> §2.2): four
    /// <c>LongInteger</c> columns (<c>ComplexTypeObjectID</c>, <c>FlatTableID</c>,
    /// <c>ConceptualTableID</c>, <c>ComplexID</c>) plus a <c>ColumnName</c>
    /// <c>Text(510)</c> variable column. The catalog row carries flag
    /// <c>0x80000000</c> (system / hidden) so the table is excluded from
    /// <see cref="TableCatalog.GetUserTablesAsync"/>.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask CreateMSysComplexColumnsAsync(CancellationToken cancellationToken)
    {
        ColumnDefinition[] columns =
        [
            new ColumnDefinition("ColumnName", typeof(string), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
            new ColumnDefinition("ComplexID", typeof(int)) { DescriptorFlagsOverride = 0x17 },
            new ColumnDefinition("ComplexTypeObjectID", typeof(int)) { DescriptorFlagsOverride = 0x13 },
            new ColumnDefinition("ConceptualTableID", typeof(int)) { DescriptorFlagsOverride = 0x13 },
            new ColumnDefinition("FlatTableID", typeof(int)) { DescriptorFlagsOverride = 0x13 },
        ];

        IndexDefinition[] indexes =
        [
            new IndexDefinition("IdxConceptualTableID", "ConceptualTableID"),
            new IndexDefinition("IdxFlatTableID", "FlatTableID"),
            new IndexDefinition("IdxID", "ComplexID") { IsPrimaryKey = true },
        ];

        await catalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan(
                [new CatalogTableArtifact(
                    Constants.SystemTableNames.ComplexColumns,
                    columns,
                    indexes,
                    Constants.SystemObjects.SystemObjectFlag,
                    EmitLvProp: false,
                    MarkSystemTableTdef: false)],
                []),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// pre-flight for table creation. Walks <paramref name="columns"/> for
    /// user-declared complex columns
    /// (<see cref="ColumnDefinition.IsAttachment"/> / <see cref="ColumnDefinition.IsMultiValue"/>
    /// where <c>ComplexId == 0</c>), validates the format, and allocates a
    /// fresh per-database <c>ComplexID</c> for each.
    /// Returns <see langword="null"/> when no allocation is needed.
    /// </summary>
    /// <param name="columns">The columns.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ArgumentException">Thrown when a multi-value column does not declare its element type or declares a Text length outside 0..255.</exception>
    /// <exception cref="NotSupportedException">Thrown when complex columns are declared for a non-ACE database or a catalog missing <c>MSysComplexColumns</c>.</exception>
    /// <exception cref="JetNotSupportedException">The complex-column system table is missing.</exception>
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

                if (def.IsMultiValue && def.MultiValueElementType == typeof(string) && (def.MaxLength < 0 || def.MaxLength > ComplexTypeTemplateTextLength))
                {
                    throw new ArgumentException(
                        $"Column '{def.Name}': multi-value Text length must be between 0 (the default) and {ComplexTypeTemplateTextLength} characters.",
                        nameof(columns));
                }

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

        if (!this.format.SupportsComplexColumns)
        {
            throw new NotSupportedException(
                "Attachment and MultiValue columns are an Access 2007+ ACE feature; declare them only on .accdb databases.");
        }

        long msysComplexPg = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysComplexPg == 0)
        {
            throw new JetNotSupportedException(JetErrorCode.SystemTableMissing, "The database does not contain a 'MSysComplexColumns' table. Create the database via AccessWriter.CreateDatabaseAsync (which scaffolds it automatically) before declaring complex columns, or open an Access-authored .accdb that already contains the catalog.", new JetErrorInfo { ObjectName = "MSysComplexColumns" });
        }

        foreach (int columnIndex in indices)
        {
            ColumnDefinition column = columns[columnIndex];
            string templateName = ResolveComplexTypeTemplateName(column)
                ?? throw new NotSupportedException($"Column '{column.Name}' has no supported native complex type template.");
            if (await catalogRows.FindSystemTableTdefPageAsync(templateName, cancellationToken).ConfigureAwait(false) <= 0)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptComplexColumn,
                    $"Required complex type template '{templateName}' is missing for '{column.Name}'.",
                    new JetErrorInfo { ColumnName = column.Name, ObjectName = templateName });
            }
        }

        int nextId = await this.GetNextComplexIdAsync(msysComplexPg, cancellationToken).ConfigureAwait(false);
        if ((long)nextId + indices.Count - 1 > int.MaxValue)
        {
            throw new InvalidOperationException($"'{Constants.SystemTableNames.ComplexColumns}' does not have enough ComplexID values remaining up to {int.MaxValue} for {indices.Count} columns.");
        }

        var allocations = new ComplexColumnAllocation[indices.Count];
        for (int i = 0; i < indices.Count; i++)
        {
            int id = nextId + i;
            allocations[i] = new ComplexColumnAllocation(indices[i], id);
        }

        return allocations;
    }

    /// <summary>
    /// Returns the next <c>ComplexID</c>: one greater than
    /// <c>MSysComplexColumns</c>' TDEF AutoNumber counter (the last ID handed
    /// out; <c>ComplexID</c> carries the AutoNumber flag), so the ID of a dropped
    /// complex column is never handed out again. Existing IDs must not exceed
    /// the counter; an inconsistent file is refused without repairing it.
    /// </summary>
    /// <param name="msysComplexPg">The MSysComplexColumns page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Every <c>ComplexID</c> up to <see cref="int.MaxValue"/> has been used.</exception>
    /// <exception cref="JetCorruptDataException">The ComplexID metadata is invalid or an existing ID exceeds the persisted counter.</exception>
    private async ValueTask<int> GetNextComplexIdAsync(long msysComplexPg, CancellationToken cancellationToken)
    {
        TableDef msysComplex = await this.tableDefs.ReadRequiredTableDefAsync(msysComplexPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? idCol = msysComplex.FindColumn("ComplexID");
        if (idCol is null || idCol.Type != LongIntegerType || !idCol.IsAutoNumber)
        {
            throw new JetCorruptDataException(
                JetErrorCode.CorruptComplexColumn,
                $"'{Constants.SystemTableNames.ComplexColumns}' has no valid Long Integer AutoNumber ComplexID column.",
                new JetErrorInfo { TableName = Constants.SystemTableNames.ComplexColumns, ColumnName = "ComplexID", PageNumber = msysComplexPg });
        }

        long counter = await autoNumbers.ReadHighWaterAsync(msysComplexPg, cancellationToken).ConfigureAwait(false);
        await this.ownedPages.ForEachLiveTableRowAsync(
            msysComplexPg,
            (row, _) =>
            {
                string idText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, idCol);
                if (!CatalogValueReader.TryParseInt32(idText, out int value) || value <= 0)
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptComplexColumn,
                        $"'{Constants.SystemTableNames.ComplexColumns}' contains an invalid ComplexID.",
                        new JetErrorInfo { TableName = Constants.SystemTableNames.ComplexColumns, ColumnName = "ComplexID", PageNumber = row.Location.DataPageNumber });
                }

                if (value > counter)
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptComplexColumn,
                        $"ComplexID {value} exceeds the '{Constants.SystemTableNames.ComplexColumns}' TDEF AutoNumber counter {counter}.",
                        new JetErrorInfo { TableName = Constants.SystemTableNames.ComplexColumns, ColumnName = "ComplexID", PageNumber = msysComplexPg });
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return counter < int.MaxValue
            ? (int)(counter + 1)
            : throw new InvalidOperationException($"'{Constants.SystemTableNames.ComplexColumns}' has used every ComplexID up to {int.MaxValue}.");
    }

    /// <summary>
    /// post-flight: for each user-declared complex column on the parent
    /// table, build a hidden flat child table per <see href="docs/design/complex-columns-format-notes.md" />
    /// §2.3 / §2.4 and append the corresponding <c>MSysComplexColumns</c> row so
    /// readers can join parent rows to their child values.
    /// </summary>
    /// <param name="parentTableName">The parent table name.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="columns">The columns.</param>
    /// <param name="allocations">The allocations.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <remarks>
    /// Emits the hidden flat-table schema, including the FK back-reference,
    /// per-kind value columns, Access-style scalar PK, and the known supporting
    /// indexes. <c>ComplexTypeObjectID</c> points at the matching native
    /// <c>MSysComplexType_*</c> template; missing required metadata is refused.
    /// </remarks>
    public async ValueTask EmitComplexColumnArtifactsAsync(
        string parentTableName,
        long parentTdefPage,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<ComplexColumnAllocation> allocations,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < allocations.Count; i++)
        {
            ComplexColumnAllocation alloc = allocations[i];
            ColumnDefinition col = columns[alloc.ColumnIndex];

            string templateName = ResolveComplexTypeTemplateName(col)
                ?? throw new NotSupportedException($"Column '{parentTableName}.{col.Name}' has no supported native complex type template.");
            long templatePage = await catalogRows.FindSystemTableTdefPageAsync(templateName, cancellationToken).ConfigureAwait(false);
            if (templatePage <= 0)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptComplexColumn,
                    $"Required complex type template '{templateName}' is missing for '{parentTableName}.{col.Name}'.",
                    new JetErrorInfo { TableName = parentTableName, ColumnName = col.Name, ObjectName = templateName });
            }

            int templateId = checked((int)templatePage);

            string flatTableName = BuildFlatTableName(col.Name);
            (ColumnDefinition[]? flatCols, IndexDefinition[]? flatIndexes) =
                BuildFlatTableSchema(parentTableName, col);

            long[] flatTablePages = await catalogArtifacts.ExecutePlanAsync(
                new CatalogArtifactPlan(
                    [new CatalogTableArtifact(
                        flatTableName,
                        flatCols,
                        flatIndexes,
                        Constants.SystemObjects.ComplexFlatTableFlags,
                        EmitAceRows: true)],
                    []),
                cancellationToken).ConfigureAwait(false);
            long flatTdefPage = flatTablePages[0];

            await this.InsertMSysComplexColumnsRowAsync(
                col.Name,
                complexId: alloc.ComplexId,
                conceptualTableId: checked((int)parentTdefPage),
                flatTableId: (int)flatTdefPage,
                complexTypeObjectId: templateId,
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Generates the canonical hidden-flat-table name <c>f_&lt;32-hex-uppercase&gt;_&lt;userColumnName&gt;</c>
    /// per the design doc §2.3 / format-probe-appendix-complex.md observations.
    /// </summary>
    /// <param name="userColumnName">Name of the user-visible complex column.</param>
    private static string BuildFlatTableName(string userColumnName)
    {
        // The 32 hex chars are a GUID without dashes — Access uses a fresh GUID per
        // flat table; we do the same so the name is unique even when two columns
        // share a name across tables.
        string guid = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture).ToUpperInvariant();
        return TruncateGeneratedName($"f_{guid}_{userColumnName}");
    }

    private static string TruncateGeneratedName(string name, int maxLength = AccessObjectName.MaxLength)
    {
        if (name.Length <= maxLength)
        {
            return name;
        }

        int length = char.IsHighSurrogate(name[maxLength - 1]) && char.IsLowSurrogate(name[maxLength])
            ? maxLength - 1
            : maxLength;
        return name[..length];
    }

    /// <summary>
    /// Builds the flat-table column list and the system-managed indexes per
    /// the per-kind schemas in the design doc §2.4 / §4.2.
    /// </summary>
    /// <param name="parentTableName">The parent table name.</param>
    /// <param name="parentColumn">The parent column.</param>
    /// <remarks>
    /// <para>
    /// Two LONG columns participate in the back-reference plumbing:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><c>_&lt;userColumnName&gt;</c> — FK back-reference holding the parent row's per-row complex reference value.</description></item>
    ///   <item><description><c>&lt;parentTable&gt;_&lt;userColumnName&gt;</c> — autoincrement scalar PK used by Access internally.</description></item>
    /// </list>
    /// <para>
    /// Naming and column ordering match Access-authored attachment flat
    /// tables in Northwind: FK back-reference first, kind-specific value
    /// columns in the middle, then the autoincrement scalar PK. Three indexes
    /// ship with the attachment table — primary key on the scalar
    /// (<c>MSysComplexPKIndex</c>), a normal index on the FK back-reference
    /// (named after the FK column), and a normal composite index on (FK,
    /// FileName) called <c>IdxFKPrimaryScalar</c>.
    /// </para>
    /// <para>
    /// The multi-value variant has no empirical fixture; the conservative
    /// schema mirrors the attachment pattern minus the composite
    /// <c>IdxFKPrimaryScalar</c> attachment index.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">Thrown when a multi-value flat-table schema is requested without an element type.</exception>
    private static (ColumnDefinition[] Columns, IndexDefinition[] Indexes) BuildFlatTableSchema(
        string parentTableName,
        ColumnDefinition parentColumn)
    {
        string fkName = TruncateGeneratedName($"_{parentColumn.Name}");
        string scalarName = TruncateGeneratedName($"{parentTableName}_{parentColumn.Name}");
        if (string.Equals(scalarName, fkName, StringComparison.OrdinalIgnoreCase))
        {
            scalarName = TruncateGeneratedName(scalarName, AccessObjectName.MaxLength - 2) + "_1";
        }

        var fk = new ColumnDefinition(fkName, typeof(int))
        {
            ForceVariableLengthStorage = true,
            DescriptorFlagsOverride = 0x02,
            DescriptorExtraFlagsOverride = 0x08,
        };
        var scalar = new ColumnDefinition(scalarName, typeof(int))
        {
            IsAutoIncrement = true,
            ForceVariableLengthStorage = true,
            DescriptorFlagsOverride = 0x06,
            DescriptorExtraFlagsOverride = 0x04,
        };

        if (parentColumn.IsAttachment)
        {
            ColumnDefinition[] cols =
            [
                fk,
                new ColumnDefinition("FileData", typeof(byte[]))
                {
                    DescriptorExtraFlagsOverride = 0x10,
                    DescriptorNonTextMiscOverride = 0x00000409,
                },
                new ColumnDefinition("FileFlags", typeof(int))
                {
                    DescriptorExtraFlagsOverride = 0x10,
                    DescriptorNonTextMiscOverride = 0x00000409,
                },
                new ColumnDefinition("FileName", typeof(string), maxLength: 255)
                {
                    IsCompressedUnicode = false,
                    DescriptorExtraFlagsOverride = 0x10,
                },
                new ColumnDefinition("FileTimeStamp", typeof(DateTime))
                {
                    DescriptorExtraFlagsOverride = 0x10,
                    DescriptorNonTextMiscOverride = 0x00000409,
                },
                new ColumnDefinition("FileType", typeof(string), maxLength: 255)
                {
                    IsCompressedUnicode = false,
                    DescriptorExtraFlagsOverride = 0x10,
                },
                new ColumnDefinition("FileURL", typeof(string))
                {
                    IsCompressedUnicode = false,
                    DescriptorExtraFlagsOverride = 0x10,
                },
                scalar,
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
        // The declared precision and scale are the items' own, so a Decimal element
        // stores them on this column (Access's Decimal(18,0) when none is declared);
        // other element types ignore them.
        Type elementType = parentColumn.MultiValueElementType
            ?? throw new InvalidOperationException("MultiValueElementType must be set on a multi-value column.");
        var valueCol = new ColumnDefinition("Value", elementType, maxLength: elementType == typeof(string) && parentColumn.MaxLength == 0 ? ComplexTypeTemplateTextLength : parentColumn.MaxLength)
        {
            DescriptorExtraFlagsOverride = 0x10,
            DescriptorNonTextMiscOverride = 0x00000409,
            NumericPrecision = parentColumn.NumericPrecision,
            NumericScale = parentColumn.NumericScale,
        };
        ColumnDefinition[] mvCols = [fk, valueCol, scalar];
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
    /// page. Schema verified in <see href="format-probe-appendix-complex.md" />.
    /// </summary>
    /// <param name="parentColumnName">The parent column name.</param>
    /// <param name="complexId">The complex id.</param>
    /// <param name="conceptualTableId">The conceptual table id.</param>
    /// <param name="flatTableId">The flat table id.</param>
    /// <param name="complexTypeObjectId">The complex type object id.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when the <c>MSysComplexColumns</c> table is missing.</exception>
    /// <exception cref="JetOperationException">The operation is refused with a structured <see cref="JetOperationException"/>.</exception>
    private async ValueTask InsertMSysComplexColumnsRowAsync(
        string parentColumnName,
        int complexId,
        int conceptualTableId,
        int flatTableId,
        int complexTypeObjectId,
        CancellationToken cancellationToken)
    {
        long pg = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (pg == 0)
        {
            throw new JetOperationException(JetErrorCode.SystemTableMissing, "MSysComplexColumns table is missing.", new JetErrorInfo { ObjectName = Constants.SystemTableNames.ComplexColumns });
        }

        TableDef msysComplex = await this.tableDefs.ReadRequiredTableDefAsync(pg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        object[] values = msysComplex.CreateNullValueRow();

        msysComplex.SetValueByName(values, "ColumnName", parentColumnName);
        msysComplex.SetValueByName(values, "ComplexTypeObjectID", complexTypeObjectId);
        msysComplex.SetValueByName(values, "FlatTableID", flatTableId);
        msysComplex.SetValueByName(values, "ConceptualTableID", conceptualTableId);
        msysComplex.SetValueByName(values, "ComplexID", complexId);

        await indexes.InsertSystemRowAndMaintainAsync(pg, msysComplex, Constants.SystemTableNames.ComplexColumns, values, cancellationToken: cancellationToken).ConfigureAwait(false);

        // ComplexID is an AutoNumber column; keep its TDEF counter at the last
        // ID handed out, as Access does, so neither this library nor Access
        // reuses it after the column is dropped.
        await autoNumbers.RaiseHighWaterAsync(pg, complexId, cancellationToken).ConfigureAwait(false);
    }

    // ── Row-level APIs for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §2.1 / §2.4 / §3.

    public async ValueTask AddComplexItemCoreAsync(
        string tableName,
        string columnName,
        IReadOnlyDictionary<string, object?> parentRowKey,
        object? payload,
        bool expectAttachment,
        CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        Guard.NotNull(parentRowKey, nameof(parentRowKey));
        this.pager.ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        if (parentRowKey.Count == 0)
        {
            throw new ArgumentException("At least one key column is required.", nameof(parentRowKey));
        }

        if (!this.format.SupportsComplexColumns)
        {
            throw new NotSupportedException(
                "Complex (Attachment / MultiValue) columns are an Access 2007+ ACE feature; only .accdb databases are supported.");
        }

        // Resolve parent table + complex column.
        ResolvedTable parentTable = await this.catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry parentEntry = parentTable.Entry;
        TableDef parentDef = parentTable.Definition;

        ColumnInfo complexCol = parentDef.FindColumn(columnName)
            ?? throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{columnName}' was not found in table '{tableName}'.", nameof(columnName), errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = columnName });

        bool isComplexCol = complexCol.Type is AttachmentType or ComplexType;
        if (!isComplexCol)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is not a complex (Attachment / MultiValue) column (type={GetTypeDisplayName(complexCol.Type)}).");
        }

        // Resolve the hidden flat child table via MSysComplexColumns.
        long flatTdefPage = await seeds.ResolveFlatTableTdefPageAsync(columnName, complexCol.Misc, cancellationToken).ConfigureAwait(false);
        if (flatTdefPage <= 0)
        {
            throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, $"No MSysComplexColumns row was found for column '{tableName}.{columnName}'.", errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = columnName });
        }

        TableDef flatDef = await this.tableDefs.ReadRequiredTableDefAsync(flatTdefPage, "<flat>", cancellationToken).ConfigureAwait(false);
        ComplexColumnKind kind = ClassifyComplexColumnKind(complexCol.Type, flatDef);
        if (kind == ComplexColumnKind.Unknown)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is a complex column, but its subtype could not be determined from the flat child table.");
        }

        if (expectAttachment && kind != ComplexColumnKind.Attachment)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is a MultiValue column; call AddMultiValueItemAsync instead.");
        }

        if (!expectAttachment && kind != ComplexColumnKind.MultiValue)
        {
            throw new NotSupportedException(
                $"Column '{tableName}.{columnName}' is an Attachment column; call AddAttachmentAsync instead.");
        }

        if (!expectAttachment && flatDef.FindColumn("Value") is { Type: TextType } valueColumn
            && payload is not null and not DBNull)
        {
            string text = Convert.ToString(payload, CultureInfo.InvariantCulture) ?? string.Empty;
            if (text.Length > valueColumn.Size / 2)
            {
                throw new ArgumentException(
                    $"Multi-value Text item exceeds the {valueColumn.Size / 2}-character limit of '{tableName}.{columnName}'.",
                    nameof(payload));
            }
        }

        // Normalize each predicate once using its actual native column semantics.
        int[] predIndexes = new int[parentRowKey.Count];
        byte[][] predValues = new byte[parentRowKey.Count][];
        int pi = 0;
        foreach (KeyValuePair<string, object?> kvp in parentRowKey)
        {
            int idx = parentDef.FindColumnIndex(kvp.Key);
            if (idx < 0)
            {
                throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{kvp.Key}' was not found in table '{tableName}'.", nameof(parentRowKey), errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = kvp.Key });
            }

            predIndexes[pi] = idx;
            try
            {
                predValues[pi] = ComplexParentKeyComparer.Encode(this.format, parentDef.Columns[idx], kvp.Value);
            }
            catch (Exception exception) when (exception is ArgumentException or FormatException or OverflowException or InvalidCastException)
            {
                throw new ArgumentException($"Parent row key for column '{tableName}.{kvp.Key}' has an incompatible value.", nameof(parentRowKey), exception);
            }

            pi++;
        }

        // Locate the unique parent row.
        RowLocation parentLocation = await this.FindUniqueParentRowAsync(parentEntry.TDefPage, parentDef, predIndexes, predValues, tableName, cancellationToken)
            .ConfigureAwait(false);

        // Refuse unsupported parent index metadata before writing an item.
        await indexes.ThrowIfIndexesUnmaintainableAsync(parentEntry.TDefPage, parentDef, tableName, cancellationToken).ConfigureAwait(false);

        // Require an existing reference consistent with the persisted counter.
        int conceptualTableId = await this.ReadRequiredComplexReferenceAsync(
            tableName,
            parentEntry.TDefPage,
            parentLocation,
            complexCol,
            cancellationToken).ConfigureAwait(false);

        // Build the flat-table row values.
        object[] flatValues = expectAttachment
            ? BuildAttachmentFlatRow(flatDef, conceptualTableId, (AttachmentInput)payload!)
            : BuildMultiValueFlatRow(flatDef, conceptualTableId, payload);

        // The flat table carries an autoincrement scalar PK column.
        // ConstraintRegistry.ApplyAsync hydrates the constraint registry from the
        // persisted FLAG_AUTO_LONG bit and seeds the next value from the
        // larger of the flat table's TDEF AutoNumber counter and its existing
        // rows, so AddAttachmentAsync / AddMultiValueItemAsync stay a
        // single-call surface. The counter is raised after the insert below.
        string flatTableName = await this.ResolveFlatTableNameAsync(flatTdefPage, cancellationToken).ConfigureAwait(false);
        await constraints.ApplyAsync(flatTableName, flatDef, flatValues, cancellationToken).ConfigureAwait(false);

        await tableRows.InsertRowDataAsync(flatTdefPage, flatDef, flatValues, cancellationToken: cancellationToken).ConfigureAwait(false);
        await indexes.MaintainIndexesAsync(flatTdefPage, flatDef, flatTableName, cancellationToken).ConfigureAwait(false);
        await autoNumbers.UpdateHighWaterAsync(flatTdefPage, flatDef, [flatValues], cancellationToken).ConfigureAwait(false);
    }

    private static ComplexColumnKind ClassifyComplexColumnKind(ColumnType parentType, TableDef flatDef)
    {
        if (parentType == AttachmentType)
        {
            return ComplexColumnKind.Attachment;
        }

        if (flatDef.FindColumn("FileData") != null && flatDef.FindColumn("FileName") != null)
        {
            return ComplexColumnKind.Attachment;
        }

        if (flatDef.FindColumn("Value") != null)
        {
            return ComplexColumnKind.MultiValue;
        }

        return ComplexColumnKind.Unknown;
    }

    /// <summary>
    /// Resolves a hidden flat-child-table TDEF page back to its
    /// <c>MSysObjects.Name</c>. Used by <see cref="AddComplexItemCoreAsync"/>
    /// to drive <c>ApplyConstraintsAsync</c> for the autoincrement
    /// scalar PK column emitted by the complex-column scaffold.
    /// </summary>
    /// <param name="flatTdefPage">The flat TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <c>MSysObjects</c> is missing or has no row for <paramref name="flatTdefPage"/>.</exception>
    /// <exception cref="JetOperationException">The operation is refused with a structured <see cref="JetOperationException"/>.</exception>
    private async ValueTask<string> ResolveFlatTableNameAsync(long flatTdefPage, CancellationToken cancellationToken)
    {
        TableDef? msys = await this.tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false)
            ?? throw new JetOperationException(JetErrorCode.SystemTableMissing, "MSysObjects catalog table is missing.", new JetErrorInfo { ObjectName = Constants.SystemTableNames.Objects });

        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage == flatTdefPage)
            {
                return row.Name;
            }
        }

        throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, $"No MSysObjects row was found for flat-child TDEF page {flatTdefPage}.");
    }

    private async ValueTask<RowLocation> FindUniqueParentRowAsync(
        long parentTdefPage,
        TableDef parentDef,
        int[] predIndexes,
        byte[][] predValues,
        string tableName,
        CancellationToken cancellationToken)
    {
        RowLocation match = default;
        bool found = false;

        bool[] wantedColumns = new bool[parentDef.Columns.Count];
        foreach (int index in predIndexes)
        {
            wantedColumns[index] = true;
        }

        await snapshots.ForEachTypedRowAsync(
            parentTdefPage,
            parentDef,
            wantedColumns,
            (location, values, _) =>
            {
                for (int predicate = 0; predicate < predIndexes.Length; predicate++)
                {
                    int index = predIndexes[predicate];
                    byte[] actual = ComplexParentKeyComparer.Encode(this.format, parentDef.Columns[index], values[index]);
                    if (!actual.AsSpan().SequenceEqual(predValues[predicate]))
                    {
                        return new ValueTask<bool>(true);
                    }
                }

                if (found)
                {
                    throw new JetOperationException(JetErrorCode.RowNotUnique, $"Parent row key matches more than one row in '{tableName}'.", errorInfo: new JetErrorInfo { TableName = tableName });
                }

                match = location;
                found = true;
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        if (!found)
        {
            throw new JetOperationException(JetErrorCode.RowNotFound, $"No row in '{tableName}' matches the supplied parent row key.", errorInfo: new JetErrorInfo { TableName = tableName });
        }

        return match;
    }

    /// <summary>Reads an existing parent reference consistent with its persisted counter without changing any parent slot.</summary>
    /// <param name="tableName">The parent table name.</param>
    /// <param name="parentTdefPage">The parent table's TDEF page.</param>
    /// <param name="parentLocation">The parent row.</param>
    /// <param name="complexCol">The complex column an item is being added to.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The existing positive complex reference.</returns>
    /// <exception cref="JetCorruptDataException">The parent row has no valid complex reference, or it exceeds the persisted counter.</exception>
    private async ValueTask<int> ReadRequiredComplexReferenceAsync(
        string tableName,
        long parentTdefPage,
        RowLocation parentLocation,
        ColumnInfo complexCol,
        CancellationToken cancellationToken)
    {
        long counter = await autoNumbers.ReadComplexHighWaterAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
        byte[] page = await this.pager.ReadPageAsync(parentLocation.DataPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (ComplexReferenceSeedReader.TryReadSlot(this.format, page, parentLocation.RowStart, parentLocation.RowSize, complexCol, out int existing)
                && existing > 0)
            {
                if (existing > counter)
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptComplexColumn,
                        $"Complex reference {existing} for '{tableName}.{complexCol.Name}' exceeds the parent TDEF complex AutoNumber counter {counter}.",
                        new JetErrorInfo { TableName = tableName, ColumnName = complexCol.Name, PageNumber = parentTdefPage });
                }

                return existing;
            }

            throw new JetCorruptDataException(
                JetErrorCode.CorruptComplexColumn,
                $"Parent row of '{tableName}' has no valid complex reference for '{complexCol.Name}'.",
                new JetErrorInfo { TableName = tableName, ColumnName = complexCol.Name, PageNumber = parentLocation.PageNumber });
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private static object[] BuildAttachmentFlatRow(TableDef flatDef, int conceptualTableId, AttachmentInput input)
    {
        object[] values = flatDef.CreateNullValueRow();

        // FK back-ref: the single LongInteger column starting with "_".
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();
        values[flatDef.FindColumnIndex(column => column == fkCol)] = conceptualTableId;

        string ext = input.FileType ?? DeriveExtension(input.FileName);

        flatDef.SetValueByName(values, "FileURL", input.FileURL ?? (object)DBNull.Value);
        flatDef.SetValueByName(values, "FileName", input.FileName);
        flatDef.SetValueByName(values, "FileType", ext);
        flatDef.SetValueByName(values, "FileFlags", DBNull.Value);
        flatDef.SetValueByName(values, "FileTimeStamp", input.FileTimeStamp ?? (object)DBNull.Value);
        flatDef.SetValueByName(values, "FileData", AttachmentWrapper.Encode(ext, input.FileData));
        return values;
    }

    private static object[] BuildMultiValueFlatRow(TableDef flatDef, int conceptualTableId, object? value)
    {
        object[] values = flatDef.CreateNullValueRow();
        ColumnInfo fkCol = flatDef.FindFlatTableForeignKeyColumn();
        values[flatDef.FindColumnIndex(column => column == fkCol)] = conceptualTableId;
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

        return fileName[(dot + 1)..].ToLowerInvariant();
    }

    // ── Cascade-on-delete for complex (Attachment / MultiValue) columns ──
    // See docs/design/complex-columns-format-notes.md §4.3.
    //
    // Whenever a parent row containing a complex column slot is deleted, the
    // associated rows in the hidden flat child table (joined via the parent's
    // 4-byte per-row complex reference slot) must also be deleted. Without this pass
    // the flat table accumulates orphaned rows, breaks referential integrity
    // expected by Microsoft Access, and may cause Compact &amp; Repair to flag
    // the file.

    /// <summary>
    /// Finds, without writing anything, the rows of the hidden flat child
    /// tables of every Attachment / MultiValue column on
    /// <paramref name="parentDef"/> that a delete of
    /// <paramref name="deletedParentLocations"/> removes, and adds them to
    /// <paramref name="plan"/>. Must be called BEFORE any of the statement's
    /// rows is marked deleted, since the per-row complex-reference slot value
    /// is needed to identify which flat rows to delete; the statement then
    /// deletes them with <see cref="ApplyComplexChildDeletesAsync"/> just
    /// before it deletes the parent rows.
    /// </summary>
    /// <param name="parentDef">The parent def.</param>
    /// <param name="deletedParentLocations">The deleted parent locations.</param>
    /// <param name="plan">The statement's group for these parent rows, which this call adds to; a flat row another group of the statement holds is left out.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <remarks>
    /// Per-flat-table cost is O(P) where P is the database page count
    /// (full sequential scan, no index seek). Multiple complex columns on
    /// the same parent perform one scan each. This matches the existing
    /// cascade-delete cost profile and the complex-reference allocator
    /// used by the row-add path.
    /// </remarks>
    public async ValueTask PlanComplexChildDeletesAsync(
        TableDef parentDef,
        List<RowLocation> deletedParentLocations,
        ComplexChildDeletes plan,
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
            if (col.Type is AttachmentType or ComplexType)
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
            long flatPg = await seeds.ResolveFlatTableTdefPageAsync(col.Name, col.Misc, cancellationToken).ConfigureAwait(false);
            if (flatPg > 0)
            {
                flatPagesByCol[col.ColNum] = flatPg;
            }
        }

        if (flatPagesByCol.Count == 0)
        {
            return;
        }

        // Read each parent row to collect the live per-row complex reference per
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

            byte[] page = await this.pager.ReadPageAsync(loc.DataPageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                foreach (ColumnInfo col in complexCols)
                {
                    if (idsByCol.TryGetValue(col.ColNum, out HashSet<int>? ids)
                        && ComplexReferenceSeedReader.TryReadSlot(this.format, page, loc.RowStart, loc.RowSize, col, out int reference))
                    {
                        _ = ids.Add(reference);
                    }
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        // For each complex column with collected IDs, scan the flat
        // child table once and plan the delete of every row whose FK
        // back-reference is in the set.
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

            TableDef flatDef = await this.tableDefs.ReadRequiredTableDefAsync(flatTdefPage, "<flat>", cancellationToken).ConfigureAwait(false);

            ColumnInfo? fkCol = flatDef.Columns.FirstOrDefault(c => c.Type == LongIntegerType && c.Name.StartsWith('_'))
                ?? flatDef.Columns.FirstOrDefault(c => c.Type == LongIntegerType);
            if (fkCol == null)
            {
                continue;
            }

            await this.ownedPages.ForEachLiveTableRowAsync(
                flatTdefPage,
                (row, _) =>
                {
                    string fkText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, fkCol);
                    if (CatalogValueReader.TryParseInt32(fkText, out int fk)
                        && ids.Contains(fk))
                    {
                        plan.Add(flatTdefPage, row.Location);
                    }

                    return new ValueTask<bool>(true);
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Deletes the flat rows the group <paramref name="plan"/> holds and
    /// lowers each flat table's row count once; the statement calls it just
    /// before it deletes the group's parent rows. Takes no cancellation token:
    /// it is part of the delete statement's writes, and a statement that has
    /// started writing runs to completion.
    /// </summary>
    /// <param name="plan">The group's flat rows, which <see cref="PlanComplexChildDeletesAsync"/> found.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public async ValueTask ApplyComplexChildDeletesAsync(ComplexChildDeletes plan)
    {
        foreach ((long flatTdefPage, List<RowLocation> rows) in plan.Tables)
        {
            TableDef flatDef = await this.tableDefs.ReadRequiredTableDefAsync(flatTdefPage, "<flat>", CancellationToken.None).ConfigureAwait(false);
            foreach (RowLocation row in rows)
            {
                await tableRows.MarkRowDeletedAsync(row.PageNumber, row.RowIndex, flatDef, CancellationToken.None).ConfigureAwait(false);
            }

            await tableRows.AdjustTDefRowCountAsync(flatTdefPage, -rows.Count, CancellationToken.None).ConfigureAwait(false);
            await indexes.MaintainIndexesAsync(flatTdefPage, flatDef, "<flat>", CancellationToken.None).ConfigureAwait(false);
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
    /// <param name="parentTdefPage">The owning parent TDEF page.</param>
    /// <param name="columnName">The column name.</param>
    /// <param name="complexId">The complex id.</param>
    /// <param name="reclaimStorage">Releases the flat table storage using its captured definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask DropSingleComplexChildAsync(long parentTdefPage, string columnName, int complexId, Func<long, TableDef, CancellationToken, ValueTask> reclaimStorage, CancellationToken cancellationToken)
    {
        long msysCxPg = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await this.tableDefs.ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msysCxDef.FindColumn("FlatTableID");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        ColumnInfo? parentIdCol = msysCxDef.FindColumn("ConceptualTableID");
        if (nameCol == null || flatIdCol == null || cxIdCol == null || parentIdCol == null)
        {
            return;
        }

        long flatTdefPage = 0;
        var deletedRows = new List<(long PageNumber, int RowIndex)>();

        await this.ownedPages.ForEachLiveTableRowAsync(
            msysCxPg,
            (row, _) =>
            {
                string parentText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, parentIdCol);
                if (!CatalogValueReader.TryParseInt64(parentText, out long parentId) || CatalogValueReader.TdefPageFromId(parentId) != parentTdefPage)
                {
                    return new ValueTask<bool>(true);
                }

                string rowName = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, nameCol);
                if (!string.Equals(rowName, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return new ValueTask<bool>(true);
                }

                string idText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, cxIdCol);
                if (!CatalogValueReader.TryParseInt32(idText, out int rid) || rid != complexId)
                {
                    return new ValueTask<bool>(true);
                }

                string flatText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, flatIdCol);
                if (CatalogValueReader.TryParseInt64(flatText, out long fid))
                {
                    flatTdefPage = CatalogValueReader.TdefPageFromId(fid);
                }

                deletedRows.Add((row.Location.PageNumber, row.Location.RowIndex));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        Dictionary<long, TableDef> definitions = await this.ReadFlatDefinitionsBeforeDropAsync([flatTdefPage], deletedRows, msysCxPg, msysCxDef, cancellationToken).ConfigureAwait(false);

        foreach ((long pg, int ri) in deletedRows)
        {
            await tableRows.MarkRowDeletedAsync(pg, ri, msysCxDef, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        if (deletedRows.Count > 0)
        {
            await tableRows.AdjustTDefRowCountAsync(msysCxPg, -deletedRows.Count, cancellationToken).ConfigureAwait(false);
            await indexes.MaintainIndexesAsync(msysCxPg, msysCxDef, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        }

        if (flatTdefPage <= 0)
        {
            return;
        }

        await this.DropFlatStorageAsync(definitions, reclaimStorage, cancellationToken).ConfigureAwait(false);
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
    /// <param name="oldColumnName">The old column name.</param>
    /// <param name="newColumnName">The new column name.</param>
    /// <param name="complexId">The complex id.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask RenameComplexColumnArtifactsAsync(string oldColumnName, string newColumnName, int complexId, CancellationToken cancellationToken)
    {
        long msysCxPg = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await this.tableDefs.ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (nameCol == null || cxIdCol == null)
        {
            return;
        }

        var matched = new List<(RowLocation Loc, object[] Values)>();
        await this.ownedPages.ForEachLiveTableRowAsync(
            msysCxPg,
            (row, _) =>
            {
                string rowName = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, nameCol);
                if (!string.Equals(rowName, oldColumnName, StringComparison.OrdinalIgnoreCase))
                {
                    return new ValueTask<bool>(true);
                }

                string idText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, cxIdCol);
                if (!CatalogValueReader.TryParseInt32(idText, out int rid) || rid != complexId)
                {
                    return new ValueTask<bool>(true);
                }

                object[] values = new object[msysCxDef.Columns.Count];
                for (int i = 0; i < values.Length; i++)
                {
                    string text = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, msysCxDef.Columns[i]);
                    values[i] = string.IsNullOrEmpty(text) ? DBNull.Value : text;
                }

                msysCxDef.SetValueByName(values, "ColumnName", newColumnName);
                matched.Add((row.Location, values));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        foreach ((RowLocation loc, object[] _) in matched)
        {
            await tableRows.MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, msysCxDef, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        foreach ((RowLocation _, object[] values) in matched)
        {
            await indexes.InsertSystemRowAndMaintainAsync(
                msysCxPg,
                msysCxDef,
                Constants.SystemTableNames.ComplexColumns,
                values,
                updateTDefRowCount: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        if (matched.Count > 0)
        {
            await indexes.MaintainIndexesAsync(msysCxPg, msysCxDef, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask UpdateComplexColumnParentTableIdAsync(int complexId, int parentTdefPage, CancellationToken cancellationToken)
    {
        long msysCxPg = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await this.tableDefs.ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        if (cxIdCol == null)
        {
            return;
        }

        var matched = new List<(RowLocation Loc, object[] Values)>();
        await this.ownedPages.ForEachLiveTableRowAsync(
            msysCxPg,
            (row, _) =>
            {
                string idText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, cxIdCol);
                if (!CatalogValueReader.TryParseInt32(idText, out int rid) || rid != complexId)
                {
                    return new ValueTask<bool>(true);
                }

                object[] values = new object[msysCxDef.Columns.Count];
                for (int i = 0; i < values.Length; i++)
                {
                    string text = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, msysCxDef.Columns[i]);
                    values[i] = string.IsNullOrEmpty(text) ? DBNull.Value : text;
                }

                msysCxDef.SetValueByName(values, "ConceptualTableID", parentTdefPage);
                matched.Add((row.Location, values));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        foreach ((RowLocation loc, object[] _) in matched)
        {
            await tableRows.MarkRowDeletedAsync(loc.PageNumber, loc.RowIndex, msysCxDef, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        foreach ((RowLocation _, object[] values) in matched)
        {
            await indexes.InsertSystemRowAndMaintainAsync(
                msysCxPg,
                msysCxDef,
                Constants.SystemTableNames.ComplexColumns,
                values,
                updateTDefRowCount: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        if (matched.Count > 0)
        {
            await indexes.MaintainIndexesAsync(msysCxPg, msysCxDef, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
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
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="reclaimStorage">Releases each flat table storage using its captured definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask DropComplexChildrenForTableAsync(long parentTdefPage, Func<long, TableDef, CancellationToken, ValueTask> reclaimStorage, CancellationToken cancellationToken)
    {
        TableDef? parentDef = await this.tableDefs.ReadTableDefAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
        if (parentDef == null)
        {
            return;
        }

        var complexCols = new List<ColumnInfo>();
        foreach (ColumnInfo col in parentDef.Columns)
        {
            if (col.Type is AttachmentType or ComplexType)
            {
                complexCols.Add(col);
            }
        }

        if (complexCols.Count == 0)
        {
            return;
        }

        long msysCxPg = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        if (msysCxPg == 0)
        {
            return;
        }

        TableDef msysCxDef = await this.tableDefs.ReadRequiredTableDefAsync(msysCxPg, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        ColumnInfo? nameCol = msysCxDef.FindColumn("ColumnName");
        ColumnInfo? flatIdCol = msysCxDef.FindColumn("FlatTableID");
        ColumnInfo? cxIdCol = msysCxDef.FindColumn("ComplexID");
        ColumnInfo? parentIdCol = msysCxDef.FindColumn("ConceptualTableID");
        if (nameCol == null || flatIdCol == null || cxIdCol == null || parentIdCol == null)
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

        await this.ownedPages.ForEachLiveTableRowAsync(
            msysCxPg,
            (row, _) =>
            {
                string parentText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, parentIdCol);
                if (!CatalogValueReader.TryParseInt64(parentText, out long parentId) || CatalogValueReader.TdefPageFromId(parentId) != parentTdefPage)
                {
                    return new ValueTask<bool>(true);
                }

                string rowName = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, nameCol);
                string idText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, cxIdCol);
                if (!CatalogValueReader.TryParseInt32(idText, out int rid))
                {
                    return new ValueTask<bool>(true);
                }

                if (!lookup.TryGetValue(rowName, out HashSet<int>? expectedIds) || !expectedIds.Contains(rid))
                {
                    return new ValueTask<bool>(true);
                }

                string flatText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, flatIdCol);
                if (CatalogValueReader.TryParseInt64(flatText, out long flatId))
                {
                    flatTdefPages.Add(CatalogValueReader.TdefPageFromId(flatId));
                }

                cxRowsToDelete.Add((row.Location.PageNumber, row.Location.RowIndex));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        Dictionary<long, TableDef> definitions = await this.ReadFlatDefinitionsBeforeDropAsync(flatTdefPages, cxRowsToDelete, msysCxPg, msysCxDef, cancellationToken).ConfigureAwait(false);

        foreach ((long pg, int ri) in cxRowsToDelete)
        {
            await tableRows.MarkRowDeletedAsync(pg, ri, msysCxDef, DeletedRowDataMode.Clear, cancellationToken).ConfigureAwait(false);
        }

        if (cxRowsToDelete.Count > 0)
        {
            await tableRows.AdjustTDefRowCountAsync(msysCxPg, -cxRowsToDelete.Count, cancellationToken).ConfigureAwait(false);
            await indexes.MaintainIndexesAsync(msysCxPg, msysCxDef, Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false);
        }

        if (flatTdefPages.Count == 0)
        {
            return;
        }

        await this.DropFlatStorageAsync(definitions, reclaimStorage, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<Dictionary<long, TableDef>> ReadFlatDefinitionsBeforeDropAsync(IEnumerable<long> flatPages, List<(long PageNumber, int RowIndex)> droppedRows, long complexTablePage, TableDef complexTableDef, CancellationToken cancellationToken)
    {
        var definitions = new Dictionary<long, TableDef>();
        var candidates = new HashSet<long>(flatPages);
        ColumnInfo flatId = complexTableDef.FindColumn("FlatTableID")!;
        var dropping = new HashSet<(long PageNumber, int RowIndex)>(droppedRows);
        await this.ownedPages.ForEachLiveTableRowAsync(
            complexTablePage,
            (row, _) =>
            {
                string flatText = ScalarColumnReader.DecodeSimpleColumnValue(this.format, row.Page, row.Location.RowStart, row.Location.RowSize, flatId);
                if (CatalogValueReader.TryParseInt64(flatText, out long flat)
                    && !dropping.Contains((row.Location.PageNumber, row.Location.RowIndex)))
                {
                    // Do not follow a corrupted row into a sibling's flat table.
                    candidates.Remove(CatalogValueReader.TdefPageFromId(flat));
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);
        TableDef msys = await this.tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in await catalogRows.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false))
        {
            // A corrupt FlatTableID must never release an ordinary user's table.
            if (candidates.Contains(row.TDefPage) && row.TDefPage > 2 && row.IsDecoded
                && row.ObjectType == Constants.SystemObjects.UserTableType
                && (row.Flags & Constants.SystemObjects.ComplexFlatTableFlags) == Constants.SystemObjects.ComplexFlatTableFlags)
            {
                definitions[row.TDefPage] = await this.tableDefs.ReadRequiredTableDefAsync(row.TDefPage, row.Name, cancellationToken).ConfigureAwait(false);
            }
        }

        return definitions;
    }

    private async ValueTask DropFlatStorageAsync(Dictionary<long, TableDef> definitions, Func<long, TableDef, CancellationToken, ValueTask> reclaimStorage, CancellationToken cancellationToken)
    {
        await catalogArtifacts.DeleteCatalogRowsByTdefPagesAsync(definitions.Keys, cancellationToken).ConfigureAwait(false);
        foreach ((long page, TableDef definition) in definitions)
        {
            await reclaimStorage(page, definition, cancellationToken).ConfigureAwait(false);
        }

        this.catalog.Invalidate();
    }
}
