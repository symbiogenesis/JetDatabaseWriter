namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using static JetDatabaseWriter.DatabaseFile;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Table DDL workflows behind <see cref="Interfaces.IAccessSchema"/>: create
/// and drop tables, and add, drop, or rename columns. Column changes rebuild
/// the table through a temporary copy that preserves rows, indexes, persisted
/// column properties, client-side constraints, and complex-column artifacts.
/// Dropped tables return their data, LVAL, index, usage-map, and TDEF pages to
/// the global free map. The public facade owns the auto-commit scope around
/// each call.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="catalog">Resolves table names and is invalidated after a rename.</param>
/// <param name="tableRows">Copies rows into the rebuilt table.</param>
/// <param name="indexMaintainer">Rebuilds forwarded indexes after a row copy.</param>
/// <param name="pageAllocator">Frees reclaimed table pages.</param>
/// <param name="longValueEncoder">Finds and frees LVAL chains owned by dropped rows.</param>
/// <param name="catalogWriter">Renames and deletes <c>MSysObjects</c> / <c>MSysACEs</c> rows.</param>
/// <param name="catalogArtifacts">Creates tables and applies catalog replacement plans.</param>
/// <param name="complexColumns">Allocates, emits, re-parents, drops, and renames complex-column artifacts.</param>
/// <param name="constraints">Carries client-side column constraints across schema changes.</param>
/// <param name="snapshots">Reads rows, index metadata, and persisted column properties before a rebuild.</param>
internal sealed class TableSchemaEditor(
    DatabaseFile db,
    TableCatalog catalog,
    TableRowStore tableRows,
    IndexMaintainer indexMaintainer,
    PageAllocator pageAllocator,
    LongValueEncoder longValueEncoder,
    CatalogWriter catalogWriter,
    CatalogArtifactWriter catalogArtifacts,
    ComplexColumnManager complexColumns,
    ConstraintRegistry constraints,
    TableSnapshotReader snapshots)
{
    internal async ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        Guard.NotNull(indexes, nameof(indexes));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required", nameof(columns));
        }

        // Pre-process the column-level IsPrimaryKey shortcut. Synthesize one
        // composite PK IndexDefinition (named "PrimaryKey") from columns
        // marked IsPrimaryKey=true, in declaration order, and force those
        // columns to IsNullable=false on the emitted TDEF. Mixing the
        // shortcut with an explicit PK IndexDefinition is rejected.
        (columns, indexes) = IndexHelpers.ApplyPrimaryKeyShortcut(columns, indexes);

        // Unsupported Jet4 key types (OLE / Attachment / Multi-Value) are
        // rejected up-front below in ResolveIndexes.

        if (await catalog.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        // Complex columns (Attachment / MultiValue) declared by the user have
        // ComplexId = 0; allocate fresh per-database ComplexIDs, then emit the hidden
        // flat child table + MSysComplexColumns row per column AFTER the parent TDEF
        // is on disk. The round-trip preservation path on RewriteTableAsync supplies a
        // non-zero ComplexId from the original TDEF and is left untouched here.
        IReadOnlyList<ComplexColumnAllocation>? complexAllocs =
            await complexColumns.PrepareComplexColumnAllocationsAsync(columns, cancellationToken).ConfigureAwait(false);
        if (complexAllocs is { Count: > 0 })
        {
            // Rewrite the column list with the allocated ComplexIds embedded so the parent
            // TDEF's misc slot points at the soon-to-be-emitted MSysComplexColumns rows.
            var rewritten = new List<ColumnDefinition>(columns);
            for (int i = 0; i < complexAllocs.Count; i++)
            {
                ComplexColumnAllocation a = complexAllocs[i];
                rewritten[a.ColumnIndex] = rewritten[a.ColumnIndex] with { ComplexId = a.ComplexId };
            }

            columns = rewritten;
        }

        uint catalogFlags = 0;
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].IsAttachment || columns[i].IsMultiValue)
            {
                catalogFlags = 0x00040000U;
                break;
            }
        }

        var tableArtifact = new CatalogTableArtifact(tableName, columns, indexes, catalogFlags);
        long tdefPageNumber = await catalogArtifacts.CreateTableAsync(tableArtifact, cancellationToken).ConfigureAwait(false);

        // Emit the hidden flat child table + MSysComplexColumns row for every
        // user-declared complex column. Done after the parent table is on disk so the
        // catalog cache reflects the parent before flat-table inserts.
        if (complexAllocs is { Count: > 0 })
        {
            await complexColumns.EmitComplexColumnArtifactsAsync(tableName, tdefPageNumber, columns, complexAllocs, cancellationToken).ConfigureAwait(false);
        }

        _ = tdefPageNumber;
    }

    internal async ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        await this.DropTableCoreAsync(tableName, dropComplexChildren: true, cancellationToken).ConfigureAwait(false);
    }

    internal ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(column, nameof(column));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        return this.RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                if (existing.Exists(c => string.Equals(c.Name, column.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidOperationException($"Column '{column.Name}' already exists in table '{tableName}'.");
                }

                return [.. existing, column];
            },
            (oldRow, _) =>
            {
                object[] next = new object[oldRow.Length + 1];
                Array.Copy(oldRow, 0, next, 0, oldRow.Length);
                next[oldRow.Length] = DBNull.Value;
                return next;
            },
            cancellationToken);
    }

    internal ValueTask DropColumnAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        int dropIndex = -1;
        return this.RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                dropIndex = existing.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
                if (dropIndex < 0)
                {
                    throw new ArgumentException($"Column '{columnName}' was not found in table '{tableName}'.", nameof(columnName));
                }

                if (existing.Count == 1)
                {
                    throw new InvalidOperationException($"Cannot drop the last remaining column from table '{tableName}'.");
                }

                var next = new List<ColumnDefinition>(existing);
                next.RemoveAt(dropIndex);
                return next;
            },
            (oldRow, _) =>
            {
                object[] next = new object[oldRow.Length - 1];
                int j = 0;
                for (int i = 0; i < oldRow.Length; i++)
                {
                    if (i == dropIndex)
                    {
                        continue;
                    }

                    next[j++] = oldRow[i];
                }

                return next;
            },
            cancellationToken);
    }

    internal ValueTask RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(oldColumnName, nameof(oldColumnName));
        Guard.NotNullOrEmpty(newColumnName, nameof(newColumnName));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        return this.RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                int idx = existing.FindIndex(c => string.Equals(c.Name, oldColumnName, StringComparison.OrdinalIgnoreCase));
                if (idx < 0)
                {
                    throw new ArgumentException($"Column '{oldColumnName}' was not found in table '{tableName}'.", nameof(oldColumnName));
                }

                if (existing.Exists(c => string.Equals(c.Name, newColumnName, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidOperationException($"Column '{newColumnName}' already exists in table '{tableName}'.");
                }

                var next = new List<ColumnDefinition>(existing);
                ColumnDefinition src = next[idx];
                next[idx] = new ColumnDefinition(newColumnName, src.ClrType, src.MaxLength)
                {
                    IsNullable = src.IsNullable,
                    DefaultValue = src.DefaultValue,
                    IsAutoIncrement = src.IsAutoIncrement,
                    IsHyperlink = src.IsHyperlink,
                    IsDateTimeExtended = src.IsDateTimeExtended,
                    ValidationRule = src.ValidationRule,
                    DefaultValueExpression = src.DefaultValueExpression,
                    ValidationRuleExpression = src.ValidationRuleExpression,
                    ValidationText = src.ValidationText,
                    Description = src.Description,

                    // Forward complex-column flags so the rebuilt TDEF re-emits
                    // a complex descriptor with the original ComplexId in the
                    // misc slot. RewriteTableAsync uses the preserved ComplexId
                    // to update MSysComplexColumns.ColumnName.
                    IsAttachment = src.IsAttachment,
                    IsMultiValue = src.IsMultiValue,
                    MultiValueElementType = src.MultiValueElementType,
                    ComplexId = src.ComplexId,
                };
                return next;
            },
            (oldRow, _) => oldRow,
            cancellationToken,
            projectIndexes: (existingIndexes, newDefs) =>
            {
                var newColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (ColumnDefinition c in newDefs)
                {
                    newColumnNames.Add(c.Name);
                }

                var result = new List<IndexDefinition>(existingIndexes.Count);
                foreach (IndexMetadata idx in existingIndexes)
                {
                    // Forward Normal (1..N column) and PrimaryKey indexes;
                    // FK indexes are reconstructed from MSysRelationships.
                    if (idx.Kind is not IndexKind.Normal and not IndexKind.PrimaryKey)
                    {
                        continue;
                    }

                    var remappedCols = new List<string>(idx.Columns.Count);
                    var descendingCols = new List<string>();
                    bool allSurvive = true;
                    foreach (IndexColumnReference ic in idx.Columns)
                    {
                        string keyColumn = ic.Name;
                        string remapped = string.Equals(keyColumn, oldColumnName, StringComparison.OrdinalIgnoreCase)
                            ? newColumnName
                            : keyColumn;

                        if (string.IsNullOrEmpty(remapped) || !newColumnNames.Contains(remapped))
                        {
                            allSurvive = false;
                            break;
                        }

                        remappedCols.Add(remapped);
                        if (!ic.IsAscending)
                        {
                            descendingCols.Add(remapped);
                        }
                    }

                    if (!allSurvive)
                    {
                        continue;
                    }

                    if (idx.Kind == IndexKind.PrimaryKey)
                    {
                        result.Add(new IndexDefinition(idx.Name, remappedCols)
                        {
                            IsPrimaryKey = true,
                            DescendingColumns = descendingCols,
                            IgnoreNulls = idx.IgnoreNulls,
                        });
                    }
                    else
                    {
                        result.Add(new IndexDefinition(idx.Name, remappedCols)
                        {
                            IsUnique = idx.HasUniqueFlag,
                            DescendingColumns = descendingCols,
                            IgnoreNulls = idx.IgnoreNulls,
                            IsRequired = idx.IsRequired,
                        });
                    }
                }

                return result;
            });
    }

    private async ValueTask RewriteTableAsync(
        string tableName,
        Func<List<ColumnDefinition>, TableDef, List<ColumnDefinition>> projectColumns,
        Func<object[], TableDef, object[]> projectRow,
        CancellationToken cancellationToken,
        Func<IReadOnlyList<IndexMetadata>, IReadOnlyList<ColumnDefinition>, List<IndexDefinition>>? projectIndexes = null)
    {
        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = table.Entry;
        TableDef tableDef = table.Definition;

        // Carry forward any client-side constraints registered for the original schema so
        // Add/Drop/Rename do not silently strip NotNull / Default / AutoIncrement / validation rules.
        constraints.TryGet(tableName, out List<ColumnConstraint>? existingConstraints);

        // Hydrate persisted-property fields from MSysObjects.LvProp so that
        // DefaultValueExpression / ValidationRuleExpression / ValidationText / Description
        // round-trip through Add/Drop/Rename semantically. Forward-compat note: unknown
        // chunks and table-level property targets are intentionally not preserved by this path.
        ColumnPropertyBlock? originalProperties =
            await snapshots.ReadLvPropBlockAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

        var existingDefs = new List<ColumnDefinition>(tableDef.Columns.Count);
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            ColumnDefinition baseDef = this.BuildColumnDefinitionFromInfo(col, originalProperties);
            if (existingConstraints != null && i < existingConstraints.Count
                && string.Equals(existingConstraints[i].Name, col.Name, StringComparison.OrdinalIgnoreCase))
            {
                ColumnConstraint c = existingConstraints[i];
                baseDef = baseDef with
                {
                    IsNullable = c.IsNullable,
                    DefaultValue = c.DefaultValue,
                    IsAutoIncrement = c.IsAutoIncrement,
                    ValidationRule = c.ValidationRule,
                };
            }

            ColumnPropertyTarget? target = originalProperties?.FindTarget(col.Name);
            if (target is not null)
            {
                baseDef = baseDef with
                {
                    DefaultValueExpression = target.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, db.Format)
                        ?? baseDef.DefaultValueExpression,
                    ValidationRuleExpression = target.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, db.Format)
                        ?? baseDef.ValidationRuleExpression,
                    ValidationText = target.GetTextValue(Constants.ColumnPropertyNames.ValidationText, db.Format)
                        ?? baseDef.ValidationText,
                    Description = target.GetTextValue(Constants.ColumnPropertyNames.Description, db.Format)
                        ?? baseDef.Description,
                };
            }

            existingDefs.Add(baseDef);
        }

        List<ColumnDefinition> newDefs = projectColumns(existingDefs, tableDef);
        if (newDefs.Count == 0)
        {
            throw new InvalidOperationException($"Table '{tableName}' must retain at least one column.");
        }

        // Snapshot existing rows AND existing indexes BEFORE we mutate the catalog,
        // so the snapshot reader sees the original schema and we can forward
        // surviving index definitions to the rebuilt table.
        using DataTable snapshot = await snapshots.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<IndexMetadata> existingIndexes = await snapshots.ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        // Default index projection: keep every existing index whose single key
        // column survives in the new schema (matched by case-insensitive name).
        // AddColumn / DropColumn use this default; RenameColumn supplies a custom
        // projection that rewrites references to the renamed column.
        List<IndexDefinition> projectedIndexes = projectIndexes != null
            ? projectIndexes(existingIndexes, newDefs)
            : IndexHelpers.DefaultIndexProjection(existingIndexes, newDefs);

        string tempName = $"~tmp_{Guid.NewGuid():N}"[..18];
        await this.CreateTableAsync(tempName, newDefs, projectedIndexes, cancellationToken).ConfigureAwait(false);

        ResolvedTable tempTable = await catalog.ResolveRequiredTableAsync(tempName, cancellationToken).ConfigureAwait(false);
        CatalogEntry tempEntry = tempTable.Entry;
        TableDef tempDef = tempTable.Definition;

        foreach (DataRow row in snapshot.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object?[] sourceItems = row.ItemArray;
            object[] sourceRow = new object[sourceItems.Length];
            for (int i = 0; i < sourceItems.Length; i++)
            {
                sourceRow[i] = sourceItems[i] ?? DBNull.Value;
            }

            object[] projected = projectRow(sourceRow, tableDef);
            await tableRows.InsertRowDataAsync(tempEntry.TDefPage, tempDef, projected, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        // rebuild forwarded indexes once after the bulk row copy completes,
        // so we don't pay the rebuild cost per row.
        if (projectedIndexes.Count > 0 && snapshot.Rows.Count > 0)
        {
            await indexMaintainer.MaintainIndexesAsync(tempEntry.TDefPage, tempDef, tempName, cancellationToken).ConfigureAwait(false);
        }

        // Drop the original table, then rename the temp catalog entry to take its place.
        // Pre-compute the LvProp blob from the projected columns so the catalog rename
        // re-emits the persisted properties under the user-facing table name.
        //
        // identify complex columns being dropped or renamed by this rewrite
        // BEFORE the cascade-skipping drop runs. Surviving complex columns (matched by
        // ComplexId between the existing and projected schemas) are preserved as-is —
        // their flat child tables and MSysComplexColumns rows stay attached to the
        // rebuilt parent. If no complex column is dropped or renamed, the temp table
        // is transplanted onto the original TDEF page so MSysComplexColumns keeps the
        // same parent object id; otherwise surviving rows are patched to the temp
        // TDEF page after the copy/swap. Dropped complex columns get their flat child
        // + catalog row removed surgically; renamed complex columns get their
        // MSysComplexColumns row rewritten with the new ColumnName.
        Dictionary<int, ColumnDefinition> newComplexById = [];
        foreach (ColumnDefinition c in newDefs)
        {
            if ((c.IsAttachment || c.IsMultiValue) && c.ComplexId != 0)
            {
                newComplexById[c.ComplexId] = c;
            }
        }

        var droppedComplex = new List<(string Name, int ComplexId)>();
        var renamedComplex = new List<(string OldName, string NewName, int ComplexId)>();
        foreach (ColumnDefinition c in existingDefs)
        {
            if (!(c.IsAttachment || c.IsMultiValue) || c.ComplexId == 0)
            {
                continue;
            }

            if (!newComplexById.TryGetValue(c.ComplexId, out ColumnDefinition? survivor))
            {
                droppedComplex.Add((c.Name, c.ComplexId));
            }
            else if (!string.Equals(survivor.Name, c.Name, StringComparison.OrdinalIgnoreCase))
            {
                renamedComplex.Add((c.Name, survivor.Name, c.ComplexId));
            }
        }

        byte[]? renamedLvProp = JetExpressionConverter.BuildLvPropBlob(newDefs, db.Format);
        if (newComplexById.Count > 0 && droppedComplex.Count == 0 && renamedComplex.Count == 0)
        {
            await this.TransplantTempTableToOriginalAsync(
                tableName,
                entry.TDefPage,
                tempName,
                tempEntry.TDefPage,
                renamedLvProp,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        await this.DropTableCoreAsync(tableName, dropComplexChildren: false, cancellationToken).ConfigureAwait(false);
        await catalogWriter.RenameTableInCatalogAsync(tempName, tableName, renamedLvProp, cancellationToken).ConfigureAwait(false);

        foreach (ColumnDefinition survivor in newComplexById.Values)
        {
            await complexColumns.UpdateComplexColumnParentTableIdAsync(
                survivor.ComplexId,
                checked((int)tempEntry.TDefPage),
                cancellationToken).ConfigureAwait(false);
        }

        foreach ((string colName, int complexId) in droppedComplex)
        {
            await complexColumns.DropSingleComplexChildAsync(colName, complexId, cancellationToken).ConfigureAwait(false);
        }

        foreach ((string oldColName, string newColName, int complexId) in renamedComplex)
        {
            await complexColumns.RenameComplexColumnArtifactsAsync(oldColName, newColName, complexId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask TransplantTempTableToOriginalAsync(
        string tableName,
        long originalTdefPage,
        string tempName,
        long tempTdefPage,
        byte[]? lvProp,
        CancellationToken cancellationToken)
    {
        byte[] tempTdef = await db.ReadPageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (tempTdef[0] != Constants.PageTypes.TableDefinition || Ri32(tempTdef, 4) != 0)
            {
                throw new NotSupportedException("Complex table schema rewrite currently requires a single-page rebuilt TDEF.");
            }

            await this.ReclaimTableStoragePagesAsync(originalTdefPage, includeTDefRoot: false, cancellationToken).ConfigureAwait(false);
            await this.PatchTablePageOwnersAsync(tempTdefPage, originalTdefPage, cancellationToken).ConfigureAwait(false);
            await db.WritePageAsync(originalTdefPage, tempTdef, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnPage(tempTdef);
        }

        await catalogArtifacts.ExecutePlanAsync(
            new CatalogArtifactPlan([], [])
            {
                CatalogReplacements =
                [
                    new UserTableCatalogReplacementArtifact(
                        tableName,
                        tableName,
                        originalTdefPage,
                        lvProp,
                        Operation: $"replacing catalog row for '{tableName}'",
                        MissingMessage: $"Catalog row for '{tableName}' was not found during schema rewrite."),
                ],
                CatalogDeletions =
                [
                    new UserTableCatalogDeletionArtifact(
                        tempName,
                        tempTdefPage,
                        Operation: $"deleting catalog row for '{tempName}'"),
                ],
            },
            cancellationToken).ConfigureAwait(false);
        await catalogWriter.DeleteAceRowsForObjectIdsAsync([tempTdefPage], cancellationToken).ConfigureAwait(false);
        await pageAllocator.DeallocatePageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask PatchTablePageOwnersAsync(long fromTdefPage, long toTdefPage, CancellationToken cancellationToken)
    {
        long totalPages = db.PhysicalPageCount;
        for (long pageNumber = 3; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await db.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                bool patchDataPage = page[0] == Constants.PageTypes.Data && Ri32(page, db.DataPage.TDefOff) == fromTdefPage;
                bool patchIndexPage = page[0] is Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf && Ri32(page, 4) == fromTdefPage;
                if (!patchDataPage && !patchIndexPage)
                {
                    continue;
                }

                int ownerOffset = patchDataPage ? db.DataPage.TDefOff : 4;
                Wi32(page, ownerOffset, checked((int)toTdefPage));
                await db.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ReturnPage(page);
            }
        }
    }

    private ColumnDefinition BuildColumnDefinitionFromInfo(ColumnInfo column, ColumnPropertyBlock? properties = null)
    {
        ColumnDefinition baseDef;
        switch (column.Type)
        {
            case TextType:
                int textSize = column.IsCalculated
                    ? Math.Max(0, column.Size - Constants.CalculatedColumn.ExtraDataLen)
                    : column.Size;
                int charLen = db.Format != DatabaseFormat.Jet3Mdb ? Math.Max(1, textSize / 2) : Math.Max(1, textSize);
                baseDef = new ColumnDefinition(column.Name, typeof(string), charLen);
                break;
            case BinaryType:
                int binarySize = column.IsCalculated
                    ? Math.Max(0, column.Size - Constants.CalculatedColumn.ExtraDataLen)
                    : column.Size;
                baseDef = new ColumnDefinition(column.Name, typeof(byte[]), binarySize > 0 ? binarySize : 255);
                break;
            case AttachmentType:
                // preserve attachment columns across
                // AddColumnAsync / DropColumnAsync / RenameColumnAsync. The parent
                // TDEF descriptor round-trips with the ComplexID intact (ColumnInfo.Misc
                // → ColumnDefinition.ComplexId → re-emitted into the rebuilt TDEF's
                // misc slot), and the existing hidden flat child table + MSysComplexColumns
                // row are kept attached because the rewrite path skips the cascade-on-drop
                // step. Per-row complex slot is null on the rebuilt parent (same as fresh
                // Insert), and the reader re-joins via the parent's auto-number primary
                // key against the flat table's `_<columnName>` FK back-reference.
                return new ColumnDefinition(column.Name, typeof(byte[]))
                {
                    IsAttachment = true,
                    ComplexId = column.Misc,
                };
            case ComplexType:
                // Access stores all complex parent descriptors as the generic
                // 0x12 type; the subtype lives in MSysComplexColumns. This
                // rewrite path only needs a generic complex marker and the
                // preserved ComplexId, so the existing flat child table remains
                // attached without allocating a new one.
                return new ColumnDefinition(column.Name, typeof(byte[]))
                {
                    IsMultiValue = true,
                    ComplexId = column.Misc,
                };
            case BooleanType:
            case ByteType:
            case IntegerType:
            case LongIntegerType:
            case MoneyType:
            case FloatType:
            case DoubleType:
            case DateTimeType:
            case OleType:
            case MemoType:
            case GuidType:
            case NumericType:
            case BigIntType:
                Type clrType = GetClrType(column.Type)
                    ?? throw new NotSupportedException($"Column '{column.Name}' has unsupported type {GetTypeDisplayName(column.Type)}.");
                baseDef = new ColumnDefinition(column.Name, clrType)
                {
                    ColumnTypeOverride = column.Type,
                };
                break;
            case DateTimeExtendedType:
                baseDef = new ColumnDefinition(column.Name, typeof(DateTime))
                {
                    IsDateTimeExtended = true,
                    ColumnTypeOverride = column.Type,
                };
                break;
            default:
                throw new InvalidOperationException($"Column '{column.Name}' has unknown type {GetTypeDisplayName(column.Type)}.");
        }

        // Surface the persisted TDEF flag bits as ColumnDefinition properties so the
        // schema-rewrite path retains NOT NULL / auto-increment metadata that Access
        // wrote into the original column descriptor. Complex columns (Attachment /
        // Complex) return early above because their Flags byte is the magic 0x07
        // marker rather than real flag bits.
        bool isAutoIncrement = (column.Flags & Constants.ColumnDescriptorFlags.AutoNumber) != 0;
        bool? requiredFromLvProp = properties?.FindTarget(column.Name)?
            .GetBooleanValue(Constants.ColumnPropertyNames.Required);
        bool isNullable = !isAutoIncrement && (requiredFromLvProp is bool req
                ? !req
                : (column.Flags & Constants.ColumnDescriptorFlags.LegacyNotNull) == 0);

        ColumnDefinition def = baseDef with
        {
            IsNullable = isNullable,
            IsAutoIncrement = isAutoIncrement,
            IsHyperlink = column.Type == MemoType && (column.Flags & Constants.ColumnDescriptorFlags.Hyperlink) != 0,
            IsDateTimeExtended = column.Type == DateTimeExtendedType,
            IsCompressedUnicode = column.IsCompressedUnicode,
        };

        // Preserve declared precision/scale through the schema-rewrite copy so
        // AddColumn / DropColumn / RenameColumn don't silently reset a NUMERIC
        // column to default 18/0. Access-authored files always populate these
        // descriptor bytes for Numeric columns.
        if (column.Type == NumericType)
        {
            def = def with { NumericPrecision = column.NumericPrecision, NumericScale = column.NumericScale };
        }

        if (column.IsCalculated)
        {
            ColumnPropertyTarget? target = properties?.FindTarget(column.Name);
            byte resultType = (byte)column.Type;
            ColumnPropertyEntry? resultTypeEntry = target?.Find(Constants.ColumnPropertyNames.ResultType);
            if (resultTypeEntry?.Value.Length >= 1)
            {
                resultType = resultTypeEntry.Value[0];
            }

            def = def with
            {
                IsCalculated = true,
                CalculationExpression = target?.GetTextValue(Constants.ColumnPropertyNames.Expression, db.Format),
                CalculatedResultType = resultType,
                IsCompressedUnicode = false,
            };
        }

        return def;
    }

    /// <summary>
    /// Shared implementation backing <see cref="DropTableAsync"/> and the
    /// <c>RewriteTableAsync</c> path. The <paramref name="dropComplexChildren"/>
    /// flag is set to <see langword="false"/> by the rewrite path so that the
    /// hidden flat child tables and matching <c>MSysComplexColumns</c> rows for
    /// surviving complex columns stay attached to the rebuilt parent.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="dropComplexChildren">The drop complex children.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <c>MSysObjects</c> is missing or no matching user table exists.</exception>
    private async ValueTask DropTableCoreAsync(string tableName, bool dropComplexChildren, CancellationToken cancellationToken)
    {
        UserTableCatalogDeletionResult deleted = await catalogWriter.DeleteUserTableCatalogRowsAsync(
            tableName,
            tdefPage: null,
            includeSystemTables: false,
            throwIfNotFound: true,
            operation: $"dropping table '{tableName}'",
            missingMessage: $"Table '{tableName}' does not exist.",
            cancellationToken).ConfigureAwait(false);

        if (dropComplexChildren)
        {
            foreach (long parentTdefPage in deleted.TDefPages)
            {
                await complexColumns.DropComplexChildrenForTableAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
            }
        }

        await catalogWriter.DeleteAceRowsForObjectIdsAsync(deleted.TDefPages, cancellationToken).ConfigureAwait(false);

        foreach (long tdefPage in deleted.TDefPages)
        {
            await this.ReclaimDroppedTablePagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        }

        constraints.Unregister(tableName);
        catalog.Invalidate();
    }

    private ValueTask ReclaimDroppedTablePagesAsync(long tdefPage, CancellationToken cancellationToken)
        => this.ReclaimTableStoragePagesAsync(tdefPage, includeTDefRoot: true, cancellationToken);

    private async ValueTask ReclaimTableStoragePagesAsync(long tdefPage, bool includeTDefRoot, CancellationToken cancellationToken)
    {
        var pagesToFree = new SortedSet<long>();
        var longValueRoots = new List<LongValueDescriptor>();

        TableDef? tableDef = await db.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long totalPages = db.PhysicalPageCount;
        if (tableDef is not null)
        {
            await db.ForEachOwnedDataPageAsync(
                tdefPage,
                (pageNumber, page, _) =>
                {
                    pagesToFree.Add(pageNumber);
                    foreach (RowBound rowBound in db.EnumerateLiveRowBounds(page))
                    {
                        longValueRoots.AddRange(longValueEncoder.CollectLongValueRoots(page, rowBound, tableDef));
                    }

                    return new ValueTask<bool>(true);
                },
                cancellationToken).ConfigureAwait(false);
        }

        byte[]? firstTdefPage = null;
        var seenTdefPages = new HashSet<long>();
        long currentTdefPage = tdefPage;
        while (currentTdefPage > 0 && currentTdefPage < totalPages && seenTdefPages.Add(currentTdefPage))
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] page = await db.ReadPageAsync(currentTdefPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.TableDefinition)
                {
                    break;
                }

                if (includeTDefRoot || currentTdefPage != tdefPage)
                {
                    _ = pagesToFree.Add(currentTdefPage);
                }

                firstTdefPage ??= (byte[])page.Clone();
                currentTdefPage = Ri32(page, 4);
            }
            finally
            {
                ReturnPage(page);
            }
        }

        if (firstTdefPage is not null && db.Format != DatabaseFormat.Jet3Mdb)
        {
            int usageMapPage = UsageMap.ReadUInt24(firstTdefPage, Constants.TableDefinition.OwnedPagesPageOffset);
            if (usageMapPage > 0)
            {
                _ = pagesToFree.Add(usageMapPage);
                await this.CollectIndexPagesFromUsageMapAsync(usageMapPage, pagesToFree, cancellationToken).ConfigureAwait(false);
            }
        }

        foreach (LongValueDescriptor root in longValueRoots)
        {
            await longValueEncoder.DeallocateLongValueAsync(root, cancellationToken).ConfigureAwait(false);
        }

        foreach (long pageNumber in pagesToFree)
        {
            if (pageNumber > 2)
            {
                await pageAllocator.DeallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask CollectIndexPagesFromUsageMapAsync(long usageMapPageNumber, SortedSet<long> pagesToFree, CancellationToken cancellationToken)
    {
        long totalPages = db.PhysicalPageCount;
        if (usageMapPageNumber <= 0 || usageMapPageNumber >= totalPages)
        {
            return;
        }

        byte[] page = await db.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.Data)
            {
                return;
            }

            foreach (RowBound rowBound in db.EnumerateLiveRowBounds(page))
            {
                if (rowBound.RowIndex < 2)
                {
                    continue;
                }

                var indexPages = new List<long>();
                if (!await UsageMap.TryEnumeratePagesAsync(
                    page,
                    rowBound,
                    db.PageSizeBytes,
                    totalPages,
                    minimumPageNumber: 3,
                    strict: false,
                    db.ReadPageAsync,
                    ReturnPage,
                    indexPages,
                    cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                foreach (long pageNumber in indexPages)
                {
                    _ = pagesToFree.Add(pageNumber);
                }
            }
        }
        finally
        {
            ReturnPage(page);
        }
    }
}
