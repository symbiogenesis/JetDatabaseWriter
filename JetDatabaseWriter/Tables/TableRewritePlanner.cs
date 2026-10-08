namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Prepares schema, properties, row projections and dependent identities for table rewrites.</summary>
/// <param name="format">The format service.</param>
/// <param name="catalog">The catalog service.</param>
/// <param name="indexMaintainer">The indexMaintainer service.</param>
/// <param name="catalogArtifacts">The catalogArtifacts service.</param>
/// <param name="constraints">The constraints service.</param>
/// <param name="relationships">The relationships service.</param>
/// <param name="snapshots">The snapshots service.</param>
/// <param name="autoNumbers">The autoNumbers service.</param>
internal sealed class TableRewritePlanner(
    JetFormat format,
    TableCatalog catalog,
    IndexMaintainer indexMaintainer,
    CatalogArtifactWriter catalogArtifacts,
    ConstraintRegistry constraints,
    RelationshipManager relationships,
    TableSnapshotReader snapshots,
    AutoNumberMaintainer autoNumbers)
{
    /// <summary>Captures and validates the complete rewrite projection before creating its replacement table.</summary>
    /// <param name="tableName">The original table.</param>
    /// <param name="projectColumns">The schema projection.</param>
    /// <param name="projectRow">The row projection.</param>
    /// <param name="mapColumnName">The column identity projection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The validated replacement and dependent metadata.</returns>
    internal async ValueTask<TableRewritePlan> PrepareAsync(
        string tableName,
        Func<List<ColumnDefinition>, TableDef, List<ColumnDefinition>> projectColumns,
        Func<object[], TableDef, object[]> projectRow,
        Func<string, string?> mapColumnName,
        CancellationToken cancellationToken)
    {
        await catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);
        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        CatalogEntry entry = table.Entry;
        TableDef tableDef = table.Definition;

        // Carry forward any client-side constraints registered for the original schema so
        // Add/Drop/Rename do not silently strip NotNull / Default / AutoIncrement / validation rules.
        constraints.TryGet(tableName, out List<ColumnConstraint>? existingConstraints);

        // Hydrate persisted-property fields from MSysObjects.LvProp so that
        // DefaultValueExpression / ValidationRuleExpression / ValidationText / Description
        // round-trip through Add/Drop/Rename semantically. The rebuilt table's blob is
        // this one projected onto the new columns (PersistedPropertyProjector), so the
        // properties the writer does not model are kept too.
        ColumnPropertyBlock? originalProperties =
            await snapshots.ReadLvPropBlockAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        ConstraintRegistry.ValidatePersistedConstraintProperties(tableName, originalProperties);

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
                    DefaultValueExpression = PersistedExpressionText.Normalize(target.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, format))
                        ?? baseDef.DefaultValueExpression,
                    ValidationRuleExpression = PersistedExpressionText.Normalize(target.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, format))
                        ?? baseDef.ValidationRuleExpression,
                    ValidationText = target.GetTextValue(Constants.ColumnPropertyNames.ValidationText, format)
                        ?? baseDef.ValidationText,
                    Description = target.GetTextValue(Constants.ColumnPropertyNames.Description, format)
                        ?? baseDef.Description,
                };
            }

            existingDefs.Add(baseDef);
        }

        List<ColumnDefinition> newDefs = projectColumns(existingDefs, tableDef);
        if (newDefs.Count == 0)
        {
            throw new JetOperationException(JetErrorCode.LastColumn, $"Table '{tableName}' must retain at least one column.", errorInfo: new JetErrorInfo { TableName = tableName });
        }

        if (newDefs.Count > Constants.TableDefinition.MaxTableColumns)
        {
            throw new JetLimitationException($"Table '{tableName}' would have {newDefs.Count} columns; a Microsoft Access table holds at most {Constants.TableDefinition.MaxTableColumns}.");
        }

        // Carry renamed columns into the expressions that name them, and refuse
        // to drop a column one of them names, before anything is written; the
        // LvProp blob and the constraint registry are both built from newDefs.
        ProjectExpressionReferences(tableName, existingDefs, newDefs, mapColumnName);

        // Project the stored properties onto the new columns, and serialize them,
        // before anything is written: the rebuilt table's catalog row carries them.
        originalProperties = this.ProjectTableRuleReferences(tableName, originalProperties, existingDefs, mapColumnName);
        ColumnPropertyBlock persistedProperties =
            PersistedPropertyProjector.ProjectForRewrite(originalProperties, existingDefs, newDefs, mapColumnName, format);
        byte[]? persistedLvProp = persistedProperties.ToBytes(format);

        // Capture the table's relationship state, and refuse to drop a
        // relationship key column, before anything is written.
        RelationshipRewriteState relationshipState =
            await relationships.CaptureForRewriteAsync(tableName, entry.TDefPage, tableDef, cancellationToken).ConfigureAwait(false);
        RelationshipManager.EnsureKeyColumnsSurvive(relationshipState, mapColumnName);
        if (!relationshipState.IsEmpty)
        {
            await catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: true, cancellationToken).ConfigureAwait(false);
        }

        // Refuse, before anything is written, a table with an index the writer
        // cannot maintain: the projection below could only drop such an index,
        // or never see it, so the rewrite would lose it without a word.
        await indexMaintainer.ThrowIfIndexesUnmaintainableAsync(entry.TDefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);

        // Snapshot existing rows AND existing indexes BEFORE we mutate the catalog,
        // so the snapshot reader sees the original schema and we can forward
        // surviving index definitions to the rebuilt table.
        using DataTable snapshot = await snapshots.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        IReadOnlyList<IndexMetadata> existingIndexes = await snapshots.ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);

        // Forward every Normal / PrimaryKey index whose key columns all survive,
        // renamed through mapColumnName, with its flags. FK indexes are
        // re-emitted below by the relationship manager.
        List<IndexDefinition> projectedIndexes = IndexHelpers.ProjectIndexes(existingIndexes, newDefs, mapColumnName);

        // Project every row before creating the temp table, so a MEMO / OLE value
        // the snapshot could not read refuses the rewrite before anything changes.
        // A dropped column's unreadable value is projected away and does not block.
        var projectedRows = new List<object[]>(snapshot.Rows.Count);
        foreach (DataRow row in snapshot.Rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            object[] projected = projectRow(TableSnapshotReader.GetDbNullNormalizedItemArray(row), tableDef);
            UnreadableLongValue.ThrowIfAny(projected, tableName);
            projectedRows.Add(projected);
        }

        await this.InitializeNewComplexReferencesAsync(tableName, tableDef, newDefs, projectedRows, cancellationToken).ConfigureAwait(false);

        // Identify complex columns being dropped or renamed by this rewrite
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
            else if (!string.Equals(survivor.Name, c.Name, StringComparison.Ordinal))
            {
                // Ordinal, so a case-only rename also rewrites ColumnName.
                renamedComplex.Add((c.Name, survivor.Name, c.ComplexId));
            }
        }

        bool transplant = newComplexById.Count > 0 && droppedComplex.Count == 0 && renamedComplex.Count == 0;

        long autoNumberHighWater = await autoNumbers.ReadHighWaterAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        long complexHighWater = await autoNumbers.ReadComplexHighWaterAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return new TableRewritePlan(tableName, entry, tableDef, newDefs, projectedIndexes, persistedProperties,
            persistedLvProp, projectedRows, relationshipState, mapColumnName, newComplexById, droppedComplex, renamedComplex, autoNumberHighWater, complexHighWater, transplant);
    }

    /// <summary>
    /// Returns the payload size of a calculated Text or Binary column: its descriptor
    /// size less the calculated-value wrapper, or <paramref name="fallback"/> when the
    /// descriptor is another type or too small to hold the wrapper.
    /// </summary>
    /// <param name="column">The calculated column.</param>
    /// <param name="sizedByDescriptor">Whether the descriptor type is the result type.</param>
    /// <param name="fallback">The size to use when the descriptor does not give one.</param>
    private static int CalculatedPayloadSize(ColumnInfo column, bool sizedByDescriptor, int fallback)
        => sizedByDescriptor && column.Size > Constants.CalculatedColumn.ExtraDataLen
            ? column.Size - Constants.CalculatedColumn.ExtraDataLen
            : fallback;

    /// <summary>
    /// Carries a rewrite's column rename into every calculated expression,
    /// validation rule and default of the projected columns that names the
    /// renamed column: <c>[Old]</c> and a bare <c>Old</c> become
    /// <c>[New]</c>, so they keep evaluating, a reference qualified by the
    /// table's own name keeps the qualifier (<c>[T].[New]</c>, which the
    /// expression engine does not evaluate yet), and the rest of the text is
    /// kept as it was (<see cref="ExpressionFieldReferences"/>). Refuses to
    /// drop a column that one of them names, as for a relationship key
    /// column: the expression would name a column that no longer exists, and
    /// a rule or default would silently stop applying. <c>ValidationText</c>
    /// and <c>Description</c> are free text and are left alone.
    /// </summary>
    /// <param name="tableName">The table being rewritten, which may qualify a reference.</param>
    /// <param name="existingDefs">The columns before the rewrite.</param>
    /// <param name="newDefs">The projected columns, updated in place.</param>
    /// <param name="mapColumnName">Maps a current column name to its name after the rewrite, or to <see langword="null"/> for a dropped column.</param>
    /// <exception cref="ArgumentException">A renamed reference would push an expression past the engine's limits.</exception>
    /// <exception cref="InvalidOperationException">A surviving column's expression names a dropped column.</exception>
    private static void ProjectExpressionReferences(
        string tableName,
        List<ColumnDefinition> existingDefs,
        List<ColumnDefinition> newDefs,
        Func<string, string?> mapColumnName)
    {
        foreach (ColumnDefinition existing in existingDefs)
        {
            string? mapped = mapColumnName(existing.Name);
            if (mapped is null)
            {
                foreach (ColumnDefinition survivor in newDefs)
                {
                    ThrowIfNamesDroppedColumn(tableName, existing.Name, survivor, "calculated expression", survivor.CalculationExpression);
                    ThrowIfNamesDroppedColumn(tableName, existing.Name, survivor, "validation rule", survivor.ValidationRuleExpression);
                    ThrowIfNamesDroppedColumn(tableName, existing.Name, survivor, "default value", survivor.DefaultValueExpression);
                }

                continue;
            }

            if (string.Equals(mapped, existing.Name, StringComparison.Ordinal))
            {
                continue;
            }

            for (int i = 0; i < newDefs.Count; i++)
            {
                ColumnDefinition column = newDefs[i];
                string? calculation = RenameReference(tableName, column, "calculated expression", column.CalculationExpression, existing.Name, mapped);
                string? rule = RenameReference(tableName, column, "validation rule", column.ValidationRuleExpression, existing.Name, mapped);
                string? defaultValue = RenameReference(tableName, column, "default value", column.DefaultValueExpression, existing.Name, mapped);
                if (!ReferenceEquals(calculation, column.CalculationExpression)
                    || !ReferenceEquals(rule, column.ValidationRuleExpression)
                    || !ReferenceEquals(defaultValue, column.DefaultValueExpression))
                {
                    newDefs[i] = column with
                    {
                        CalculationExpression = calculation,
                        ValidationRuleExpression = rule,
                        DefaultValueExpression = defaultValue,
                    };
                }
            }
        }
    }

    /// <summary>
    /// Renames the references to <paramref name="oldColumnName"/> in one of
    /// <paramref name="column"/>'s expressions.
    /// </summary>
    /// <param name="tableName">The table being rewritten.</param>
    /// <param name="column">The column the expression belongs to.</param>
    /// <param name="property">The expression's property, for the message.</param>
    /// <param name="expression">The expression text, or <see langword="null"/>.</param>
    /// <param name="oldColumnName">The renamed column's current name.</param>
    /// <param name="newColumnName">The renamed column's new name.</param>
    /// <returns>The rewritten text, or <paramref name="expression"/> itself when it does not name the column.</returns>
    /// <exception cref="ArgumentException">The rewritten text breaks a limit the original met.</exception>
    private static string? RenameReference(string tableName, ColumnDefinition column, string property, string? expression, string oldColumnName, string newColumnName)
    {
        if (expression is null || !ExpressionFieldReferences.References(expression, oldColumnName, tableName))
        {
            return expression;
        }

        // RenameColumnAsync has checked the new name against the Access
        // naming rules, which exclude ']', so it can be written as [New]. A
        // bare reference gains brackets and the new name may be longer. An
        // expression past the limits stops evaluating (a rule or default
        // silently), so refuse a rename that would push one over them.
        string renamed = ExpressionFieldReferences.Rename(expression, oldColumnName, newColumnName, tableName)!;
        if (!FitsExpressionLimits(renamed) && FitsExpressionLimits(expression))
        {
            throw new ArgumentException(
                $"Cannot rename column '{oldColumnName}' of table '{tableName}' to '{newColumnName}': the {property} of column '{column.Name}' ('{expression}') names it, and with the new name it would exceed the expression limits ({CalculatedExpressionLimits.MaxExpressionLength} characters, {CalculatedExpressionLimits.MaxColumnReferences} [field] references).",
                nameof(newColumnName));
        }

        return renamed;
    }

    private static void ThrowIfNamesDroppedColumn(string tableName, string droppedColumn, ColumnDefinition survivor, string property, string? expression)
    {
        if (ExpressionFieldReferences.References(expression, droppedColumn, tableName))
        {
            throw new JetOperationException(JetErrorCode.ColumnReferencedByExpression, $"Cannot drop column '{droppedColumn}' from table '{tableName}': the {property} of column '{survivor.Name}' ('{expression}') names it. Change or drop that column first.", errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = droppedColumn });
        }
    }

    private static bool FitsExpressionLimits(string expression)
    {
        try
        {
            CalculatedExpressionLimits.ValidateExpressionShape(expression, CalculatedExpressionLimits.MaxExpressionLength, "Expression");
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    /// <summary>
    /// Returns the per-row reference shared by every surviving complex column,
    /// or <see langword="null"/> when there are no surviving complex columns.
    /// Invalid or disagreeing persisted references are corruption.
    /// </summary>
    /// <param name="tableName">The table being rewritten.</param>
    /// <param name="tdefPage">The original table-definition page.</param>
    /// <param name="row">The projected row.</param>
    /// <param name="survivingColumns">The indexes of existing complex columns in the projected row.</param>
    /// <param name="newDefs">The projected column list.</param>
    /// <exception cref="JetCorruptDataException">A surviving slot is invalid or disagrees with the row's shared reference.</exception>
    private static int? SharedComplexReference(string tableName, long tdefPage, object[] row, List<int> survivingColumns, List<ColumnDefinition> newDefs)
    {
        int? shared = null;
        foreach (int i in survivingColumns)
        {
            ColumnDefinition column = newDefs[i];
            if (row[i] is not ComplexIdRef { Id: > 0 } reference)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptComplexColumn,
                    $"Parent row of '{tableName}' has no valid complex reference for '{column.Name}'.",
                    new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = tdefPage });
            }

            if (shared is int previous && previous != reference.Id)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptComplexColumn,
                    $"Parent row of '{tableName}' holds complex reference {reference.Id} for '{column.Name}' instead of its shared reference {previous}.",
                    new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = tdefPage });
            }

            shared = reference.Id;
        }

        return shared;
    }

    private static void FillNullComplexSlots(object[] row, List<int> complexColumns, int reference)
    {
        var value = new ComplexIdRef(reference);
        foreach (int i in complexColumns)
        {
            if (row[i] is null or DBNull)
            {
                row[i] = value;
            }
        }
    }

    /// <summary>Preserves a table rule's field dependencies through a schema rewrite.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="properties">The original properties.</param>
    /// <param name="existing">The original columns.</param>
    /// <param name="mapColumnName">The column projection.</param>
    /// <returns>The projected properties.</returns>
    /// <exception cref="JetOperationException">The rewrite drops a field used by the table rule.</exception>
    private ColumnPropertyBlock? ProjectTableRuleReferences(string tableName, ColumnPropertyBlock? properties, IReadOnlyList<ColumnDefinition> existing, Func<string, string?> mapColumnName)
    {
        string? expression = PersistedExpressionText.Normalize(properties?.FindTableTarget()?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, format));
        if (expression is null)
        {
            return properties;
        }

        string projected = expression;
        foreach (ColumnDefinition column in existing)
        {
            if (!ExpressionFieldReferences.References(expression, column.Name, tableName))
            {
                continue;
            }

            if (mapColumnName(column.Name) is not { } newName)
            {
                throw new JetOperationException(JetErrorCode.ColumnReferencedByExpression, $"Cannot drop column '{column.Name}' from table '{tableName}': its table validation rule ('{expression}') names it.", errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = column.Name });
            }

            projected = ExpressionFieldReferences.Rename(projected, column.Name, newName, tableName)!;
        }

        if (string.Equals(projected, expression, StringComparison.Ordinal))
        {
            return properties;
        }

        _ = CalculatedExpressionPlan.Parse(projected);
        var builder = ColumnPropertyBlockBuilder.FromBlock(properties!);
        ColumnPropertyTargetBuilder target = builder.GetOrAddTableTarget();
        int entryIndex = target.Entries.FindIndex(static entry => string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationRule, StringComparison.OrdinalIgnoreCase));
        ColumnPropertyEntryBuilder entry = target.Entries[entryIndex];
        target.AddText(entry.Name, projected, format);
        ColumnPropertyEntryBuilder replacement = target.Entries[^1];
        entry.Value = replacement.Value;
        target.Entries.RemoveAt(target.Entries.Count - 1);
        return ColumnPropertyBlock.Parse(builder.ToBytes(format), format);
    }

    /// <summary>
    /// Initializes newly added complex columns with per-row references after
    /// validating surviving slots independently of the allocation cache. Each
    /// existing column must uniquely identify its rows with positive references
    /// within the persisted counter, and a row's surviving columns must agree.
    /// Reuse that shared reference, or allocate one when no complex column survives.
    /// </summary>
    /// <param name="tableName">The table being rewritten.</param>
    /// <param name="tableDef">Its current definition.</param>
    /// <param name="newDefs">The projected column list.</param>
    /// <param name="rows">The projected rows, aligned with <paramref name="newDefs"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="JetCorruptDataException">An existing complex column has no valid ID, or its persisted references are invalid, duplicated, above the counter, or disagree within a row.</exception>
    private async ValueTask InitializeNewComplexReferencesAsync(
        string tableName,
        TableDef tableDef,
        List<ColumnDefinition> newDefs,
        List<object[]> rows,
        CancellationToken cancellationToken)
    {
        List<int> survivingColumns = [];
        List<int> addedColumns = [];
        for (int i = 0; i < newDefs.Count; i++)
        {
            if (newDefs[i].IsAttachment || newDefs[i].IsMultiValue)
            {
                if (newDefs[i].ComplexId == 0 && newDefs[i].SourceColumn is null)
                {
                    addedColumns.Add(i);
                }
                else
                {
                    survivingColumns.Add(i);
                }
            }
        }

        if (addedColumns.Count == 0)
        {
            return;
        }

        ResolvedTable parent = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        long tdefPage = parent.Entry.TDefPage;
        long counter = await autoNumbers.ReadComplexHighWaterAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        // Check each surviving column before deciding which reference to reuse.
        // A warm allocation cache must not bypass validation of stored data.
        foreach (int i in survivingColumns)
        {
            ColumnDefinition column = newDefs[i];
            if (column.ComplexId <= 0)
            {
                throw new JetCorruptDataException(
                    JetErrorCode.CorruptComplexColumn,
                    $"Existing complex column '{tableName}.{column.Name}' has no valid ComplexID.",
                    new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = tdefPage });
            }

            var holders = new HashSet<int>();
            foreach (object[] row in rows)
            {
                if (row[i] is not ComplexIdRef { Id: > 0 } reference)
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptComplexColumn,
                        $"Parent row of '{tableName}' has no valid complex reference for '{column.Name}'.",
                        new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = tdefPage });
                }

                if (reference.Id > counter)
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptComplexColumn,
                        $"Parent complex reference {reference.Id} for '{tableName}.{column.Name}' exceeds its persisted counter {counter}.",
                        new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = tdefPage });
                }

                if (!holders.Add(reference.Id))
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptComplexColumn,
                        $"Parent rows of '{tableName}' reuse complex reference {reference.Id} for '{column.Name}'.",
                        new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = tdefPage });
                }
            }
        }

        int?[] shared = new int?[rows.Count];
        for (int r = 0; r < rows.Count; r++)
        {
            shared[r] = SharedComplexReference(tableName, tdefPage, rows[r], survivingColumns, newDefs);
        }

        List<object[]> needing = [];
        for (int r = 0; r < rows.Count; r++)
        {
            object[] row = rows[r];
            if (!addedColumns.Exists(i => row[i] is null or DBNull))
            {
                continue;
            }

            // Initialize only added columns; surviving slots are preserved.
            if (shared[r] is int reference)
            {
                FillNullComplexSlots(row, addedColumns, reference);
            }
            else
            {
                needing.Add(row);
            }
        }

        if (needing.Count == 0)
        {
            return;
        }

        int first = await constraints.AllocateComplexReferencesAsync(tableName, tableDef, needing.Count, cancellationToken).ConfigureAwait(false);
        for (int k = 0; k < needing.Count; k++)
        {
            FillNullComplexSlots(needing[k], addedColumns, first + k);
        }
    }

    private ColumnDefinition BuildColumnDefinitionFromInfo(ColumnInfo column, ColumnPropertyBlock? properties = null)
    {
        // A calculated column is projected from its result type, which can differ
        // from its descriptor type in Access-authored tables (a Memo result in a
        // Text descriptor, a Currency result in a Double one). The
        // public declaration uses the result type; SourceColumn retains native storage.
        ColumnType valueType = ResolveValueType(column);
        bool sizedByDescriptor = !column.IsCalculated || valueType == column.Type;
        ColumnDefinition baseDef;
        switch (valueType)
        {
            case TextType:
                int textSize = column.IsCalculated ? CalculatedPayloadSize(column, sizedByDescriptor, Constants.CalculatedColumn.MaxTextResultBytes) : column.Size;
                int charLen = format.IsJet3 ? Math.Max(1, textSize) : Math.Max(1, textSize / 2);
                baseDef = new ColumnDefinition(column.Name, typeof(string), charLen);
                break;
            case BinaryType:
            case BigBinaryType:
                int binarySize = column.IsCalculated ? CalculatedPayloadSize(column, sizedByDescriptor, 0) : column.Size;
                baseDef = new ColumnDefinition(column.Name, typeof(byte[]), binarySize > 0 ? binarySize : 255)
                {
                    ColumnTypeOverride = valueType,
                };
                break;
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
                    SourceColumn = column,
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
                Type clrType = GetClrType(valueType)
                    ?? throw new NotSupportedException($"Column '{column.Name}' has unsupported type {GetTypeDisplayName(valueType)}.");
                baseDef = new ColumnDefinition(column.Name, clrType)
                {
                    ColumnTypeOverride = valueType,
                };
                break;
            case DateTimeExtendedType:
                baseDef = new ColumnDefinition(column.Name, typeof(DateTime))
                {
                    IsDateTimeExtended = true,
                    ColumnTypeOverride = valueType,
                };
                break;
            default:
                throw new InvalidOperationException($"Column '{column.Name}' has unknown type {GetTypeDisplayName(valueType)}.");
        }

        // Surface the persisted TDEF flag bits as ColumnDefinition properties so the
        // schema-rewrite path retains NOT NULL / auto-increment metadata that Access
        // wrote into the original column descriptor. Complex columns (Attachment /
        // Complex) return early above because their Flags byte is the magic 0x07
        // marker rather than real flag bits.
        bool isAutoIncrement = column.IsAutoNumber;
        bool? requiredFromLvProp = properties?.FindTarget(column.Name)?
            .GetBooleanValue(Constants.ColumnPropertyNames.Required);
        bool isNullable = !isAutoIncrement && requiredFromLvProp is not true;

        ColumnDefinition def = baseDef with
        {
            IsNullable = isNullable,
            IsAutoIncrement = isAutoIncrement,
            IsHyperlink = column.Type == MemoType && (column.Flags & Constants.ColumnDescriptorFlags.Hyperlink) != 0,
            IsDateTimeExtended = valueType == DateTimeExtendedType,
            IsCompressedUnicode = column.IsCompressedUnicode,
            TextSortOrderOverride = column.TextSortOrder,
        };

        // Preserve declared precision/scale through the schema-rewrite copy so
        // AddColumn / DropColumn / RenameColumn don't silently reset a NUMERIC
        // column to default 18/0. Access-authored files always populate these
        // descriptor bytes for Numeric columns.
        if (column.Type == NumericType && valueType == NumericType)
        {
            def = def with { NumericPrecision = column.NumericPrecision, NumericScale = column.NumericScale };
        }

        if (column.IsCalculated)
        {
            ColumnPropertyTarget? target = properties?.FindTarget(column.Name);
            byte resultType = (byte)valueType;
            ColumnPropertyEntry? resultTypeEntry = target?.Find(Constants.ColumnPropertyNames.ResultType);
            if (resultTypeEntry?.Value.Length >= 1)
            {
                resultType = resultTypeEntry.Value[0];
            }

            def = def with
            {
                IsCalculated = true,
                CalculationExpression = PersistedExpressionText.Normalize(target?.GetTextValue(Constants.ColumnPropertyNames.Expression, format)),
                CalculatedResultType = resultType,
                IsCompressedUnicode = false,
            };
        }

        return def with { SourceColumn = column };
    }
}
