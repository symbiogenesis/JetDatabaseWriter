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
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding;
using static JetDatabaseWriter.DatabaseFile;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Table DDL workflows behind <see cref="Interfaces.IAccessSchema"/>: create
/// and drop tables, and add, drop, or rename columns. Column changes rebuild
/// the table through a temporary copy that preserves rows, indexes, the
/// persisted column properties the writer models, client-side constraints,
/// complex-column artifacts, and foreign-key relationships (the table's FK
/// index entries, its partners' links to it, and renamed key columns in
/// <c>MSysRelationships</c>). A renamed column's new name is written into the
/// calculated expressions, validation rules and defaults that name it. A
/// column that a relationship uses as a key column, or that another column's
/// expression names, cannot be dropped, and a table that any relationship
/// names cannot be dropped either, as in Microsoft Access. Dropped tables
/// return their data, LVAL, index, usage-map, and TDEF pages to the global
/// free map, and no partner FK entry is left naming the freed TDEF page. The
/// public facade owns the auto-commit scope around each call.
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
/// <param name="relationships">Carries foreign-key index entries and relationship links across a rebuild, and refuses or unlinks a dropped table's relationships.</param>
/// <param name="snapshots">Reads rows, index metadata, and persisted column properties before a rebuild.</param>
/// <param name="autoNumbers">Carries the AutoNumber high-water value over to the rebuilt TDEF.</param>
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
    RelationshipManager relationships,
    TableSnapshotReader snapshots,
    AutoNumberMaintainer autoNumbers)
{
    /// <summary>
    /// Public CreateTable entry point: checks the arguments before any catalog
    /// I/O, in this order: the table name and the argument lists, then that
    /// the format can hold each declared column, then each calculated
    /// expression's syntax and each default. Then it creates the table.
    /// <see cref="RewriteTableAsync"/> calls <see cref="CreateTableAsync"/>
    /// directly, so a table holding an older expression can still be altered.
    /// </summary>
    /// <param name="tableName">The new table's name.</param>
    /// <param name="columns">The column definitions.</param>
    /// <param name="indexes">The index definitions.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes when the table is in the catalog.</returns>
    internal ValueTask CreateDeclaredTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        Guard.NotNull(indexes, nameof(indexes));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i] is { } column)
            {
                _ = TDefPageBuilder.ValidateColumnForFormat(column, db.Format);
            }
        }

        for (int i = 0; i < columns.Count; i++)
        {
            ValidateDeclaredCalculatedExpression(columns[i], nameof(columns));
            ValidateDeclaredDefault(columns[i], nameof(columns));
        }

        return this.CreateTableAsync(tableName, columns, indexes, cancellationToken);
    }

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

        this.ThrowIfTooManyColumns(tableName, columns.Count);

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

    /// <summary>
    /// Public DropTable entry point. Refuses, before anything is written, to
    /// drop a table that any <c>MSysRelationships</c> row names, as Microsoft
    /// Access does (error 3303); otherwise drops the table, its complex-column
    /// children and any partner FK entries that still name it.
    /// </summary>
    /// <param name="tableName">The table to drop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the table is gone.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the table does not exist or takes part in a relationship.</exception>
    internal async ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        // A missing table still reports "does not exist" from the drop below,
        // even when MSysRelationships has rows left that name it.
        IReadOnlyList<string> relationshipNames =
            await relationships.FindRelationshipNamesForTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (relationshipNames.Count > 0
            && await catalog.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) is not null)
        {
            RelationshipManager.EnsureTableHasNoRelationships(tableName, relationshipNames);
        }

        await this.DropTableCoreAsync(tableName, rewriting: false, cancellationToken).ConfigureAwait(false);
    }

    internal ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(column, nameof(column));
        db.ThrowIfDisposedOrCancelled(cancellationToken);

        // Argument checks before the table is read: the format first, so a
        // calculated column on an .mdb reports that, then the expression,
        // then the default.
        _ = TDefPageBuilder.ValidateColumnForFormat(column, db.Format);
        ValidateDeclaredCalculatedExpression(column, nameof(column));
        ValidateDeclaredDefault(column, nameof(column));

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
            name => name,
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
            name => string.Equals(name, columnName, StringComparison.OrdinalIgnoreCase) ? null : name,
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

                // Change only the name. The copy keeps the descriptor type
                // override (Currency), precision and scale, the calculated
                // expression and result type, Unicode compression, and the
                // complex-column ComplexId that RewriteTableAsync uses to
                // update MSysComplexColumns.ColumnName.
                var next = new List<ColumnDefinition>(existing);
                next[idx] = next[idx].WithName(newColumnName);
                return next;
            },
            (oldRow, _) => oldRow,
            name => string.Equals(name, oldColumnName, StringComparison.OrdinalIgnoreCase) ? newColumnName : name,
            cancellationToken);
    }

    /// <summary>
    /// Definition-time check for a calculated column the caller is declaring
    /// now (CreateTable / AddColumn): rejects operators Access does not have
    /// (such as Excel's postfix <c>%</c>) and syntax the expression parser
    /// cannot read, so the error surfaces when the column is defined rather
    /// than on the first insert. Functions and column names are still resolved
    /// at evaluation.
    /// </summary>
    /// <param name="column">The column being declared.</param>
    /// <param name="paramName">The public parameter that carries the column.</param>
    /// <exception cref="ArgumentException">The expression is not valid Access expression syntax.</exception>
    private static void ValidateDeclaredCalculatedExpression(ColumnDefinition? column, string paramName)
    {
        if (column is not { IsCalculated: true, CalculationExpression: { } expression } || string.IsNullOrWhiteSpace(expression))
        {
            return;
        }

        try
        {
            _ = CalculatedExpressionPlan.Parse(expression);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"Column '{column.Name}': {ex.Message}", paramName, ex);
        }
    }

    /// <summary>
    /// Definition-time check for a default the caller is declaring now
    /// (CreateTable / AddColumn). Access gives AutoNumber, calculated,
    /// Attachment and multi-value columns no default, because it generates
    /// their values, so a <see cref="ColumnDefinition.DefaultValue"/> or
    /// <see cref="ColumnDefinition.DefaultValueExpression"/> on one is rejected
    /// before anything is written. A property another tool stored on such a
    /// column reaches <see cref="RewriteTableAsync"/> without this check; the
    /// constraint registry ignores it. A floating-point CLR default must be
    /// finite, because Access has no literal to persist NaN or an infinity as.
    /// </summary>
    /// <param name="column">The column being declared.</param>
    /// <param name="paramName">The public parameter name, for <see cref="ArgumentException"/>.</param>
    /// <exception cref="ArgumentException">The column declares a default it cannot have.</exception>
    private static void ValidateDeclaredDefault(ColumnDefinition? column, string paramName)
    {
        if (column?.DeclaresDefault != true)
        {
            return;
        }

        if (column.CanHaveDefault)
        {
            if (column.DefaultValue is double d ? !double.IsFinite(d) : column.DefaultValue is float f && !float.IsFinite(f))
            {
                throw new ArgumentException($"Column '{column.Name}': a floating-point DefaultValue must be finite; Access has no literal for NaN or an infinity.", paramName);
            }

            return;
        }

        string reason;
        if (column.IsAutoIncrement)
        {
            reason = "an AutoNumber column cannot have a DefaultValue or DefaultValueExpression; its value is generated on insert";
        }
        else if (column.IsCalculated)
        {
            reason = "a calculated column cannot have a DefaultValue or DefaultValueExpression; its value comes from CalculationExpression";
        }
        else
        {
            reason = "an Attachment or multi-value column cannot have a DefaultValue or DefaultValueExpression; its items are stored in a hidden child table";
        }

        throw new ArgumentException($"Column '{column.Name}': {reason}.", paramName);
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
    /// renamed column, so they keep evaluating: <c>[Old]</c> and a bare
    /// <c>Old</c>, qualified by the table's own name or not, become
    /// <c>[New]</c>, and the rest of the text is kept as it was
    /// (<see cref="ExpressionFieldReferences"/>). Refuses to drop a column
    /// that one of them names, as for a relationship key column: the
    /// expression would name a column that no longer exists, and a rule or
    /// default would silently stop applying. <c>ValidationText</c> and
    /// <c>Description</c> are free text and are left alone.
    /// </summary>
    /// <param name="tableName">The table being rewritten, which may qualify a reference.</param>
    /// <param name="existingDefs">The columns before the rewrite.</param>
    /// <param name="newDefs">The projected columns, updated in place.</param>
    /// <param name="mapColumnName">Maps a current column name to its name after the rewrite, or to <see langword="null"/> for a dropped column.</param>
    /// <exception cref="ArgumentException">A renamed reference would need a name containing <c>]</c>, or would push an expression past the engine's limits.</exception>
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
    /// <exception cref="ArgumentException">The expression names the column and the new name contains <c>]</c>, or the rewritten text breaks a limit the original met.</exception>
    private static string? RenameReference(string tableName, ColumnDefinition column, string property, string? expression, string oldColumnName, string newColumnName)
    {
        if (expression is null || !ExpressionFieldReferences.References(expression, oldColumnName, tableName))
        {
            return expression;
        }

        string conflict = $"Cannot rename column '{oldColumnName}' of table '{tableName}' to '{newColumnName}': the {property} of column '{column.Name}' ('{expression}') names it";
        if (newColumnName.Contains(']', StringComparison.Ordinal))
        {
            throw new ArgumentException($"{conflict}, and a name containing ']' cannot be written as a [field] reference.", nameof(newColumnName));
        }

        // A bare reference gains brackets and the new name may be longer. An
        // expression past the limits stops evaluating (a rule or default
        // silently), so refuse a rename that would push one over them.
        string renamed = ExpressionFieldReferences.Rename(expression, oldColumnName, newColumnName, tableName)!;
        if (!FitsExpressionLimits(renamed) && FitsExpressionLimits(expression))
        {
            throw new ArgumentException(
                $"{conflict}, and with the new name it would exceed the expression limits ({CalculatedExpressionLimits.MaxExpressionLength} characters, {CalculatedExpressionLimits.MaxColumnReferences} [field] references).",
                nameof(newColumnName));
        }

        return renamed;
    }

    private static void ThrowIfNamesDroppedColumn(string tableName, string droppedColumn, ColumnDefinition survivor, string property, string? expression)
    {
        if (ExpressionFieldReferences.References(expression, droppedColumn, tableName))
        {
            throw new InvalidOperationException(
                $"Cannot drop column '{droppedColumn}' from table '{tableName}': the {property} of column '{survivor.Name}' ('{expression}') names it. Change or drop that column first.");
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
    /// Rejects a Jet3 table of more than 255 columns before anything is
    /// written: a Jet3 row stores <c>num_cols</c> in one byte, and Access
    /// allows 255 fields per table.
    /// </summary>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="columnCount">The number of columns the table would have.</param>
    /// <exception cref="JetLimitationException">The database is Jet3 and <paramref name="columnCount"/> is over 255.</exception>
    private void ThrowIfTooManyColumns(string tableName, int columnCount)
    {
        if (db.Format == DatabaseFormat.Jet3Mdb && columnCount > Constants.TableDefinition.MaxJet3Columns)
        {
            throw new JetLimitationException(
                $"Table '{tableName}' would have {columnCount} columns; a Jet3 (Access 97) table holds at most {Constants.TableDefinition.MaxJet3Columns}.");
        }
    }

    /// <summary>
    /// Rebuilds <paramref name="tableName"/> into a temporary copy with the
    /// projected schema, copies every row, and swaps the copy in. The table's
    /// foreign-key index entries are re-emitted on the copy, and its partner
    /// tables and <c>MSysRelationships</c> rows are pointed at the result.
    /// </summary>
    /// <param name="tableName">The table to rewrite.</param>
    /// <param name="projectColumns">Builds the new column list from the current one.</param>
    /// <param name="projectRow">Maps a current row to the new column list.</param>
    /// <param name="mapColumnName">
    /// Maps a current column name to its name after the rewrite, or to
    /// <see langword="null"/> for a dropped column. Drives the index
    /// projection, the relationship key columns, the FK index entries, and
    /// the column references in the table's expressions.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when the projection leaves no columns, or drops a relationship key column or a column another column's expression names.</exception>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when a renamed column's new name cannot be written into an expression that names it.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown, before anything is written, when a row holds a MEMO or OLE value in a kept column whose stored data cannot be read.</exception>
    private async ValueTask RewriteTableAsync(
        string tableName,
        Func<List<ColumnDefinition>, TableDef, List<ColumnDefinition>> projectColumns,
        Func<object[], TableDef, object[]> projectRow,
        Func<string, string?> mapColumnName,
        CancellationToken cancellationToken)
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

        this.ThrowIfTooManyColumns(tableName, newDefs.Count);

        // Carry renamed columns into the expressions that name them, and refuse
        // to drop a column one of them names, before anything is written; the
        // LvProp blob and the constraint registry are both built from newDefs.
        ProjectExpressionReferences(tableName, existingDefs, newDefs, mapColumnName);

        // Capture the table's relationship state, and refuse to drop a
        // relationship key column, before anything is written.
        RelationshipRewriteState relationshipState =
            await relationships.CaptureForRewriteAsync(tableName, entry.TDefPage, tableDef, cancellationToken).ConfigureAwait(false);
        RelationshipManager.EnsureKeyColumnsSurvive(relationshipState, mapColumnName);

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

        await this.AssignMissingComplexReferencesAsync(tableName, tableDef, newDefs, projectedRows, cancellationToken).ConfigureAwait(false);

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
            else if (!string.Equals(survivor.Name, c.Name, StringComparison.OrdinalIgnoreCase))
            {
                renamedComplex.Add((c.Name, survivor.Name, c.ComplexId));
            }
        }

        bool transplant = newComplexById.Count > 0 && droppedComplex.Count == 0 && renamedComplex.Count == 0;

        string tempName = $"~tmp_{Guid.NewGuid():N}"[..18];
        await this.CreateTableAsync(tempName, newDefs, projectedIndexes, cancellationToken).ConfigureAwait(false);

        ResolvedTable tempTable = await catalog.ResolveRequiredTableAsync(tempName, cancellationToken).ConfigureAwait(false);
        CatalogEntry tempEntry = tempTable.Entry;
        TableDef tempDef = tempTable.Definition;

        // Re-emit the foreign-key index entries on the copy before the row copy,
        // so the single index rebuild below also fills their leaves. A
        // self-referencing entry points at the page the table ends up on: the
        // original TDEF page when the copy is transplanted, else the copy's.
        long finalTdefPage = transplant ? entry.TDefPage : tempEntry.TDefPage;
        IReadOnlyDictionary<int, int> fkIndexNumbers = await relationships.EmitFkEntriesForRewriteAsync(
            relationshipState,
            tempEntry.TDefPage,
            tempDef,
            finalTdefPage,
            mapColumnName,
            cancellationToken).ConfigureAwait(false);
        if (fkIndexNumbers.Count > 0)
        {
            tempDef = await db.ReadRequiredTableDefAsync(tempEntry.TDefPage, tempName, cancellationToken).ConfigureAwait(false);
        }

        // The temp TDEF starts with its AutoNumber and complex AutoNumber
        // counters at 0. Carry the original's high-water values (and anything
        // larger the copied rows hold) over to it, or values and per-row
        // complex references freed by deleting the top rows would be handed
        // out again once the temp table takes the original's place.
        long autoNumberHighWater = await autoNumbers.ReadHighWaterAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        long complexHighWater = await autoNumbers.ReadComplexHighWaterAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var writtenRows = new List<LocatedRow>(projectedRows.Count);
        foreach (object[] projected in projectedRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RowLocation location = await tableRows.InsertRowDataLocAsync(tempEntry.TDefPage, tempDef, projected, cancellationToken: cancellationToken).ConfigureAwait(false);
            writtenRows.Add(new LocatedRow(location, projected));
            autoNumberHighWater = Math.Max(autoNumberHighWater, AutoNumberMaintainer.MaxAutoNumberValue(tempDef, projected));
            complexHighWater = Math.Max(complexHighWater, AutoNumberMaintainer.MaxComplexReference(tempDef, projected));
        }

        await autoNumbers.RaiseHighWaterAsync(tempEntry.TDefPage, autoNumberHighWater, cancellationToken).ConfigureAwait(false);
        await autoNumbers.RaiseComplexHighWaterAsync(tempEntry.TDefPage, complexHighWater, cancellationToken).ConfigureAwait(false);

        // Rebuild forwarded indexes once after the bulk row copy completes,
        // so we don't pay the rebuild cost per row. The rebuild keys the rows
        // just written at the locations the inserts returned, rather than
        // re-reading the copy. Re-emitted FK entries need the rebuild even on
        // an empty table, so it records their leaves in the index usage map.
        if (fkIndexNumbers.Count > 0 || (projectedIndexes.Count > 0 && writtenRows.Count > 0))
        {
            await indexMaintainer.RebuildIndexesAsync(tempEntry.TDefPage, tempDef, tempName, writtenRows, cancellationToken).ConfigureAwait(false);
        }

        // Drop the original table, then rename the temp catalog entry to take its place.
        // Pre-compute the LvProp blob from the projected columns so the catalog rename
        // re-emits the persisted properties under the user-facing table name.
        byte[]? renamedLvProp = JetExpressionConverter.BuildLvPropBlob(newDefs, db.Format);
        if (transplant)
        {
            await this.TransplantTempTableToOriginalAsync(
                tableName,
                entry.TDefPage,
                tableDef,
                tempName,
                tempEntry.TDefPage,
                renamedLvProp,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await this.DropTableCoreAsync(tableName, rewriting: true, cancellationToken).ConfigureAwait(false);
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

        // Point every partner table's FK entry at the table's final TDEF page
        // and renumbered entries, and rename key columns in MSysRelationships.
        await relationships.CompleteRewriteAsync(relationshipState, finalTdefPage, fkIndexNumbers, mapColumnName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gives every projected row a per-row complex reference in each complex
    /// column, as an insert does: a complex column the rewrite adds, or a slot
    /// an earlier build left null, gets a fresh reference shared by the row's
    /// null complex slots. The references come from the table's complex
    /// counter, and the TDEF complex AutoNumber the rewrite carries to the
    /// rebuilt table covers them.
    /// </summary>
    /// <param name="tableName">The table being rewritten.</param>
    /// <param name="tableDef">Its current definition.</param>
    /// <param name="newDefs">The projected column list.</param>
    /// <param name="rows">The projected rows, aligned with <paramref name="newDefs"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask AssignMissingComplexReferencesAsync(
        string tableName,
        TableDef tableDef,
        List<ColumnDefinition> newDefs,
        List<object[]> rows,
        CancellationToken cancellationToken)
    {
        List<int> complexColumns = [];
        for (int i = 0; i < newDefs.Count; i++)
        {
            if (newDefs[i].IsAttachment || newDefs[i].IsMultiValue)
            {
                complexColumns.Add(i);
            }
        }

        List<object[]> needing = complexColumns.Count == 0
            ? []
            : rows.FindAll(row => complexColumns.Exists(i => row[i] is null or DBNull));
        if (needing.Count == 0)
        {
            return;
        }

        int first = await constraints.AllocateComplexReferencesAsync(tableName, tableDef, needing.Count, cancellationToken).ConfigureAwait(false);
        for (int k = 0; k < needing.Count; k++)
        {
            var reference = new ComplexIdRef(first + k);
            foreach (int i in complexColumns)
            {
                if (needing[k][i] is null or DBNull)
                {
                    needing[k][i] = reference;
                }
            }
        }
    }

    /// <summary>
    /// Moves the rebuilt copy onto the original TDEF page, so the complex columns'
    /// <c>MSysComplexColumns</c> rows keep naming the table, and frees the original's
    /// storage.
    /// </summary>
    /// <param name="tableName">The table being rewritten.</param>
    /// <param name="originalTdefPage">The original's TDEF page, which the copy takes over.</param>
    /// <param name="originalDef">The original's definition, with its calculated result types.</param>
    /// <param name="tempName">The rebuilt copy's name.</param>
    /// <param name="tempTdefPage">The rebuilt copy's TDEF page.</param>
    /// <param name="lvProp">The persisted properties for the table's catalog row.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the copy has replaced the original.</returns>
    /// <exception cref="NotSupportedException">The rebuilt copy's TDEF spans more than one page.</exception>
    private async ValueTask TransplantTempTableToOriginalAsync(
        string tableName,
        long originalTdefPage,
        TableDef originalDef,
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

            await this.ReclaimTableStoragePagesAsync(originalTdefPage, originalDef, includeTDefRoot: false, cancellationToken).ConfigureAwait(false);
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

        // The rebuilt schema's constraints were registered under the temp name.
        // Move them onto the table, as the drop-and-rename path does, so the
        // stale pre-rewrite list (with its old column count and in-session
        // AutoNumber seed) is not applied to the next insert.
        constraints.Unregister(tableName);
        constraints.Rename(tempName, tableName);
    }

    private async ValueTask PatchTablePageOwnersAsync(long fromTdefPage, long toTdefPage, CancellationToken cancellationToken)
    {
        long totalPages = db.PageCount;
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
        // A calculated column is projected from its result type, which can differ
        // from its descriptor type in Access-authored tables (a Memo result in a
        // Text descriptor, a Currency result in a Double one); the rebuilt
        // descriptor then carries the result type, as the writer's own do.
        ColumnType valueType = ResolveValueType(column);
        bool sizedByDescriptor = !column.IsCalculated || valueType == column.Type;
        ColumnDefinition baseDef;
        switch (valueType)
        {
            case TextType:
                int textSize = column.IsCalculated ? CalculatedPayloadSize(column, sizedByDescriptor, Constants.CalculatedColumn.MaxTextResultBytes) : column.Size;
                int charLen = db.Format != DatabaseFormat.Jet3Mdb ? Math.Max(1, textSize / 2) : Math.Max(1, textSize);
                baseDef = new ColumnDefinition(column.Name, typeof(string), charLen);
                break;
            case BinaryType:
                int binarySize = column.IsCalculated ? CalculatedPayloadSize(column, sizedByDescriptor, 0) : column.Size;
                baseDef = new ColumnDefinition(column.Name, typeof(byte[]), binarySize > 0 ? binarySize : 255);
                break;
            case AttachmentType:
                // preserve attachment columns across
                // AddColumnAsync / DropColumnAsync / RenameColumnAsync. The parent
                // TDEF descriptor round-trips with the ComplexID intact (ColumnInfo.Misc
                // → ColumnDefinition.ComplexId → re-emitted into the rebuilt TDEF's
                // misc slot), and the existing hidden flat child table + MSysComplexColumns
                // row are kept attached because the rewrite path skips the cascade-on-drop
                // step. Each row's per-row complex reference is copied to the rebuilt
                // parent row, so the flat table's `_<columnName>` FK back-reference
                // still joins to it.
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
            IsDateTimeExtended = valueType == DateTimeExtendedType,
            IsCompressedUnicode = column.IsCompressedUnicode,
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
                CalculationExpression = target?.GetTextValue(Constants.ColumnPropertyNames.Expression, db.Format),
                CalculatedResultType = resultType,
                IsCompressedUnicode = false,
            };
        }

        return def;
    }

    /// <summary>
    /// Shared implementation backing <see cref="DropTableAsync"/> and the
    /// <c>RewriteTableAsync</c> path. The rewrite path sets
    /// <paramref name="rewriting"/>, so the hidden flat child tables and
    /// matching <c>MSysComplexColumns</c> rows for surviving complex columns
    /// stay attached to the rebuilt parent, and the partner tables' FK entries
    /// are left for <see cref="RelationshipManager.CompleteRewriteAsync"/> to
    /// re-link. A real drop removes both.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rewriting">Whether a schema rewrite is replacing the table with its rebuilt copy.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when <c>MSysObjects</c> is missing or no matching user table exists.</exception>
    private async ValueTask DropTableCoreAsync(string tableName, bool rewriting, CancellationToken cancellationToken)
    {
        Dictionary<long, TableDef> definitions = await this.ReadDefinitionsBeforeDropAsync(tableName, cancellationToken).ConfigureAwait(false);
        UserTableCatalogDeletionResult deleted = await catalogWriter.DeleteUserTableCatalogRowsAsync(
            tableName,
            tdefPage: null,
            includeSystemTables: false,
            throwIfNotFound: true,
            operation: $"dropping table '{tableName}'",
            missingMessage: $"Table '{tableName}' does not exist.",
            cancellationToken).ConfigureAwait(false);

        if (!rewriting)
        {
            // No MSysRelationships row names the table (DropTableAsync refused
            // otherwise), but its TDEF can still hold FK entries that earlier
            // builds left behind. Remove the partner entries that name it
            // while its TDEF is intact, so none is left naming a freed page
            // or, later, the unrelated table that reuses it.
            foreach (long tdefPage in deleted.TDefPages)
            {
                await relationships.RemovePartnerLinksAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            }

            foreach (long parentTdefPage in deleted.TDefPages)
            {
                await complexColumns.DropComplexChildrenForTableAsync(parentTdefPage, cancellationToken).ConfigureAwait(false);
            }
        }

        await catalogWriter.DeleteAceRowsForObjectIdsAsync(deleted.TDefPages, cancellationToken).ConfigureAwait(false);

        foreach (long tdefPage in deleted.TDefPages)
        {
            await this.ReclaimTableStoragePagesAsync(tdefPage, definitions.GetValueOrDefault(tdefPage), includeTDefRoot: true, cancellationToken).ConfigureAwait(false);
        }

        constraints.Unregister(tableName);
        catalog.Invalidate();
    }

    /// <summary>
    /// Reads the definition of each user table named <paramref name="tableName"/>,
    /// by TDEF page, while its <c>MSysObjects</c> row still holds the calculated
    /// columns' <c>ResultType</c>. A calculated column whose result is Memo or OLE
    /// can have another descriptor type in an Access-authored table, and the
    /// reclaim needs the result type to find the LVAL rows its values point to.
    /// </summary>
    /// <param name="tableName">The table about to be dropped.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The definitions, by TDEF page.</returns>
    private async ValueTask<Dictionary<long, TableDef>> ReadDefinitionsBeforeDropAsync(string tableName, CancellationToken cancellationToken)
    {
        var definitions = new Dictionary<long, TableDef>();
        foreach (CatalogEntry entry in await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(entry.Name, tableName, StringComparison.OrdinalIgnoreCase)
                && await catalog.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false) is TableDef tableDef)
            {
                definitions[entry.TDefPage] = tableDef;
            }
        }

        return definitions;
    }

    /// <summary>
    /// Frees a table's storage: its data pages, the LVAL rows its rows' long values
    /// point to, the pages its usage maps list after the first two (index pages, and
    /// in an Access-authored table each long-value column's LVAL pages), its usage-map
    /// page, and its TDEF pages.
    /// </summary>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="tableDef">
    /// The table's definition, read with its calculated result types before its
    /// catalog row was deleted; <see langword="null"/> reads it from the TDEF, by
    /// descriptor type alone.
    /// </param>
    /// <param name="includeTDefRoot">Whether to free the first TDEF page too; a transplant keeps it.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the pages are freed.</returns>
    private async ValueTask ReclaimTableStoragePagesAsync(long tdefPage, TableDef? tableDef, bool includeTDefRoot, CancellationToken cancellationToken)
    {
        var pagesToFree = new SortedSet<long>();
        var longValueRoots = new List<LongValueDescriptor>();

        tableDef ??= await db.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long totalPages = db.PageCount;
        if (tableDef is not null)
        {
            // Overflow rows are followed to their moved bytes, so their long
            // values are freed too.
            await db.ForEachOwnedDataPageAsync(
                tdefPage,
                (pageNumber, page, token) =>
                {
                    pagesToFree.Add(pageNumber);
                    return db.ForEachRowOnPageAsync(
                        pageNumber,
                        page,
                        (row, _) =>
                        {
                            var rowBound = new RowBound(row.Location.DataRowIndex, row.Location.RowStart, row.Location.RowSize);
                            longValueRoots.AddRange(longValueEncoder.CollectLongValueRoots(row.Page, rowBound, tableDef));
                            return new ValueTask<bool>(true);
                        },
                        token);
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
            int usageMapPage = UsageMap.ReadUInt24(firstTdefPage, db.TDef.UsedPagesPage);
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
        long totalPages = db.PageCount;
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
