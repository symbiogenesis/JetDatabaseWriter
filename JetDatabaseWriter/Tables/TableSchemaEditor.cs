namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Table DDL workflows behind <see cref="Interfaces.IAccessSchema"/>: create
/// and drop tables, and add, drop, or rename columns. Column changes rebuild
/// the table through a temporary copy that preserves rows, indexes, every
/// persisted column and table property except the table's <c>NameMap</c>
/// (<see cref="PersistedPropertyProjector"/>), client-side constraints,
/// complex-column artifacts, and foreign-key relationships (the table's FK
/// index entries, its partners' links to it, and renamed key columns in
/// <c>MSysRelationships</c>). A renamed column's new name is written into the
/// calculated expressions, validation rules and defaults that name it. A
/// column that a relationship uses as a key column, or that another column's
/// expression names, cannot be dropped, and enforced relationships to another table
/// block a table drop. New table, column
/// and index names must follow the Access naming rules
/// (<see cref="AccessObjectName"/>); names the table already carries are
/// kept as they are. Dropped tables return their data, LVAL, index,
/// usage-map, and TDEF pages to the global free map, and no partner FK entry
/// is left naming the freed TDEF page. The public facade owns the
/// auto-commit scope around each call.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="pager">The writer's page file, through which a transplanted TDEF and re-owned data pages are written.</param>
/// <param name="catalog">Resolves table names and is invalidated after a rename.</param>
/// <param name="tableRows">Copies rows into the rebuilt table.</param>
/// <param name="indexMaintainer">Rebuilds forwarded indexes after a row copy.</param>
/// <param name="catalogWriter">Renames and deletes <c>MSysObjects</c> / <c>MSysACEs</c> rows.</param>
/// <param name="catalogArtifacts">Creates tables and applies catalog replacement plans.</param>
/// <param name="complexColumns">Allocates, emits, re-parents, drops, and renames complex-column artifacts.</param>
/// <param name="constraints">Carries client-side column constraints across schema changes.</param>
/// <param name="relationships">Carries foreign-key index entries and relationship links across a rebuild, and refuses or unlinks a dropped table's relationships.</param>
/// <param name="snapshots">Reads rows, index metadata, and persisted column properties before a rebuild.</param>
/// <param name="rewritePlanner">Prepares validated rewrite projections.</param>
/// <param name="tableStorage">Copies, transfers and reclaims table storage.</param>
internal sealed class TableSchemaEditor(
    JetFormat format,
    TableDefReader tableDefs,
    Pager pager,
    TableCatalog catalog,
    TableRowStore tableRows,
    IndexMaintainer indexMaintainer,
    CatalogWriter catalogWriter,
    CatalogArtifactWriter catalogArtifacts,
    ComplexColumnManager complexColumns,
    ConstraintRegistry constraints,
    RelationshipManager relationships,
    TableSnapshotReader snapshots,
    TableRewritePlanner rewritePlanner,
    TableStorageEditor tableStorage)
{
    /// <summary>Changes only the table rule properties after validating existing rows.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rule">The proposed rule or removal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetOperationException">The catalog lacks one matching table row or required fields.</exception>
    internal async ValueTask SetTableValidationRuleAsync(string tableName, TableValidationRule? rule, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        await catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);
        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ColumnPropertyBlock? properties = await snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        ConstraintRegistry.ValidatePersistedConstraintProperties(tableName, properties);
        ColumnPropertyBlockBuilder builder = properties is null ? new ColumnPropertyBlockBuilder() : ColumnPropertyBlockBuilder.FromBlock(properties);
        ColumnPropertyTargetBuilder target = builder.GetOrAddTableTarget();
        ColumnPropertyEntryBuilder? originalRule = target.Entries.Find(static entry => string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationRule, StringComparison.OrdinalIgnoreCase));
        ColumnPropertyEntryBuilder? originalText = target.Entries.Find(static entry => string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationText, StringComparison.OrdinalIgnoreCase));
        target.Entries.RemoveAll(static entry => string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationRule, StringComparison.OrdinalIgnoreCase)
            || string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationText, StringComparison.OrdinalIgnoreCase));
        if (rule != null)
        {
            Guard.NotNullOrEmpty(rule.Expression, nameof(rule));
            var candidate = new TableValidationConstraint(rule.Expression, rule.ValidationText) { Plan = CalculatedExpressionPlan.Parse(rule.Expression, allowQualifiedReferences: false) };
            foreach (LocatedRow row in await snapshots.ReadRowsAsync(table.Entry.TDefPage, cancellationToken).ConfigureAwait(false))
            {
                await constraints.ValidateTableRuleAsync(tableName, table.Definition, row.Values, candidate, cancellationToken).ConfigureAwait(false);
            }

            target.AddMemoText(Constants.ColumnPropertyNames.ValidationRule, rule.Expression, format);
            target.Entries[^1].DataType = originalRule?.DataType ?? MemoType;
            target.Entries[^1].DdlFlag = originalRule?.DdlFlag ?? 0x01;
            if (rule.ValidationText != null)
            {
                target.AddText(Constants.ColumnPropertyNames.ValidationText, rule.ValidationText, format);
                target.Entries[^1].DataType = originalText?.DataType ?? TextType;
                target.Entries[^1].DdlFlag = originalText?.DdlFlag ?? 0x01;
            }
        }

        byte[]? blob = builder.ToBytes(format);
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        int nameIndex = objects.FindColumnIndex("Name");
        int idIndex = objects.FindColumnIndex("Id");
        int propertyIndex = objects.FindColumnIndex("LvProp");
        if (nameIndex < 0 || idIndex < 0 || propertyIndex < 0)
        {
            throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, "MSysObjects lacks table property fields.");
        }

        List<LocatedRow> matches = (await snapshots.ReadRowsAsync(2, cancellationToken).ConfigureAwait(false)).FindAll(row =>
            string.Equals(row.Values[nameIndex] as string, tableName, StringComparison.OrdinalIgnoreCase)
            && Convert.ToInt64(row.Values[idIndex], System.Globalization.CultureInfo.InvariantCulture) == table.Entry.TDefPage);
        if (matches.Count != 1)
        {
            throw new JetOperationException(JetErrorCode.CatalogObjectNotFound, $"Table '{tableName}' has {matches.Count} matching catalog rows.");
        }

        LocatedRow original = matches[0];
        object[] replacement = (object[])original.Values.Clone();
        replacement[propertyIndex] = blob ?? (object)DBNull.Value;
        _ = tableRows.EncodeRow(objects, replacement);
        await catalogWriter.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        await tableRows.MarkRowDeletedAsync(original.Location.PageNumber, original.Location.RowIndex, objects, CancellationToken.None).ConfigureAwait(false);
        await tableRows.InsertRowDataAsync(2, objects, replacement, updateTDefRowCount: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
        await indexMaintainer.MaintainIndexesAsync(2, objects, Constants.SystemTableNames.Objects, CancellationToken.None).ConfigureAwait(false);
        catalog.Invalidate();
        constraints.InvalidateTableRule(tableName);
    }

    /// <summary>
    /// Public CreateTable entry point: checks the arguments before any catalog
    /// I/O, in this order: the table name and the argument lists, including
    /// the Access naming rules for the table, column and index names
    /// (<see cref="AccessObjectName"/>) and, on Jet3, that each name is in the
    /// database's code page, then that the format can hold each
    /// declared column, then each calculated expression's syntax and each
    /// default. Then it creates the table. <see cref="RewriteTableAsync"/>
    /// calls <see cref="CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, IReadOnlyList{IndexDefinition}, ColumnPropertyBlock?, CancellationToken)"/> directly, so a table holding an
    /// stored expression, or a name an existing file already carries, can
    /// still be altered.
    /// </summary>
    /// <param name="tableName">The new table's name.</param>
    /// <param name="columns">The column definitions.</param>
    /// <param name="indexes">The index definitions.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes when the table is in the catalog.</returns>
    internal ValueTask CreateDeclaredTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken)
    {
        AccessObjectName.ThrowIfInvalid(tableName, nameof(tableName), "table");
        AccessObjectName.ThrowIfNotStorable(format, tableName, nameof(tableName), "table");
        Guard.NotNull(columns, nameof(columns));
        Guard.NotNull(indexes, nameof(indexes));
        pager.ThrowIfDisposedOrCancelled(cancellationToken);

        ValidateDeclaredNames(format, columns, indexes);

        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i] is { } column)
            {
                _ = TDefPageBuilder.ValidateColumnForFormat(column, format, nameof(columns));
            }
        }

        for (int i = 0; i < columns.Count; i++)
        {
            ValidateDeclaredAutoIncrement(columns[i], nameof(columns));
            ValidateDeclaredCalculatedExpression(columns[i], nameof(columns));
            ValidateDeclaredDefault(columns[i], nameof(columns));
        }

        _ = JetExpressionConverter.BuildLvPropBlob(columns, format);

        return this.CreateTableAsync(tableName, columns, indexes, cancellationToken);
    }

    internal ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken)
        => this.CreateTableAsync(tableName, columns, indexes, persistedProperties: null, cancellationToken);

    /// <summary>
    /// Creates a table without the declaration checks of
    /// <see cref="CreateDeclaredTableAsync"/>. A schema rewrite passes
    /// <paramref name="persistedProperties"/>, the original table's properties
    /// projected onto the rebuilt columns, which the catalog row stores instead of
    /// the properties built from <paramref name="columns"/>.
    /// </summary>
    /// <param name="tableName">The new table's name.</param>
    /// <param name="columns">The column definitions.</param>
    /// <param name="indexes">The index definitions.</param>
    /// <param name="persistedProperties">The properties to store, or <see langword="null"/> to build them from <paramref name="columns"/>.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes when the table is in the catalog.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="columns"/> is empty.</exception>
    /// <exception cref="InvalidOperationException">Thrown when a table named <paramref name="tableName"/> already exists.</exception>
    /// <exception cref="JetObjectExistsException">The operation is refused with a structured <see cref="JetObjectExistsException"/>.</exception>
    internal async ValueTask CreateTableAsync(
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<IndexDefinition> indexes,
        ColumnPropertyBlock? persistedProperties,
        CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        Guard.NotNull(indexes, nameof(indexes));
        pager.ThrowIfDisposedOrCancelled(cancellationToken);
        await catalogArtifacts.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        await catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required", nameof(columns));
        }

        ThrowIfTooManyColumns(tableName, columns.Count);

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
            throw new JetObjectExistsException(JetErrorCode.TableExists, $"Table '{tableName}' already exists.", errorInfo: new JetErrorInfo { TableName = tableName });
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
            var withComplexIndexes = new List<IndexDefinition>(indexes);
            foreach (ComplexColumnAllocation allocation in complexAllocs)
            {
                string columnName = columns[allocation.ColumnIndex].Name;
                string prefix = columnName.Length > 31 ? columnName[..31] : columnName;
                withComplexIndexes.Add(new IndexDefinition($"{prefix}_{Guid.NewGuid():N}", columnName)
                {
                    IsUnique = true,
                    IsRequired = true,
                    IsComplexReferenceIndex = true,
                });
            }

            indexes = withComplexIndexes;
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

        var tableArtifact = new CatalogTableArtifact(tableName, columns, indexes, catalogFlags) { PersistedProperties = persistedProperties };
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
        pager.ThrowIfDisposedOrCancelled(cancellationToken);

        // A missing table still reports "does not exist" from the drop below,
        // even when MSysRelationships has rows left that name it.
        IReadOnlyList<string> relationshipNames = [];
        if (await catalog.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false) is not null)
        {
            relationshipNames = await relationships.PlanTableDropRelationshipsAsync(tableName, cancellationToken).ConfigureAwait(false);
        }

        await catalogArtifacts.ThrowIfCatalogIndexesUnmaintainableAsync(cancellationToken).ConfigureAwait(false);
        await catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);
        foreach (string relationshipName in relationshipNames)
        {
            await relationships.DropRelationshipAsync(relationshipName, cancellationToken).ConfigureAwait(false);
        }

        await tableStorage.DropTableCoreAsync(tableName, rewriting: false, cancellationToken).ConfigureAwait(false);
    }

    internal ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(column, nameof(column));
        AccessObjectName.ThrowIfInvalidMember(column.Name, nameof(column), "column");
        AccessObjectName.ThrowIfNotStorable(format, column.Name, nameof(column), "column");
        pager.ThrowIfDisposedOrCancelled(cancellationToken);

        // Argument checks before the table is read: the name, then the format,
        // so a calculated column on an .mdb reports that, then the expression,
        // then the default.
        _ = TDefPageBuilder.ValidateColumnForFormat(column, format, nameof(column));
        ValidateDeclaredAutoIncrement(column, nameof(column));
        ValidateDeclaredCalculatedExpression(column, nameof(column));
        ValidateDeclaredDefault(column, nameof(column));
        _ = JetExpressionConverter.BuildLvPropBlob([column], format);

        return this.RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                if (existing.Exists(c => string.Equals(c.Name, column.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new JetObjectExistsException(JetErrorCode.ColumnExists, $"Column '{column.Name}' already exists in table '{tableName}'.", errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = column.Name });
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
        pager.ThrowIfDisposedOrCancelled(cancellationToken);

        int dropIndex = -1;
        return this.RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                dropIndex = existing.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
                if (dropIndex < 0)
                {
                    throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{columnName}' was not found in table '{tableName}'.", nameof(columnName), errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = columnName });
                }

                if (existing.Count == 1)
                {
                    throw new JetOperationException(JetErrorCode.LastColumn, $"Cannot drop the last remaining column from table '{tableName}'.", errorInfo: new JetErrorInfo { TableName = tableName });
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

    internal async ValueTask RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(oldColumnName, nameof(oldColumnName));
        AccessObjectName.ThrowIfInvalid(newColumnName, nameof(newColumnName), "column");
        AccessObjectName.ThrowIfNotStorable(format, newColumnName, nameof(newColumnName), "column");
        pager.ThrowIfDisposedOrCancelled(cancellationToken);

        // A rename to the name the column already has, spelled as stored,
        // changes nothing, so the table is not rewritten once the table and
        // the column are known to exist. Any other spelling of the same name
        // is a case-only rename, which Access is believed to allow (not
        // checked), and goes through the rewrite like any rename.
        if (string.Equals(oldColumnName, newColumnName, StringComparison.OrdinalIgnoreCase))
        {
            await catalogArtifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);
            ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            int current = table.Definition.FindColumnIndex(oldColumnName);
            if (current < 0)
            {
                throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{oldColumnName}' was not found in table '{tableName}'.", nameof(oldColumnName), errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = oldColumnName });
            }

            if (string.Equals(table.Definition.Columns[current].Name, newColumnName, StringComparison.Ordinal))
            {
                return;
            }
        }

        await this.RewriteTableAsync(
            tableName,
            (existing, _) =>
            {
                int idx = existing.FindIndex(c => string.Equals(c.Name, oldColumnName, StringComparison.OrdinalIgnoreCase));
                if (idx < 0)
                {
                    throw new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, $"Column '{oldColumnName}' was not found in table '{tableName}'.", nameof(oldColumnName), errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = oldColumnName });
                }

                // Access compares column names ignoring case. The renamed
                // column is left out, so a rename may change only the case.
                for (int i = 0; i < existing.Count; i++)
                {
                    if (i != idx && string.Equals(existing[i].Name, newColumnName, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new JetObjectExistsException(JetErrorCode.ColumnExists, $"Column '{newColumnName}' already exists in table '{tableName}'.", errorInfo: new JetErrorInfo { TableName = tableName, ColumnName = newColumnName });
                    }
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
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Checks the column and index names a CreateTable caller declares: each
    /// definition is present, its name follows the Access naming rules
    /// (<see cref="AccessObjectName"/>) and the database can store it, and no
    /// two columns share a name, which Access compares ignoring case.
    /// <see cref="IndexHelpers.ResolveIndexes"/> rejects duplicate index names.
    /// </summary>
    /// <param name="format">The database's immutable format profile.</param>
    /// <param name="columns">The declared columns.</param>
    /// <param name="indexes">The declared indexes.</param>
    /// <exception cref="ArgumentException">A definition is <see langword="null"/>, a name is missing, breaks a rule or is not in a Jet3 database's code page, or two columns share a name.</exception>
    private static void ValidateDeclaredNames(JetFormat format, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes)
    {
        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i] is not { } column)
            {
                throw new ArgumentException($"The column at position {i} is null.", nameof(columns));
            }

            AccessObjectName.ThrowIfInvalidMember(column.Name, nameof(columns), "column", i);
            AccessObjectName.ThrowIfNotStorable(format, column.Name, nameof(columns), "column", i);
            if (!columnNames.Add(column.Name))
            {
                throw new ArgumentException(
                    $"Column name '{column.Name}' is used more than once; Access compares column names ignoring case.",
                    nameof(columns));
            }
        }

        for (int i = 0; i < indexes.Count; i++)
        {
            if (indexes[i] is not { } index)
            {
                throw new ArgumentException($"The index at position {i} is null.", nameof(indexes));
            }

            AccessObjectName.ThrowIfInvalidMember(index.Name, nameof(indexes), "index", i);
            AccessObjectName.ThrowIfNotStorable(format, index.Name, nameof(indexes), "index", i);
        }
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

    /// <summary>Checks declared AutoNumber types before catalog I/O.</summary>
    /// <param name="column">The column being declared.</param>
    /// <param name="paramName">The public parameter name.</param>
    /// <exception cref="NotSupportedException">The column requests AutoNumber on a byte or long type.</exception>
    /// <exception cref="ArgumentException">The column requests AutoNumber on a non-integer type.</exception>
    private static void ValidateDeclaredAutoIncrement(ColumnDefinition? column, string paramName)
    {
        if (column?.IsAutoIncrement != true)
        {
            return;
        }

        if (column.ClrType == typeof(byte) || column.ClrType == typeof(long))
        {
            throw new NotSupportedException(
                $"Column '{column.Name}': IsAutoIncrement is only supported for Int16, Int32 and Guid; '{column.ClrType}' is not supported.");
        }

        if (column.ClrType != typeof(short) && column.ClrType != typeof(int) && column.ClrType != typeof(Guid))
        {
            throw new ArgumentException(
                $"Column '{column.Name}' is marked IsAutoIncrement=true but its CLR type '{column.ClrType}' is not an integer or GUID type.",
                paramName);
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
    /// A numeric CLR default persisted as a literal (no
    /// <see cref="ColumnDefinition.DefaultValueExpression"/>) must give the
    /// column a value when that literal is read back as the column's type, as
    /// every writer applies it (<see cref="ColumnDefaultValue.TryReadBackNumber"/>);
    /// 1e39 on a Single column or 300 on a Byte column would give none.
    /// </summary>
    /// <param name="column">The column being declared.</param>
    /// <param name="paramName">The public parameter name, for <see cref="ArgumentException"/>.</param>
    /// <exception cref="ArgumentException">The column declares a default it cannot have.</exception>
    /// <exception cref="JetArgumentException">The operation is refused with a structured <see cref="JetArgumentException"/>.</exception>
    private static void ValidateDeclaredDefault(ColumnDefinition? column, string paramName)
    {
        if (column?.DeclaresDefault != true)
        {
            return;
        }

        if (column.CanHaveDefault)
        {
            if (string.IsNullOrWhiteSpace(column.DefaultValueExpression))
            {
                _ = JetExpressionConverter.ToJetExpression(column.DefaultValue);
            }

            if (column.DefaultValue is double d ? !double.IsFinite(d) : column.DefaultValue is float f && !float.IsFinite(f))
            {
                throw new JetArgumentException(JetErrorCode.DefaultNotAllowed, $"Column '{column.Name}': a floating-point DefaultValue must be finite; Access has no literal for NaN or an infinity.", paramName, new JetErrorInfo { ColumnName = column.Name });
            }

            if (string.IsNullOrWhiteSpace(column.DefaultValueExpression)
                && ColumnDefaultValue.TryReadBackNumber(column.DefaultValue, column.ClrType, out object? stored)
                && stored is null)
            {
                throw new JetArgumentException(JetErrorCode.DefaultNotAllowed, $"Column '{column.Name}': the {column.DefaultValue!.GetType().Name} DefaultValue {JetExpressionConverter.ToJetExpression(column.DefaultValue)} cannot be stored in a {column.ClrType.Name} column.", paramName, new JetErrorInfo { ColumnName = column.Name });
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

        throw new JetArgumentException(JetErrorCode.DefaultNotAllowed, $"Column '{column.Name}': {reason}.", paramName, errorInfo: new JetErrorInfo { ColumnName = column.Name });
    }

    /// <summary>
    /// Rejects a table of more than 255 columns before anything is written:
    /// Microsoft Access allows 255 fields per table in every database format.
    /// </summary>
    /// <param name="tableName">The table name, for the message.</param>
    /// <param name="columnCount">The number of columns the table would have.</param>
    /// <exception cref="JetLimitationException"><paramref name="columnCount"/> is over 255.</exception>
    private static void ThrowIfTooManyColumns(string tableName, int columnCount)
    {
        if (columnCount > Constants.TableDefinition.MaxTableColumns)
        {
            throw new JetLimitationException(
                $"Table '{tableName}' would have {columnCount} columns; a Microsoft Access table holds at most {Constants.TableDefinition.MaxTableColumns}.");
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
    /// <exception cref="ArgumentException">Thrown, before anything is written, when a renamed column's new name would push an expression that names it past the expression limits.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown, before anything is written, when a row holds a MEMO or OLE value in a kept column whose stored data cannot be read.</exception>
    /// <exception cref="JetLimitationException">Thrown, before anything is written, when an index of the table names a column the table does not have, or the index section of its table definition cannot be parsed or runs past the end of it (<see cref="IndexMaintainer.ThrowIfIndexesUnmaintainableAsync"/>).</exception>
    /// <exception cref="JetOperationException">The operation is refused with a structured <see cref="JetOperationException"/>.</exception>
    private async ValueTask RewriteTableAsync(
        string tableName,
        Func<List<ColumnDefinition>, TableDef, List<ColumnDefinition>> projectColumns,
        Func<object[], TableDef, object[]> projectRow,
        Func<string, string?> mapColumnName,
        CancellationToken cancellationToken)
    {
        TableRewritePlan plan = await rewritePlanner.PrepareAsync(tableName, projectColumns, projectRow, mapColumnName, cancellationToken).ConfigureAwait(false);
        string tempName = $"~tmp_{Guid.NewGuid():N}"[..18];
        await this.CreateTableAsync(tempName, plan.Columns, plan.Indexes, plan.Properties, cancellationToken).ConfigureAwait(false);
        await tableStorage.ApplyAsync(plan, tempName, cancellationToken).ConfigureAwait(false);
    }
}
