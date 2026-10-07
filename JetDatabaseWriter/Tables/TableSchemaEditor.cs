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
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

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
/// <param name="ownedPages">The database's owned-page discovery and row walks.</param>
/// <param name="pager">The writer's page file, through which a transplanted TDEF and re-owned data pages are written.</param>
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
    JetFormat format,
    TableDefReader tableDefs,
    OwnedDataPages ownedPages,
    Pager pager,
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
    /// <summary>Changes only the table rule properties after validating existing rows.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rule">The proposed rule or removal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetOperationException">The catalog lacks one matching table row or required fields.</exception>
    internal async ValueTask SetTableValidationRuleAsync(string tableName, TableValidationRule? rule, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
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
            var candidate = new TableValidationConstraint(rule.Expression, rule.ValidationText) { Plan = CalculatedExpressionPlan.Parse(rule.Expression) };
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
        foreach (string relationshipName in relationshipNames)
        {
            await relationships.DropRelationshipAsync(relationshipName, cancellationToken).ConfigureAwait(false);
        }

        await this.DropTableCoreAsync(tableName, rewriting: false, cancellationToken).ConfigureAwait(false);
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
    /// Returns the per-row complex reference every surviving complex column of
    /// <paramref name="row"/> holds (one with a <see cref="ColumnDefinition.ComplexId"/>,
    /// which keeps its flat table), or <see langword="null"/> when the row has
    /// no such column, or one of them is null or holds another reference.
    /// </summary>
    /// <param name="row">The projected row.</param>
    /// <param name="complexColumns">The indexes of the projected complex columns.</param>
    /// <param name="newDefs">The projected column list.</param>
    private static int? SharedComplexReference(object[] row, List<int> complexColumns, List<ColumnDefinition> newDefs)
    {
        int? shared = null;
        foreach (int i in complexColumns)
        {
            if (newDefs[i].ComplexId == 0)
            {
                continue;
            }

            if (row[i] is not ComplexIdRef { Id: > 0 } reference || (shared is int previous && previous != reference.Id))
            {
                return null;
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

        ThrowIfTooManyColumns(tableName, newDefs.Count);

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

        string tempName = $"~tmp_{Guid.NewGuid():N}"[..18];
        await this.CreateTableAsync(tempName, newDefs, projectedIndexes, persistedProperties, cancellationToken).ConfigureAwait(false);

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
            tempDef = await tableDefs.ReadRequiredTableDefAsync(tempEntry.TDefPage, tempName, cancellationToken).ConfigureAwait(false);
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
        // Either way the table's catalog row carries the projected persisted properties.
        if (transplant)
        {
            await this.TransplantTempTableToOriginalAsync(
                tableName,
                entry.TDefPage,
                tableDef,
                tempName,
                tempEntry.TDefPage,
                persistedLvProp,
                cancellationToken).ConfigureAwait(false);

            // New complex columns were emitted under the temporary parent.
            // Their catalog ownership must follow the transplanted TDEF too.
            foreach (ColumnInfo column in tempDef.Columns)
            {
                if ((column.Type is ComplexType) && column.Misc > 0)
                {
                    await complexColumns.UpdateComplexColumnParentTableIdAsync(
                        column.Misc,
                        checked((int)entry.TDefPage),
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
        else
        {
            await this.DropTableCoreAsync(tableName, rewriting: true, cancellationToken).ConfigureAwait(false);
            await catalogWriter.RenameTableInCatalogAsync(tempName, tableName, persistedLvProp, cancellationToken).ConfigureAwait(false);

            foreach (ColumnDefinition survivor in newComplexById.Values)
            {
                await complexColumns.UpdateComplexColumnParentTableIdAsync(
                    survivor.ComplexId,
                    checked((int)tempEntry.TDefPage),
                    cancellationToken).ConfigureAwait(false);
            }

            foreach ((string colName, int complexId) in droppedComplex)
            {
                await complexColumns.DropSingleComplexChildAsync(entry.TDefPage, colName, complexId, (page, definition, token) => this.ReclaimTableStoragePagesAsync(page, definition, includeTDefRoot: true, token), cancellationToken).ConfigureAwait(false);
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
    /// Initializes newly added complex columns with per-row references. Reuse
    /// a surviving shared reference when it uniquely identifies the row;
    /// otherwise allocate a fresh reference for the new columns. Existing
    /// complex slots retain their original values, including Null.
    /// </summary>
    /// <param name="tableName">The table being rewritten.</param>
    /// <param name="tableDef">Its current definition.</param>
    /// <param name="newDefs">The projected column list.</param>
    /// <param name="rows">The projected rows, aligned with <paramref name="newDefs"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask InitializeNewComplexReferencesAsync(
        string tableName,
        TableDef tableDef,
        List<ColumnDefinition> newDefs,
        List<object[]> rows,
        CancellationToken cancellationToken)
    {
        List<int> complexColumns = [];
        List<int> addedColumns = [];
        for (int i = 0; i < newDefs.Count; i++)
        {
            if (newDefs[i].IsAttachment || newDefs[i].IsMultiValue)
            {
                complexColumns.Add(i);
                if (newDefs[i].ComplexId == 0 && newDefs[i].SourceColumn is null)
                {
                    addedColumns.Add(i);
                }
            }
        }

        if (addedColumns.Count == 0)
        {
            return;
        }

        // Every surviving parent reference must belong to one row.
        int?[] shared = new int?[rows.Count];
        var holders = new HashSet<int>();
        for (int r = 0; r < rows.Count; r++)
        {
            if (SharedComplexReference(rows[r], complexColumns, newDefs) is int reference)
            {
                shared[r] = reference;
                if (!holders.Add(reference))
                {
                    ColumnDefinition column = newDefs.First(definition => definition.ComplexId != 0 && (definition.IsAttachment || definition.IsMultiValue));
                    ResolvedTable parent = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptComplexColumn,
                        $"Parent rows of '{tableName}' reuse complex reference {reference} for '{column.Name}'.",
                        new JetErrorInfo { TableName = tableName, ColumnName = column.Name, PageNumber = parent.Entry.TDefPage });
                }
            }
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
        byte[] tempTdef = await pager.ReadPageAsync(tempTdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (tempTdef[0] != Constants.PageTypes.TableDefinition || Ri32(tempTdef, 4) != 0)
            {
                throw new NotSupportedException("Complex table schema rewrite currently requires a single-page rebuilt TDEF.");
            }

            await this.ReclaimTableStoragePagesAsync(originalTdefPage, originalDef, includeTDefRoot: false, cancellationToken).ConfigureAwait(false);
            await this.PatchTablePageOwnersAsync(tempTdefPage, originalTdefPage, cancellationToken).ConfigureAwait(false);
            await pager.WritePageAsync(originalTdefPage, tempTdef, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(tempTdef);
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
        long totalPages = pager.PageCount;
        for (long pageNumber = 3; pageNumber < totalPages; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                bool patchDataPage = page[0] == Constants.PageTypes.Data && Ri32(page, format.DataPage.TDefOff) == fromTdefPage;
                bool patchIndexPage = page[0] is Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf && Ri32(page, 4) == fromTdefPage;
                if (!patchDataPage && !patchIndexPage)
                {
                    continue;
                }

                int ownerOffset = patchDataPage ? format.DataPage.TDefOff : 4;
                Wi32(page, ownerOffset, checked((int)toTdefPage));
                await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                PageBuffers.Return(page);
            }
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
            // No MSysRelationships row names the table (DropTableAsync removed or refused
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
                await complexColumns.DropComplexChildrenForTableAsync(parentTdefPage, (page, definition, token) => this.ReclaimTableStoragePagesAsync(page, definition, includeTDefRoot: true, token), cancellationToken).ConfigureAwait(false);
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
    /// page with the bitmap pages of its REFERENCE rows, and its TDEF pages.
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

        tableDef ??= await tableDefs.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long totalPages = pager.PageCount;
        if (tableDef is not null)
        {
            // Overflow rows are followed to their moved bytes, so their long
            // values are freed too.
            await ownedPages.ForEachOwnedDataPageAsync(
                tdefPage,
                (pageNumber, page, token) =>
                {
                    pagesToFree.Add(pageNumber);
                    return ownedPages.ForEachRowOnPageAsync(
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
            byte[] page = await pager.ReadPageAsync(currentTdefPage, cancellationToken).ConfigureAwait(false);
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
                PageBuffers.Return(page);
            }
        }

        if (firstTdefPage is not null && !format.IsJet3)
        {
            int usageMapPage = UsageMap.ReadUInt24(firstTdefPage, format.TDef.UsedPagesPage);
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
        long totalPages = pager.PageCount;
        if (usageMapPageNumber <= 0 || usageMapPageNumber >= totalPages)
        {
            return;
        }

        byte[] page = await pager.ReadPageAsync(usageMapPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.Data)
            {
                return;
            }

            foreach (RowBound rowBound in DataPageRows.EnumerateLiveRowBounds(format, page))
            {
                // A REFERENCE row's bitmap pages go with the table, the owned
                // and free-space rows' included.
                await UsageMap.CollectReferenceBitmapPagesAsync(page, rowBound, totalPages, pager.ReadPageAsync, PageBuffers.Return, pagesToFree, cancellationToken).ConfigureAwait(false);
                if (rowBound.RowIndex < 2)
                {
                    continue;
                }

                var indexPages = new List<long>();
                if (!await UsageMap.TryEnumeratePagesAsync(
                    page,
                    rowBound,
                    format.PageSize,
                    totalPages,
                    minimumPageNumber: 3,
                    strict: false,
                    pager.ReadPageAsync,
                    PageBuffers.Return,
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
            PageBuffers.Return(page);
        }
    }
}
