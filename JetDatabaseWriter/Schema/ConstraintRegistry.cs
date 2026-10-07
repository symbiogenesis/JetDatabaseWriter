namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Per-table client-side constraint registry. Manages column-level constraints
/// (NOT NULL, auto-increment, default values, validation rules) and applies them
/// at insert and update time. Keyed by table name (case-insensitive).
/// </summary>
/// <remarks>
/// A table created by this writer is registered from its <see cref="ColumnDefinition"/>
/// list, including the CLR <see cref="ColumnDefinition.DefaultValue"/> and
/// <see cref="ColumnDefinition.ValidationRule"/>. Any other table is hydrated from its
/// TDEF and <c>MSysObjects.LvProp</c>, which carry Required, AutoNumber, the calculated
/// expression and the persisted <c>DefaultValue</c> and <c>ValidationRule</c>
/// expressions, but not CLR delegates.
/// </remarks>
/// <param name="readLvPropForTable">
/// Delegate used to load <c>MSysObjects.LvProp</c> for a table by name when the
/// registry needs to hydrate from the persisted column properties (e.g. the
/// <c>Required</c> Boolean that backs <c>IsNullable</c>). May return <c>null</c>
/// when the table has no property block. Optional — if not supplied, hydration
/// reads AutoNumber flags and assumes non-AutoNumber columns are nullable.
/// </param>
/// <param name="readUsedAutoNumberHighWater">
/// Delegate that returns the largest AutoNumber value a table (by name, with
/// its definition) has used in a column (by index): the larger of its
/// persisted TDEF counter, the last value handed out, and the largest value
/// the column holds. The first AutoNumber a writer session assigns follows it,
/// so values freed by deleting the top rows in an earlier session are not
/// reused, and values another tool stored without raising the counter are not
/// handed out again. Optional; when not supplied the session starts from 1.
/// </param>
/// <param name="readComplexReferenceHighWater">
/// Delegate that returns a table's persisted TDEF complex AutoNumber after
/// validating its parent references and flat-table foreign keys against it.
/// The first reference a writer session assigns follows that counter.
/// Optional; when not supplied the session starts from 1.
/// </param>
/// <param name="textCollation">The database text comparison collation.</param>
internal sealed class ConstraintRegistry(
    Func<string, CancellationToken, ValueTask<ColumnPropertyBlock?>>? readLvPropForTable = null,
    Func<string, TableDef, int, CancellationToken, ValueTask<long>>? readUsedAutoNumberHighWater = null,
    Func<string, TableDef, CancellationToken, ValueTask<long>>? readComplexReferenceHighWater = null,
    JetDatabaseWriter.Indexes.Collation.JetTextCollation? textCollation = null)
{
    private readonly Dictionary<string, List<ColumnConstraint>> constraints =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, TableValidationConstraint?> tableRules = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Rewinds each auto-increment counter listed in <paramref name="checkpoints"/>
    /// back to the value it held before <see cref="ApplyAsync"/>
    /// advanced it. Restore runs in reverse so a multi-row batch that advances
    /// the same counter several times returns to the earliest checkpoint.
    /// </summary>
    /// <param name="checkpoints">The checkpoints.</param>
    public static void RestoreAutoCounters(List<(ColumnConstraint Constraint, long? PreviousValue)>? checkpoints)
    {
        if (checkpoints == null)
        {
            return;
        }

        for (int index = checkpoints.Count - 1; index >= 0; index--)
        {
            (ColumnConstraint? constraint, long? previousValue) = checkpoints[index];
            constraint.NextAutoValue = previousValue;
        }
    }

    public void Register(string tableName, IReadOnlyList<ColumnDefinition> defs)
    {
        this.tableRules.Remove(tableName);
        var list = new List<ColumnConstraint>(defs.Count);
        bool anyConstraint = false;
        foreach (ColumnDefinition def in defs)
        {
            ColumnConstraint c = ToConstraint(def);
            anyConstraint |= c.HasAnyConstraint;

            if (c.IsAutoIncrement && !IsIntegralType(c.ClrType) && c.ClrType != typeof(Guid))
            {
                throw new ArgumentException(
                    $"Column '{c.Name}' is marked IsAutoIncrement=true but its CLR type '{c.ClrType}' is not an integer or GUID type.",
                    nameof(defs));
            }

            if (c.IsAutoIncrement && (c.ClrType == typeof(byte) || c.ClrType == typeof(long)))
            {
                // Jet's FLAG_AUTO_LONG only persists Int16/Int32 counters; tinyint and BigInt
                // ("Large Number") autonumber columns require schema bits the writer does not
                // emit yet. Reject up-front so callers get a typed signal instead of a corrupt
                // schema on first insert.
                throw new NotSupportedException(
                    $"Column '{c.Name}': IsAutoIncrement is only supported for Int16, Int32 and Guid; '{c.ClrType}' is not supported.");
            }

            list.Add(c);
        }

        if (anyConstraint)
        {
            this.constraints[tableName] = list;
        }
        else
        {
            this.constraints.Remove(tableName);
        }
    }

    public void Unregister(string tableName)
    {
        this.constraints.Remove(tableName);
        this.tableRules.Remove(tableName);
    }

    public void Rename(string oldName, string newName)
    {
        if (this.tableRules.TryGetValue(oldName, out TableValidationConstraint? rule))
        {
            this.tableRules.Remove(oldName);
            this.tableRules[newName] = rule;
        }

        if (this.constraints.TryGetValue(oldName, out List<ColumnConstraint>? list))
        {
            this.constraints.Remove(oldName);
            this.constraints[newName] = list;
        }
    }

    /// <summary>
    /// Attempts to retrieve the constraint list for a table.
    /// Returns <c>true</c> if constraints were registered for this table.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="constraints">The constraints.</param>
    public bool TryGet(string tableName, [NotNullWhen(true)] out List<ColumnConstraint>? constraints) => this.constraints.TryGetValue(tableName, out constraints);

    /// <summary>Refuses ambiguous persisted constraints before any values are changed.</summary>
    /// <param name="tableName">The table whose metadata is being validated.</param>
    /// <param name="properties">The persisted property block.</param>
    /// <exception cref="JetCorruptDataException">Repeated constraint values conflict.</exception>
    internal static void ValidatePersistedConstraintProperties(string tableName, ColumnPropertyBlock? properties)
    {
        if (properties is null)
        {
            return;
        }

        var targets = new Dictionary<string, Dictionary<string, ColumnPropertyEntry>>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnPropertyTarget target in properties.Targets)
        {
            if (!targets.TryGetValue(target.Name, out Dictionary<string, ColumnPropertyEntry>? entries))
            {
                entries = new Dictionary<string, ColumnPropertyEntry>(StringComparer.OrdinalIgnoreCase);
                targets.Add(target.Name, entries);
            }

            foreach (ColumnPropertyEntry entry in target.Entries)
            {
                bool modelled = string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationRule, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(entry.Name, Constants.ColumnPropertyNames.ValidationText, StringComparison.OrdinalIgnoreCase)
                    || (target.Name.Length != 0 && (string.Equals(entry.Name, Constants.ColumnPropertyNames.DefaultValue, StringComparison.OrdinalIgnoreCase)
                        || string.Equals(entry.Name, Constants.ColumnPropertyNames.Required, StringComparison.OrdinalIgnoreCase)));
                if (!modelled)
                {
                    continue;
                }

                if (entries.TryGetValue(entry.Name, out ColumnPropertyEntry? previous)
                    && (previous.DataType != entry.DataType || !previous.Value.AsSpan().SequenceEqual(entry.Value)))
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptTableDefinition,
                        $"Table '{tableName}' has conflicting repeated '{entry.Name}' properties for target '{target.Name}'.",
                        new JetErrorInfo { TableName = tableName });
                }

                entries[entry.Name] = entry;
            }
        }
    }

    /// <summary>
    /// Captures the registry's contents: every table's constraint list and each
    /// AutoNumber and complex-reference counter. A transaction takes one when it
    /// begins, because its DDL re-registers, unregisters and renames entries and
    /// its inserts advance counters, and none of that is in the journal it discards.
    /// </summary>
    /// <returns>A snapshot to pass to <see cref="Restore"/>.</returns>
    internal ConstraintRegistrySnapshot CaptureSnapshot()
    {
        var tables = new Dictionary<string, List<ColumnConstraint>>(this.constraints.Count, StringComparer.OrdinalIgnoreCase);
        var autoCounters = new List<(ColumnConstraint Constraint, long? NextAutoValue)>();
        foreach (KeyValuePair<string, List<ColumnConstraint>> entry in this.constraints)
        {
            tables.Add(entry.Key, [.. entry.Value]);
            foreach (ColumnConstraint constraint in entry.Value)
            {
                if (constraint.IsAutoIncrement || constraint.IsComplexReference)
                {
                    autoCounters.Add((constraint, constraint.NextAutoValue));
                }
            }
        }

        return new ConstraintRegistrySnapshot(tables, autoCounters, new Dictionary<string, TableValidationConstraint?>(this.tableRules, StringComparer.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Puts the registry back to <paramref name="snapshot"/>. Tables registered
    /// since are forgotten, tables unregistered or renamed since get their
    /// original constraint lists back under their original names (including
    /// in-code <c>DefaultValue</c> and <c>ValidationRule</c>, which exist only
    /// here), and AutoNumber counters rewind.
    /// </summary>
    /// <param name="snapshot">A snapshot from <see cref="CaptureSnapshot"/>.</param>
    internal void Restore(ConstraintRegistrySnapshot snapshot)
    {
        this.tableRules.Clear();
        foreach (KeyValuePair<string, TableValidationConstraint?> entry in snapshot.TableRules)
        {
            this.tableRules.Add(entry.Key, entry.Value);
        }

        this.constraints.Clear();
        foreach (KeyValuePair<string, List<ColumnConstraint>> entry in snapshot.Tables)
        {
            this.constraints.Add(entry.Key, [.. entry.Value]);
        }

        foreach ((ColumnConstraint constraint, long? nextAutoValue) in snapshot.AutoCounters)
        {
            constraint.NextAutoValue = nextAutoValue;
        }
    }

    /// <summary>
    /// Applies registered column constraints to <paramref name="values"/>, the
    /// row an insert is about to write, and returns a list of auto-increment
    /// counter checkpoints captured for the row. Callers should pass the
    /// returned list to <see cref="RestoreAutoCounters"/> if a later step (FK
    /// enforcement, data-page write, deferred unique-index check) rejects the
    /// row, so the counter rewinds to the value the failed insert tried to
    /// consume.
    /// </summary>
    /// <remarks>
    /// As in an Access SQL <c>INSERT</c>, a supplied null or
    /// <see cref="DBNull"/> is stored as null even when the column has a
    /// default, and a NOT NULL column rejects it. A column set to
    /// <see cref="DbDefault.Value"/> (which the insert front ends also put in
    /// the columns a row leaves out) gets its CLR or persisted default, or
    /// null when it has none. An AutoNumber column generates its next value,
    /// a complex column gets the row's complex reference, and a calculated
    /// column is computed, for any of the three. The row's complex reference
    /// is shared by all its complex columns: one the caller supplies in any
    /// of them, else the next from the table's counter.
    /// </remarks>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values, in table-column order. Every <see cref="DbDefault"/> is replaced, so it never reaches the row encoder.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">A NOT NULL column is null after defaults and AutoNumber values are applied.</exception>
    /// <exception cref="ArgumentException">A validation rule rejects a value, or the row's complex columns are given different references or one outside 1 to <see cref="int.MaxValue"/>.</exception>
    /// <exception cref="JetValidationRuleException">The operation is refused with a structured <see cref="JetValidationRuleException"/>.</exception>
    /// <exception cref="JetConstraintException">The operation is refused with a structured <see cref="JetConstraintException"/>.</exception>
    public async ValueTask<List<(ColumnConstraint Constraint, long? PreviousValue)>?> ApplyAsync(
        string tableName, TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        // Replace DbDefault before anything else, so the sentinel never reaches
        // the encoder, the unique checks or an expression.
        bool[]? requestsDefault = TakeDefaultRequests(values);

        List<ColumnConstraint> list = await this.GetOrHydrateAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false);

        // Metadata is aligned by column identity; a wrong-width row is always refused.
        if (list.Count != tableDef.Columns.Count || values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException($"Table '{tableName}' requires {tableDef.Columns.Count} values, but the row has {values.Length}.", nameof(values));
        }

        List<(ColumnConstraint Constraint, long? PreviousValue)>? checkpoints = null;
        CalculatedExpressionEvaluationContext? defaultContext = null;
        try
        {
            // Access gives every row one per-row complex reference, shared by
            // all its complex columns, when the row is inserted. A reference
            // the caller supplies is the row's, and the counter moves past it.
            int? rowReference = SuppliedComplexReference(tableName, list, values);
            if (rowReference is int supplied)
            {
                await this.KeepComplexCounterAboveAsync(tableName, tableDef, list, supplied, checkpoints = [], cancellationToken).ConfigureAwait(false);
            }

            for (int i = 0; i < list.Count; i++)
            {
                ColumnConstraint c = list[i];
                object? value = values[i];
                bool isNull = value is null or DBNull;
                bool takeDefault = requestsDefault?[i] == true;

                if (c.IsCalculated)
                {
                    values[i] = value ?? DBNull.Value;
                    continue;
                }

                if (c.IsComplexReference)
                {
                    // A null complex column gets the row's reference: the one
                    // supplied, or else the next from the table's counter.
                    if (isNull)
                    {
                        rowReference ??= await this.NextComplexReferenceAsync(tableName, tableDef, list, 1, checkpoints ??= [], cancellationToken).ConfigureAwait(false);
                        values[i] = new ComplexIdRef(rowReference.Value);
                    }

                    continue;
                }

                // Only a column the insert left out takes its default; an
                // explicit null is stored as null, as in Access SQL.
                if (takeDefault && c.DefaultValue != null)
                {
                    value = c.DefaultValue;
                    isNull = false;
                }
                else if (takeDefault && c.DefaultValueExpression != null)
                {
                    ColumnDefaultValue defaultValue = c.DefaultValuePlan ??= ColumnDefaultValue.Compile(c.DefaultValueExpression);
                    if (defaultValue.TryEvaluate(
                        c.ClrType,
                        () => defaultContext ??= new CalculatedExpressionEvaluationContext(tableDef, list, values, force: false, tableName, textCollation),
                        out object evaluated))
                    {
                        value = evaluated;
                        isNull = evaluated is DBNull;
                    }
                    else
                    {
                        throw JetErrors.Validation(JetErrorCode.ValidationRuleViolation, $"Default expression '{c.DefaultValueExpression}' for column '{c.Name}' on table '{tableName}' cannot be evaluated for the column type.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
                    }
                }

                if (isNull && c.IsAutoIncrement)
                {
                    if (c.ClrType == typeof(Guid))
                    {
                        value = Guid.NewGuid();
                    }
                    else
                    {
                        long? previous = c.NextAutoValue;
                        long next = await this.GetNextAutoValueAsync(tableName, tableDef, c, i, cancellationToken).ConfigureAwait(false);
                        (checkpoints ??= new List<(ColumnConstraint, long?)>(1)).Add((c, previous));
                        value = ConvertIntegral(next, c.ClrType);
                    }

                    isNull = false;
                }

                if (isNull && !c.IsNullable)
                {
                    throw JetErrors.Constraint(JetErrorCode.NotNullViolation, takeDefault ? $"Column '{c.Name}' on table '{tableName}' is marked NOT NULL and no value was supplied." : $"Column '{c.Name}' on table '{tableName}' is marked NOT NULL and cannot be set to null.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
                }

                if (!isNull && c.ValidationRule != null && !c.ValidationRule(value))
                {
                    throw JetErrors.Validation(JetErrorCode.ValidationRuleViolation, $"Validation rule for column '{c.Name}' on table '{tableName}' rejected value '{value}'.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
                }

                values[i] = value ?? DBNull.Value;
            }

            CalculatedExpressionEvaluator.Apply(tableDef, list, values, force: false, tableName, textCollation);
            ValidateCalculatedResults(tableName, list, values);
            CheckValidationRuleExpressions(tableName, tableDef, list, values, assignedColumns: null, textCollation);
            await this.CheckTableValidationRuleAsync(tableName, tableDef, list, values, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // A constraint failure after we already advanced one or more
            // auto-number counters must rewind those counters so the next
            // insert reuses the slot the rejected row would have taken.
            RestoreAutoCounters(checkpoints);
            throw;
        }

        return checkpoints;
    }

    /// <summary>
    /// Applies the update-time constraint pass to <paramref name="values"/>, the
    /// full post-update image of one row. Every column in
    /// <paramref name="assignedColumns"/> is checked the way an insert checks a
    /// supplied value: null is rejected for a NOT NULL or AutoNumber column, a
    /// non-null value must satisfy the column's <see cref="ColumnConstraint.ValidationRule"/>
    /// delegate, and any value must satisfy its persisted
    /// <see cref="ColumnConstraint.ValidationRuleExpression"/>. Defaults are not
    /// substituted, because a default applies only when a row is created.
    /// Calculated columns are recomputed from the new values.
    /// </summary>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="values">The post-update row, in table-column order.</param>
    /// <param name="assignedColumns">The indexes of the columns the update assigns.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">An assigned NOT NULL or AutoNumber column is set to null.</exception>
    /// <exception cref="ArgumentException">A validation rule rejects an assigned value.</exception>
    /// <exception cref="JetConstraintException">The operation is refused with a structured <see cref="JetConstraintException"/>.</exception>
    /// <exception cref="JetValidationRuleException">The operation is refused with a structured <see cref="JetValidationRuleException"/>.</exception>
    public async ValueTask ApplyUpdateAsync(
        string tableName,
        TableDef tableDef,
        object[] values,
        IEnumerable<int> assignedColumns,
        CancellationToken cancellationToken)
    {
        List<ColumnConstraint> list = await this.GetOrHydrateAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false);
        if (list.Count != tableDef.Columns.Count || values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException($"Table '{tableName}' requires {tableDef.Columns.Count} values, but the row has {values.Length}.", nameof(values));
        }

        var assigned = new List<int>(assignedColumns);
        foreach (int i in assigned)
        {
            ColumnConstraint c = list[i];
            if (c.IsCalculated)
            {
                // Recomputed below; the stored value never comes from the caller.
                continue;
            }

            object? value = values[i];
            bool isNull = value is null or DBNull;

            // An AutoNumber column is NOT NULL whatever IsNullable says: insert
            // only accepts null there because it generates a value, and an
            // update never renumbers a row.
            if (isNull && (!c.IsNullable || c.IsAutoIncrement))
            {
                throw JetErrors.Constraint(JetErrorCode.NotNullViolation, $"Column '{c.Name}' on table '{tableName}' is marked NOT NULL and cannot be set to null.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
            }

            if (!isNull && c.ValidationRule != null && !c.ValidationRule(value))
            {
                throw JetErrors.Validation(JetErrorCode.ValidationRuleViolation, $"Validation rule for column '{c.Name}' on table '{tableName}' rejected value '{value}'.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
            }
        }

        CalculatedExpressionEvaluator.Apply(tableDef, list, values, force: true, tableName, textCollation);
        ValidateCalculatedResults(tableName, list, values);
        CheckValidationRuleExpressions(tableName, tableDef, list, values, assigned, textCollation);
        await this.CheckTableValidationRuleAsync(tableName, tableDef, list, values, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Allocates <paramref name="count"/> consecutive per-row complex
    /// references for <paramref name="tableName"/> and returns the first. They
    /// come from the same session counter inserts use, seeded from the table's
    /// validated TDEF complex AutoNumber, so a reference handed out here is
    /// never assigned to an inserted row too. The caller raises the TDEF counter
    /// when it stores them. Schema rewrites use this for newly added complex
    /// columns when no surviving column supplies the row's reference; the table
    /// may have no complex column yet.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table's current definition.</param>
    /// <param name="count">How many references to allocate.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The table would use a reference above <see cref="int.MaxValue"/>.</exception>
    internal async ValueTask<int> AllocateComplexReferencesAsync(string tableName, TableDef tableDef, int count, CancellationToken cancellationToken)
    {
        List<ColumnConstraint> list = await this.GetOrHydrateAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false);
        return await this.NextComplexReferenceAsync(tableName, tableDef, list, count, checkpoints: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Invalidates a table's rule after its persisted properties change.</summary>
    /// <param name="tableName">The table name.</param>
    internal void InvalidateTableRule(string tableName) => this.tableRules.Remove(tableName);

    /// <summary>Checks a proposed rule against one existing row before property mutation.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="values">The existing row.</param>
    /// <param name="rule">The proposed validation rule.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask ValidateTableRuleAsync(string tableName, TableDef tableDef, object[] values, TableValidationConstraint rule, CancellationToken cancellationToken)
    {
        List<ColumnConstraint> columns = await this.GetOrHydrateAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false);
        this.EvaluateTableValidationRule(tableName, tableDef, columns, values, rule);
    }

    private static ColumnConstraint ToConstraint(ColumnDefinition def)
    {
        // Access gives AutoNumber, calculated and complex columns no default.
        // CreateTable and AddColumn reject one declared on them, but a schema
        // rewrite carries over a DefaultValue property another tool stored on
        // one; as in HydrateFromTableDef, it must not stop the column generating
        // its value. A DBNull CLR default is no default.
        bool takesDefault = def.CanHaveDefault;
        return new()
        {
            Name = def.Name,
            StorageType = JetTypeInfo.TypeCodeFromDefinition(def),
            ClrType = def.ClrType,
            IsNullable = def.IsNullable,
            DefaultValue = takesDefault ? ToAppliedClrDefault(def) : null,
            IsAutoIncrement = def.IsAutoIncrement,
            ValidationRule = def.ValidationRule,
            DefaultValueExpression = takesDefault ? NullIfBlank(def.DefaultValueExpression) ?? JetExpressionConverter.ToJetExpression(def.DefaultValue) : null,
            ValidationRuleExpression = NullIfBlank(def.ValidationRuleExpression),
            ValidationText = def.ValidationText,
            IsCalculated = def.IsCalculated,
            CalculationExpression = def.CalculationExpression,
            CalculatedResultType = JetTypeInfo.TypeCodeFromDefinition(def),
            IsComplexReference = def.IsAttachment || def.IsMultiValue,
        };
    }

    /// <summary>
    /// Applies a CLR default through the same literal conversion as a reopened writer.
    /// An explicit expression keeps the declaring writer's CLR override. Otherwise a
    /// literal that cannot be evaluated or converted supplies no default in either session.
    /// </summary>
    /// <param name="def">The column declaration.</param>
    /// <returns>The converted default, or null when the literal supplies no value.</returns>
    private static object? ToAppliedClrDefault(ColumnDefinition def)
    {
        object? value = def.DefaultValue;
        if (value is null or DBNull)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(def.DefaultValueExpression))
        {
            return value;
        }

        string expression = JetExpressionConverter.ToJetExpression(value)!;
        Type targetType = def.ClrType == typeof(Hyperlink) ? typeof(string) : def.ClrType;
        return ColumnDefaultValue.Compile(expression).TryEvaluate(
            targetType,
            static () => new CalculatedExpressionEvaluationContext(new TableDef(), [], [], force: false),
            out object evaluated)
            ? evaluated
            : null;
    }

    /// <summary>
    /// Replaces every <see cref="DbDefault"/> in <paramref name="values"/> with
    /// <see cref="DBNull.Value"/> and records which columns asked for their default.
    /// </summary>
    /// <param name="values">The row, in table-column order.</param>
    /// <returns>A flag per column, or <see langword="null"/> when no column asked for its default.</returns>
    private static bool[]? TakeDefaultRequests(object[] values)
    {
        bool[]? requested = null;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is DbDefault)
            {
                (requested ??= new bool[values.Length])[i] = true;
                values[i] = DBNull.Value;
            }
        }

        return requested;
    }

    /// <summary>
    /// Returns the per-row complex reference the caller supplied in the row's
    /// complex columns, or <see langword="null"/> when they hold none. Every
    /// complex column of a row shares one reference, so all the ones supplied
    /// must be equal, as Jackcess requires.
    /// </summary>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="constraints">The column constraints, in table-column order.</param>
    /// <param name="values">The row, in table-column order.</param>
    /// <returns>The supplied reference, or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentException">Two complex columns hold different references, or one holds a reference outside 1 to <see cref="int.MaxValue"/>.</exception>
    private static int? SuppliedComplexReference(string tableName, List<ColumnConstraint> constraints, object[] values)
    {
        int? shared = null;
        string? sharedColumn = null;
        for (int i = 0; i < constraints.Count; i++)
        {
            ColumnConstraint c = constraints[i];
            if (!c.IsComplexReference || values[i] is null or DBNull || !TryGetComplexReference(values[i], out long supplied))
            {
                continue;
            }

            if (supplied is < 1 or > int.MaxValue)
            {
                throw new ArgumentException(
                    $"Column '{c.Name}' on table '{tableName}' holds {supplied}, which is not a per-row complex reference; references run from 1 to {int.MaxValue}.");
            }

            if (shared is int previous && previous != supplied)
            {
                throw new ArgumentException(
                    $"Columns '{sharedColumn}' and '{c.Name}' on table '{tableName}' hold different complex references ({previous} and {supplied}); all the complex columns of a row share one.");
            }

            shared = (int)supplied;
            sharedColumn = c.Name;
        }

        return shared;
    }

    private static bool TryGetComplexReference(object value, out long reference)
    {
        switch (value)
        {
            case ComplexIdRef complexRef:
                reference = complexRef.Id;
                return true;
            case int i:
                reference = i;
                return true;
            case long l:
                reference = l;
                return true;
            case short s:
                reference = s;
                return true;
            case byte b:
                reference = b;
                return true;
            default:
                reference = 0;
                return false;
        }
    }

    private static string? NullIfBlank(string? text) => string.IsNullOrWhiteSpace(text) ? null : text;

    /// <summary>Builds a contextual table-rule refusal.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rule">The persisted rule.</param>
    /// <param name="reason">The failure reason.</param>
    /// <returns>The contextual validation failure.</returns>
    private static JetValidationRuleException TableRuleFailure(string tableName, TableValidationConstraint rule, string reason)
        => JetErrors.Validation(JetErrorCode.TableValidationRuleViolation, $"Table validation rule '{rule.Expression}' on table '{tableName}' {reason}. {rule.ValidationText}", new JetErrorInfo { TableName = tableName, Reason = reason });

    /// <summary>
    /// Evaluates the persisted <see cref="ColumnConstraint.ValidationRuleExpression"/> of each
    /// non-calculated column in <paramref name="assignedColumns"/> (every column when
    /// <see langword="null"/>) against the finished row. A rule this library cannot parse or
    /// evaluate refuses the operation before mutation; see <see cref="ColumnValidationRule"/>.
    /// </summary>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="constraints">The column constraints, in table-column order.</param>
    /// <param name="values">The row, in table-column order.</param>
    /// <param name="assignedColumns">The columns to check, or <see langword="null"/> for all.</param>
    /// <param name="textCollation">The database's text comparison collation.</param>
    /// <exception cref="ArgumentException">A rule evaluates to False.</exception>
    /// <exception cref="JetValidationRuleException">The operation is refused with a structured <see cref="JetValidationRuleException"/>.</exception>
    private static void CheckValidationRuleExpressions(
        string tableName,
        TableDef tableDef,
        List<ColumnConstraint> constraints,
        object[] values,
        List<int>? assignedColumns,
        JetDatabaseWriter.Indexes.Collation.JetTextCollation? textCollation)
    {
        CalculatedExpressionEvaluationContext? context = null;
        for (int i = 0; i < constraints.Count; i++)
        {
            ColumnConstraint c = constraints[i];
            if (c.ValidationRuleExpression is null || (assignedColumns != null && !c.IsCalculated && !assignedColumns.Contains(i)))
            {
                continue;
            }

            ColumnValidationRule rule = c.ValidationRulePlan ??= ColumnValidationRule.Compile(c.ValidationRuleExpression, c.Name);
            context ??= new CalculatedExpressionEvaluationContext(tableDef, constraints, values, force: false, tableName, textCollation);
            try
            {
                if (rule.Accepts(context))
                {
                    continue;
                }
            }
            catch (Exception ex) when (ColumnValidationRule.IsEvaluationFailure(ex))
            {
                throw JetErrors.Validation(JetErrorCode.ValidationRuleViolation, $"Validation rule '{c.ValidationRuleExpression}' for column '{c.Name}' on table '{tableName}' cannot be evaluated.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
            }

            object value = values[i];
            string shown = value is null or DBNull ? "Null" : "'" + Convert.ToString(value, CultureInfo.InvariantCulture) + "'";
            string message = $"Validation rule '{c.ValidationRuleExpression}' for column '{c.Name}' on table '{tableName}' rejected value {shown}.";
            throw JetErrors.Validation(JetErrorCode.ValidationRuleViolation, string.IsNullOrEmpty(c.ValidationText) ? message : message + " " + c.ValidationText, new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
        }
    }

    private static void ValidateCalculatedResults(string tableName, List<ColumnConstraint> constraints, object[] values)
    {
        for (int i = 0; i < constraints.Count; i++)
        {
            ColumnConstraint c = constraints[i];
            if (!c.IsCalculated)
            {
                continue;
            }

            object? value = values[i];
            bool isNull = value is null or DBNull;
            if (isNull && !c.IsNullable)
            {
                throw JetErrors.Constraint(JetErrorCode.NotNullViolation, $"Calculated column '{c.Name}' on table '{tableName}' evaluated to NULL but is marked NOT NULL.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
            }

            if (!isNull && c.ValidationRule != null && !c.ValidationRule(value))
            {
                throw JetErrors.Validation(JetErrorCode.ValidationRuleViolation, $"Validation rule for calculated column '{c.Name}' on table '{tableName}' rejected value '{value}'.", new JetErrorInfo { TableName = tableName, ColumnName = c.Name });
            }
        }
    }

    private static bool IsIntegralType(Type t) => t == typeof(byte) || t == typeof(short) || t == typeof(int) || t == typeof(long);

    // The return type must remain 'object' so callers can store the boxed integral
    // (byte/short/int/long) directly into a values[] array preserving the column's CLR type.
#pragma warning disable CA1859
    private static object ConvertIntegral(long value, Type targetType)
#pragma warning restore CA1859
    {
        if (targetType == typeof(byte))
        {
            return checked((byte)value);
        }

        if (targetType == typeof(short))
        {
            return checked((short)value);
        }

        if (targetType == typeof(int))
        {
            return checked((int)value);
        }

        if (targetType == typeof(long))
        {
            return value;
        }

        return value;
    }

    private static ColumnType ResolveCalculatedResultType(ColumnInfo col, ColumnPropertyTarget? target)
    {
        if (!col.IsCalculated)
        {
            return default;
        }

        ColumnPropertyEntry? resultType = target?.Find(Constants.ColumnPropertyNames.ResultType);
        return resultType?.Value.Length > 0 ? (ColumnType)resultType.Value[0] : col.Type;
    }

    /// <summary>Evaluates the complete row against a cached rule.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="columns">The column metadata.</param>
    /// <param name="values">The complete row.</param>
    /// <param name="rule">The validation rule.</param>
    /// <exception cref="JetValidationRuleException">The rule cannot be evaluated or rejects the row.</exception>
    private void EvaluateTableValidationRule(string tableName, TableDef tableDef, List<ColumnConstraint> columns, object[] values, TableValidationConstraint rule)
    {
        try
        {
            rule.Plan ??= CalculatedExpressionPlan.Parse(rule.Expression, allowQualifiedReferences: false);
            var context = new CalculatedExpressionEvaluationContext(tableDef, columns, values, force: false, tableName, textCollation);
            object result = rule.Plan.Root.Evaluate(context, rule.Plan);
            if (CalculatedExpressionCoercion.IsNull(result) || CalculatedExpressionCoercion.ToBoolean(result))
            {
                return;
            }
        }
        catch (Exception ex) when (ColumnValidationRule.IsEvaluationFailure(ex))
        {
            throw TableRuleFailure(tableName, rule, "cannot be evaluated");
        }

        throw TableRuleFailure(tableName, rule, "rejected the row");
    }

    /// <summary>Checks the persisted table rule against the complete candidate row.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="columns">The column constraints.</param>
    /// <param name="values">The complete candidate row.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetValidationRuleException">The stored rule rejects the row or cannot be evaluated.</exception>
    private async ValueTask CheckTableValidationRuleAsync(string tableName, TableDef tableDef, List<ColumnConstraint> columns, object[] values, CancellationToken cancellationToken)
    {
        if (!this.tableRules.TryGetValue(tableName, out TableValidationConstraint? rule))
        {
            ColumnPropertyBlock? properties = readLvPropForTable is null ? null : await readLvPropForTable(tableName, cancellationToken).ConfigureAwait(false);
            this.CacheTableValidationRule(tableName, properties);
            rule = this.tableRules[tableName];
        }

        if (rule is null)
        {
            return;
        }

        this.EvaluateTableValidationRule(tableName, tableDef, columns, values, rule);
    }

    /// <summary>Remembers the rule or its absence without relying on property block order.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="properties">The readable persisted properties, or absence.</param>
    private void CacheTableValidationRule(string tableName, ColumnPropertyBlock? properties)
    {
        ValidatePersistedConstraintProperties(tableName, properties);
        ColumnPropertyTarget? target = properties?.FindTableTarget();
        string? expression = NullIfBlank(PersistedExpressionText.Normalize(target?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, properties!.Format)));
        this.tableRules[tableName] = expression is null ? null : new TableValidationConstraint(expression, target?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, properties!.Format));
    }

    private static bool MatchesColumnIdentity(ColumnConstraint constraint, ColumnInfo column)
        => string.Equals(constraint.Name, column.Name, StringComparison.OrdinalIgnoreCase)
            && constraint.StorageType == column.Type
            && constraint.IsAutoIncrement == column.IsAutoNumber
            && constraint.IsCalculated == column.IsCalculated
            && constraint.IsComplexReference == (column.Type is ComplexType);

    private async ValueTask<List<ColumnConstraint>> GetOrHydrateAsync(string tableName, TableDef tableDef, CancellationToken cancellationToken)
    {
        this.constraints.TryGetValue(tableName, out List<ColumnConstraint>? list);
        if (list != null && list.Count == tableDef.Columns.Count)
        {
            bool aligned = true;
            for (int index = 0; index < list.Count; index++)
            {
                aligned &= MatchesColumnIdentity(list[index], tableDef.Columns[index]);
            }

            if (aligned)
            {
                return list;
            }
        }

        // The table may have been created by an earlier writer instance (or by Access
        // itself). Hydrate the registry from the persisted column flags and LvProp so
        // NOT NULL, AutoIncrement, and calculated-column expressions still take effect
        // after the database is closed and reopened.
        ColumnPropertyBlock? props = readLvPropForTable is null
            ? null
            : await readLvPropForTable(tableName, cancellationToken).ConfigureAwait(false);
        List<ColumnConstraint> hydrated = this.HydrateFromTableDef(tableName, tableDef, props);
        if (list != null)
        {
            for (int index = 0; index < hydrated.Count; index++)
            {
                ColumnConstraint current = hydrated[index];
                ColumnConstraint? registered = list.Find(candidate =>
                    string.Equals(candidate.Name, current.Name, StringComparison.OrdinalIgnoreCase)
                    && candidate.ClrType == current.ClrType
                    && candidate.StorageType == current.StorageType
                    && candidate.IsAutoIncrement == current.IsAutoIncrement
                    && candidate.IsCalculated == current.IsCalculated
                    && (!current.IsCalculated || candidate.CalculatedResultType == current.CalculatedResultType)
                    && candidate.IsComplexReference == current.IsComplexReference);
                if (registered != null)
                {
                    hydrated[index] = registered;
                }
            }
        }

        return hydrated;
    }

    /// <summary>
    /// Rebuilds a per-column constraint list from the persisted TDEF column flags
    /// and (when supplied) the table's <c>MSysObjects.LvProp</c> property block.
    /// IsNullable comes from the LvProp <c>Required</c> Boolean when present
    /// (DAO/Access wire format). Integer and GUID AutoNumber flags are restored from the
    /// TDEF descriptor. The LvProp <c>DefaultValue</c>, <c>ValidationRule</c> and
    /// <c>ValidationText</c> properties become the persisted default and rule
    /// expressions. The CLR <see cref="ColumnConstraint.DefaultValue"/> and
    /// <see cref="ColumnConstraint.ValidationRule"/> are only present when the
    /// same writer instance declared them; a CLR default also reaches later
    /// writers, because it is persisted as a literal <c>DefaultValue</c> expression.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="properties">The properties.</param>
    private List<ColumnConstraint> HydrateFromTableDef(string tableName, TableDef tableDef, ColumnPropertyBlock? properties = null)
    {
        this.CacheTableValidationRule(tableName, properties);
        var list = new List<ColumnConstraint>(tableDef.Columns.Count);
        foreach (ColumnInfo col in tableDef.Columns)
        {
            ColumnPropertyTarget? propertyTarget = properties?.FindTarget(col.Name);

            // Complex columns (Attachment / Complex) carry a magic Flags = 0x07
            // marker rather than real flag bits.
            // Bit 0x02 is now always set by the writer for DAO compatibility (Jackcess
            // UNKNOWN_FF_FLAG_MASK), so it can no longer carry IsNullable. IsNullable
            // is sourced from MSysObjects.LvProp's Required Boolean (DAO wire format).
            bool isComplex = col.Type is ComplexType;
            bool isNullable;
            bool isAutoIncrement = col.IsAutoNumber;
            if (isComplex)
            {
                isNullable = true;
            }
            else if (isAutoIncrement)
            {
                isNullable = false;
            }
            else
            {
                bool? required = propertyTarget?.GetBooleanValue(Constants.ColumnPropertyNames.Required);
                isNullable = required is not true;
            }

            ColumnType calculatedResultType = ResolveCalculatedResultType(col, propertyTarget);
            ColumnType constraintType = calculatedResultType != default ? calculatedResultType : col.Type;
            JetFormat? propertyFormat = properties?.Format;
            string? calculationExpression = PersistedExpressionText.Normalize(propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.Expression, propertyFormat!));

            // Access gives AutoNumber, calculated and complex columns no default; a stray
            // DefaultValue property on one must not stop the column generating its value.
            bool takesDefault = !isComplex && !isAutoIncrement && !col.IsCalculated;

            ColumnConstraint c = new()
            {
                Name = col.Name,
                StorageType = col.Type,
                ClrType = JetTypeInfo.GetClrType(constraintType) ?? typeof(object),
                IsNullable = isNullable,
                IsAutoIncrement = isAutoIncrement,
                DefaultValueExpression = takesDefault
                    ? NullIfBlank(PersistedExpressionText.Normalize(propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, propertyFormat!)))
                    : null,
                ValidationRuleExpression = isComplex
                    ? null
                    : NullIfBlank(PersistedExpressionText.Normalize(propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, propertyFormat!))),
                ValidationText = propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, propertyFormat!),

                IsCalculated = col.IsCalculated,
                CalculationExpression = calculationExpression,
                CalculatedResultType = calculatedResultType,
                IsComplexReference = isComplex,
            };

            list.Add(c);
        }

        // Always cache the hydrated list even when no column carries a constraint,
        // so subsequent inserts on the same table skip both HydrateFromTableDef and the
        // (potentially expensive) readLvPropForTable LvProp scan. Without this negative
        // caching, every row in a multi-row InsertRowsAsync re-reads MSysObjects.LvProp.
        this.constraints[tableName] = list;

        return list;
    }

    /// <summary>
    /// Hands out the next AutoNumber of column <paramref name="columnIndex"/>.
    /// The first one in a writer session follows the largest value the table
    /// has used (<c>readUsedAutoNumberHighWater</c>); later ones continue from
    /// the session counter.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="c">The column's constraint, which holds the session counter.</param>
    /// <param name="columnIndex">The column's index in <paramref name="tableDef"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<long> GetNextAutoValueAsync(string tableName, TableDef tableDef, ColumnConstraint c, int columnIndex, CancellationToken cancellationToken)
    {
        if (c.NextAutoValue == null)
        {
            long used = readUsedAutoNumberHighWater is null
                ? 0
                : await readUsedAutoNumberHighWater(tableName, tableDef, columnIndex, cancellationToken).ConfigureAwait(false);
            c.NextAutoValue = used + 1;
        }

        long assigned = c.NextAutoValue.Value;
        c.NextAutoValue = assigned + 1;
        return assigned;
    }

    /// <summary>
    /// Hands out <paramref name="count"/> consecutive per-row complex
    /// references and returns the first. The table's first complex constraint
    /// holds the session counter (seeded on first use from
    /// <c>readComplexReferenceHighWater</c>); a table with no complex column
    /// yet (a schema rewrite adding its first one) allocates straight from the
    /// seed, which its re-registration after the rewrite reads again.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="list">The table's constraints.</param>
    /// <param name="count">How many references to hand out.</param>
    /// <param name="checkpoints">Receives the counter's previous value, so a rejected insert can rewind it; <see langword="null"/> when the caller does not rewind.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">The table would use a reference above <see cref="int.MaxValue"/>.</exception>
    private async ValueTask<int> NextComplexReferenceAsync(
        string tableName,
        TableDef tableDef,
        List<ColumnConstraint> list,
        int count,
        List<(ColumnConstraint Constraint, long? PreviousValue)>? checkpoints,
        CancellationToken cancellationToken)
    {
        ColumnConstraint? holder = list.Count == tableDef.Columns.Count ? list.Find(c => c.IsComplexReference) : null;
        long first = holder?.NextAutoValue ?? (await this.ReadComplexSeedAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false) + 1);
        if (first + count - 1 > int.MaxValue)
        {
            throw new InvalidOperationException($"Table '{tableName}' has used every complex column reference up to {int.MaxValue}.");
        }

        if (holder is not null)
        {
            checkpoints?.Add((holder, holder.NextAutoValue));
            holder.NextAutoValue = first + count;
        }

        return (int)first;
    }

    /// <summary>
    /// Moves the table's complex-reference counter past a reference the caller
    /// supplied, so a later row is not given the same one.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="list">The table's constraints.</param>
    /// <param name="supplied">The supplied reference.</param>
    /// <param name="checkpoints">Receives the counter's previous value.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask KeepComplexCounterAboveAsync(
        string tableName,
        TableDef tableDef,
        List<ColumnConstraint> list,
        long supplied,
        List<(ColumnConstraint Constraint, long? PreviousValue)> checkpoints,
        CancellationToken cancellationToken)
    {
        ColumnConstraint? holder = list.Find(c => c.IsComplexReference);
        if (holder is null)
        {
            return;
        }

        long next = holder.NextAutoValue ?? (await this.ReadComplexSeedAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false) + 1);
        if (supplied >= next || holder.NextAutoValue is null)
        {
            checkpoints.Add((holder, holder.NextAutoValue));
            holder.NextAutoValue = Math.Max(next, supplied + 1);
        }
    }

    private ValueTask<long> ReadComplexSeedAsync(string tableName, TableDef tableDef, CancellationToken cancellationToken) =>
        readComplexReferenceHighWater is null
            ? new ValueTask<long>(0L)
            : readComplexReferenceHighWater(tableName, tableDef, cancellationToken);
}
