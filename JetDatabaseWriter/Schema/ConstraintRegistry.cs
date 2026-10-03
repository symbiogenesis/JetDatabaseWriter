namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
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
/// <param name="readTableSnapshot">
/// Delegate used to read a table snapshot for seeding auto-increment counters.
/// </param>
/// <param name="readLvPropForTable">
/// Delegate used to load <c>MSysObjects.LvProp</c> for a table by name when the
/// registry needs to hydrate from the persisted column properties (e.g. the
/// <c>Required</c> Boolean that backs <c>IsNullable</c>). May return <c>null</c>
/// when the table has no property block. Optional — if not supplied, hydration
/// falls back to the legacy TDEF flag bit only.
/// </param>
internal sealed class ConstraintRegistry(
    Func<string, CancellationToken, ValueTask<DataTable>> readTableSnapshot,
    Func<string, CancellationToken, ValueTask<ColumnPropertyBlock?>>? readLvPropForTable = null)
{
    private readonly Dictionary<string, List<ColumnConstraint>> constraints =
        new(StringComparer.OrdinalIgnoreCase);

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
        var list = new List<ColumnConstraint>(defs.Count);
        bool anyConstraint = false;
        foreach (ColumnDefinition def in defs)
        {
            ColumnConstraint c = ToConstraint(def);
            anyConstraint |= c.HasAnyConstraint;

            if (c.IsAutoIncrement && !IsIntegralType(c.ClrType))
            {
                throw new ArgumentException(
                    $"Column '{c.Name}' is marked IsAutoIncrement=true but its CLR type '{c.ClrType}' is not an integer type.",
                    nameof(defs));
            }

            if (c.IsAutoIncrement && (c.ClrType == typeof(byte) || c.ClrType == typeof(long)))
            {
                // Jet's FLAG_AUTO_LONG only persists Int16/Int32 counters; tinyint and BigInt
                // ("Large Number") autonumber columns require schema bits the writer does not
                // emit yet. Reject up-front so callers get a typed signal instead of a corrupt
                // schema on first insert.
                throw new NotSupportedException(
                    $"Column '{c.Name}': IsAutoIncrement is only supported for Int16 and Int32; '{c.ClrType}' is not supported.");
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

    public void Unregister(string tableName) => this.constraints.Remove(tableName);

    public void Rename(string oldName, string newName)
    {
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

    /// <summary>
    /// Captures the registry's contents: every table's constraint list and each
    /// AutoNumber constraint's counter. A transaction takes one when it begins,
    /// because its DDL re-registers, unregisters and renames entries and its
    /// inserts advance counters, and none of that is in the journal it discards.
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
                if (constraint.IsAutoIncrement)
                {
                    autoCounters.Add((constraint, constraint.NextAutoValue));
                }
            }
        }

        return new ConstraintRegistrySnapshot(tables, autoCounters);
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
    /// Applies registered column constraints to <paramref name="values"/> and
    /// returns a list of auto-increment counter checkpoints captured for the
    /// row. Callers should pass the returned list to
    /// <see cref="RestoreAutoCounters"/> if a later step (FK enforcement,
    /// data-page write, deferred unique-index check) rejects the row, so the
    /// counter rewinds to the value the failed insert tried to consume.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<List<(ColumnConstraint Constraint, long? PreviousValue)>?> ApplyAsync(
        string tableName, TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        List<ColumnConstraint> list = await this.GetOrHydrateAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false);

        // The constraint list is positionally aligned with the columns at registration time.
        // Add/Drop/Rename re-registers, so the count must match. Defensive bail-out otherwise.
        if (list.Count != tableDef.Columns.Count || values.Length != tableDef.Columns.Count)
        {
            return null;
        }

        List<(ColumnConstraint Constraint, long? PreviousValue)>? checkpoints = null;
        CalculatedExpressionEvaluationContext? defaultContext = null;
        try
        {
            for (int i = 0; i < list.Count; i++)
            {
                ColumnConstraint c = list[i];
                object? value = values[i];
                bool isNull = value is null or DBNull;

                if (c.IsCalculated)
                {
                    values[i] = value ?? DBNull.Value;
                    continue;
                }

                if (isNull && c.DefaultValue != null)
                {
                    value = c.DefaultValue;
                    isNull = false;
                }
                else if (isNull && c.DefaultValueExpression != null)
                {
                    ColumnDefaultValue defaultValue = c.DefaultValuePlan ??= ColumnDefaultValue.Compile(c.DefaultValueExpression);
                    if (defaultValue.TryEvaluate(
                        c.ClrType,
                        () => defaultContext ??= new CalculatedExpressionEvaluationContext(tableDef, list, values, force: false),
                        out object evaluated))
                    {
                        value = evaluated;
                        isNull = false;
                    }
                }

                if (isNull && c.IsAutoIncrement)
                {
                    long? previous = c.NextAutoValue;
                    long next = await this.GetNextAutoValueAsync(tableName, c, i, cancellationToken).ConfigureAwait(false);
                    (checkpoints ??= new List<(ColumnConstraint, long?)>(1)).Add((c, previous));
                    value = ConvertIntegral(next, c.ClrType);
                    isNull = false;
                }

                if (isNull && !c.IsNullable)
                {
                    throw new InvalidOperationException(
                        $"Column '{c.Name}' on table '{tableName}' is marked NOT NULL and no value was supplied.");
                }

                if (!isNull && c.ValidationRule != null && !c.ValidationRule(value))
                {
                    throw new ArgumentException(
                        $"Validation rule for column '{c.Name}' on table '{tableName}' rejected value '{value}'.");
                }

                values[i] = value ?? DBNull.Value;
            }

            CalculatedExpressionEvaluator.Apply(tableDef, list, values, force: false);
            ValidateCalculatedResults(tableName, list, values);
            CheckValidationRuleExpressions(tableName, tableDef, list, values, assignedColumns: null);
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

    public async ValueTask ApplyCalculatedAsync(string tableName, TableDef tableDef, object[] values, bool force, CancellationToken cancellationToken)
    {
        List<ColumnConstraint> list = await this.GetOrHydrateAsync(tableName, tableDef, cancellationToken).ConfigureAwait(false);
        if (list.Count != tableDef.Columns.Count || values.Length != tableDef.Columns.Count)
        {
            return;
        }

        CalculatedExpressionEvaluator.Apply(tableDef, list, values, force);
        ValidateCalculatedResults(tableName, list, values);
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
            return;
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
                throw new InvalidOperationException(
                    $"Column '{c.Name}' on table '{tableName}' is marked NOT NULL and cannot be set to null.");
            }

            if (!isNull && c.ValidationRule != null && !c.ValidationRule(value))
            {
                throw new ArgumentException(
                    $"Validation rule for column '{c.Name}' on table '{tableName}' rejected value '{value}'.");
            }
        }

        CalculatedExpressionEvaluator.Apply(tableDef, list, values, force: true);
        ValidateCalculatedResults(tableName, list, values);
        CheckValidationRuleExpressions(tableName, tableDef, list, values, assigned);
    }

    private static ColumnConstraint ToConstraint(ColumnDefinition def) => new()
    {
        Name = def.Name,
        ClrType = def.ClrType,
        IsNullable = def.IsNullable,
        DefaultValue = def.DefaultValue,
        IsAutoIncrement = def.IsAutoIncrement,
        ValidationRule = def.ValidationRule,
        DefaultValueExpression = NullIfBlank(def.DefaultValueExpression),
        ValidationRuleExpression = NullIfBlank(def.ValidationRuleExpression),
        ValidationText = def.ValidationText,
        IsCalculated = def.IsCalculated,
        CalculationExpression = def.CalculationExpression,
        CalculatedResultType = JetTypeInfo.TypeCodeFromDefinition(def),
    };

    private static string? NullIfBlank(string? text) => string.IsNullOrWhiteSpace(text) ? null : text;

    /// <summary>
    /// Evaluates the persisted <see cref="ColumnConstraint.ValidationRuleExpression"/> of each
    /// non-calculated column in <paramref name="assignedColumns"/> (every column when
    /// <see langword="null"/>) against the finished row. A rule this library cannot parse or
    /// evaluate is not enforced; see <see cref="ColumnValidationRule"/>.
    /// </summary>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="constraints">The column constraints, in table-column order.</param>
    /// <param name="values">The row, in table-column order.</param>
    /// <param name="assignedColumns">The columns to check, or <see langword="null"/> for all.</param>
    /// <exception cref="ArgumentException">A rule evaluates to False.</exception>
    private static void CheckValidationRuleExpressions(
        string tableName,
        TableDef tableDef,
        List<ColumnConstraint> constraints,
        object[] values,
        List<int>? assignedColumns)
    {
        CalculatedExpressionEvaluationContext? context = null;
        int count = assignedColumns?.Count ?? constraints.Count;
        for (int n = 0; n < count; n++)
        {
            int i = assignedColumns?[n] ?? n;
            ColumnConstraint c = constraints[i];
            if (c.ValidationRuleExpression is null || c.IsCalculated)
            {
                continue;
            }

            ColumnValidationRule rule = c.ValidationRulePlan ??= ColumnValidationRule.Compile(c.ValidationRuleExpression, c.Name);
            if (!rule.IsSupported)
            {
                continue;
            }

            context ??= new CalculatedExpressionEvaluationContext(tableDef, constraints, values, force: false);
            if (rule.Accepts(context))
            {
                continue;
            }

            object value = values[i];
            string shown = value is null or DBNull ? "Null" : "'" + Convert.ToString(value, CultureInfo.InvariantCulture) + "'";
            string message = $"Validation rule '{c.ValidationRuleExpression}' for column '{c.Name}' on table '{tableName}' rejected value {shown}.";
            throw new ArgumentException(string.IsNullOrEmpty(c.ValidationText) ? message : message + " " + c.ValidationText);
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
                throw new InvalidOperationException(
                    $"Calculated column '{c.Name}' on table '{tableName}' evaluated to NULL but is marked NOT NULL.");
            }

            if (!isNull && c.ValidationRule != null && !c.ValidationRule(value))
            {
                throw new ArgumentException(
                    $"Validation rule for calculated column '{c.Name}' on table '{tableName}' rejected value '{value}'.");
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

    private async ValueTask<List<ColumnConstraint>> GetOrHydrateAsync(string tableName, TableDef tableDef, CancellationToken cancellationToken)
    {
        if (this.constraints.TryGetValue(tableName, out List<ColumnConstraint>? list) && list != null)
        {
            return list;
        }

        // The table may have been created by an earlier writer instance (or by Access
        // itself). Hydrate the registry from the persisted column flags and LvProp so
        // NOT NULL, AutoIncrement, and calculated-column expressions still take effect
        // after the database is closed and reopened.
        ColumnPropertyBlock? props = readLvPropForTable is null
            ? null
            : await readLvPropForTable(tableName, cancellationToken).ConfigureAwait(false);
        return this.HydrateFromTableDef(tableName, tableDef, props);
    }

    /// <summary>
    /// Rebuilds a per-column constraint list from the persisted TDEF column flags
    /// and (when supplied) the table's <c>MSysObjects.LvProp</c> property block.
    /// IsNullable comes from the LvProp <c>Required</c> Boolean when present
    /// (DAO/Access wire format), falling back to the legacy writer-private TDEF
    /// flag bit <c>0x08</c>. <c>FLAG_AUTO_LONG (0x04)</c> is restored from the
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
        var list = new List<ColumnConstraint>(tableDef.Columns.Count);
        foreach (ColumnInfo col in tableDef.Columns)
        {
            ColumnPropertyTarget? propertyTarget = properties?.FindTarget(col.Name);

            // Complex columns (Attachment / Complex) carry a magic Flags = 0x07
            // marker rather than real flag bits; do not interpret 0x02 / 0x04 / 0x08 here.
            // Bit 0x02 is now always set by the writer for DAO compatibility (Jackcess
            // UNKNOWN_FF_FLAG_MASK), so it can no longer carry IsNullable. IsNullable
            // is sourced from MSysObjects.LvProp's Required Boolean (DAO wire format),
            // falling back to the legacy 0x08 bit only when LvProp is absent.
            bool isComplex = col.Type is AttachmentType or ComplexType;
            bool isNullable;
            bool isAutoIncrement = !isComplex && (col.Flags & Constants.ColumnDescriptorFlags.AutoNumber) != 0;
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
                isNullable = required is bool r ? !r : (col.Flags & Constants.ColumnDescriptorFlags.LegacyNotNull) == 0;
            }

            ColumnType calculatedResultType = ResolveCalculatedResultType(col, propertyTarget);
            ColumnType constraintType = calculatedResultType != default ? calculatedResultType : col.Type;
            DatabaseFormat propertyFormat = properties?.Format ?? default;
            string? calculationExpression = propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.Expression, propertyFormat);

            // Access gives AutoNumber, calculated and complex columns no default; a stray
            // DefaultValue property on one must not stop the column generating its value.
            bool takesDefault = !isComplex && !isAutoIncrement && !col.IsCalculated;

            ColumnConstraint c = new()
            {
                Name = col.Name,
                ClrType = JetTypeInfo.GetClrType(constraintType) ?? typeof(object),
                IsNullable = isNullable,
                IsAutoIncrement = isAutoIncrement,
                DefaultValueExpression = takesDefault
                    ? NullIfBlank(propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, propertyFormat))
                    : null,
                ValidationRuleExpression = isComplex
                    ? null
                    : NullIfBlank(propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, propertyFormat)),
                ValidationText = propertyTarget?.GetTextValue(Constants.ColumnPropertyNames.ValidationText, propertyFormat),
                IsCalculated = col.IsCalculated,
                CalculationExpression = calculationExpression,
                CalculatedResultType = calculatedResultType,
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

    private async ValueTask<long> GetNextAutoValueAsync(string tableName, ColumnConstraint c, int columnIndex, CancellationToken cancellationToken)
    {
        if (c.NextAutoValue == null)
        {
            long max = 0;
            using DataTable snapshot = await readTableSnapshot(tableName, cancellationToken).ConfigureAwait(false);
            if (snapshot.Columns.Count > columnIndex)
            {
                foreach (DataRow row in snapshot.Rows)
                {
                    object cell = row[columnIndex];
                    if (cell is null or DBNull)
                    {
                        continue;
                    }

                    try
                    {
                        long v = Convert.ToInt64(cell, CultureInfo.InvariantCulture);
                        if (v > max)
                        {
                            max = v;
                        }
                    }
                    catch (FormatException)
                    {
                    }
                    catch (InvalidCastException)
                    {
                    }
                    catch (OverflowException)
                    {
                    }
                }
            }

            c.NextAutoValue = max + 1;
        }

        long assigned = c.NextAutoValue.Value;
        c.NextAutoValue = assigned + 1;
        return assigned;
    }
}
