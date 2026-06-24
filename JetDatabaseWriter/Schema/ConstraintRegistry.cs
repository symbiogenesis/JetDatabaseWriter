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
/// at insert time. Keyed by table name (case-insensitive).
/// </summary>
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

    private static ColumnConstraint ToConstraint(ColumnDefinition def) => new()
    {
        Name = def.Name,
        ClrType = def.ClrType,
        IsNullable = def.IsNullable,
        DefaultValue = def.DefaultValue,
        IsAutoIncrement = def.IsAutoIncrement,
        ValidationRule = def.ValidationRule,
        IsCalculated = def.IsCalculated,
        CalculationExpression = def.CalculationExpression,
        CalculatedResultType = JetTypeInfo.TypeCodeFromDefinition(def),
    };

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
    /// TDEF descriptor. DefaultValue and ValidationRule remain client-side and
    /// are only present when the same writer instance declared them.
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

            ColumnConstraint c = new()
            {
                Name = col.Name,
                ClrType = JetTypeInfo.GetClrType(constraintType) ?? typeof(object),
                IsNullable = isNullable,
                IsAutoIncrement = isAutoIncrement,
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
