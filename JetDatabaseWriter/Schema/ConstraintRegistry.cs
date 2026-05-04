namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

/// <summary>
/// Per-table client-side constraint registry. Manages column-level constraints
/// (NOT NULL, auto-increment, default values, validation rules) and applies them
/// at insert time. Keyed by table name (case-insensitive).
/// </summary>
/// <param name="readTableSnapshot">
/// Delegate used to read a table snapshot for seeding auto-increment counters.
/// </param>
internal sealed class ConstraintRegistry(Func<string, CancellationToken, ValueTask<DataTable>> readTableSnapshot)
{
    private readonly Dictionary<string, List<ColumnConstraint>> _constraints =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Rewinds each auto-increment counter listed in <paramref name="checkpoints"/>
    /// back to the value it held before <see cref="ApplyAsync"/>
    /// advanced it. Used by the insert paths to undo counter advances when a
    /// deferred constraint (the post-write unique-index check) rejects the row
    /// after it has already consumed an auto-number value.
    /// </summary>
    public static void RestoreAutoCounters(List<(ColumnConstraint Constraint, long? PreviousValue)>? checkpoints)
    {
        if (checkpoints == null)
        {
            return;
        }

        foreach ((ColumnConstraint c, long? prev) in checkpoints)
        {
            c.NextAutoValue = prev;
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
            _constraints[tableName] = list;
        }
        else
        {
            _constraints.Remove(tableName);
        }
    }

    public void Unregister(string tableName)
    {
        _constraints.Remove(tableName);
    }

    public void Rename(string oldName, string newName)
    {
        if (_constraints.TryGetValue(oldName, out List<ColumnConstraint>? list))
        {
            _constraints.Remove(oldName);
            _constraints[newName] = list;
        }
    }

    /// <summary>
    /// Attempts to retrieve the constraint list for a table.
    /// Returns <c>true</c> if constraints were registered for this table.
    /// </summary>
    public bool TryGet(string tableName, [NotNullWhen(true)] out List<ColumnConstraint>? constraints)
    {
        return _constraints.TryGetValue(tableName, out constraints);
    }

    /// <summary>
    /// Applies registered column constraints to <paramref name="values"/> and
    /// returns a list of auto-increment counter checkpoints captured for the
    /// row. Callers should pass the returned list to
    /// <see cref="RestoreAutoCounters"/> if a later step (FK enforcement,
    /// data-page write, deferred unique-index check) rejects the row, so the
    /// counter rewinds to the value the failed insert tried to consume.
    /// </summary>
    public async ValueTask<List<(ColumnConstraint Constraint, long? PreviousValue)>?> ApplyAsync(
        string tableName, TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        if (!_constraints.TryGetValue(tableName, out List<ColumnConstraint>? list) || list == null)
        {
            // The table may have been created by an earlier writer instance (or by Access
            // itself). Hydrate the registry from the persisted column flags so NOT NULL /
            // AutoIncrement constraints declared at CreateTableAsync time still take effect
            // after the database is closed and reopened. DefaultValue and ValidationRule are
            // not persisted in the JET TDEF and remain client-side only.
            list = HydrateFromTableDef(tableName, tableDef);
        }

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
                bool isNull = value is null || value is DBNull;

                if (isNull && c.DefaultValue != null)
                {
                    value = c.DefaultValue;
                    isNull = false;
                }

                if (isNull && c.IsAutoIncrement)
                {
                    long? previous = c.NextAutoValue;
                    long next = await GetNextAutoValueAsync(tableName, c, i, cancellationToken).ConfigureAwait(false);
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

    private static ColumnConstraint ToConstraint(ColumnDefinition def)
    {
        return new ColumnConstraint
        {
            Name = def.Name,
            ClrType = def.ClrType,
            IsNullable = def.IsNullable,
            DefaultValue = def.DefaultValue,
            IsAutoIncrement = def.IsAutoIncrement,
            ValidationRule = def.ValidationRule,
        };
    }

    private static bool IsIntegralType(Type t)
    {
        return t == typeof(byte) || t == typeof(short) || t == typeof(int) || t == typeof(long);
    }

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

    private static Type TdefTypeToClrType(byte type)
    {
        switch (type)
        {
            case T_BOOL: return typeof(bool);
            case T_BYTE: return typeof(byte);
            case T_INT: return typeof(short);
            case T_LONG: return typeof(int);
            case T_MONEY: return typeof(decimal);
            case T_FLOAT: return typeof(float);
            case T_DOUBLE: return typeof(double);
            case T_DATETIME: return typeof(DateTime);
            case T_NUMERIC: return typeof(decimal);
            case T_GUID: return typeof(Guid);
            case T_TEXT:
            case T_MEMO: return typeof(string);
            case T_BINARY:
            case T_OLE: return typeof(byte[]);
            default: return typeof(object);
        }
    }

    /// <summary>
    /// Rebuilds a per-column constraint list from the persisted TDEF column flags.
    /// Only the bits that JET physically stores (FLAG_NULL_ALLOWED, FLAG_AUTO_LONG)
    /// are restored — DefaultValue and ValidationRule remain client-side and are
    /// only present when the same writer instance declared them.
    /// </summary>
    private List<ColumnConstraint> HydrateFromTableDef(string tableName, TableDef tableDef)
    {
        var list = new List<ColumnConstraint>(tableDef.Columns.Count);
        bool anyConstraint = false;
        foreach (ColumnInfo col in tableDef.Columns)
        {
            // Complex columns (T_ATTACHMENT / T_COMPLEX) carry a magic Flags = 0x07
            // marker rather than real flag bits; do not interpret 0x02 / 0x04 here.
            bool isComplex = col.Type == T_ATTACHMENT || col.Type == T_COMPLEX;
            bool isNullable = isComplex || (col.Flags & 0x02) != 0;
            bool isAutoIncrement = !isComplex && (col.Flags & 0x04) != 0;

            ColumnConstraint c = new()
            {
                Name = col.Name,
                ClrType = TdefTypeToClrType(col.Type),
                IsNullable = isNullable,
                IsAutoIncrement = isAutoIncrement,
            };

            anyConstraint |= c.HasAnyConstraint;
            list.Add(c);
        }

        if (anyConstraint)
        {
            _constraints[tableName] = list;
        }

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
                    if (cell is null || cell is DBNull)
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
