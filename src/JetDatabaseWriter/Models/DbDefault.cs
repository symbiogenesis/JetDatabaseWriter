namespace JetDatabaseWriter.Models;

using System;

/// <summary>
/// Stands for a column's default value in a row passed to an insert, the way
/// <see cref="DBNull.Value"/> stands for database null, and like <c>DEFAULT</c> in a SQL
/// <c>INSERT</c>.
/// </summary>
/// <remarks>
/// <para>
/// An insert stores every value it is given: <see langword="null"/> and
/// <see cref="DBNull.Value"/> store database null even in a column that has a default, as an
/// explicit Null does in an Access SQL <c>INSERT</c>. A column the insert leaves out (a
/// <see cref="RowValues"/> row that does not name it, or a column no property of a POCO maps
/// to) or sets to <see cref="Value"/> gets its default instead: an AutoNumber column
/// generates its next value, a calculated column is computed, and any other column stores
/// its <see cref="ColumnDefinition.DefaultValue"/> or
/// <see cref="ColumnDefinition.DefaultValueExpression"/> (or the <c>DefaultValue</c>
/// property stored in the file), or database null when it has none.
/// </para>
/// <para>
/// Only inserts accept it. An update never applies a default, so
/// <c>UpdateRowsAsync</c> throws <see cref="ArgumentException"/> for it.
/// </para>
/// </remarks>
public sealed class DbDefault
{
    /// <summary>The only instance, which stands for a column's default value.</summary>
    public static readonly DbDefault Value = new();

    private DbDefault()
    {
    }

    /// <summary>Returns <c>DEFAULT</c>, the SQL keyword this value stands for.</summary>
    /// <returns>The string <c>DEFAULT</c>.</returns>
    public override string ToString() => "DEFAULT";
}
