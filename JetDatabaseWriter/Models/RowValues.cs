namespace JetDatabaseWriter.Models;

using System;
using System.Collections;
using System.Collections.Generic;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// A named-column row payload for inserts. Each entry maps a column name to its
/// value, so column identity is by name rather than by position. This removes the
/// silent-corruption risk of positional <c>object?[]</c> rows whose order can drift
/// from the table schema.
/// </summary>
/// <remarks>
/// <para>
/// Column names are matched case-insensitively against the target table. Columns
/// that are not named are left to the engine's default — an AutoNumber column
/// generates its next value, and any other omitted column is stored as database
/// null. Both <see langword="null"/> and <see cref="DBNull.Value"/> represent
/// database null.
/// </para>
/// <para>
/// Instances support collection-initializer syntax and a fluent <see cref="Set"/>
/// builder:
/// <code>
/// var row = new RowValues { ["Name"] = "Alice", ["Score"] = 95.5m };
/// // or
/// var row = RowValues.Create().Set("Name", "Alice").Set("Score", 95.5m);
/// </code>
/// </para>
/// </remarks>
public sealed class RowValues : IEnumerable<KeyValuePair<string, object?>>
{
    private readonly Dictionary<string, object?> values =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Initializes a new instance of the <see cref="RowValues"/> class.</summary>
    public RowValues()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RowValues"/> class populated from
    /// an existing set of column-name/value pairs.
    /// </summary>
    /// <param name="values">The column-name/value pairs to seed the row with.</param>
    public RowValues(IEnumerable<KeyValuePair<string, object?>> values)
    {
        Guard.NotNull(values, nameof(values));
        foreach (KeyValuePair<string, object?> pair in values)
        {
            this[pair.Key] = pair.Value;
        }
    }

    /// <summary>Gets the number of named columns in this row.</summary>
    public int Count => this.values.Count;

    /// <summary>Gets the column names assigned in this row.</summary>
    public IReadOnlyCollection<string> Columns => this.values.Keys;

    /// <summary>Gets or sets the value for the named column.</summary>
    /// <param name="columnName">The column name (case-insensitive).</param>
    /// <returns>The value assigned to <paramref name="columnName"/>.</returns>
    public object? this[string columnName]
    {
        get
        {
            Guard.NotNullOrEmpty(columnName, nameof(columnName));
            return this.values[columnName];
        }

        set
        {
            Guard.NotNullOrEmpty(columnName, nameof(columnName));
            this.values[columnName] = value;
        }
    }

    /// <summary>Creates a new, empty <see cref="RowValues"/>.</summary>
    /// <returns>A new instance.</returns>
    public static RowValues Create() => [];

    /// <summary>Assigns a value to a column and returns this instance for chaining.</summary>
    /// <param name="columnName">The column name (case-insensitive).</param>
    /// <param name="value">The value to assign. <see langword="null"/> and <see cref="DBNull.Value"/> both mean database null.</param>
    /// <returns>This instance.</returns>
    public RowValues Set(string columnName, object? value)
    {
        this[columnName] = value;
        return this;
    }

    /// <summary>Adds a value for a column.</summary>
    /// <remarks>Supports collection-initializer syntax. Throws if the column was already assigned.</remarks>
    /// <param name="columnName">The column name (case-insensitive).</param>
    /// <param name="value">The value to assign.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="columnName"/> was already assigned.</exception>
    public void Add(string columnName, object? value)
    {
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        if (this.values.ContainsKey(columnName))
        {
            throw new ArgumentException($"Column '{columnName}' has already been assigned.", nameof(columnName));
        }

        this.values[columnName] = value;
    }

    /// <summary>Determines whether the named column has been assigned a value.</summary>
    /// <param name="columnName">The column name (case-insensitive).</param>
    /// <returns><see langword="true"/> when the column is present.</returns>
    public bool Contains(string columnName)
    {
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        return this.values.ContainsKey(columnName);
    }

    /// <summary>Attempts to get the value for the named column.</summary>
    /// <param name="columnName">The column name (case-insensitive).</param>
    /// <param name="value">When this method returns, the value if found; otherwise <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the column is present.</returns>
    public bool TryGetValue(string columnName, out object? value)
    {
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        return this.values.TryGetValue(columnName, out value);
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() => this.values.GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
}
