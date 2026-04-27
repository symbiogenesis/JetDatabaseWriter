namespace JetDatabaseWriter;

using System;

/// <summary>
/// Rich metadata about a database column including type, size, and nullability information.
/// </summary>
public sealed record ColumnMetadata
{
    /// <summary>Gets or initializes the column name.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Gets or initializes the Access-friendly type name (e.g., "Text", "Long Integer", "Date/Time").</summary>
    public string TypeName { get; init; } = string.Empty;

    /// <summary>Gets or initializes the CLR type that best represents this column.</summary>
    public Type ClrType { get; init; } = typeof(object);

    /// <summary>Gets or initializes the maximum length for variable-length columns, or null for fixed-length.</summary>
    public int? MaxLength { get; init; }

    /// <summary>Gets a value indicating whether the column allows null values (always true for JET databases).</summary>
    public bool IsNullable { get; init; }

    /// <summary>Gets a value indicating whether the column is fixed-length.</summary>
    public bool IsFixedLength { get; init; }

    /// <summary>Gets or initializes the zero-based ordinal position in the table.</summary>
    public int Ordinal { get; init; }

    /// <summary>Gets or initializes the structured size — use <see cref="ColumnSize.ToString"/> for a human-readable description.</summary>
    public ColumnSize Size { get; init; }

    /// <summary>
    /// Gets or initializes the persisted Jet expression string used as the column default at the
    /// database engine level (e.g. <c>"0"</c>, <c>"\"hi\""</c>, <c>"=Now()"</c>). Sourced from
    /// <c>MSysObjects.LvProp</c>; <see langword="null"/> when no default is persisted or the blob
    /// is absent (typical for databases created by this library prior to write-side support).
    /// </summary>
    public string? DefaultValueExpression { get; init; }

    /// <summary>
    /// Gets or initializes the persisted Jet expression evaluated by the database engine on
    /// insert/update (e.g. <c>"&gt;=0 And &lt;=100"</c>). Sourced from <c>MSysObjects.LvProp</c>.
    /// </summary>
    public string? ValidationRuleExpression { get; init; }

    /// <summary>
    /// Gets or initializes the user-facing message shown by Access when
    /// <see cref="ValidationRuleExpression"/> fails. Sourced from <c>MSysObjects.LvProp</c>.
    /// </summary>
    public string? ValidationText { get; init; }

    /// <summary>
    /// Gets or initializes the free-text column description. Sourced from <c>MSysObjects.LvProp</c>.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Gets or initializes the declared precision (1..28) for a
    /// <c>decimal</c> / <c>T_NUMERIC</c> column. Sourced from the TDEF
    /// column descriptor's <c>misc</c> slot at descriptor offset 11. Zero
    /// for non-numeric columns.
    /// </summary>
    public byte NumericPrecision { get; init; }

    /// <summary>
    /// Gets or initializes the declared scale (0..28) for a <c>decimal</c>
    /// / <c>T_NUMERIC</c> column. Sourced from descriptor offset 12.
    /// Defines the canonical sort-key scale for any index over this column.
    /// </summary>
    public byte NumericScale { get; init; }

    /// <summary>
    /// Returns a compact human-readable description of the column in the form
    /// <c>"Name (TypeName, Size)"</c> — useful for diagnostics, log messages,
    /// and debugger output.
    /// </summary>
    /// <returns>A short single-line description of this column.</returns>
    public override string ToString() => $"{Name} ({TypeName}, {Size})";
}
