namespace JetDatabaseWriter;

using System;

/// <summary>
/// Defines a column for use with <see cref="IAccessWriter.CreateTableAsync(string, System.Collections.Generic.IReadOnlyList{ColumnDefinition}, System.Threading.CancellationToken)"/>.
/// </summary>
public sealed record ColumnDefinition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ColumnDefinition"/> class.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="clrType">The CLR type for this column (e.g., typeof(string), typeof(int)).</param>
    /// <param name="maxLength">Maximum length for variable-length types (e.g., string). Ignored for fixed-length types.</param>
    public ColumnDefinition(string name, Type clrType, int maxLength = 0)
    {
        Name = name;
        ClrType = clrType;
        MaxLength = maxLength;
    }

    /// <summary>Gets the column name.</summary>
    public string Name { get; }

    /// <summary>Gets the CLR type that this column stores.</summary>
    public Type ClrType { get; }

    /// <summary>Gets the maximum length for variable-length columns. 0 means default.</summary>
    public int MaxLength { get; }

    /// <summary>
    /// Gets a value indicating whether this column accepts null / <see cref="DBNull.Value"/>.
    /// Default is <c>true</c>. When <c>false</c>, the writer rejects inserts whose value for
    /// this column is null after <see cref="DefaultValue"/> substitution and auto-increment
    /// assignment have run.
    /// </summary>
    /// <remarks>
    /// Persisted in the JET TDEF column-flag bit <c>FLAG_NULL_ALLOWED (0x02)</c>. The
    /// constraint is restored when the database is reopened by any <see cref="AccessWriter"/>
    /// and is surfaced to readers via <see cref="ColumnMetadata.IsNullable"/>.
    /// </remarks>
    public bool IsNullable { get; init; } = true;

    /// <summary>
    /// Gets an optional default value substituted for null / <see cref="DBNull.Value"/> at
    /// insert time. The value must be assignment-compatible with <see cref="ClrType"/>.
    /// Enforced client-side by the <see cref="AccessWriter"/> instance that declared it —
    /// not written into the file.
    /// </summary>
    public object? DefaultValue { get; init; }

    /// <summary>
    /// Gets a value indicating whether this column auto-assigns a monotonically increasing
    /// integer when the supplied value is null / <see cref="DBNull.Value"/>. The next value
    /// is seeded from <c>max(existing) + 1</c> on first use (or <c>1</c> for an empty table)
    /// and incremented per insert. Only valid for <see cref="byte"/>, <see cref="short"/>,
    /// <see cref="int"/>, and <see cref="long"/> columns.
    /// </summary>
    /// <remarks>
    /// Persisted in the JET TDEF column-flag bit <c>FLAG_AUTO_LONG (0x04)</c>. The
    /// auto-increment behaviour is restored when the database is reopened.
    /// </remarks>
    public bool IsAutoIncrement { get; init; }

    /// <summary>
    /// Gets an optional client-side validation predicate invoked for every supplied
    /// non-null value before the row is written. Returning <c>false</c> raises an
    /// <see cref="ArgumentException"/>. Not persisted — a CLR delegate cannot be
    /// serialized into the JET file.
    /// </summary>
    public Func<object?, bool>? ValidationRule { get; init; }

    /// <summary>
    /// Gets the persisted Jet expression string used as the column default at the database
    /// engine level (e.g. <c>"0"</c>, <c>"\"hi\""</c>, <c>"=Now()"</c>). When set, this value
    /// is written into <c>MSysObjects.LvProp</c> so it survives across writer instances and
    /// is honoured by Microsoft Access. Takes precedence over <see cref="DefaultValue"/> for
    /// persistence; the CLR <see cref="DefaultValue"/> continues to drive the in-process
    /// <see cref="DBNull"/>-substitution path.
    /// </summary>
    public string? DefaultValueExpression { get; init; }

    /// <summary>
    /// Gets the persisted Jet expression evaluated by the database engine on insert / update
    /// (e.g. <c>"&gt;=0 And &lt;=100"</c>). Persisted in <c>MSysObjects.LvProp</c>. Independent
    /// of the in-process <see cref="ValidationRule"/> delegate.
    /// </summary>
    public string? ValidationRuleExpression { get; init; }

    /// <summary>
    /// Gets the user-facing message Microsoft Access displays when
    /// <see cref="ValidationRuleExpression"/> rejects a value. Persisted in
    /// <c>MSysObjects.LvProp</c>.
    /// </summary>
    public string? ValidationText { get; init; }

    /// <summary>
    /// Gets the free-text column description shown in Access Design View. Persisted in
    /// <c>MSysObjects.LvProp</c>.
    /// </summary>
    public string? Description { get; init; }
}
