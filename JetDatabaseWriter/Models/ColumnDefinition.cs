namespace JetDatabaseWriter.Models;

using System;
using JetDatabaseWriter.Core;

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
    /// Gets a value indicating whether this column is a Microsoft Access Hyperlink column.
    /// Persisted in the JET TDEF column-flag bit <c>HYPERLINK_FLAG_MASK = 0x80</c> so that
    /// Access opens the column with the Hyperlink data-format affordance (clickable values,
    /// Insert Hyperlink dialog, etc.). Implies <c>T_MEMO</c>: the underlying CLR type must be
    /// <see cref="string"/> or <see cref="Hyperlink"/> and any <see cref="MaxLength"/> hint
    /// is ignored. <c>CreateTableAsync</c> throws <see cref="ArgumentException"/>
    /// if the bit is requested on a non-text column. Surfaced to readers via
    /// <see cref="ColumnMetadata.IsHyperlink"/>; values are auto-materialized as
    /// <see cref="Hyperlink"/> instances when the bit is observed on read.
    /// See <c>docs/design/hyperlink-format-notes.md</c>.
    /// </summary>
    public bool IsHyperlink { get; init; }

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

    /// <summary>
    /// Gets a value indicating whether this column participates in the table's
    /// primary key. Setting this on one or more columns of a
    /// <c>CreateTableAsync</c> call is shorthand for synthesizing a single
    /// composite <see cref="IndexDefinition"/> named <c>"PrimaryKey"</c> with
    /// <see cref="IndexDefinition.IsPrimaryKey"/> set to <c>true</c>, in
    /// declaration order. PK columns are forced non-nullable on the emitted
    /// TDEF (any <see cref="IsNullable"/> = <c>true</c> is overridden).
    /// Mixing this shortcut with an explicit PK <see cref="IndexDefinition"/>
    /// in the same <c>CreateTableAsync</c> call throws
    /// <see cref="System.ArgumentException"/>.
    /// </summary>
    public bool IsPrimaryKey { get; init; }

    /// <summary>
    /// Gets a value indicating whether this column is an Access 2007+ Attachment
    /// column (JET <c>T_ATTACHMENT = 0x11</c>). Backed on disk by a hidden flat
    /// child table containing one row per attached file. ACE (.accdb) only.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Declaring an attachment column emits the parent TDEF column descriptor
    /// (<c>col_type = 0x11</c>, <c>col_len = 4</c>, bitmask <c>0x07</c>, 4-byte
    /// <c>misc</c> slot for the <c>ComplexID</c>), allocates a fresh
    /// per-database <c>ComplexID</c>, and emits the hidden flat child table
    /// plus the <c>MSysComplexColumns</c> catalog row.
    /// </para>
    /// <para>
    /// Existing attachment columns read from an Access-authored database are
    /// preserved through <c>AddColumnAsync</c> / <c>DropColumnAsync</c> /
    /// <c>RenameColumnAsync</c>.
    /// </para>
    /// </remarks>
    public bool IsAttachment { get; init; }

    /// <summary>
    /// Gets a value indicating whether this column is an Access 2007+ Multi-Value
    /// (complex) column (JET <c>T_COMPLEX = 0x12</c>). Stores zero or more values
    /// of <see cref="MultiValueElementType"/> per parent row in a hidden flat
    /// child table. ACE (.accdb) only.
    /// </summary>
    /// <remarks>
    /// Declaring a multi-value column follows the same emission path as
    /// <see cref="IsAttachment"/>: parent TDEF column descriptor plus a hidden
    /// flat child table and an <c>MSysComplexColumns</c> catalog row. Existing
    /// multi-value columns survive <c>AddColumnAsync</c> /
    /// <c>DropColumnAsync</c> / <c>RenameColumnAsync</c>.
    /// </remarks>
    public bool IsMultiValue { get; init; }

    /// <summary>
    /// Gets the CLR element type stored in a multi-value column (e.g.
    /// <c>typeof(string)</c>, <c>typeof(int)</c>). Required when
    /// <see cref="IsMultiValue"/> is <see langword="true"/>; ignored otherwise.
    /// </summary>
    public Type? MultiValueElementType { get; init; }

    /// <summary>
    /// Gets the per-database <c>ComplexID</c> recovered from the parent TDEF
    /// column descriptor's <c>misc</c> slot. Internal — set only when round-tripping
    /// an existing complex column through the schema-evolution path so the
    /// rewritten TDEF carries the same ID and continues to join to its
    /// <c>MSysComplexColumns</c> row + hidden flat table. New complex columns
    /// declared by the user via <see cref="IsAttachment"/> / <see cref="IsMultiValue"/>
    /// leave this at <c>0</c>; the writer populates it on table creation.
    /// </summary>
    internal int ComplexId { get; init; }

    /// <summary>
    /// Gets the declared precision (1..28, total significant digits) for a
    /// <c>decimal</c> / <c>T_NUMERIC</c> column. Default <c>18</c> matches
    /// the Microsoft Access "Number → Decimal" UI default. Persisted to the
    /// JET TDEF column-descriptor <c>misc</c> slot at descriptor-relative
    /// offset 11 (Jet4 / ACE only — Jet3 has no <c>T_NUMERIC</c>). Ignored
    /// for non-decimal columns.
    /// </summary>
    public byte NumericPrecision { get; init; } = 18;

    /// <summary>
    /// Gets the declared scale (0..28, decimal places) for a <c>decimal</c>
    /// / <c>T_NUMERIC</c> column. Default <c>0</c> matches the Microsoft
    /// Access "Number → Decimal" UI default. Persisted at descriptor-relative
    /// offset 12. Index encoders rescale every cell value to this scale via
    /// <see cref="System.MidpointRounding.ToEven"/> rounding so a single
    /// canonical scale governs the B-tree (mirroring Access, which stores
    /// every <c>T_NUMERIC</c> cell at the declared scale). Must satisfy
    /// <c>NumericScale &lt;= NumericPrecision</c>.
    /// </summary>
    public byte NumericScale { get; init; }
}
