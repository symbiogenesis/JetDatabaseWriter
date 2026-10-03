namespace JetDatabaseWriter.Models;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;

/// <summary>
/// Defines a column for use with <see cref="IAccessSchema.CreateTableAsync(string, IReadOnlyList{ColumnDefinition}, System.Threading.CancellationToken)"/>.
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
        this.Name = name;
        this.ClrType = clrType;
        this.MaxLength = maxLength;
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
    /// assignment have run, and updates that set the column to null, with
    /// <see cref="InvalidOperationException"/>. An update is checked only for the columns
    /// it assigns.
    /// </summary>
    /// <remarks>
    /// Persisted as the boolean <c>Required = True</c> property in <c>MSysObjects.LvProp</c>
    /// (the wire format DAO/Access use). The TDEF column-flag byte has no DAO-recognised
    /// nullability bit, so LvProp is the only round-trip-safe persistence channel. The
    /// constraint is restored when the database is reopened by any <see cref="AccessWriter"/>
    /// and is surfaced to readers via <see cref="ColumnMetadata.IsNullable"/>.
    /// </remarks>
    public bool IsNullable { get; init; } = true;

    /// <summary>
    /// Gets an optional default value substituted for null / <see cref="DBNull.Value"/> at
    /// insert time. The value must be assignment-compatible with <see cref="ClrType"/>.
    /// Defaults apply only when a row is created: an update that sets the column to null
    /// stores null.
    /// </summary>
    /// <remarks>
    /// When <see cref="DefaultValueExpression"/> is not set, the value is also written into
    /// <c>MSysObjects.LvProp</c> as a literal <c>DefaultValue</c> expression (for example
    /// <c>7</c>, <c>"text"</c>, <c>True</c> or <c>#2024-02-29 08:30:00#</c>), so a later
    /// <see cref="AccessWriter"/> and Microsoft Access apply the same default. Text, Boolean,
    /// integer, floating-point, <see cref="decimal"/>, <see cref="DateTime"/> and
    /// <see cref="Guid"/> values can be written this way; any other type, such as
    /// <c>byte[]</c>, makes table creation throw <see cref="NotSupportedException"/>. The
    /// literal is persisted only when the catalog has an <c>LvProp</c> column
    /// (<see cref="AccessWriterOptions.WriteFullCatalogSchema"/>, the default).
    /// </remarks>
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
    /// auto-increment behaviour is restored when the database is reopened. An update
    /// may assign an explicit value, which raises the stored high-water as an explicit
    /// insert value does, but setting the column to null throws
    /// <see cref="InvalidOperationException"/>.
    /// </remarks>
    public bool IsAutoIncrement { get; init; }

    /// <summary>
    /// Gets a value indicating whether this column is a Microsoft Access Hyperlink column.
    /// Persisted in the JET TDEF column-flag bit <c>HYPERLINK_FLAG_MASK = 0x80</c> so that
    /// Access opens the column with the Hyperlink data-format affordance (clickable values,
    /// Insert Hyperlink dialog, etc.). Implies <c>Memo</c>: the underlying CLR type must be
    /// <see cref="string"/> or <see cref="Hyperlink"/> and any <see cref="MaxLength"/> hint
    /// is ignored. <c>CreateTableAsync</c> throws <see cref="ArgumentException"/>
    /// if the bit is requested on a non-text column. Surfaced to readers via
    /// <see cref="ColumnMetadata.IsHyperlink"/>; values are auto-materialized as
    /// <see cref="Hyperlink"/> instances when the bit is observed on read.
    /// See <see href="docs/design/hyperlink-format-notes.md" />.
    /// </summary>
    public bool IsHyperlink { get; init; }

    /// <summary>
    /// Gets a value indicating whether this Jet4/ACE <c>Text</c> or <c>Memo</c>
    /// column should be authored with the <c>COMPRESSED_UNICODE_EXT_FLAG_MASK</c>
    /// (bit <c>0x01</c> of the descriptor ExtraFlags byte at offset 16) set.
    /// Default is <c>true</c> — matching what Microsoft Access emits when you
    /// create a Short Text or Long Text column through the table designer
    /// (the "Unicode Compression" property defaults to "Yes").
    /// </summary>
    /// <remarks>
    /// DAO's programmatic <c>TableDef.CreateField(name, dbText)</c> path omits
    /// the bit (verified via the <c>rt-dao-baseline</c> FormatProbe mode,
    /// legacy <c>DIAG_RT_DAO_BASELINE</c>); set this
    /// to <see langword="false"/> when you need byte-for-byte parity with
    /// DAO-authored TDEFs. The reader/encoder treat the <c>FF FE</c>
    /// compressed marker as the canonical signal regardless of this flag, so
    /// toggling it is a schema-roundtrip / Access-UI-compatibility concern
    /// only. Ignored for non-text columns and for Jet3 (which has no
    /// ExtraFlags byte).
    /// </remarks>
    public bool IsCompressedUnicode { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether a <see cref="DateTime"/> column should
    /// be authored as Access 2019+ <c>Date/Time Extended</c> instead of the
    /// classic 8-byte <c>Date/Time</c> type. The stored value has 100 ns tick
    /// precision and is read back as a <see cref="DateTime"/> whose
    /// <see cref="DateTime.Kind"/> is <see cref="DateTimeKind.Unspecified"/>.
    /// </summary>
    /// <remarks>
    /// The default remains classic <c>Date/Time</c> for compatibility with
    /// existing databases and older Access formats. This option is supported
    /// only for ACCDB databases.
    /// </remarks>
    public bool IsDateTimeExtended { get; init; }

    /// <summary>
    /// Gets an optional client-side validation predicate invoked for every non-null value
    /// an insert supplies or an update assigns, before the row is written. Returning
    /// <c>false</c> raises an <see cref="ArgumentException"/>.
    /// </summary>
    /// <remarks>
    /// A CLR delegate cannot be serialized into the JET file, so this rule binds only the
    /// <see cref="AccessWriter"/> instance that created the table, which keeps it across its
    /// own column and table renames, additions and drops. A writer that opens the database
    /// later does not enforce it. For a rule that every writer and Microsoft Access enforce,
    /// use <see cref="ValidationRuleExpression"/>.
    /// </remarks>
    public Func<object?, bool>? ValidationRule { get; init; }

    /// <summary>
    /// Gets the persisted Jet expression used as the column default (e.g. <c>"0"</c>,
    /// <c>"\"hi\""</c>, <c>"=Now()"</c>, <c>"Date()"</c>). It is written into
    /// <c>MSysObjects.LvProp</c>, so it survives across writer instances and is honoured by
    /// Microsoft Access.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every <see cref="AccessWriter"/> evaluates the expression when an inserted row leaves
    /// the column null / <see cref="DBNull.Value"/> (the same rule as
    /// <see cref="DefaultValue"/>), converts the result to the column's type, and stores it.
    /// An update that sets the column to null stores null. A persisted <c>DefaultValue</c>
    /// written by Microsoft Access is applied the same way.
    /// </para>
    /// <para>
    /// When both are set, the declaring writer uses the CLR <see cref="DefaultValue"/> and
    /// persists this expression, so later writers use the expression.
    /// </para>
    /// <para>
    /// The expression is evaluated with this library's calculated-column expression engine.
    /// When it uses a function or syntax the engine does not support (for example
    /// <c>GenGUID()</c> or <c>CurrentUser()</c>), evaluates to Null, or yields a value that
    /// cannot be converted to the column's type, no default is applied and the column stays
    /// null (so a NOT NULL column then rejects the row). Microsoft Access still applies it.
    /// </para>
    /// </remarks>
    public string? DefaultValueExpression { get; init; }

    /// <summary>
    /// Gets the persisted Access validation rule for the column (e.g.
    /// <c>"&gt;=0 And &lt;=100"</c>). Persisted in <c>MSysObjects.LvProp</c> and enforced
    /// by every <see cref="AccessWriter"/> and by Microsoft Access. Independent of the
    /// in-process <see cref="ValidationRule"/> delegate; when both are set, both apply.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The writer checks the rule against every value an insert stores (after default and
    /// AutoNumber substitution) and every value an update assigns, before any row is
    /// written, and throws <see cref="ArgumentException"/> when it rejects one; the message
    /// includes <see cref="ValidationText"/>. A validation rule written by Microsoft Access is
    /// enforced the same way.
    /// </para>
    /// <para>
    /// As in Access, the column is the implicit left operand of a term that starts with an
    /// operator (<c>"&lt;&gt;0"</c>, <c>"Is Not Null"</c>, <c>"Between 1 And 10"</c>,
    /// <c>"In (1,2,3)"</c>, <c>"Like \"A*\""</c>, <c>"&lt;=Date()"</c>), a bare value term is
    /// an equality test (<c>"0 Or &gt;100"</c>), and a term may also name the column
    /// (<c>"Len([Code]) = 3"</c>). Comparisons with Null are Null and only a False result
    /// rejects, so a rule that does not test for Null accepts Null.
    /// </para>
    /// <para>
    /// The rule is evaluated with this library's calculated-column expression engine. A rule
    /// that uses syntax or a function the engine does not support (for example
    /// <c>DLookUp</c>), or whose evaluation fails, is not enforced by the writer rather than
    /// blocking every write to the table. Microsoft Access still enforces it. Text
    /// comparisons are ordinal and case-insensitive, which can differ from the database's
    /// sort order for non-ASCII text.
    /// </para>
    /// </remarks>
    public string? ValidationRuleExpression { get; init; }

    /// <summary>
    /// Gets the user-facing message Microsoft Access displays when
    /// <see cref="ValidationRuleExpression"/> rejects a value. Persisted in
    /// <c>MSysObjects.LvProp</c>. The writer appends it to the message of the
    /// <see cref="ArgumentException"/> it throws for the same rejection.
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
    /// <see cref="ArgumentException"/>.
    /// </summary>
    public bool IsPrimaryKey { get; init; }

    /// <summary>
    /// Gets a value indicating whether this column is an Access 2007+ Attachment
    /// column. Backed on disk by a hidden flat child table containing one row
    /// per attached file. ACE (.accdb) only.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Declaring an attachment column emits an Access-style generic complex
    /// parent TDEF column descriptor (<c>col_type = 0x12</c>, <c>col_len = 4</c>,
    /// bitmask <c>0x07</c>, 4-byte <c>misc</c> slot for the <c>ComplexID</c>), allocates a fresh
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
    /// (complex) column (JET <c>Complex = 0x12</c>). Stores zero or more values
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

    internal bool ForceVariableLengthStorage { get; init; }

    internal byte? DescriptorFlagsOverride { get; init; }

    internal ColumnType? ColumnTypeOverride { get; init; }

    internal byte? DescriptorExtraFlagsOverride { get; init; }

    internal int? DescriptorMiscOverride { get; init; }

    /// <summary>
    /// Gets the declared precision (1..28, total significant digits) for a
    /// <c>decimal</c> / <c>Numeric</c> column. Default <c>18</c> matches
    /// the Microsoft Access "Number → Decimal" UI default. Persisted to the
    /// JET TDEF column-descriptor <c>misc</c> slot at descriptor-relative
    /// offset 11 (Jet4 / ACE only — Jet3 has no <c>Numeric</c>). Ignored
    /// for non-decimal columns.
    /// </summary>
    public byte NumericPrecision { get; init; } = 18;

    /// <summary>
    /// Gets the declared scale (0..28, decimal places) for a <c>decimal</c>
    /// / <c>Numeric</c> column. Default <c>0</c> matches the Microsoft
    /// Access "Number → Decimal" UI default. Persisted at descriptor-relative
    /// offset 12. Index encoders rescale every cell value to this scale via
    /// <see cref="MidpointRounding.ToEven"/> rounding so a single
    /// canonical scale governs the B-tree (mirroring Access, which stores
    /// every <c>Numeric</c> cell at the declared scale). Must satisfy
    /// <c>NumericScale &lt;= NumericPrecision</c>.
    /// </summary>
    public byte NumericScale { get; init; }

    /// <summary>
    /// Gets a value indicating whether this column is an Access 2010+
    /// calculated (expression) column. Calculated columns store the result of
    /// a Jet/VBA expression (<see cref="CalculationExpression"/>). ACE
    /// (.accdb) only.
    /// </summary>
    /// <remarks>
    /// <para>
    /// On disk, calculated columns are flagged via the
    /// <see cref="Constants.CalculatedColumn.ExtFlagMask"/> bits in the column
    /// descriptor's extra-flags byte; the expression and result type are
    /// persisted in <c>MSysObjects.LvProp</c> as the <c>Expression</c> and
    /// <c>ResultType</c> properties; every stored cell carries a 23-byte
    /// calculated-value wrapper.
    /// </para>
    /// <para>
    /// The writer can create ACCDB calculated columns, stores caller-supplied
    /// row values as persisted cached results, fills missing values on insert
    /// when <see cref="CalculationExpression"/> is in the supported row-local
    /// subset, and recomputes values on update. Microsoft Access will recompute
    /// the value when it opens the file.
    /// See <see href="docs/design/calculated-columns-format-notes.md" />.
    /// </para>
    /// </remarks>
    public bool IsCalculated { get; init; }

    /// <summary>
    /// Gets the Jet/VBA expression Microsoft Access evaluates to compute this
    /// column's value (e.g. <c>"[FirstName] &amp; \" \" &amp; [LastName]"</c>).
    /// Required when <see cref="IsCalculated"/> is <see langword="true"/>;
    /// ignored otherwise. Persisted in <c>MSysObjects.LvProp</c> as the
    /// <see cref="Constants.ColumnPropertyNames.Expression"/> property.
    /// </summary>
    public string? CalculationExpression { get; init; }

    /// <summary>
    /// Gets the JET column-type code (see <see cref="ColumnType"/>)
    /// of the value <see cref="CalculationExpression"/> produces. When left at
    /// zero for a calculated column, the writer derives the result type from
    /// <see cref="ClrType"/>. Persisted in <c>MSysObjects.LvProp</c> as the
    /// <see cref="Constants.ColumnPropertyNames.ResultType"/> property and also
    /// written into the column descriptor's <c>col_type</c> byte.
    /// </summary>
    public byte CalculatedResultType { get; init; }
}
