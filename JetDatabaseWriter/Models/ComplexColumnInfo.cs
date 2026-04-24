namespace JetDatabaseWriter;

/// <summary>
/// Metadata for one Access 2007+ complex (attachment / multi-value /
/// version-history) column, joined across the parent table's TDEF column
/// descriptor and the <c>MSysComplexColumns</c> catalog row that references
/// the hidden flat child table.
/// </summary>
/// <remarks>
/// Returned by <see cref="IAccessReader.GetComplexColumnsAsync"/>. The
/// per-row child values are still surfaced through the existing typed
/// row APIs (<c>byte[]</c> for attachments); this record exposes the
/// structural plumbing that joins parent and child.
/// </remarks>
public sealed record ComplexColumnInfo
{
    /// <summary>Gets the name of the column on the parent table.</summary>
    public string ColumnName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the per-database <c>ComplexID</c> stored in the parent TDEF column
    /// descriptor's misc/misc_ext slot. Joins the parent column to its
    /// <c>MSysComplexColumns</c> row.
    /// </summary>
    public int ComplexId { get; init; }

    /// <summary>Gets the classification of the complex column (attachment, multi-value, or version-history).</summary>
    public ComplexColumnKind Kind { get; init; }

    /// <summary>
    /// Gets the name of the hidden flat child table that holds the per-value rows
    /// (e.g. <c>"f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments"</c>). Empty when the
    /// flat table could not be resolved from <c>MSysComplexColumns</c>.
    /// </summary>
    public string FlatTableName { get; init; } = string.Empty;

    /// <summary>
    /// Gets the <c>FlatTableID</c> recorded in <c>MSysComplexColumns</c>.
    /// The lower 24 bits are the flat-table TDEF page number.
    /// </summary>
    public int FlatTableId { get; init; }

    /// <summary>
    /// Gets the <c>ConceptualTableID</c> base value recorded in <c>MSysComplexColumns</c>.
    /// Each parent row's 4-byte payload is one cursor value joining that row to its child rows.
    /// </summary>
    public int ConceptualTableId { get; init; }

    /// <summary>
    /// Gets the <c>ComplexTypeObjectID</c> recorded in <c>MSysComplexColumns</c>.
    /// Points at the <c>MSysComplexType_*</c> template table whose schema dictates
    /// the kind of the flat child table.
    /// </summary>
    public int ComplexTypeObjectId { get; init; }

    /// <summary>
    /// Gets the name of the <c>MSysComplexType_*</c> template table this column was
    /// classified against (e.g. <c>"MSysComplexType_Attachment"</c>,
    /// <c>"MSysComplexType_Long"</c>). Empty when the template could not be resolved.
    /// </summary>
    public string ComplexTypeName { get; init; } = string.Empty;
}
