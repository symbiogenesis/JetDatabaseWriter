namespace JetDatabaseWriter.Enums;

/// <summary>
/// Subtype of an Access 2007+ "complex" column, as classified by the
/// <c>MSysComplexType_*</c> template table referenced by
/// <c>MSysComplexColumns.ComplexTypeObjectID</c>.
/// </summary>
public enum ComplexColumnKind
{
    /// <summary>The complex column was found in the parent TDEF but its template type table could not be classified.</summary>
    Unknown = 0,

    /// <summary>An Attachment column (<c>MSysComplexType_Attachment</c>).</summary>
    Attachment = 1,

    /// <summary>A multi-value column whose flat-table schema matches one of the <c>MSysComplexType_*</c> primitive templates (e.g. <c>Long</c>, <c>Text</c>, <c>GUID</c>).</summary>
    MultiValue = 2,

    /// <summary>A version-history column attached to a memo column flagged "Append Only".</summary>
    VersionHistory = 3,
}
