namespace JetDatabaseWriter;

/// <summary>
/// Classification of a JET logical index, derived from the
/// <c>index_type</c> byte in the TDEF logical-index entry.
/// </summary>
public enum IndexKind
{
    /// <summary>A regular (non-PK, non-FK) index.</summary>
    Normal = 0,

    /// <summary>The primary key of the table (<c>index_type</c> = <c>0x01</c>).</summary>
    PrimaryKey = 1,

    /// <summary>A foreign-key index (<c>index_type</c> = <c>0x02</c>).</summary>
    ForeignKey = 2,
}
