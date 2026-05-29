namespace JetDatabaseWriter.Enums;

internal enum IntermediateOpType
{
    Replace = 0,
    InsertAfter = 1,

    /// <summary>Drop the entry at <c>OriginalIndex</c>. The
    /// other field (<c>NewEntry</c>) is unused.</summary>
    Remove = 2,
}
