namespace JetDatabaseWriter.Enums;

internal enum IntermediateOpType
{
    Replace,
    InsertAfter,

    /// <summary>Drop the entry at <c>OriginalIndex</c>. The
    /// other field (<c>NewEntry</c>) is unused.</summary>
    Remove,
}
