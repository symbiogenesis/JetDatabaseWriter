namespace JetDatabaseWriter.Maintenance;

/// <summary>The stored assignment state of a field, without interpreting its payload.</summary>
internal enum CompactRawFieldKind
{
    Absent = 0,
    Null = 1,
    Boolean = 2,
    Stored = 3,
}
