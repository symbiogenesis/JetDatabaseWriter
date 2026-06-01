namespace JetDatabaseWriter;

internal enum IndexQueryKind
{
    All = 0,
    Exact = 1,
    KeyPrefix = 2,
    Range = 3,
}
