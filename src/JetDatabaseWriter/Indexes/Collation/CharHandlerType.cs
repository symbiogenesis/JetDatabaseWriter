namespace JetDatabaseWriter.Indexes.Collation;

internal enum CharHandlerType
{
    Simple = 0,
    International = 1,
    Unprintable = 2,
    UnprintableExt = 3,
    InternationalExt = 4,
    Significant = 5,
    Surrogate = 6,
    Ignored = 7,
}
