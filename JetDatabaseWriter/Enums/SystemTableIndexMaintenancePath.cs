namespace JetDatabaseWriter.Enums;

/// <summary>
/// Last maintenance path used by <see cref="AccessWriter.InsertSystemRowAndMaintainAsync"/>.
/// </summary>
internal enum SystemTableIndexMaintenancePath
{
    None = 0,
    SkippedNoMaintainableIndexes = 1,
    Incremental = 2,
}
