namespace JetDatabaseWriter.Tests.Infrastructure;

/// <summary>Where a synthetic overflow row's header and data ended up.</summary>
/// <param name="HeaderPage">The page holding the header slot (the row's identity).</param>
/// <param name="HeaderRow">The header slot's row index.</param>
/// <param name="DataPage">The page holding the moved row data, or 0 when the layout is corrupt.</param>
/// <param name="DataRow">The moved row data's row index.</param>
/// <param name="DataStart">The moved row data's offset on <paramref name="DataPage"/>.</param>
/// <param name="DataSize">The moved row data's length.</param>
internal readonly record struct SyntheticOverflowRow(long HeaderPage, int HeaderRow, long DataPage, int DataRow, int DataStart, int DataSize);
