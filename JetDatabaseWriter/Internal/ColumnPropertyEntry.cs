namespace JetDatabaseWriter;

/// <summary>
/// One property entry within a <see cref="ColumnPropertyTarget"/>.
/// </summary>
/// <param name="Name">Property name from the blob's name pool.</param>
/// <param name="DataType">Jet column-type code (see <see cref="ColumnPropertyBlock"/> data-type constants).</param>
/// <param name="DdlFlag">Header flag byte at entry offset 2. Hypothesis: 0x00 normal, 0x01 = "DDL-set".</param>
/// <param name="Value">Raw value bytes — interpretation depends on <paramref name="DataType"/>.</param>
internal sealed record ColumnPropertyEntry(
    string Name,
    byte DataType,
    byte DdlFlag,
    byte[] Value);
