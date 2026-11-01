namespace JetDatabaseWriter.ComplexColumns.Models;

/// <summary>One scalar-admitted complex item's identity, parent reference and physical row location.</summary>
/// <param name="Id">The stored item AutoNumber.</param>
/// <param name="ParentReference">The stored parent reference.</param>
/// <param name="PageNumber">The original live row's data page.</param>
/// <param name="RowIndex">The original live row's slot.</param>
internal readonly record struct ComplexItemLocation(int Id, int ParentReference, long PageNumber, int RowIndex);
