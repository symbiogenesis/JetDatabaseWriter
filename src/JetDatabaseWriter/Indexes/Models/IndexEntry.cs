namespace JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Represents a single index entry: key bytes, data page, and data row.
/// Used for both encoding and decoding index leaf entries.
/// </summary>
/// <param name="Key">The encoded index key.</param>
/// <param name="DataPage">The data page.</param>
/// <param name="DataRow">The data row.</param>
internal readonly record struct IndexEntry(byte[] Key, long DataPage, byte DataRow);
