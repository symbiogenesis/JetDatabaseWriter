namespace JetDatabaseWriter.Internal.Models;

/// <summary>
/// Represents a single index entry: key bytes, data page, and data row.
/// Used for both encoding and decoding index leaf entries.
/// </summary>
internal readonly record struct IndexEntry(byte[] Key, long DataPage, byte DataRow);
