namespace JetDatabaseWriter.Schema.Models;

/// <summary>A source chunk and its original name-pool content or opaque payload.</summary>
/// <param name="ChunkType">The stored chunk type.</param>
/// <param name="Payload">The original name-pool payload, empty for other chunk types.</param>
/// <param name="Names">The decoded names when this is a name-pool chunk.</param>
/// <param name="UnknownIndex">The opaque chunk's index, or -1 for a recognized chunk.</param>
internal sealed record ColumnPropertySourceChunk(ushort ChunkType, byte[] Payload, string[]? Names, int UnknownIndex);
