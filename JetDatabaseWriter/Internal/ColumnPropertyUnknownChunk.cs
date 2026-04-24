namespace JetDatabaseWriter;

/// <summary>
/// A chunk whose type was not recognised by the parser. Preserved verbatim so that
/// downstream writers can emit it back unchanged (forward-compat guarantee).
/// </summary>
/// <param name="ChunkType">The unknown chunk-type code.</param>
/// <param name="Payload">The chunk payload (excluding the 6-byte chunk header).</param>
internal sealed record ColumnPropertyUnknownChunk(ushort ChunkType, byte[] Payload);
