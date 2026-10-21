namespace JetDatabaseWriter.Schema.Models;

/// <summary>
/// Chunk-type codes carried in the 6-byte chunk header of an
/// <c>MSysObjects.LvProp</c> blob (<c>KKD\0</c> / <c>MR2\0</c>).
/// </summary>
/// <remarks>
/// Source: mdbtools <c>src/libmdb/props.c</c> (<c>mdb_kkd_to_props</c>).
/// </remarks>
internal enum ColumnPropertyChunkType : ushort
{
    /// <summary>Object-level property block; an empty target name identifies the table.</summary>
    PropertyBlock = 0x0000,

    /// <summary>Column property block.</summary>
    PropertyBlockAlt1 = 0x0001,

    /// <summary>Index property block, whose name can also identify a column.</summary>
    PropertyBlockAlt2 = 0x0002,

    /// <summary>Name-pool chunk — exactly one per blob, must precede any property block.</summary>
    NamePool = 0x0080,
}
