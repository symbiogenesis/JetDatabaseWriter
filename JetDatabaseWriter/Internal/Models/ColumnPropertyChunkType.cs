namespace JetDatabaseWriter.Internal.Models;

/// <summary>
/// Chunk-type codes carried in the 6-byte chunk header of an
/// <c>MSysObjects.LvProp</c> blob (<c>KKD\0</c> / <c>MR2\0</c>).
/// </summary>
/// <remarks>
/// Source: mdbtools <c>src/libmdb/props.c</c> (<c>mdb_kkd_to_props</c>).
/// </remarks>
internal enum ColumnPropertyChunkType : ushort
{
    /// <summary>Property block targeted at an object — column, table, or index.</summary>
    PropertyBlock = 0x0000,

    /// <summary>Property block subtype 1. mdbtools treats identically to <see cref="PropertyBlock"/>.</summary>
    PropertyBlockAlt1 = 0x0001,

    /// <summary>Property block subtype 2. mdbtools treats identically to <see cref="PropertyBlock"/>.</summary>
    PropertyBlockAlt2 = 0x0002,

    /// <summary>Name-pool chunk — exactly one per blob, must precede any property block.</summary>
    NamePool = 0x0080,
}
