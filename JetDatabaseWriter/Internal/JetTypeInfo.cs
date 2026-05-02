namespace JetDatabaseWriter.Internal;

using System;
using static JetDatabaseWriter.Constants.ColumnTypes;

/// <summary>
/// Per-JET column-type metadata table. Centralises facts that previously
/// lived in scattered <c>switch (col.Type)</c> blocks across the reader,
/// writer, and column-info model — fixed on-disk byte size, default CLR
/// projection, and "always variable-length" classification — so adding a
/// new type code requires editing exactly one file.
/// </summary>
internal static class JetTypeInfo
{
    /// <summary>
    /// Returns the on-disk fixed byte size for a fixed-length JET column type
    /// (<c>BYTE/INT/LONG/MONEY/FLOAT/DOUBLE/DATETIME/GUID/NUMERIC</c>), or
    /// <c>0</c> for variable-length types and unknown codes. Mirrors the
    /// per-type sizes documented in mdbtools <c>HACKING.md</c>.
    /// </summary>
    /// <param name="type">JET column-type code (see <see cref="JetDatabaseWriter.Constants.ColumnTypes"/>).</param>
    public static int GetFixedSize(byte type) => type switch
    {
        T_BYTE => 1,
        T_INT => 2,
        T_LONG => 4,
        T_MONEY => 8,
        T_FLOAT => 4,
        T_DOUBLE => 8,
        T_DATETIME => 8,
        T_GUID => 16,
        T_NUMERIC => 17,

        // Complex/attachment columns store a 4-byte ComplexId in the row's
        // fixed area (the actual payload lives in the hidden child table
        // joined via the ComplexId). Access writes col_len = 4 for both.
        T_COMPLEX => 4,
        T_ATTACHMENT => 4,

        // Access 365 "Date/Time Extended" — 42-byte fixed slot. Not yet
        // exercised by the writer, but reported here so the reader's
        // ResolveColumnSlice path can size the slot correctly.
        T_DATETIMEEXT => 42,

        _ => 0,
    };

    /// <summary>
    /// Returns <see langword="true"/> for the four JET types
    /// (<c>TEXT/BINARY/MEMO/OLE</c>) that are <i>always</i> stored in the
    /// row's variable-length area. Other types may still live in the variable
    /// area when the per-column <c>FLAG_FIXED</c> bit is cleared in the TDEF
    /// descriptor — see <see cref="Models.ColumnInfo.IsFixed"/>.
    /// </summary>
    public static bool IsAlwaysVariableLength(byte type)
        => type is T_TEXT or T_BINARY or T_MEMO or T_OLE;

    /// <summary>
    /// Returns the CLR type used when projecting a TDEF column descriptor
    /// back to a public <c>ColumnDefinition</c>. Returns <see langword="null"/>
    /// for the magic complex-column codes (<c>T_COMPLEX</c> /
    /// <c>T_ATTACHMENT</c>) which require additional fields beyond a CLR
    /// type — callers must handle them separately — and for unknown codes.
    /// </summary>
    public static Type? GetClrType(byte type) => type switch
    {
        T_BOOL => typeof(bool),
        T_BYTE => typeof(byte),
        T_INT => typeof(short),
        T_LONG => typeof(int),
        T_MONEY => typeof(decimal),
        T_FLOAT => typeof(float),
        T_DOUBLE => typeof(double),
        T_DATETIME => typeof(DateTime),
        T_NUMERIC => typeof(decimal),
        T_GUID => typeof(Guid),
        T_TEXT => typeof(string),
        T_MEMO => typeof(string),
        T_BINARY => typeof(byte[]),
        T_OLE => typeof(byte[]),
        _ => null,
    };
}
