namespace JetDatabaseWriter.Internal;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
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
    /// Returns the CLR type used when projecting a TDEF column descriptor back
    /// to a public <c>ColumnDefinition</c>. Complex-column codes (<c>T_COMPLEX</c>
    /// / <c>T_ATTACHMENT</c>) map to <see cref="byte"/>[] — the surface CLR type the
    /// reader resolves them to after joining the hidden flat child table — but
    /// callers that need the additional metadata (ComplexId, IsAttachment,
    /// IsMultiValue) must still special-case those codes before reaching this
    /// projection. Returns <see langword="null"/> for unknown codes.
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
        T_ATTACHMENT => typeof(byte[]),
        T_COMPLEX => typeof(byte[]),
        _ => null,
    };

    /// <summary>
    /// Returns the human-friendly Access display name for a JET column-type code
    /// (e.g. <c>"Long Integer"</c> for <c>T_LONG</c>). Unknown codes surface as
    /// the hex representation <c>"0xNN"</c>. Mirrors Access's UI labels and the
    /// names exposed by the legacy DAO/ADO type-name properties.
    /// </summary>
    public static string GetTypeDisplayName(byte type) => type switch
    {
        T_BOOL => "Yes/No",
        T_BYTE => "Byte",
        T_INT => "Integer",
        T_LONG => "Long Integer",
        T_MONEY => "Currency",
        T_FLOAT => "Single",
        T_DOUBLE => "Double",
        T_DATETIME => "Date/Time",
        T_BINARY => "Binary",
        T_TEXT => "Text",
        T_OLE => "OLE Object",
        T_MEMO => "Memo",
        T_GUID => "GUID",
        T_NUMERIC => "Decimal",
        T_ATTACHMENT => "Attachment",
        T_COMPLEX => "Complex",
        T_DATETIMEEXT => "Date/Time Extended",
        _ => $"0x{type:X2}",
    };

    /// <summary>
    /// Returns the user-facing <see cref="ColumnSize"/> for a column.
    /// <paramref name="declaredSize"/> is the on-disk descriptor size (the
    /// per-column <c>size</c> field) used for variable-width types like
    /// <c>T_TEXT</c> (Jet4 stores chars * 2 there) and unknown fixed types.
    /// </summary>
    public static ColumnSize GetColumnSize(byte type, int declaredSize) => type switch
    {
        T_BOOL => ColumnSize.FromBits(1),
        T_BYTE => ColumnSize.FromBytes(1),
        T_INT => ColumnSize.FromBytes(2),
        T_LONG or T_FLOAT => ColumnSize.FromBytes(4),
        T_MONEY or T_DOUBLE or T_DATETIME => ColumnSize.FromBytes(8),
        T_GUID => ColumnSize.FromBytes(16),
        T_NUMERIC => ColumnSize.FromBytes(17),
        T_TEXT => ColumnSize.FromChars(declaredSize > 0 ? declaredSize / 2 : 255),
        T_MEMO or T_OLE or T_ATTACHMENT or T_COMPLEX => ColumnSize.Lval,
        _ => declaredSize > 0 ? ColumnSize.FromBytes(declaredSize) : ColumnSize.Variable,
    };
}
