namespace JetDatabaseWriter.Internal;

using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Internal.Models;
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
    /// Returns <see langword="true"/> when the column is a MEMO whose TDEF flag
    /// byte has Jackcess <c>HYPERLINK_FLAG_MASK = 0x80</c> set — Microsoft Access
    /// surfaces such columns through the Hyperlink data-format affordance.
    /// See <c>docs/design/hyperlink-format-notes.md</c>.
    /// </summary>
    public static bool IsHyperlinkColumn(ColumnInfo col)
        => col.Type == T_MEMO && (col.Flags & 0x80) != 0;

    /// <summary>
    /// Returns the CLR projection type for a column, accounting for the
    /// MEMO/Hyperlink override (<see cref="IsHyperlinkColumn"/>). Falls back
    /// to <see cref="string"/> for unknown type codes — matching the
    /// long-standing reader contract.
    /// </summary>
    public static Type ResolveClrType(ColumnInfo col)
        => IsHyperlinkColumn(col) ? typeof(Hyperlink) : GetClrType(col.Type) ?? typeof(string);

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

    // ── Fixed-column decoding ────────────────────────────────────────
    //
    // The two ReadFixed* helpers below decode a fixed-width JET column
    // value out of a raw row buffer. They live next to the per-type
    // metadata above (GetFixedSize / GetClrType / GetTypeDisplayName) so
    // the per-type switch tables stay co-located. ReadFixedString is the
    // legacy lossy/diagnostic path; ReadFixedTyped is the typed-reader
    // hot path. See <c>docs/design/typed-row-read-perf-plan.md</c>.

    /// <summary>
    /// Formats a fixed-width JET column value as a culture-invariant string.
    /// When <paramref name="strictNumeric"/> is <see langword="true"/>, T_NUMERIC values
    /// that overflow .NET's <see cref="decimal"/> range or carry an out-of-range scale
    /// surface as <see cref="JetLimitationException"/> instead of being silently elided
    /// to the empty string — the contract the typed reader path relies on.
    /// </summary>
    internal static string ReadFixedString(ReadOnlySpan<byte> row, int start, byte type, int size, bool strictNumeric = false)
    {
        try
        {
            switch (type)
            {
                case T_BYTE:
                    return row[start].ToString(CultureInfo.InvariantCulture);
                case T_INT:
                    return ((short)BinaryPrimitives.ReadUInt16LittleEndian(row.Slice(start, 2))).ToString(CultureInfo.InvariantCulture);
                case T_LONG:
                    return BinaryPrimitives.ReadInt32LittleEndian(row.Slice(start, 4)).ToString(CultureInfo.InvariantCulture);
                case T_FLOAT:
                    return ReadSingleLittleEndian(row.Slice(start, 4)).ToString("G", CultureInfo.InvariantCulture);
                case T_DOUBLE:
                    return ReadDoubleLittleEndian(row.Slice(start, 8)).ToString("G", CultureInfo.InvariantCulture);
                case T_DATETIME:
                    return DateTime.FromOADate(ReadDoubleLittleEndian(row.Slice(start, 8))).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                case T_MONEY:
                    return decimal.FromOACurrency(BinaryPrimitives.ReadInt64LittleEndian(row.Slice(start, 8))).ToString("F4", CultureInfo.InvariantCulture);
                case T_GUID:
                    return new Guid(row.Slice(start, 16)).ToString("B");
                case T_NUMERIC:
                    return ReadNumericString(row, start, strictNumeric);
                case T_COMPLEX:
                case T_ATTACHMENT:
                    return size >= 4 ? $"__CX:{BinaryPrimitives.ReadInt32LittleEndian(row.Slice(start, 4))}__" : string.Empty;
                default:
                    return ToHexStringNoSeparator(row.Slice(start, Math.Min(size, 8)));
            }
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
        catch (IndexOutOfRangeException)
        {
            return string.Empty;
        }
        catch (OverflowException)
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// Decodes a fixed-width JET column value directly to its boxed CLR primitive,
    /// bypassing the lossy <see cref="ReadFixedString"/> +
    /// <c>TypedValueParser.ParseValue</c> round-trip used by the diagnostics path.
    /// The typed-reader hot path uses this to avoid per-column culture-invariant
    /// string formatting and re-parsing.
    /// <para>
    /// Type mapping mirrors <see cref="GetClrType(byte)"/>:
    /// <c>T_BYTE → byte</c>, <c>T_INT → short</c>, <c>T_LONG → int</c>,
    /// <c>T_FLOAT → float</c>, <c>T_DOUBLE → double</c>,
    /// <c>T_DATETIME → DateTime</c> (un-truncated; <see cref="ReadFixedString"/>
    /// formats with <c>"yyyy-MM-dd HH:mm:ss"</c> and loses sub-second precision —
    /// the typed path keeps full precision),
    /// <c>T_MONEY → decimal</c>, <c>T_GUID → Guid</c>,
    /// <c>T_NUMERIC → decimal</c>,
    /// <c>T_COMPLEX</c>/<c>T_ATTACHMENT → <see cref="ComplexIdRef"/></c> typed
    /// sentinel carrying the row's complex_id directly (the legacy
    /// <c>"__CX:N__"</c> string round-trip used by <see cref="ReadFixedString"/>
    /// is avoided on the typed hot path), and unknown types fall through to
    /// the same hex-string representation <see cref="ReadFixedString"/> emits.
    /// </para>
    /// <para>
    /// Returns <see cref="DBNull.Value"/> when the underlying byte access throws
    /// (<see cref="ArgumentException"/>, <see cref="IndexOutOfRangeException"/>,
    /// <see cref="OverflowException"/>) — matching the empty-string-then-DBNull
    /// behaviour of the round-trip path. When <paramref name="strictNumeric"/>
    /// is <see langword="true"/>, T_NUMERIC values that overflow or carry an
    /// out-of-range scale surface as <see cref="JetLimitationException"/>; with
    /// <see langword="false"/> they collapse to <see cref="DBNull.Value"/>.
    /// </para>
    /// </summary>
    internal static object ReadFixedTyped(ReadOnlySpan<byte> row, int start, byte type, int size, bool strictNumeric = false)
    {
        try
        {
            switch (type)
            {
                case T_BYTE:
                    return row[start];
                case T_INT:
                    // BinaryPrimitives.ReadInt16LittleEndian sign-extends correctly
                    // under <CheckForOverflowUnderflow>true</CheckForOverflowUnderflow>;
                    // the legacy "(short)Ru16(...)" cast throws OverflowException for
                    // values with the high bit set and ReadFixedString silently maps
                    // those to string.Empty → DBNull. The typed path keeps the value.
                    return BinaryPrimitives.ReadInt16LittleEndian(row.Slice(start, 2));
                case T_LONG:
                    return BinaryPrimitives.ReadInt32LittleEndian(row.Slice(start, 4));
                case T_FLOAT:
                    return ReadSingleLittleEndian(row.Slice(start, 4));
                case T_DOUBLE:
                    return ReadDoubleLittleEndian(row.Slice(start, 8));
                case T_DATETIME:
                    return DateTime.FromOADate(ReadDoubleLittleEndian(row.Slice(start, 8)));
                case T_MONEY:
                    return decimal.FromOACurrency(BinaryPrimitives.ReadInt64LittleEndian(row.Slice(start, 8)));
                case T_GUID:
                    return new Guid(row.Slice(start, 16));
                case T_NUMERIC:
                    return ReadNumericTyped(row, start, strictNumeric);
                case T_COMPLEX:
                case T_ATTACHMENT:
                    return size >= 4
                        ? new ComplexIdRef(BinaryPrimitives.ReadInt32LittleEndian(row.Slice(start, 4)))
                        : DBNull.Value;
                default:
                    return ToHexStringNoSeparator(row.Slice(start, Math.Min(size, 8)));
            }
        }
        catch (ArgumentException)
        {
            return DBNull.Value;
        }
        catch (IndexOutOfRangeException)
        {
            return DBNull.Value;
        }
        catch (OverflowException)
        {
            return DBNull.Value;
        }
    }

    /// <summary>
    /// Reads a Jet T_NUMERIC value (17 bytes:
    /// <c>[precision][scale][sign][pad][lo:4][mid:4][hi:4]</c>). When <paramref name="strict"/>
    /// is <see langword="false"/> (the default, used by lossy diagnostics paths) returns the
    /// empty string for scale > 28, OLE-decimal overflow, or insufficient bytes. When
    /// <see langword="true"/> (the typed-reader path) those conditions throw
    /// <see cref="JetLimitationException"/> so the caller can surface the schema mismatch.
    /// </summary>
    private static string ReadNumericString(ReadOnlySpan<byte> b, int start, bool strict)
    {
        // Need bytes [start, start+15] — 16 total — even though the on-disk
        // T_NUMERIC slot is 17 bytes (the precision byte at offset 0 is currently unused).
        if (start + 16 > b.Length)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"T_NUMERIC slot at offset {start} extends past the row buffer (need 16 bytes, have {Math.Max(0, b.Length - start)}).");
            }

            return string.Empty;
        }

        byte scale = b[start + 1];
        bool negative = b[start + 2] != 0;
        uint lo = BinaryPrimitives.ReadUInt32LittleEndian(b.Slice(start + 4, 4));
        uint mid = BinaryPrimitives.ReadUInt32LittleEndian(b.Slice(start + 8, 4));
        uint hi = BinaryPrimitives.ReadUInt32LittleEndian(b.Slice(start + 12, 4));

        if (scale > 28)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"T_NUMERIC scale {scale} exceeds the .NET decimal maximum of 28.");
            }

            return string.Empty;
        }

        try
        {
            return new decimal((int)lo, (int)mid, (int)hi, negative, scale).ToString("G", CultureInfo.InvariantCulture);
        }
        catch (OverflowException ex)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"T_NUMERIC value overflow (hi=0x{hi:X8}, mid=0x{mid:X8}, lo=0x{lo:X8}, scale={scale})", ex);
            }

            return string.Empty;
        }
    }

    /// <summary>
    /// Typed counterpart to <see cref="ReadNumericString"/>: returns the boxed
    /// <see cref="decimal"/> directly. Strict-mode failure modes (insufficient
    /// bytes, scale > 28, decimal overflow) throw <see cref="JetLimitationException"/>
    /// to match the contract the typed reader path relies on; non-strict failures
    /// collapse to <see cref="DBNull.Value"/> (the typed analogue of
    /// <see cref="ReadNumericString"/>'s empty-string return).
    /// </summary>
    private static object ReadNumericTyped(ReadOnlySpan<byte> b, int start, bool strict)
    {
        if (start + 16 > b.Length)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"T_NUMERIC slot at offset {start} extends past the row buffer (need 16 bytes, have {Math.Max(0, b.Length - start)}).");
            }

            return DBNull.Value;
        }

        byte scale = b[start + 1];
        bool negative = b[start + 2] != 0;
        uint lo = BinaryPrimitives.ReadUInt32LittleEndian(b.Slice(start + 4, 4));
        uint mid = BinaryPrimitives.ReadUInt32LittleEndian(b.Slice(start + 8, 4));
        uint hi = BinaryPrimitives.ReadUInt32LittleEndian(b.Slice(start + 12, 4));

        if (scale > 28)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"T_NUMERIC scale {scale} exceeds the .NET decimal maximum of 28.");
            }

            return DBNull.Value;
        }

        try
        {
            // Bit-pattern reinterpretation: uint → int must be unchecked because
            // the project sets <CheckForOverflowUnderflow>true</CheckForOverflowUnderflow>
            // and the high-bit-set forms (e.g. decimal.MaxValue's 0xFFFFFFFF lo/mid/hi)
            // would otherwise throw. The legacy ReadNumericString trips this same
            // checked-cast and silently collapses to string.Empty → DBNull; the
            // typed path keeps the value.
            return new decimal(unchecked((int)lo), unchecked((int)mid), unchecked((int)hi), negative, scale);
        }
        catch (OverflowException ex)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"T_NUMERIC value overflow (hi=0x{hi:X8}, mid=0x{mid:X8}, lo=0x{lo:X8}, scale={scale})", ex);
            }

            return DBNull.Value;
        }
    }

    // ── Pure byte-decoding helpers ────────────────────────────────
    // Live here (rather than AccessBase) so JetTypeInfo's per-type byte→value
    // switches don't take an upward dependency on Core, and so non-Core
    // callers (IndexLeafIncremental, etc.) can use them without going through
    // the AccessBase inheritance chain.

    /// <summary>Reads a 24-bit little-endian unsigned integer.</summary>
    internal static int ReadUInt24LittleEndian(ReadOnlySpan<byte> source) =>
        source[0] | (source[1] << 8) | (source[2] << 16);

    /// <summary>Reads a 24-bit big-endian unsigned integer.</summary>
    internal static int ReadUInt24BigEndian(ReadOnlySpan<byte> source) =>
        (source[0] << 16) | (source[1] << 8) | source[2];

    /// <summary>Reads an IEEE-754 single-precision float in little-endian byte order.</summary>
    internal static float ReadSingleLittleEndian(ReadOnlySpan<byte> source) =>
#if NET5_0_OR_GREATER
        BinaryPrimitives.ReadSingleLittleEndian(source);
#else
        BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(source));
#endif

    /// <summary>Reads an IEEE-754 double-precision float in little-endian byte order.</summary>
    internal static double ReadDoubleLittleEndian(ReadOnlySpan<byte> source) =>
#if NET5_0_OR_GREATER
        BinaryPrimitives.ReadDoubleLittleEndian(source);
#else
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(source));
#endif

    /// <summary>Encodes <paramref name="source"/> as an upper-case hex string with no separators.</summary>
    internal static string ToHexStringNoSeparator(ReadOnlySpan<byte> source) =>
#if NET5_0_OR_GREATER
        Convert.ToHexString(source);
#else
        BitConverter.ToString(source.ToArray()).Replace("-", string.Empty, StringComparison.Ordinal);
#endif

    // ── UTF-16LE bytes \u2192 string helpers ─────────────────────────────────
    // The on-disk text encoding for Jet4/ACE is UCS-2 LE, which is the exact
    // in-memory layout of <c>char</c> on every platform .NET supports today
    // (all little-endian). Re-interpreting the byte span as a char span
    // therefore skips the validation pass and intermediate buffers that
    // <c>Encoding.Unicode.GetString</c> performs. On a hypothetical big-endian
    // host the JIT-constant <c>BitConverter.IsLittleEndian</c> check folds
    // the fast path away and we fall back to <c>Encoding.Unicode</c>.

    /// <summary>
    /// Decodes a UCS-2 LE byte slice into a <see cref="string"/>. The slice
    /// length must be even; the caller is responsible for trimming any odd
    /// trailing byte before calling. Allocates exactly one string.
    /// </summary>
    internal static string DecodeUtf16LE(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return string.Empty;
        }

        if (BitConverter.IsLittleEndian)
        {
            return new string(MemoryMarshal.Cast<byte, char>(bytes));
        }

        return Encoding.Unicode.GetString(bytes);
    }

    /// <summary>
    /// Appends a UCS-2 LE byte slice to <paramref name="sb"/> without
    /// allocating an intermediate <see cref="string"/>. The slice length
    /// must be even; the caller is responsible for trimming any odd
    /// trailing byte before calling.
    /// </summary>
    internal static void AppendUtf16LE(StringBuilder sb, ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return;
        }

        if (BitConverter.IsLittleEndian)
        {
            _ = sb.Append(MemoryMarshal.Cast<byte, char>(bytes));
            return;
        }

        _ = sb.Append(Encoding.Unicode.GetString(bytes));
    }

    // ── Phase 3 typed primitive readers ───────────────────────────────
    // Used by RowMapper<T>'s compiled direct decoder. Each helper returns
    // the unboxed CLR value for a single fixed-width column type, reading
    // straight off the page bytes. Callers must validate that
    // <c>start + size</c> is within the page; the helpers do not catch.

    /// <summary>Direct byte read at <paramref name="start"/>.</summary>
    internal static byte ReadByteAt(byte[] page, int start) => page[start];

    /// <summary>Reads a little-endian Int16 (T_INT) at <paramref name="start"/>.</summary>
    internal static short ReadInt16LE(byte[] page, int start) =>
        BinaryPrimitives.ReadInt16LittleEndian(page.AsSpan(start, 2));

    /// <summary>Reads a little-endian Int32 (T_LONG) at <paramref name="start"/>.</summary>
    internal static int ReadInt32LE(byte[] page, int start) =>
        BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(start, 4));

    /// <summary>Reads a little-endian Int64 at <paramref name="start"/>.</summary>
    internal static long ReadInt64LE(byte[] page, int start) =>
        BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(start, 8));

    /// <summary>Reads a little-endian Single (T_FLOAT) at <paramref name="start"/>.</summary>
    internal static float ReadFloatLE(byte[] page, int start) =>
        ReadSingleLittleEndian(page.AsSpan(start, 4));

    /// <summary>Reads a little-endian Double (T_DOUBLE) at <paramref name="start"/>.</summary>
    internal static double ReadDoubleLE(byte[] page, int start) =>
        ReadDoubleLittleEndian(page.AsSpan(start, 8));

    /// <summary>Reads a T_DATETIME (8-byte OLE date) at <paramref name="start"/>.</summary>
    internal static DateTime ReadDateTimeLE(byte[] page, int start) =>
        DateTime.FromOADate(ReadDoubleLittleEndian(page.AsSpan(start, 8)));

    /// <summary>Reads a T_MONEY (8-byte OLE currency) at <paramref name="start"/>.</summary>
    internal static decimal ReadMoneyLE(byte[] page, int start) =>
        decimal.FromOACurrency(BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(start, 8)));

    /// <summary>Reads a T_GUID (16-byte) at <paramref name="start"/>.</summary>
    internal static Guid ReadGuidAt(byte[] page, int start) =>
        new Guid(page.AsSpan(start, 16));

    /// <summary>
    /// Reads a T_NUMERIC value at <paramref name="start"/> as a typed
    /// <see cref="decimal"/>, skipping the boxing the
    /// <see cref="ReadFixedTyped"/> path performs. Throws
    /// <see cref="OverflowException"/> / <see cref="ArgumentException"/> on
    /// invalid scale or out-of-range values; the Phase 3 direct decoder
    /// catches these and leaves the property at its default.
    /// </summary>
    internal static decimal ReadDecimalLE(byte[] page, int start)
    {
        byte scale = page[start + 1];
        bool negative = page[start + 2] != 0;
        uint lo = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(start + 4, 4));
        uint mid = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(start + 8, 4));
        uint hi = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(start + 12, 4));

        // Bit-pattern reinterpretation: uint → int must be unchecked because
        // the project sets <CheckForOverflowUnderflow>true</CheckForOverflowUnderflow>;
        // mirrors ReadNumericTyped's well-trodden path.
        return new decimal(unchecked((int)lo), unchecked((int)mid), unchecked((int)hi), negative, scale);
    }
}

/// <summary>
/// Typed-row sentinel for <c>T_COMPLEX</c>/<c>T_ATTACHMENT</c> slots emitted
/// by <see cref="JetTypeInfo.ReadFixedTyped"/>. Carries the parent row's
/// complex_id directly so the post-processing pass can resolve attachment
/// bytes without parsing the legacy <c>"__CX:N__"</c> string format.
/// </summary>
internal readonly record struct ComplexIdRef(int Id);
