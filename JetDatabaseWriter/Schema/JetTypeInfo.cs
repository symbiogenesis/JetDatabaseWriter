namespace JetDatabaseWriter.Schema;

using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;

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
    /// (<c>BYTE/INT/LONG/MONEY/FLOAT/DOUBLE/DATETIME/GUID/NUMERIC/BIGINT</c>), or
    /// <c>0</c> for variable-length types and unknown codes. Mirrors the
    /// per-type sizes documented in mdbtools <see href="HACKING.md" />.
    /// </summary>
    /// <param name="type">JET column-type code (see <see cref="ColumnType"/>).</param>
    public static int GetFixedSize(ColumnType type) => type switch
    {
        ByteType => 1,
        IntegerType => 2,
        LongIntegerType => 4,
        MoneyType => 8,
        FloatType => 4,
        DoubleType => 8,
        DateTimeType => 8,
        GuidType => 16,
        NumericType => 17,
        BigIntType => 8,

        // Complex/attachment columns store a 4-byte ComplexId in the row's
        // fixed area (the actual payload lives in the hidden child table
        // joined via the ComplexId). Access writes col_len = 4 for both.
        ComplexType => 4,
        AttachmentType => 4,

        // Access 2019+ "Date/Time Extended" — 42-byte fixed slot.
        DateTimeExtendedType => 42,

        BooleanType or
        BinaryType or
        TextType or
        OleType or
        MemoType or
        _ => 0,
    };

    /// <summary>
    /// Returns <see langword="true"/> for the four JET types
    /// (<c>TEXT/BINARY/MEMO/OLE</c>) that are <i>always</i> stored in the
    /// row's variable-length area. Other types may still live in the variable
    /// area when the per-column <c>FLAG_FIXED</c> bit is cleared in the TDEF
    /// descriptor — see <see cref="ColumnInfo.IsFixed"/>.
    /// </summary>
    /// <param name="type">The JET column type or operation type.</param>
    public static bool IsAlwaysVariableLength(ColumnType type)
        => type is TextType or BinaryType or MemoType or OleType;

    /// <summary>
    /// Returns the CLR type used when projecting a TDEF column descriptor back
    /// to a public <c>ColumnDefinition</c>. Complex-column codes (<c>Complex</c>
    /// / <c>Attachment</c>) map to <see cref="byte"/>[] — the surface CLR type the
    /// reader resolves them to after joining the hidden flat child table — but
    /// callers that need the additional metadata (ComplexId, IsAttachment,
    /// IsMultiValue) must still special-case those codes before reaching this
    /// projection. Returns <see langword="null"/> for unknown codes.
    /// </summary>
    /// <param name="type">The JET column type or operation type.</param>
    public static Type? GetClrType(ColumnType type) => type switch
    {
        BooleanType => typeof(bool),
        ByteType => typeof(byte),
        IntegerType => typeof(short),
        LongIntegerType => typeof(int),
        MoneyType => typeof(decimal),
        FloatType => typeof(float),
        DoubleType => typeof(double),
        DateTimeType => typeof(DateTime),
        NumericType => typeof(decimal),
        BigIntType => typeof(long),
        GuidType => typeof(Guid),
        TextType => typeof(string),
        MemoType => typeof(string),
        BinaryType => typeof(byte[]),
        OleType => typeof(byte[]),
        AttachmentType => typeof(byte[]),
        ComplexType => typeof(byte[]),
        DateTimeExtendedType => typeof(DateTime),
        _ => null,
    };

    /// <summary>
    /// Returns <see langword="true"/> when the column is a MEMO whose TDEF flag
    /// byte has Jackcess <c>HYPERLINK_FLAG_MASK = 0x80</c> set — Microsoft Access
    /// surfaces such columns through the Hyperlink data-format affordance.
    /// See <see href="docs/design/hyperlink-format-notes.md" />.
    /// </summary>
    /// <param name="col">The column descriptor.</param>
    public static bool IsHyperlinkColumn(ColumnInfo col)
        => col.Type == MemoType && (col.Flags & Constants.ColumnDescriptorFlags.Hyperlink) != 0;

    /// <summary>
    /// Returns the CLR projection type for a column, accounting for the
    /// MEMO/Hyperlink override (<see cref="IsHyperlinkColumn"/>). Falls back
    /// to <see cref="string"/> for unknown type codes — matching the
    /// long-standing reader contract.
    /// </summary>
    /// <param name="col">The column descriptor.</param>
    public static Type ResolveClrType(ColumnInfo col)
        => IsHyperlinkColumn(col) ? typeof(Hyperlink) : GetClrType(ResolveValueType(col)) ?? typeof(string);

    /// <summary>
    /// Returns the logical value type for a column. For calculated columns,
    /// this prefers the persisted <c>ResultType</c> LvProp value when the
    /// reader has hydrated it; otherwise it falls back to the descriptor type.
    /// </summary>
    /// <param name="col">The column descriptor.</param>
    public static ColumnType ResolveValueType(ColumnInfo col)
        => col.IsCalculated && col.CalculatedResultType != default ? col.CalculatedResultType : col.Type;

    /// <summary>
    /// Returns the human-friendly Access display name for a JET column-type code
    /// (e.g. <c>"Long Integer"</c> for <c>LongInteger</c>). Unknown codes surface as
    /// the hex representation <c>"0xNN"</c>. Mirrors Access's UI labels and the
    /// names exposed by the legacy DAO/ADO type-name properties.
    /// </summary>
    /// <param name="type">The JET column type or operation type.</param>
    public static string GetTypeDisplayName(ColumnType type) => type switch
    {
        BooleanType => "Yes/No",
        ByteType => "Byte",
        IntegerType => "Integer",
        LongIntegerType => "Long Integer",
        MoneyType => "Currency",
        FloatType => "Single",
        DoubleType => "Double",
        DateTimeType => "Date/Time",
        BinaryType => "Binary",
        TextType => "Text",
        OleType => "OLE Object",
        MemoType => "Memo",
        GuidType => "GUID",
        NumericType => "Decimal",
        AttachmentType => "Attachment",
        ComplexType => "Complex",
        BigIntType => "Big Integer",
        DateTimeExtendedType => "Date/Time Extended",
        _ => $"0x{(byte)type:X2}",
    };

    /// <summary>
    /// Returns the user-facing <see cref="ColumnSize"/> for a column.
    /// <paramref name="declaredSize"/> is the on-disk descriptor size (the
    /// per-column <c>size</c> field) used for variable-width types like
    /// <c>Text</c> (Jet4 stores chars * 2 there) and unknown fixed types.
    /// </summary>
    /// <param name="type">The JET column type or operation type.</param>
    /// <param name="declaredSize">The declared size.</param>
    public static ColumnSize GetColumnSize(ColumnType type, int declaredSize) => type switch
    {
        BooleanType => ColumnSize.FromBits(1),
        ByteType => ColumnSize.FromBytes(1),
        IntegerType => ColumnSize.FromBytes(2),
        LongIntegerType or FloatType => ColumnSize.FromBytes(4),
        MoneyType or DoubleType or DateTimeType or BigIntType => ColumnSize.FromBytes(8),
        GuidType => ColumnSize.FromBytes(16),
        NumericType => ColumnSize.FromBytes(17),
        TextType => ColumnSize.FromChars(declaredSize > 0 ? declaredSize / 2 : 255),
        MemoType or OleType or AttachmentType or ComplexType => ColumnSize.Lval,
        BinaryType or DateTimeExtendedType or _ => declaredSize > 0 ? ColumnSize.FromBytes(declaredSize) : ColumnSize.Variable,
    };

    internal static ColumnType TypeCodeFromDefinition(ColumnDefinition column)
    {
        if (column.IsCalculated && column.CalculatedResultType != 0)
        {
            return (ColumnType)column.CalculatedResultType;
        }

        // Complex columns override the CLR-driven mapping. Access writes the
        // generic complex type byte for both Attachment and MultiValue parent
        // descriptors; the subtype lives in MSysComplexColumns.
        if (column.IsAttachment && column.IsMultiValue)
        {
            throw new ArgumentException($"Column '{column.Name}' cannot be both Attachment and MultiValue.", nameof(column));
        }

        if (column.IsAttachment)
        {
            return ComplexType;
        }

        if (column.IsMultiValue)
        {
            return ComplexType;
        }

        if (column.ColumnTypeOverride is ColumnType descriptorType)
        {
            return descriptorType;
        }

        if (column.IsDateTimeExtended)
        {
            if (column.ClrType != typeof(DateTime))
            {
                throw new ArgumentException(
                    $"Column '{column.Name}' has IsDateTimeExtended = true but CLR type '{column.ClrType}' is not DateTime.",
                    nameof(column));
            }

            return DateTimeExtendedType;
        }

        Type clrType = column.ClrType;

        switch (Type.GetTypeCode(clrType))
        {
            case TypeCode.Boolean:
                return BooleanType;
            case TypeCode.Byte:
                return ByteType;
            case TypeCode.Int16:
                return IntegerType;
            case TypeCode.Int32:
                return LongIntegerType;
            case TypeCode.Int64:
                return BigIntType;
            case TypeCode.Single:
                return FloatType;
            case TypeCode.Double:
                return DoubleType;
            case TypeCode.DateTime:
                return DateTimeType;
            case TypeCode.Decimal:
                return NumericType;
            case TypeCode.String:
                return column.MaxLength is > 0 and <= 255 ? TextType : MemoType;
            case TypeCode.Object:
                if (clrType == typeof(Guid))
                {
                    return GuidType;
                }

                if (clrType == typeof(Hyperlink))
                {
                    // typeof(Hyperlink) is shorthand for a MEMO column; TDefPageBuilder.BuildTableDefinition
                    // adds HYPERLINK_FLAG_MASK (0x80) unless DescriptorFlagsOverride replaces
                    // the computed TDEF column-flag byte.
                    return MemoType;
                }

                if (clrType == typeof(byte[]))
                {
                    return column.MaxLength is > 0 and <= 255 ? BinaryType : OleType;
                }

                throw new NotSupportedException($"CLR type '{clrType}' is not supported for table creation.");
            case TypeCode.Char:
            case TypeCode.DBNull:
            case TypeCode.Empty:
            case TypeCode.SByte:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
            case TypeCode.UInt64:
                throw new NotSupportedException($"CLR type '{clrType}' is not supported for table creation.");
            default:
                throw new InvalidOperationException($"CLR type '{clrType}' is unknown.");
        }
    }

    internal static void ValidateCalculatedColumn(ColumnDefinition column, DatabaseFormat format)
    {
        if (!column.IsCalculated)
        {
            return;
        }

        if (format != DatabaseFormat.AceAccdb)
        {
            throw new NotSupportedException(
                $"Column '{column.Name}': calculated columns are only supported in ACCDB databases.");
        }

        if (string.IsNullOrWhiteSpace(column.CalculationExpression))
        {
            throw new ArgumentException(
                $"Column '{column.Name}' is calculated but has no CalculationExpression.",
                nameof(column));
        }

        if (column.IsAttachment || column.IsMultiValue || column.IsHyperlink || column.ClrType == typeof(Hyperlink))
        {
            throw new NotSupportedException(
                $"Column '{column.Name}': calculated Attachment, MultiValue, and Hyperlink columns are not supported.");
        }

        if (column.IsAutoIncrement)
        {
            throw new NotSupportedException(
                $"Column '{column.Name}': calculated columns cannot be AutoNumber columns.");
        }

        ColumnType type = TypeCodeFromDefinition(column);
        switch (type)
        {
            case BooleanType:
            case ByteType:
            case IntegerType:
            case LongIntegerType:
            case MoneyType:
            case BigIntType:
            case FloatType:
            case DoubleType:
            case DateTimeType:
            case BinaryType:
            case TextType:
            case MemoType:
            case GuidType:
            case NumericType:
                return;
            case OleType:
            case AttachmentType:
            case ComplexType:
            case DateTimeExtendedType:
                throw new NotSupportedException(
                    $"Column '{column.Name}': calculated result type {GetTypeDisplayName(type)} is not supported.");
            default:
                throw new InvalidOperationException(
                    $"Column '{column.Name}': calculated result type {GetTypeDisplayName(type)} is unknown.");
        }
    }

    /// <summary>
    /// Validates and returns the precision (1..28) declared on a
    /// <c>Numeric</c> column definition. Defaults to <c>18</c> when the
    /// caller leaves <see cref="ColumnDefinition.NumericPrecision"/> at its
    /// initial value (matches Access "Number → Decimal" UI default).
    /// </summary>
    /// <param name="definition">The definition.</param>
    internal static byte ResolveNumericPrecision(ColumnDefinition definition)
    {
        byte p = definition.NumericPrecision == 0 ? (byte)18 : definition.NumericPrecision;
        Guard.InRange(p, 1, 28, $"Column '{definition.Name}' NumericPrecision");
        return p;
    }

    /// <summary>
    /// Validates and returns the scale (0..28, &lt;= precision) declared on a
    /// <c>Numeric</c> column definition. Defaults to <c>0</c> (Access UI
    /// default). The incremental index path uses this value as the
    /// canonical sort-key scale.
    /// </summary>
    /// <param name="definition">The definition.</param>
    internal static byte ResolveNumericScale(ColumnDefinition definition)
    {
        byte s = definition.NumericScale;
        byte p = definition.NumericPrecision == 0 ? (byte)18 : definition.NumericPrecision;
        Guard.InRange(s, 0, 28, $"Column '{definition.Name}' NumericScale");
        Guard.InRange(s, 0, p, $"Column '{definition.Name}' NumericScale (NumericPrecision={p})");
        return s;
    }

    // ── Fixed-column decoding ────────────────────────────────────────
    //
    // The two ReadFixed* helpers below decode a fixed-width JET column
    // value out of a raw row buffer. They live next to the per-type
    // metadata above (GetFixedSize / GetClrType / GetTypeDisplayName) so
    // the per-type switch tables stay co-located. ReadFixedString is the
    // legacy lossy/diagnostic path; ReadFixedTyped is the typed-reader
    // hot path.

    private static bool FixedReadInBounds(ColumnType type, int start, int size, int rowLength)
    {
        // Up-front bounds guard shared by ReadFixedString/ReadFixedTyped so the
        // per-type decoders never index or slice past the row buffer, replacing the
        // previous reliance on catching IndexOutOfRangeException / ArgumentException
        // for out-of-range offsets. Numeric is excluded because it self-validates
        // inside TryReadNumericDecimal with strict-mode JetLimitationException
        // semantics that must still propagate.
        if (type == NumericType)
        {
            return true;
        }

        int fixedSize = GetFixedSize(type);
        int required = fixedSize > 0 ? fixedSize : Math.Min(size, 8);
        return start >= 0 && required <= rowLength - start;
    }

    /// <summary>
    /// Formats a fixed-width JET column value as a culture-invariant string.
    /// When <paramref name="strictNumeric"/> is <see langword="true"/>, Numeric values
    /// that overflow .NET's <see cref="decimal"/> range or carry an out-of-range scale
    /// surface as <see cref="JetLimitationException"/> instead of being silently elided
    /// to the empty string — the contract the typed reader path relies on.
    /// </summary>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="type">The JET column type or operation type.</param>
    /// <param name="size">The size in bytes.</param>
    /// <param name="strictNumeric">The strict numeric.</param>
    internal static string ReadFixedString(ReadOnlySpan<byte> row, int start, ColumnType type, int size, bool strictNumeric = false)
    {
        if (!FixedReadInBounds(type, start, size, row.Length))
        {
            return string.Empty;
        }

        try
        {
            return type switch
            {
                ByteType => row[start].ToString(CultureInfo.InvariantCulture),
                IntegerType => Ri16(row, start).ToString(CultureInfo.InvariantCulture),
                LongIntegerType => Ri32(row, start).ToString(CultureInfo.InvariantCulture),
                FloatType => ReadSingleLittleEndian(row.Slice(start, 4)).ToString("G", CultureInfo.InvariantCulture),
                DoubleType => ReadDoubleLittleEndian(row.Slice(start, 8)).ToString("G", CultureInfo.InvariantCulture),
                DateTimeType => DateTime.FromOADate(ReadDoubleLittleEndian(row.Slice(start, 8))).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                MoneyType => decimal.FromOACurrency(Ri64(row, start)).ToString("F4", CultureInfo.InvariantCulture),
                BigIntType => Ri64(row, start).ToString(CultureInfo.InvariantCulture),
                GuidType => new Guid(row.Slice(start, 16)).ToString("B"),
                NumericType => ReadNumericString(row, start, scale: 0, strictNumeric),
                ComplexType or AttachmentType => size >= 4 ? $"__CX:{Ri32(row, start)}__" : string.Empty,
                DateTimeExtendedType => ReadDateTimeExtended(row, start, size).ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture),
                BooleanType or
                BinaryType or
                TextType or
                OleType or
                MemoType or
                _ => ToHexStringNoSeparator(row.Slice(start, Math.Min(size, 8))),
            };
        }
        catch (Exception ex) when (ex is ArgumentException or OverflowException)
        {
            // Bounds are validated up front by FixedReadInBounds, so the only
            // failures that reach here are genuinely invalid on-disk values: a bad
            // OA date (DateTime.FromOADate) or a malformed 42-byte Date/Time
            // Extended payload. Surface those as the diagnostic empty string.
            // Strict Numeric limits propagate as JetLimitationException, and an
            // IndexOutOfRangeException would signal a real slicing bug, so neither
            // is caught here.
            return string.Empty;
        }
    }

    internal static string ReadFixedString(ReadOnlySpan<byte> row, int start, ColumnInfo column, int size, bool strictNumeric = false)
        => column.Type == NumericType
            ? ReadNumericString(row, start, column.NumericScale, strictNumeric)
            : ReadFixedString(row, start, column.Type, size, strictNumeric);

    /// <summary>
    /// Decodes a fixed-width JET column value directly to its boxed CLR primitive,
    /// bypassing the lossy <c>ReadFixedString</c> +
    /// <c>TypedValueParser.ParseValue</c> round-trip used by the diagnostics path.
    /// The typed-reader hot path uses this to avoid per-column culture-invariant
    /// string formatting and re-parsing.
    /// <para>
    /// Type mapping mirrors <see cref="GetClrType(ColumnType)"/>:
    /// <c>Byte → byte</c>, <c>Integer → short</c>, <c>LongInteger → int</c>,
    /// <c>Float → float</c>, <c>Double → double</c>,
    /// <c>DateTime → DateTime</c> (un-truncated; <c>ReadFixedString</c>
    /// formats with <c>"yyyy-MM-dd HH:mm:ss"</c> and loses sub-second precision —
    /// the typed path keeps full precision),
    /// <c>Money → decimal</c>, <c>BigInt → long</c>, <c>Guid → Guid</c>,
    /// <c>Numeric → decimal</c>,
    /// <c>Complex</c>/<c>Attachment → <see cref="ComplexIdRef"/></c> typed
    /// sentinel carrying the row's complex_id directly (the legacy
    /// <c>"__CX:N__"</c> string round-trip used by <c>ReadFixedString</c>
    /// is avoided on the typed hot path), and unknown types fall through to
    /// the same hex-string representation <c>ReadFixedString</c> emits.
    /// </para>
    /// <para>
    /// Returns <see cref="DBNull.Value"/> when the underlying byte access throws
    /// (<see cref="ArgumentException"/>, <see cref="IndexOutOfRangeException"/>,
    /// <see cref="OverflowException"/>) — matching the empty-string-then-DBNull
    /// behaviour of the round-trip path. When <paramref name="strictNumeric"/>
    /// is <see langword="true"/>, Numeric values that overflow or carry an
    /// out-of-range scale surface as <see cref="JetLimitationException"/>; with
    /// <see langword="false"/> they collapse to <see cref="DBNull.Value"/>.
    /// </para>
    /// </summary>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="type">The JET column type or operation type.</param>
    /// <param name="size">The size in bytes.</param>
    /// <param name="strictNumeric">The strict numeric.</param>
    internal static object ReadFixedTyped(ReadOnlySpan<byte> row, int start, ColumnType type, int size, bool strictNumeric = false)
    {
        if (!FixedReadInBounds(type, start, size, row.Length))
        {
            return DBNull.Value;
        }

        try
        {
            return type switch
            {
                ByteType => row[start],

                // Ri16 sign-extends correctly under <CheckForOverflowUnderflow>true</CheckForOverflowUnderflow>;
                // the legacy "(short)Ru16(...)" cast throws OverflowException for
                // values with the high bit set and ReadFixedString silently maps
                // those to string.Empty → DBNull. The typed path keeps the value.
                IntegerType => Ri16(row, start),
                LongIntegerType => Ri32(row, start),
                FloatType => ReadSingleLittleEndian(row.Slice(start, 4)),
                DoubleType => ReadDoubleLittleEndian(row.Slice(start, 8)),
                DateTimeType => DateTime.FromOADate(ReadDoubleLittleEndian(row.Slice(start, 8))),
                MoneyType => decimal.FromOACurrency(Ri64(row, start)),
                BigIntType => Ri64(row, start),
                GuidType => new Guid(row.Slice(start, 16)),
                NumericType => ReadNumericTyped(row, start, scale: 0, strictNumeric),
                DateTimeExtendedType => ReadDateTimeExtended(row, start, size),
                ComplexType or AttachmentType => size >= 4
                                        ? new ComplexIdRef(Ri32(row, start))
                                        : DBNull.Value,
                BooleanType or
                BinaryType or
                TextType or
                OleType or
                MemoType or
                _ => ToHexStringNoSeparator(row.Slice(start, Math.Min(size, 8))),
            };
        }
        catch (Exception ex) when (ex is ArgumentException or OverflowException)
        {
            // See ReadFixedString: bounds are pre-validated by FixedReadInBounds,
            // so only invalid OA dates and malformed Date/Time Extended payloads
            // reach here. Strict Numeric limits propagate as JetLimitationException
            // and an IndexOutOfRangeException would signal a real slicing bug.
            return DBNull.Value;
        }
    }

    internal static object ReadFixedTyped(ReadOnlySpan<byte> row, int start, ColumnInfo column, int size, bool strictNumeric = false)
        => column.Type == NumericType
            ? ReadNumericTyped(row, start, column.NumericScale, strictNumeric)
            : ReadFixedTyped(row, start, column.Type, size, strictNumeric);

    /// <summary>
    /// Reads a Jet Numeric value (17-byte slot:
    /// <c>[sign][16-byte unsigned magnitude]</c>; scale comes from the descriptor). When <paramref name="strict"/>
    /// is <see langword="false"/> (the default, used by lossy diagnostics paths) returns the
    /// empty string for scale > 28, OLE-decimal overflow, or insufficient bytes. When
    /// <see langword="true"/> (the typed-reader path) those conditions throw
    /// <see cref="JetLimitationException"/> so the caller can surface the schema mismatch.
    /// </summary>
    /// <param name="b">The second value or byte buffer.</param>
    /// <param name="start">The start.</param>
    /// <param name="scale">The scale.</param>
    /// <param name="strict">The strict.</param>
    private static string ReadNumericString(ReadOnlySpan<byte> b, int start, int scale, bool strict)
    {
        if (!TryReadNumericDecimal(b, start, scale, strict, out decimal value))
        {
            return string.Empty;
        }

        return value.ToString("G", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Typed counterpart to <see cref="ReadNumericString"/>: returns the boxed
    /// <see cref="decimal"/> directly. Strict-mode failure modes (insufficient
    /// bytes, scale > 28, decimal overflow) throw <see cref="JetLimitationException"/>
    /// to match the contract the typed reader path relies on; non-strict failures
    /// collapse to <see cref="DBNull.Value"/> (the typed analogue of
    /// <see cref="ReadNumericString"/>'s empty-string return).
    /// </summary>
    /// <param name="b">The second value or byte buffer.</param>
    /// <param name="start">The start.</param>
    /// <param name="scale">The scale.</param>
    /// <param name="strict">The strict.</param>
    private static object ReadNumericTyped(ReadOnlySpan<byte> b, int start, int scale, bool strict) => TryReadNumericDecimal(b, start, scale, strict, out decimal value)
            ? value
            : DBNull.Value;

    private static bool TryReadNumericDecimal(ReadOnlySpan<byte> b, int start, int scale, bool strict, out decimal value)
    {
        value = default;

        if (start + 17 > b.Length)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"Numeric slot at offset {start} extends past the row buffer (need 17 bytes, have {Math.Max(0, b.Length - start)}).");
            }

            return false;
        }

        if (scale > 28)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"Numeric scale {scale} exceeds the .NET decimal maximum of 28.");
            }

            return false;
        }

        bool negative = b[start] != 0;
        Span<byte> magnitudeBe = stackalloc byte[16];
        b.Slice(start + 1, 16).CopyTo(magnitudeBe);
        FixNumericByteOrder(magnitudeBe);

        Span<byte> magnitudeLe = stackalloc byte[16];
        for (int i = 0; i < 16; i++)
        {
            magnitudeLe[i] = magnitudeBe[15 - i];
        }

        if (magnitudeLe[12] != 0 || magnitudeLe[13] != 0 || magnitudeLe[14] != 0 || magnitudeLe[15] != 0)
        {
            if (strict)
            {
                throw new JetLimitationException("Numeric value exceeds the .NET decimal 96-bit mantissa range.");
            }

            return false;
        }

        try
        {
            uint lo = Ru32(magnitudeLe, 0);
            uint mid = Ru32(magnitudeLe, 4);
            uint hi = Ru32(magnitudeLe, 8);
            value = new decimal(unchecked((int)lo), unchecked((int)mid), unchecked((int)hi), negative, (byte)scale);
            return true;
        }
        catch (OverflowException ex)
        {
            if (strict)
            {
                throw new JetLimitationException(
                    $"Numeric value overflow (scale={scale})", ex);
            }

            return false;
        }
    }

    internal static void FixNumericByteOrder(Span<byte> bytes)
    {
#if NET8_0_OR_GREATER
        // .NET 8 JIT emits efficient code for uint endianness reversal; process
        // full 4-byte words first, then handle any trailing bytes defensively.
        Span<uint> words = MemoryMarshal.Cast<byte, uint>(bytes);
        for (int i = 0; i < words.Length; i++)
        {
            words[i] = BinaryPrimitives.ReverseEndianness(words[i]);
        }

        int tailStart = words.Length * sizeof(uint);
#else
        const int tailStart = 0;
#endif

        for (int i = tailStart; i + 3 < bytes.Length; i += 4)
        {
            (bytes[i], bytes[i + 3]) = (bytes[i + 3], bytes[i]);
            (bytes[i + 1], bytes[i + 2]) = (bytes[i + 2], bytes[i + 1]);
        }
    }

    // ── Pure byte-decoding helpers ────────────────────────────────
    // Live here (rather than AccessBase) so JetTypeInfo's per-type byte→value
    // switches don't take an upward dependency on Core, and so non-Core
    // callers (index codecs, etc.) can use them without going through
    // the AccessBase inheritance chain.

    // Terse little-endian primitives — workhorses called from row/page/index
    // crackers and the encryption layer. R = read, W = write; u/i = unsigned/
    // signed; bit width. Each accepts a byte[] or (ReadOnly)Span<byte> base
    // plus an absolute offset.

    internal static short Ri16(byte[] b, int o) =>
        BinaryPrimitives.ReadInt16LittleEndian(b.AsSpan(o, 2));

    internal static short Ri16(ReadOnlySpan<byte> b, int o) =>
        BinaryPrimitives.ReadInt16LittleEndian(b.Slice(o, 2));

    internal static ushort Ru16(byte[] b, int o) =>
        BinaryPrimitives.ReadUInt16LittleEndian(b.AsSpan(o, 2));

    internal static ushort Ru16(ReadOnlySpan<byte> b, int o) =>
        BinaryPrimitives.ReadUInt16LittleEndian(b.Slice(o, 2));

    internal static int Ri32(byte[] b, int o) =>
        BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(o, 4));

    internal static int Ri32(ReadOnlySpan<byte> b, int o) =>
        BinaryPrimitives.ReadInt32LittleEndian(b.Slice(o, 4));

    internal static uint Ru32(byte[] b, int o) =>
        BinaryPrimitives.ReadUInt32LittleEndian(b.AsSpan(o, 4));

    internal static uint Ru32(ReadOnlySpan<byte> b, int o) =>
        BinaryPrimitives.ReadUInt32LittleEndian(b.Slice(o, 4));

    internal static long Ri64(byte[] b, int o) =>
        BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(o, 8));

    internal static long Ri64(ReadOnlySpan<byte> b, int o) =>
        BinaryPrimitives.ReadInt64LittleEndian(b.Slice(o, 8));

    internal static void Wu16(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(o, 2), (ushort)value);

    internal static void Wu16(Span<byte> b, int o, int value) =>
        BinaryPrimitives.WriteUInt16LittleEndian(b.Slice(o, 2), (ushort)value);

    internal static void Wu32(byte[] b, int o, uint value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(o, 4), value);

    internal static void Wu32(Span<byte> b, int o, uint value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(b.Slice(o, 4), value);

    internal static void Wu32(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(o, 4), unchecked((uint)value));

    internal static void Wu32(Span<byte> b, int o, int value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(b.Slice(o, 4), unchecked((uint)value));

    internal static void Wi16(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteInt16LittleEndian(b.AsSpan(o, 2), (short)value);

    internal static void Wi16(Span<byte> b, int o, int value) =>
        BinaryPrimitives.WriteInt16LittleEndian(b.Slice(o, 2), (short)value);

    internal static void Wi32(byte[] b, int o, int value) =>
        BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(o, 4), value);

    internal static void Wi32(Span<byte> b, int o, int value) =>
        BinaryPrimitives.WriteInt32LittleEndian(b.Slice(o, 4), value);

    internal static void Wi64(byte[] b, int o, long value) =>
        BinaryPrimitives.WriteInt64LittleEndian(b.AsSpan(o, 8), value);

    internal static void Wi64(Span<byte> b, int o, long value) =>
        BinaryPrimitives.WriteInt64LittleEndian(b.Slice(o, 8), value);

    internal static void WriteUInt24(byte[] b, int o, int value)
    {
        Wu16(b, o, value & 0xFFFF);
        b[o + 2] = (byte)((value >> 16) & 0xFF);
    }

    internal static void WriteField(byte[] b, int o, int fieldSize, int value)
    {
        if (fieldSize == 1)
        {
            b[o] = (byte)value;
        }
        else
        {
            Wu16(b, o, value);
        }
    }

    /// <summary>Reads a 24-bit little-endian unsigned integer.</summary>
    /// <param name="source">The source.</param>
    internal static int ReadUInt24LittleEndian(ReadOnlySpan<byte> source) =>
        source[0] | (source[1] << 8) | (source[2] << 16);

    /// <summary>Reads a 24-bit big-endian unsigned integer.</summary>
    /// <param name="source">The source.</param>
    internal static int ReadUInt24BigEndian(ReadOnlySpan<byte> source) =>
        (source[0] << 16) | (source[1] << 8) | source[2];

    /// <summary>Reads an IEEE-754 single-precision float in little-endian byte order.</summary>
    /// <param name="source">The source.</param>
    internal static float ReadSingleLittleEndian(ReadOnlySpan<byte> source) =>
#if NET5_0_OR_GREATER
        BinaryPrimitives.ReadSingleLittleEndian(source);
#else
        BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(source));
#endif

    /// <summary>Reads an IEEE-754 double-precision float in little-endian byte order.</summary>
    /// <param name="source">The source.</param>
    internal static double ReadDoubleLittleEndian(ReadOnlySpan<byte> source) =>
#if NET5_0_OR_GREATER
        BinaryPrimitives.ReadDoubleLittleEndian(source);
#else
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(source));
#endif

    /// <summary>Encodes <paramref name="source"/> as an upper-case hex string with no separators.</summary>
    /// <param name="source">The source.</param>
    internal static string ToHexStringNoSeparator(ReadOnlySpan<byte> source) =>
#if NET5_0_OR_GREATER
        Convert.ToHexString(source);
#else
        BitConverter.ToString(source.ToArray()).Replace("-", string.Empty, StringComparison.Ordinal);
#endif

    // ── UTF-16LE bytes \u2192 string helpers ─────────────────────────────────
    // The on-disk text encoding for Jet4/ACE is UCS-2 LE, which is the exact
    // in-memory layout of <c>char</c> on supported .NET runtimes
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
    /// <param name="bytes">The bytes.</param>
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

    // ── JET4 compressed-unicode text codec ──────────────────
    // Pure static encode/decode for Jet4/ACE Text and Memo columns, sitting
    // next to the UTF-16LE helper above. Moved out of AccessBase because the
    // codec is format-agnostic and holds no instance/database state.

    /// <summary>
    /// Encodes a string for storage in a Jet4 text/memo column.
    /// When all characters are in the U+0001..U+00FF range, emits the
    /// compressed form (<c>0xFF 0xFE</c> marker + 1 byte per character),
    /// which the reader decodes via <c>DecompressJet4</c>.
    /// Otherwise emits plain UCS-2 LE.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="compress">The compress.</param>
    /// <remarks>
    /// The "no NUL" restriction (chars must be > U+0000) avoids ambiguity
    /// with the compressed-mode toggle byte (<c>0x00</c>). The compressed
    /// form is only chosen when it actually saves bytes (length &gt;= 3
    /// characters), so 1- and 2-character strings are still written as
    /// plain UCS-2 to avoid the 2-byte marker overhead.
    /// </remarks>
    internal static byte[] EncodeJet4Text(string value, bool compress = true) => EncodeJet4Text(value, int.MaxValue, compress);

    /// <summary>
    /// Encodes a string into Jet4 text format, truncating to at most
    /// <paramref name="maxBytes"/> output bytes. Avoids a secondary
    /// <c>Array.Resize</c> when the caller has a column-size limit.
    /// </summary>
    /// <param name="value">The string to encode.</param>
    /// <param name="maxBytes">Maximum output byte count.</param>
    /// <param name="compress">When <see langword="true"/> (the default) and
    /// all characters fit in Latin-1, emits the compressed form
    /// (<c>0xFF 0xFE</c> marker + 1 byte/char). When <see langword="false"/>
    /// always emits plain UCS-2 LE. Callers should pass <see langword="false"/>
    /// for columns whose <c>ExtraFlags</c> byte does not have the
    /// <see cref="Constants.CompressedUnicodeExtFlagMask"/> bit set.</param>
    internal static byte[] EncodeJet4Text(string value, int maxBytes, bool compress = true)
    {
        if (string.IsNullOrEmpty(value))
        {
            return [];
        }

        bool compressible = compress && value.Length >= 3;
        if (compressible)
        {
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                if (c is '\0' or > (char)0xFF)
                {
                    compressible = false;
                    break;
                }
            }
        }

        if (!compressible)
        {
            int charCount = Math.Min(value.Length, maxBytes / 2);
            byte[] result = new byte[charCount * 2];
            Encoding.Unicode.GetBytes(value.AsSpan(0, charCount), result);
            return result;
        }

        int compressedLen = Math.Min(value.Length + 2, maxBytes);
        int charsToEncode = compressedLen - 2;
        if (charsToEncode <= 0)
        {
            return [];
        }

        byte[] compressed = new byte[charsToEncode + 2];
        compressed[0] = 0xFF;
        compressed[1] = 0xFE;
        for (int i = 0; i < charsToEncode; i++)
        {
            compressed[i + 2] = (byte)value[i];
        }

        return compressed;
    }

    /// <summary>
    /// Decodes Jet4 text (UCS-2 / UTF-16LE).
    /// If data starts with the compressed-unicode marker 0xFF 0xFE, the
    /// JET4 compressed-string algorithm is applied first.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decoded string.</returns>
    internal static string DecodeJet4Text(byte[] bytes, int start, int len)
    {
        if (len < 2)
        {
            return string.Empty;
        }

        if (bytes[start] == 0xFF && bytes[start + 1] == 0xFE)
        {
            return DecompressJet4(bytes, start + 2, len - 2);
        }

        // Plain UCS-2 LE — length must be even
        int evenLen = len & ~1;
        return evenLen > 0 ? DecodeUtf16LE(bytes.AsSpan(start, evenLen)) : string.Empty;
    }

    /// <summary>
    /// Decodes Jet4 text from a span-backed buffer. Array-backed reader hot paths
    /// use the byte-array overload so compressed strings can be built directly.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decoded string.</returns>
    internal static string DecodeJet4Text(ReadOnlySpan<byte> bytes, int start, int len)
    {
        if (len < 2)
        {
            return string.Empty;
        }

        if (bytes[start] == 0xFF && bytes[start + 1] == 0xFE)
        {
            return DecompressJet4(bytes, start + 2, len - 2);
        }

        // Plain UCS-2 LE — length must be even
        int evenLen = len & ~1;
        return evenLen > 0 ? DecodeUtf16LE(bytes.Slice(start, evenLen)) : string.Empty;
    }

    /// <summary>
    /// Decodes the JET4 "compressed unicode" encoding.
    /// A 0x00 byte toggles between 1-byte compressed (ASCII) and 2-byte
    /// uncompressed (UCS-2) mode.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decompressed string.</returns>
    private static string DecompressJet4(byte[] bytes, int start, int len)
    {
        // Fast path: if no 0x00 byte appears in the data, the entire string
        // is compressed Latin-1 with no mode switches. This is the overwhelming
        // majority of text values in real Jet4 databases.
        int end = start + len;
        bool allCompressed = true;
        for (int index = start; index < end; index++)
        {
            if (bytes[index] == 0x00)
            {
                allCompressed = false;
                break;
            }
        }

        if (allCompressed)
        {
            return CreateFromCompressed(bytes, start, len);
        }

        return DecompressJet4Slow(bytes, start, len);
    }

    /// <summary>
    /// Decodes the JET4 "compressed unicode" encoding from a span-backed buffer.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decompressed string.</returns>
    private static string DecompressJet4(ReadOnlySpan<byte> bytes, int start, int len)
    {
        int end = start + len;
        bool allCompressed = true;
        for (int index = start; index < end; index++)
        {
            if (bytes[index] == 0x00)
            {
                allCompressed = false;
                break;
            }
        }

        if (allCompressed)
        {
            return CreateFromCompressed(bytes, start, len);
        }

        return DecompressJet4Slow(bytes, start, len);
    }

    private static string CreateFromCompressed(byte[] bytes, int start, int len) =>
#if NET6_0_OR_GREATER
        Encoding.Latin1.GetString(bytes, start, len);
#else
        string.Create(
            len,
            (Bytes: bytes, Start: start),
            static (chars, state) =>
            {
                for (int index = 0; index < chars.Length; index++)
                {
                    chars[index] = (char)state.Bytes[state.Start + index];
                }
            });
#endif

    private static string CreateFromCompressed(ReadOnlySpan<byte> bytes, int start, int len)
    {
        char[] chars = new char[len];
        for (int index = 0; index < len; index++)
        {
            chars[index] = (char)bytes[start + index];
        }

        return new string(chars);
    }

    private static string DecompressJet4Slow(byte[] bytes, int start, int len)
    {
        int charCount = CountDecompressedChars(bytes, start, len);
        return string.Create(
            charCount,
            (Bytes: bytes, Start: start, Length: len),
            static (chars, state) => FillDecompressed(state.Bytes, state.Start, state.Length, chars));
    }

    private static string DecompressJet4Slow(ReadOnlySpan<byte> bytes, int start, int len)
    {
        // Two-pass: count output chars first, then fill directly into char[].
        int charCount = CountDecompressedChars(bytes, start, len);
        char[] chars = new char[charCount];
        FillDecompressed(bytes, start, len, chars);
        return new string(chars);
    }

    private static int CountDecompressedChars(ReadOnlySpan<byte> bytes, int start, int len)
    {
        int count = 0;
        bool compressed = true;
        int i = start, end = start + len;

        while (i < end)
        {
            if (compressed)
            {
                if (bytes[i] == 0x00)
                {
                    compressed = false;
                    i++;
                    continue;
                }

                count++;
                i++;
            }
            else
            {
                int runStart = i;
                while (i + 1 < end && !(bytes[i] == 0x00 && bytes[i + 1] == 0x00))
                {
                    i += 2;
                }

                count += (i - runStart) / 2;

                if (i + 1 >= end)
                {
                    break;
                }

                compressed = true;
                i += 2;
            }
        }

        return count;
    }

    private static void FillDecompressed(ReadOnlySpan<byte> bytes, int start, int len, Span<char> output)
    {
        int pos = 0;
        bool compressed = true;
        int i = start, end = start + len;

        while (i < end)
        {
            if (compressed)
            {
                if (bytes[i] == 0x00)
                {
                    compressed = false;
                    i++;
                    continue;
                }

                output[pos++] = (char)bytes[i++];
            }
            else
            {
                int runStart = i;
                while (i + 1 < end && !(bytes[i] == 0x00 && bytes[i + 1] == 0x00))
                {
                    i += 2;
                }

                int runLen = i - runStart;
                for (int r = 0; r < runLen; r += 2)
                {
                    output[pos++] = (char)(bytes[runStart + r] | (bytes[runStart + r + 1] << 8));
                }

                if (i + 1 >= end)
                {
                    break;
                }

                compressed = true;
                i += 2;
            }
        }
    }

    // ── Typed primitive readers ───────────────────────────────
    // Used by RowMapper<T>'s compiled direct decoder. Each helper returns
    // the unboxed CLR value for a single fixed-width column type, reading
    // straight off the page bytes. Callers must validate that
    // <c>start + size</c> is within the page; the helpers do not catch.

    /// <summary>Direct byte read at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static byte ReadByteAt(byte[] page, int start) => page[start];

    /// <summary>Reads a little-endian Int16 (Integer) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static short ReadInt16LE(byte[] page, int start) =>
        BinaryPrimitives.ReadInt16LittleEndian(page.AsSpan(start, 2));

    /// <summary>Reads a little-endian Int32 (LongInteger) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static int ReadInt32LE(byte[] page, int start) =>
        BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(start, 4));

    /// <summary>Reads a little-endian Int64 (BigInt / Large Number) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static long ReadInt64LE(byte[] page, int start) =>
        BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(start, 8));

    /// <summary>Reads a little-endian Single (Float) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static float ReadFloatLE(byte[] page, int start) =>
        ReadSingleLittleEndian(page.AsSpan(start, 4));

    /// <summary>Reads a little-endian Double (Double) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static double ReadDoubleLE(byte[] page, int start) =>
        ReadDoubleLittleEndian(page.AsSpan(start, 8));

    /// <summary>Reads a DateTime (8-byte OLE date) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static DateTime ReadDateTimeLE(byte[] page, int start) =>
        DateTime.FromOADate(ReadDoubleLittleEndian(page.AsSpan(start, 8)));

    /// <summary>Reads a Money (8-byte OLE currency) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static decimal ReadMoneyLE(byte[] page, int start) =>
        decimal.FromOACurrency(BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(start, 8)));

    /// <summary>Reads a Guid (16-byte) at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static Guid ReadGuidAt(byte[] page, int start) =>
        new(page.AsSpan(start, 16));

    /// <summary>
    /// Reads a Numeric value at <paramref name="start"/> as a typed
    /// <see cref="decimal"/>, skipping the boxing the
    /// <c>ReadFixedTyped</c> path performs. Throws
    /// <see cref="OverflowException"/> / <see cref="ArgumentException"/> on
    /// invalid scale or out-of-range values; the direct decoder
    /// catches these and leaves the property at its default.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="scale">The scale.</param>
    internal static decimal ReadDecimalLE(byte[] page, int start, int scale)
    {
        _ = TryReadNumericDecimal(page, start, scale, strict: true, out decimal value);
        return value;
    }

    /// <summary>Reads an Access Date/Time Extended value at <paramref name="start"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="start">The start.</param>
    internal static DateTime ReadDateTimeExtendedAt(byte[] page, int start) => ReadDateTimeExtended(page, start, GetFixedSize(DateTimeExtendedType));

    /// <summary>
    /// Decodes the 42-byte Access 2019+ Date/Time Extended payload.
    /// Layout: 19 ASCII digits for days since 0001-01-01, ':', 12 ASCII
    /// digits for whole seconds since midnight, 7 ASCII fractional-second
    /// digits in 100 ns units, ':', '7', NUL.
    /// </summary>
    /// <param name="row">The row bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="size">The available fixed slot size.</param>
    /// <exception cref="ArgumentException">Thrown when the payload is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the time-of-day is out of range.</exception>
    internal static DateTime ReadDateTimeExtended(ReadOnlySpan<byte> row, int start, int size)
    {
        const int required = 42;
        if (size < required)
        {
            throw new ArgumentException($"Date/Time Extended payload must be {required} bytes but only {size} bytes were available.");
        }

        ReadOnlySpan<byte> payload = row.Slice(start, required);
        if (payload[19] != (byte)':'
            || payload[39] != (byte)':'
            || payload[40] != (byte)'7'
            || payload[41] != 0x00)
        {
            throw new ArgumentException("Date/Time Extended payload has an invalid trailer.");
        }

        long days = ReadPaddedAsciiInt64(payload[..19]);
        long seconds = ReadPaddedAsciiInt64(payload.Slice(20, 12));
        long fractions = ReadPaddedAsciiInt64(payload.Slice(32, 7));
        if (seconds >= 24L * 60L * 60L || fractions >= TimeSpan.TicksPerSecond)
        {
            throw new ArgumentOutOfRangeException(nameof(row), "Date/Time Extended time-of-day is out of range.");
        }

        long ticks = checked((days * TimeSpan.TicksPerDay) + (seconds * TimeSpan.TicksPerSecond) + fractions);
        return new DateTime(ticks, DateTimeKind.Unspecified);
    }

    /// <summary>Encodes a <see cref="DateTime"/> into the 42-byte Date/Time Extended layout.</summary>
    /// <param name="destination">The destination span.</param>
    /// <param name="value">The value.</param>
    /// <exception cref="ArgumentException">Thrown when the destination span is too small.</exception>
    internal static void WriteDateTimeExtended(Span<byte> destination, DateTime value)
    {
        const int required = 42;
        if (destination.Length < required)
        {
            throw new ArgumentException($"Date/Time Extended destination must be at least {required} bytes.", nameof(destination));
        }

        long days = value.Ticks / TimeSpan.TicksPerDay;
        long timeTicks = value.Ticks % TimeSpan.TicksPerDay;
        long seconds = timeTicks / TimeSpan.TicksPerSecond;
        long fractions = timeTicks % TimeSpan.TicksPerSecond;

        WritePaddedAsciiInt64(destination[..19], days);
        destination[19] = (byte)':';
        WritePaddedAsciiInt64(destination.Slice(20, 12), seconds);
        WritePaddedAsciiInt64(destination.Slice(32, 7), fractions);
        destination[39] = (byte)':';
        destination[40] = (byte)'7';
        destination[41] = 0x00;
    }

    private static long ReadPaddedAsciiInt64(ReadOnlySpan<byte> value)
    {
        long result = 0;
        for (int i = 0; i < value.Length; i++)
        {
            byte digit = value[i];
            if (digit is < (byte)'0' or > (byte)'9')
            {
                throw new ArgumentException("Date/Time Extended payload contains a non-digit character.");
            }

            result = checked((result * 10) + (digit - (byte)'0'));
        }

        return result;
    }

    private static void WritePaddedAsciiInt64(Span<byte> destination, long value)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "Date/Time Extended fields cannot be negative.");
        }

        for (int i = destination.Length - 1; i >= 0; i--)
        {
            destination[i] = (byte)('0' + (value % 10));
            value /= 10;
        }

        if (value != 0)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "Date/Time Extended field does not fit in the destination width.");
        }
    }
}

/// <summary>
/// Typed-row sentinel for <c>Complex</c>/<c>Attachment</c> slots emitted
/// by <c>JetTypeInfo.ReadFixedTyped</c>. Carries the parent row's
/// complex_id directly so the post-processing pass can resolve attachment
/// bytes without parsing the legacy <c>"__CX:N__"</c> string format.
/// </summary>
/// <param name="Id">The identifier.</param>
internal readonly record struct ComplexIdRef(int Id);
