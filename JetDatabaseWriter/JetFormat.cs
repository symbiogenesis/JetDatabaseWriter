namespace JetDatabaseWriter;

using System;
using System.Runtime.CompilerServices;
using System.Text;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// The immutable format profile of one database file: its format, page size,
/// code page and the per-format byte layouts of data pages, LVAL pages, TDEF
/// headers, column descriptors, row trailers, index sections and index
/// pages, the text and name codecs that depend on them, and the capability
/// flags and per-format values that code asks for instead of comparing
/// formats. One instance is built per open file from its header
/// (<see cref="FromHeader"/>), or for a new database of a format
/// (<see cref="ForNewDatabase"/>). It holds no file state and never changes.
/// Outside this file and the enum itself, only the encryption code, which
/// classifies a file and unmasks its header before any profile exists, and
/// the facades name a <see cref="DatabaseFormat"/> value; FormatKnowledgeTests
/// enforces it.
/// </summary>
internal sealed class JetFormat
{
    /// <summary>
    /// <see cref="AnsiEncoding"/> with an encoder that throws for a character
    /// the code page does not have; Jet3 text and names are encoded with it.
    /// </summary>
    private readonly Encoding ansiTextEncoder;

    /// <summary>
    /// Initializes static members of the <see cref="JetFormat"/> class: registers
    /// the code-page encodings .NET does not load by default. Without them every
    /// Jet3 code page, Windows-1252 included, would fall back to UTF-8 without
    /// an error. The registration also serves Jet3 <c>LvProp</c> text, which
    /// <see cref="PropertyTextEncodingOf"/> encodes in Windows-1252, and the
    /// <c>LvProp</c> parser, which only runs once a database file has built its
    /// profile.
    /// </summary>
    static JetFormat() => Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    private JetFormat(DatabaseFormat kind, int codePage)
    {
        bool jet3 = kind == DatabaseFormat.Jet3Mdb;
        bool ace = kind == DatabaseFormat.AceAccdb;
        this.Kind = kind;
        this.IsJet3 = jet3;
        this.PageSize = PageSizeOf(kind);

        // An unknown code page falls back to UTF-8: Jet3 files that earlier
        // builds of this library created left the header unmasked, so its raw
        // zeros decode as code page 17019, and they hold UTF-8 text.
        Encoding ansiEncoding;
        try
        {
            ansiEncoding = Encoding.GetEncoding(codePage);
        }
        catch (ArgumentException)
        {
            ansiEncoding = Encoding.UTF8;
            codePage = 65001;
        }
        catch (NotSupportedException)
        {
            ansiEncoding = Encoding.UTF8;
            codePage = 65001;
        }

        this.CodePage = codePage;
        this.AnsiEncoding = ansiEncoding;
        this.ansiTextEncoder = CreateStrictEncoder(ansiEncoding);

        // Format-specific TDEF / page / column / row layouts:
        //   Jet4 / ACE (Access 2000–2019): TDEF 8+55 = 63 bytes, column descriptor 25 bytes.
        //   Jet3        (Access 97):       TDEF 8+35 = 43 bytes, column descriptor 18 bytes.
        // ACE adds the complex AutoNumber to the Jet4 TDEF header.
        (TDefHeaderLayout tdef, byte newDatabaseVersion) = kind switch
        {
            DatabaseFormat.Jet3Mdb => (TDefHeaderLayout.Jet3, (byte)0x00),
            DatabaseFormat.Jet4Mdb => (TDefHeaderLayout.Jet4, (byte)0x01),
            DatabaseFormat.AceAccdb => (TDefHeaderLayout.Ace, (byte)0x02),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown database format."),
        };
        this.TDef = tdef;
        this.NewDatabaseVersion = newDatabaseVersion;
        this.DataPage = jet3 ? DataPageLayout.Jet3 : DataPageLayout.Jet4;
        this.LvalPage = jet3 ? LvalPageLayout.Jet3 : LvalPageLayout.Jet4;
        this.ColumnDescriptor = jet3 ? ColumnDescriptorLayout.Jet3 : ColumnDescriptorLayout.Jet4;
        this.RowFields = jet3 ? RowFieldSizes.Jet3 : RowFieldSizes.Jet4;
        this.Index = jet3 ? IndexLayout.Jet3 : IndexLayout.Jet4;
        this.IndexPage = jet3 ? IndexPageLayout.Jet3 : IndexPageLayout.Jet4;

        // Jet3 has no Numeric type; Large Number, Date/Time Extended, complex
        // and calculated columns arrived with ACE.
        this.SupportsNumeric = !jet3;
        this.SupportsBigInt = ace;
        this.SupportsDateTimeExtended = ace;
        this.SupportsComplexColumns = ace;
        this.SupportsCalculatedColumns = ace;
        this.LegacyNumericIndexKeys = kind == DatabaseFormat.Jet4Mdb;
        this.WritesTDefFormatMagic = !jet3;
        this.WritesTDefFreeSpace = !jet3;
        this.SupportsIndexSeeks = !jet3;

        this.VersionName = jet3 ? "Jet3" : "Jet4/ACE";
        this.CommitLockOffset = ace ? 0xFFFFFFFCL : 0xFFFFFFFEL;
    }

    /// <summary>Gets the database format.</summary>
    internal DatabaseFormat Kind { get; }

    /// <summary>
    /// Gets a value indicating whether the format is Jet3 (Access 97), whose
    /// row and TDEF layouts use one-byte counts, ANSI names and no format
    /// magic. Code that branches on a fact a capability flag below names asks
    /// the flag instead.
    /// </summary>
    internal bool IsJet3 { get; }

    /// <summary>Gets the page size in bytes: 2048 on Jet3, 4096 on Jet4 and ACE.</summary>
    internal int PageSize { get; }

    /// <summary>
    /// Gets the decoded database code page: the header's, 1252 when the header
    /// holds none, or 65001 (UTF-8) when .NET has no encoding for it.
    /// </summary>
    internal int CodePage { get; }

    /// <summary>
    /// Gets the database's ANSI code-page encoding, which decodes Jet3 text and
    /// names. Its encoder substitutes a best-fit character or <c>?</c> for one
    /// the code page does not have, so Jet3 text and names are encoded with
    /// <see cref="EncodeAnsiText"/> instead, which refuses such a character.
    /// </summary>
    internal Encoding AnsiEncoding { get; }

    /// <summary>Gets per-format byte offsets within a data-page (page type 0x01) header — see <see cref="DataPageLayout"/>.</summary>
    internal DataPageLayout DataPage { get; }

    /// <summary>Gets the per-format layout of an LVAL page — see <see cref="LvalPageLayout"/>.</summary>
    internal LvalPageLayout LvalPage { get; }

    /// <summary>Gets per-format byte offsets within a TDEF block plus real-idx entry size — see <see cref="TDefHeaderLayout"/>.</summary>
    internal TDefHeaderLayout TDef { get; }

    /// <summary>Gets per-format byte offsets within one column descriptor — see <see cref="ColumnDescriptorLayout"/>.</summary>
    internal ColumnDescriptorLayout ColumnDescriptor { get; }

    /// <summary>Gets per-format byte sizes of the in-row trailer fields — see <see cref="RowFieldSizes"/>.</summary>
    internal RowFieldSizes RowFields { get; }

    /// <summary>
    /// Gets per-format byte offsets and entry sizes for the TDEF page's real-idx
    /// physical descriptor (§3.1) and logical-idx entry (§3.2) sections.
    /// </summary>
    internal IndexLayout Index { get; }

    /// <summary>Gets the per-format index page header layout — see <see cref="IndexPageLayout"/>.</summary>
    internal IndexPageLayout IndexPage { get; }

    /// <summary>
    /// Gets a value indicating whether the format has the Numeric (Decimal,
    /// type <c>0x10</c>) column type: Jet4 and ACE. Jet3 has none, so a
    /// decimal column there is stored as Currency.
    /// </summary>
    internal bool SupportsNumeric { get; }

    /// <summary>Gets a value indicating whether the format has the Large Number (BigInt, type <c>0x13</c>) column type: ACE only.</summary>
    internal bool SupportsBigInt { get; }

    /// <summary>Gets a value indicating whether the format has the Date/Time Extended (type <c>0x14</c>) column type: ACE only.</summary>
    internal bool SupportsDateTimeExtended { get; }

    /// <summary>
    /// Gets a value indicating whether the format has complex columns
    /// (Attachment and multi-value) and the <c>MSysComplexColumns</c> catalog
    /// they need: ACE only.
    /// </summary>
    internal bool SupportsComplexColumns { get; }

    /// <summary>Gets a value indicating whether the format has calculated columns: ACE only.</summary>
    internal bool SupportsCalculatedColumns { get; }

    /// <summary>
    /// Gets a value indicating whether Numeric index keys use Jet4's legacy
    /// fixed-point encoding rather than ACE's (<see cref="IndexKeyEncoder"/>):
    /// Jet4 only.
    /// </summary>
    internal bool LegacyNumericIndexKeys { get; }

    /// <summary>
    /// Gets a value indicating whether the writer stamps the format magic
    /// (<see cref="Constants.TableDefinition.Jet4.FormatMagic"/>) into the TDEF
    /// header at offset 12, each column descriptor and each logical-idx entry,
    /// and the leading magic into each real-idx descriptor: Jet4 and ACE, whose
    /// layouts reserve those fields. Jet3 has none of them.
    /// </summary>
    internal bool WritesTDefFormatMagic { get; }

    /// <summary>
    /// Gets a value indicating whether the writer stamps a TDEF page's free
    /// space into page bytes 2..3: Jet4 and ACE. On Jet3 it leaves them zero,
    /// where Access 97 keeps <c>VC</c>.
    /// </summary>
    internal bool WritesTDefFreeSpace { get; }

    /// <summary>
    /// Gets a value indicating whether index seeks are used: Jet4 and ACE.
    /// Jet3 reads by table scan until its General 97 text keys are encoded
    /// right.
    /// </summary>
    internal bool SupportsIndexSeeks { get; }

    /// <summary>Gets the version name the statistics and the catalog diagnostics print: <c>Jet3</c> or <c>Jet4/ACE</c>.</summary>
    internal string VersionName { get; }

    /// <summary>
    /// Gets the offset of the one-byte commit lock that Microsoft Access, OLE
    /// DB JET and ACE take to gate a commit: <c>0xFFFFFFFC</c> on ACE,
    /// <c>0xFFFFFFFE</c> on Jet3 and Jet4.
    /// </summary>
    internal long CommitLockOffset { get; }

    /// <summary>
    /// Gets the version byte at header offset <c>0x14</c> that the writer
    /// stamps into a new database: 0 on Jet3, 1 on Jet4 and 2 on ACE, which
    /// <see cref="DetectFormat"/> reads back. Later Access versions write
    /// higher values into an ACE file.
    /// </summary>
    internal byte NewDatabaseVersion { get; }

    /// <summary>
    /// Gets the NUL-terminated signature at header offset 4:
    /// <c>Standard Jet DB</c> on Jet3 and Jet4, <c>Standard ACE DB</c> on ACE.
    /// </summary>
    internal ReadOnlySpan<byte> HeaderSignature => this.Kind == DatabaseFormat.AceAccdb ? "Standard ACE DB\0"u8 : "Standard Jet DB\0"u8;

    /// <summary>
    /// Builds the profile of the database whose page-0 header is
    /// <paramref name="header"/>: the format from the version byte at 0x14
    /// (<see cref="DetectFormat"/>), and the code page from the masked header
    /// word at 0x3C, or 1252 when it holds none.
    /// </summary>
    /// <param name="header">The first <see cref="Constants.DatabaseHeader.Length"/> bytes of page 0, as stored.</param>
    /// <returns>The profile.</returns>
    internal static JetFormat FromHeader(byte[] header)
    {
        DatabaseFormat kind = DetectFormat(header);

        // Codepage / sort order: stored as a UInt16 at hdr[0x3C], scrambled by
        // the constant-key RC4 stream Microsoft Access applies to header bytes
        // [0x18 .. 0x18+126/128]. EncryptionManager.DecodeHeaderCodePage handles
        // the descrambling so we recover the real codepage (e.g. 1252) instead
        // of a corrupted byte. ACE / ACCDB stores text as UTF-16 in user data
        // so the codepage there is largely cosmetic, but Jet3 .mdb files (and
        // Jet4 catalog names) need it correct to round-trip non-ASCII names.
        int codePage = EncryptionManager.DecodeHeaderCodePage(header, kind);
        if (codePage <= 0)
        {
            codePage = 1252;
        }

        return new JetFormat(kind, codePage);
    }

    /// <summary>
    /// Builds the profile of a new database of <paramref name="format"/>, with
    /// code page 1252, which the writer stamps into every new database.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The profile.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="format"/> is not a defined format.</exception>
    internal static JetFormat ForNewDatabase(DatabaseFormat format)
        => format is DatabaseFormat.Jet3Mdb or DatabaseFormat.Jet4Mdb or DatabaseFormat.AceAccdb
            ? new JetFormat(format, 1252)
            : throw new ArgumentOutOfRangeException(nameof(format), format, $"Unsupported database format: {format}.");

    /// <summary>
    /// Classifies a JET/ACE file by the format-version byte at header offset
    /// <c>0x14</c>: 0 is Jet3, 1 is Jet4, and 2 or more is ACE (ACCDB). The
    /// encryption code classifies a file with it before the file has a
    /// profile.
    /// </summary>
    /// <param name="header">The header, as stored; the version byte precedes the masked region.</param>
    /// <returns>The format.</returns>
    internal static DatabaseFormat DetectFormat(byte[] header) => header[0x14] switch
    {
        >= 2 => DatabaseFormat.AceAccdb,
        >= 1 => DatabaseFormat.Jet4Mdb,
        _ => DatabaseFormat.Jet3Mdb,
    };

    /// <summary>Returns the page size in bytes for the given database format (2048 for Jet3, 4096 for Jet4/ACE).</summary>
    /// <param name="format">The format.</param>
    /// <returns>The page size in bytes.</returns>
    internal static int PageSizeOf(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet4 : Constants.PageSizes.Jet3;

    /// <summary>
    /// Returns the encoding of the text in an <c>LvProp</c> property block of
    /// <paramref name="format"/>: UTF-16LE on Jet4 and ACE, and Windows-1252 on
    /// Jet3, whatever the database's code page.
    /// </summary>
    /// <param name="format">The format.</param>
    /// <returns>The encoding.</returns>
    internal static Encoding PropertyTextEncodingOf(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? Encoding.GetEncoding(1252) : Encoding.Unicode;

    /// <summary>
    /// Returns the magic an <c>LvProp</c> property block of
    /// <paramref name="format"/> starts with: <c>KKD\0</c> on Jet3,
    /// <c>MR2\0</c> on Jet4 and ACE.
    /// </summary>
    /// <param name="format">The format.</param>
    /// <returns>The magic, as a little-endian 32-bit value.</returns>
    internal static uint PropertyBlockMagicOf(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? ColumnPropertyBlock.MagicKkd : ColumnPropertyBlock.MagicMr2;

    /// <summary>
    /// Reads the per-row column count from the row header at
    /// <paramref name="rowStart"/>. Jet3 stores it as a single byte; Jet4/ACE
    /// uses a 16-bit little-endian word.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <returns>The row's column count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int ReadRowColumnCount(byte[] page, int rowStart)
        => this.IsJet3 ? page[rowStart] : Ru16(page, rowStart);

    /// <summary>
    /// Decodes a text/memo slice using the format-appropriate codec
    /// (Jet4 compressed/UCS-2 or Jet3 ANSI). Empty slices return
    /// <see cref="string.Empty"/>.
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="start">The start.</param>
    /// <param name="len">The length in bytes.</param>
    /// <returns>The decoded text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal string DecodeText(byte[] bytes, int start, int len)
    {
        if (len <= 0)
        {
            return string.Empty;
        }

        return this.IsJet3 ? this.AnsiEncoding.GetString(bytes, start, len) : DecodeJet4Text(bytes, start, len);
    }

    /// <summary>
    /// Encodes a string for storage using the format-appropriate codec
    /// (Jet4 with optional compression vs Jet3 ANSI code-page bytes).
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="compress">Whether Jet4/ACE text may use the compressed-Unicode form.</param>
    /// <returns>The stored bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeText(string value, bool compress = true)
        => this.IsJet3 ? this.EncodeAnsiText(value) : EncodeJet4Text(value, compress);

    /// <summary>
    /// Encodes a string for storage using the format-appropriate codec,
    /// truncating the Jet4 path to at most <paramref name="maxBytes"/> output bytes.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="maxBytes">The most bytes the Jet4/ACE encoding may produce.</param>
    /// <param name="compress">Whether Jet4/ACE text may use the compressed-Unicode form.</param>
    /// <returns>The stored bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal byte[] EncodeText(string value, int maxBytes, bool compress = true)
        => this.IsJet3 ? this.EncodeAnsiText(value) : EncodeJet4Text(value, maxBytes, compress);

    /// <summary>
    /// Encodes Jet3 text or a Jet3 name in the database's code page. A
    /// character the code page does not have throws instead of being stored as
    /// .NET's best-fit match or <c>?</c>; the public write paths refuse such
    /// text before anything is written (<see cref="DescribeUnstorableCharacter"/>),
    /// so this throw is only a backstop.
    /// </summary>
    /// <param name="value">The text.</param>
    /// <returns>The code-page bytes.</returns>
    /// <exception cref="EncoderFallbackException"><paramref name="value"/> holds a character the code page does not have.</exception>
    internal byte[] EncodeAnsiText(string value) => this.ansiTextEncoder.GetBytes(value);

    /// <summary>
    /// Describes the first character of <paramref name="text"/> this database
    /// cannot store, for an error message, or returns <see langword="null"/>
    /// when it can store all of them. Jet4 and ACE store text and names as
    /// UTF-16, which holds any string. Jet3 stores them in the database's code
    /// page, Windows-1252 for new files, where .NET would write a best-fit
    /// match or <c>?</c> instead (Łódź as Lódz, 中文 as ??). The stored name
    /// would then differ from the caller's, so the table could not be found by
    /// it and a second such name passed the duplicate check, and an index key
    /// built from the caller's text would not match its row.
    /// </summary>
    /// <param name="text">The name or value.</param>
    /// <returns>The character and its code point, such as <c>'中' (U+4E2D)</c>, or <see langword="null"/>.</returns>
    internal string? DescribeUnstorableCharacter(string text)
    {
        if (!this.IsJet3)
        {
            return null;
        }

        try
        {
            _ = this.ansiTextEncoder.GetByteCount(text);
            return null;
        }
        catch (EncoderFallbackException ex)
        {
            if (ex.CharUnknownHigh != '\0')
            {
                return $"'{ex.CharUnknownHigh}{ex.CharUnknownLow}' (U+{char.ConvertToUtf32(ex.CharUnknownHigh, ex.CharUnknownLow):X4})";
            }

            // A lone surrogate is not printable on its own.
            return char.IsSurrogate(ex.CharUnknown)
                ? $"the unpaired surrogate U+{(int)ex.CharUnknown:X4}"
                : $"'{ex.CharUnknown}' (U+{(int)ex.CharUnknown:X4})";
        }
    }

    /// <summary>
    /// Builds the message for text that <see cref="DescribeUnstorableCharacter"/>
    /// found a character in.
    /// </summary>
    /// <param name="subject">What the text is, such as "The table name '中文'".</param>
    /// <param name="character">The description <see cref="DescribeUnstorableCharacter"/> returned.</param>
    /// <returns>The message.</returns>
    internal string UnstorableTextMessage(string subject, string character)
        => $"{subject} cannot be stored: it contains {character}, which is not in code page {this.CodePage}. A Jet3 (Access 97) database stores text and object names in its code page.";

    /// <summary>
    /// Reads a single column name from the TDEF byte array at <paramref name="pos"/>,
    /// advancing <paramref name="pos"/> past the name bytes.
    /// Returns the byte length consumed, or -1 if the name extends beyond <paramref name="td"/>.
    /// </summary>
    /// <param name="td">The logical TDEF bytes.</param>
    /// <param name="pos">The byte position.</param>
    /// <param name="name">The name.</param>
    /// <returns>The bytes consumed, or -1.</returns>
    internal int ReadColumnName(byte[] td, ref int pos, out string name)
    {
        name = string.Empty;
        if (pos >= td.Length)
        {
            return -1;
        }

        if (!this.IsJet3)
        {
            if (pos + 2 > td.Length)
            {
                return -1;
            }

            int len = Ru16(td, pos);
            pos += 2;
            if (pos + len > td.Length)
            {
                return -1;
            }

            name = DecodeUtf16LE(td.AsSpan(pos, len));
            pos += len;
            return len + 2;
        }
        else
        {
            int len = td[pos++];
            if (pos + len > td.Length)
            {
                return -1;
            }

            name = this.AnsiEncoding.GetString(td, pos, len);
            pos += len;
            return len + 1;
        }
    }

    /// <summary>
    /// Encodes <paramref name="name"/> as a TDEF name record, the inverse of
    /// <see cref="ReadColumnName"/>: on Jet4 / ACE a 2-byte length and the
    /// UTF-16LE bytes, on Jet3 a 1-byte length and the bytes in the
    /// database's ANSI code page.
    /// </summary>
    /// <param name="name">The column or index name.</param>
    /// <returns>The length-prefixed name record.</returns>
    /// <exception cref="ArgumentException">Thrown when the encoded name is longer than its length prefix can hold (255 bytes on Jet3).</exception>
    /// <exception cref="EncoderFallbackException">Thrown on Jet3 when the name holds a character the code page does not have (<see cref="EncodeAnsiText"/>).</exception>
    internal byte[] EncodeTDefNameRecord(string name)
    {
        bool jet3 = this.IsJet3;
        byte[] nameBytes = jet3 ? this.EncodeAnsiText(name) : Encoding.Unicode.GetBytes(name);
        int prefixSize = jet3 ? 1 : 2;
        int maxLength = jet3 ? byte.MaxValue : ushort.MaxValue;
        if (nameBytes.Length > maxLength)
        {
            throw new ArgumentException(
                $"The name '{name}' encodes to {nameBytes.Length} bytes; a {this.Kind} table definition stores at most {maxLength}.",
                nameof(name));
        }

        byte[] record = new byte[prefixSize + nameBytes.Length];
        if (jet3)
        {
            record[0] = (byte)nameBytes.Length;
        }
        else
        {
            Wu16(record, 0, nameBytes.Length);
        }

        Buffer.BlockCopy(nameBytes, 0, record, prefixSize, nameBytes.Length);
        return record;
    }

    /// <summary>
    /// Returns an encoding that encodes as <paramref name="encoding"/> does but
    /// throws <see cref="EncoderFallbackException"/> for a character it cannot
    /// encode. .NET's code-page encodings substitute a best-fit match or
    /// <c>?</c>, and UTF-8 substitutes U+FFFD for an unpaired surrogate.
    /// </summary>
    /// <param name="encoding">The database's code-page encoding.</param>
    /// <returns>The strict encoding.</returns>
    private static Encoding CreateStrictEncoder(Encoding encoding)
    {
        if (encoding is UTF8Encoding)
        {
            return new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        }

        var strict = (Encoding)encoding.Clone();
        strict.EncoderFallback = EncoderFallback.ExceptionFallback;
        return strict;
    }
}
