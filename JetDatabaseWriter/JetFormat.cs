namespace JetDatabaseWriter;

using System;
using System.Runtime.CompilerServices;
using System.Text;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// The immutable format profile of one database file: its format, page size,
/// code page and the per-format byte layouts of data pages, LVAL pages, TDEF
/// headers, column descriptors, row trailers, index sections and index
/// pages, plus the text and name codecs that depend on them. One instance is
/// built per open file from its header (<see cref="FromHeader"/>), or for a
/// format alone (<see cref="For"/>). It holds no file state and never changes.
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
    /// an error. The registration also serves the code that decodes Jet3 LvProp
    /// text with <c>Encoding.GetEncoding(1252)</c>, which only runs once a
    /// database file has built its profile.
    /// </summary>
    static JetFormat() => Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    private JetFormat(DatabaseFormat kind, int codePage)
    {
        this.Kind = kind;
        this.IsJet3 = kind == DatabaseFormat.Jet3Mdb;
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
        this.DataPage = DataPageLayout.For(kind);
        this.LvalPage = LvalPageLayout.For(kind);
        this.TDef = TDefHeaderLayout.For(kind);
        this.ColumnDescriptor = ColumnDescriptorLayout.For(kind);
        this.RowFields = RowFieldSizes.For(kind);
        this.Index = IndexLayout.For(kind);
        this.IndexPage = IndexPageLayout.ForFormat(kind);
    }

    /// <summary>Gets the database format.</summary>
    internal DatabaseFormat Kind { get; }

    /// <summary>Gets a value indicating whether the format is Jet3 (Access 97).</summary>
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
    /// Builds the profile of the database whose page-0 header is
    /// <paramref name="header"/>: the format from the version byte at 0x14,
    /// and the code page from the masked header word at 0x3C, or 1252 when
    /// it holds none.
    /// </summary>
    /// <param name="header">The first <see cref="Constants.DatabaseHeader.Length"/> bytes of page 0, as stored.</param>
    /// <returns>The profile.</returns>
    internal static JetFormat FromHeader(byte[] header)
    {
        DatabaseFormat kind = EncryptionConverter.DetectFormat(header);

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
    /// Builds the profile of a database of <paramref name="format"/> with code
    /// page 1252, which the writer stamps into every new database.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The profile.</returns>
    internal static JetFormat For(DatabaseFormat format) => new(format, 1252);

    /// <summary>Returns the page size in bytes for the given database format (2048 for Jet3, 4096 for Jet4/ACE).</summary>
    /// <param name="format">The format.</param>
    /// <returns>The page size in bytes.</returns>
    internal static int PageSizeOf(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet4 : Constants.PageSizes.Jet3;

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
