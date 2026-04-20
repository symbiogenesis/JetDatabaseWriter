namespace JetDatabaseReader;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;

#pragma warning disable SA1401 // Field should be private — fields are private protected (assembly-only)

/// <summary>
/// Abstract base class for Access database readers and writers.
/// Contains shared JET format parsing, page I/O, catalog access, and text decoding.
/// </summary>
public abstract class AccessBase : IAccessBase
{
    // ── Column type codes (mdbtools HACKING.md) ──────────────────────
    private protected const byte T_BOOL = 0x01; // 1 bit  – stored in null_mask
    private protected const byte T_BYTE = 0x02; // 1 byte
    private protected const byte T_INT = 0x03; // 2 bytes (signed)
    private protected const byte T_LONG = 0x04; // 4 bytes (signed)
    private protected const byte T_MONEY = 0x05; // 8 bytes (int64 / 10000)
    private protected const byte T_FLOAT = 0x06; // 4 bytes (IEEE 754)
    private protected const byte T_DOUBLE = 0x07; // 8 bytes (IEEE 754)
    private protected const byte T_DATETIME = 0x08; // 8 bytes (OA date)
    private protected const byte T_BINARY = 0x09; // variable (≤ 255 bytes)
    private protected const byte T_TEXT = 0x0A; // variable (UCS-2 in Jet4, ANSI in Jet3)
    private protected const byte T_OLE = 0x0B; // LVAL
    private protected const byte T_MEMO = 0x0C; // LVAL or inline
    private protected const byte T_GUID = 0x0F; // 16 bytes
    private protected const byte T_NUMERIC = 0x10; // 17 bytes scaled decimal
    private protected const byte T_ATTACHMENT = 0x11; // complex: attachment (Access 2007+)
    private protected const byte T_COMPLEX = 0x12; // complex: multi-value / version history (Access 2007+)

    // Catalog (MSysObjects) constants
    private protected const int OBJ_TABLE = 1;
    private protected const int OBJ_LINKED_TABLE = 4;
    private protected const int OBJ_LINKED_ODBC = 6;
    private protected const uint SYSTABLE_MASK = 0x80000002U;

    // ── Format-specific offsets ───────────────────────────────────────

    // Data page
    private protected readonly int _dpTDefOff;    // offset of tdef_pg (4 bytes)
    private protected readonly int _dpNumRows;    // offset of num_rows (2 bytes)
    private protected readonly int _dpRowsStart;  // offset of first row-offset entry

    // TDEF page (absolute offsets within the TDEF byte array)
    private protected readonly int _tdNumCols;    // offset of num_cols    (2 bytes)
    private protected readonly int _tdNumRealIdx; // offset of num_real_idx (4 bytes)
    private protected readonly int _tdBlockEnd;   // first byte after table-definition block

    // Column descriptor (per-column, fixed-size block)
    private protected readonly int _colDescSz;
    private protected readonly int _colTypeOff;
    private protected readonly int _colVarOff;    // offset_V – var-col index
    private protected readonly int _colFixedOff;  // offset_F – byte offset in fixed area
    private protected readonly int _colSzOff;     // col_len
    private protected readonly int _colFlagsOff;  // bitmask
    private protected readonly int _colNumOff;    // col_num (includes deleted)

    // Per-real-index entry size (skipped during column parsing)
    private protected readonly int _realIdxEntrySz;

    // Row field sizes (differ between Jet3 and Jet4)
    private protected readonly int _numColsFldSz;  // 1 or 2
    private protected readonly int _varEntrySz;    // 1 or 2  (var_table entry)
    private protected readonly int _eodFldSz;      // 1 or 2
    private protected readonly int _varLenFldSz;   // 1 or 2

    private protected readonly int _pgSz;
    private protected readonly bool _jet4;
    private protected readonly FileStream _fs;
    private protected readonly Encoding _ansiEncoding;
    private protected readonly int _codePage;

    /// <summary>
    /// Jet3 XOR decryption mask (128 bytes). Non-null when Jet3 encryption flag is set.
    /// Applied cyclically to pages 1+ during ReadPage.
    /// </summary>
    private protected readonly byte[]? _jet3XorMask;

    /// <summary>
    /// Jet4 RC4 database key (from header offset 0x3E). Non-null when RC4 encryption is active.
    /// Used to derive per-page RC4 keys for decrypting pages 1+.
    /// </summary>
    private protected uint? _rc4DbKey;

    private protected bool _disposed;

    static AccessBase()
    {
        // On .NET Core / .NET 5+ code-page encodings (e.g. Windows-1252) are not
        // available by default. Register them once so GetEncoding() works for any
        // ANSI code page stored in the JET database header.
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessBase"/> class
    /// from the database file header.
    /// </summary>
    /// <param name="fs">An open <see cref="FileStream"/> for the database file.</param>
    private protected AccessBase(FileStream fs)
    {
        _fs = fs;

        // Read enough of the database definition page (page 0)
        var hdr = new byte[0x80];
        _ = _fs.Seek(0, SeekOrigin.Begin);
        _ = _fs.Read(hdr, 0, hdr.Length);

        // Offset 0x14: 0 = Jet3, ≥ 1 = Jet4+
        byte ver = hdr[0x14];
        _jet4 = ver >= 1;
        _pgSz = _jet4 ? 4096 : 2048;

        // Jet3 XOR encryption: byte 0x62 bit 0x01 means pages 1+ are XOR-masked
        if (!_jet4 && hdr.Length > 0x62 && (hdr[0x62] & 0x01) != 0)
        {
            _jet3XorMask = new byte[]
            {
                0xEC, 0x7B, 0x28, 0x07, 0x77, 0x26, 0x13, 0x82,
                0x75, 0x4E, 0x22, 0x04, 0x42, 0xCE, 0xB3, 0x19,
                0xA1, 0x32, 0x75, 0x46, 0xE3, 0x66, 0x27, 0x37,
                0x19, 0x9E, 0xA3, 0x56, 0x85, 0x3A, 0xD6, 0xDE,
                0xEC, 0x03, 0xE6, 0xFC, 0xF8, 0x85, 0x8F, 0xA0,
                0x1B, 0x20, 0xAD, 0xE5, 0x0E, 0x7A, 0xF7, 0x38,
                0x54, 0xFC, 0x10, 0x4E, 0x25, 0x22, 0xBD, 0xC7,
                0x5D, 0x62, 0x5E, 0x44, 0xBB, 0x6D, 0xCB, 0xB5,
                0x90, 0x14, 0xDE, 0xC5, 0xD7, 0xA5, 0x4F, 0x84,
                0xBE, 0xE5, 0x06, 0x62, 0xC5, 0xF1, 0xBB, 0xBB,
                0xE3, 0xBB, 0x4C, 0xFD, 0x38, 0x7B, 0xDA, 0x88,
                0x1F, 0x5C, 0x2E, 0x5A, 0x49, 0xEB, 0x47, 0xE2,
                0xCA, 0xAD, 0xCE, 0x73, 0xBB, 0x25, 0xF9, 0xED,
                0x47, 0x59, 0x4C, 0x42, 0xEF, 0xF0, 0xB1, 0x58,
                0x45, 0x58, 0x5D, 0xF3, 0xBC, 0x27, 0xBC, 0x60,
                0x19, 0xEB, 0xB1, 0xF9, 0x4F, 0x5D, 0xD1, 0x12,
            };
        }

        // Offset 0x3C (Jet4) or 0x3A (Jet3): sort order / code page ID
        // Common: 1033=en-US(1252), 1049=ru(1251), 1041=ja(932)
        int cpOffset = _jet4 ? 0x3C : 0x3A;
        int sortOrder = (hdr.Length > cpOffset + 1) ? Ru16(hdr, cpOffset) : 0;
        _codePage = (sortOrder >> 8) & 0xFF;
        if (_codePage == 0)
        {
            _codePage = 1252;  // default to Windows-1252 if unknown
        }

        try
        {
            _ansiEncoding = Encoding.GetEncoding(_codePage);
        }
        catch (ArgumentException)
        {
            _ansiEncoding = Encoding.UTF8;
            _codePage = 65001;
        }
        catch (NotSupportedException)
        {
            _ansiEncoding = Encoding.UTF8;
            _codePage = 65001;
        }

        if (_jet4)
        {
            // ── Jet4 / ACE (Access 2000 – 2019, .mdb + .accdb) ──────
            // Data page
            _dpTDefOff = 4;
            _dpNumRows = 12;   // extra 4-byte field after tdef_pg
            _dpRowsStart = 14;

            // TDEF: 8-byte header + 55-byte Jet4 block = 63 total
            _tdNumCols = 45;
            _tdNumRealIdx = 51;
            _tdBlockEnd = 63;

            // Column descriptor (25 bytes)
            _colDescSz = 25;
            _colTypeOff = 0;   // col_type  (1)
            _colVarOff = 7;   // offset_V  (2): 1+4+2
            _colFixedOff = 21;   // offset_F  (2): 1+4+2+2+2+2+2+1+1+4
            _colSzOff = 23;   // col_len   (2)
            _colFlagsOff = 15;   // bitmask   (1): 1+4+2+2+2+2+2
            _colNumOff = 5;   // col_num   (2)

            _realIdxEntrySz = 12;
            _numColsFldSz = 2;
            _varEntrySz = 2;
            _eodFldSz = 2;
            _varLenFldSz = 2;
        }
        else
        {
            // ── Jet3 (Access 97, .mdb) ────────────────────────────
            // Data page
            _dpTDefOff = 4;
            _dpNumRows = 8;
            _dpRowsStart = 10;

            // TDEF: 8-byte header + 35-byte Jet3 block = 43 total
            _tdNumCols = 25;
            _tdNumRealIdx = 31;
            _tdBlockEnd = 43;

            // Column descriptor (18 bytes)
            _colDescSz = 18;
            _colTypeOff = 0;   // col_type  (1)
            _colVarOff = 3;   // offset_V  (2): 1+2
            _colFixedOff = 14;   // offset_F  (2): 1+2+2+2+2+2+2+1
            _colSzOff = 16;   // col_len   (2)
            _colFlagsOff = 13;   // bitmask   (1)
            _colNumOff = 1;   // col_num   (2)

            _realIdxEntrySz = 8;
            _numColsFldSz = 1;
            _varEntrySz = 1;
            _eodFldSz = 1;
            _varLenFldSz = 1;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases resources used by this instance.
    /// </summary>
    /// <param name="disposing">True if called from <see cref="Dispose()"/>; false if called from a finalizer.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            _fs?.Dispose();
        }

        _disposed = true;
    }

    // ── Static helpers ────────────────────────────────────────────────
    private protected static void ReturnPage(byte[] page)
    {
        ArrayPool<byte>.Shared.Return(page);
    }

#pragma warning disable CA5351 // MD5 is required by the Jet4 RC4 key derivation spec
    /// <summary>
    /// Derives the RC4 key for a specific page: MD5(dbKey LE + pageNumber LE)[0..4].
    /// </summary>
    private protected static byte[] DeriveRc4PageKey(uint dbKey, uint pageNumber)
    {
        byte[] input = new byte[8];
        BitConverter.GetBytes(dbKey).CopyTo(input, 0);
        BitConverter.GetBytes(pageNumber).CopyTo(input, 4);
        using var md5 = System.Security.Cryptography.MD5.Create();
        byte[] hash = md5.ComputeHash(input);
        byte[] key = new byte[4];
        Buffer.BlockCopy(hash, 0, key, 0, 4);
        return key;
    }
#pragma warning restore CA5351

    /// <summary>In-place RC4 transform (encrypt and decrypt are the same operation).</summary>
    private protected static void Rc4Transform(byte[] data, int offset, int length, byte[] key)
    {
        byte[] s = new byte[256];
        for (int i = 0; i < 256; i++)
        {
            s[i] = (byte)i;
        }

        int j = 0;
        for (int i = 0; i < 256; i++)
        {
            j = (j + s[i] + key[i % key.Length]) & 0xFF;
            (s[i], s[j]) = (s[j], s[i]);
        }

        int x = 0, y = 0;
        for (int k = 0; k < length; k++)
        {
            x = (x + 1) & 0xFF;
            y = (y + s[x]) & 0xFF;
            (s[x], s[y]) = (s[y], s[x]);
            data[offset + k] ^= s[(s[x] + s[y]) & 0xFF];
        }
    }

    private protected static ushort Ru16(byte[] b, int o) =>
        (ushort)(b[o] | (b[o + 1] << 8));

    private protected static int Ri32(byte[] b, int o) =>
        b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);

    private protected static uint Ru32(byte[] b, int o) => (uint)Ri32(b, o);

    private protected static void Wu16(byte[] b, int o, int value)
    {
        b[o] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
    }

    private protected static void Wi32(byte[] b, int o, int value)
    {
        b[o] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
        b[o + 2] = (byte)((value >> 16) & 0xFF);
        b[o + 3] = (byte)((value >> 24) & 0xFF);
    }

    private protected static void WriteUInt24(byte[] b, int o, int value)
    {
        b[o] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
        b[o + 2] = (byte)((value >> 16) & 0xFF);
    }

    private protected static void WriteField(byte[] b, int o, int fieldSize, int value)
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

    /// <summary>Returns the expected byte size for a fixed-length column type.</summary>
    private protected static int FixedSize(byte type, int declaredSize)
    {
        switch (type)
        {
            case T_BYTE: return 1;
            case T_INT: return 2;
            case T_LONG: return 4;
            case T_MONEY: return 8;
            case T_FLOAT: return 4;
            case T_DOUBLE: return 8;
            case T_DATETIME: return 8;
            case T_GUID: return 16;
            case T_NUMERIC: return 17;
            default: return declaredSize > 0 ? declaredSize : 0;
        }
    }

    /// <summary>
    /// Decodes Jet4 text (UCS-2 / UTF-16LE).
    /// If data starts with the compressed-unicode marker 0xFF 0xFE, the
    /// JET4 compressed-string algorithm is applied first.
    /// </summary>
    /// <returns>The decoded string.</returns>
    private protected static string DecodeJet4Text(byte[] b, int start, int len)
    {
        if (len < 2)
        {
            return string.Empty;
        }

        if (b[start] == 0xFF && b[start + 1] == 0xFE)
        {
            return DecompressJet4(b, start + 2, len - 2);
        }

        // Plain UCS-2 LE — length must be even
        int evenLen = len & ~1;
        return evenLen > 0 ? Encoding.Unicode.GetString(b, start, evenLen) : string.Empty;
    }

    /// <summary>
    /// Decodes the JET4 "compressed unicode" encoding.
    /// A 0x00 byte toggles between 1-byte compressed (ASCII) and 2-byte
    /// uncompressed (UCS-2) mode.
    /// </summary>
    /// <returns>The decompressed string.</returns>
    private protected static string DecompressJet4(byte[] b, int start, int len)
    {
        var sb = new StringBuilder(len);
        bool compressed = true;
        int i = start, end = start + len;

        while (i < end)
        {
            if (compressed)
            {
                if (b[i] == 0x00)
                {
                    compressed = false;
                    i++;
                    continue;
                }

                _ = sb.Append((char)b[i++]);
            }
            else
            {
                if (i + 1 >= end)
                {
                    break;
                }

                if (b[i] == 0x00 && b[i + 1] == 0x00)
                {
                    compressed = true;
                    i += 2;
                    continue;
                }

                _ = sb.Append((char)(b[i] | (b[i + 1] << 8)));
                i += 2;
            }
        }

        return sb.ToString();
    }

    // ── Fixed-column string decoding ─────────────────────────────────

    private protected static string ReadFixedString(byte[] row, int start, byte type, int size)
    {
        try
        {
            switch (type)
            {
                case T_BYTE:
                    return row[start].ToString(System.Globalization.CultureInfo.InvariantCulture);
                case T_INT:
                    return ((short)Ru16(row, start)).ToString(System.Globalization.CultureInfo.InvariantCulture);
                case T_LONG:
                    return Ri32(row, start).ToString(System.Globalization.CultureInfo.InvariantCulture);
                case T_FLOAT:
                    return BitConverter.ToSingle(row, start).ToString("G", System.Globalization.CultureInfo.InvariantCulture);
                case T_DOUBLE:
                    return BitConverter.ToDouble(row, start).ToString("G", System.Globalization.CultureInfo.InvariantCulture);
                case T_DATETIME:
                    return DateTime.FromOADate(BitConverter.ToDouble(row, start)).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
                case T_MONEY:
                    return (BitConverter.ToInt64(row, start) / 10000.0m).ToString("F4", System.Globalization.CultureInfo.InvariantCulture);
                case T_GUID:
                    return new Guid(ReadGuidBytes(row, start)).ToString("B");
                case T_NUMERIC:
                    return ReadNumericString(row, start);
                default:
                    return BitConverter.ToString(row, start, Math.Min(size, 8));
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

    private protected static byte[] ReadGuidBytes(byte[] b, int start)
    {
        var guidBytes = new byte[16];
        Buffer.BlockCopy(b, start, guidBytes, 0, 16);
        return guidBytes;
    }

    private protected static string ReadNumericString(byte[] b, int start)
    {
        if (start + 16 >= b.Length)
        {
            return string.Empty;
        }

        byte scale = b[start + 1];
        bool negative = b[start + 2] != 0;
        uint lo = Ru32(b, start + 4);
        uint mid = Ru32(b, start + 8);
        uint hi = Ru32(b, start + 12);

        if (scale > 28)
        {
            return string.Empty;
        }

        try
        {
            return new decimal((int)lo, (int)mid, (int)hi, negative, scale).ToString("G", System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (OverflowException)
        {
            return string.Empty;
        }
    }

    // ── Page I/O ─────────────────────────────────────────────────────

    private protected byte[] ReadPage(long n)
    {
        var buf = ArrayPool<byte>.Shared.Rent(_pgSz);
        _ = _fs.Seek(n * _pgSz, SeekOrigin.Begin);

        // FileStream.Read is not guaranteed to return all bytes in one call
        int read = 0;
        while (read < _pgSz)
        {
            int got = _fs.Read(buf, read, _pgSz - read);
            if (got == 0)
            {
                break;
            }

            read += got;
        }

        // Jet3 XOR decryption: pages 1+ are masked with a fixed 128-byte key
        if (_jet3XorMask != null && n >= 1)
        {
            long fileOffset = n * _pgSz;
            for (int b = 0; b < _pgSz; b++)
            {
                buf[b] ^= _jet3XorMask[(int)((fileOffset + b - _pgSz) % _jet3XorMask.Length)];
            }
        }

        // Jet4 RC4 decryption: pages 1+ are decrypted with a per-page key
        if (_rc4DbKey.HasValue && n >= 1)
        {
            byte[] rc4Key = DeriveRc4PageKey(_rc4DbKey.Value, (uint)n);
            Rc4Transform(buf, 0, _pgSz, rc4Key);
        }

        return buf;
    }

    // ── TDEF parsing ─────────────────────────────────────────────────

    /// <summary>
    /// Concatenates the TDEF page chain starting at <paramref name="startPage"/>
    /// into a single byte array. Pages after the first have their 8-byte
    /// TDEF header stripped before appending.
    /// </summary>
    private protected byte[]? ReadTDefBytes(long startPage)
    {
        var parts = new List<byte[]>();
        var seen = new HashSet<long>();
        long pg = startPage;

        while (pg != 0 && !seen.Contains(pg))
        {
            _ = seen.Add(pg);
            byte[] p = ReadPage(pg);
            if (p[0] != 0x02)
            {
                ReturnPage(p);
                break;   // not a TDEF page
            }

            parts.Add(p);
            pg = Ru32(p, 4);           // next_pg (0 = end of chain)
        }

        if (parts.Count == 0)
        {
            return null;
        }

        if (parts.Count == 1)
        {
            var single = new byte[_pgSz];
            Buffer.BlockCopy(parts[0], 0, single, 0, _pgSz);
            ReturnPage(parts[0]);
            return single;
        }

        // Concatenate: full first page, then continuation pages minus 8-byte TDEF header
        int total = _pgSz;
        for (int i = 1; i < parts.Count; i++)
        {
            total += _pgSz - 8;
        }

        var result = new byte[total];
        Buffer.BlockCopy(parts[0], 0, result, 0, _pgSz);
        int pos = _pgSz;
        for (int i = 1; i < parts.Count; i++)
        {
            int len = _pgSz - 8;
            Buffer.BlockCopy(parts[i], 8, result, pos, len);
            pos += len;
        }

        for (int i = 0; i < parts.Count; i++)
        {
            ReturnPage(parts[i]);
        }

        return result;
    }

    private protected TableDef? ReadTableDef(long tdefPage)
    {
        byte[]? td = ReadTDefBytes(tdefPage);
        if (td == null || td.Length < _tdBlockEnd)
        {
            return null;
        }

        int numCols = Ru16(td, _tdNumCols);
        int numRealIdx = Ri32(td, _tdNumRealIdx);

        // Safety: corrupt or unusual TDEFs can report absurd index counts
        if (numRealIdx < 0 || numRealIdx > 1000)
        {
            numRealIdx = 0;
        }

        if (numCols > 4096)
        {
            return null;
        }

        // Column descriptors follow immediately after block + first real-idx entries
        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int namePos = colStart + (numCols * _colDescSz);

        if (namePos > td.Length)
        {
            return null;
        }

        var cols = new List<ColumnInfo>(numCols);
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * _colDescSz);
            if (o + _colDescSz > td.Length)
            {
                break;
            }

            cols.Add(new ColumnInfo
            {
                Type = td[o + _colTypeOff],
                ColNum = Ru16(td, o + _colNumOff),
                VarIdx = Ru16(td, o + _colVarOff),
                FixedOff = Ru16(td, o + _colFixedOff),
                Size = Ru16(td, o + _colSzOff),
                Flags = td[o + _colFlagsOff],
            });
        }

        // Column names follow directly after all descriptors (in TDEF / descriptor order).
        // Names MUST be read before sorting so each name maps to the correct descriptor.
        for (int i = 0; i < cols.Count; i++)
        {
            int nameLen = ReadColumnName(td, ref namePos, out string name);
            if (nameLen < 0)
            {
                break;
            }

            cols[i].Name = name;
        }

        // Sort by col_num AFTER names are assigned.
        cols.Sort((a, b) => a.ColNum.CompareTo(b.ColNum));

        // Detect deleted-column gaps: if ColNum sequence has gaps, flag it
        bool hasDeletedColumns = cols.Count >= 2
            && cols[cols.Count - 1].ColNum - cols[0].ColNum != cols.Count - 1;

        return new TableDef
        {
            Columns = cols,
            RowCount = td.Length > 20 ? (long)Ru32(td, 16) : 0,
            HasDeletedColumns = hasDeletedColumns,
        };
    }

    /// <summary>
    /// Reads a single column name from the TDEF byte array at <paramref name="pos"/>,
    /// advancing <paramref name="pos"/> past the name bytes.
    /// Returns the byte length consumed, or -1 if the name extends beyond <paramref name="td"/>.
    /// </summary>
    private protected int ReadColumnName(byte[] td, ref int pos, out string name)
    {
        name = string.Empty;
        if (pos >= td.Length)
        {
            return -1;
        }

        if (_jet4)
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

            name = Encoding.Unicode.GetString(td, pos, len);
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

            name = _ansiEncoding.GetString(td, pos, len);
            pos += len;
            return len + 1;
        }
    }

    // ── Page write I/O ───────────────────────────────────────────────

    private protected void WritePage(long pageNumber, byte[] page)
    {
        _ = _fs.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
        _fs.Write(page, 0, _pgSz);
        _fs.Flush();
    }

    private protected long AppendPage(byte[] page)
    {
        long pageNumber = _fs.Length / _pgSz;
        _ = _fs.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
        _fs.Write(page, 0, _pgSz);
        _fs.Flush();
        return pageNumber;
    }

    // ── Disposed check ───────────────────────────────────────────────

    private protected void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(GetType().Name);
        }
    }

    // ── Catalog access ───────────────────────────────────────────────

    /// <summary>Finds a catalog entry by name (case-insensitive).</summary>
    private protected CatalogEntry GetCatalogEntry(string tableName)
    {
        return GetUserTables().Find(e =>
            string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Returns all user-visible table names and their TDEF page numbers.</summary>
    private protected abstract List<CatalogEntry> GetUserTables();

    // ── Table page enumeration ───────────────────────────────────────

    /// <summary>
    /// Yields data pages belonging to the table whose TDEF starts at <paramref name="tdefPage"/>.
    /// Override in derived classes to use cached page reads.
    /// </summary>
    private protected virtual IEnumerable<byte[]> EnumerateTablePages(long tdefPage)
    {
        long total = _fs.Length / _pgSz;
        for (long p = 3; p < total; p++)
        {
            byte[] page = ReadPage(p);
            if (page[0] != 0x01)
            {
                ReturnPage(page);
                continue;
            }

            if ((long)Ri32(page, _dpTDefOff) != tdefPage)
            {
                ReturnPage(page);
                continue;
            }

            yield return page;
        }
    }

    /// <summary>
    /// Yields the bounds (row index, start offset, size) of every live (non-deleted, non-overflow)
    /// row on the given data <paramref name="page"/>.
    /// </summary>
    private protected IEnumerable<RowBound> EnumerateLiveRowBounds(byte[] page)
    {
        int numRows = Ru16(page, _dpNumRows);
        if (numRows == 0)
        {
            yield break;
        }

        var rawOffsets = new int[numRows];
        for (int r = 0; r < numRows; r++)
        {
            rawOffsets[r] = Ru16(page, _dpRowsStart + (r * 2));
        }

        var positions = new int[numRows];
        int posCount = 0;
        for (int r = 0; r < numRows; r++)
        {
            int pos = rawOffsets[r] & 0x1FFF;
            if (pos > 0 && pos < _pgSz)
            {
                positions[posCount++] = pos;
            }
        }

        Array.Sort(positions, 0, posCount);

        for (int r = 0; r < numRows; r++)
        {
            int raw = rawOffsets[r];
            if ((raw & 0xC000) != 0)
            {
                continue;
            }

            int rowStart = raw & 0x1FFF;
            int rowEnd = _pgSz - 1;
            int searchIdx = Array.BinarySearch(positions, 0, posCount, rowStart);
            int nextIdx = searchIdx >= 0 ? searchIdx + 1 : ~searchIdx;
            if (nextIdx < posCount)
            {
                rowEnd = positions[nextIdx] - 1;
            }

            yield return new RowBound(r, rowStart, rowEnd - rowStart + 1);
        }
    }

    // ── Inner types ──────────────────────────────────────────────────

    private protected readonly record struct RowBound
    {
        public RowBound(int rowIndex, int rowStart, int rowSize)
        {
            RowIndex = rowIndex;
            RowStart = rowStart;
            RowSize = rowSize;
        }

        public int RowIndex { get; }

        public int RowStart { get; }

        public int RowSize { get; }
    }

    private protected sealed record CatalogEntry
    {
        public string Name { get; set; } = string.Empty;

        public long TDefPage { get; set; }
    }
}
