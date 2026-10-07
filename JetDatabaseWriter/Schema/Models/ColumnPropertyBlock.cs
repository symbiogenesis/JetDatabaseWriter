namespace JetDatabaseWriter.Schema.Models;

using System;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Parsed representation of an <c>MSysObjects.LvProp</c> blob (<c>KKD\0</c> / <c>MR2\0</c>).
/// Read-only model produced by <see cref="Parse(byte[], JetFormat)"/>; round-trip
/// (re-emit unknown chunks unchanged) is supported via <see cref="UnknownChunks"/>.
/// </summary>
/// <remarks>
/// On-disk layout per <see href="docs/design/persisted-column-properties-format-notes.md" />
/// — derived from mdbtools <c>src/libmdb/props.c</c>. The format-notes document
/// supersedes §3.2 of the parent design doc.
/// </remarks>
internal sealed class ColumnPropertyBlock
{
    /// <summary>
    /// &quot;MR2\0&quot; little-endian.
    /// </summary>
    internal const uint MagicMr2 = 0x0032524D;

    /// <summary>
    /// &quot;KKD\0&quot; little-endian.
    /// </summary>
    internal const uint MagicKkd = 0x00444B4B;

    /// <summary>Gets the database format the blob was parsed against.</summary>
    public JetFormat Format { get; private init; } = null!;

    /// <summary>
    /// Gets the parsed property targets, in source order: one per column that has
    /// properties, named after it, and the table itself, under an empty name
    /// (see <see cref="FindTableTarget"/>).
    /// </summary>
    public IReadOnlyList<ColumnPropertyTarget> Targets { get; private init; } = [];

    /// <summary>Gets opaque chunks the parser did not recognise. Preserved verbatim for forward-compatible round-trip.</summary>
    public IReadOnlyList<ColumnPropertyUnknownChunk> UnknownChunks { get; private init; } = [];

    /// <summary>Gets the recognized stored signature independently of the outer database generation.</summary>
    internal uint Magic { get; private init; }

    /// <summary>Gets the text encoding selected by the stored property signature.</summary>
    internal Encoding? TextEncoding { get; private init; }

    /// <summary>
    /// Returns a block with no targets and no unknown chunks, which serializes to
    /// no <c>LvProp</c> value at all.
    /// </summary>
    /// <param name="format">The database format the block belongs to.</param>
    public static ColumnPropertyBlock Empty(JetFormat format) => new() { Format = format };

    /// <summary>
    /// Parses an <c>LvProp</c> blob. Only a null value means absent properties.
    /// A present malformed blob throws instead of returning partial metadata.
    /// </summary>
    /// <param name="blob">Raw blob bytes (entire <c>LvProp</c> cell payload, magic included).</param>
    /// <param name="format">Database format — selects Jet3 vs Jet4 string encoding.</param>
    /// <returns>Parsed block, or <see langword="null"/> if the property value is absent.</returns>
    /// <exception cref="JetCorruptDataException">A present property blob is malformed.</exception>
    public static ColumnPropertyBlock? Parse(byte[]? blob, JetFormat format)
    {
        if (blob is null)
        {
            return null;
        }

        if (blob.Length < sizeof(uint))
        {
            throw Corrupt(0, "Truncated property magic");
        }

        uint magic = Ru32(blob, 0);
        if (magic is not MagicMr2 and not MagicKkd)
        {
            throw Corrupt(0, "Unrecognized property magic");
        }

        bool isJet3 = magic == MagicKkd;
        Encoding stringEncoding = Encoding.GetEncoding(
            isJet3 ? format.CodePage : Encoding.Unicode.CodePage,
            EncoderFallback.ExceptionFallback,
            DecoderFallback.ExceptionFallback);

        var nameTable = new List<string>();
        var targets = new List<ColumnPropertyTarget>();
        var unknown = new List<ColumnPropertyUnknownChunk>();

        int pos = 4;
        while (pos < blob.Length)
        {
            if (blob.Length - pos < 6)
            {
                throw Corrupt(pos, "Truncated chunk header");
            }

            uint chunkLen = Ru32(blob, pos);
            var chunkType = (ColumnPropertyChunkType)Ru16(blob, pos + 4);

            if (chunkLen < 6 || chunkLen > (uint)(blob.Length - pos))
            {
                throw Corrupt(pos, "Invalid chunk length");
            }

            int payloadStart = pos + 6;
            int payloadLen = (int)chunkLen - 6;

            switch (chunkType)
            {
                case ColumnPropertyChunkType.NamePool:
                    nameTable.Clear();
                    ReadNamePool(blob, payloadStart, payloadLen, stringEncoding, nameTable);
                    break;

                case ColumnPropertyChunkType.PropertyBlock:
                case ColumnPropertyChunkType.PropertyBlockAlt1:
                case ColumnPropertyChunkType.PropertyBlockAlt2:
                    ColumnPropertyTarget target = ReadPropertyBlock(
                        blob, payloadStart, payloadLen, chunkType, nameTable, stringEncoding);
                    targets.Add(target);

                    break;

                default:
                    byte[] opaque = new byte[payloadLen];
                    Buffer.BlockCopy(blob, payloadStart, opaque, 0, payloadLen);
                    unknown.Add(new ColumnPropertyUnknownChunk((ushort)chunkType, opaque));
                    break;
            }

            pos += (int)chunkLen;
        }

        return new ColumnPropertyBlock
        {
            Format = format,
            Magic = magic,
            TextEncoding = stringEncoding,
            Targets = targets,
            UnknownChunks = unknown,
        };
    }

    /// <summary>
    /// Returns the property target whose <see cref="ColumnPropertyTarget.Name"/>
    /// matches <paramref name="name"/> case-insensitively, or <see langword="null"/>
    /// if no such target exists.
    /// </summary>
    /// <param name="name">The name.</param>
    public ColumnPropertyTarget? FindTarget(string name)
    {
        foreach (ColumnPropertyTarget t in this.Targets)
        {
            if (string.Equals(t.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return t;
            }
        }

        return null;
    }

    /// <summary>
    /// Returns the table-level target: the first target with an empty name, or
    /// <see langword="null"/> when there is none. Access writes it as a
    /// property block of chunk type <c>0x00</c> that can sit anywhere in the
    /// blob: usually first in Jet3 and Jet4 blobs and usually last in ACCDB
    /// blobs, but last of four in compIndexTestV2000's <c>Table1</c> and third of
    /// eighteen in calcFieldTestV2010's <c>Table1</c>. Never assume a position.
    /// It is never matched by the table's name.
    /// </summary>
    public ColumnPropertyTarget? FindTableTarget()
    {
        foreach (ColumnPropertyTarget t in this.Targets)
        {
            if (t.Name.Length == 0)
            {
                return t;
            }
        }

        return null;
    }

    /// <summary>
    /// Serializes the block for <paramref name="format"/> through
    /// <see cref="ColumnPropertyBlockBuilder"/>. Returns <see langword="null"/>
    /// when the block has no targets and no unknown chunks.
    /// </summary>
    /// <param name="format">The database format the bytes are written for.</param>
    public byte[]? ToBytes(JetFormat format) => ColumnPropertyBlockBuilder.FromBlock(this).ToBytes(format);

    private static void ReadNamePool(
        byte[] blob, int start, int length, Encoding stringEncoding, List<string> dest)
    {
        int pos = start;
        int end = start + length;
        while (pos < end)
        {
            if (end - pos < sizeof(ushort))
            {
                throw Corrupt(pos, "Truncated property name length");
            }

            int nameLen = Ru16(blob, pos);
            pos += sizeof(ushort);
            if (nameLen > end - pos)
            {
                throw Corrupt(pos, "Truncated property name");
            }

            dest.Add(ReadText(blob, pos, nameLen, stringEncoding));
            pos += nameLen;
        }
    }

    private static ColumnPropertyTarget ReadPropertyBlock(
        byte[] blob,
        int start,
        int length,
        ColumnPropertyChunkType chunkType,
        List<string> nameTable,
        Encoding stringEncoding)
    {
        // The first four bytes are opaque; the target name follows them.
        if (length < 6)
        {
            throw Corrupt(start, "Truncated property target header");
        }

        int pos = start + sizeof(uint);
        int end = start + length;
        int targetNameLen = Ru16(blob, pos);
        pos += sizeof(ushort);
        if (targetNameLen > end - pos)
        {
            throw Corrupt(pos, "Truncated property target name");
        }

        string targetName = ReadText(blob, pos, targetNameLen, stringEncoding);
        pos += targetNameLen;
        var entries = new List<ColumnPropertyEntry>();
        while (pos < end)
        {
            if (end - pos < 8)
            {
                throw Corrupt(pos, "Truncated property entry header");
            }

            int entryLen = Ru16(blob, pos);
            if (entryLen < 8 || entryLen > end - pos)
            {
                throw Corrupt(pos, "Invalid property entry length");
            }

            byte ddlFlag = blob[pos + 2];
            var dataType = (ColumnType)blob[pos + 3];
            int nameIndex = Ru16(blob, pos + 4);
            int valueLen = Ru16(blob, pos + 6);
            if (valueLen > entryLen - 8 || nameIndex >= nameTable.Count)
            {
                throw Corrupt(pos, "Invalid property value length or name index");
            }

            if (dataType is ColumnType.TextType or ColumnType.MemoType)
            {
                _ = ReadText(blob, pos + 8, valueLen, stringEncoding);
            }

            byte[] value = new byte[valueLen];
            Buffer.BlockCopy(blob, pos + 8, value, 0, valueLen);
            entries.Add(new ColumnPropertyEntry(nameTable[nameIndex], dataType, ddlFlag, value));
            pos += entryLen;
        }

        return new ColumnPropertyTarget(targetName, chunkType, entries) { TextEncoding = stringEncoding };
    }

    private static string ReadText(byte[] blob, int start, int length, Encoding encoding)
    {
        try
        {
            return encoding.GetString(blob, start, length);
        }
        catch (DecoderFallbackException exception)
        {
            throw new JetCorruptDataException(
                JetErrorCode.CorruptCatalog,
                $"Malformed MSysObjects.LvProp text at byte offset {start}.",
                new JetErrorInfo { TableName = "MSysObjects", ColumnName = "LvProp", Reason = "Invalid property text encoding" },
                exception);
        }
    }

    private static JetCorruptDataException Corrupt(int offset, string reason) => new(
        JetErrorCode.CorruptCatalog,
        $"Malformed MSysObjects.LvProp at byte offset {offset}: {reason}.",
        new JetErrorInfo { TableName = "MSysObjects", ColumnName = "LvProp", Reason = reason });
}
