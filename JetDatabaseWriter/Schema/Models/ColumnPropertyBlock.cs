namespace JetDatabaseWriter.Schema.Models;

using System;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Enums;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Parsed representation of an <c>MSysObjects.LvProp</c> blob (<c>KKD\0</c> / <c>MR2\0</c>).
/// Read-only model produced by <see cref="Parse(byte[], DatabaseFormat)"/>; round-trip
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
    private const uint MagicMr2 = 0x0032524D;

    /// <summary>
    /// &quot;KKD\0&quot; little-endian.
    /// </summary>
    private const uint MagicKkd = 0x00444B4B;

    /// <summary>Gets the database format the blob was parsed against.</summary>
    public DatabaseFormat Format { get; private init; }

    /// <summary>Gets the parsed property targets, in source order. Targets may include the table itself and individual columns.</summary>
    public IReadOnlyList<ColumnPropertyTarget> Targets { get; private init; } = [];

    /// <summary>Gets opaque chunks the parser did not recognise. Preserved verbatim for forward-compatible round-trip.</summary>
    public IReadOnlyList<ColumnPropertyUnknownChunk> UnknownChunks { get; private init; } = [];

    /// <summary>
    /// Parses an <c>LvProp</c> blob. Returns <see langword="null"/> for null or
    /// empty input, and for blobs whose magic header is unrecognised. Never throws
    /// for malformed payloads — truncates at the first inconsistency and returns
    /// what was parsed up to that point.
    /// </summary>
    /// <param name="blob">Raw blob bytes (entire <c>LvProp</c> cell payload, magic included).</param>
    /// <param name="format">Database format — selects Jet3 vs Jet4 string encoding.</param>
    /// <returns>Parsed block, or <see langword="null"/> if the blob is empty or has an unknown magic.</returns>
    public static ColumnPropertyBlock? Parse(byte[]? blob, DatabaseFormat format)
    {
        if (blob is null || blob.Length < 4)
        {
            return null;
        }

        uint magic = Ru32(blob, 0);
        if (magic != MagicMr2 && magic != MagicKkd)
        {
            return null;
        }

        bool isJet3 = magic == MagicKkd;
        Encoding stringEncoding = isJet3
            ? Encoding.GetEncoding(1252) // Jet3 fallback until per-database codepages flow into LvProp parsing.
            : Encoding.Unicode;

        var nameTable = new List<string>();
        var targets = new List<ColumnPropertyTarget>();
        var unknown = new List<ColumnPropertyUnknownChunk>();

        int pos = 4;
        while (pos + 6 <= blob.Length)
        {
            uint chunkLen = Ru32(blob, pos);
            var chunkType = (ColumnPropertyChunkType)Ru16(blob, pos + 4);

            if (chunkLen < 6 || pos + chunkLen > (uint)blob.Length)
            {
                break; // truncated / malformed — stop, return what we have
            }

            int payloadStart = pos + 6;
            int payloadLen = (int)chunkLen - 6;

            switch (chunkType)
            {
                case ColumnPropertyChunkType.NamePool:
                    nameTable.Clear();
                    ReadNamePool(blob, payloadStart, payloadLen, stringEncoding, isJet3, nameTable);
                    break;

                case ColumnPropertyChunkType.PropertyBlock:
                case ColumnPropertyChunkType.PropertyBlockAlt1:
                case ColumnPropertyChunkType.PropertyBlockAlt2:
                    ColumnPropertyTarget? target = ReadPropertyBlock(
                        blob, payloadStart, payloadLen, chunkType, nameTable, stringEncoding, isJet3);
                    if (target is not null)
                    {
                        targets.Add(target);
                    }

                    break;

                default:
                    var opaque = new byte[payloadLen];
                    Buffer.BlockCopy(blob, payloadStart, opaque, 0, payloadLen);
                    unknown.Add(new ColumnPropertyUnknownChunk((ushort)chunkType, opaque));
                    break;
            }

            pos += (int)chunkLen;
        }

        return new ColumnPropertyBlock
        {
            Format = format,
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

    private static void ReadNamePool(
        byte[] blob, int start, int length, Encoding stringEncoding, bool isJet3, List<string> dest)
    {
        int pos = start;
        int end = start + length;
        while (pos + 2 <= end)
        {
            int nameLen = Ru16(blob, pos);
            pos += 2;
            if (nameLen < 0 || pos + nameLen > end)
            {
                return;
            }

            // Jet3 may use a uint8 length prefix for property names; mdbtools uses uint16
            // uniformly. See `format-notes.md` §4.4.
            _ = isJet3;

            dest.Add(stringEncoding.GetString(blob, pos, nameLen));
            pos += nameLen;
        }
    }

    private static ColumnPropertyTarget? ReadPropertyBlock(
        byte[] blob,
        int start,
        int length,
        ColumnPropertyChunkType chunkType,
        List<string> nameTable,
        Encoding stringEncoding,
        bool isJet3)
    {
        // Header (per mdbtools mdb_read_props):
        //   [0..3] uint32 of unclear purpose — read and ignored.
        //   [4..5] uint16 targetNameLen
        //   [6..6+targetNameLen] target name (column / table)
        //   then property entries until end of payload.
        if (length < 6)
        {
            return null;
        }

        int pos = start + 4;
        int end = start + length;
        int targetNameLen = Ru16(blob, pos);
        pos += 2;
        if (pos + targetNameLen > end)
        {
            return null;
        }

        string targetName = stringEncoding.GetString(blob, pos, targetNameLen);
        pos += targetNameLen;
        _ = isJet3;

        var entries = new List<ColumnPropertyEntry>();
        while (pos + 8 <= end)
        {
            int entryLen = Ru16(blob, pos);
            if (entryLen < 8 || pos + entryLen > end)
            {
                break;
            }

            byte ddlFlag = blob[pos + 2];
            byte dataType = blob[pos + 3];
            int nameIndex = Ru16(blob, pos + 4);
            int valueLen = Ru16(blob, pos + 6);

            if (8 + valueLen > entryLen || nameIndex >= nameTable.Count)
            {
                break;
            }

            var value = new byte[valueLen];
            Buffer.BlockCopy(blob, pos + 8, value, 0, valueLen);

            entries.Add(new ColumnPropertyEntry(
                nameTable[nameIndex],
                dataType,
                ddlFlag,
                value));

            pos += entryLen;
        }

        return new ColumnPropertyTarget(targetName, chunkType, entries);
    }
}
