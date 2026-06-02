namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Mutable builder + serializer for <c>MSysObjects.LvProp</c> blobs
/// (<c>MR2\0</c> / <c>KKD\0</c>).
/// </summary>
/// <remarks>
/// <para>
/// Mirrors the on-disk layout consumed by <see cref="ColumnPropertyBlock.Parse(byte[], DatabaseFormat)"/>;
/// see <see href="docs/design/persisted-column-properties-format-notes.md" /> §2 for the
/// authoritative byte layout.
/// </para>
/// <para>
/// Round-trip guarantee: an unmodified blob parsed via
/// <see cref="ColumnPropertyBlock.Parse(byte[], DatabaseFormat)"/> and re-serialized via
/// <see cref="FromBlock(ColumnPropertyBlock)"/> + <see cref="ToBytes(DatabaseFormat)"/>
/// reproduces a byte stream that the parser interprets identically (entries, targets,
/// and unknown chunks all preserved). Byte-identity with the original is *not*
/// guaranteed because the inner property-block header carries opaque bytes that the
/// parser discards.
/// </para>
/// </remarks>
internal sealed class ColumnPropertyBlockBuilder
{
    private const int MagicLength = 4;
    private const int ChunkHeaderLength = sizeof(uint) + sizeof(ushort);
    private const int PropertyBlockTargetHeaderLength = sizeof(uint) + sizeof(ushort);
    private const int PropertyEntryHeaderLength = sizeof(ushort) + sizeof(byte) + sizeof(byte) + sizeof(ushort) + sizeof(ushort);

    /// <summary>Gets the mutable list of property targets in emission order. The first target is conventionally the table itself.</summary>
    public List<ColumnPropertyTargetBuilder> Targets { get; } = [];

    /// <summary>Gets the mutable list of opaque chunks to re-emit verbatim (forward-compat).</summary>
    public List<ColumnPropertyUnknownChunk> UnknownChunks { get; } = [];

    /// <summary>
    /// Gets a value indicating whether the builder would emit zero targets and
    /// zero unknown chunks — i.e. the resulting blob would carry only the magic
    /// header and is therefore not worth persisting.
    /// </summary>
    public bool IsEmpty => this.Targets.Count == 0 && this.UnknownChunks.Count == 0;

    /// <summary>
    /// Constructs a builder seeded with the parsed targets and unknown chunks of an
    /// existing block — the entry point for round-trip preservation.
    /// </summary>
    /// <param name="block">The block.</param>
    public static ColumnPropertyBlockBuilder FromBlock(ColumnPropertyBlock block)
    {
        Guard.NotNull(block, nameof(block));
        var b = new ColumnPropertyBlockBuilder();
        foreach (ColumnPropertyTarget t in block.Targets)
        {
            var tb = new ColumnPropertyTargetBuilder
            {
                Name = t.Name,
                ChunkType = t.ChunkType,
            };
            foreach (ColumnPropertyEntry e in t.Entries)
            {
                tb.Entries.Add(new ColumnPropertyEntryBuilder
                {
                    Name = e.Name,
                    DataType = e.DataType,
                    DdlFlag = e.DdlFlag,
                    Value = (byte[])e.Value.Clone(),
                });
            }

            b.Targets.Add(tb);
        }

        foreach (ColumnPropertyUnknownChunk u in block.UnknownChunks)
        {
            b.UnknownChunks.Add(new ColumnPropertyUnknownChunk(u.ChunkType, (byte[])u.Payload.Clone()));
        }

        return b;
    }

    /// <summary>
    /// Adds (or returns an existing) target by case-insensitive name. New targets
    /// default to chunk-type <c>0x01</c> (the property-block subtype DAO emits for new columns).
    /// </summary>
    /// <param name="name">The name.</param>
    public ColumnPropertyTargetBuilder GetOrAddTarget(string name)
    {
        Guard.NotNullOrEmpty(name, nameof(name));
        foreach (ColumnPropertyTargetBuilder t in this.Targets)
        {
            if (string.Equals(t.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return t;
            }
        }

        var nt = new ColumnPropertyTargetBuilder { Name = name, ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1 };
        this.Targets.Add(nt);
        return nt;
    }

    /// <summary>
    /// Removes the target whose name matches <paramref name="name"/> case-insensitively.
    /// No-op if no such target exists. Returns <see langword="true"/> when a target was removed.
    /// </summary>
    /// <param name="name">The name.</param>
    public bool RemoveTarget(string name)
    {
        for (int i = 0; i < this.Targets.Count; i++)
        {
            if (string.Equals(this.Targets[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                this.Targets.RemoveAt(i);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Renames the target whose current name matches <paramref name="oldName"/> to
    /// <paramref name="newName"/>. No-op if no such target exists.
    /// </summary>
    /// <param name="oldName">The old name.</param>
    /// <param name="newName">The new name.</param>
    public void RenameTarget(string oldName, string newName)
    {
        Guard.NotNullOrEmpty(newName, nameof(newName));
        foreach (ColumnPropertyTargetBuilder t in this.Targets)
        {
            if (string.Equals(t.Name, oldName, StringComparison.OrdinalIgnoreCase))
            {
                t.Name = newName;
                return;
            }
        }
    }

    /// <summary>
    /// Serializes to bytes. Returns <see langword="null"/> when the builder is empty
    /// (no targets, no unknown chunks) to signal that no <c>LvProp</c> cell is needed.
    /// </summary>
    /// <param name="format">Database format. Selects Jet3 codepage vs Jet4 UTF-16LE string encoding.</param>
    /// <exception cref="InvalidOperationException">If a chunk would exceed the on-disk uint16 / uint32 length limits.</exception>
    public byte[]? ToBytes(DatabaseFormat format)
    {
        if (this.IsEmpty)
        {
            return null;
        }

        bool isJet3 = format == DatabaseFormat.Jet3Mdb;
        Encoding stringEncoding = isJet3 ? Encoding.GetEncoding(1252) : Encoding.Unicode;

        // Build the name pool from every distinct entry name encountered, in stable
        // first-seen order. The parser indexes by uint16 so we cap at 65,535 names entries.
        var nameToIndex = new Dictionary<string, ushort>(StringComparer.Ordinal);
        var nameOrder = new List<string>();
        foreach (ColumnPropertyTargetBuilder t in this.Targets)
        {
            foreach (ColumnPropertyEntryBuilder e in t.Entries)
            {
                if (!nameToIndex.ContainsKey(e.Name))
                {
                    if (nameOrder.Count >= ushort.MaxValue)
                    {
                        throw new InvalidOperationException("Property name pool exceeds the uint16 index limit.");
                    }

                    nameToIndex[e.Name] = (ushort)nameOrder.Count;
                    nameOrder.Add(e.Name);
                }
            }
        }

        byte[] namePoolPayload = BuildNamePoolPayload(nameOrder, stringEncoding);

        byte[][] propertyBlockPayloads = new byte[this.Targets.Count][];
        int totalLength = MagicLength;
        totalLength = AddChunkLength(totalLength, namePoolPayload.Length);

        for (int targetIndex = 0; targetIndex < this.Targets.Count; targetIndex++)
        {
            propertyBlockPayloads[targetIndex] = BuildPropertyBlockPayload(this.Targets[targetIndex], nameToIndex, stringEncoding);
            totalLength = AddChunkLength(totalLength, propertyBlockPayloads[targetIndex].Length);
        }

        foreach (ColumnPropertyUnknownChunk unknownChunk in this.UnknownChunks)
        {
            totalLength = AddChunkLength(totalLength, unknownChunk.Payload.Length);
        }

        byte[] blob = new byte[totalLength];
        int offset = 0;
        WriteMagic(blob, ref offset, isJet3);

        // Name-pool chunk (always first; mdbtools requires it before property blocks).
        WriteChunk(blob, ref offset, ColumnPropertyChunkType.NamePool, namePoolPayload);

        // Property-block chunks.
        for (int targetIndex = 0; targetIndex < this.Targets.Count; targetIndex++)
        {
            WriteChunk(blob, ref offset, this.Targets[targetIndex].ChunkType, propertyBlockPayloads[targetIndex]);
        }

        // Unknown chunks (preserved verbatim — re-emit at the end so they don't shadow
        // the name pool the parser depends on).
        foreach (ColumnPropertyUnknownChunk unknownChunk in this.UnknownChunks)
        {
            WriteChunk(blob, ref offset, (ColumnPropertyChunkType)unknownChunk.ChunkType, unknownChunk.Payload);
        }

        return blob;
    }

    private static byte[] BuildNamePoolPayload(List<string> names, Encoding encoding)
    {
        int[] byteCounts = new int[names.Count];
        int payloadLength = 0;
        for (int nameIndex = 0; nameIndex < names.Count; nameIndex++)
        {
            int byteCount = GetUInt16StringByteCount(encoding, names[nameIndex], "Property name");
            byteCounts[nameIndex] = byteCount;
            payloadLength = AddPayloadLength(payloadLength, sizeof(ushort) + byteCount, "name-pool payload");
        }

        byte[] payload = new byte[payloadLength];
        int offset = 0;
        for (int nameIndex = 0; nameIndex < names.Count; nameIndex++)
        {
            int byteCount = byteCounts[nameIndex];
            WriteLengthPrefixedEncodedString(payload, ref offset, encoding, names[nameIndex], byteCount);
        }

        return payload;
    }

    private static byte[] BuildPropertyBlockPayload(
        ColumnPropertyTargetBuilder target,
        Dictionary<string, ushort> nameToIndex,
        Encoding encoding)
    {
        int targetNameByteCount = GetUInt16StringByteCount(encoding, target.Name, "Property target name");
        int payloadLength = PropertyBlockTargetHeaderLength + targetNameByteCount;
        int[] entryLengths = new int[target.Entries.Count];
        for (int entryIndex = 0; entryIndex < target.Entries.Count; entryIndex++)
        {
            ColumnPropertyEntryBuilder entry = target.Entries[entryIndex];
            int valueLength = entry.Value.Length;
            int entryLength = PropertyEntryHeaderLength + valueLength;
            if (entryLength > ushort.MaxValue)
            {
                throw new InvalidOperationException($"Property entry '{entry.Name}' value is {valueLength} bytes; max supported is {ushort.MaxValue - PropertyEntryHeaderLength}.");
            }

            entryLengths[entryIndex] = entryLength;
            payloadLength = AddPayloadLength(payloadLength, entryLength, "property-block payload");
        }

        byte[] payload = new byte[payloadLength];
        int offset = 0;

        // Inner header — first 4 bytes are opaque per mdbtools (read & discarded).
        // DAO writes the byte count through the target-name field, not the whole
        // payload length: sizeof(uint32) + sizeof(uint16) + targetNameBytes.
        WriteUInt32(payload, ref offset, (uint)(PropertyBlockTargetHeaderLength + targetNameByteCount));
        WriteLengthPrefixedEncodedString(payload, ref offset, encoding, target.Name, targetNameByteCount);

        for (int entryIndex = 0; entryIndex < target.Entries.Count; entryIndex++)
        {
            ColumnPropertyEntryBuilder entry = target.Entries[entryIndex];
            if (!nameToIndex.TryGetValue(entry.Name, out ushort nameIndex))
            {
                throw new InvalidOperationException($"Entry name '{entry.Name}' was not registered in the name pool.");
            }

            int entryLength = entryLengths[entryIndex];
            int valueLength = entry.Value.Length;
            WriteUInt16(payload, ref offset, (ushort)entryLength);
            payload[offset++] = entry.DdlFlag;
            payload[offset++] = (byte)entry.DataType;
            WriteUInt16(payload, ref offset, nameIndex);
            WriteUInt16(payload, ref offset, (ushort)valueLength);
            WriteBytes(payload, ref offset, entry.Value);
        }

        return payload;
    }

    private static int AddChunkLength(int totalLength, int payloadLength) => AddLength(totalLength, GetChunkLength(payloadLength), "Property block blob", null);

    private static int AddPayloadLength(int payloadLength, int additionalLength, string payloadDescription) => AddLength(payloadLength, additionalLength, "Property", payloadDescription);

    private static int AddLength(int length, long additionalLength, string valueDescription, string? detail)
    {
        long newLength = length + additionalLength;
        if (newLength > int.MaxValue)
        {
            string description = detail is null ? valueDescription : $"{valueDescription} {detail}";
            throw new InvalidOperationException($"{description} would be {newLength} bytes, exceeding the supported array length.");
        }

        return (int)newLength;
    }

    private static int GetUInt16StringByteCount(Encoding encoding, string value, string valueDescription)
    {
        int byteCount = encoding.GetByteCount(value);
        if (byteCount > ushort.MaxValue)
        {
            throw new InvalidOperationException($"{valueDescription} '{value}' encodes to {byteCount} bytes, exceeding the uint16 length limit.");
        }

        return byteCount;
    }

    private static void WriteChunk(byte[] blob, ref int offset, ColumnPropertyChunkType chunkType, ReadOnlySpan<byte> payload)
    {
        long chunkLength = GetChunkLength(payload.Length);
        WriteUInt32(blob, ref offset, (uint)chunkLength);
        WriteUInt16(blob, ref offset, (ushort)chunkType);
        WriteBytes(blob, ref offset, payload);
    }

    private static long GetChunkLength(int payloadLength)
    {
        long chunkLength = ChunkHeaderLength + (long)payloadLength;
        if (chunkLength > uint.MaxValue)
        {
            throw new InvalidOperationException($"Property chunk would be {chunkLength} bytes, exceeding the uint32 length limit.");
        }

        return chunkLength;
    }

    private static void WriteMagic(byte[] blob, ref int offset, bool isJet3)
    {
        ReadOnlySpan<byte> magic = isJet3 ? "KKD\0"u8 : "MR2\0"u8;
        WriteBytes(blob, ref offset, magic);
    }

    private static void WriteLengthPrefixedEncodedString(
        byte[] buffer,
        ref int offset,
        Encoding encoding,
        string value,
        int byteCount)
    {
        WriteUInt16(buffer, ref offset, (ushort)byteCount);
        WriteEncodedString(buffer, ref offset, encoding, value, byteCount);
    }

    private static void WriteEncodedString(byte[] buffer, ref int offset, Encoding encoding, string value, int byteCount) => offset += encoding.GetBytes(value.AsSpan(), buffer.AsSpan(offset, byteCount));

    private static void WriteUInt16(byte[] buffer, ref int offset, ushort value)
    {
        Wu16(buffer, offset, value);
        offset += sizeof(ushort);
    }

    private static void WriteUInt32(byte[] buffer, ref int offset, uint value)
    {
        Wu32(buffer, offset, value);
        offset += sizeof(uint);
    }

    private static void WriteBytes(byte[] buffer, ref int offset, ReadOnlySpan<byte> value)
    {
        value.CopyTo(buffer.AsSpan(offset));
        offset += value.Length;
    }
}
