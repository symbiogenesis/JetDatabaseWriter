namespace JetDatabaseWriter.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Mutable builder + serializer for <c>MSysObjects.LvProp</c> blobs
/// (<c>MR2\0</c> / <c>KKD\0</c>).
/// </summary>
/// <remarks>
/// Mirrors the on-disk layout consumed by <see cref="ColumnPropertyBlock.Parse(byte[], DatabaseFormat)"/>;
/// see <c>docs/design/persisted-column-properties-format-notes.md</c> §2 for the
/// authoritative byte layout.
///
/// Round-trip guarantee: an unmodified blob parsed via
/// <see cref="ColumnPropertyBlock.Parse(byte[], DatabaseFormat)"/> and re-serialized via
/// <see cref="FromBlock(ColumnPropertyBlock)"/> + <see cref="ToBytes(DatabaseFormat)"/>
/// reproduces a byte stream that the parser interprets identically (entries, targets,
/// and unknown chunks all preserved). Byte-identity with the original is *not*
/// guaranteed because the inner property-block header carries opaque bytes that the
/// parser discards.
/// </remarks>
internal sealed class ColumnPropertyBlockBuilder
{
    private const int MagicLength = 4;
    private const int ChunkHeaderLength = sizeof(uint) + sizeof(ushort);
    private const int PropertyBlockTargetHeaderLength = sizeof(uint) + sizeof(ushort);
    private const int PropertyEntryHeaderLength = sizeof(ushort) + sizeof(byte) + sizeof(byte) + sizeof(ushort) + sizeof(ushort);

    /// <summary>Gets the mutable list of property targets in emission order. The first target is conventionally the table itself.</summary>
    public List<TargetBuilder> Targets { get; } = [];

    /// <summary>Gets the mutable list of opaque chunks to re-emit verbatim (forward-compat).</summary>
    public List<ColumnPropertyUnknownChunk> UnknownChunks { get; } = [];

    /// <summary>
    /// Gets a value indicating whether the builder would emit zero targets and
    /// zero unknown chunks — i.e. the resulting blob would carry only the magic
    /// header and is therefore not worth persisting.
    /// </summary>
    public bool IsEmpty => Targets.Count == 0 && UnknownChunks.Count == 0;

    /// <summary>
    /// Constructs a builder seeded with the parsed targets and unknown chunks of an
    /// existing block — the entry point for round-trip preservation.
    /// </summary>
    public static ColumnPropertyBlockBuilder FromBlock(ColumnPropertyBlock block)
    {
        Guard.NotNull(block, nameof(block));
        var b = new ColumnPropertyBlockBuilder();
        foreach (ColumnPropertyTarget t in block.Targets)
        {
            var tb = new TargetBuilder
            {
                Name = t.Name,
                ChunkType = t.ChunkType,
            };
            foreach (ColumnPropertyEntry e in t.Entries)
            {
                tb.Entries.Add(new EntryBuilder
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
    public TargetBuilder GetOrAddTarget(string name)
    {
        Guard.NotNullOrEmpty(name, nameof(name));
        foreach (TargetBuilder t in Targets)
        {
            if (string.Equals(t.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return t;
            }
        }

        var nt = new TargetBuilder { Name = name, ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1 };
        Targets.Add(nt);
        return nt;
    }

    /// <summary>
    /// Removes the target whose name matches <paramref name="name"/> case-insensitively.
    /// No-op if no such target exists. Returns <see langword="true"/> when a target was removed.
    /// </summary>
    public bool RemoveTarget(string name)
    {
        for (int i = 0; i < Targets.Count; i++)
        {
            if (string.Equals(Targets[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                Targets.RemoveAt(i);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Renames the target whose current name matches <paramref name="oldName"/> to
    /// <paramref name="newName"/>. No-op if no such target exists.
    /// </summary>
    public void RenameTarget(string oldName, string newName)
    {
        Guard.NotNullOrEmpty(newName, nameof(newName));
        foreach (TargetBuilder t in Targets)
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
        if (IsEmpty)
        {
            return null;
        }

        bool isJet3 = format == DatabaseFormat.Jet3Mdb;
        Encoding stringEncoding = isJet3 ? Encoding.GetEncoding(1252) : Encoding.Unicode;

        // Build the name pool from every distinct entry name encountered, in stable
        // first-seen order. The parser indexes by uint16 so we cap at 65,535 names entries.
        var nameToIndex = new Dictionary<string, ushort>(StringComparer.Ordinal);
        var nameOrder = new List<string>();
        foreach (TargetBuilder t in Targets)
        {
            foreach (EntryBuilder e in t.Entries)
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

        var propertyBlockPayloads = new byte[Targets.Count][];
        int totalLength = MagicLength;
        totalLength = AddChunkLength(totalLength, namePoolPayload.Length);

        for (int targetIndex = 0; targetIndex < Targets.Count; targetIndex++)
        {
            propertyBlockPayloads[targetIndex] = BuildPropertyBlockPayload(Targets[targetIndex], nameToIndex, stringEncoding);
            totalLength = AddChunkLength(totalLength, propertyBlockPayloads[targetIndex].Length);
        }

        foreach (ColumnPropertyUnknownChunk unknownChunk in UnknownChunks)
        {
            totalLength = AddChunkLength(totalLength, unknownChunk.Payload.Length);
        }

        byte[] blob = new byte[totalLength];
        int offset = 0;
        WriteMagic(blob, ref offset, isJet3);

        // Name-pool chunk (always first; mdbtools requires it before property blocks).
        WriteChunk(blob, ref offset, ColumnPropertyChunkType.NamePool, namePoolPayload);

        // Property-block chunks.
        for (int targetIndex = 0; targetIndex < Targets.Count; targetIndex++)
        {
            WriteChunk(blob, ref offset, Targets[targetIndex].ChunkType, propertyBlockPayloads[targetIndex]);
        }

        // Unknown chunks (preserved verbatim — re-emit at the end so they don't shadow
        // the name pool the parser depends on).
        foreach (ColumnPropertyUnknownChunk unknownChunk in UnknownChunks)
        {
            WriteChunk(blob, ref offset, (ColumnPropertyChunkType)unknownChunk.ChunkType, unknownChunk.Payload);
        }

        return blob;
    }

    private static byte[] BuildNamePoolPayload(List<string> names, Encoding encoding)
    {
        var byteCounts = new int[names.Count];
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
        TargetBuilder target,
        Dictionary<string, ushort> nameToIndex,
        Encoding encoding)
    {
        int targetNameByteCount = GetUInt16StringByteCount(encoding, target.Name, "Property target name");
        int payloadLength = PropertyBlockTargetHeaderLength + targetNameByteCount;
        var entryLengths = new int[target.Entries.Count];
        for (int entryIndex = 0; entryIndex < target.Entries.Count; entryIndex++)
        {
            EntryBuilder entry = target.Entries[entryIndex];
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
            EntryBuilder entry = target.Entries[entryIndex];
            if (!nameToIndex.TryGetValue(entry.Name, out ushort nameIndex))
            {
                throw new InvalidOperationException($"Entry name '{entry.Name}' was not registered in the name pool.");
            }

            int entryLength = entryLengths[entryIndex];
            int valueLength = entry.Value.Length;
            WriteUInt16(payload, ref offset, (ushort)entryLength);
            payload[offset++] = entry.DdlFlag;
            payload[offset++] = entry.DataType;
            WriteUInt16(payload, ref offset, nameIndex);
            WriteUInt16(payload, ref offset, (ushort)valueLength);
            WriteBytes(payload, ref offset, entry.Value);
        }

        return payload;
    }

    private static int AddChunkLength(int totalLength, int payloadLength)
    {
        return AddLength(totalLength, GetChunkLength(payloadLength), "Property block blob", null);
    }

    private static int AddPayloadLength(int payloadLength, int additionalLength, string payloadDescription)
    {
        return AddLength(payloadLength, additionalLength, "Property", payloadDescription);
    }

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

    private static void WriteEncodedString(byte[] buffer, ref int offset, Encoding encoding, string value, int byteCount)
    {
        offset += encoding.GetBytes(value.AsSpan(), buffer.AsSpan(offset, byteCount));
    }

    private static void WriteUInt16(byte[] buffer, ref int offset, ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(offset), value);
        offset += sizeof(ushort);
    }

    private static void WriteUInt32(byte[] buffer, ref int offset, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(offset), value);
        offset += sizeof(uint);
    }

    private static void WriteBytes(byte[] buffer, ref int offset, ReadOnlySpan<byte> value)
    {
        value.CopyTo(buffer.AsSpan(offset));
        offset += value.Length;
    }

    /// <summary>Mutable builder for a single property target (table or column).</summary>
    internal sealed class TargetBuilder
    {
        /// <summary>Gets or sets the target name (column name, or table name for the table-level target).</summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the chunk-type code. Defaults to <see cref="ColumnPropertyChunkType.PropertyBlock"/> (<c>0x0000</c>), the subtype this library emits for new targets.</summary>
        public ColumnPropertyChunkType ChunkType { get; set; }

        /// <summary>Gets the mutable list of property entries in emission order.</summary>
        public List<EntryBuilder> Entries { get; } = [];

        /// <summary>Adds a Text-typed (<c>0x0A</c>) string property using the supplied database format's encoding.</summary>
        public void AddText(string propertyName, string value, DatabaseFormat format)
        {
            Guard.NotNullOrEmpty(propertyName, nameof(propertyName));
            Guard.NotNull(value, nameof(value));
            Encoding enc = format == DatabaseFormat.Jet3Mdb ? Encoding.GetEncoding(1252) : Encoding.Unicode;
            Entries.Add(new EntryBuilder
            {
                Name = propertyName,
                DataType = ColumnPropertyBlock.DataTypeText,
                DdlFlag = 0x00,
                Value = enc.GetBytes(value),
            });
        }

        /// <summary>
        /// Adds a Boolean-typed (<c>0x01</c>) property. Stored on disk as a single
        /// byte: <c>0xFF</c> = true, <c>0x00</c> = false. Matches the wire format
        /// DAO/Access emit for Boolean column properties such as <c>Required</c>.
        /// </summary>
        public void AddBoolean(string propertyName, bool value)
        {
            Guard.NotNullOrEmpty(propertyName, nameof(propertyName));
            Entries.Add(new EntryBuilder
            {
                Name = propertyName,
                DataType = ColumnPropertyBlock.DataTypeBoolean,
                DdlFlag = 0x01,
                Value = [value ? (byte)0xFF : (byte)0x00],
            });
        }
    }

    /// <summary>Mutable builder for a single property entry within a target.</summary>
    internal sealed class EntryBuilder
    {
        /// <summary>Gets or sets the property name (e.g. <c>"DefaultValue"</c>).</summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the Jet column-type code (see <see cref="ColumnPropertyBlock"/>'s <c>DataType*</c> constants).</summary>
        public byte DataType { get; set; }

        /// <summary>Gets or sets the flag byte at entry offset 2.</summary>
        public byte DdlFlag { get; set; }

        /// <summary>Gets or sets the raw value bytes per <see cref="DataType"/>'s encoding.</summary>
        public byte[] Value { get; set; } = [];
    }
}
