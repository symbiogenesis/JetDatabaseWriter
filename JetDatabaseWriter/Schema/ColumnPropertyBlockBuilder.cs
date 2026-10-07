namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Mutable builder + serializer for <c>MSysObjects.LvProp</c> blobs
/// (<c>MR2\0</c> / <c>KKD\0</c>).
/// </summary>
/// <remarks>
/// <para>
/// Mirrors the on-disk layout consumed by <see cref="ColumnPropertyBlock.Parse(byte[], JetFormat)"/>;
/// see <see href="docs/design/persisted-column-properties-format-notes.md" /> §2 for the
/// authoritative byte layout.
/// </para>
/// <para>
/// Round-trip guarantee: an unmodified blob parsed via
/// <see cref="ColumnPropertyBlock.Parse(byte[], JetFormat)"/> and re-serialized via
/// <see cref="FromBlock(ColumnPropertyBlock)"/> + <see cref="ToBytes(JetFormat)"/>
/// preserves entries, targets, entry padding, duplicate and unused property names,
/// opaque target headers and source chunk ordering.
/// </para>
/// </remarks>
internal sealed class ColumnPropertyBlockBuilder
{
    private const int MagicLength = 4;
    private const int ChunkHeaderLength = sizeof(uint) + sizeof(ushort);
    private const int PropertyBlockTargetHeaderLength = sizeof(uint) + sizeof(ushort);
    private const int PropertyEntryHeaderLength = sizeof(ushort) + sizeof(byte) + sizeof(byte) + sizeof(ushort) + sizeof(ushort);

    private readonly List<ColumnPropertyUnknownChunk> sourceUnknownChunks = [];
    private Encoding? sourceTextEncoding;
    private uint? sourceMagic;
    private IReadOnlyList<ColumnPropertySourceChunk> sourceChunks = [];

    /// <summary>
    /// Gets the mutable property targets. Existing targets retain their source chunk
    /// positions; new targets follow list order. The table-level target has an empty name and can be
    /// anywhere in the list (see <see cref="ColumnPropertyBlock.FindTableTarget"/>).
    /// </summary>
    public List<ColumnPropertyTargetBuilder> Targets { get; } = [];

    /// <summary>Gets the mutable list of opaque chunks to re-emit verbatim (forward-compat).</summary>
    public List<ColumnPropertyUnknownChunk> UnknownChunks { get; } = [];

    /// <summary>
    /// Gets a value indicating whether the builder would emit zero targets and
    /// zero unknown chunks and has no preserved source chunks or stored signature.
    /// </summary>
    public bool IsEmpty => this.Targets.Count == 0 && this.UnknownChunks.Count == 0 && this.sourceChunks.Count == 0 && this.sourceMagic is null;

    /// <summary>
    /// Constructs a builder seeded with the parsed targets and unknown chunks of an
    /// existing block — the entry point for round-trip preservation.
    /// </summary>
    /// <param name="block">The block.</param>
    public static ColumnPropertyBlockBuilder FromBlock(ColumnPropertyBlock block)
    {
        Guard.NotNull(block, nameof(block));
        var b = new ColumnPropertyBlockBuilder
        {
            sourceTextEncoding = block.TextEncoding,
            sourceMagic = block.Magic == 0 ? null : block.Magic,
            sourceChunks = block.SourceChunks,
        };
        foreach (ColumnPropertyTarget t in block.Targets)
        {
            b.Targets.Add(FromTarget(t));
        }

        foreach (ColumnPropertyUnknownChunk u in block.UnknownChunks)
        {
            var copy = new ColumnPropertyUnknownChunk(u.ChunkType, (byte[])u.Payload.Clone());
            b.UnknownChunks.Add(copy);
            b.sourceUnknownChunks.Add(copy);
        }

        return b;
    }

    /// <summary>
    /// Returns a mutable copy of a parsed target: its name, chunk type, and every
    /// entry with its name, data type, DDL flag and value bytes, in source order.
    /// </summary>
    /// <param name="target">The parsed target.</param>
    public static ColumnPropertyTargetBuilder FromTarget(ColumnPropertyTarget target)
    {
        Guard.NotNull(target, nameof(target));
        var tb = new ColumnPropertyTargetBuilder
        {
            Name = target.Name,
            ChunkType = target.ChunkType,
            TextEncoding = target.TextEncoding,
            SourceHeader = target.SourceHeader,
            SourceChunkIndex = target.SourceChunkIndex,
            SourceHeaderIsNameLength = target.SourceHeaderIsNameLength,
        };
        foreach (ColumnPropertyEntry e in target.Entries)
        {
            tb.Entries.Add(new ColumnPropertyEntryBuilder
            {
                Name = e.Name,
                DataType = e.DataType,
                DdlFlag = e.DdlFlag,
                Value = (byte[])e.Value.Clone(),
                Padding = (byte[])e.Padding.Clone(),
                SourceNameIndex = e.SourceNameIndex,
            });
        }

        return tb;
    }

    /// <summary>
    /// Returns the table-level target (the first one with an empty name), or adds
    /// one with an empty name and chunk type <c>0x00</c> at index 0. Access puts
    /// the table's block at no fixed position (see
    /// <see cref="ColumnPropertyBlock.FindTableTarget"/>), so readers find it by
    /// its empty name; index 0 is only where this builder adds a new one.
    /// </summary>
    public ColumnPropertyTargetBuilder GetOrAddTableTarget()
    {
        foreach (ColumnPropertyTargetBuilder t in this.Targets)
        {
            if (t.Name.Length == 0)
            {
                return t;
            }
        }

        var table = new ColumnPropertyTargetBuilder { Name = string.Empty, ChunkType = ColumnPropertyChunkType.PropertyBlock, TextEncoding = this.sourceTextEncoding };
        this.Targets.Insert(0, table);
        return table;
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

        var nt = new ColumnPropertyTargetBuilder { Name = name, ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1, TextEncoding = this.sourceTextEncoding };
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
    /// Serializes to bytes. Returns <see langword="null"/> when a new builder is empty
    /// (no targets or unknown chunks). Parsed source signatures and name pools remain present.
    /// </summary>
    /// <param name="format">Database format. Selects Jet3 codepage vs Jet4 UTF-16LE string encoding.</param>
    /// <exception cref="InvalidOperationException">If a chunk would exceed the on-disk uint16 / uint32 length limits.</exception>
    public byte[]? ToBytes(JetFormat format)
    {
        if (this.IsEmpty)
        {
            return null;
        }

        Encoding stringEncoding = this.sourceTextEncoding ?? format.PropertyTextEncoding;

        // Keep every original pool in place, including unused and duplicate names.
        // A target uses the last pool preceding its original chunk.
        var pools = new Dictionary<int, List<string>> { [-1] = [] };
        var poolIndices = new Dictionary<int, Dictionary<string, int>> { [-1] = new(StringComparer.Ordinal) };
        var sourceTargetPools = new Dictionary<int, int>();
        int currentPool = -1;
        for (int chunkIndex = 0; chunkIndex < this.sourceChunks.Count; chunkIndex++)
        {
            ColumnPropertySourceChunk chunk = this.sourceChunks[chunkIndex];
            if (chunk.Names is { } names)
            {
                pools.Add(chunkIndex, new List<string>(names));
                poolIndices.Add(chunkIndex, IndexNames(names));
                currentPool = chunkIndex;
            }
            else if (chunk.UnknownIndex < 0)
            {
                sourceTargetPools.Add(chunkIndex, currentPool);
            }
        }

        var targetsByChunk = new Dictionary<int, List<ColumnPropertyTargetBuilder>>();
        var targetPools = new Dictionary<ColumnPropertyTargetBuilder, int>();
        var newTargets = new List<ColumnPropertyTargetBuilder>();
        foreach (ColumnPropertyTargetBuilder target in this.Targets)
        {
            bool sourceTarget = sourceTargetPools.TryGetValue(target.SourceChunkIndex, out int poolIndex);
            if (sourceTarget)
            {
                if (!targetsByChunk.TryGetValue(target.SourceChunkIndex, out List<ColumnPropertyTargetBuilder>? chunkTargets))
                {
                    chunkTargets = [];
                    targetsByChunk.Add(target.SourceChunkIndex, chunkTargets);
                }

                chunkTargets.Add(target);
            }
            else
            {
                poolIndex = currentPool;
                newTargets.Add(target);
            }

            targetPools.Add(target, poolIndex);
            if (sourceTarget && poolIndex < 0 && target.Entries.Count == 0)
            {
                continue;
            }

            if (!pools.TryGetValue(poolIndex, out List<string>? names))
            {
                names = [];
                pools.Add(poolIndex, names);
                poolIndices.Add(poolIndex, new Dictionary<string, int>(StringComparer.Ordinal));
            }

            Dictionary<string, int> indices = poolIndices[poolIndex];
            foreach (ColumnPropertyEntryBuilder entry in target.Entries)
            {
                if (!indices.ContainsKey(entry.Name))
                {
                    if (names.Count > ushort.MaxValue)
                    {
                        throw new InvalidOperationException("Property name pool exceeds the uint16 index limit.");
                    }

                    indices.Add(entry.Name, names.Count);
                    names.Add(entry.Name);
                }
            }
        }

        var chunks = new List<(ColumnPropertyChunkType Type, byte[] Payload)>();
        var retainedUnknown = new HashSet<ColumnPropertyUnknownChunk>(this.UnknownChunks);
        var emittedUnknown = new HashSet<ColumnPropertyUnknownChunk>();
        bool insertedPool = false;
        for (int chunkIndex = 0; chunkIndex < this.sourceChunks.Count; chunkIndex++)
        {
            ColumnPropertySourceChunk chunk = this.sourceChunks[chunkIndex];
            if (chunk.Names is not null)
            {
                byte[] appendedNames = BuildNamePoolPayload(pools[chunkIndex].GetRange(chunk.Names.Length, pools[chunkIndex].Count - chunk.Names.Length), stringEncoding);
                byte[] payload = new byte[AddPayloadLength(chunk.Payload.Length, appendedNames.Length, "name-pool payload")];
                chunk.Payload.CopyTo(payload, 0);
                appendedNames.CopyTo(payload, chunk.Payload.Length);
                chunks.Add((ColumnPropertyChunkType.NamePool, payload));
            }
            else if (chunk.UnknownIndex >= 0)
            {
                ColumnPropertyUnknownChunk unknown = this.sourceUnknownChunks[chunk.UnknownIndex];
                if (retainedUnknown.Contains(unknown))
                {
                    chunks.Add(((ColumnPropertyChunkType)unknown.ChunkType, unknown.Payload));
                    _ = emittedUnknown.Add(unknown);
                }
            }
            else if (targetsByChunk.TryGetValue(chunkIndex, out List<ColumnPropertyTargetBuilder>? chunkTargets))
            {
                foreach (ColumnPropertyTargetBuilder target in chunkTargets)
                {
                    int poolIndex = targetPools[target];
                    if (poolIndex < 0 && target.Entries.Count > 0 && !insertedPool)
                    {
                        chunks.Add((ColumnPropertyChunkType.NamePool, BuildNamePoolPayload(pools[-1], stringEncoding)));
                        insertedPool = true;
                    }

                    chunks.Add((target.ChunkType, BuildPropertyBlockPayload(target, pools[poolIndex], poolIndices[poolIndex], stringEncoding)));
                }
            }
        }

        // Append new names to the last existing pool; existing indices stay unchanged.
        if (newTargets.Count > 0)
        {
            if (currentPool < 0 && !insertedPool)
            {
                chunks.Add((ColumnPropertyChunkType.NamePool, BuildNamePoolPayload(pools[-1], stringEncoding)));
            }

            foreach (ColumnPropertyTargetBuilder target in newTargets)
            {
                chunks.Add((target.ChunkType, BuildPropertyBlockPayload(target, pools[currentPool], poolIndices[currentPool], stringEncoding)));
            }
        }

        foreach (ColumnPropertyUnknownChunk unknown in this.UnknownChunks)
        {
            if (!emittedUnknown.Contains(unknown))
            {
                chunks.Add(((ColumnPropertyChunkType)unknown.ChunkType, unknown.Payload));
            }
        }

        int totalLength = MagicLength;
        foreach ((ColumnPropertyChunkType _, byte[] payload) in chunks)
        {
            totalLength = AddChunkLength(totalLength, payload.Length);
        }

        byte[] blob = new byte[totalLength];
        int offset = 0;
        WriteUInt32(blob, ref offset, this.sourceMagic ?? JetFormat.PropertyBlockMagicOf(format.Kind));
        foreach ((ColumnPropertyChunkType type, byte[] payload) in chunks)
        {
            WriteChunk(blob, ref offset, type, payload);
        }

        return blob;
    }

    /// <summary>Creates an empty builder with the same preserved property signature and text encoding.</summary>
    internal ColumnPropertyBlockBuilder CreateEmptyWithSameEncoding() => new()
    {
        sourceTextEncoding = this.sourceTextEncoding,
        sourceMagic = this.sourceMagic,
    };

    private static Dictionary<string, int> IndexNames(string[] names)
    {
        var indices = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int index = 0; index < names.Length; index++)
        {
            _ = indices.TryAdd(names[index], index);
        }

        return indices;
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
        List<string> names,
        Dictionary<string, int> nameIndices,
        Encoding encoding)
    {
        int targetNameByteCount = GetUInt16StringByteCount(encoding, target.Name, "Property target name");
        int payloadLength = PropertyBlockTargetHeaderLength + targetNameByteCount;
        int[] entryLengths = new int[target.Entries.Count];
        for (int entryIndex = 0; entryIndex < target.Entries.Count; entryIndex++)
        {
            ColumnPropertyEntryBuilder entry = target.Entries[entryIndex];
            int valueLength = entry.Value.Length;
            long entryLength = PropertyEntryHeaderLength + (long)valueLength + entry.Padding.Length;
            if (entryLength > ushort.MaxValue)
            {
                throw new InvalidOperationException($"Property entry '{entry.Name}' value is {valueLength} bytes; max supported is {ushort.MaxValue - PropertyEntryHeaderLength}.");
            }

            entryLengths[entryIndex] = (int)entryLength;
            payloadLength = AddPayloadLength(payloadLength, (int)entryLength, "property-block payload");
        }

        byte[] payload = new byte[payloadLength];
        int offset = 0;

        // Preserve unrecognized headers. Update a recognized native name-length field.
        // DAO writes the byte count through the target-name field, not the whole
        // payload length: sizeof(uint32) + sizeof(uint16) + targetNameBytes.
        uint nameHeaderLength = (uint)(PropertyBlockTargetHeaderLength + targetNameByteCount);
        WriteUInt32(payload, ref offset, target.SourceHeaderIsNameLength ? nameHeaderLength : target.SourceHeader ?? nameHeaderLength);
        WriteLengthPrefixedEncodedString(payload, ref offset, encoding, target.Name, targetNameByteCount);

        for (int entryIndex = 0; entryIndex < target.Entries.Count; entryIndex++)
        {
            ColumnPropertyEntryBuilder entry = target.Entries[entryIndex];
            int index = entry.SourceNameIndex is { } sourceIndex && sourceIndex < names.Count && names[sourceIndex] == entry.Name
                ? sourceIndex
                : nameIndices.GetValueOrDefault(entry.Name, -1);
            if (index < 0 || index > ushort.MaxValue)
            {
                throw new InvalidOperationException($"Entry name '{entry.Name}' was not registered in the name pool.");
            }

            int entryLength = entryLengths[entryIndex];
            int valueLength = entry.Value.Length;
            WriteUInt16(payload, ref offset, (ushort)entryLength);
            payload[offset++] = entry.DdlFlag;
            payload[offset++] = (byte)entry.DataType;
            WriteUInt16(payload, ref offset, (ushort)index);
            WriteUInt16(payload, ref offset, (ushort)valueLength);
            WriteBytes(payload, ref offset, entry.Value);
            WriteBytes(payload, ref offset, entry.Padding);
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
