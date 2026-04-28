namespace JetDatabaseWriter.Internal.Builders;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;

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
    /// default to chunk-type <c>0x00</c> (the property-block subtype this library emits).
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

        var nt = new TargetBuilder { Name = name, ChunkType = ColumnPropertyChunkType.PropertyBlock };
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

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.Unicode, leaveOpen: true);

        // Magic
        byte[] magic = isJet3
            ? [(byte)'K', (byte)'K', (byte)'D', 0x00]
            : [(byte)'M', (byte)'R', (byte)'2', 0x00];
        bw.Write(magic);

        // Name-pool chunk (always first; mdbtools requires it before property blocks).
        WriteChunk(bw, ColumnPropertyChunkType.NamePool, BuildNamePoolPayload(nameOrder, stringEncoding));

        // Property-block chunks.
        foreach (TargetBuilder t in Targets)
        {
            WriteChunk(bw, t.ChunkType, BuildPropertyBlockPayload(t, nameToIndex, stringEncoding));
        }

        // Unknown chunks (preserved verbatim — re-emit at the end so they don't shadow
        // the name pool the parser depends on).
        foreach (ColumnPropertyUnknownChunk u in UnknownChunks)
        {
            WriteChunk(bw, (ColumnPropertyChunkType)u.ChunkType, u.Payload);
        }

        bw.Flush();
        return ms.ToArray();
    }

    private static byte[] BuildNamePoolPayload(List<string> names, Encoding encoding)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.Unicode, leaveOpen: true);
        foreach (string n in names)
        {
            byte[] bytes = encoding.GetBytes(n);
            if (bytes.Length > ushort.MaxValue)
            {
                throw new InvalidOperationException($"Property name '{n}' encodes to {bytes.Length} bytes, exceeding the uint16 length limit.");
            }

            bw.Write((ushort)bytes.Length);
            bw.Write(bytes);
        }

        bw.Flush();
        return ms.ToArray();
    }

    private static byte[] BuildPropertyBlockPayload(
        TargetBuilder target,
        Dictionary<string, ushort> nameToIndex,
        Encoding encoding)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.Unicode, leaveOpen: true);

        // Inner header — first 4 bytes are opaque per mdbtools (read & discarded).
        // Convention used here: write the payload's eventual total byte count for
        // forward debuggability; the parser ignores it.
        bw.Write(0u); // placeholder; patched at the end below.

        byte[] nameBytes = encoding.GetBytes(target.Name);
        if (nameBytes.Length > ushort.MaxValue)
        {
            throw new InvalidOperationException($"Property target name '{target.Name}' encodes to {nameBytes.Length} bytes, exceeding the uint16 length limit.");
        }

        bw.Write((ushort)nameBytes.Length);
        bw.Write(nameBytes);

        foreach (EntryBuilder e in target.Entries)
        {
            if (!nameToIndex.TryGetValue(e.Name, out ushort nameIndex))
            {
                throw new InvalidOperationException($"Entry name '{e.Name}' was not registered in the name pool.");
            }

            int valueLen = e.Value.Length;
            int entryLen = 8 + valueLen;
            if (entryLen > ushort.MaxValue)
            {
                throw new InvalidOperationException($"Property entry '{e.Name}' value is {valueLen} bytes; max supported is {ushort.MaxValue - 8}.");
            }

            bw.Write((ushort)entryLen);
            bw.Write(e.DdlFlag);
            bw.Write(e.DataType);
            bw.Write(nameIndex);
            bw.Write((ushort)valueLen);
            bw.Write(e.Value);
        }

        bw.Flush();
        byte[] payload = ms.ToArray();

        // Patch the leading uint32 with the payload byte count (cosmetic; ignored on read).
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), (uint)payload.Length);
        return payload;
    }

    private static void WriteChunk(BinaryWriter bw, ColumnPropertyChunkType chunkType, byte[] payload)
    {
        long chunkLen = 6L + payload.Length;
        if (chunkLen > uint.MaxValue)
        {
            throw new InvalidOperationException($"Property chunk would be {chunkLen} bytes, exceeding the uint32 length limit.");
        }

        bw.Write((uint)chunkLen);
        bw.Write((ushort)chunkType);
        bw.Write(payload);
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
