namespace JetDatabaseWriter.Internal;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Internal.Helpers;

/// <summary>
/// Minimal writer for the OLE / Microsoft Compound File Binary (CFB / OLE2)
/// format ("MS-CFB"). Emits a CFB v4 (4096-byte sector) document containing
/// the supplied named streams as top-level entries under the Root Entry.
///
/// Only the subset needed to wrap Office Crypto API "Agile" encrypted .accdb
/// files is implemented:
///   • Single root storage with N stream children (typically two:
///     <c>EncryptionInfo</c> and <c>EncryptedPackage</c>)
///   • Mini-stream cutoff is set to 0, so every stream uses the regular FAT
///   • Up to 109 FAT sectors (~436 MiB of payload), addressed via the
///     fixed 109-entry header DIFAT — no DIFAT extension sectors
///   • Up to 4096 directory entries (32 directory sectors of 128 entries)
///
/// The directory red-black tree is laid out as a left-leaning chain (each
/// entry's right sibling pointing at the next), which is a valid
/// representation accepted by Microsoft Office and the
/// <see cref="CompoundFileReader"/> in this assembly.
/// </summary>
internal static class CompoundFileWriter
{
    private const int CfbSectorSize = 4096;       // v4 sector size
    private const int DirEntrySize = 128;
    private const int EntriesPerFatSector = CfbSectorSize / 4;  // 1024
    private const int EntriesPerDirSector = CfbSectorSize / DirEntrySize; // 32
    private const uint FreeSect = 0xFFFFFFFFu;
    private const uint EndOfChain = 0xFFFFFFFEu;
    private const uint FatSect = 0xFFFFFFFDu;
    private const int MaxHeaderDifatEntries = 109;

    /// <summary>
    /// Builds an in-memory CFB v4 compound document containing the supplied
    /// top-level streams, in the order provided. Stream names must be ≤ 31
    /// UTF-16 code units.
    /// </summary>
    public static byte[] Build(IReadOnlyList<KeyValuePair<string, byte[]>> streams)
    {
        Guard.NotNull(streams, nameof(streams));
        if (streams.Count == 0)
        {
            throw new ArgumentException("At least one stream is required.", nameof(streams));
        }

        // ── Lay out sectors ────────────────────────────────────────────
        // Order:
        //   0..(numDirSectors-1)        : directory sectors
        //   next                        : stream payload sectors (1 chain per stream)
        //   FAT sectors are computed last and inserted at the end of the file
        //   layout (their sector indices are then reserved in the FAT itself).

        int dirEntryCount = 1 + streams.Count; // root + N streams
        int numDirSectors = (dirEntryCount + EntriesPerDirSector - 1) / EntriesPerDirSector;

        // Stream sector counts and start sectors.
        var streamStart = new int[streams.Count];
        var streamSectors = new int[streams.Count];
        int sectorCursor = numDirSectors;
        for (int i = 0; i < streams.Count; i++)
        {
            int len = streams[i].Value?.Length ?? 0;
            int sectors = len == 0 ? 0 : (len + CfbSectorSize - 1) / CfbSectorSize;
            streamSectors[i] = sectors;
            streamStart[i] = sectors == 0 ? unchecked((int)EndOfChain) : sectorCursor;
            sectorCursor += sectors;
        }

        // Total non-FAT sectors. We must size FAT to also cover its own sectors.
        int dataSectors = sectorCursor;

        // Iterate to find a stable (numFatSectors, totalSectors) pair where
        // the FAT can map every sector of the file (data + FAT itself).
        int numFatSectors = 0;
        int totalSectors;
        while (true)
        {
            totalSectors = dataSectors + numFatSectors;
            int needed = (totalSectors + EntriesPerFatSector - 1) / EntriesPerFatSector;
            if (needed == numFatSectors)
            {
                break;
            }

            numFatSectors = needed;
        }

        if (numFatSectors > MaxHeaderDifatEntries)
        {
            throw new NotSupportedException(
                $"Compound file requires {numFatSectors} FAT sectors, but only " +
                $"{MaxHeaderDifatEntries} are addressable without DIFAT extension sectors. " +
                "The decrypted database is too large to re-wrap with this writer.");
        }

        // FAT sector indices follow all data sectors.
        var fatSectorIds = new int[numFatSectors];
        for (int i = 0; i < numFatSectors; i++)
        {
            fatSectorIds[i] = dataSectors + i;
        }

        // ── Allocate the file buffer ───────────────────────────────────
        long fileSize = CfbSectorSize + ((long)totalSectors * CfbSectorSize);
        if (fileSize > int.MaxValue)
        {
            throw new NotSupportedException(
                $"Compound file output would exceed 2 GiB ({fileSize:N0} bytes); not supported.");
        }

        byte[] file = new byte[fileSize];

        // ── Build the FAT (in a flat array first, then lay into sectors) ─
        var fat = new uint[(long)numFatSectors * EntriesPerFatSector];
        for (long i = 0; i < fat.LongLength; i++)
        {
            fat[i] = FreeSect;
        }

        // Mark FAT sectors themselves.
        for (int i = 0; i < numFatSectors; i++)
        {
            fat[fatSectorIds[i]] = FatSect;
        }

        // Directory chain.
        ChainSectors(fat, startSector: 0, sectorCount: numDirSectors);

        // Stream chains.
        for (int i = 0; i < streams.Count; i++)
        {
            if (streamSectors[i] > 0)
            {
                ChainSectors(fat, streamStart[i], streamSectors[i]);
            }
        }

        // ── Header ────────────────────────────────────────────────────
        WriteHeader(file, fatSectorIds, firstDirSector: 0, numDirSectors: numDirSectors);

        // ── Directory entries ─────────────────────────────────────────
        // Layout: index 0 = Root Entry, indices 1..N = streams (in input order).
        // Red-black tree: root.child = 1; each stream entry's right sibling
        // points at the next, forming a left-leaning chain. The last entry's
        // right sibling = FREESECT.
        int dirOffset = SectorOffset(0);
        WriteRootEntry(file, dirOffset, child: 1);
        for (int i = 0; i < streams.Count; i++)
        {
            uint right = (i == streams.Count - 1) ? FreeSect : (uint)(i + 2);
            WriteStreamEntry(
                file,
                dirOffset + ((i + 1) * DirEntrySize),
                streams[i].Key,
                startSector: unchecked((uint)streamStart[i]),
                size: streams[i].Value?.Length ?? 0,
                leftSibling: FreeSect,
                rightSibling: right);
        }

        // Zero-fill any leftover directory entries (unused slots).
        int totalDirEntries = numDirSectors * EntriesPerDirSector;
        for (int i = dirEntryCount; i < totalDirEntries; i++)
        {
            WriteUnusedEntry(file, dirOffset + (i * DirEntrySize));
        }

        // ── Stream payloads ───────────────────────────────────────────
        for (int i = 0; i < streams.Count; i++)
        {
            byte[]? payload = streams[i].Value;
            if (payload == null || payload.Length == 0)
            {
                continue;
            }

            Buffer.BlockCopy(payload, 0, file, SectorOffset(streamStart[i]), payload.Length);
        }

        // ── FAT sectors (written last so they reflect every chain marked above) ──
        for (int i = 0; i < numFatSectors; i++)
        {
            int sectorOff = SectorOffset(fatSectorIds[i]);
            for (int j = 0; j < EntriesPerFatSector; j++)
            {
                long fatIdx = ((long)i * EntriesPerFatSector) + j;
                uint v = fatIdx < fat.LongLength ? fat[fatIdx] : FreeSect;
                BinaryPrimitives.WriteUInt32LittleEndian(
                    file.AsSpan(sectorOff + (j * 4), 4),
                    v);
            }
        }

        return file;
    }

    private static void ChainSectors(uint[] fat, int startSector, int sectorCount)
    {
        for (int k = 0; k < sectorCount; k++)
        {
            int sec = startSector + k;
            fat[sec] = (k == sectorCount - 1) ? EndOfChain : (uint)(sec + 1);
        }
    }

    private static int SectorOffset(int sectorIndex) =>
        CfbSectorSize + (sectorIndex * CfbSectorSize);

    private static void WriteHeader(byte[] file, int[] fatSectorIds, int firstDirSector, int numDirSectors)
    {
        Span<byte> h = file.AsSpan(0, 512);

        CompoundFileReader.CfbSignature.CopyTo(h);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x18, 2), 0x003E);  // minor version
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1A, 2), 0x0004);  // major version (v4 = 4096-byte sectors)
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1C, 2), 0xFFFE);  // little-endian byte order
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1E, 2), 0x000C);  // sector shift = 12 → 4096
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x20, 2), 0x0006);  // mini sector shift = 6 → 64
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x28, 4), numDirSectors);
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x2C, 4), fatSectorIds.Length);
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x30, 4), firstDirSector);

        // 0x34 transaction signature stays 0; 0x38 mini-stream cutoff = 0
        // (forces all streams into the regular FAT; no mini-FAT used).
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x38, 4), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x3C, 4), EndOfChain);  // first mini-FAT sector
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x40, 4), 0);            // # mini-FAT sectors
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x44, 4), EndOfChain);  // first DIFAT extension sector
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(0x48, 4), 0);            // # DIFAT extension sectors

        // 0x4C..0xFF: 109-entry header DIFAT.
        for (int i = 0; i < MaxHeaderDifatEntries; i++)
        {
            uint v = i < fatSectorIds.Length ? (uint)fatSectorIds[i] : FreeSect;
            BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C + (i * 4), 4), v);
        }
    }

    private static void WriteRootEntry(byte[] file, int offset, uint child)
    {
        WriteDirEntry(
            file,
            offset,
            "Root Entry",
            objectType: 5,
            startSector: EndOfChain,
            sizeLow: 0,
            sizeHigh: 0,
            leftSibling: FreeSect,
            rightSibling: FreeSect,
            child: child);
    }

    private static void WriteStreamEntry(
        byte[] file,
        int offset,
        string name,
        uint startSector,
        long size,
        uint leftSibling,
        uint rightSibling)
    {
        WriteDirEntry(
            file,
            offset,
            name,
            objectType: 2,
            startSector: startSector,
            sizeLow: unchecked((uint)(size & 0xFFFFFFFFu)),
            sizeHigh: unchecked((uint)((size >> 32) & 0xFFFFFFFFu)),
            leftSibling: leftSibling,
            rightSibling: rightSibling,
            child: FreeSect);
    }

    private static void WriteUnusedEntry(byte[] file, int offset)
    {
        Span<byte> e = file.AsSpan(offset, DirEntrySize);
        e.Clear();
        e[0x42] = 0x00; // unallocated
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x44, 4), FreeSect);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x48, 4), FreeSect);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x4C, 4), FreeSect);
    }

    private static void WriteDirEntry(
        byte[] file,
        int offset,
        string name,
        byte objectType,
        uint startSector,
        uint sizeLow,
        uint sizeHigh,
        uint leftSibling,
        uint rightSibling,
        uint child)
    {
        Span<byte> e = file.AsSpan(offset, DirEntrySize);
        e.Clear();

        byte[] nameBytes = Encoding.Unicode.GetBytes(name);
        if (nameBytes.Length > 62)
        {
            throw new ArgumentException("Directory entry name too long (max 31 UTF-16 code units).", nameof(name));
        }

        nameBytes.CopyTo(e);
        ushort nameLen = (ushort)(nameBytes.Length + 2); // include UTF-16 NUL terminator
        BinaryPrimitives.WriteUInt16LittleEndian(e.Slice(0x40, 2), nameLen);

        e[0x42] = objectType;
        e[0x43] = 0x01; // black (red-black tree color)

        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x44, 4), leftSibling);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x48, 4), rightSibling);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x4C, 4), child);

        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x74, 4), startSector);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x78, 4), sizeLow);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x7C, 4), sizeHigh);
    }
}
