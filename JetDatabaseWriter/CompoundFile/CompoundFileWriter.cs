namespace JetDatabaseWriter.CompoundFile;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Infrastructure;
using CfbConstants = JetDatabaseWriter.Constants.CompoundFile;

/// <summary>
/// Minimal writer for the OLE / Microsoft Compound File Binary (CFB / OLE2)
/// format ("MS-CFB"). Emits a compound document containing the supplied named
/// streams as top-level entries under the Root Entry.
///
/// Only the subset needed to wrap Office Crypto API "Agile" encrypted .accdb
/// files is implemented:
///   • Single root storage with N stream children (typically two:
///     <c>EncryptionInfo</c> and <c>EncryptedPackage</c>)
///   • By default, mini-stream cutoff is set to 0, so every stream uses the regular FAT
///   • Up to 109 FAT sectors, addressed via the
///     fixed 109-entry header DIFAT — no DIFAT extension sectors
///   • Directory entries stored in regular directory sectors
///
/// The directory red-black tree is laid out as an ascending right-sibling
/// chain, which is a valid representation accepted by Microsoft Office and
/// the <see cref="CompoundFileReader"/> in this assembly.
/// </summary>
internal static class CompoundFileWriter
{
    /// <summary>
    /// Builds an in-memory CFB v3 compound document containing the supplied
    /// top-level streams. Stream names must be ≤ 31 UTF-16 code units.
    /// </summary>
    public static byte[] Build(IReadOnlyList<KeyValuePair<string, byte[]>> streams)
    {
        return Build(
            streams,
            sectorSize: CfbConstants.V3.SectorSize,
            sectorShift: CfbConstants.V3.SectorShift,
            majorVersion: CfbConstants.V3.MajorVersion,
            miniStreamCutoff: 0,
            requireRegularFatStreams: false);
    }

    /// <summary>
    /// Builds an Office-crypto-compatible CFB compound document with the
    /// standard 4096-byte mini-stream cutoff. Streams are stored in the regular
    /// FAT, so callers must pad any non-empty stream to at least the cutoff.
    /// </summary>
    public static byte[] BuildOfficeCrypto(IReadOnlyList<KeyValuePair<string, byte[]>> streams)
    {
        return Build(
            streams,
            sectorSize: CfbConstants.V4.SectorSize,
            sectorShift: CfbConstants.V4.SectorShift,
            majorVersion: CfbConstants.V4.MajorVersion,
            miniStreamCutoff: 4096,
            requireRegularFatStreams: true);
    }

    private static byte[] Build(
        IReadOnlyList<KeyValuePair<string, byte[]>> streams,
        int sectorSize,
        ushort sectorShift,
        ushort majorVersion,
        uint miniStreamCutoff,
        bool requireRegularFatStreams)
    {
        Guard.NotNull(streams, nameof(streams));
        if (streams.Count == 0)
        {
            throw new ArgumentException("At least one stream is required.", nameof(streams));
        }

        var orderedStreams = new List<KeyValuePair<string, byte[]>>(streams);
        orderedStreams.Sort(static (left, right) =>
            StringComparer.OrdinalIgnoreCase.Compare(left.Key, right.Key));
        streams = orderedStreams;

        int entriesPerFatSector = sectorSize / 4;
        int entriesPerDirSector = sectorSize / CfbConstants.DirEntrySize;
        int entriesPerDifatSector = entriesPerFatSector - 1;

        // ── Lay out sectors ────────────────────────────────────────────
        // Order:
        //   0..(numDirSectors-1)        : directory sectors
        //   next                        : stream payload sectors (1 chain per stream)
        //   FAT sectors are computed last and inserted at the end of the file
        //   layout (their sector indices are then reserved in the FAT itself).

        int dirEntryCount = 1 + streams.Count; // root + N streams
        int numDirSectors = (dirEntryCount + entriesPerDirSector - 1) / entriesPerDirSector;

        // Stream sector counts and start sectors.
        var streamStart = new int[streams.Count];
        var streamSectors = new int[streams.Count];
        int sectorCursor = numDirSectors;
        for (int i = 0; i < streams.Count; i++)
        {
            int len = streams[i].Value?.Length ?? 0;
            if (requireRegularFatStreams && len > 0 && len < miniStreamCutoff)
            {
                throw new ArgumentException(
                    $"Stream '{streams[i].Key}' is {len} bytes; Office-crypto CFB output requires non-empty streams to be at least {miniStreamCutoff} bytes.",
                    nameof(streams));
            }

            int sectors = len == 0 ? 0 : (len + sectorSize - 1) / sectorSize;
            streamSectors[i] = sectors;
            streamStart[i] = sectors == 0 ? unchecked((int)CfbConstants.EndOfChain) : sectorCursor;
            sectorCursor += sectors;
        }

        // Total non-FAT / non-DIFAT sectors. We must size FAT to also cover
        // its own sectors plus any DIFAT extension sectors.
        int dataSectors = sectorCursor;

        // Iterate to find a stable (numFatSectors, numDifatSectors,
        // totalSectors) tuple where the FAT can map every sector of the file
        // and the DIFAT can address every FAT sector.
        int numFatSectors = 0;
        int numDifatSectors = 0;
        int totalSectors;
        while (true)
        {
            totalSectors = dataSectors + numFatSectors + numDifatSectors;
            int needed = (totalSectors + entriesPerFatSector - 1) / entriesPerFatSector;
            int neededDifat = needed <= CfbConstants.MaxHeaderDifatEntries
                ? 0
                : (needed - CfbConstants.MaxHeaderDifatEntries + entriesPerDifatSector - 1) / entriesPerDifatSector;
            if (needed == numFatSectors && neededDifat == numDifatSectors)
            {
                break;
            }

            numFatSectors = needed;
            numDifatSectors = neededDifat;
        }

        // FAT sectors follow all data sectors; DIFAT extension sectors follow FAT.
        var fatSectorIds = new int[numFatSectors];
        for (int i = 0; i < numFatSectors; i++)
        {
            fatSectorIds[i] = dataSectors + i;
        }

        var difatSectorIds = new int[numDifatSectors];
        for (int i = 0; i < numDifatSectors; i++)
        {
            difatSectorIds[i] = dataSectors + numFatSectors + i;
        }

        // ── Allocate the file buffer ───────────────────────────────────
        long fileSize = sectorSize + ((long)totalSectors * sectorSize);
        if (fileSize > int.MaxValue)
        {
            throw new NotSupportedException(
                $"Compound file output would exceed 2 GiB ({fileSize:N0} bytes); not supported.");
        }

        byte[] file = new byte[fileSize];

        // ── Build the FAT (in a flat array first, then lay into sectors) ─
        var fat = new uint[(long)numFatSectors * entriesPerFatSector];
        for (long i = 0; i < fat.LongLength; i++)
        {
            fat[i] = CfbConstants.FreeSect;
        }

        // Mark FAT sectors themselves.
        for (int i = 0; i < numFatSectors; i++)
        {
            fat[fatSectorIds[i]] = CfbConstants.FatSect;
        }

        for (int i = 0; i < numDifatSectors; i++)
        {
            fat[difatSectorIds[i]] = CfbConstants.DifSect;
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
        WriteHeader(
            file,
            fatSectorIds,
            firstDirSector: 0,
            numDirSectors: numDirSectors,
            difatSectorIds: difatSectorIds,
            sectorShift: sectorShift,
            majorVersion: majorVersion,
            miniStreamCutoff: miniStreamCutoff);

        // ── Directory entries ─────────────────────────────────────────
        // Layout: index 0 = Root Entry, indices 1..N = streams in sorted order.
        // Red-black tree: root.child = 1; each stream entry's right sibling
        // points at the next greater name. The last entry's right sibling = FREESECT.
        int dirOffset = SectorOffset(0, sectorSize);
        WriteRootEntry(file, dirOffset, child: 1);
        for (int i = 0; i < streams.Count; i++)
        {
            uint right = (i == streams.Count - 1) ? CfbConstants.FreeSect : (uint)(i + 2);
            WriteStreamEntry(
                file,
                dirOffset + ((i + 1) * CfbConstants.DirEntrySize),
                streams[i].Key,
                startSector: unchecked((uint)streamStart[i]),
                size: streams[i].Value?.Length ?? 0,
                leftSibling: CfbConstants.FreeSect,
                rightSibling: right);
        }

        // Zero-fill any leftover directory entries (unused slots).
        int totalDirEntries = numDirSectors * entriesPerDirSector;
        for (int i = dirEntryCount; i < totalDirEntries; i++)
        {
            WriteUnusedEntry(file, dirOffset + (i * CfbConstants.DirEntrySize));
        }

        // ── Stream payloads ───────────────────────────────────────────
        for (int i = 0; i < streams.Count; i++)
        {
            byte[]? payload = streams[i].Value;
            if (payload == null || payload.Length == 0)
            {
                continue;
            }

            Buffer.BlockCopy(payload, 0, file, SectorOffset(streamStart[i], sectorSize), payload.Length);
        }

        // ── FAT sectors (written last so they reflect every chain marked above) ──
        for (int i = 0; i < numFatSectors; i++)
        {
            int sectorOff = SectorOffset(fatSectorIds[i], sectorSize);
            for (int j = 0; j < entriesPerFatSector; j++)
            {
                long fatIdx = ((long)i * entriesPerFatSector) + j;
                uint v = fatIdx < fat.LongLength ? fat[fatIdx] : CfbConstants.FreeSect;
                BinaryPrimitives.WriteUInt32LittleEndian(
                    file.AsSpan(sectorOff + (j * 4), 4),
                    v);
            }
        }

        // ── DIFAT extension sectors ────────────────────────────────────────
        for (int i = 0; i < numDifatSectors; i++)
        {
            int sectorOff = SectorOffset(difatSectorIds[i], sectorSize);
            int fatIndexBase = CfbConstants.MaxHeaderDifatEntries + (i * entriesPerDifatSector);
            for (int j = 0; j < entriesPerDifatSector; j++)
            {
                int fatIndex = fatIndexBase + j;
                uint v = fatIndex < fatSectorIds.Length ? (uint)fatSectorIds[fatIndex] : CfbConstants.FreeSect;
                BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(sectorOff + (j * 4), 4), v);
            }

            uint next = i == numDifatSectors - 1 ? CfbConstants.EndOfChain : (uint)difatSectorIds[i + 1];
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(sectorOff + (entriesPerDifatSector * 4), 4), next);
        }

        return file;
    }

    private static void ChainSectors(uint[] fat, int startSector, int sectorCount)
    {
        for (int k = 0; k < sectorCount; k++)
        {
            int sec = startSector + k;
            fat[sec] = (k == sectorCount - 1) ? CfbConstants.EndOfChain : (uint)(sec + 1);
        }
    }

    private static int SectorOffset(int sectorIndex, int sectorSize) =>
        sectorSize + (sectorIndex * sectorSize);

    private static void WriteHeader(
        byte[] file,
        int[] fatSectorIds,
        int firstDirSector,
        int numDirSectors,
        int[] difatSectorIds,
        ushort sectorShift,
        ushort majorVersion,
        uint miniStreamCutoff)
    {
        Span<byte> h = file.AsSpan(0, 512);

        CompoundFileReader.CfbSignature.CopyTo(h);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(CfbConstants.HeaderOffsets.MinorVersion, 2), 0x003E);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(CfbConstants.HeaderOffsets.MajorVersion, 2), majorVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(CfbConstants.HeaderOffsets.ByteOrder, 2), 0xFFFE);  // little-endian
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(CfbConstants.HeaderOffsets.SectorShift, 2), sectorShift);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(CfbConstants.HeaderOffsets.MiniSectorShift, 2), 0x0006);  // 6 → 64
        BinaryPrimitives.WriteInt32LittleEndian(
            h.Slice(CfbConstants.HeaderOffsets.NumDirSectors, 4),
            majorVersion == CfbConstants.V3.MajorVersion ? 0 : numDirSectors);
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(CfbConstants.HeaderOffsets.NumFatSectors, 4), fatSectorIds.Length);
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(CfbConstants.HeaderOffsets.FirstDirSector, 4), firstDirSector);

        // 0x34 transaction signature stays 0. No mini-FAT is emitted; callers
        // using the standard cutoff must ensure all streams are large enough
        // to be read from the regular FAT.
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(CfbConstants.HeaderOffsets.MiniStreamCutoff, 4), miniStreamCutoff);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(CfbConstants.HeaderOffsets.FirstMiniFatSector, 4), CfbConstants.EndOfChain);
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(CfbConstants.HeaderOffsets.NumMiniFatSectors, 4), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(
            h.Slice(CfbConstants.HeaderOffsets.FirstDifatSector, 4),
            difatSectorIds.Length == 0 ? CfbConstants.EndOfChain : (uint)difatSectorIds[0]);
        BinaryPrimitives.WriteInt32LittleEndian(h.Slice(CfbConstants.HeaderOffsets.NumDifatSectors, 4), difatSectorIds.Length);

        // 0x4C..0xFF: 109-entry header DIFAT.
        for (int i = 0; i < CfbConstants.MaxHeaderDifatEntries; i++)
        {
            uint v = i < fatSectorIds.Length ? (uint)fatSectorIds[i] : CfbConstants.FreeSect;
            BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(CfbConstants.HeaderOffsets.HeaderDifat + (i * 4), 4), v);
        }
    }

    private static void WriteRootEntry(byte[] file, int offset, uint child)
    {
        WriteDirEntry(
            file,
            offset,
            "Root Entry",
            objectType: 5,
            startSector: CfbConstants.EndOfChain,
            sizeLow: 0,
            sizeHigh: 0,
            leftSibling: CfbConstants.FreeSect,
            rightSibling: CfbConstants.FreeSect,
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
            child: CfbConstants.FreeSect);
    }

    private static void WriteUnusedEntry(byte[] file, int offset)
    {
        Span<byte> e = file.AsSpan(offset, CfbConstants.DirEntrySize);
        e.Clear();
        e[0x42] = 0x00; // unallocated
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x44, 4), CfbConstants.FreeSect);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x48, 4), CfbConstants.FreeSect);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x4C, 4), CfbConstants.FreeSect);
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
        Span<byte> e = file.AsSpan(offset, CfbConstants.DirEntrySize);
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
