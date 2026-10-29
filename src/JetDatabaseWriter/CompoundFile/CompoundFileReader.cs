namespace JetDatabaseWriter.CompoundFile;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Minimal reader for the OLE / Microsoft Compound File Binary (CFB / OLE2)
/// format ("MS-CFB"). Parses the FAT, mini-FAT, directory, and exposes
/// named streams as byte arrays for internal container diagnostics and test
/// support. Office compound packages are unsupported as public database inputs.
/// </summary>
internal static class CompoundFileReader
{
    /// <summary>
    /// Returns true when the leading bytes match the OLE compound file magic.
    /// </summary>
    /// <param name="header">Buffer holding at least 8 bytes from offset 0.</param>
    public static bool HasCompoundFileMagic(byte[] header) => header?.Length >= 8 &&
            header.AsSpan(0, 8).SequenceEqual(Constants.CompoundFile.Signature);

    /// <summary>
    /// Parses the compound file and returns its named top-level streams.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <param name="maxBytes">The physical file and aggregate metadata/decoded buffer byte budget. Fixed header/scratch buffers and collection overhead are excluded.</param>
    /// <exception cref="ArgumentOutOfRangeException">The byte budget is not positive.</exception>
    /// <exception cref="JetLimitationException">The container exceeds the configured byte budget.</exception>
    public static async ValueTask<Dictionary<string, byte[]>> ReadStreamsAsync(Stream stream, CancellationToken cancellationToken, int maxBytes = 256 * 1024 * 1024)
    {
        Guard.NotNull(stream, nameof(stream));
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBytes);
#else
        if (maxBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBytes));
        }
#endif

        if (stream.Length > maxBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The compound file exceeds the configured container byte budget.");
        }

        byte[] header = await ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);
        CfbHeader hdr = ParseHeader(header);

        ValidateHeaderCounts(hdr, (stream.Length / hdr.SectorSize) - 1);
        var budget = new BufferBudget(maxBytes);

        byte[] scratch = ArrayPool<byte>.Shared.Rent(hdr.SectorSize);
        try
        {
            if (hdr.SectorSize > header.Length)
            {
                int paddingSize = hdr.SectorSize - header.Length;
                await ReadExactlyAsync(stream, scratch.AsMemory(0, paddingSize), cancellationToken).ConfigureAwait(false);
                ValidateZeroHeaderBytes(scratch.AsSpan(0, paddingSize));
            }

            var ownedSectors = new HashSet<uint>();
            uint[] fat = await BuildFatAsync(stream, header, hdr, scratch, ownedSectors, budget, cancellationToken).ConfigureAwait(false);
            uint[] miniFat = await ReadMiniFatAsync(stream, hdr, fat, ownedSectors, budget, cancellationToken).ConfigureAwait(false);
            byte[] directory = await ReadChainAsync(stream, hdr.FirstDirSector, hdr.SectorSize, fat, ownedSectors, budget, cancellationToken, expectedSectors: hdr.SectorSize == 512 ? -1L : hdr.NumDirSectors).ConfigureAwait(false);
            byte[] miniStream = await ReadMiniStreamAsync(stream, directory, hdr, fat, ownedSectors, budget, cancellationToken).ConfigureAwait(false);

            return await ExtractStreamsAsync(stream, directory, miniStream, hdr, fat, miniFat, ownedSectors, budget, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
    }

    private static async ValueTask<byte[]> ReadHeaderAsync(Stream stream, CancellationToken cancellationToken)
    {
        byte[] header = new byte[512];
        _ = stream.Seek(0, SeekOrigin.Begin);
        await ReadExactlyAsync(stream, header.AsMemory(), cancellationToken).ConfigureAwait(false);

        if (!HasCompoundFileMagic(header))
        {
            throw Corrupt("Stream is not a valid OLE compound file (signature mismatch).");
        }

        return header;
    }

    private static CfbHeader ParseHeader(byte[] header)
    {
        ValidateZeroHeaderBytes(header.AsSpan(0x08, 16)); // Reserved header CLSID.
        ValidateZeroHeaderBytes(header.AsSpan(0x22, 6));
        if (BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.ByteOrder, 2)) != 0xFFFE)
        {
            throw Corrupt("Unsupported CFB byte order.");
        }

        ushort majorVersion = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.MajorVersion, 2));
        ushort sectorShift = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.SectorShift, 2));
        ushort miniSectorShift = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.MiniSectorShift, 2));

        if (sectorShift is not 9 and not 12)
        {
            throw Corrupt($"Unsupported CFB sector shift: {sectorShift}.");
        }

        if (majorVersion is not 3 and not 4)
        {
            throw Corrupt($"Unsupported CFB major version: {majorVersion}.");
        }

        if ((majorVersion == 3 && sectorShift != 9) || (majorVersion == 4 && sectorShift != 12))
        {
            throw Corrupt(
                $"CFB major version {majorVersion} does not match sector shift {sectorShift}.");
        }

        if (miniSectorShift != 6)
        {
            throw Corrupt($"Unsupported CFB mini-sector shift: {miniSectorShift}.");
        }

        uint numDirSectors = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.NumDirSectors, 4));
        if (majorVersion == 3 && numDirSectors != 0)
        {
            throw Corrupt("CFB version 3 must not declare a directory sector count.");
        }

        uint miniStreamCutoff = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.MiniStreamCutoff, 4));

        // The internal v3 diagnostic writer deliberately uses zero to place
        // every stream in the regular FAT. Standard CFB requires 4096.
        if (miniStreamCutoff != Constants.CompoundFile.StandardMiniStreamCutoff
            && (majorVersion != 3 || miniStreamCutoff != 0))
        {
            throw Corrupt($"Unsupported CFB mini-stream cutoff: {miniStreamCutoff}.");
        }

        return new CfbHeader(
            SectorSize: 1 << sectorShift,
            MiniSectorSize: 1 << miniSectorShift,
            NumDirSectors: numDirSectors,
            NumFatSectors: BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.NumFatSectors, 4)),
            FirstDirSector: BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.FirstDirSector, 4)),
            MiniStreamCutoff: miniStreamCutoff,
            FirstMiniFatSector: BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.FirstMiniFatSector, 4)),
            NumMiniFatSectors: BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.NumMiniFatSectors, 4)),
            FirstDifatSector: BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.FirstDifatSector, 4)),
            NumDifatSectors: BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.NumDifatSectors, 4)));
    }

    private static void ValidateZeroHeaderBytes(ReadOnlySpan<byte> bytes)
    {
        foreach (byte value in bytes)
        {
            if (value != 0)
            {
                throw Corrupt("CFB reserved header bytes must be zero.");
            }
        }
    }

    private static async ValueTask<uint[]> BuildFatAsync(
        Stream stream,
        byte[] header,
        CfbHeader hdr,
        byte[] scratch,
        HashSet<uint> ownedSectors,
        BufferBudget budget,
        CancellationToken cancellationToken)
    {
        // Both the temporary sector IDs and materialized FAT remain live
        // during FAT construction. Admit them before either allocation.
        long fatBytes = hdr.NumFatSectors * hdr.SectorSize;
        long sectorIdBytes = hdr.NumFatSectors * sizeof(uint);
        budget.Reserve(fatBytes + sectorIdBytes);

        // ── Collect FAT sector ids: first from the header DIFAT, then
        // from any DIFAT extension sectors. ─────────────────────────
        var fatSectorIds = new List<uint>(checked((int)hdr.NumFatSectors));
        const int headerDifatCount = Constants.CompoundFile.MaxHeaderDifatEntries;
        for (int i = 0; i < headerDifatCount; i++)
        {
            uint entry = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(Constants.CompoundFile.HeaderOffsets.HeaderDifat + (i * 4), 4));
            if (entry < Constants.CompoundFile.FatSectMin)
            {
                AddFatSector(entry, hdr, ownedSectors, fatSectorIds);
            }
        }

        uint difatSector = hdr.FirstDifatSector;
        int difatSectorsRead = 0;
        int entriesPerDifatSector = (hdr.SectorSize / 4) - 1;
        var visitedDifat = new HashSet<uint>();
        while (difatSector != Constants.CompoundFile.EndOfChain
            && difatSector != Constants.CompoundFile.FreeSect
            && difatSectorsRead < hdr.NumDifatSectors)
        {
            if (!visitedDifat.Add(difatSector))
            {
                throw Corrupt($"CFB DIFAT chain contains a cycle at sector {difatSector}.");
            }

            ClaimSector(difatSector, ownedSectors, "DIFAT");
            await ReadSectorIntoAsync(stream, difatSector, scratch, hdr.SectorSize, cancellationToken).ConfigureAwait(false);
            for (int i = 0; i < entriesPerDifatSector; i++)
            {
                uint entry = BinaryPrimitives.ReadUInt32LittleEndian(scratch.AsSpan(i * 4));
                if (entry < Constants.CompoundFile.FatSectMin)
                {
                    AddFatSector(entry, hdr, ownedSectors, fatSectorIds);
                }
            }

            difatSector = BinaryPrimitives.ReadUInt32LittleEndian(scratch.AsSpan(hdr.SectorSize - 4));
            difatSectorsRead++;
        }

        if (difatSectorsRead != hdr.NumDifatSectors
            || difatSector is not Constants.CompoundFile.EndOfChain and not Constants.CompoundFile.FreeSect
            || fatSectorIds.Count != hdr.NumFatSectors)
        {
            throw Corrupt("CFB FAT or DIFAT sector count contradicts its chain.");
        }

        // ── Materialise the FAT itself by reading each FAT sector. ──
        int entriesPerSector = hdr.SectorSize / 4;
        uint[] fat = new uint[fatSectorIds.Count * entriesPerSector];
        for (int i = 0; i < fatSectorIds.Count; i++)
        {
            await ReadSectorIntoAsync(stream, fatSectorIds[i], scratch, hdr.SectorSize, cancellationToken).ConfigureAwait(false);
            int baseIndex = i * entriesPerSector;
            for (int j = 0; j < entriesPerSector; j++)
            {
                fat[baseIndex + j] = BinaryPrimitives.ReadUInt32LittleEndian(scratch.AsSpan(j * 4));
            }
        }

        budget.Release(sectorIdBytes);
        return fat;
    }

    private static async ValueTask<uint[]> ReadMiniFatAsync(
        Stream stream,
        CfbHeader hdr,
        uint[] fat,
        HashSet<uint> ownedSectors,
        BufferBudget budget,
        CancellationToken cancellationToken)
    {
        if (hdr.NumMiniFatSectors == 0)
        {
            return [];
        }

        // Conversion needs both the raw bytes and uint entries simultaneously.
        budget.Check(hdr.NumMiniFatSectors * hdr.SectorSize * 2);
        byte[] miniFatBytes = await ReadChainAsync(stream, hdr.FirstMiniFatSector, hdr.SectorSize, fat, ownedSectors, budget, cancellationToken, expectedSectors: hdr.NumMiniFatSectors).ConfigureAwait(false);
        budget.Reserve(miniFatBytes.Length);
        int count = miniFatBytes.Length / 4;
        uint[] miniFat = new uint[count];
        for (int i = 0; i < count; i++)
        {
            miniFat[i] = BinaryPrimitives.ReadUInt32LittleEndian(miniFatBytes.AsSpan(i * 4));
        }

        budget.Release(miniFatBytes.Length);
        return miniFat;
    }

    private static async ValueTask<byte[]> ReadMiniStreamAsync(
        Stream stream,
        byte[] directory,
        CfbHeader hdr,
        uint[] fat,
        HashSet<uint> ownedSectors,
        BufferBudget budget,
        CancellationToken cancellationToken)
    {
        if (directory.Length < Constants.CompoundFile.DirEntrySize)
        {
            throw Corrupt("CFB directory has no complete root entry.");
        }

        // Root entry holds the start sector + size of the mini stream.
        uint rootStart = BinaryPrimitives.ReadUInt32LittleEndian(directory.AsSpan(0x74));

        // Per MS-CFB §2.6.1, the high 32 bits of the stream-size field MUST
        // be ignored when the major version is 3 (512-byte sectors); some
        // writers leave garbage there.
        long rootSize = hdr.SectorSize == 512
            ? BinaryPrimitives.ReadUInt32LittleEndian(directory.AsSpan(0x78))
            : (long)BinaryPrimitives.ReadUInt64LittleEndian(directory.AsSpan(0x78, 8));

        if (rootStart == Constants.CompoundFile.EndOfChain
            || rootStart == Constants.CompoundFile.FreeSect
            || rootSize <= 0)
        {
            return [];
        }

        // Pass the exact size so the chain reader can allocate the final
        // buffer once and skip a trailing Array.Resize allocation+copy.
        return await ReadChainAsync(stream, rootStart, hdr.SectorSize, fat, ownedSectors, budget, cancellationToken, rootSize).ConfigureAwait(false);
    }

    private static async ValueTask<Dictionary<string, byte[]>> ExtractStreamsAsync(
        Stream stream,
        byte[] directory,
        byte[] miniStream,
        CfbHeader hdr,
        uint[] fat,
        uint[] miniFat,
        HashSet<uint> ownedSectors,
        BufferBudget budget,
        CancellationToken cancellationToken)
    {
        int dirCount = directory.Length / Constants.CompoundFile.DirEntrySize;

        // Upper-bound the capacity to the directory entry count so the
        // dictionary never has to rehash while we walk the directory.
        var streams = new Dictionary<string, byte[]>(dirCount, StringComparer.Ordinal);
        var ownedMiniSectors = new HashSet<uint>();
        for (int i = 0; i < dirCount; i++)
        {
            int off = i * Constants.CompoundFile.DirEntrySize;

            // 0 = unallocated, 1 = storage, 2 = stream, 5 = root
            if (directory[off + 0x42] != 2)
            {
                continue;
            }

            ReadOnlySpan<byte> entry = directory.AsSpan(off, Constants.CompoundFile.DirEntrySize);
            ushort nameLen = BinaryPrimitives.ReadUInt16LittleEndian(entry.Slice(0x40, 2));
            if (nameLen is < 2 or > 64 || (nameLen & 1) != 0)
            {
                throw Corrupt("CFB stream name has an invalid UTF-16 byte length.");
            }

            if (entry[nameLen - 2] != 0 || entry[nameLen - 1] != 0)
            {
                throw Corrupt("CFB stream name is not NUL-terminated.");
            }

            // nameLen includes the trailing UTF-16 NUL.
            string name = Encoding.Unicode.GetString(directory, off, nameLen - 2);
            uint startSector = BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(0x74, 4));

            // Stream size is stored as a single little-endian uint64 spanning
            // 0x78..0x7F; for v3 (512-byte sectors) the high 32 bits MUST be
            // ignored per MS-CFB §2.6.1 (some writers leave garbage there).
            long size = hdr.SectorSize == 512
                ? BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(0x78, 4))
                : (long)BinaryPrimitives.ReadUInt64LittleEndian(entry.Slice(0x78, 8));

            if (size <= 0)
            {
                streams[name] = [];
                continue;
            }

            budget.Check(size);

            // Pass the exact size so the chain readers can size the
            // destination buffer precisely and avoid a tail Array.Resize.
            streams[name] = size < hdr.MiniStreamCutoff
                ? ReadMiniChain(miniStream, startSector, hdr.MiniSectorSize, miniFat, ownedMiniSectors, size, budget, cancellationToken)
                : await ReadChainAsync(stream, startSector, hdr.SectorSize, fat, ownedSectors, budget, cancellationToken, size).ConfigureAwait(false);
        }

        return streams;
    }

    private static async ValueTask ReadSectorIntoAsync(Stream stream, uint sectorIndex, byte[] buffer, int sectorSize, CancellationToken cancellationToken)
    {
        // Header occupies the first sector position (512 bytes for v3, padded
        // to 4096 for v4); sector index 0 begins immediately after that.
        long offset = ((long)sectorIndex + 1) * sectorSize;
        if (!BinaryBuffer.IsRange(stream.Length, offset, sectorSize))
        {
            throw Corrupt("CFB sector points beyond the physical file.");
        }

        _ = stream.Seek(offset, SeekOrigin.Begin);
        await ReadExactlyAsync(stream, buffer.AsMemory(0, sectorSize), cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<byte[]> ReadChainAsync(
        Stream stream,
        uint startSector,
        int sectorSize,
        uint[] fat,
        HashSet<uint> ownedSectors,
        BufferBudget budget,
        CancellationToken cancellationToken,
        long exactSize = -1,
        long expectedSectors = -1)
    {
        // Pre-walk the FAT to compute the chain length so the destination
        // buffer can be sized exactly. This avoids both per-sector array
        // allocations and a final List<byte[]> → byte[] block-copy pass,
        // and (when exactSize is supplied) skips the trailing Array.Resize.
        int chainLength = WalkChainLength(startSector, fat, "FAT", (stream.Length / sectorSize) - 1, ownedSectors, cancellationToken);
        if (expectedSectors >= 0 && chainLength != expectedSectors)
        {
            throw Corrupt("CFB metadata sector count contradicts its chain.");
        }

        byte[] result = AllocateChainBuffer(chainLength, sectorSize, exactSize, budget);

        uint sector = startSector;
        int dstOffset = 0;
        while (dstOffset < result.Length)
        {
            // Coalesce a run of physically contiguous sectors so a single
            // Seek + ReadAsync covers as much of the chain as possible. For
            // files written in one pass this is typically the entire chain,
            // collapsing N seeks/reads into one.
            int remaining = result.Length - dstOffset;
            (uint runStart, int runSectors, uint next) = CoalesceRun(sector, fat, sectorSize, remaining);

            // Header occupies the first sector position; sector index 0
            // begins immediately after that.
            long offset = ((long)runStart + 1) * sectorSize;
            _ = stream.Seek(offset, SeekOrigin.Begin);

            // For a final, partial sector we only read the logical bytes
            // we actually need — the disk sector's trailing padding is
            // simply left unread, so no scratch/pool buffer is required.
            int toRead = Math.Min(runSectors * sectorSize, remaining);
            await ReadExactlyAsync(stream, result.AsMemory(dstOffset, toRead), cancellationToken).ConfigureAwait(false);
            dstOffset += toRead;
            sector = next;
        }

        return result;
    }

    private static byte[] ReadMiniChain(byte[] miniStream, uint startSector, int miniSectorSize, uint[] miniFat, HashSet<uint> ownedSectors, long exactSize, BufferBudget budget, CancellationToken cancellationToken)
    {
        // Pre-walk the mini-FAT to count the chain so we can size the
        // result buffer exactly and skip the intermediate index list.
        int chainLength = WalkChainLength(startSector, miniFat, "mini-FAT", miniStream.Length / miniSectorSize, ownedSectors, cancellationToken);
        byte[] result = AllocateChainBuffer(chainLength, miniSectorSize, exactSize, budget);

        uint sector = startSector;
        int dstOffset = 0;
        while (dstOffset < result.Length)
        {
            // Coalesce a run of contiguous mini-sectors into a single
            // Buffer.BlockCopy. Mini-streams are typically small but this
            // still removes a per-sector loop iteration in the common case.
            int remaining = result.Length - dstOffset;
            (uint runStart, int runSectors, uint next) = CoalesceRun(sector, miniFat, miniSectorSize, remaining);

            long offset = runStart * miniSectorSize;
            long runBytes = (long)runSectors * miniSectorSize;
            if (offset + runBytes > miniStream.Length)
            {
                throw Corrupt("CFB mini-sector offset exceeds mini-stream size.");
            }

            // Trailing partial mini-sector copies only the logical bytes
            // that fit; the rest of the on-disk sector is discarded.
            int copy = Math.Min((int)runBytes, remaining);
            Buffer.BlockCopy(miniStream, (int)offset, result, dstOffset, copy);
            dstOffset += copy;
            sector = next;
        }

        return result;
    }

    /// <summary>
    /// Starting at <paramref name="sector"/>, walks forward through the FAT
    /// while links are physically contiguous (next == current + 1) and
    /// returns the coalesced run plus the FAT link that follows it. The run
    /// is capped at the number of sectors needed to satisfy
    /// <paramref name="remaining"/> bytes, so callers can stop early on the
    /// final partial sector.
    /// </summary>
    /// <param name="sector">The sector.</param>
    /// <param name="fat">The FAT sector table.</param>
    /// <param name="sectorSize">The sector size.</param>
    /// <param name="remaining">The remaining.</param>
    private static (uint RunStart, int RunSectors, uint Next) CoalesceRun(
        uint sector, uint[] fat, int sectorSize, int remaining)
    {
        uint runStart = sector;
        int runSectors = 1;
        uint next = fat[sector];
        int maxRunSectors = checked((int)(((long)remaining + sectorSize - 1) / sectorSize));
        while (runSectors < maxRunSectors && next == sector + 1 && next < fat.Length)
        {
            sector = next;
            next = fat[sector];
            runSectors++;
        }

        return (runStart, runSectors, next);
    }

    /// <summary>
    /// Walks a FAT/mini-FAT chain starting at <paramref name="startSector"/> and
    /// returns its length in sectors, validating that each link is in range and
    /// that the chain neither loops, shares sectors with another chain, nor exceeds the table size.
    /// </summary>
    /// <param name="startSector">The start sector.</param>
    /// <param name="fat">The FAT sector table.</param>
    /// <param name="fatKind">The fat kind.</param>
    /// <param name="physicalSectorCount">The number of sectors physically present.</param>
    /// <param name="ownedSectors">Sectors already claimed by metadata or another chain in this address space.</param>
    /// <param name="cancellationToken">A token used to cancel traversal.</param>
    /// <exception cref="JetCorruptDataException">The sector chain is out of range, cyclic, or shares sectors with another chain.</exception>
    private static int WalkChainLength(uint startSector, uint[] fat, string fatKind, long physicalSectorCount, HashSet<uint> ownedSectors, CancellationToken cancellationToken)
    {
        int count = 0;
        uint cur = startSector;
        while (cur is not Constants.CompoundFile.EndOfChain
            and not Constants.CompoundFile.FreeSect)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (cur >= fat.Length || cur >= physicalSectorCount)
            {
                throw Corrupt($"CFB {fatKind} sector index {cur} exceeds {fatKind} length {fat.Length}.");
            }

            if (!ownedSectors.Add(cur))
            {
                throw Corrupt($"CFB {fatKind} sector {cur} forms a cycle or overlaps another chain.");
            }

            count++;
            cur = fat[cur];
        }

        return count;
    }

    /// <summary>
    /// Allocates the destination buffer for a chain of <paramref name="chainLength"/>
    /// sectors. When <paramref name="exactSize"/> is supplied and smaller than the
    /// raw sector capacity the buffer is sized to the exact logical length, so no
    /// trailing Array.Resize is needed once the chain has been read.
    /// </summary>
    /// <param name="chainLength">The chain length.</param>
    /// <param name="sectorSize">The sector size.</param>
    /// <param name="exactSize">The exact size.</param>
    /// <param name="budget">The live buffer byte budget.</param>
    /// <exception cref="JetCorruptDataException">The logical size exceeds the chain capacity or buffer limit.</exception>
    private static byte[] AllocateChainBuffer(int chainLength, int sectorSize, long exactSize, BufferBudget budget)
    {
        long capacity = (long)chainLength * sectorSize;
        long logicalLength = exactSize >= 0 ? exactSize : capacity;
        if (logicalLength > capacity || logicalLength > int.MaxValue)
        {
            throw Corrupt("CFB stream length exceeds its chain capacity or supported buffer size.");
        }

        budget.Reserve(logicalLength);
        int length = checked((int)logicalLength);
        return new byte[length];
    }

    private static void ClaimSector(uint sector, HashSet<uint> ownedSectors, string sectorKind)
    {
        if (!ownedSectors.Add(sector))
        {
            throw Corrupt($"CFB {sectorKind} sector {sector} overlaps another metadata sector.");
        }
    }

    private static void ValidateHeaderCounts(CfbHeader hdr, long physicalSectors)
    {
        if (hdr.NumDirSectors > physicalSectors
            || hdr.NumFatSectors > physicalSectors
            || hdr.NumDifatSectors > physicalSectors
            || hdr.NumMiniFatSectors > physicalSectors
            || (long)hdr.NumDirSectors + hdr.NumFatSectors + hdr.NumDifatSectors + hdr.NumMiniFatSectors > physicalSectors)
        {
            throw Corrupt("CFB metadata sector counts exceed the physical file.");
        }

        if (hdr.SectorSize == 4096)
        {
            ValidateChainStart(hdr.FirstDirSector, hdr.NumDirSectors, physicalSectors);
        }

        ValidateChainStart(hdr.FirstDifatSector, hdr.NumDifatSectors, physicalSectors);
        ValidateChainStart(hdr.FirstMiniFatSector, hdr.NumMiniFatSectors, physicalSectors);
    }

    private static void ValidateChainStart(uint start, uint count, long physicalSectors)
    {
        bool absent = start is Constants.CompoundFile.EndOfChain or Constants.CompoundFile.FreeSect;
        bool hasSectors = count != 0;
        if (hasSectors == absent || (!absent && start >= physicalSectors))
        {
            throw Corrupt("CFB metadata chain start contradicts its sector count or physical file.");
        }
    }

    private static void AddFatSector(uint sector, CfbHeader hdr, HashSet<uint> ownedSectors, List<uint> fatSectorIds)
    {
        if (fatSectorIds.Count >= hdr.NumFatSectors)
        {
            throw Corrupt("CFB DIFAT contains more FAT sectors than the header declares.");
        }

        ClaimSector(sector, ownedSectors, "FAT");
        fatSectorIds.Add(sector);
    }

    private static JetCorruptDataException Corrupt(string message) => new(
        JetErrorCode.CorruptCompoundFile,
        message,
        new JetErrorInfo { ObjectName = "CompoundFile", Reason = message });

    private static async ValueTask ReadExactlyAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        int read = await StreamRead.ReadAtLeastAsync(stream, buffer, buffer.Length, throwOnEndOfStream: false, cancellationToken).ConfigureAwait(false);
        if (read != buffer.Length)
        {
            throw Corrupt("The compound file is truncated.");
        }
    }

    private sealed class BufferBudget(int maxBytes)
    {
        private long remaining = maxBytes;

        public void Check(long bytes)
        {
            if (bytes > this.remaining)
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Compound file buffers exceed the configured aggregate byte budget.");
            }
        }

        public void Reserve(long bytes)
        {
            this.Check(bytes);
            this.remaining -= bytes;
        }

        public void Release(long bytes) => this.remaining += bytes;
    }

    private readonly record struct CfbHeader(
        int SectorSize,
        int MiniSectorSize,
        uint NumDirSectors,
        uint NumFatSectors,
        uint FirstDirSector,
        uint MiniStreamCutoff,
        uint FirstMiniFatSector,
        uint NumMiniFatSectors,
        uint FirstDifatSector,
        uint NumDifatSectors);
}
