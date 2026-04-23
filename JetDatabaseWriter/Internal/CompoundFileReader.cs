namespace JetDatabaseWriter;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Minimal reader for the OLE / Microsoft Compound File Binary (CFB / OLE2)
/// format ("MS-CFB"). Parses the FAT, mini-FAT, directory, and exposes
/// named top-level streams as byte arrays. Used to unwrap Office Crypto
/// API ("Agile") encrypted .accdb files which embed the encrypted database
/// inside a compound document.
/// </summary>
internal static class CompoundFileReader
{
    private const uint FreeSect = 0xFFFFFFFFu;
    private const uint EndOfChain = 0xFFFFFFFEu;
    private const int DirEntrySize = 128;
    private const uint FatSectMin = 0xFFFFFFFAu; // entries >= this are reserved markers

    /// <summary>
    /// Returns true when the leading bytes match the OLE compound file magic.
    /// </summary>
    /// <param name="header">Buffer holding at least 8 bytes from offset 0.</param>
    public static bool HasCompoundFileMagic(byte[] header)
    {
        return header != null && header.Length >= 8 &&
            header[0] == 0xD0 && header[1] == 0xCF && header[2] == 0x11 && header[3] == 0xE0 &&
            header[4] == 0xA1 && header[5] == 0xB1 && header[6] == 0x1A && header[7] == 0xE1;
    }

    /// <summary>
    /// Parses the compound file and returns its named top-level streams.
    /// </summary>
    public static async ValueTask<Dictionary<string, byte[]>> ReadStreamsAsync(Stream stream, CancellationToken cancellationToken)
    {
        Guard.NotNull(stream, nameof(stream));

        byte[] header = await ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);
        CfbHeader hdr = ParseHeader(header);

        byte[] scratch = ArrayPool<byte>.Shared.Rent(hdr.SectorSize);
        try
        {
            uint[] fat = await BuildFatAsync(stream, header, hdr, scratch, cancellationToken).ConfigureAwait(false);
            uint[] miniFat = await ReadMiniFatAsync(stream, hdr, fat, cancellationToken).ConfigureAwait(false);
            byte[] directory = await ReadChainAsync(stream, hdr.FirstDirSector, hdr.SectorSize, fat, cancellationToken).ConfigureAwait(false);
            byte[] miniStream = await ReadMiniStreamAsync(stream, directory, hdr, fat, cancellationToken).ConfigureAwait(false);

            return await ExtractStreamsAsync(stream, directory, miniStream, hdr, fat, miniFat, cancellationToken).ConfigureAwait(false);
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
        await ReadExactAsync(stream, header, 0, header.Length, cancellationToken).ConfigureAwait(false);

        if (!HasCompoundFileMagic(header))
        {
            throw new InvalidDataException("Stream is not a valid OLE compound file (signature mismatch).");
        }

        return header;
    }

    private static CfbHeader ParseHeader(byte[] header)
    {
        ushort majorVersion = ReadUInt16Le(header, 0x1A);
        ushort sectorShift = ReadUInt16Le(header, 0x1E);
        ushort miniSectorShift = ReadUInt16Le(header, 0x20);

        if (sectorShift != 9 && sectorShift != 12)
        {
            throw new InvalidDataException($"Unsupported CFB sector shift: {sectorShift}.");
        }

        if ((majorVersion == 3 && sectorShift != 9) || (majorVersion == 4 && sectorShift != 12))
        {
            throw new InvalidDataException(
                $"CFB major version {majorVersion} does not match sector shift {sectorShift}.");
        }

        return new CfbHeader(
            SectorSize: 1 << sectorShift,
            MiniSectorSize: 1 << miniSectorShift,
            NumFatSectors: ReadUInt32Le(header, 0x2C),
            FirstDirSector: ReadUInt32Le(header, 0x30),
            MiniStreamCutoff: ReadUInt32Le(header, 0x38),
            FirstMiniFatSector: ReadUInt32Le(header, 0x3C),
            NumMiniFatSectors: ReadUInt32Le(header, 0x40),
            FirstDifatSector: ReadUInt32Le(header, 0x44),
            NumDifatSectors: ReadUInt32Le(header, 0x48));
    }

    private static async ValueTask<uint[]> BuildFatAsync(
        Stream stream,
        byte[] header,
        CfbHeader hdr,
        byte[] scratch,
        CancellationToken cancellationToken)
    {
        // ── Collect FAT sector ids: first from the header DIFAT, then
        // from any DIFAT extension sectors. ─────────────────────────
        var fatSectorIds = new List<uint>((int)hdr.NumFatSectors);
        int headerDifatCount = Math.Min(109, (int)hdr.NumFatSectors);
        for (int i = 0; i < headerDifatCount; i++)
        {
            uint entry = ReadUInt32Le(header, 0x4C + (i * 4));
            if (entry < FatSectMin)
            {
                fatSectorIds.Add(entry);
            }
        }

        uint difatSector = hdr.FirstDifatSector;
        int difatSectorsRead = 0;
        int entriesPerDifatSector = (hdr.SectorSize / 4) - 1;
        while (difatSector != EndOfChain && difatSector != FreeSect && difatSectorsRead < hdr.NumDifatSectors)
        {
            await ReadSectorIntoAsync(stream, difatSector, scratch, hdr.SectorSize, cancellationToken).ConfigureAwait(false);
            for (int i = 0; i < entriesPerDifatSector; i++)
            {
                uint entry = ReadUInt32Le(scratch, i * 4);
                if (entry < FatSectMin && fatSectorIds.Count < (int)hdr.NumFatSectors)
                {
                    fatSectorIds.Add(entry);
                }
            }

            difatSector = ReadUInt32Le(scratch, hdr.SectorSize - 4);
            difatSectorsRead++;
        }

        // ── Materialise the FAT itself by reading each FAT sector. ──
        int entriesPerSector = hdr.SectorSize / 4;
        var fat = new uint[fatSectorIds.Count * entriesPerSector];
        for (int i = 0; i < fatSectorIds.Count; i++)
        {
            await ReadSectorIntoAsync(stream, fatSectorIds[i], scratch, hdr.SectorSize, cancellationToken).ConfigureAwait(false);
            int baseIndex = i * entriesPerSector;
            for (int j = 0; j < entriesPerSector; j++)
            {
                fat[baseIndex + j] = ReadUInt32Le(scratch, j * 4);
            }
        }

        return fat;
    }

    private static async ValueTask<uint[]> ReadMiniFatAsync(
        Stream stream,
        CfbHeader hdr,
        uint[] fat,
        CancellationToken cancellationToken)
    {
        if (hdr.NumMiniFatSectors == 0 || hdr.FirstMiniFatSector == EndOfChain || hdr.FirstMiniFatSector == FreeSect)
        {
            return [];
        }

        byte[] miniFatBytes = await ReadChainAsync(stream, hdr.FirstMiniFatSector, hdr.SectorSize, fat, cancellationToken).ConfigureAwait(false);
        int count = miniFatBytes.Length / 4;
        var miniFat = new uint[count];
        for (int i = 0; i < count; i++)
        {
            miniFat[i] = ReadUInt32Le(miniFatBytes, i * 4);
        }

        return miniFat;
    }

    private static async ValueTask<byte[]> ReadMiniStreamAsync(
        Stream stream,
        byte[] directory,
        CfbHeader hdr,
        uint[] fat,
        CancellationToken cancellationToken)
    {
        // Root entry holds the start sector + size of the mini stream.
        uint rootStart = ReadUInt32Le(directory, 0x74);
        long rootSize = ReadUInt32Le(directory, 0x78) | ((long)ReadUInt32Le(directory, 0x7C) << 32);

        if (rootStart == EndOfChain || rootStart == FreeSect || rootSize <= 0)
        {
            return [];
        }

        // Pass the exact size so the chain reader can allocate the final
        // buffer once and skip a trailing Array.Resize allocation+copy.
        return await ReadChainAsync(stream, rootStart, hdr.SectorSize, fat, cancellationToken, rootSize).ConfigureAwait(false);
    }

    private static async ValueTask<Dictionary<string, byte[]>> ExtractStreamsAsync(
        Stream stream,
        byte[] directory,
        byte[] miniStream,
        CfbHeader hdr,
        uint[] fat,
        uint[] miniFat,
        CancellationToken cancellationToken)
    {
        int dirCount = directory.Length / DirEntrySize;

        // Upper-bound the capacity to the directory entry count so the
        // dictionary never has to rehash while we walk the directory.
        var streams = new Dictionary<string, byte[]>(dirCount, StringComparer.Ordinal);
        for (int i = 0; i < dirCount; i++)
        {
            int off = i * DirEntrySize;

            // 0 = unallocated, 1 = storage, 2 = stream, 5 = root
            if (directory[off + 0x42] != 2)
            {
                continue;
            }

            ReadOnlySpan<byte> entry = directory.AsSpan(off, DirEntrySize);
            ushort nameLen = BinaryPrimitives.ReadUInt16LittleEndian(entry.Slice(0x40, 2));
            if (nameLen == 0 || nameLen > 64)
            {
                continue;
            }

            // nameLen includes the trailing UTF-16 NUL.
            string name = Encoding.Unicode.GetString(directory, off, nameLen - 2);
            uint startSector = BinaryPrimitives.ReadUInt32LittleEndian(entry.Slice(0x74, 4));

            // Stream size is stored as a single little-endian uint64 spanning
            // 0x78..0x7F; cast to long since we cap usable size at 2 GiB anyway.
            long size = (long)BinaryPrimitives.ReadUInt64LittleEndian(entry.Slice(0x78, 8));

            if (size <= 0)
            {
                streams[name] = [];
                continue;
            }

            // Pass the exact size so the chain readers can size the
            // destination buffer precisely and avoid a tail Array.Resize.
            byte[] data = size < hdr.MiniStreamCutoff
                ? ReadMiniChain(miniStream, startSector, hdr.MiniSectorSize, miniFat, size)
                : await ReadChainAsync(stream, startSector, hdr.SectorSize, fat, cancellationToken, size).ConfigureAwait(false);

            streams[name] = data;
        }

        return streams;
    }

    private static async ValueTask ReadSectorIntoAsync(Stream stream, uint sectorIndex, byte[] buffer, int sectorSize, CancellationToken cancellationToken)
    {
        // Header occupies the first sector position (512 bytes for v3, padded
        // to 4096 for v4); sector index 0 begins immediately after that.
        long offset = (sectorIndex + 1) * sectorSize;
        _ = stream.Seek(offset, SeekOrigin.Begin);
        await ReadExactAsync(stream, buffer, 0, sectorSize, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<byte[]> ReadChainAsync(
        Stream stream,
        uint startSector,
        int sectorSize,
        uint[] fat,
        CancellationToken cancellationToken,
        long exactSize = -1)
    {
        // Pre-walk the FAT to compute the chain length so the destination
        // buffer can be sized exactly. This avoids both per-sector array
        // allocations and a final List<byte[]> → byte[] block-copy pass,
        // and (when exactSize is supplied) skips the trailing Array.Resize.
        int chainLength = WalkChainLength(startSector, fat, "FAT");
        byte[] result = AllocateChainBuffer(chainLength, sectorSize, exactSize);

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
            await ReadExactAsync(stream, result, dstOffset, toRead, cancellationToken).ConfigureAwait(false);
            dstOffset += toRead;
            sector = next;
        }

        return result;
    }

    private static byte[] ReadMiniChain(byte[] miniStream, uint startSector, int miniSectorSize, uint[] miniFat, long exactSize = -1)
    {
        // Pre-walk the mini-FAT to count the chain so we can size the
        // result buffer exactly and skip the intermediate index list.
        int chainLength = WalkChainLength(startSector, miniFat, "mini-FAT");
        byte[] result = AllocateChainBuffer(chainLength, miniSectorSize, exactSize);

        uint sector = startSector;
        int dstOffset = 0;
        while (dstOffset < result.Length)
        {
            // Coalesce a run of contiguous mini-sectors into a single
            // Buffer.BlockCopy. Mini-streams are typically small but this
            // still removes a per-sector loop iteration in the common case.
            int remaining = result.Length - dstOffset;
            (uint runStart, int runSectors, uint next) = CoalesceRun(sector, miniFat, miniSectorSize, remaining);

            long offset = (long)runStart * miniSectorSize;
            long runBytes = (long)runSectors * miniSectorSize;
            if (offset + runBytes > miniStream.Length)
            {
                throw new InvalidDataException("CFB mini-sector offset exceeds mini-stream size.");
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
    private static (uint RunStart, int RunSectors, uint Next) CoalesceRun(
        uint sector, uint[] fat, int sectorSize, int remaining)
    {
        uint runStart = sector;
        int runSectors = 1;
        uint next = fat[sector];
        int maxRunSectors = (remaining + sectorSize - 1) / sectorSize;
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
    /// that the chain neither loops nor exceeds the table size.
    /// </summary>
    private static int WalkChainLength(uint startSector, uint[] fat, string fatKind)
    {
        int count = 0;
        int safety = 0;
        int limit = fat.Length + 16;
        uint cur = startSector;
        while (cur != EndOfChain && cur != FreeSect)
        {
            if (cur >= fat.Length)
            {
                throw new InvalidDataException($"CFB {fatKind} sector index {cur} exceeds {fatKind} length {fat.Length}.");
            }

            count++;
            cur = fat[cur];

            if (++safety > limit)
            {
                throw new InvalidDataException($"CFB {fatKind} chain loops or exceeds expected length.");
            }
        }

        return count;
    }

    /// <summary>
    /// Allocates the destination buffer for a chain of <paramref name="chainLength"/>
    /// sectors. When <paramref name="exactSize"/> is supplied and smaller than the
    /// raw sector capacity the buffer is sized to the exact logical length, so no
    /// trailing Array.Resize is needed once the chain has been read.
    /// </summary>
    private static byte[] AllocateChainBuffer(int chainLength, int sectorSize, long exactSize)
    {
        long capacity = (long)chainLength * sectorSize;
        int length = exactSize >= 0 && exactSize < capacity ? (int)exactSize : (int)capacity;
        return new byte[length];
    }

    private static async ValueTask ReadExactAsync(Stream stream, byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int read = 0;
        while (read < count)
        {
            int got = await stream.ReadAsync(buffer.AsMemory(offset + read, count - read), cancellationToken).ConfigureAwait(false);
            if (got == 0)
            {
                throw new EndOfStreamException("Unexpected end of stream while reading CFB data.");
            }

            read += got;
        }
    }

    private static ushort ReadUInt16Le(byte[] buf, int offset) =>
        (ushort)(buf[offset] | (buf[offset + 1] << 8));

    private static uint ReadUInt32Le(byte[] buf, int offset)
    {
        unchecked
        {
            return buf[offset]
                | ((uint)buf[offset + 1] << 8)
                | ((uint)buf[offset + 2] << 16)
                | ((uint)buf[offset + 3] << 24);
        }
    }

    private readonly record struct CfbHeader(
        int SectorSize,
        int MiniSectorSize,
        uint NumFatSectors,
        uint FirstDirSector,
        uint MiniStreamCutoff,
        uint FirstMiniFatSector,
        uint NumMiniFatSectors,
        uint FirstDifatSector,
        uint NumDifatSectors);
}
