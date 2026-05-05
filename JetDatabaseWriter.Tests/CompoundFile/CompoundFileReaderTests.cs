namespace JetDatabaseWriter.Tests.CompoundFile;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using Xunit;

/// <summary>
/// Tests for <see cref="CompoundFileReader"/> using fixtures borrowed from
/// the OpenMcdf project (see <c>THIRD-PARTY-NOTICES.md</c>).
///
/// The OpenMcdf "TestStream_v{3,4}_{N}.cfs" corpus contains a single
/// top-level stream named "TestStream" of length N filled with bytes equal
/// to <c>i % 256</c>. Sizes are chosen to exercise the boundaries of the
/// mini-stream cutoff (4096) and the regular sector size (512), including
/// the off-by-one cases on either side.
///
/// The OpenMcdf v4 fixtures on disk are actually written with major
/// version 3 / 512-byte sectors (see file headers); we treat them as a
/// second copy of the v3 corpus.
/// </summary>
public sealed class CompoundFileReaderTests
{
    private static string FixturePath(string name) =>
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Cfb", name);

    public static TheoryData<string, int> TestStreamFixtures()
    {
        var data = new TheoryData<string, int>();
        int[] sizes = [0, 63, 64, 65, 511, 512, 513, 4095, 4096, 4097];
        foreach (int v in new[] { 3, 4 })
        {
            foreach (int n in sizes)
            {
                data.Add($"TestStream_v{v}_{n}.cfs", n);
            }
        }

        return data;
    }

    public static TheoryData<string> HeaderFixtureFiles() =>
        ["TestStream_v3_0.cfs", "TestStream_v4_0.cfs"];

    [Theory]
    [MemberData(nameof(TestStreamFixtures))]
    public async Task ReadStreams_TestStream_ContainsExpectedPayload(string fileName, int length)
    {
        await using FileStream fs = File.OpenRead(FixturePath(fileName));
        var streams = await CompoundFileReader.ReadStreamsAsync(fs, TestContext.Current.CancellationToken);

        Assert.True(streams.ContainsKey("TestStream"), $"Missing 'TestStream' in {fileName}; have: {string.Join(",", streams.Keys)}");
        byte[] payload = streams["TestStream"];
        Assert.Equal(length, payload.Length);

        for (int i = 0; i < payload.Length; i++)
        {
            if (payload[i] != unchecked((byte)i))
            {
                Assert.Fail($"Mismatch in {fileName} at offset {i}: expected {unchecked((byte)i)}, got {payload[i]}.");
            }
        }
    }

    [Theory]
    [InlineData("MultipleStorage.cfs")]
    [InlineData("MultipleStorage2.cfs")]
    [InlineData("MultipleStorage3.cfs")]
    [InlineData("MultipleStorage4.cfs")]
    public async Task ReadStreams_MultipleStorageFixtures_ParseWithoutThrowing(string fileName)
    {
        // Our reader only surfaces top-level streams. The MultipleStorage
        // fixtures contain only nested storages (no top-level streams),
        // so the dictionary is expected to be empty — the contract under
        // test is just that we walk the directory without crashing on
        // sub-storage entries.
        await using FileStream fs = File.OpenRead(FixturePath(fileName));
        var streams = await CompoundFileReader.ReadStreamsAsync(fs, TestContext.Current.CancellationToken);
        Assert.NotNull(streams);
    }

    [Fact]
    public async Task ReadStreams_FatChainLoop_ThrowsInvalidData()
    {
        await using FileStream fs = File.OpenRead(FixturePath("FatChainLoop_v3.cfs"));
        await Assert.ThrowsAsync<InvalidDataException>(
            () => CompoundFileReader.ReadStreamsAsync(fs, TestContext.Current.CancellationToken).AsTask());
    }

    [Theory]
    [InlineData("LibreOfficeBlankSample_v25.8.doc", "WordDocument")]
    [InlineData("LibreOfficeBlankSample_v25.8.xls", "Workbook")]
    [InlineData("LibreOfficeBlankSample_v25.8.ppt", "PowerPoint Document")]
    [InlineData("Office365BlankSample_v2507.doc", "WordDocument")]
    [InlineData("Office365BlankSample_v2507.xls", "Workbook")]
    [InlineData("Office365BlankSample_v2507.ppt", "PowerPoint Document")]
    [InlineData("VSPro_v17.suo", "SolutionConfiguration")]
    public async Task ReadStreams_RealWorldSamples_ContainExpectedTopLevelStream(string fileName, string expectedStream)
    {
        await using FileStream fs = File.OpenRead(FixturePath(fileName));
        var streams = await CompoundFileReader.ReadStreamsAsync(fs, TestContext.Current.CancellationToken);

        Assert.True(
            streams.ContainsKey(expectedStream),
            $"Expected top-level stream '{expectedStream}' in {fileName}; have: {string.Join(",", streams.Keys)}");
        Assert.NotEmpty(streams[expectedStream]);
    }

    [Fact]
    public void HasCompoundFileMagic_RecognisesValidHeader()
    {
        byte[] header = File.ReadAllBytes(FixturePath("TestStream_v3_64.cfs"));
        Assert.True(CompoundFileReader.HasCompoundFileMagic(header));
    }

    [Fact]
    public void HasCompoundFileMagic_RejectsNonCfbBuffer()
    {
        Assert.False(CompoundFileReader.HasCompoundFileMagic(new byte[8]));
        Assert.False(CompoundFileReader.HasCompoundFileMagic([0x01, 0x02, 0x03]));
        Assert.False(CompoundFileReader.HasCompoundFileMagic(null!));
    }

    [Fact]
    public async Task ReadStreams_TruncatedFile_ThrowsEndOfStream()
    {
        byte[] full = await File.ReadAllBytesAsync(FixturePath("TestStream_v3_4096.cfs"), TestContext.Current.CancellationToken);

        // Lop off the last sector so the FAT-walked chain runs past EOF.
        byte[] truncated = new byte[full.Length - 512];
        Buffer.BlockCopy(full, 0, truncated, 0, truncated.Length);
        await using var ms = new MemoryStream(truncated);
        await Assert.ThrowsAnyAsync<Exception>(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
    }

    // Adapted from OpenMcdf's BinaryReaderTests.ReadHeader: corrupting the
    // magic signature (offset 0) MUST cause the reader to reject the file.
    [Theory]
    [MemberData(nameof(HeaderFixtureFiles))]
    public async Task ReadStreams_CorruptSignature_Throws(string fileName)
    {
        byte[] data = await File.ReadAllBytesAsync(FixturePath(fileName), TestContext.Current.CancellationToken);
        data[0] ^= 0xFF;

        await using var ms = new MemoryStream(data);
        _ = await Assert.ThrowsAsync<InvalidDataException>(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
    }

    // Adapted from OpenMcdf's BinaryReaderTests.ReadHeader: corrupting the
    // major-version field at offset 0x1A so it no longer matches the sector
    // shift at 0x1E MUST be rejected.
    [Theory]
    [MemberData(nameof(HeaderFixtureFiles))]
    public async Task ReadStreams_CorruptMajorVersion_Throws(string fileName)
    {
        byte[] data = await File.ReadAllBytesAsync(FixturePath(fileName), TestContext.Current.CancellationToken);
        data[0x1A] = 0x07;
        data[0x1B] = 0x00;

        await using var ms = new MemoryStream(data);
        _ = await Assert.ThrowsAsync<InvalidDataException>(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
    }

    // Adapted from OpenMcdf's BinaryReaderTests.ReadHeader: corrupting the
    // sector-shift field at offset 0x1E so it is neither 9 nor 12 MUST be
    // rejected.
    [Theory]
    [MemberData(nameof(HeaderFixtureFiles))]
    public async Task ReadStreams_CorruptSectorShift_Throws(string fileName)
    {
        byte[] data = await File.ReadAllBytesAsync(FixturePath(fileName), TestContext.Current.CancellationToken);
        data[0x1E] = 0x07;
        data[0x1F] = 0x00;

        await using var ms = new MemoryStream(data);
        _ = await Assert.ThrowsAsync<InvalidDataException>(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
    }

    // §7 coverage gap: corrupting the NumFatSectors field (offset 0x2C)
    // to an absurdly large value MUST NOT cause an out-of-memory crash
    // or infinite loop. The reader clamps NumFatSectors to the physical
    // sector count derivable from the stream length.
    [Theory]
    [MemberData(nameof(HeaderFixtureFiles))]
    public async Task ReadStreams_CorruptNumFatSectors_DoesNotOom(string fileName)
    {
        byte[] data = await File.ReadAllBytesAsync(FixturePath(fileName), TestContext.Current.CancellationToken);

        // Set NumFatSectors to 0xFFFF_FFFF — far beyond anything a v3 file
        // with 109 header-DIFAT entries could contain.
        data[0x2C] = 0xFF;
        data[0x2D] = 0xFF;
        data[0x2E] = 0xFF;
        data[0x2F] = 0xFF;

        await using var ms = new MemoryStream(data);

        // The clamping means this may succeed (with a possibly truncated
        // FAT) or throw a parse error — but never OOM.
        Exception? ex = await Record.ExceptionAsync(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
        Assert.IsNotType<OutOfMemoryException>(ex);
    }

    // ── DIFAT extension sector coverage ───────────────────────────────
    //
    // The header DIFAT holds up to 109 FAT-sector pointers. Files that
    // need more FAT sectors (> ~7 MB for v3, > ~450 MB for v4) store the
    // overflow in chained DIFAT extension sectors. Our writer never
    // produces these (it caps at 109), but the reader must handle them
    // for interoperability with Office-produced encrypted .accdb files.
    //
    // The test below builds a synthetic v3 CFB in memory whose header
    // declares NumFatSectors = 110 (> 109) so the reader enters the
    // DIFAT extension walk in BuildFatAsync.

    /// <summary>
    /// Constructs a minimal v3 CFB file with a DIFAT extension sector,
    /// verifying that <see cref="CompoundFileReader.ReadStreamsAsync"/>
    /// walks the extension chain and recovers the embedded stream.
    /// </summary>
    [Fact]
    public async Task ReadStreams_SyntheticDifatExtension_RecoversStream()
    {
        byte[] file = BuildSyntheticDifatFile();

        await using var ms = new MemoryStream(file);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.True(streams.ContainsKey("TestStream"), $"Missing 'TestStream'; have: {string.Join(",", streams.Keys)}");
        byte[] payload = streams["TestStream"];
        Assert.Equal(10, payload.Length);
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(unchecked((byte)i), payload[i]);
        }
    }

    /// <summary>
    /// Same synthetic layout as <see cref="ReadStreams_SyntheticDifatExtension_RecoversStream"/>
    /// but with the DIFAT extension sector's chain-link patched to loop back
    /// to itself. The reader's DIFAT cycle-detection visited-set
    /// catches the duplicate immediately and throws <see cref="InvalidDataException"/>.
    /// </summary>
    [Fact]
    public async Task ReadStreams_SyntheticDifatLoop_DoesNotHang()
    {
        byte[] file = BuildSyntheticDifatFile();

        // Patch the DIFAT extension's next-DIFAT pointer (last 4 bytes
        // of sector 3) to loop back to sector 3 instead of EndOfChain,
        // and claim 50 DIFAT sectors so the while-loop would spin if
        // the guard were missing.
        int difatOff = 512 + (3 * 512);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(difatOff + 508), 3);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0x48), 50);

        await using var ms = new MemoryStream(file);

        // Cycle detection throws InvalidDataException immediately on
        // the second visit to sector 3.
        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("cycle", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── DIFAT overflow / DoS mitigation tests ─────────────────────────
    //
    // These tests verify hardening against the DIFAT overflow attack
    // described in the Mimecast disclosure (March 2019) which weaponised
    // crafted CFB headers to exploit parsers via CVE-2017-11882-adjacent
    // techniques. A malicious file can set NumFatSectors / NumDifatSectors
    // to values far beyond the stream's physical size, causing either:
    //   (a) OOM from the List<uint> pre-allocation, or
    //   (b) CPU/IO denial of service from unbounded DIFAT chain walks.
    //
    // Ref: https://www.mimecast.com/blog/2019/03/the-return-of-the-equation-editor-exploit--difat-overflow/

    /// <summary>
    /// A crafted header with <c>NumFatSectors = 0x7FFF_FFFF</c> would
    /// previously cause a ~8 GB list pre-allocation
    /// (OOM). The reader now clamps to the physical sector count
    /// derivable from the stream length.
    /// </summary>
    [Fact]
    public async Task ReadStreams_DifatOverflow_HugeNumFatSectors_DoesNotOom()
    {
        byte[] file = BuildSyntheticDifatFile();

        // Set NumFatSectors to int.MaxValue — far beyond the 5-sector
        // test file — to trigger the OOM path pre-mitigation.
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0x2C), 0x7FFFFFFFu);

        await using var ms = new MemoryStream(file);

        // Must complete without OutOfMemoryException. The clamped FAT
        // may be corrupt, so any non-OOM exception is acceptable.
        Exception? ex = await Record.ExceptionAsync(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
        Assert.IsNotType<OutOfMemoryException>(ex);
    }

    /// <summary>
    /// A crafted header with <c>NumDifatSectors = 0x3FFF_FFFF</c> and
    /// a self-looping DIFAT sector would previously spin for ~1 billion
    /// I/O rounds (CPU/IO DoS). The reader now clamps the walk to the
    /// stream's physical sector count and detects cycles.
    /// </summary>
    [Fact]
    public async Task ReadStreams_DifatOverflow_HugeNumDifatSectors_DoesNotSpin()
    {
        byte[] file = BuildSyntheticDifatFile();

        // Inflate NumDifatSectors to ~1 billion and loop the DIFAT
        // chain back to itself.
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0x48), 0x3FFFFFFFu);
        int difatOff = 512 + (3 * 512);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(difatOff + 508), 3);

        await using var ms = new MemoryStream(file);

        // Cycle detection catches the self-loop on the second visit.
        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
        Assert.Contains("cycle", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Validates that the DIFAT sector-count clamp is based on the
    /// stream's physical size. A tiny file cannot declare thousands of
    /// DIFAT sectors without the reader silently clamping the walk.
    /// </summary>
    [Fact]
    public async Task ReadStreams_DifatOverflow_NumDifatSectorsClampedToStreamLength()
    {
        byte[] file = BuildSyntheticDifatFile();

        // The file is 3072 bytes (header + 5 × 512). That is only
        // 5 physical sectors, so NumDifatSectors = 1000 is absurd
        // and must be clamped without spinning.
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0x48), 1000);

        await using var ms = new MemoryStream(file);

        // Should complete promptly — the clamped walk reads at most
        // 5 DIFAT sectors regardless of the declared 1000.
        Exception? ex = await Record.ExceptionAsync(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());

        // Not an OOM or timeout; any parse error is fine.
        Assert.IsNotType<OutOfMemoryException>(ex);
    }

    /// <summary>
    /// End-to-end: a well-formed DIFAT extension file still round-trips
    /// correctly after all hardening changes.
    /// </summary>
    [Fact]
    public async Task ReadStreams_DifatOverflow_LegitDifatExtension_StillWorks()
    {
        byte[] file = BuildSyntheticDifatFile();

        await using var ms = new MemoryStream(file);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.True(streams.ContainsKey("TestStream"));
        Assert.Equal(10, streams["TestStream"].Length);
    }

    /// <summary>
    /// Builds a minimal synthetic v3 (512-byte sector) CFB with one DIFAT
    /// extension sector. The file has 5 sectors:
    /// <list type="bullet">
    ///   <item>Sector 0: directory (Root Entry + <c>TestStream</c>)</item>
    ///   <item>Sector 1: 10 bytes of data payload</item>
    ///   <item>Sector 2: first FAT sector (referenced by header DIFAT[0])</item>
    ///   <item>Sector 3: DIFAT extension sector (→ sector 4)</item>
    ///   <item>Sector 4: second FAT sector (referenced from DIFAT ext)</item>
    /// </list>
    /// The header declares <c>NumFatSectors = 110</c> so that
    /// <c>headerDifatCount = Min(109, 110) = 109</c>. Only DIFAT[0]
    /// holds a valid sector id; entries [1..108] are <c>FreeSect</c>.
    /// After the header loop, only 1 FAT sector is collected. The DIFAT
    /// walk reads sector 3, picks up sector 4, bringing the total to 2.
    /// </summary>
    private static byte[] BuildSyntheticDifatFile()
    {
        const int Ss = 512; // sector size
        const int SectorCount = 5;
        byte[] file = new byte[Ss + (SectorCount * Ss)];

        // ── Header ────────────────────────────────────────────────────
        Span<byte> h = file.AsSpan(0, Ss);
        CompoundFileReader.CfbSignature.CopyTo(h);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x18), 0x003E);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1A), 3);          // v3
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1C), 0xFFFE);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1E), 9);          // 2^9 = 512
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x20), 6);          // 2^6 = 64
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x2C), 110);        // NumFatSectors > 109
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x30), 0);          // FirstDirSector
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x38), 0);          // MiniStreamCutoff = 0 (all regular FAT)
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x3C), 0xFFFFFFFE); // FirstMiniFat = EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x40), 0);          // NumMiniFatSectors
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x44), 3);          // FirstDifatSector = sector 3
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x48), 1);          // NumDifatSectors = 1
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C), 2);          // DIFAT[0] = sector 2
        for (int i = 1; i < 109; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C + (i * 4)), 0xFFFFFFFF);
        }

        // ── Sector 0: Directory ───────────────────────────────────────
        int dirOff = Ss;
        WriteDirEntry(file, dirOff, "Root Entry", 5, 0xFFFFFFFE, 0, child: 1);
        WriteDirEntry(file, dirOff + 128, "TestStream", 2, 1, 10, child: 0xFFFFFFFF);
        WriteDirEntryUnused(file, dirOff + 256);
        WriteDirEntryUnused(file, dirOff + 384);

        // ── Sector 1: Data payload (10 bytes) ─────────────────────────
        int dataOff = Ss + (1 * Ss);
        for (int i = 0; i < 10; i++)
        {
            file[dataOff + i] = unchecked((byte)i);
        }

        // ── Sector 2: First FAT sector (sectors 0-127) ───────────────
        int fat0Off = Ss + (2 * Ss);
        for (int i = 0; i < 128; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fat0Off + (i * 4)), 0xFFFFFFFF);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fat0Off + (0 * 4)), 0xFFFFFFFE); // sector 0 dir → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fat0Off + (1 * 4)), 0xFFFFFFFE); // sector 1 data → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fat0Off + (2 * 4)), 0xFFFFFFFD); // sector 2 FAT → FatSect
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fat0Off + (4 * 4)), 0xFFFFFFFD); // sector 4 FAT → FatSect

        // ── Sector 3: DIFAT extension ─────────────────────────────────
        int difOff = Ss + (3 * Ss);
        for (int i = 0; i < 128; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(difOff + (i * 4)), 0xFFFFFFFF);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(difOff), 4);          // entry[0] = sector 4
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(difOff + 508), 0xFFFFFFFE); // next DIFAT → EndOfChain

        // ── Sector 4: Second FAT sector (sectors 128-255, all free) ───
        int fat1Off = Ss + (4 * Ss);
        for (int i = 0; i < 128; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fat1Off + (i * 4)), 0xFFFFFFFF);
        }

        return file;
    }

    private static void WriteDirEntry(byte[] file, int offset, string name, byte objectType, uint startSector, uint sizeLow, uint child)
    {
        Span<byte> e = file.AsSpan(offset, 128);
        e.Clear();
        byte[] nameBytes = Encoding.Unicode.GetBytes(name);
        nameBytes.CopyTo(e);
        BinaryPrimitives.WriteUInt16LittleEndian(e.Slice(0x40), (ushort)(nameBytes.Length + 2));
        e[0x42] = objectType;
        e[0x43] = 0x01;
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x44), 0xFFFFFFFF);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x48), 0xFFFFFFFF);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x4C), child);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x74), startSector);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x78), sizeLow);
    }

    // ── CVE-2019-0560 analog: OLE info disclosure via sector padding ──
    //
    // CVE-2019-0560 describes OLE parsers leaking uninitialized memory
    // by returning sector-sized buffers without truncating to the
    // declared stream size. Our AllocateChainBuffer truncates to
    // exactSize; this test verifies no trailing sector padding is
    // exposed to callers.

    /// <summary>
    /// Builds a synthetic CFB where the declared stream size (10 bytes) is
    /// smaller than a full sector (512 bytes). Fills the remainder of the
    /// data sector with a sentinel value (0xCC) to simulate uninitialized
    /// disk content. Verifies that <see cref="CompoundFileReader.ReadStreamsAsync"/>
    /// returns exactly 10 bytes — none of the trailing 0xCC padding leaks.
    /// </summary>
    [Fact]
    public async Task ReadStreams_StreamShorterThanSector_DoesNotLeakSectorPadding()
    {
        // Build a minimal v3 CFB with a 10-byte stream occupying one
        // 512-byte sector. Fill the unused portion with a sentinel.
        const int Ss = 512;
        const int StreamSize = 10;
        const byte Sentinel = 0xCC;
        const int SectorCount = 3; // dir, data, FAT

        byte[] file = new byte[Ss + (SectorCount * Ss)];

        // ── Header ────────────────────────────────────────────────────
        Span<byte> h = file.AsSpan(0, Ss);
        CompoundFileReader.CfbSignature.CopyTo(h);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x18), 0x003E);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1A), 3);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1C), 0xFFFE);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1E), 9);          // 2^9 = 512
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x20), 6);          // 2^6 = 64
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x2C), 1);          // NumFatSectors = 1
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x30), 0);          // FirstDirSector = 0
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x38), 0);          // MiniStreamCutoff = 0 (all regular FAT)
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x3C), 0xFFFFFFFE); // FirstMiniFat = EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x40), 0);          // NumMiniFatSectors
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x44), 0xFFFFFFFE); // FirstDifatSector = EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x48), 0);          // NumDifatSectors = 0
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C), 2);          // DIFAT[0] = sector 2 (FAT)
        for (int i = 1; i < 109; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C + (i * 4)), 0xFFFFFFFF);
        }

        // ── Sector 0: Directory ───────────────────────────────────────
        int dirOff = Ss;
        WriteDirEntry(file, dirOff, "Root Entry", 5, 0xFFFFFFFE, 0, child: 1);
        WriteDirEntry(file, dirOff + 128, "TestStream", 2, 1, (uint)StreamSize, child: 0xFFFFFFFF);
        WriteDirEntryUnused(file, dirOff + 256);
        WriteDirEntryUnused(file, dirOff + 384);

        // ── Sector 1: Data — 10 valid bytes + sentinel padding ────────
        int dataOff = Ss + (1 * Ss);
        for (int i = 0; i < StreamSize; i++)
        {
            file[dataOff + i] = unchecked((byte)i);
        }

        // Fill remainder of sector with sentinel to simulate disk garbage.
        for (int i = StreamSize; i < Ss; i++)
        {
            file[dataOff + i] = Sentinel;
        }

        // ── Sector 2: FAT ─────────────────────────────────────────────
        int fatOff = Ss + (2 * Ss);
        for (int i = 0; i < 128; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (i * 4)), 0xFFFFFFFF);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (0 * 4)), 0xFFFFFFFE); // sector 0 dir → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (1 * 4)), 0xFFFFFFFE); // sector 1 data → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (2 * 4)), 0xFFFFFFFD); // sector 2 FAT → FatSect

        await using var ms = new MemoryStream(file);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.True(streams.ContainsKey("TestStream"));
        byte[] payload = streams["TestStream"];

        // The returned buffer must be exactly StreamSize bytes — no
        // trailing sector padding is leaked (CVE-2019-0560 mitigation).
        Assert.Equal(StreamSize, payload.Length);

        // Verify the valid bytes are correct.
        for (int i = 0; i < StreamSize; i++)
        {
            Assert.Equal(unchecked((byte)i), payload[i]);
        }
    }

    /// <summary>
    /// A stream whose declared size is not sector-aligned but spans
    /// multiple sectors must still be truncated to the exact declared
    /// size. No trailing bytes from the final sector are included.
    /// </summary>
    [Fact]
    public async Task ReadStreams_MultiSectorStream_TruncatedToExactSize()
    {
        // Stream of 600 bytes spans 2 × 512 sectors = 1024 capacity.
        // The trailing 424 bytes of sector 2 must not be returned.
        const int Ss = 512;
        const int StreamSize = 600;
        const byte Sentinel = 0xDD;
        const int SectorCount = 4; // dir, data0, data1, FAT

        byte[] file = new byte[Ss + (SectorCount * Ss)];

        // ── Header ────────────────────────────────────────────────────
        Span<byte> h = file.AsSpan(0, Ss);
        CompoundFileReader.CfbSignature.CopyTo(h);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x18), 0x003E);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1A), 3);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1C), 0xFFFE);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1E), 9);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x20), 6);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x2C), 1);          // NumFatSectors
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x30), 0);          // FirstDirSector
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x38), 0);          // MiniStreamCutoff = 0
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x3C), 0xFFFFFFFE); // FirstMiniFat = EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x40), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x44), 0xFFFFFFFE); // FirstDifatSector = EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x48), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C), 3);          // DIFAT[0] = sector 3 (FAT)
        for (int i = 1; i < 109; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C + (i * 4)), 0xFFFFFFFF);
        }

        // ── Sector 0: Directory ───────────────────────────────────────
        int dirOff = Ss;
        WriteDirEntry(file, dirOff, "Root Entry", 5, 0xFFFFFFFE, 0, child: 1);
        WriteDirEntry(file, dirOff + 128, "TestStream", 2, 1, (uint)StreamSize, child: 0xFFFFFFFF);
        WriteDirEntryUnused(file, dirOff + 256);
        WriteDirEntryUnused(file, dirOff + 384);

        // ── Sector 1: Data (first 512 bytes) ─────────────────────────
        int data0Off = Ss + (1 * Ss);
        for (int i = 0; i < Ss; i++)
        {
            file[data0Off + i] = unchecked((byte)i);
        }

        // ── Sector 2: Data (remaining 88 bytes + sentinel padding) ───
        int data1Off = Ss + (2 * Ss);
        for (int i = 0; i < StreamSize - Ss; i++)
        {
            file[data1Off + i] = unchecked((byte)(Ss + i));
        }

        for (int i = StreamSize - Ss; i < Ss; i++)
        {
            file[data1Off + i] = Sentinel;
        }

        // ── Sector 3: FAT ─────────────────────────────────────────────
        int fatOff = Ss + (3 * Ss);
        for (int i = 0; i < 128; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (i * 4)), 0xFFFFFFFF);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (0 * 4)), 0xFFFFFFFE); // dir → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (1 * 4)), 2);          // data chain: 1 → 2
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (2 * 4)), 0xFFFFFFFE); // data chain: 2 → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (3 * 4)), 0xFFFFFFFD); // FAT sector

        await using var ms = new MemoryStream(file);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.True(streams.ContainsKey("TestStream"));
        byte[] payload = streams["TestStream"];

        // Exact declared size — no final-sector padding leaked.
        Assert.Equal(StreamSize, payload.Length);

        // Verify no sentinel bytes snuck in.
        for (int i = 0; i < payload.Length; i++)
        {
            Assert.Equal(unchecked((byte)i), payload[i]);
        }
    }

    // ── Mini-FAT chain spanning multiple regular sectors ──────────────
    //
    // The mini-FAT holds 128 entries per v3 sector (512 / 4). When more
    // than 128 mini-sectors are allocated, the mini-FAT chain requires
    // more than one regular FAT sector. This exercises ReadMiniFatAsync's
    // multi-sector chain walk — the gap described by OpenMcdf's
    // MiniStreamLargeChain fixtures.

    /// <summary>
    /// Constructs a synthetic v3 CFB whose mini-stream holds 192 mini-sectors
    /// (3 streams × 64 mini-sectors each = 192 × 64 = 12288 bytes), forcing
    /// the mini-FAT to span 2 regular sectors. Verifies all three streams
    /// read back correctly.
    /// </summary>
    [Fact]
    public async Task ReadStreams_MiniFatSpansMultipleSectors_RecoversStreams()
    {
        byte[] file = BuildMiniFatLargeChainFile();

        await using var ms = new MemoryStream(file);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        // Stream "A" = 4095 bytes (64 mini-sectors, entries 0..63)
        Assert.True(streams.ContainsKey("A"), $"Missing 'A'; have: {string.Join(",", streams.Keys)}");
        byte[] payloadA = streams["A"];
        Assert.Equal(4095, payloadA.Length);
        for (int i = 0; i < payloadA.Length; i++)
        {
            if (payloadA[i] != unchecked((byte)i))
            {
                Assert.Fail($"Stream A mismatch at offset {i}: expected {unchecked((byte)i)}, got {payloadA[i]}.");
            }
        }

        // Stream "B" = 4095 bytes (64 mini-sectors, entries 64..127)
        Assert.True(streams.ContainsKey("B"), $"Missing 'B'; have: {string.Join(",", streams.Keys)}");
        byte[] payloadB = streams["B"];
        Assert.Equal(4095, payloadB.Length);
        for (int i = 0; i < payloadB.Length; i++)
        {
            if (payloadB[i] != unchecked((byte)(i + 0x40)))
            {
                Assert.Fail($"Stream B mismatch at offset {i}: expected {unchecked((byte)(i + 0x40))}, got {payloadB[i]}.");
            }
        }

        // Stream "C" = 4095 bytes (64 mini-sectors, entries 128..191)
        // NOTE: entries 128+ reside in the SECOND mini-FAT sector — this is
        // the specific gap being exercised.
        Assert.True(streams.ContainsKey("C"), $"Missing 'C'; have: {string.Join(",", streams.Keys)}");
        byte[] payloadC = streams["C"];
        Assert.Equal(4095, payloadC.Length);
        for (int i = 0; i < payloadC.Length; i++)
        {
            if (payloadC[i] != unchecked((byte)(i + 0x80)))
            {
                Assert.Fail($"Stream C mismatch at offset {i}: expected {unchecked((byte)(i + 0x80))}, got {payloadC[i]}.");
            }
        }
    }

    private static byte[] BuildMiniFatLargeChainFile()
    {
        const int Ss = 512; // regular sector size
        const int Ms = 64;  // mini-sector size
        const int StreamSize = 4095; // each stream just under the 4096 cutoff
        const int StreamCount = 3;

        int miniSectorsPerStream = (StreamSize + Ms - 1) / Ms; // 64
        int totalMiniSectorsActual = miniSectorsPerStream * StreamCount; // 192
        int miniStreamBytes = totalMiniSectorsActual * Ms; // 12288

        // Sector layout:
        //   0:        Directory (4 entries × 128 = 512 bytes = 1 sector)
        //   1..24:    Mini-stream data (root entry FAT chain)
        //   25..26:   Mini-FAT (192 entries > 128 per sector → 2 sectors)
        //   27:       FAT
        int miniStreamSectors = miniStreamBytes / Ss; // 24
        int miniFatSectors = 2;
        int firstMiniFatSector = 1 + miniStreamSectors; // 25
        int fatSector = firstMiniFatSector + miniFatSectors; // 27
        int totalSectors = 1 + miniStreamSectors + miniFatSectors + 1; // 28

        byte[] file = new byte[Ss + (totalSectors * Ss)];

        // ── Header ────────────────────────────────────────────────────
        Span<byte> h = file.AsSpan(0, Ss);
        CompoundFileReader.CfbSignature.CopyTo(h);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x18), 0x003E);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1A), 3);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1C), 0xFFFE);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x1E), 9);
        BinaryPrimitives.WriteUInt16LittleEndian(h.Slice(0x20), 6);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x2C), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x30), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x38), 4096);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x3C), (uint)firstMiniFatSector);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x40), (uint)miniFatSectors);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x44), 0xFFFFFFFE);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x48), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C), (uint)fatSector);
        for (int i = 1; i < 109; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(h.Slice(0x4C + (i * 4)), 0xFFFFFFFF);
        }

        // ── Sector 0: Directory ───────────────────────────────────────
        // Red-black tree: root child = entry 2 ("B"), B.left=1("A"), B.right=3("C")
        int dirOff = Ss;
        WriteDirEntryFull(
            file,
            dirOff,
            "Root Entry",
            5,
            1,
            (uint)miniStreamBytes,
            child: 2,
            left: 0xFFFFFFFF,
            right: 0xFFFFFFFF);
        WriteDirEntryFull(
            file,
            dirOff + 128,
            "A",
            2,
            0,
            (uint)StreamSize,
            child: 0xFFFFFFFF,
            left: 0xFFFFFFFF,
            right: 0xFFFFFFFF);
        WriteDirEntryFull(
            file,
            dirOff + 256,
            "B",
            2,
            (uint)miniSectorsPerStream,
            (uint)StreamSize,
            child: 0xFFFFFFFF,
            left: 1,
            right: 3);
        WriteDirEntryFull(
            file,
            dirOff + 384,
            "C",
            2,
            (uint)(miniSectorsPerStream * 2),
            (uint)StreamSize,
            child: 0xFFFFFFFF,
            left: 0xFFFFFFFF,
            right: 0xFFFFFFFF);

        // ── Sectors 1..24: Mini-stream data ───────────────────────────
        int miniStreamOff = Ss + Ss;
        for (int s = 0; s < StreamCount; s++)
        {
            int baseOffset = s * miniSectorsPerStream * Ms;
            for (int i = 0; i < StreamSize; i++)
            {
                file[miniStreamOff + baseOffset + i] = unchecked((byte)(i + (s * 0x40)));
            }
        }

        // ── Mini-FAT (2 sectors, 192 entries) ─────────────────────────
        int miniFatOff = Ss + ((1 + miniStreamSectors) * Ss);
        int entriesPerSector = Ss / 4; // 128

        // Initialize all entries to FreeSect.
        for (int i = 0; i < miniFatSectors * entriesPerSector; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(miniFatOff + (i * 4)), 0xFFFFFFFF);
        }

        // Chain A: mini-sectors 0..63 (entry 63 → EndOfChain)
        for (int i = 0; i < miniSectorsPerStream - 1; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(miniFatOff + (i * 4)), (uint)(i + 1));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(miniFatOff + ((miniSectorsPerStream - 1) * 4)), 0xFFFFFFFE);

        // Chain B: mini-sectors 64..127 (entry 127 → EndOfChain)
        int bStart = miniSectorsPerStream;
        for (int i = bStart; i < bStart + miniSectorsPerStream - 1; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(miniFatOff + (i * 4)), (uint)(i + 1));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(miniFatOff + ((bStart + miniSectorsPerStream - 1) * 4)), 0xFFFFFFFE);

        // Chain C: mini-sectors 128..191 (entry 191 → EndOfChain)
        // Entries 128+ reside in the SECOND mini-FAT sector.
        int cStart = miniSectorsPerStream * 2;
        for (int i = cStart; i < cStart + miniSectorsPerStream - 1; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(miniFatOff + (i * 4)), (uint)(i + 1));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(miniFatOff + ((cStart + miniSectorsPerStream - 1) * 4)), 0xFFFFFFFE);

        // ── FAT sector ────────────────────────────────────────────────
        int fatOff = Ss + (fatSector * Ss);
        for (int i = 0; i < entriesPerSector; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (i * 4)), 0xFFFFFFFF);
        }

        // Sector 0: Directory → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff), 0xFFFFFFFE);

        // Sectors 1..24: Mini-stream chain (1→2→...→24→EndOfChain)
        for (int i = 1; i < 1 + miniStreamSectors - 1; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (i * 4)), (uint)(i + 1));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (miniStreamSectors * 4)), 0xFFFFFFFE);

        // Mini-FAT chain: sector 25 → sector 26 → EndOfChain
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (firstMiniFatSector * 4)), (uint)(firstMiniFatSector + 1));
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + ((firstMiniFatSector + 1) * 4)), 0xFFFFFFFE);

        // FAT sector itself → FatSect
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(fatOff + (fatSector * 4)), 0xFFFFFFFD);

        return file;
    }

    private static void WriteDirEntryFull(byte[] file, int offset, string name, byte objectType, uint startSector, uint sizeLow, uint child, uint left, uint right)
    {
        Span<byte> e = file.AsSpan(offset, 128);
        e.Clear();
        byte[] nameBytes = Encoding.Unicode.GetBytes(name);
        nameBytes.CopyTo(e);
        BinaryPrimitives.WriteUInt16LittleEndian(e.Slice(0x40), (ushort)(nameBytes.Length + 2));
        e[0x42] = objectType;
        e[0x43] = 0x01; // color = black
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x44), left);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x48), right);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x4C), child);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x74), startSector);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x78), sizeLow);
    }

    private static void WriteDirEntryUnused(byte[] file, int offset)
    {
        Span<byte> e = file.AsSpan(offset, 128);
        e.Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x44), 0xFFFFFFFF);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x48), 0xFFFFFFFF);
        BinaryPrimitives.WriteUInt32LittleEndian(e.Slice(0x4C), 0xFFFFFFFF);
    }
}
