namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

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
        new() { "TestStream_v3_0.cfs", "TestStream_v4_0.cfs" };

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
        using var ms = new MemoryStream(truncated);
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

        using var ms = new MemoryStream(data);
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

        using var ms = new MemoryStream(data);
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

        using var ms = new MemoryStream(data);
        _ = await Assert.ThrowsAsync<InvalidDataException>(
            () => CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken).AsTask());
    }
}
