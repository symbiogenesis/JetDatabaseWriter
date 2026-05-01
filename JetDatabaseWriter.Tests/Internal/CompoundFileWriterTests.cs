namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Round-trip tests for <see cref="CompoundFileWriter"/> verified by
/// <see cref="CompoundFileReader"/>. Modelled on OpenMcdf's StreamTests
/// (WriteThenRead pattern), but using only this assembly's CFB code.
///
/// <see cref="CompoundFileWriter"/> emits CFB v4 (4096-byte sectors) with
/// a single root storage and N top-level streams — matching the encrypted
/// .accdb wrapping use case.
/// </summary>
public sealed class CompoundFileWriterTests
{
    public static TheoryData<int> StreamSizes()
    {
        var data = new TheoryData<int>();
        foreach (int n in new[] { 0, 1, 63, 64, 65, 511, 512, 513, 4095, 4096, 4097, 8192, 65_536, 100_000 })
        {
            data.Add(n);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(StreamSizes))]
    public async Task RoundTrip_SingleStream_RecoversExactPayload(int length)
    {
        byte[] payload = CreatePatternedBuffer(length);

        byte[] cfb = CompoundFileWriter.Build([new("EncryptedPackage", payload)]);
        Assert.True(CompoundFileReader.HasCompoundFileMagic(cfb));

        await using var ms = new MemoryStream(cfb);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.True(streams.ContainsKey("EncryptedPackage"));
        Assert.Equal(payload, streams["EncryptedPackage"]);
    }

    [Fact]
    public async Task RoundTrip_TypicalAgileEncryptedAccdbShape()
    {
        // Mirrors the real shape produced by the Agile-encrypted .accdb
        // wrapper: a small EncryptionInfo blob and a larger EncryptedPackage.
        byte[] info = CreatePatternedBuffer(2048);
        byte[] pkg = CreatePatternedBuffer(80_000);

        byte[] cfb = CompoundFileWriter.Build(
        [
            new("EncryptionInfo", info),
            new("EncryptedPackage", pkg),
        ]);

        await using var ms = new MemoryStream(cfb);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.Equal(2, streams.Count);
        Assert.Equal(info, streams["EncryptionInfo"]);
        Assert.Equal(pkg, streams["EncryptedPackage"]);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(64)]
    public async Task RoundTrip_ManyStreams_AllRecovered(int streamCount)
    {
        var entries = new List<KeyValuePair<string, byte[]>>(streamCount);
        for (int i = 0; i < streamCount; i++)
        {
            // Vary sizes so we hit several FAT sector boundaries.
            entries.Add(new($"Stream{i:D3}", CreatePatternedBuffer(1000 + (i * 113))));
        }

        byte[] cfb = CompoundFileWriter.Build(entries);

        await using var ms = new MemoryStream(cfb);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.Equal(streamCount, streams.Count);
        foreach (var (name, expected) in entries)
        {
            Assert.True(streams.ContainsKey(name), $"Missing {name}");
            Assert.Equal(expected, streams[name]);
        }
    }

    [Fact]
    public void Build_NullStreams_Throws()
    {
        _ = Assert.Throws<ArgumentNullException>(() =>
            CompoundFileWriter.Build(null!));
    }

    [Fact]
    public void Build_EmptyStreamList_Throws()
    {
        _ = Assert.Throws<ArgumentException>(() =>
            CompoundFileWriter.Build(Array.Empty<KeyValuePair<string, byte[]>>()));
    }

    [Fact]
    public void Build_OutputUsesV4SectorSize()
    {
        byte[] cfb = CompoundFileWriter.Build([new("S", CreatePatternedBuffer(64))]);

        // sectorShift at offset 0x1E (little-endian uint16); 12 == 4096-byte sectors.
        ushort sectorShift = (ushort)(cfb[0x1E] | (cfb[0x1F] << 8));
        Assert.Equal(12, sectorShift);

        // majorVersion at offset 0x1A; v4 == 4.
        ushort majorVersion = (ushort)(cfb[0x1A] | (cfb[0x1B] << 8));
        Assert.Equal(4, majorVersion);
    }

    [Fact]
    public async Task RoundTrip_PreservesEnumerationOrder()
    {
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("Alpha", CreatePatternedBuffer(10)),
            new("Bravo", CreatePatternedBuffer(20)),
            new("Charlie", CreatePatternedBuffer(30)),
        };

        byte[] cfb = CompoundFileWriter.Build(entries);
        await using var ms = new MemoryStream(cfb);
        var streams = await CompoundFileReader.ReadStreamsAsync(ms, TestContext.Current.CancellationToken);

        Assert.Equal(["Alpha", "Bravo", "Charlie"], streams.Keys);
    }

    private static byte[] CreatePatternedBuffer(int length)
    {
        // Same fill pattern as the OpenMcdf reference fixtures so payload
        // checks are uniformly easy to debug.
        byte[] buffer = new byte[length];
        for (int i = 0; i < length; i++)
        {
            buffer[i] = unchecked((byte)i);
        }

        return buffer;
    }
}
