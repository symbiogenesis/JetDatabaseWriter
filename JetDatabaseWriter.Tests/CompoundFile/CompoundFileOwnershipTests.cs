namespace JetDatabaseWriter.Tests.CompoundFile;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using Xunit;

public sealed class CompoundFileOwnershipTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task ReadStreams_RejectsSharedRegularChainOrTail(int sharedSectorOffset)
    {
        byte[] bytes = CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("A", new byte[1024]),
            new KeyValuePair<string, byte[]>("B", new byte[1024]),
        ]);
        int directory = SectorOffset(ReadUInt32(bytes, 0x30));
        uint firstStart = ReadUInt32(bytes, directory + 128 + 0x74);
        int secondEntry = directory + 256;
        WriteUInt32(bytes, secondEntry + 0x74, firstStart + checked((uint)sharedSectorOffset));
        WriteUInt32(bytes, secondEntry + 0x78, 1);

        await AssertRejectedAsync(bytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadStreams_RejectsStreamOverlappingDirectoryOrFat(bool overlapFat)
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("A", [1])]);
        uint directorySector = ReadUInt32(bytes, 0x30);
        uint sharedSector = overlapFat ? ReadUInt32(bytes, 0x4C) : directorySector;
        int directory = SectorOffset(directorySector);
        WriteUInt32(bytes, directory + 128 + 0x74, sharedSector);
        if (overlapFat)
        {
            WriteUInt32(bytes, SectorOffset(sharedSector) + checked((int)sharedSector * 4), Constants.CompoundFile.EndOfChain);
        }

        await AssertRejectedAsync(bytes);
    }

    [Fact]
    public async Task ReadStreams_RejectsMiniFatOverlappingDirectory()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("A", [1])]);
        WriteUInt32(bytes, 0x3C, ReadUInt32(bytes, 0x30));
        WriteUInt32(bytes, 0x40, 1);

        await AssertRejectedAsync(bytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadStreams_RejectsRootMiniStreamOverlappingDirectoryOrStream(bool overlapStream)
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("A", [1])]);
        uint directorySector = ReadUInt32(bytes, 0x30);
        int directory = SectorOffset(directorySector);
        uint rootStart = overlapStream ? ReadUInt32(bytes, directory + 128 + 0x74) : directorySector;
        WriteUInt32(bytes, directory + 0x74, rootStart);
        WriteUInt32(bytes, directory + 0x78, 1);

        await AssertRejectedAsync(bytes);
    }

    [Fact]
    public async Task ReadStreams_RejectsRepeatedFatSectorDeclaration()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("A", [1])]);
        WriteUInt32(bytes, 0x2C, 2);
        WriteUInt32(bytes, 0x50, ReadUInt32(bytes, 0x4C));

        await AssertRejectedAsync(bytes);
    }

    [Fact]
    public async Task ReadStreams_RejectsFatSectorReusedAsDifat()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("A", [1])]);
        WriteUInt32(bytes, 0x44, ReadUInt32(bytes, 0x4C));
        WriteUInt32(bytes, 0x48, 1);

        await AssertRejectedAsync(bytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadStreams_RejectsDifatExtensionReferencingHeaderFatOrItself(bool referenceDifat)
    {
        byte[] bytes = CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("A", [1]),
            new KeyValuePair<string, byte[]>("Spare", new byte[512]),
        ]);
        int directory = SectorOffset(ReadUInt32(bytes, 0x30));
        uint difatSector = ReadUInt32(bytes, directory + 256 + 0x74);
        uint fatSector = ReadUInt32(bytes, 0x4C);
        bytes[directory + 256 + 0x42] = 0;
        WriteUInt32(bytes, directory + 128 + 0x48, Constants.CompoundFile.FreeSect);
        WriteUInt32(bytes, 0x2C, 2);
        WriteUInt32(bytes, 0x44, difatSector);
        WriteUInt32(bytes, 0x48, 1);
        int difat = SectorOffset(difatSector);
        for (int index = 0; index < 127; index++)
        {
            WriteUInt32(bytes, difat + (index * 4), Constants.CompoundFile.FreeSect);
        }

        WriteUInt32(bytes, difat, referenceDifat ? difatSector : fatSector);
        WriteUInt32(bytes, difat + 508, Constants.CompoundFile.EndOfChain);

        await AssertRejectedAsync(bytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadStreams_RejectsSharedMiniChainOrTail(bool sharedTail)
    {
        byte[] bytes = BuildMiniStreamFile();
        int directory = SectorOffset(ReadUInt32(bytes, 0x30));
        int miniFat = SectorOffset(ReadUInt32(bytes, 0x3C));
        if (sharedTail)
        {
            WriteUInt32(bytes, miniFat, 1);
            WriteUInt32(bytes, miniFat + 4, Constants.CompoundFile.EndOfChain);
            WriteUInt32(bytes, miniFat + (8 * 4), 1);
        }
        else
        {
            WriteUInt32(bytes, directory + 256 + 0x74, 0);
        }

        await AssertRejectedAsync(bytes);
    }

    [Fact]
    public async Task ReadStreams_DistinctMiniChainsMayShareRootStorageSector()
    {
        byte[] bytes = BuildMiniStreamFile();
        int directory = SectorOffset(ReadUInt32(bytes, 0x30));
        int miniFat = SectorOffset(ReadUInt32(bytes, 0x3C));
        int miniStream = SectorOffset(ReadUInt32(bytes, directory + 0x74));
        WriteUInt32(bytes, directory + 256 + 0x74, 1);
        WriteUInt32(bytes, miniFat + 4, Constants.CompoundFile.EndOfChain);
        bytes[miniStream + 64] = 2;
        await using var stream = new MemoryStream(bytes);
        Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken);

        Assert.Equal(new byte[] { 1 }, streams["A"]);
        Assert.Equal(new byte[] { 2 }, streams["B"]);
    }

    private static byte[] BuildMiniStreamFile()
    {
        byte[] bytes = CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("A", [1]),
            new KeyValuePair<string, byte[]>("B", [2]),
            new KeyValuePair<string, byte[]>("MiniFat", new byte[512]),
        ]);
        int directory = SectorOffset(ReadUInt32(bytes, 0x30));
        uint miniStreamSector = ReadUInt32(bytes, directory + 128 + 0x74);
        uint secondSector = ReadUInt32(bytes, directory + 256 + 0x74);
        uint miniFatSector = ReadUInt32(bytes, directory + 384 + 0x74);
        int fat = SectorOffset(ReadUInt32(bytes, 0x4C));
        WriteUInt32(bytes, fat + checked((int)miniStreamSector * 4), secondSector);
        WriteUInt32(bytes, directory + 0x74, miniStreamSector);
        WriteUInt32(bytes, directory + 0x78, 1024);
        WriteUInt32(bytes, directory + 128 + 0x74, 0);
        WriteUInt32(bytes, directory + 256 + 0x74, 8);
        bytes[directory + 384 + 0x42] = 0;
        WriteUInt32(bytes, directory + 256 + 0x48, Constants.CompoundFile.FreeSect);
        WriteUInt32(bytes, 0x38, 4096);
        WriteUInt32(bytes, 0x3C, miniFatSector);
        WriteUInt32(bytes, 0x40, 1);
        int miniFat = SectorOffset(miniFatSector);
        for (int index = 0; index < 128; index++)
        {
            WriteUInt32(bytes, miniFat + (index * 4), Constants.CompoundFile.FreeSect);
        }

        WriteUInt32(bytes, miniFat, Constants.CompoundFile.EndOfChain);
        WriteUInt32(bytes, miniFat + (8 * 4), Constants.CompoundFile.EndOfChain);
        return bytes;
    }

    private static async Task AssertRejectedAsync(byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes);
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    private static uint ReadUInt32(byte[] bytes, int offset) =>
        BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset, 4));

    private static void WriteUInt32(byte[] bytes, int offset, uint value) =>
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset, 4), value);

    private static int SectorOffset(uint sector) => checked(((int)sector + 1) * 512);
}
