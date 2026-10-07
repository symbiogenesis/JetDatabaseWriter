namespace JetDatabaseWriter.Tests.CompoundFile;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using Xunit;

public sealed class CompoundFileCorruptionTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(38)]
    public async Task ReadStreams_RejectsInvalidMiniSectorShift(ushort shift)
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1, 2, 3])]);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(0x20), shift);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReadStreams_RejectsDeclaredStreamLargerThanChain()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1, 2, 3])]);
        int directorySector = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0x30)));
        int entry = ((directorySector + 1) * 512) + 128;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(entry + 0x78), 1024);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReadStreams_RejectsChainSectorPastPhysicalEnd()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1, 2, 3])]);
        int directorySector = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0x30)));
        int entry = ((directorySector + 1) * 512) + 128;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(entry + 0x74), 100);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }
}
