namespace JetDatabaseWriter.Tests.CompoundFile;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Exceptions;
using Xunit;

public sealed class CompoundFileCorruptionTests
{
    [Fact]
    public async Task ReadStreams_RefusesConfiguredContainerBudgetBeforeParsing()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1, 2, 3])]);
        await using var stream = new MemoryStream(bytes);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken, maxBytes: 512).AsTask());

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Fact]
    public async Task ReadStreams_RefusesSharedChainBufferAmplification()
    {
        byte[] bytes = CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("Large", new byte[4096]),
            new KeyValuePair<string, byte[]>("Small1", [1]),
            new KeyValuePair<string, byte[]>("Small2", [2]),
        ]);
        int directorySector = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0x30)));
        int directory = (directorySector + 1) * 512;
        uint sharedStart = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(directory + 128 + 0x74));
        for (int index = 2; index <= 3; index++)
        {
            int entry = directory + (index * 128);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(entry + 0x74), sharedStart);
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(entry + 0x78), 4096);
        }

        await using var stream = new MemoryStream(bytes);
        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken, maxBytes: bytes.Length).AsTask());

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(38)]
    public async Task ReadStreams_RejectsInvalidMiniSectorShift(int shift)
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1, 2, 3])]);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(0x20), checked((ushort)shift));
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
