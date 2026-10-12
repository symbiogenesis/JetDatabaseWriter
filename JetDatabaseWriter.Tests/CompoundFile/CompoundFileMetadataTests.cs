namespace JetDatabaseWriter.Tests.CompoundFile;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Exceptions;
using Xunit;

public sealed class CompoundFileMetadataTests
{
    [Theory]
    [InlineData(0x2C)]
    [InlineData(0x40)]
    [InlineData(0x48)]
    public async Task ReadStreams_RejectsMetadataCountBeyondPhysicalSectors(int countOffset)
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1])]);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(countOffset), uint.MaxValue);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReadStreams_RejectsFatCountExceedingDifatEntries()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1])]);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x2C), 2);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Theory]
    [InlineData(0x40)]
    [InlineData(0x48)]
    public async Task ReadStreams_RejectsNonzeroMetadataCountWithoutChain(int countOffset)
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1])]);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(countOffset), 1);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(1, 2)]
    [InlineData(2, 1)]
    public async Task ReadStreams_RejectsMiniFatCountContradictingChain(int sectorCount, int declaredCount)
    {
        byte[] bytes = BuildMiniFatFile(sectorCount);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x40), checked((uint)declaredCount));
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReadStreams_RefusesMiniFatConversionExceedingLiveMetadataBudget()
    {
        byte[] bytes = BuildMiniFatFile(16);
        await using var stream = new MemoryStream(bytes);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken, bytes.Length).AsTask());

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
        Assert.Equal(bytes.Length, stream.Position); // Only the header and final FAT sector were read.
    }

    [Fact]
    public async Task ReadStreams_RejectsExtraFatDeclaration()
    {
        byte[] bytes = CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("Payload", [1]),
            new KeyValuePair<string, byte[]>("Spare", new byte[512]),
        ]);
        int directory = checked(((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0x30)) + 1) * 512);
        uint spareSector = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(directory + 256 + 0x74));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x50), spareSector);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReadStreams_PreservesSectorReadIoException()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1])]);
        var expected = new IOException("Sector read failure.");
        await using var stream = new FailingSectorReadStream(bytes, expected);

        IOException actual = await Assert.ThrowsAsync<IOException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());

        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task ReadStreams_PreservesCancellation()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Payload", [1])]);
        await using var stream = new MemoryStream(bytes);
        using var source = new CancellationTokenSource();
        await source.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            CompoundFileReader.ReadStreamsAsync(stream, source.Token).AsTask());
    }

    private static byte[] BuildMiniFatFile(int sectorCount)
    {
        byte[] bytes = CompoundFileWriter.Build(
        [
            new KeyValuePair<string, byte[]>("Payload", [1]),
            new KeyValuePair<string, byte[]>("ZMiniFat", new byte[sectorCount * 512]),
        ]);
        int directory = checked(((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0x30)) + 1) * 512);
        uint miniFatStart = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(directory + 256 + 0x74));
        bytes[directory + 256 + 0x42] = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x3C), miniFatStart);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x40), checked((uint)sectorCount));
        return bytes;
    }

    private sealed class FailingSectorReadStream(byte[] bytes, IOException failure) : MemoryStream(bytes)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            this.Position > 0
                ? new ValueTask<int>(Task.FromException<int>(failure))
                : base.ReadAsync(buffer, cancellationToken);
    }
}
