namespace JetDatabaseWriter.Tests.CompoundFile;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Exceptions;
using Xunit;

public sealed class CompoundFileHeaderTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(0xFEFF)]
    [InlineData(0xFFFF)]
    public async Task ReadStreams_RejectsUnsupportedByteOrder(int byteOrder)
    {
        byte[] bytes = BuildFile(version4: false);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(0x1C), checked((ushort)byteOrder));
        await using var stream = new MemoryStream(bytes);

        await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(JetErrorCode.CorruptCompoundFile, () =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(512, stream.Position);
    }

    [Theory]
    [InlineData(0x08, 16)]
    [InlineData(0x22, 6)]
    public async Task ReadStreams_RejectsNonzeroReservedHeaderBytes(int offset, int length)
    {
        for (int index = offset; index < offset + length; index++)
        {
            byte[] bytes = BuildFile(version4: false);
            bytes[index] = 1;
            await using var stream = new MemoryStream(bytes);

            await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(JetErrorCode.CorruptCompoundFile, () =>
                CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
            Assert.Equal(512, stream.Position);
        }
    }

    [Theory]
    [InlineData(512)]
    [InlineData(4095)]
    public async Task ReadStreams_RejectsNonzeroVersion4HeaderPadding(int offset)
    {
        byte[] bytes = BuildFile(version4: true);
        bytes[offset] = 1;
        await using var stream = new MemoryStream(bytes);

        await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(JetErrorCode.CorruptCompoundFile, () =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(4096, stream.Position);
    }

    [Theory]
    [InlineData(1u)]
    [InlineData(uint.MaxValue)]
    public async Task ReadStreams_RejectsVersion3DirectorySectorCount(uint count)
    {
        byte[] bytes = BuildFile(version4: false);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x28), count);
        await using var stream = new MemoryStream(bytes);

        await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(JetErrorCode.CorruptCompoundFile, () =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(512, stream.Position);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(2u)]
    [InlineData(uint.MaxValue)]
    public async Task ReadStreams_RejectsVersion4DirectoryCountContradictingChain(uint count)
    {
        byte[] bytes = BuildFile(version4: true);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x28), count);
        await using var stream = new MemoryStream(bytes);

        await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(JetErrorCode.CorruptCompoundFile, () =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task ReadStreams_AcceptsVersion4MultipleDirectorySectors()
    {
        var entries = new List<KeyValuePair<string, byte[]>>();
        for (int index = 0; index < 32; index++)
        {
            entries.Add(new KeyValuePair<string, byte[]>($"Stream{index}", []));
        }

        byte[] bytes = CompoundFileWriter.BuildOfficeCrypto(entries);
        Assert.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0x28)));
        await using var stream = new MemoryStream(bytes);

        Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken);

        Assert.Equal(entries.Count, streams.Count);
        Assert.All(streams.Values, Assert.Empty);
    }

    [Theory]
    [InlineData(false, 1u)]
    [InlineData(false, 4095u)]
    [InlineData(false, uint.MaxValue)]
    [InlineData(true, 0u)]
    [InlineData(true, 1u)]
    [InlineData(true, 4095u)]
    [InlineData(true, uint.MaxValue)]
    public async Task ReadStreams_RejectsUnsupportedMiniStreamCutoff(bool version4, uint cutoff)
    {
        byte[] bytes = BuildFile(version4);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x38), cutoff);
        await using var stream = new MemoryStream(bytes);

        await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(JetErrorCode.CorruptCompoundFile, () =>
            CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(512, stream.Position);
    }

    [Theory]
    [InlineData(false, 0u)]
    [InlineData(false, 4096u)]
    [InlineData(true, 4096u)]
    public async Task ReadStreams_AcceptsStandardCutoffAndInternalVersion3Dialect(bool version4, uint cutoff)
    {
        byte[] bytes = BuildFile(version4);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x38), cutoff);
        await using var stream = new MemoryStream(bytes);

        Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken);

        Assert.Equal(new byte[4096], streams["Payload"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadStreams_AcceptsAdvisoryMinorVersionAndTransactionSignature(bool version4)
    {
        byte[] bytes = BuildFile(version4);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(0x18), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0x34), uint.MaxValue);
        await using var stream = new MemoryStream(bytes);

        Dictionary<string, byte[]> streams = await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken);

        Assert.Equal(new byte[4096], streams["Payload"]);
    }

    private static byte[] BuildFile(bool version4)
    {
        KeyValuePair<string, byte[]>[] entries = [new("Payload", new byte[4096])];
        return version4 ? CompoundFileWriter.BuildOfficeCrypto(entries) : CompoundFileWriter.Build(entries);
    }
}
