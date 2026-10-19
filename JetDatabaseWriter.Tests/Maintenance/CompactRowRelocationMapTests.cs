namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using Xunit;

public sealed class CompactRowRelocationMapTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Lookup_PreservesOpaqueKeysAndUsesOriginalHeaderIdentity()
    {
        await using MemoryStream ledger = Ledger((0x300, 0xA00), (0x301, 0xA01), (0x800, 0xA02));
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, 3, 1024, Ct);
        byte[] key = [0xFF, 0xFE, 0, 0x80];
        IndexEntry actual = await map.RelocateAsync(new(key, 8, 0), Ct);
        Assert.Same(key, actual.Key);
        Assert.Equal(10, actual.DataPage);
        Assert.Equal(2, actual.DataRow);
        Assert.Equal(10, (await map.RelocateAsync(new(key, 3, 0), Ct)).DataPage);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await map.RelocateAsync(new(key, 4, 0), Ct));
    }

    [Theory]
    [InlineData(0x300U, 0xA00U, 0x300U, 0xA01U)]
    [InlineData(0x301U, 0xA00U, 0x300U, 0xA01U)]
    [InlineData(0x300U, 0xA00U, 0x301U, 0xA00U)]
    [InlineData(0x300U, 0xA01U, 0x301U, 0xA00U)]
    [InlineData(0U, 0xA00U, 0x301U, 0xA01U)]
    public async Task Open_RefusesDuplicateUnorderedAndInvalidMappings(uint sourceA, uint destinationA, uint sourceB, uint destinationB)
    {
        await using MemoryStream ledger = Ledger((sourceA, destinationA), (sourceB, destinationB));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRowRelocationMap.OpenAsync(ledger, 2, 1024, Ct));
    }

    [Fact]
    public async Task Lookup_EnforcesCumulativeIoBudgetAndDetectsChangedLength()
    {
        await using MemoryStream ledger = Ledger((0x300, 0xA00));
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, 1, 16, Ct);
        _ = await map.RelocateAsync(new([1], 3, 0), Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await map.RelocateAsync(new([1], 3, 0), Ct));
        Assert.Equal(16, map.ReadBytes);
        ledger.SetLength(16);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await map.RelocateAsync(new([1], 3, 0), Ct));
    }

    [Fact]
    public async Task Open_RefusesIncompleteLedgerAndSupportsPartialStreamReads()
    {
        await using MemoryStream ledger = Ledger((0x300, 0xA00));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRowRelocationMap.OpenAsync(ledger, 2, 1024, Ct));
        await using var partial = new PartialReadStream(ledger.ToArray());
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(partial, 1, 1024, Ct);
        Assert.Equal(36, map.ReadBytes);
        Assert.Equal(10, (await map.RelocateAsync(new([1], 3, 0), Ct)).DataPage);
        Assert.Equal(72, map.ReadBytes);
        await using var limited = new PartialReadStream(ledger.ToArray());
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRowRelocationMap.OpenAsync(limited, 1, 35, Ct));
    }

    [Fact]
    public async Task Open_PropagatesRealIoFailuresAndCancellation()
    {
        var failure = new IOException("ledger read failed");
        await using var ledger = new ReadFailureStream(failure);
        ledger.SetLength(8);
        Assert.Same(failure, await Assert.ThrowsAsync<IOException>(async () => await CompactRowRelocationMap.OpenAsync(ledger, 1, 1024, Ct)));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRowRelocationMap.OpenAsync(ledger, 1, 1024, cancellation.Token));
    }

    private static MemoryStream Ledger(params (uint Source, uint Destination)[] rows)
    {
        var stream = new MemoryStream();
        foreach ((uint source, uint destination) in rows)
        {
            byte[] record = new byte[8];
            BinaryPrimitives.WriteUInt32LittleEndian(record, source);
            BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), destination);
            stream.Write(record);
        }

        return stream;
    }

    private sealed class PartialReadStream(byte[] bytes) : MemoryStream(bytes)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => base.ReadAsync(buffer[..Math.Min(1, buffer.Length)], cancellationToken);
    }

    private sealed class ReadFailureStream(IOException failure) : MemoryStream
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromException<int>(failure);
    }
}
