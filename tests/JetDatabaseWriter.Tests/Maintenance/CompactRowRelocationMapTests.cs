namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Verification;
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

    [Fact]
    public async Task Lookup_PreservesEqualKeyOrderAcrossNativeBookmarkBoundaries()
    {
        (uint Source, uint Destination)[] rows =
        [
            (0x100, 0x100),
            (0x101, 0x1FF),
            (0x1FF, 0x200),
            (0x200, 0xFFFFFE00),
            (0xFFFFFFFE, 0xFFFFFFFE),
            (0xFFFFFFFF, 0xFFFFFFFF),
        ];
        await using MemoryStream ledger = Ledger(rows);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, rows.Length, 4096, Ct);
        byte[] key = [0xFF, 0, 0x80];
        uint priorDestination = 0;
        foreach ((uint source, uint destination) in rows)
        {
            IndexEntry actual = await map.RelocateAsync(new(key, source >> 8, checked((byte)(source & 255))), Ct);
            uint actualBookmark = checked(((uint)actual.DataPage << 8) | actual.DataRow);
            Assert.Same(key, actual.Key);
            Assert.Equal(destination, actualBookmark);
            Assert.True(actualBookmark > priorDestination);
            priorDestination = actualBookmark;
        }
    }

    [Theory]
    [InlineData(0x100U)]
    [InlineData(0x1FFU)]
    [InlineData(0x300U)]
    [InlineData(0xFFFFFFFFU)]
    public async Task Lookup_RefusesMissingIdentitiesBeforeBetweenAndAfterRows(uint target)
    {
        await using MemoryStream ledger = Ledger((0x200, 0x700), (0x201, 0x701), (0x400, 0x900));
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, 3, 4096, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await map.RelocateAsync(new([0x80], target >> 8, checked((byte)(target & 255))), Ct));
    }

    [Fact]
    public async Task Lookup_EmptyLedgerRefusesWithoutReading()
    {
        await using MemoryStream ledger = Ledger();
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, 0, 8, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await map.RelocateAsync(new([1], 1, 0), Ct));
        Assert.Equal(0, map.ReadBytes);
    }

    [Fact]
    public async Task Lookup_MatchesIndependentMapAcrossSparseSearchPaths()
    {
        const int count = 511;
        var random = new Random(4407);
        var rows = new (uint Source, uint Destination)[count];
        var expected = new Dictionary<uint, uint>();
        uint source = 0x100;
        uint destination = 0x40000;
        for (int index = 0; index < count; index++)
        {
            source += checked((uint)random.Next(1, 250));
            destination += checked((uint)random.Next(1, 97));
            rows[index] = (source, destination);
            expected.Add(source, destination);
        }

        await using MemoryStream ledger = Ledger(rows);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, count, 100_000, Ct);
        byte[] key = [0xA5, 0x5A];
        for (int index = 0; index < count; index++)
        {
            uint target = rows[index * 37 % count].Source;
            IndexEntry actual = await map.RelocateAsync(new(key, target >> 8, checked((byte)(target & 255))), Ct);
            Assert.Same(key, actual.Key);
            Assert.Equal(expected[target], checked(((uint)actual.DataPage << 8) | actual.DataRow));
        }

        Assert.InRange(map.ReadBytes, count * 8L, count * 8L * 10);
    }

    [Fact]
    public void SearchPivot_HandlesFullSignedLengthWithoutOverflow()
    {
        Assert.Equal(long.MaxValue / 2, IndexMutationKernel.RelocationMiddle(0, long.MaxValue));
        Assert.Equal(long.MaxValue - 1, IndexMutationKernel.RelocationMiddle(long.MaxValue - 1, long.MaxValue));
        Assert.Equal(long.MaxValue - 50, IndexMutationKernel.RelocationMiddle(long.MaxValue - 100, long.MaxValue));
    }

    [Theory]
    [InlineData(0x300U, 0xA00U, 0x300U, 0xA01U)]
    [InlineData(0x301U, 0xA00U, 0x300U, 0xA01U)]
    [InlineData(0x300U, 0xA00U, 0x301U, 0xA00U)]
    [InlineData(0x300U, 0xA01U, 0x301U, 0xA00U)]
    [InlineData(0U, 0xA00U, 0x301U, 0xA01U)]
    [InlineData(0xFFU, 0xA00U, 0x301U, 0xA01U)]
    [InlineData(0x300U, 0xFFU, 0x301U, 0xA01U)]
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
