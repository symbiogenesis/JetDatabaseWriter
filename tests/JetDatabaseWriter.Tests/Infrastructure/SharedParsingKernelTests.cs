namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Independent checks of shared bounded parsing and exact acquisition.</summary>
public sealed class SharedParsingKernelTests
{
    [Fact]
    public void ChildPointer_OverflowingOffset_RefusesWithoutArithmeticFailure()
    {
        Assert.Equal(0, IndexPageCodec.DecodeIntermediateChildPointer(new byte[8], int.MaxValue));
        Assert.False(UsageMap.TryReadPointer(new byte[8], int.MaxValue, out _));
    }

    [Fact]
    public void ByteRange_NativeAndSignedBoundaries_MatchWideArithmetic()
    {
        ulong[] values = [0, 1, 4, int.MaxValue, (ulong)int.MaxValue + 1, long.MaxValue, (ulong)long.MaxValue + 1, ulong.MaxValue];
        foreach (ulong length in values)
        {
            foreach (ulong offset in values)
            {
                foreach (ulong count in values)
                {
                    bool expected = length <= long.MaxValue && (decimal)offset + count <= length;
                    Assert.Equal(expected, RecoveryKernel.IsByteRange(length, offset, count));
                }
            }
        }

        long[] signedValues = [long.MinValue, -1, 0, 1, 4, int.MaxValue, long.MaxValue];
        foreach (long length in signedValues)
        {
            foreach (long offset in signedValues)
            {
                foreach (long count in signedValues)
                {
                    bool expected = length >= 0 && offset >= 0 && count >= 0 && (decimal)offset + count <= length;
                    Assert.Equal(expected, BinaryBuffer.IsRange(length, offset, count));
                }
            }
        }
    }

    [Fact]
    public void BufferRead_NativeBoundaries_MatchWideArithmetic()
    {
        uint[] values = [0, 1, 2, 7, int.MaxValue, (uint)int.MaxValue + 1, uint.MaxValue];
        foreach (uint length in values)
        {
            foreach (uint offset in values)
            {
                foreach (uint count in values)
                {
                    bool valid = length <= int.MaxValue && offset < length && count > 0 && (ulong)offset + count <= length;
                    uint expected = valid ? checked(offset + count) : uint.MaxValue;
                    Assert.Equal(expected, RecoveryKernel.BufferReadAdvance(length, offset, count));
                }
            }
        }
    }

    [Fact]
    public async Task ExactReads_EveryPartition_AcquiresExactlyRequestedSlice()
    {
        byte[] source = [1, 2, 3, 4, 5, 6, 7, 8, 99];
        for (int mask = 0; mask < 128; mask++)
        {
            int[] chunks = Partition(mask);
            await using var synchronous = new ChunkedStream(source, chunks);
            byte[] syncDestination = [77, 0, 0, 0, 0, 0, 0, 0, 0, 88];
            StreamRead.ReadExactly(synchronous, syncDestination.AsSpan(1, 8));
            Assert.Equal([77, 1, 2, 3, 4, 5, 6, 7, 8, 88], syncDestination);
            Assert.Equal(8, synchronous.Position);
            Assert.Equal(chunks.Length, synchronous.ReadCalls);

            await using var asynchronous = new ChunkedStream(source, chunks);
            byte[] asyncDestination = [77, 0, 0, 0, 0, 0, 0, 0, 0, 88];
            await StreamRead.ReadExactlyAsync(asynchronous, asyncDestination.AsMemory(1, 8), TestContext.Current.CancellationToken);
            Assert.Equal(syncDestination, asyncDestination);
            Assert.Equal(8, asynchronous.Position);
            Assert.Equal(chunks.Length, asynchronous.ReadCalls);
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(5)]
    public async Task ReadMinimum_InvalidBounds_RefusesBeforeReading(int minimum)
    {
        await using var stream = new ChunkedStream([1, 2, 3, 4], [4]);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await StreamRead.ReadAtLeastAsync(stream, new byte[4], minimum, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(0, stream.ReadCalls);
    }

    [Fact]
    public async Task ReadMinimum_PermitsOvershootAndPartialEof()
    {
        await using var overshoot = new ChunkedStream([1, 2, 3, 4, 5, 6], [5, 1]);
        byte[] destination = new byte[8];
        Assert.Equal(5, await StreamRead.ReadAtLeastAsync(overshoot, destination, 3, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(5, overshoot.Position);
        Assert.Equal(1, overshoot.ReadCalls);

        await using var shortStream = new ChunkedStream([1, 2, 3, 4], [2, 2]);
        Array.Fill(destination, (byte)99);
        Assert.Equal(4, await StreamRead.ReadAtLeastAsync(shortStream, destination, 5, throwOnEndOfStream: false, TestContext.Current.CancellationToken));
        Assert.Equal([1, 2, 3, 4, 99, 99, 99, 99], destination);
    }

    [Fact]
    public async Task ExactReads_PartialEof_PreserveAcquiredPrefixAndRefuse()
    {
        await using var synchronous = new ChunkedStream([1, 2], [1, 1]);
        byte[] destination = [99, 99, 99, 99];
        Assert.Throws<EndOfStreamException>(() => StreamRead.ReadExactly(synchronous, destination));
        Assert.Equal([1, 2, 99, 99], destination);

        await using var asynchronous = new ChunkedStream([1, 2], [1, 1]);
        Array.Fill(destination, (byte)99);
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await StreamRead.ReadExactlyAsync(asynchronous, destination, TestContext.Current.CancellationToken));
        Assert.Equal([1, 2, 99, 99], destination);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(5)]
    public async Task ExactReads_InvalidReturnedCount_RefusesAfterOneCall(int count)
    {
        await using var synchronous = new ChunkedStream([1, 2, 3, 4], [4]) { ReturnedCount = count };
        Assert.Throws<IOException>(() => StreamRead.ReadExactly(synchronous, new byte[4]));
        Assert.Equal(1, synchronous.ReadCalls);

        await using var asynchronous = new ChunkedStream([1, 2, 3, 4], [4]) { ReturnedCount = count };
        await Assert.ThrowsAsync<IOException>(async () =>
            await StreamRead.ReadExactlyAsync(asynchronous, new byte[4], TestContext.Current.CancellationToken));
        Assert.Equal(1, asynchronous.ReadCalls);
    }

    [Fact]
    public async Task ExactReads_CancellationAndFailureAfterPrefix_StopWithoutRetry()
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using var canceled = new ChunkedStream([1, 2, 3, 4], [2, 2]) { AfterRead = cancellation.Cancel };
        byte[] destination = [99, 99, 99, 99];
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await StreamRead.ReadExactlyAsync(canceled, destination, cancellation.Token));
        Assert.Equal([1, 2, 99, 99], destination);
        Assert.Equal(1, canceled.ReadCalls);

        var failure = new IOException("selected read failure");
        await using var faulted = new ChunkedStream([1, 2, 3, 4], [2, 2]) { FailOnCall = 2, Failure = failure };
        Array.Fill(destination, (byte)99);
        IOException observed = await Assert.ThrowsAsync<IOException>(async () =>
            await StreamRead.ReadExactlyAsync(faulted, destination, TestContext.Current.CancellationToken));
        Assert.Same(failure, observed);
        Assert.Equal([1, 2, 99, 99], destination);
        Assert.Equal(2, faulted.ReadCalls);
    }

    [Fact]
    public async Task EmptyReads_DoNotInvokeStreamEvenWithCanceledToken()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await using var stream = new ChunkedStream([], []) { ReturnedCount = -1 };
        StreamRead.ReadExactly(stream, []);
        await StreamRead.ReadExactlyAsync(stream, Memory<byte>.Empty, cancellation.Token);
        Assert.Equal(0, await StreamRead.ReadAtLeastAsync(stream, new byte[4], 0, cancellationToken: cancellation.Token));
        Assert.Equal(0, stream.ReadCalls);
    }

    [Fact]
    public void CopySlice_InvalidPositiveRange_RefusesBeforeAllocation()
    {
        byte[] source = [1, 2, 3, 4];
        Assert.Throws<ArgumentOutOfRangeException>(() => BinaryBuffer.CopySlice(source, int.MaxValue, 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => BinaryBuffer.CopySlice(source, 1, int.MaxValue));
        Assert.Throws<ArgumentOutOfRangeException>(() => BinaryBuffer.CopySlice(source, -1, 1));
        Assert.Equal([1, 2, 3, 4], source);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PageStore_PositionalAndSeekReads_AdmitRangesAndAcquireExactly(bool inline)
    {
        string path = Path.Combine(Path.GetTempPath(), $"jdw-shared-read-{Guid.NewGuid():N}.tmp");
        await using var file = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
        await file.WriteAsync(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, TestContext.Current.CancellationToken);
        file.Position = 1;
        await using var store = new StreamPageStore(file, leaveOpen: true);
        store.EnablePositionalReads();
        Assert.Equal(!LibraryTarget.IsNetStandard, store.Capabilities.PositionalReads);

        byte[] destination = new byte[3];
        await store.ReadAsync(2, destination, inline, TestContext.Current.CancellationToken);
        Assert.Equal([3, 4, 5], destination);
        Assert.Equal(LibraryTarget.IsNetStandard ? 5 : 1, file.Position);

        file.Position = 1;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await store.ReadAsync(long.MaxValue, new byte[2], inline, TestContext.Current.CancellationToken));
        Assert.Equal(1, file.Position);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await store.ReadAsync(-1, new byte[2], inline, TestContext.Current.CancellationToken));
        Assert.Equal(1, file.Position);

        destination = [99, 99];
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await store.ReadAsync(7, destination, inline, TestContext.Current.CancellationToken));
        Assert.Equal([8, 99], destination);
    }

    private static int[] Partition(int mask)
    {
        var chunks = new List<int>();
        int start = 0;
        for (int boundary = 1; boundary <= 8; boundary++)
        {
            if (boundary == 8 || (mask & (1 << (boundary - 1))) != 0)
            {
                chunks.Add(boundary - start);
                start = boundary;
            }
        }

        return [.. chunks];
    }

    private sealed class ChunkedStream(byte[] source, int[] counts) : MemoryStream(source, writable: false)
    {
        public int ReadCalls { get; private set; }

        public int? ReturnedCount { get; init; }

        public int FailOnCall { get; init; }

        public IOException? Failure { get; init; }

        public Action? AfterRead { get; init; }

        public override int Read(Span<byte> buffer)
        {
            this.ReadCalls++;
            if (this.ReadCalls == this.FailOnCall)
            {
                throw this.Failure ?? new IOException("selected read failure");
            }

            if (this.ReturnedCount is int invalidCount)
            {
                return invalidCount;
            }

            int count = this.ReadCalls <= counts.Length ? Math.Min(buffer.Length, counts[this.ReadCalls - 1]) : buffer.Length;
            int read = base.Read(buffer[..count]);
            this.AfterRead?.Invoke();
            return read;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(this.Read(buffer.Span));
        }
    }
}
