namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class LongValueStreamingTests
{
    [Fact]
    public async Task CopyPayloadAsync_StreamsBeyondDefaultReadBudgetThroughOneReusedPage()
    {
        const int pageSize = 4096;
        const int chunkSize = pageSize - 4;
        const int length = 0x1000000 + 17;
        const int rowCount = (length + chunkSize - 1) / chunkSize;
        byte[] page = new byte[pageSize];
        int located = 0;
        await using var output = new VerifyingStream();

        await LongValueStore.CopyPayloadAsync(
            LongValueDescriptor.Chained(length, 256, 0xFEEDABCD),
            ReadOnlyMemory<byte>.Empty,
            output,
            pageSize,
            maxValueBytes: length,
            maxChainRows: rowCount,
            (pointer, _) =>
            {
                Assert.Equal(LongValueStore.MakeRowPointer(located + 1, 0), pointer);
                int offset = located * chunkSize;
                Assert.Equal(offset, output.Length);
                int count = Math.Min(chunkSize, length - offset);
                Wu32(page, 0, LongValueStore.MakeRowPointer(located + 2, 0));
                for (int index = 0; index < count; index++)
                {
                    page[index + 4] = Pattern(offset + index);
                }

                located++;
                return new ValueTask<LvalRowLocation>(new LvalRowLocation(page, 0, count + 4, null));
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(length, output.Length);
        Assert.Equal(rowCount, located);
        Assert.Equal(chunkSize, output.MaxWriteSize);
    }

    [Fact]
    public async Task CopyLongValueRawBytesAsync_PreservesOpaqueInlinePayloadAndIgnoresFollowingBytes()
    {
        byte[] payload = [0xFF, 0xFE, 0, 0x80, 0x7F, 0xFF];
        byte[] row = new byte[32];
        LongValueDescriptor.Inline(payload.Length).WriteTo(row.AsSpan(2));
        payload.CopyTo(row.AsSpan(14));
        row[20] = 0x55;
        var decoder = new LongValueDecoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!);
        await using var output = new MemoryStream();

        await decoder.CopyLongValueRawBytesAsync(row, 2, 19, output, maxChainRows: 0, TestContext.Current.CancellationToken);

        Assert.Equal(payload, output.ToArray());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task CopyPayloadAsync_RejectsReservedModeBeforeReadingOrWriting(int length)
    {
        await using var output = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                new LongValueDescriptor(length, Constants.LongValue.StorageModeMask, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                4096,
                100,
                100,
                (_, _) => throw new InvalidOperationException("must not read"),
                TestContext.Current.CancellationToken));
        Assert.Equal(0, output.Length);
    }

    [Theory]
    [InlineData(2, 100)]
    [InlineData(100, 0)]
    public async Task CopyPayloadAsync_RejectsBasicLimitsBeforeReadingOrWriting(int maxBytes, int maxRows)
    {
        await using var output = new MemoryStream();
        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(3, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                4096,
                maxBytes,
                maxRows,
                (_, _) => throw new InvalidOperationException("must not read"),
                TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
        Assert.Equal(0, output.Length);
    }

    [Fact]
    public async Task CopyPayloadAsync_RejectsInlineTruncationBeforeWriting()
    {
        await using var output = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Inline(3),
                new byte[] { 1, 2 },
                output,
                4096,
                100,
                0,
                (_, _) => throw new InvalidOperationException("must not read"),
                TestContext.Current.CancellationToken));
        Assert.Equal(0, output.Length);
    }

    [Fact]
    public async Task CopyPayloadAsync_StopsAtRowBudgetBeforeLocatingAnotherRow()
    {
        await using var output = new MemoryStream();
        int located = 0;
        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(10, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                100,
                maxChainRows: 2,
                (_, _) =>
                {
                    located++;
                    return new ValueTask<LvalRowLocation>(ChainedRow((uint)((located + 1) * 256), [1]));
                },
                TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
        Assert.Equal(2, located);
        Assert.Equal(2, output.Length);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(256u)]
    public async Task CopyPayloadAsync_RefusesTruncationOrCycleWithoutInventingMissingBytes(uint next)
    {
        await using var output = new MemoryStream();
        int located = 0;
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(10, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                100,
                100,
                (_, _) =>
                {
                    located++;
                    return new ValueTask<LvalRowLocation>(ChainedRow(next, [1, 2, 3]));
                },
                TestContext.Current.CancellationToken));
        Assert.Equal(1, located);
        Assert.Equal(new byte[] { 1, 2, 3 }, output.ToArray());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CopyPayloadAsync_PreservesDeclaredBytesWithoutInterpretingTrailingStorage(bool singlePage)
    {
        await using var output = new MemoryStream();
        byte[] page = [0xAB, 0xCD, 0xEF, 0, 0xFF, 0xFE, 0x80, 0, 0xAA];
        await LongValueStore.CopyPayloadAsync(
            singlePage ? LongValueDescriptor.SinglePage(3, 256, 42) : LongValueDescriptor.Chained(3, 256, 42),
            ReadOnlyMemory<byte>.Empty,
            output,
            64,
            100,
            100,
            (_, _) => new ValueTask<LvalRowLocation>(new LvalRowLocation(page, singlePage ? 4 : 0, singlePage ? 5 : 9, null)),
            TestContext.Current.CancellationToken);
        Assert.Equal(new byte[] { 0xFF, 0xFE, 0x80 }, output.ToArray());
    }

    [Fact]
    public async Task CopyPayloadAsync_PropagatesSourceIoFailureUnchanged()
    {
        var failure = new IOException("source failure");
        await using var output = new MemoryStream();
        IOException actual = await Assert.ThrowsAsync<IOException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(10, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                100,
                100,
                (_, _) => ValueTask.FromException<LvalRowLocation>(failure),
                TestContext.Current.CancellationToken));
        Assert.Same(failure, actual);
    }

    [Fact]
    public async Task CopyPayloadAsync_PropagatesDestinationIoFailureBeforeReadingAnotherRow()
    {
        var failure = new IOException("destination failure");
        await using var output = new FailingWriteStream(failure);
        int located = 0;
        IOException actual = await Assert.ThrowsAsync<IOException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(10, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                100,
                100,
                (_, _) =>
                {
                    located++;
                    return new ValueTask<LvalRowLocation>(ChainedRow(512, [1, 2, 3]));
                },
                TestContext.Current.CancellationToken));
        Assert.Same(failure, actual);
        Assert.Equal(1, located);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyPayloadAsync_PreservesSourceAndDestinationInvalidDataExceptionIdentity(bool destinationFails)
    {
        var failure = new InvalidDataException("storage failure");
        await using MemoryStream output = destinationFails ? new FailingWriteStream(failure) : new MemoryStream();
        InvalidDataException actual = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(3, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                100,
                100,
                (_, _) => destinationFails
                    ? new ValueTask<LvalRowLocation>(ChainedRow(0, [1, 2, 3]))
                    : ValueTask.FromException<LvalRowLocation>(failure),
                TestContext.Current.CancellationToken));
        Assert.Same(failure, actual);
    }

    [Theory]
    [InlineData(0, 100, 100, "pageSize")]
    [InlineData(64, -1, 100, "maxValueBytes")]
    [InlineData(64, 100, -1, "maxChainRows")]
    public async Task CopyPayloadAsync_ReportsInvalidBudgetParameter(int pageSize, int maxBytes, int maxRows, string parameter)
    {
        await using var output = new MemoryStream();
        ArgumentOutOfRangeException failure = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Inline(0),
                ReadOnlyMemory<byte>.Empty,
                output,
                pageSize,
                maxBytes,
                maxRows,
                (_, _) => throw new InvalidOperationException("must not read"),
                TestContext.Current.CancellationToken));
        Assert.Equal(parameter, failure.ParamName);
        Assert.Equal(0, output.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyPayloadAsync_RejectsNullDestinationOrLocator(bool nullDestination)
    {
        await using var output = new MemoryStream();
        ArgumentNullException failure = await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Inline(0),
                ReadOnlyMemory<byte>.Empty,
                nullDestination ? null! : output,
                64,
                100,
                100,
                nullDestination ? (_, _) => throw new InvalidOperationException("must not read") : null!,
                TestContext.Current.CancellationToken));
        Assert.Equal(nullDestination ? "destination" : "locateRowAsync", failure.ParamName);
    }

    [Fact]
    public async Task CopyPayloadAsync_CancelsBetweenChunksBeforeReadingAnotherRow()
    {
        using var cancellation = new CancellationTokenSource();
        await using var output = new VerifyingStream(cancellation.Cancel);
        int located = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(10, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                100,
                100,
                (_, _) =>
                {
                    located++;
                    return new ValueTask<LvalRowLocation>(ChainedRow(512, [Pattern(0)]));
                },
                cancellation.Token));
        Assert.Equal(1, located);
        Assert.Equal(1, output.Length);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task CopyPayloadAsync_ReleasesLocatedPageOnSuccessAndEveryFailure(int failureMode)
    {
        byte[] page = [0, 0, 0, 0, 1, 2, 3];
        int released = 0;
        await using Stream output = failureMode == 3 ? new FailingWriteStream(new IOException("write failure")) : new MemoryStream();
        async Task CopyAsync() => await LongValueStore.CopyPayloadAsync(
            LongValueDescriptor.Chained(3, 256, 0),
            ReadOnlyMemory<byte>.Empty,
            output,
            64,
            100,
            100,
            (_, _) => new ValueTask<LvalRowLocation>(new LvalRowLocation(
                page, failureMode == 2 ? page.Length : 0, page.Length, failureMode == 1 ? "deleted row" : null)),
            TestContext.Current.CancellationToken,
            releasedPage =>
            {
                Assert.Same(page, releasedPage);
                released++;
            });

        if (failureMode is 1 or 2)
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(CopyAsync);
        }
        else if (failureMode == 3)
        {
            await Assert.ThrowsAsync<IOException>(CopyAsync);
        }
        else
        {
            await CopyAsync();
        }

        Assert.Equal(1, released);
    }

    [Fact]
    public async Task CopyPayloadAsync_DoesNotReleaseBeforeAsynchronousDestinationConsumesPage()
    {
        byte[] page = [0, 0, 0, 0, 1, 2, 3];
        var writeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finishWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var output = new DelayedWriteStream(writeStarted, finishWrite);
        bool released = false;
        Task copy = LongValueStore.CopyPayloadAsync(
            LongValueDescriptor.Chained(3, 256, 0),
            ReadOnlyMemory<byte>.Empty,
            output,
            64,
            100,
            100,
            (_, _) => new ValueTask<LvalRowLocation>(new LvalRowLocation(page, 0, page.Length, null)),
            TestContext.Current.CancellationToken,
            _ => released = true).AsTask();

        await writeStarted.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.False(released);
        finishWrite.SetResult();
        await copy;
        Assert.True(released);
        Assert.Equal(new byte[] { 1, 2, 3 }, output.ToArray());
    }

    private static byte Pattern(int offset) => unchecked((byte)((offset * 13) + 1));

    private static LvalRowLocation ChainedRow(uint next, byte[] payload)
    {
        byte[] page = new byte[payload.Length + 4];
        Wu32(page, 0, next);
        payload.CopyTo(page.AsSpan(4));
        return new LvalRowLocation(page, 0, page.Length, null);
    }

    private sealed class FailingWriteStream(Exception failure) : MemoryStream
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromException(failure);
    }

    private sealed class DelayedWriteStream(TaskCompletionSource writeStarted, TaskCompletionSource finishWrite) : MemoryStream
    {
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            writeStarted.SetResult();
            await finishWrite.Task.WaitAsync(cancellationToken);
            await base.WriteAsync(buffer, cancellationToken);
        }
    }

    private sealed class VerifyingStream(Action? afterWrite = null) : Stream
    {
        private long length;

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => this.length;

        public override long Position { get => this.length; set => throw new NotSupportedException(); }

        internal int MaxWriteSize { get; private set; }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => this.Verify(buffer.AsSpan(offset, count));

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Verify(buffer.Span);
            return ValueTask.CompletedTask;
        }

        private void Verify(ReadOnlySpan<byte> bytes)
        {
            for (int index = 0; index < bytes.Length; index++)
            {
                Assert.Equal(Pattern(checked((int)this.length + index)), bytes[index]);
            }

            this.length += bytes.Length;
            this.MaxWriteSize = Math.Max(this.MaxWriteSize, bytes.Length);
            afterWrite?.Invoke();
        }
    }
}
