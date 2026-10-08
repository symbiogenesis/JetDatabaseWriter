namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Relationships;
using Xunit;

/// <summary>Byte budgets apply to consumption, including a source that grows after opening.</summary>
public sealed class LinkedTextSourceStreamTests
{
    [Fact]
    public async Task GrowingSource_ThrowsWhenAdditionalBytesExceedBudget()
    {
        await using var source = new MemoryStream();
        source.Write([1, 2]);
        source.Position = 0;
        await using var bounded = new LinkedTextSourceStream(source, 2, "Growing");
        byte[] bytes = new byte[2];
        Assert.Equal(2, await bounded.ReadAsync(bytes.AsMemory(), TestContext.Current.CancellationToken));
        source.WriteByte(3);
        source.Position = 2;
        InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            _ = await bounded.ReadAsync(bytes.AsMemory(), TestContext.Current.CancellationToken));
        Assert.Contains(nameof(AccessReaderOptions.LinkedTextMaxSourceFileBytes), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CanceledRead_DoesNotConsumeBudget()
    {
        await using var source = new MemoryStream([1]);
        await using var bounded = new LinkedTextSourceStream(source, long.MaxValue, "Canceled");
        byte[] bytes = new byte[1];
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            _ = await bounded.ReadAsync(bytes.AsMemory(), canceled.Token));
        Assert.Equal(0, bounded.Position);
        Assert.Equal(1, await bounded.ReadAsync(bytes.AsMemory(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public void ExcessRead_StaysRefusedAfterSourceIsConsumed()
    {
        using var source = new MemoryStream([1, 2]);
        using var bounded = new LinkedTextSourceStream(source, 1, "Exceeded");
        byte[] bytes = new byte[2];
        _ = Assert.Throws<InvalidDataException>(() => bounded.Read(bytes, 0, bytes.Length));
        _ = Assert.Throws<InvalidDataException>(() => bounded.Read(bytes, 0, bytes.Length));
    }

    [Fact]
    public void ExactBudget_SynchronousReadAllowsEndOfStream()
    {
        using var source = new MemoryStream([1, 2]);
        using var bounded = new LinkedTextSourceStream(source, 2, "Exact");
        byte[] bytes = new byte[8];
        Assert.Equal(2, bounded.Read(bytes, 0, bytes.Length));
        Assert.Equal(0, bounded.Read(bytes, 0, bytes.Length));
    }

    [Fact]
    public async Task ExactBudget_AllowsEndOfStreamAndEmptyReads()
    {
        await using var source = new MemoryStream([1, 2]);
        await using var bounded = new LinkedTextSourceStream(source, 2, "Exact");
        byte[] bytes = new byte[8];
        Assert.Equal(2, await bounded.ReadAsync(bytes.AsMemory(), TestContext.Current.CancellationToken));
        Assert.Equal(0, await bounded.ReadAsync(Memory<byte>.Empty, TestContext.Current.CancellationToken));
        Assert.Equal(0, await bounded.ReadAsync(bytes.AsMemory(), TestContext.Current.CancellationToken));
    }
}
