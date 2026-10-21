namespace JetDatabaseWriter.Tests.Transactions;

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>
/// Tests for <see cref="JetByteRangeLock"/> — the cooperative JET page-lock helper.
/// Lock acquisition uses <see cref="FileStream.Lock(long, long)"/> where the BCL
/// supports byte-range locks. Same-process contention tests remain Windows-only
/// because POSIX record locks are process-scoped.
/// </summary>
public sealed class JetByteRangeLockTests : IDisposable
{
    private static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    private readonly string tempPath;

    public JetByteRangeLockTests()
    {
        this.tempPath = Path.Combine(Path.GetTempPath(), $"JetByteRangeLockTests_{Guid.NewGuid():N}.bin");
        File.WriteAllBytes(this.tempPath, new byte[16 * 4096]);
    }

    public void Dispose()
    {
        try
        {
            File.Delete(this.tempPath);
        }
        catch (IOException)
        {
            /* best-effort */
        }
    }

    [Fact]
    public async Task Disabled_ReturnsInertInstance_NoOps()
    {
        await using FileStream fs = OpenReadWriteStream(this.tempPath);

        var helper = JetByteRangeLock.Create(fs, enabled: false, lockTimeoutMilliseconds: 1_000);

        Assert.False(helper.IsEnabled);

        // Should return immediately with a no-op disposable on a disabled instance.
        using IDisposable token = await helper.AcquirePageLockAsync(pageNumber: 0, pageSize: 4096, TestContext.Current.CancellationToken);
        Assert.NotNull(token);
    }

    [Fact]
    public void Create_NonFileStream_ReturnsInertInstance()
    {
        using var ms = new MemoryStream();

        var helper = JetByteRangeLock.Create(ms, enabled: true, lockTimeoutMilliseconds: 1_000);

        Assert.False(helper.IsEnabled);
    }

    [Fact]
    public void Create_NegativeLockTimeout_ThrowsArgumentOutOfRangeException()
    {
        using var ms = new MemoryStream();

        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            JetByteRangeLock.Create(ms, enabled: false, lockTimeoutMilliseconds: -1));

        Assert.Equal("value", ex.ParamName);
    }

    [Fact]
    public async Task Acquire_SameInstance_TwoPages_BothSucceed()
    {
        await using FileStream fs = OpenReadWriteStream(this.tempPath);
        var helper = JetByteRangeLock.Create(fs, enabled: true, lockTimeoutMilliseconds: 1_000);

        Assert.Equal(JetByteRangeLock.PlatformSupportsByteRangeLocks(), helper.IsEnabled);

        using IDisposable a = await helper.AcquirePageLockAsync(pageNumber: 1, pageSize: 4096, TestContext.Current.CancellationToken);
        using IDisposable b = await helper.AcquirePageLockAsync(pageNumber: 2, pageSize: 4096, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TwoFileStreams_ContendForSamePage_SecondTimesOut_OnWindows()
    {
        if (!IsWindows)
        {
            return;
        }

        await using FileStream first = OpenReadWriteStream(this.tempPath);
        await using FileStream second = OpenReadWriteStream(this.tempPath);

        var holder = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 1_000);
        var contender = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 200);

        Assert.True(holder.IsEnabled);
        Assert.True(contender.IsEnabled);

        using IDisposable held = await holder.AcquirePageLockAsync(pageNumber: 3, pageSize: 4096, TestContext.Current.CancellationToken);

        JetLockException ex = await Assert.ThrowsAsync<JetLockException>(async () =>
            await contender.AcquirePageLockAsync(pageNumber: 3, pageSize: 4096, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.LockTimeout, ex.ErrorCode);
        Assert.Equal(3, ex.ErrorInfo.PageNumber);
        Assert.Contains("Timed out", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoFileStreams_ContendForSamePage_AsyncSecondTimesOut_OnWindows()
    {
        if (!IsWindows)
        {
            return;
        }

        await using FileStream first = OpenReadWriteStream(this.tempPath, FileOptions.Asynchronous);
        await using FileStream second = OpenReadWriteStream(this.tempPath, FileOptions.Asynchronous);

        var holder = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 1_000);
        var contender = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 200);

        using IDisposable held = await holder.AcquirePageLockAsync(pageNumber: 4, pageSize: 4096, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetLockException>(async () =>
            await contender.AcquirePageLockAsync(pageNumber: 4, pageSize: 4096, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TwoFileStreams_DifferentPages_BothAcquire_OnWindows()
    {
        if (!IsWindows)
        {
            return;
        }

        await using FileStream first = OpenReadWriteStream(this.tempPath);
        await using FileStream second = OpenReadWriteStream(this.tempPath);

        var a = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 500);
        var b = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 500);

        using IDisposable t1 = await a.AcquirePageLockAsync(pageNumber: 5, pageSize: 4096, TestContext.Current.CancellationToken);
        using IDisposable t2 = await b.AcquirePageLockAsync(pageNumber: 6, pageSize: 4096, TestContext.Current.CancellationToken);
    }

    /// <summary>Cancellation is honored even when acquisition would succeed immediately or be a no-op.</summary>
    /// <param name="enabled">Whether byte-range locking is requested.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Acquire_CanceledBeforeAttempt_DoesNotAcquire(bool enabled)
    {
        await using FileStream stream = OpenReadWriteStream(this.tempPath, FileOptions.Asynchronous);
        var helper = JetByteRangeLock.Create(stream, enabled, lockTimeoutMilliseconds: 1_000);
        using var canceled = new System.Threading.CancellationTokenSource();
        await canceled.CancelAsync();

        OperationCanceledException pageError = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            using IDisposable unexpected = await helper.AcquirePageLockAsync(1, 4096, canceled.Token);
        });
        Assert.Equal(canceled.Token, pageError.CancellationToken);

        OperationCanceledException commitError = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            long? unexpected = await helper.AcquireCommitLockOffsetAsync(65536, canceled.Token);
            helper.ReleaseCommitLock(unexpected);
        });
        Assert.Equal(canceled.Token, commitError.CancellationToken);

        using IDisposable acquired = await helper.AcquirePageLockAsync(1, 4096, TestContext.Current.CancellationToken);
        Assert.NotNull(acquired);
    }

    private static FileStream OpenReadWriteStream(string path, FileOptions options = FileOptions.None) =>
        new(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.ReadWrite,
                Share = FileShare.ReadWrite,
                Options = options,
                BufferSize = 4096,
            });
}
