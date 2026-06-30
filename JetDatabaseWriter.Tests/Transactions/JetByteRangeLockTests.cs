namespace JetDatabaseWriter.Tests.Transactions;

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
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
    public void Disabled_ReturnsInertInstance_NoOps()
    {
        using FileStream fs = OpenReadWriteStream(this.tempPath);

        var helper = JetByteRangeLock.Create(fs, enabled: false, lockTimeoutMilliseconds: 1_000);

        Assert.False(helper.IsEnabled);

        // Should return immediately with a no-op disposable on a disabled instance.
        using IDisposable token = helper.AcquirePageLock(pageNumber: 0, pageSize: 4096);
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
    public void Acquire_SameInstance_TwoPages_BothSucceed()
    {
        using FileStream fs = OpenReadWriteStream(this.tempPath);
        var helper = JetByteRangeLock.Create(fs, enabled: true, lockTimeoutMilliseconds: 1_000);

        Assert.Equal(JetByteRangeLock.PlatformSupportsByteRangeLocks(), helper.IsEnabled);

        using IDisposable a = helper.AcquirePageLock(pageNumber: 1, pageSize: 4096);
        using IDisposable b = helper.AcquirePageLock(pageNumber: 2, pageSize: 4096);
    }

    [Fact]
    public void TwoFileStreams_ContendForSamePage_SecondTimesOut_OnWindows()
    {
        if (!IsWindows)
        {
            return;
        }

        using FileStream first = OpenReadWriteStream(this.tempPath);
        using FileStream second = OpenReadWriteStream(this.tempPath);

        var holder = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 1_000);
        var contender = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 200);

        Assert.True(holder.IsEnabled);
        Assert.True(contender.IsEnabled);

        using IDisposable held = holder.AcquirePageLock(pageNumber: 3, pageSize: 4096);

        IOException ex = Assert.Throws<IOException>(() => contender.AcquirePageLock(pageNumber: 3, pageSize: 4096));
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

        await Assert.ThrowsAsync<IOException>(async () =>
            await contender.AcquirePageLockAsync(pageNumber: 4, pageSize: 4096, TestContext.Current.CancellationToken));
    }

    [Fact]
    public void TwoFileStreams_DifferentPages_BothAcquire_OnWindows()
    {
        if (!IsWindows)
        {
            return;
        }

        using FileStream first = OpenReadWriteStream(this.tempPath);
        using FileStream second = OpenReadWriteStream(this.tempPath);

        var a = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 500);
        var b = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 500);

        using IDisposable t1 = a.AcquirePageLock(pageNumber: 5, pageSize: 4096);
        using IDisposable t2 = b.AcquirePageLock(pageNumber: 6, pageSize: 4096);
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
