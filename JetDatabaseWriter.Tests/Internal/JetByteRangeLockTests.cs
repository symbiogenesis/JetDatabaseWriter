namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal;
using Xunit;

/// <summary>
/// Tests for <see cref="JetByteRangeLock"/> — the cooperative JET page-lock helper.
/// Lock acquisition uses Win32 <c>LockFileEx</c>; the contention tests are skipped on
/// non-Windows hosts where the helper degrades to a no-op.
/// </summary>
public sealed class JetByteRangeLockTests : IDisposable
{
    private static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    private readonly string _tempPath;

    public JetByteRangeLockTests()
    {
        _tempPath = Path.Combine(Path.GetTempPath(), $"JetByteRangeLockTests_{Guid.NewGuid():N}.bin");
        File.WriteAllBytes(_tempPath, new byte[16 * 4096]);
    }

    public void Dispose()
    {
        try
        {
            File.Delete(_tempPath);
        }
        catch (IOException)
        {
            /* best-effort */
        }
    }

    [Fact]
    public void Disabled_ReturnsInertInstance_NoOps()
    {
        using var fs = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

        JetByteRangeLock helper = JetByteRangeLock.Create(fs, enabled: false, lockTimeoutMilliseconds: 1_000);

        Assert.False(helper.IsEnabled);

        // Should return immediately with a no-op disposable on a disabled instance.
        using IDisposable token = helper.AcquirePageLock(pageNumber: 0, pageSize: 4096);
        Assert.NotNull(token);
    }

    [Fact]
    public void Create_NonFileStream_ReturnsInertInstance()
    {
        using var ms = new MemoryStream();

        JetByteRangeLock helper = JetByteRangeLock.Create(ms, enabled: true, lockTimeoutMilliseconds: 1_000);

        Assert.False(helper.IsEnabled);
    }

    [Fact]
    public void Acquire_SameInstance_TwoPages_BothSucceed()
    {
        using var fs = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        JetByteRangeLock helper = JetByteRangeLock.Create(fs, enabled: true, lockTimeoutMilliseconds: 1_000);

        using IDisposable a = helper.AcquirePageLock(pageNumber: 1, pageSize: 4096);
        using IDisposable b = helper.AcquirePageLock(pageNumber: 2, pageSize: 4096);
    }

    [Fact]
    public void TwoFileStreams_ContendForSamePage_SecondTimesOut_OnWindows()
    {
        if (!IsWindows)
        {
            return; // No-op on non-Windows; helper is inert there.
        }

        using var first = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        using var second = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

        JetByteRangeLock holder = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 1_000);
        JetByteRangeLock contender = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 200);

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

        await using var first = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);
        await using var second = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);

        JetByteRangeLock holder = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 1_000);
        JetByteRangeLock contender = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 200);

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

        using var first = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        using var second = new FileStream(_tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

        JetByteRangeLock a = JetByteRangeLock.Create(first, enabled: true, lockTimeoutMilliseconds: 500);
        JetByteRangeLock b = JetByteRangeLock.Create(second, enabled: true, lockTimeoutMilliseconds: 500);

        using IDisposable t1 = a.AcquirePageLock(pageNumber: 5, pageSize: 4096);
        using IDisposable t2 = b.AcquirePageLock(pageNumber: 6, pageSize: 4096);
    }
}
