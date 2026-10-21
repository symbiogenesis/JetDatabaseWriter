namespace JetDatabaseWriter.Tests.Transactions;

using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.TestSupport;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Checks native record locks with a separate process rather than another POSIX handle.</summary>
public sealed class CrossProcessLockTests
{
    /// <summary>Page and commit locks exclude other processes and release their native ranges.</summary>
    /// <param name="commitLock">Whether to exercise the high-offset commit sentinel.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HeldRange_ExcludesChildProcess_AndReleaseAllowsAcquisition(bool commitLock)
    {
        string path = Path.Combine(Path.GetTempPath(), $"CrossProcessLockTests_{Guid.NewGuid():N}.bin");
        await File.WriteAllBytesAsync(path, new byte[8192], TestContext.Current.CancellationToken);
        try
        {
            await using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            var helper = JetByteRangeLock.Create(stream, enabled: true, lockTimeoutMilliseconds: 1000);
            if (OperatingSystem.IsMacOS())
            {
                // The BCL deliberately does not support FileStream.Lock on macOS.
                Assert.False(helper.IsEnabled);
                using IDisposable inert = await helper.AcquirePageLockAsync(1, 4096, TestContext.Current.CancellationToken);
                Assert.NotNull(inert);
                return;
            }

            Assert.True(helper.IsEnabled);
            long offset = commitLock ? 0xFFFFFFFCL : 4096;
            long length = commitLock ? 1 : 4096;
            if (commitLock)
            {
                long? held = await helper.AcquireCommitLockOffsetAsync(offset, TestContext.Current.CancellationToken);
                try
                {
                    AssertChildLock(path, offset, length, expectedExitCode: 23);
                    AssertChildLock(path, 0, 1, expectedExitCode: 0);
                }
                finally
                {
                    helper.ReleaseCommitLock(held);
                }
            }
            else
            {
                using (IDisposable held = await helper.AcquirePageLockAsync(1, 4096, TestContext.Current.CancellationToken))
                {
                    AssertChildLock(path, offset, length, expectedExitCode: 23);
                    AssertChildLock(path, 0, 1, expectedExitCode: 0);
                }
            }

            AssertChildLock(path, offset, length, expectedExitCode: 0);
            Assert.Equal(8192, stream.Length);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static void AssertChildLock(string path, long offset, long length, int expectedExitCode)
    {
        string quotedPath = path.Replace("'", "''", StringComparison.Ordinal);
        string script = $"$ErrorActionPreference='Stop'; $s=[System.IO.FileStream]::new('{quotedPath}', [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::ReadWrite); " +
            $"try {{ try {{ $s.Lock({offset.ToString(CultureInfo.InvariantCulture)}, {length.ToString(CultureInfo.InvariantCulture)}) }} catch [System.IO.IOException] {{ exit 23 }}; " +
            $"$s.Unlock({offset.ToString(CultureInfo.InvariantCulture)}, {length.ToString(CultureInfo.InvariantCulture)}); exit 0 }} finally {{ $s.Dispose() }}";
        string encoded = Convert.ToBase64String(Encoding.Unicode.GetBytes(script));
        PowerShellRunResult result = PowerShellProcessRunner.Run(
            "pwsh",
            ["-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
            TimeSpan.FromSeconds(60));
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
        Assert.True(result.ExitCode == expectedExitCode, $"Child exit {result.ExitCode}, expected {expectedExitCode}: {result.StandardError}");
    }
}
