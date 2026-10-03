namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>
/// Runs a real Windows PowerShell host through <see cref="PowerShellProcessRunner"/>, which the
/// DAO probe and the DAO round-trip scripts use. A hung host must be killed with every process it
/// started once the timeout passes, and a child process that keeps the output pipes open after
/// the host exits must not block the caller. Both legs run these, so the runner is checked on
/// net8.0 as well as net10.0.
/// </summary>
public sealed class PowerShellProcessRunnerTests
{
    private const string WindowsOnly = "Runs powershell.exe and ping.exe, which only Windows has.";

    private static string PowerShellPath =>
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), @"WindowsPowerShell\v1.0\powershell.exe");

    [Fact]
    public void Run_ScriptWritesBothStreams_ReturnsOutputAndExitCode()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), WindowsOnly);

        PowerShellRunResult result = PowerShellProcessRunner.Run(
            PowerShellPath,
            CommandArguments("Write-Output 'out'; [Console]::Error.WriteLine('err'); exit 3"),
            TimeSpan.FromSeconds(60));

        Assert.Equal(3, result.ExitCode);
        Assert.Contains("out", result.StandardOutput, StringComparison.Ordinal);
        Assert.Contains("err", result.StandardError, StringComparison.Ordinal);
        Assert.False(result.TimedOut);
        Assert.True(result.OutputComplete);
    }

    [Fact]
    public async Task Run_HostOutlivesTimeout_KillsHostAndGrandchild()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), WindowsOnly);

        // The host starts a ping that would run for two minutes and shares the host's output
        // pipes, records the ping's process id, and sleeps for two minutes. The id goes to a
        // file because the host's buffered output is lost when it is killed. The 30 s timeout
        // leaves room for a slow host start on a busy machine (12 s was seen here).
        string pidPath = Path.Combine(Path.GetTempPath(), $"ps-runner-{Guid.NewGuid():N}.txt");
        var stopwatch = Stopwatch.StartNew();
        PowerShellRunResult result = PowerShellProcessRunner.Run(
            PowerShellPath,
            CommandArguments(StartPingScript(pidPath) + "; Start-Sleep -Seconds 120"),
            TimeSpan.FromSeconds(30));
        stopwatch.Stop();

        int? pingId = ReadProcessId(pidPath);
        try
        {
            Assert.True(result.TimedOut);
            Assert.Equal(-1, result.ExitCode);
            Assert.True(
                stopwatch.Elapsed < TimeSpan.FromSeconds(90),
                $"Run returned after {stopwatch.Elapsed.TotalSeconds:F1} s; the 30 s timeout did not stop the host.");
            Assert.True(pingId.HasValue, "The host did not record the ping's process id before the timeout.");
            Assert.True(
                await HasExitedWithinAsync(pingId.Value, TimeSpan.FromSeconds(5)),
                "The ping the host started is still running after the timeout killed the host.");
        }
        finally
        {
            TryKill(pingId);
            File.Delete(pidPath);
        }
    }

    [Fact]
    public void Run_GrandchildKeepsPipeOpen_ReturnsAfterDrainTimeout()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), WindowsOnly);

        // The host exits at once, but the ping it started keeps the output pipes open for two
        // minutes.
        string pidPath = Path.Combine(Path.GetTempPath(), $"ps-runner-{Guid.NewGuid():N}.txt");
        var stopwatch = Stopwatch.StartNew();
        PowerShellRunResult result = PowerShellProcessRunner.Run(
            PowerShellPath,
            CommandArguments(StartPingScript(pidPath) + "; Write-Output 'host done'; exit 0"),
            TimeSpan.FromSeconds(90),
            drainTimeout: TimeSpan.FromSeconds(1));
        stopwatch.Stop();

        int? pingId = ReadProcessId(pidPath);
        try
        {
            Assert.True(
                stopwatch.Elapsed < TimeSpan.FromSeconds(60),
                $"Run returned after {stopwatch.Elapsed.TotalSeconds:F1} s; it waited for the ping to close the pipes.");
            Assert.Equal(0, result.ExitCode);
            Assert.False(result.TimedOut);
            Assert.False(result.OutputComplete);
            Assert.Contains("host done", result.StandardOutput, StringComparison.Ordinal);
            Assert.True(pingId.HasValue, "The host did not record the ping's process id.");
        }
        finally
        {
            TryKill(pingId);
            File.Delete(pidPath);
        }
    }

    private static string[] CommandArguments(string script) =>
        ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script];

    private static string StartPingScript(string pidPath) =>
        "$p = Start-Process ping.exe -ArgumentList '-n','120','127.0.0.1' -NoNewWindow -PassThru; " +
        $"Set-Content -LiteralPath {AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(pidPath)} -Value $p.Id";

    private static int? ReadProcessId(string pidPath)
    {
        try
        {
            return int.TryParse(File.ReadAllText(pidPath).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out int id)
                ? id
                : null;
        }
        catch (IOException)
        {
            // The host never wrote the file.
            return null;
        }
    }

    private static async Task<bool> HasExitedWithinAsync(int processId, TimeSpan timeout)
    {
        var stopwatch = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                using var process = Process.GetProcessById(processId);
                if (process.HasExited)
                {
                    return true;
                }
            }
            catch (ArgumentException)
            {
                // No process has that id any more.
                return true;
            }

            if (stopwatch.Elapsed >= timeout)
            {
                return false;
            }

            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
    }

    private static void TryKill(int? processId)
    {
        if (processId is not int id)
        {
            return;
        }

        try
        {
            using var process = Process.GetProcessById(id);
            process.Kill(entireProcessTree: true);
        }
        catch (ArgumentException)
        {
            // Already gone.
        }
        catch (InvalidOperationException)
        {
            // Exited while being killed.
        }
    }
}
