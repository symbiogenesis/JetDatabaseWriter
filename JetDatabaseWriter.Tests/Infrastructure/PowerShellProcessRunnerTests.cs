namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.TestSupport;
using Microsoft.Win32.SafeHandles;
using Xunit;

/// <summary>
/// Runs a real Windows PowerShell host through <see cref="PowerShellProcessRunner"/>, which the
/// DAO probe and the DAO round-trip scripts use. A hung host must be killed with every process it
/// started once the timeout passes, a process in that tree that cannot be killed must not turn
/// the timeout into an exception, and a child process that keeps the output pipes open after the
/// host exits must not block the caller. Both legs run these, so the runner is checked on net8.0
/// as well as net10.0.
/// </summary>
public sealed partial class PowerShellProcessRunnerTests
{
    private const string WindowsOnly = "Runs powershell.exe and ping.exe, which only Windows has.";

    /// <summary>
    /// The first timeout of the tests that let a host outlive it. The timeout must pass only
    /// after the host has started the ping and recorded its id, and host start-up varies widely
    /// (well under a second on CI, 12 s on a busy machine), so a test whose host was too slow
    /// runs again with twice the timeout, up to <see cref="LastTimeout"/>.
    /// </summary>
    private static readonly TimeSpan FirstTimeout = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan LastTimeout = TimeSpan.FromSeconds(40);

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
        // file because the host's buffered output is lost when it is killed.
        for (TimeSpan timeout = FirstTimeout; ; timeout += timeout)
        {
            string pidPath = Path.Combine(Path.GetTempPath(), $"ps-runner-{Guid.NewGuid():N}.txt");
            var stopwatch = Stopwatch.StartNew();
            PowerShellRunResult result = PowerShellProcessRunner.Run(
                PowerShellPath,
                CommandArguments(StartPingScript(pidPath) + "; Start-Sleep -Seconds 120"),
                timeout);
            stopwatch.Stop();

            int? pingId = ReadProcessId(pidPath);
            try
            {
                Assert.True(result.TimedOut);
                Assert.Equal(-1, result.ExitCode);
                Assert.True(
                    stopwatch.Elapsed < timeout + TimeSpan.FromSeconds(60),
                    $"Run returned after {stopwatch.Elapsed.TotalSeconds:F1} s; the {timeout.TotalSeconds:F0} s timeout did not stop the host.");
                if (pingId is null && timeout < LastTimeout)
                {
                    // The host started too slowly to record the id before the timeout.
                    continue;
                }

                Assert.True(pingId.HasValue, "The host did not record the ping's process id before the timeout.");
                Assert.True(
                    await HasExitedWithinAsync(pingId.Value, TimeSpan.FromSeconds(5)),
                    "The ping the host started is still running after the timeout killed the host.");
                return;
            }
            finally
            {
                TryKill(pingId);
                File.Delete(pidPath);
            }
        }
    }

    [Fact]
    [SupportedOSPlatform("windows")]
    public async Task Run_TimeoutCannotKillGrandchild_ReturnsTimedOutResult()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), WindowsOnly);

        // As soon as the host records the ping's process id, the test denies this user the
        // right to terminate the ping, as an elevated or protected process would. The run
        // holds no privileges, because SeDebugPrivilege opens a process whatever its
        // access-control list denies. When the timeout passes, Kill(entireProcessTree: true)
        // kills the host, fails on the ping and reports that failure in an AggregateException.
        // The run must still come back as a timeout: the DAO probe and scripts treat anything
        // else as a broken environment.
        for (TimeSpan timeout = FirstTimeout; ; timeout += timeout)
        {
            string pidPath = Path.Combine(Path.GetTempPath(), $"ps-runner-{Guid.NewGuid():N}.txt");
            using var stopGuard = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            Task<Process?> guardedPing = DenyTerminateOnceStartedAsync(pidPath, stopGuard.Token);
            try
            {
                PowerShellRunResult result = RunWithoutPrivileges(() => PowerShellProcessRunner.Run(
                    PowerShellPath,
                    CommandArguments(StartPingScript(pidPath) + "; Start-Sleep -Seconds 120"),
                    timeout,
                    drainTimeout: TimeSpan.FromSeconds(1)));
                await stopGuard.CancelAsync();
                Process? ping = await guardedPing;

                Assert.True(result.TimedOut);
                Assert.Equal(-1, result.ExitCode);
                if (ping is not { HasExited: false } && timeout < LastTimeout)
                {
                    // The host started too slowly: the timeout killed the ping, or the host before
                    // it recorded the id, before the test could deny the right to terminate it.
                    continue;
                }

                Assert.True(ping is not null, "The host did not record the ping's process id before the timeout.");
                Assert.False(ping.HasExited, "The ping exited, so killing it was never refused and the test checked nothing.");
                return;
            }
            finally
            {
                await stopGuard.CancelAsync();
                using Process? ping = await guardedPing;
                KillWithOwnHandle(ping);
                File.Delete(pidPath);
            }
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

    [Fact(DisableParallelization = true)]
    public void Run_ThreadPoolBusy_CapturesOutputBeforeDrainTimeout()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), WindowsOnly);

        // Every thread-pool thread is blocked, with more work queued behind them, as can happen
        // when the whole suite runs in parallel. The lines the host writes before it exits must
        // still reach the result within the drain timeout. The test starves the pool, so it runs
        // alone.
        var gate = new PoolGate();
        int blockers = ThreadPool.ThreadCount + (Environment.ProcessorCount * 8);
        for (int i = 0; i < blockers; i++)
        {
            _ = ThreadPool.UnsafeQueueUserWorkItem(static blocker => blocker.Block(), gate, preferLocal: false);
        }

        try
        {
            PowerShellRunResult result = PowerShellProcessRunner.Run(
                PowerShellPath,
                CommandArguments("Write-Output 'host done'; exit 0"),
                TimeSpan.FromSeconds(60),
                drainTimeout: TimeSpan.FromSeconds(2));

            Assert.Equal(0, result.ExitCode);
            Assert.True(result.OutputComplete, "The output did not reach its end within the drain timeout.");
            Assert.Contains("host done", result.StandardOutput, StringComparison.Ordinal);
        }
        finally
        {
            gate.Release();
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

    /// <summary>
    /// Waits for the host to record the ping's process id, opens the ping with full access, and
    /// then adds an entry to its access-control list that denies this user the right to terminate
    /// it, so every handle opened afterwards, including the one <c>Kill(entireProcessTree: true)</c>
    /// opens, lacks that right.
    /// </summary>
    /// <param name="pidPath">The file the host writes the ping's process id to.</param>
    /// <param name="stop">Stops the wait for the process id.</param>
    /// <returns>The ping, whose own handle can still kill it, or <see langword="null"/> when no id was recorded before <paramref name="stop"/>.</returns>
    /// <exception cref="InvalidOperationException">The current Windows identity has no user SID.</exception>
    [SupportedOSPlatform("windows")]
    private static async Task<Process?> DenyTerminateOnceStartedAsync(string pidPath, CancellationToken stop)
    {
        using var identity = WindowsIdentity.GetCurrent();
        SecurityIdentifier user = identity.User ?? throw new InvalidOperationException("The current Windows identity has no user SID.");
        while (!stop.IsCancellationRequested)
        {
            if (ReadProcessId(pidPath) is int id)
            {
                var ping = Process.GetProcessById(id);
                ProcessSecurity.DenyTerminate(ping.SafeHandle, user);
                return ping;
            }

            try
            {
                await Task.Delay(50, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return null;
    }

    /// <summary>
    /// Runs <paramref name="run"/> on this thread while impersonating this user's token with every
    /// privilege removed. When the process token holds SeDebugPrivilege, as the elevated token of
    /// a CI runner does, .NET enables it for the whole process, and a process opened under it
    /// grants any access its access-control list denies.
    /// </summary>
    /// <typeparam name="T">The result of <paramref name="run"/>.</typeparam>
    /// <param name="run">The work to run without privileges.</param>
    /// <returns>What <paramref name="run"/> returned.</returns>
    /// <exception cref="Win32Exception">The token without privileges could not be created.</exception>
    [SupportedOSPlatform("windows")]
    private static T RunWithoutPrivileges<T>(Func<T> run)
    {
        using var identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Duplicate | TokenAccessLevels.Query | TokenAccessLevels.Impersonate);
        if (!NativeMethods.CreateRestrictedToken(
            identity.AccessToken,
            NativeMethods.DisableMaxPrivilege,
            0,
            IntPtr.Zero,
            0,
            IntPtr.Zero,
            0,
            IntPtr.Zero,
            out SafeAccessTokenHandle token))
        {
            throw new Win32Exception();
        }

        using (token)
        {
            return WindowsIdentity.RunImpersonated(token, run);
        }
    }

    private static void KillWithOwnHandle(Process? process)
    {
        if (process is null)
        {
            return;
        }

        // The handle this Process opened before the deny entry was added still has the right to
        // terminate the process.
        process.Kill();
        _ = process.WaitForExit(TimeSpan.FromSeconds(10));
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

    private static partial class NativeMethods
    {
        /// <summary>Removes every privilege but SeChangeNotifyPrivilege from the new token.</summary>
        public const int DisableMaxPrivilege = 0x1;

        [LibraryImport("advapi32.dll", SetLastError = true)]
        [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static partial bool CreateRestrictedToken(
            SafeAccessTokenHandle existingTokenHandle,
            int flags,
            int disableSidCount,
            IntPtr sidsToDisable,
            int deletePrivilegeCount,
            IntPtr privilegesToDelete,
            int restrictedSidCount,
            IntPtr sidsToRestrict,
            out SafeAccessTokenHandle newTokenHandle);
    }

    /// <summary>The discretionary access-control list of a process, read and written through a handle to it.</summary>
    [SupportedOSPlatform("windows")]
    private sealed class ProcessSecurity : NativeObjectSecurity
    {
        private const int ProcessTerminate = 0x0001;

        private ProcessSecurity(SafeHandle process)
            : base(isContainer: false, ResourceType.KernelObject, process, AccessControlSections.Access)
        {
        }

        public override Type AccessRightType => typeof(int);

        public override Type AccessRuleType => typeof(ProcessAccessRule);

        public override Type AuditRuleType => typeof(AuditRule);

        /// <summary>Denies <paramref name="user"/> <c>PROCESS_TERMINATE</c> on the process <paramref name="process"/> refers to.</summary>
        /// <param name="process">A handle with <c>READ_CONTROL</c> and <c>WRITE_DAC</c> access.</param>
        /// <param name="user">The user to deny.</param>
        public static void DenyTerminate(SafeHandle process, SecurityIdentifier user)
        {
            var security = new ProcessSecurity(process);
            security.AddAccessRule(new ProcessAccessRule(user, ProcessTerminate, AccessControlType.Deny));
            security.Persist(process, AccessControlSections.Access);
        }

        public override AccessRule AccessRuleFactory(
            IdentityReference identityReference,
            int accessMask,
            bool isInherited,
            InheritanceFlags inheritanceFlags,
            PropagationFlags propagationFlags,
            AccessControlType type) =>
            new ProcessAccessRule(identityReference, accessMask, type);

        public override AuditRule AuditRuleFactory(
            IdentityReference identityReference,
            int accessMask,
            bool isInherited,
            InheritanceFlags inheritanceFlags,
            PropagationFlags propagationFlags,
            AuditFlags flags) =>
            throw new NotSupportedException("Process audit rules are not used.");
    }

    /// <summary>One entry of a process's access-control list.</summary>
    /// <param name="identity">The user or group the entry applies to.</param>
    /// <param name="accessMask">The process access rights it allows or denies.</param>
    /// <param name="type">Whether it allows or denies them.</param>
    [SupportedOSPlatform("windows")]
    private sealed class ProcessAccessRule(IdentityReference identity, int accessMask, AccessControlType type)
        : AccessRule(identity, accessMask, isInherited: false, InheritanceFlags.None, PropagationFlags.None, type);

    /// <summary>
    /// Holds the thread-pool threads that call <see cref="Block"/> until <see cref="Release"/>.
    /// It is never disposed, so a work item that starts after the release, once the test has
    /// ended, returns at once.
    /// </summary>
    private sealed class PoolGate
    {
        private readonly object sync = new();
        private bool released;

        public void Block()
        {
            lock (this.sync)
            {
                while (!this.released)
                {
                    Monitor.Wait(this.sync);
                }
            }
        }

        public void Release()
        {
            lock (this.sync)
            {
                this.released = true;
                Monitor.PulseAll(this.sync);
            }
        }
    }
}
