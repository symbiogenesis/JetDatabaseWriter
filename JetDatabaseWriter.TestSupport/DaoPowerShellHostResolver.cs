namespace JetDatabaseWriter.TestSupport;

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

internal static class DaoPowerShellHostResolver
{
    private const string PowerShellRelativePath = @"WindowsPowerShell\v1.0\powershell.exe";
    private const string DaoProbeScript = "$ErrorActionPreference = 'Stop'; $engine = New-Object -ComObject DAO.DBEngine.120; try { exit 0 } finally { if ($null -ne $engine) { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }; [GC]::Collect(); [GC]::WaitForPendingFinalizers() }";

    /// <summary>
    /// How long one host may take to activate DAO. Starting Windows PowerShell took 12 s on a
    /// busy test machine, so the probe allows a minute before it gives up on a host.
    /// </summary>
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromMinutes(1);

    public static DaoPowerShellHostProbeResult Probe(string? msAccessPath = null)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return new DaoPowerShellHostProbeResult(null, "Windows PowerShell host detection requires Windows.");
        }

        string windowsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.Windows);
        bool preferWow64Host = msAccessPath is not null
            ? msAccessPath.Contains("Program Files (x86)", StringComparison.OrdinalIgnoreCase)
            : RuntimeInformation.OSArchitecture == Architecture.X64;

        string? firstFailure = null;
        bool sawCandidate = false;
        foreach (string hostPath in GetCandidateHostPaths(windowsDirectory, preferWow64Host))
        {
            if (!File.Exists(hostPath))
            {
                continue;
            }

            sawCandidate = true;
            if (TryProbeDaoHost(hostPath, out string? failure))
            {
                return new DaoPowerShellHostProbeResult(hostPath, null);
            }

            firstFailure ??= failure;
        }

        if (!sawCandidate)
        {
            return new DaoPowerShellHostProbeResult(null, "Windows PowerShell host not found at any expected location.");
        }

        return new DaoPowerShellHostProbeResult(
            null,
            firstFailure ?? "DAO.DBEngine.120 could not be activated from a compatible Windows PowerShell host.");
    }

    private static IReadOnlyList<string> GetCandidateHostPaths(string windowsDirectory, bool preferWow64Host)
        => GetCandidateHostPaths(windowsDirectory, Environment.Is64BitOperatingSystem, Environment.Is64BitProcess, preferWow64Host);

    internal static IReadOnlyList<string> GetCandidateHostPaths(
        string windowsDirectory,
        bool is64BitOperatingSystem,
        bool is64BitProcess,
        bool preferWow64Host)
    {
        if (string.IsNullOrWhiteSpace(windowsDirectory))
        {
            throw new ArgumentException("Windows directory is required.", nameof(windowsDirectory));
        }

        string nativeHostPath = is64BitOperatingSystem && !is64BitProcess
            ? BuildPowerShellPath(windowsDirectory, "Sysnative")
            : BuildPowerShellPath(windowsDirectory, "System32");
        string? wow64HostPath = is64BitOperatingSystem
            ? BuildPowerShellPath(windowsDirectory, "SysWOW64")
            : null;

        var candidates = new List<string>(capacity: 2);
        if (preferWow64Host)
        {
            if (wow64HostPath is not null)
            {
                candidates.Add(wow64HostPath);
            }

            candidates.Add(nativeHostPath);
        }
        else
        {
            candidates.Add(nativeHostPath);
            if (wow64HostPath is not null)
            {
                candidates.Add(wow64HostPath);
            }
        }

        return candidates;
    }

    private static bool TryProbeDaoHost(string powershellPath, out string? failure)
    {
        try
        {
            PowerShellRunResult run = PowerShellProcessRunner.Run(
                powershellPath,
                ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", DaoProbeScript],
                ProbeTimeout);
            if (run.TimedOut)
            {
                failure = $"Timed out while probing DAO with '{powershellPath}'.";
                return false;
            }

            if (run.ExitCode == 0)
            {
                failure = null;
                return true;
            }

            string detail = string.IsNullOrWhiteSpace(run.StandardError) ? run.StandardOutput : run.StandardError;
            failure = $"DAO.DBEngine.120 activation failed via '{powershellPath}': {detail.Trim()}";
            return false;
        }
        catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            failure = $"DAO probe launch failed via '{powershellPath}': {ex.Message}";
            return false;
        }
    }

    private static string BuildPowerShellPath(string windowsDirectory, string systemDirectoryName) => windowsDirectory.Replace('/', '\\').TrimEnd('\\') + "\\" + systemDirectoryName + "\\" + PowerShellRelativePath;

    internal sealed record DaoPowerShellHostProbeResult(string? HostPath, string? FailureReason);
}
