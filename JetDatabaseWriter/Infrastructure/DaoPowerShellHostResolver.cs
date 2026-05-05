namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

internal static class DaoPowerShellHostResolver
{
    private const string PowerShellRelativePath = @"WindowsPowerShell\v1.0\powershell.exe";
    private const string DaoProbeScript = "$ErrorActionPreference = 'Stop'; $engine = New-Object -ComObject DAO.DBEngine.120; try { exit 0 } finally { if ($null -ne $engine) { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }; [GC]::Collect(); [GC]::WaitForPendingFinalizers() }";

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
        var psi = new ProcessStartInfo(powershellPath)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        psi.ArgumentList.Add("-NoProfile");
        psi.ArgumentList.Add("-ExecutionPolicy");
        psi.ArgumentList.Add("Bypass");
        psi.ArgumentList.Add("-Command");
        psi.ArgumentList.Add(DaoProbeScript);

        try
        {
            using var process = Process.Start(psi);
            if (process is null)
            {
                failure = $"Failed to start PowerShell host '{powershellPath}'.";
                return false;
            }

            string stdout = process.StandardOutput.ReadToEnd();
            string stderr = process.StandardError.ReadToEnd();
            if (!process.WaitForExit(10000))
            {
                TryKill(process);
                failure = $"Timed out while probing DAO with '{powershellPath}'.";
                return false;
            }

            if (process.ExitCode == 0)
            {
                failure = null;
                return true;
            }

            string detail = string.IsNullOrWhiteSpace(stderr) ? stdout : stderr;
            failure = $"DAO.DBEngine.120 activation failed via '{powershellPath}': {detail.Trim()}";
            return false;
        }
        catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            failure = $"DAO probe launch failed via '{powershellPath}': {ex.Message}";
            return false;
        }
    }

    private static string BuildPowerShellPath(string windowsDirectory, string systemDirectoryName)
    {
        return Path.Combine(windowsDirectory, systemDirectoryName, PowerShellRelativePath);
    }

    private static void TryKill(Process process)
    {
        try
        {
#if NETSTANDARD2_1
            process.Kill();
#else
            process.Kill(entireProcessTree: true);
#endif
        }
        catch (InvalidOperationException)
        {
            // Process already exited.
        }
        catch (NotSupportedException)
        {
            // Not supported on this host.
        }
    }

    internal sealed record DaoPowerShellHostProbeResult(string? HostPath, string? FailureReason);
}
