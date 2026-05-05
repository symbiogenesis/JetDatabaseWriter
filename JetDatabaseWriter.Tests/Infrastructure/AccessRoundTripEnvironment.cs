namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Probes for a Microsoft Access install (MSACCESS.EXE plus a bitness-matched
/// Windows PowerShell host that can load <c>DAO.DBEngine.120</c>) and exposes
/// the result so tests can opt in via xUnit's runtime skip mechanism.
/// </summary>
/// <remarks>
/// <para>
/// Driving Compact &amp; Repair through DAO is preferred over
/// <c>MSACCESS.EXE /compact</c> because the Office launcher detaches its own
/// process when an Access instance is already running and the "compacted"
/// file never appears for the test to validate.
/// </para>
/// <para>
/// The DAO COM class is bitness-sensitive: it is registered as x86, x64, or
/// ARM64 depending on the Office install. A .NET test process can't activate
/// it in-proc when the bitness doesn't match. To stay portable we shell out
/// to a <c>powershell.exe</c> host whose bitness matches the DAO registration
/// (resolved by <see cref="DaoPowerShellHostResolver"/>) and run a small
/// script that constructs the COM object and calls
/// <c>CompactDatabase(src, dst)</c>.
/// </para>
/// <para>
/// All probes are no-ops on non-Windows platforms; <see cref="SkipReason"/>
/// returns a stable string and tests are skipped, not run.
/// </para>
/// </remarks>
internal static class AccessRoundTripEnvironment
{
    private static readonly Lazy<ProbeResult> ProbeOnce = new(DetectCore);

    /// <summary>Gets the path to <c>MSACCESS.EXE</c>, or <c>null</c> when not installed.</summary>
    public static string? MsAccessPath => ProbeOnce.Value.MsAccess;

    /// <summary>Gets the path to a Windows PowerShell host that can activate <c>DAO.DBEngine.120</c>.</summary>
    public static string? PowerShellPath => ProbeOnce.Value.PowerShell;

    /// <summary>Gets the skip reason for tests that need DAO Compact &amp; Repair, or <c>null</c> when the environment is usable.</summary>
    public static string? SkipReason => ProbeOnce.Value.Skip;

    /// <summary>Gets a value indicating whether the round-trip environment is fully available.</summary>
    public static bool IsAvailable => ProbeOnce.Value.Skip is null;

    /// <summary>
    /// Runs <c>DAO.DBEngine.120.CompactDatabase(source, dest)</c> through a
    /// bitness-matched <c>powershell.exe</c>. The source file MUST be closed
    /// (no open <see cref="JetDatabaseWriter.AccessWriter"/> /
    /// <see cref="JetDatabaseWriter.AccessReader"/>) before calling.
    /// </summary>
    /// <param name="source">Existing database file to compact.</param>
    /// <param name="dest">Output path; will be overwritten if it exists.</param>
    /// <param name="timeout">Maximum wait for the PowerShell host to exit.</param>
    /// <returns>Tuple of (process exit code, captured stdout, captured stderr).</returns>
    public static CompactResult RunDaoCompact(string source, string dest, TimeSpan timeout)
    {
        if (!IsAvailable)
        {
            throw new InvalidOperationException(SkipReason);
        }

        string scriptPath = Path.Combine(Path.GetDirectoryName(dest)!, $"compact-{Guid.NewGuid():N}.ps1");
        string srcLiteral = source.Replace("'", "''", StringComparison.Ordinal);
        string dstLiteral = dest.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$src = '" + srcLiteral + "'\n" +
            "$dst = '" + dstLiteral + "'\n" +
            "if (Test-Path $dst) { Remove-Item -LiteralPath $dst -Force }\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            "  $engine.CompactDatabase($src, $dst)\n" +
            "  exit 0\n" +
            "} finally {\n" +
            "  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null\n" +
            "  [GC]::Collect(); [GC]::WaitForPendingFinalizers()\n" +
            "}\n";
        File.WriteAllText(scriptPath, script);

        try
        {
            var psi = new ProcessStartInfo(PowerShellPath!)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            psi.ArgumentList.Add("-NoProfile");
            psi.ArgumentList.Add("-ExecutionPolicy");
            psi.ArgumentList.Add("Bypass");
            psi.ArgumentList.Add("-File");
            psi.ArgumentList.Add(scriptPath);

            using var p = Process.Start(psi)!;
            string stdout = p.StandardOutput.ReadToEnd();
            string stderr = p.StandardError.ReadToEnd();
            if (!p.WaitForExit((int)timeout.TotalMilliseconds))
            {
                TryKill(p);
                return new CompactResult(-1, stdout, stderr + $"\n[timeout after {timeout.TotalSeconds}s]");
            }

            return new CompactResult(p.ExitCode, stdout, stderr);
        }
        finally
        {
            TryDelete(scriptPath);
        }
    }

    /// <summary>
    /// Runs an arbitrary PowerShell script through the bitness-matched host
    /// that can activate <c>DAO.DBEngine.120</c>. The script receives no
    /// special variables — callers embed paths/values via string literals.
    /// </summary>
    /// <param name="script">PowerShell script body to execute.</param>
    /// <param name="workDir">Directory for the temp .ps1 file.</param>
    /// <param name="timeout">Maximum wait for the PowerShell host to exit.</param>
    /// <returns>Process exit code, captured stdout, captured stderr.</returns>
    public static CompactResult RunDaoScript(string script, string workDir, TimeSpan timeout)
    {
        if (!IsAvailable)
        {
            throw new InvalidOperationException(SkipReason);
        }

        string scriptPath = Path.Combine(workDir, $"dao-script-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(scriptPath, script);

        try
        {
            var psi = new ProcessStartInfo(PowerShellPath!)
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            psi.ArgumentList.Add("-NoProfile");
            psi.ArgumentList.Add("-ExecutionPolicy");
            psi.ArgumentList.Add("Bypass");
            psi.ArgumentList.Add("-File");
            psi.ArgumentList.Add(scriptPath);

            using var p = Process.Start(psi)!;
            string stdout = p.StandardOutput.ReadToEnd();
            string stderr = p.StandardError.ReadToEnd();
            if (!p.WaitForExit((int)timeout.TotalMilliseconds))
            {
                TryKill(p);
                return new CompactResult(-1, stdout, stderr + $"\n[timeout after {timeout.TotalSeconds}s]");
            }

            return new CompactResult(p.ExitCode, stdout, stderr);
        }
        finally
        {
            TryDelete(scriptPath);
        }
    }

    private static ProbeResult DetectCore()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return new ProbeResult(null, null, "Round-trip via Microsoft Access requires Windows.");
        }

        string[] msaccessCandidates =
        [
            @"C:\Program Files\Microsoft Office\root\Office16\MSACCESS.EXE",
            @"C:\Program Files (x86)\Microsoft Office\root\Office16\MSACCESS.EXE",
            @"C:\Program Files\Microsoft Office\Office16\MSACCESS.EXE",
            @"C:\Program Files (x86)\Microsoft Office\Office16\MSACCESS.EXE",
        ];
        string? msaccess = msaccessCandidates.FirstOrDefault(File.Exists);
        if (msaccess is null)
        {
            return new ProbeResult(null, null, "MSACCESS.EXE not found in any standard install location.");
        }

        DaoPowerShellHostResolver.DaoPowerShellHostProbeResult hostProbe = DaoPowerShellHostResolver.Probe(msaccess);
        return new ProbeResult(msaccess, hostProbe.HostPath, hostProbe.FailureReason);
    }

    private static void TryKill(Process p)
    {
        try
        {
            p.Kill(entireProcessTree: true);
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

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
            // Another process holds the file — leave it for OS cleanup.
        }
        catch (UnauthorizedAccessException)
        {
            // Permission issue — leave it for OS cleanup.
        }
    }

    private sealed record ProbeResult(string? MsAccess, string? PowerShell, string? Skip);

    /// <summary>Result of a single Compact &amp; Repair invocation.</summary>
    /// <param name="ExitCode">Process exit code (0 on success).</param>
    /// <param name="StdOut">Captured standard output.</param>
    /// <param name="StdErr">Captured standard error.</param>
    internal sealed record CompactResult(int ExitCode, string StdOut, string StdErr);
}
