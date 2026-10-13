namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Runtime.InteropServices;
using JetDatabaseWriter.TestSupport;

/// <summary>Runs Jet3 native checks through a bounded x86 DAO3.6 PowerShell host.</summary>
internal static class Jet3RoundTripEnvironment
{
    private static readonly Lazy<bool> Availability = new(Probe);

    /// <summary>Gets a value indicating whether an x86 DAO3.6 host can be activated.</summary>
    public static bool IsAvailable => Availability.Value;

    /// <summary>Runs a script with the compatible DAO engine and an isolated process working directory.</summary>
    /// <param name="script">The body using the engine variable.</param>
    /// <param name="workDir">An absolute temporary directory.</param>
    /// <param name="timeout">The maximum process runtime.</param>
    /// <returns>The bounded process result.</returns>
    /// <exception cref="ArgumentException">The working directory is relative.</exception>
    public static PowerShellRunResult RunEngineScript(string script, string workDir, TimeSpan timeout)
    {
        if (!Path.IsPathFullyQualified(workDir))
        {
            throw new ArgumentException("The DAO working directory must be absolute.", nameof(workDir));
        }

        string directory = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(workDir);
        string body = $$"""
            $ErrorActionPreference = 'Stop'
            $previousProcessDirectory = [Environment]::CurrentDirectory
            Push-Location -LiteralPath {{directory}}
            try {
                [Environment]::CurrentDirectory = {{directory}}
                $engine = New-Object -ComObject DAO.DBEngine.36
                try {
                    {{script}}
                } finally {
                    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null
                    [GC]::Collect()
                    [GC]::WaitForPendingFinalizers()
                }
            } finally {
                [Environment]::CurrentDirectory = $previousProcessDirectory
                Pop-Location
            }
            """;
        string file = Path.Combine(workDir, $"dao36-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(file, body);
        try
        {
            return PowerShellProcessRunner.Run(HostPath, ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", file], timeout, workingDirectory: workDir);
        }
        finally
        {
            File.Delete(file);
        }
    }

    private static string HostPath => Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Windows), Environment.Is64BitOperatingSystem ? "SysWOW64" : "System32", "WindowsPowerShell", "v1.0", "powershell.exe");

    private static bool Probe()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows) || !File.Exists(HostPath))
        {
            return false;
        }

        string directory = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), $"jet-dao36-probe-{Guid.NewGuid():N}")).FullName;
        try
        {
            PowerShellRunResult result = RunEngineScript("Write-Output 'DAO36_READY'", directory, TimeSpan.FromMinutes(1));
            return result.ExitCode == 0 && !result.TimedOut && result.OutputComplete;
        }
        finally
        {
            Directory.Delete(directory);
        }
    }
}
