namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Runs a crash child with separate startup and mutation deadlines and common diagnostics.</summary>
internal static class CrashRecoveryProcess
{
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan MutationTimeout = TimeSpan.FromSeconds(60);

    public static void MarkReady(string path) => File.WriteAllText(path + ".ready", "ready");

    public static void Terminate(string path, string phase)
    {
        File.WriteAllText(path + ".crashed", phase);
        using var process = Process.GetCurrentProcess();
        process.Kill();
        process.WaitForExit(); // Kill is asynchronous; never resume mutation, disposal or rollback.
    }

    public static void AssertChild(
        string method,
        string path,
        string expectedPhase,
        string scenario,
        IReadOnlyDictionary<string, string> environment,
        string password = "")
    {
        string readinessFile = path + ".ready";
        File.Delete(readinessFile);
        File.Delete(path + ".crashed");
        PowerShellRunResult result = PowerShellProcessRunner.Run(
            "dotnet",
            [typeof(CrashRecoveryProcess).Assembly.Location, "-method", method],
            MutationTimeout,
            workingDirectory: Path.GetDirectoryName(path),
            environmentOverrides: environment,
            readinessFile: readinessFile,
            readinessTimeout: StartupTimeout);
        bool ready = File.Exists(readinessFile);
        string phase = File.Exists(path + ".crashed") ? File.ReadAllText(path + ".crashed") : "[not reached]";
        string diagnostic = Describe(scenario, path, password, ready, phase, result);
        Assert.False(result.TimedOut, diagnostic);
        Assert.True(result.ProcessExited, diagnostic);
        Assert.True(result.OutputComplete, diagnostic);
        if (result.ExitCode == 0)
        {
            Assert.Fail($"Child returned normally. {diagnostic}");
        }

        Assert.True(ready, $"Child did not finish startup. {diagnostic}");
        if (!string.Equals(expectedPhase, phase, StringComparison.Ordinal))
        {
            Assert.Fail($"Expected crash phase '{expectedPhase}'. {diagnostic}");
        }
    }

    public static string Describe(string scenario, string path, string password, bool ready, string phase, PowerShellRunResult result)
    {
        string diagnostic = $"Crash child ({scenario}): ready={ready}, phase={phase}, exit={result.ExitCode}, timed out={result.TimedOut}, terminated={result.ProcessExited}, output complete={result.OutputComplete}.{Environment.NewLine}Standard output:{Environment.NewLine}{result.StandardOutput}{Environment.NewLine}Standard error:{Environment.NewLine}{result.StandardError}";
        diagnostic = diagnostic.Replace(path, "[database]", StringComparison.Ordinal);
        if (password.Length > 0)
        {
            diagnostic = diagnostic.Replace(password, "[password]", StringComparison.Ordinal);
        }

        return diagnostic;
    }
}
