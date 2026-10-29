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

    public static void MarkReady(string path) => File.WriteAllText(path + ".ready", RuntimeProductVersion(typeof(object).Assembly.Location));

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
        string parentRuntime = RuntimeProductVersion(typeof(object).Assembly.Location);
        string readinessFile = path + ".ready";
        File.Delete(readinessFile);
        File.Delete(path + ".crashed");
        PowerShellRunResult result = PowerShellProcessRunner.Run(
            "dotnet",
            ChildArguments(method, typeof(object).Assembly.Location),
            MutationTimeout,
            workingDirectory: Path.GetDirectoryName(path),
            environmentOverrides: environment,
            readinessFile: readinessFile,
            readinessTimeout: StartupTimeout);
        bool ready = File.Exists(readinessFile);
        string phase = File.Exists(path + ".crashed") ? File.ReadAllText(path + ".crashed") : "[not reached]";
        string childRuntime = ready ? File.ReadAllText(readinessFile) : "[not reported]";
        string diagnostic = Describe(scenario, path, password, ready, phase, result, parentRuntime, childRuntime);
        Assert.False(result.TimedOut, diagnostic);
        Assert.True(result.ProcessExited, diagnostic);
        Assert.True(result.OutputComplete, diagnostic);
        if (result.ExitCode == 0)
        {
            Assert.Fail($"Child returned normally. {diagnostic}");
        }

        // Process.Kill uses TerminateProcess(..., -1) on Windows and SIGKILL
        // on Unix. The Unix child wait reports 128 + signal number (9).
        int expectedExitCode = OperatingSystem.IsWindows() ? -1 : 137;
        Assert.True(result.ExitCode == expectedExitCode, $"Child did not exit by forced termination (expected {expectedExitCode}). {diagnostic}");

        Assert.True(ready, $"Child did not finish startup. {diagnostic}");
        AssertMatchingRuntime(parentRuntime, childRuntime, diagnostic);
        if (!string.Equals(expectedPhase, phase, StringComparison.Ordinal))
        {
            Assert.Fail($"Expected crash phase '{expectedPhase}'. {diagnostic}");
        }
    }

    public static string Describe(string scenario, string path, string password, bool ready, string phase, PowerShellRunResult result, string parentRuntime, string childRuntime)
    {
        string diagnostic = $"Crash child ({scenario}): ready={ready}, phase={phase}, exit={result.ExitCode}, timed out={result.TimedOut}, terminated={result.ProcessExited}, output complete={result.OutputComplete}, parent runtime={parentRuntime}, child runtime={childRuntime}.{Environment.NewLine}Standard output:{Environment.NewLine}{result.StandardOutput}{Environment.NewLine}Standard error:{Environment.NewLine}{result.StandardError}";
        diagnostic = diagnostic.Replace(path, "[database]", StringComparison.Ordinal);
        if (password.Length > 0)
        {
            diagnostic = diagnostic.Replace(password, "[password]", StringComparison.Ordinal);
        }

        return diagnostic;
    }

    internal static string[] ChildArguments(string method, string coreLibraryPath)
        => ["exec", "--fx-version", RuntimeProductVersion(coreLibraryPath), "--roll-forward", "Disable", typeof(CrashRecoveryProcess).Assembly.Location, "--jdw-crash-child", method];

    internal static void AssertMatchingRuntime(string parentRuntime, string childRuntime, string diagnostic)
        => Assert.True(string.Equals(parentRuntime, childRuntime, StringComparison.Ordinal), $"Child runtime differs from the parent runtime. {diagnostic}");

    private static string RuntimeProductVersion(string coreLibraryPath)
    {
        string? versionDirectory = Path.GetDirectoryName(coreLibraryPath);
        if (versionDirectory is null || Path.GetFileName(Path.GetDirectoryName(versionDirectory)) != "Microsoft.NETCore.App")
        {
            throw new InvalidOperationException("Crash evidence requires a loaded CoreLib from an exact shared-framework product directory.");
        }

        // Informational metadata can carry a servicing suffix absent from the
        // installed framework version. The loaded framework directory preserves
        // the host's actual release, preview or RC product name without guessing.
        return Path.GetFileName(versionDirectory);
    }
}
