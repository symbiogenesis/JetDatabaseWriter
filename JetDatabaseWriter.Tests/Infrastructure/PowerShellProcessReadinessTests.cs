namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks startup and operation deadlines independently with a real child runner.</summary>
public sealed class PowerShellProcessReadinessTests
{
    private const string ChildReadyPath = "JDW_RUNNER_READINESS_PATH";

    [Fact]
    public async Task Run_ReadinessSignal_StartsOperationDeadline()
    {
        if (Environment.GetEnvironmentVariable(ChildReadyPath) is { } childPath)
        {
            await Task.Delay(TimeSpan.FromSeconds(8), TestContext.Current.CancellationToken);
            await File.WriteAllTextAsync(childPath, "ready", TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
            await Console.Out.WriteLineAsync("operation complete");
            return;
        }

        WithReadyPath(path =>
        {
            PowerShellRunResult result = RunChild(nameof(this.Run_ReadinessSignal_StartsOperationDeadline), path, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(60));
            Assert.False(result.TimedOut, result.StandardOutput + result.StandardError);
            Assert.True(result.ProcessExited);
            Assert.True(result.OutputComplete);
            Assert.Equal(0, result.ExitCode);
            Assert.Contains("operation complete", result.StandardOutput, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Run_ChildNeverReady_StartupDeadlineKillsProcess()
    {
        if (Environment.GetEnvironmentVariable(ChildReadyPath) is not null)
        {
            await Task.Delay(TimeSpan.FromMinutes(2), TestContext.Current.CancellationToken);
            return;
        }

        WithReadyPath(path =>
        {
            PowerShellRunResult result = RunChild(nameof(this.Run_ChildNeverReady_StartupDeadlineKillsProcess), path, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(1));
            Assert.True(result.TimedOut);
            Assert.True(result.ProcessExited);
            Assert.False(File.Exists(path));
        });
    }

    [Fact]
    public async Task Run_ReadyChildHangs_OperationDeadlineKillsProcess()
    {
        if (Environment.GetEnvironmentVariable(ChildReadyPath) is { } childPath)
        {
            await File.WriteAllTextAsync(childPath, "ready", TestContext.Current.CancellationToken);
            await Console.Out.WriteLineAsync("started operation");
            await Console.Error.WriteLineAsync("operation stalled");
            await Task.Delay(TimeSpan.FromMinutes(2), TestContext.Current.CancellationToken);
            return;
        }

        WithReadyPath(path =>
        {
            PowerShellRunResult result = RunChild(nameof(this.Run_ReadyChildHangs_OperationDeadlineKillsProcess), path, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60));
            Assert.True(result.TimedOut);
            Assert.True(result.ProcessExited);
            Assert.True(result.OutputComplete);
            Assert.True(File.Exists(path));
            Assert.Contains("started operation", result.StandardOutput, StringComparison.Ordinal);
            Assert.Contains("operation stalled", result.StandardError, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Run_ChildExitsBeforeReady_PreservesExitAndOutput()
    {
        if (Environment.GetEnvironmentVariable(ChildReadyPath) is not null)
        {
            await Console.Out.WriteLineAsync("exited before readiness");
            return;
        }

        WithReadyPath(path =>
        {
            PowerShellRunResult result = RunChild(nameof(this.Run_ChildExitsBeforeReady_PreservesExitAndOutput), path, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60));
            Assert.False(result.TimedOut);
            Assert.True(result.ProcessExited);
            Assert.True(result.OutputComplete);
            Assert.Equal(0, result.ExitCode);
            Assert.False(File.Exists(path));
            Assert.Contains("exited before readiness", result.StandardOutput, StringComparison.Ordinal);
        });
    }

    private static PowerShellRunResult RunChild(string method, string readyPath, TimeSpan timeout, TimeSpan readinessTimeout) =>
        PowerShellProcessRunner.Run(
            "dotnet",
            [typeof(PowerShellProcessReadinessTests).Assembly.Location, "-method", $"{typeof(PowerShellProcessReadinessTests).FullName}.{method}"],
            timeout,
            workingDirectory: Path.GetDirectoryName(readyPath),
            environmentOverrides: new Dictionary<string, string> { [ChildReadyPath] = readyPath },
            readinessFile: readyPath,
            readinessTimeout: readinessTimeout);

    private static void WithReadyPath(Action<string> action)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"ProcessReadinessTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            action(Path.Combine(directory, "ready"));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
