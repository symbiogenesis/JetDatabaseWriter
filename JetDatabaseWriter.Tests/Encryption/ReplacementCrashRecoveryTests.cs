namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks replacement boundaries after process termination without unwinding cleanup.</summary>
public sealed class ReplacementCrashRecoveryTests
{
    private const string ChildPath = "JDW_REPLACEMENT_CRASH_PATH";
    private const string ChildBoundary = "JDW_REPLACEMENT_CRASH_BOUNDARY";
    private const string Method = "JetDatabaseWriter.Tests.Encryption.ReplacementCrashRecoveryTests.Commit_ProcessKilled_RetainsCompleteImages";

    /// <summary>Process interruption retains the complete destination and original backup.</summary>
    [Fact]
    public async Task Commit_ProcessKilled_RetainsCompleteImages()
    {
        if (Environment.GetEnvironmentVariable(ChildPath) is { } childPath)
        {
            string target = Environment.GetEnvironmentVariable(ChildBoundary)!;
            EncryptionReplacementCommit.Commit(childPath + ".reenc-owned.tmp", childPath, boundary =>
            {
                if (boundary == target)
                {
                    File.WriteAllText(childPath + ".crashed", boundary);
                    using var process = Process.GetCurrentProcess();
                    process.Kill();
                }
            });
            Assert.Fail("The requested replacement boundary was never interrupted.");
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementCrashRecoveryTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            string baseline = Path.Combine(directory, "baseline.accdb");
            string replacement = Path.Combine(directory, "replacement.accdb");
            await CreateImageAsync(baseline, "Original");
            await CreateImageAsync(replacement, "Replacement");
            byte[] oldBytes = await File.ReadAllBytesAsync(baseline, TestContext.Current.CancellationToken);
            byte[] newBytes = await File.ReadAllBytesAsync(replacement, TestContext.Current.CancellationToken);
            Assert.NotEqual(oldBytes, newBytes);
            foreach (string boundary in new[] { "prepared", "renamed", "committed" })
            {
                string path = Path.Combine(directory, boundary + ".accdb");
                string temporary = path + ".reenc-owned.tmp";
                File.Copy(baseline, path);
                File.Copy(replacement, temporary);
                await using (var staged = new FileStream(temporary, FileMode.Open, FileAccess.ReadWrite))
                {
#pragma warning disable CA1849 // No asynchronous Flush(flushToDisk: true) API exists.
                    staged.Flush(flushToDisk: true);
#pragma warning restore CA1849
                }

                AssertCrashChild(path, boundary);
                Assert.Equal(oldBytes, await File.ReadAllBytesAsync(temporary + ".original", TestContext.Current.CancellationToken));
                bool prepared = boundary == "prepared";
                Assert.Equal(prepared ? oldBytes : newBytes, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                Assert.Equal(prepared, File.Exists(temporary));
                if (prepared)
                {
                    Assert.Equal(newBytes, await File.ReadAllBytesAsync(temporary, TestContext.Current.CancellationToken));
                }

                await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
                Assert.Equal(prepared ? "Original" : "Replacement", Assert.Single(await reader.ListTablesAsync(TestContext.Current.CancellationToken)));
                await using AccessReader originalReader = await AccessReader.OpenAsync(temporary + ".original", new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
                Assert.Equal("Original", Assert.Single(await originalReader.ListTablesAsync(TestContext.Current.CancellationToken)));
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task CreateImageAsync(string path, string table)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.AceAccdb, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(table, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(table, [1], TestContext.Current.CancellationToken);
    }

    private static void AssertCrashChild(string path, string boundary)
    {
        static string Quote(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
        string assembly = typeof(ReplacementCrashRecoveryTests).Assembly.Location;
        string script = $"$env:{ChildPath}={Quote(path)}; $env:{ChildBoundary}={Quote(boundary)}; & dotnet {Quote(assembly)} -method {Quote(Method)}; exit $LASTEXITCODE";
        PowerShellRunResult result = PowerShellProcessRunner.Run("pwsh", ["-NoProfile", "-NonInteractive", "-EncodedCommand", Convert.ToBase64String(Encoding.Unicode.GetBytes(script))], TimeSpan.FromSeconds(60));
        Assert.False(result.TimedOut, result.StandardError);
        Assert.True(result.OutputComplete, result.StandardError);
        Assert.NotEqual(0, result.ExitCode);
        Assert.True(File.Exists(path + ".crashed"), $"Child did not reach replacement boundary: {result.StandardOutput} {result.StandardError}");
        Assert.Equal(boundary, File.ReadAllText(path + ".crashed"));
    }
}
