namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks creation publication after termination without cleanup unwinding.</summary>
public sealed class CreationCrashRecoveryTests
{
    private const string ChildPath = "JDW_CREATION_CRASH_PATH";
    private const string ChildBoundary = "JDW_CREATION_CRASH_BOUNDARY";
    private const string Method = "JetDatabaseWriter.Tests.Writer.CreationCrashRecoveryTests.Publish_ProcessKilled_PreservesCompleteDestination";

    /// <summary>Interrupted initialization and publication expose only complete destination images.</summary>
    [Fact]
    public async Task Publish_ProcessKilled_PreservesCompleteDestination()
    {
        if (Environment.GetEnvironmentVariable(ChildPath) is { } childPath)
        {
            string target = Environment.GetEnvironmentVariable(ChildBoundary)!;
            void Observe(string boundary)
            {
                if (boundary != target)
                {
                    return;
                }

                File.WriteAllText(childPath + ".crashed", boundary);
                using var process = Process.GetCurrentProcess();
                process.Kill();
                process.WaitForExit(); // Kill is asynchronous; never resume publication or cleanup.
            }

            byte[] image = await File.ReadAllBytesAsync(childPath + ".image", TestContext.Current.CancellationToken);
            async ValueTask Initialize(FileStream staging, CancellationToken token)
            {
                if (target != "Partial")
                {
                    string password = Path.GetFileName(childPath).StartsWith("True", StringComparison.Ordinal) ? "Native123" : string.Empty;
                    await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(staging, DatabaseFormat.AceAccdb, new AccessWriterOptions { Password = password.AsMemory(), UseLockFile = false }, leaveOpen: true, token);
                    await writer.CreateTableAsync("Complete", [new("Id", typeof(int))], token);
                    await writer.InsertRowAsync("Complete", [42], token);
                    return;
                }

                int half = image.Length / 2;
                await staging.WriteAsync(image.AsMemory(0, half), token);
                await staging.FlushAsync(token);
                Observe("Partial");
                await staging.WriteAsync(image.AsMemory(half), token);
            }

            void OnBoundary(string boundary, string temporary)
            {
                if (boundary == "Publishing")
                {
                    File.Copy(temporary, childPath + ".image", overwrite: true);
                }

                Observe(boundary);
            }

            await DatabaseCreationPublication.PublishAsync(childPath, Initialize, OnBoundary, TestContext.Current.CancellationToken);
            Observe("Published");
            Assert.Fail("The requested creation boundary was never interrupted.");
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), $"CreationCrashRecoveryTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            foreach (bool encrypted in new[] { false, true })
            {
                string password = encrypted ? "Native123" : string.Empty;
                string baseline = Path.Combine(directory, encrypted ? "encrypted.accdb" : "plain.accdb");
                await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(baseline, DatabaseFormat.AceAccdb, new AccessWriterOptions { Password = password.AsMemory(), UseLockFile = false }, TestContext.Current.CancellationToken))
                {
                    await writer.CreateTableAsync("Complete", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
                    await writer.InsertRowAsync("Complete", [42], TestContext.Current.CancellationToken);
                }

                byte[] complete = await File.ReadAllBytesAsync(baseline, TestContext.Current.CancellationToken);
                foreach (string boundary in new[] { "Partial", "Publishing", "Published" })
                {
                    foreach (bool competing in new[] { false, true })
                    {
                        if (competing && boundary == "Published")
                        {
                            continue;
                        }

                        string path = Path.Combine(directory, $"{encrypted}-{boundary}-{competing}.accdb");
                        await File.WriteAllBytesAsync(path + ".image", complete, TestContext.Current.CancellationToken);
                        byte[] competitor = [1, 3, 5, 7];
                        if (competing)
                        {
                            await File.WriteAllBytesAsync(path, competitor, TestContext.Current.CancellationToken);
                        }

                        AssertCrashChild(path, boundary);
                        byte[] expected = await File.ReadAllBytesAsync(path + ".image", TestContext.Current.CancellationToken);
                        if (competing)
                        {
                            Assert.Equal(competitor, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                        }
                        else if (boundary == "Published")
                        {
                            Assert.Equal(expected, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { Password = password.AsMemory(), UseLockFile = false }, TestContext.Current.CancellationToken);
                            Assert.Equal("Complete", Assert.Single(await reader.ListTablesAsync(TestContext.Current.CancellationToken)));
                            using DataTable table = await reader.ReadTableAsync("Complete", cancellationToken: TestContext.Current.CancellationToken);
                            Assert.Equal(42, Assert.Single(table.Rows.Cast<DataRow>())["Id"]);
                        }
                        else
                        {
                            Assert.False(File.Exists(path));
                        }

                        string[] staging = Directory.GetFiles(directory, Path.GetFileName(path) + ".create-*.tmp");
                        if (boundary == "Published")
                        {
                            Assert.Empty(staging);
                        }
                        else
                        {
                            byte[] abandoned = await File.ReadAllBytesAsync(Assert.Single(staging), TestContext.Current.CancellationToken);
                            Assert.Equal(boundary == "Partial" ? complete.AsSpan(0, complete.Length / 2).ToArray() : expected, abandoned);
                        }
                    }
                }
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static void AssertCrashChild(string path, string boundary)
    {
        static string Quote(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
        string assembly = typeof(CreationCrashRecoveryTests).Assembly.Location;
        string script = $"$env:{ChildPath}={Quote(path)}; $env:{ChildBoundary}={Quote(boundary)}; & dotnet {Quote(assembly)} -method {Quote(Method)}; exit $LASTEXITCODE";
        PowerShellRunResult result = PowerShellProcessRunner.Run("pwsh", ["-NoProfile", "-NonInteractive", "-EncodedCommand", Convert.ToBase64String(Encoding.Unicode.GetBytes(script))], TimeSpan.FromSeconds(60));
        Assert.False(result.TimedOut, result.StandardError);
        Assert.True(result.OutputComplete, result.StandardError);
        Assert.NotEqual(0, result.ExitCode);
        Assert.True(File.Exists(path + ".crashed"), $"Child did not reach creation boundary: {result.StandardOutput} {result.StandardError}");
        Assert.Equal(boundary, File.ReadAllText(path + ".crashed"));
    }
}
