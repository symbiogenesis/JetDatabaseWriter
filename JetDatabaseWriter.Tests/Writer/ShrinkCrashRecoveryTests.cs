namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks shrink recovery after process termination without disposal.</summary>
public sealed class ShrinkCrashRecoveryTests
{
    private const string ChildPath = "JDW_SHRINK_CRASH_PATH";
    private const string ChildBoundary = "JDW_SHRINK_CRASH_BOUNDARY";
    private const string ChildPassword = "JDW_SHRINK_CRASH_PASSWORD";
    private const string Method = "JetDatabaseWriter.Tests.Writer.ShrinkCrashRecoveryTests.InterruptedShrink_RecoversExactOriginalOrCommittedImage";

    /// <summary>Secure tail writes and truncation remain recoverable until shrink commits.</summary>
    [Fact]
    public async Task InterruptedShrink_RecoversExactOriginalOrCommittedImage()
    {
        if (Environment.GetEnvironmentVariable(ChildPath) is { } childPath)
        {
            await CrashAsync(childPath);
            Assert.Fail("The requested shrink interruption was never reached.");
            return;
        }

        await CheckAsync(encrypted: false);
        await CheckAsync(encrypted: true);
    }

    private static async Task CheckAsync(bool encrypted)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"ShrinkCrashRecoveryTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string baselinePath = Path.Combine(directory, "baseline.accdb");
        string password = encrypted ? "Native123" : string.Empty;
        try
        {
            if (encrypted)
            {
                File.Copy(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), baselinePath);
            }
            else
            {
                await using AccessWriter created = await AccessWriter.CreateDatabaseAsync(baselinePath, DatabaseFormat.AceAccdb, cancellationToken: TestContext.Current.CancellationToken);
            }

            await using (AccessWriter writer = await AccessWriter.OpenAsync(baselinePath, new AccessWriterOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("Retained", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Retained", [1], TestContext.Current.CancellationToken);
                await writer.CreateTableAsync("Tail", [new("Payload", typeof(byte[]))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Tail", [new byte[32_000]], TestContext.Current.CancellationToken);
                await writer.DropTableAsync("Tail", TestContext.Current.CancellationToken);
            }

            byte[] baseline = await File.ReadAllBytesAsync(baselinePath, TestContext.Current.CancellationToken);
            string expectedPath = Path.Combine(directory, "expected.accdb");
            File.Copy(baselinePath, expectedPath);
            await using (AccessWriter expected = await AccessWriter.OpenAsync(expectedPath, Options(password), TestContext.Current.CancellationToken))
            {
                Assert.True(await expected.ShrinkDatabaseAsync(TestContext.Current.CancellationToken) > 0);
            }

            byte[] committed = await File.ReadAllBytesAsync(expectedPath, TestContext.Current.CancellationToken);
            Assert.True(committed.Length < baseline.Length);
            foreach (string boundary in new[] { "write", "truncate", "committed" })
            {
                string path = Path.Combine(directory, boundary + ".accdb");
                File.Copy(baselinePath, path);
                AssertCrashChild(path, password, boundary);
                bool successful = boundary == "committed";
                Assert.Equal(!successful, File.Exists(path + ".jdw-journal"));
                if (!successful)
                {
                    await Assert.ThrowsAnyAsync<IOException>(async () =>
                    {
                        await using AccessReader rejected = await AccessReader.OpenAsync(path, new AccessReaderOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken);
                    });
                }

                if (!successful)
                {
                    await using AccessWriter recovered = await AccessWriter.OpenAsync(path, Options(password), TestContext.Current.CancellationToken);
                    Assert.NotNull(recovered);
                }

                Assert.Equal(successful ? committed : baseline, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                Assert.False(File.Exists(path + ".jdw-journal"));
                await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken);
                using System.Data.DataTable rows = await reader.ReadTableAsync("Retained", cancellationToken: TestContext.Current.CancellationToken);
                Assert.Equal(1, Assert.Single(rows.Rows.Cast<System.Data.DataRow>())["Id"]);
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static AccessWriterOptions Options(string password) => new(password)
    {
        UseLockFile = true,
        UseByteRangeLocks = false,
        SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
    };

    private static async Task CrashAsync(string path)
    {
        string password = Environment.GetEnvironmentVariable(ChildPassword) ?? string.Empty;
        string boundary = Environment.GetEnvironmentVariable(ChildBoundary)!;
        await using var stream = new CrashFileStream(path, boundary);
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, Options(password), leaveOpen: true, TestContext.Current.CancellationToken);
        stream.Armed = true;
        Assert.True(await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken) > 0);
        if (boundary == "committed")
        {
            stream.Crash();
        }
    }

    private static void AssertCrashChild(string path, string password, string boundary)
    {
        static string Quote(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
        string assembly = typeof(ShrinkCrashRecoveryTests).Assembly.Location;
        string script = $"$env:{ChildPath}={Quote(path)}; $env:{ChildPassword}={Quote(password)}; $env:{ChildBoundary}={Quote(boundary)}; & dotnet {Quote(assembly)} -method {Quote(Method)}; exit $LASTEXITCODE";
        PowerShellRunResult result = PowerShellProcessRunner.Run("pwsh", ["-NoProfile", "-NonInteractive", "-EncodedCommand", Convert.ToBase64String(Encoding.Unicode.GetBytes(script))], TimeSpan.FromSeconds(60));
        Assert.False(result.TimedOut, result.StandardError);
        Assert.True(result.OutputComplete, result.StandardError);
        Assert.NotEqual(0, result.ExitCode);
        Assert.True(File.Exists(path + ".crashed"), $"Child did not reach shrink boundary: {result.StandardOutput} {result.StandardError}");
        Assert.Equal(boundary, File.ReadAllText(path + ".crashed"));
    }

    private sealed class CrashFileStream(string path, string boundary)
        : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        public bool Armed { get; set; }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await base.WriteAsync(buffer, cancellationToken);
            if (this.Armed && boundary == "write")
            {
                this.Crash();
            }
        }

        public override void SetLength(long value)
        {
            base.SetLength(value);
            if (this.Armed && boundary == "truncate")
            {
                this.Crash();
            }
        }

        public void Crash()
        {
            this.Flush(flushToDisk: true);
            File.WriteAllText(this.Name + ".crashed", boundary);
            using var process = Process.GetCurrentProcess();
            process.Kill();
            process.WaitForExit(); // Kill is asynchronous; do not run cleanup while termination is pending.
        }
    }
}
