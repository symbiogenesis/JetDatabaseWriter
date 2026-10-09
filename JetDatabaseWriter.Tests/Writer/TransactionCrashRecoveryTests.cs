namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks crash recovery after a process dies without running disposal or rollback.</summary>
public sealed class TransactionCrashRecoveryTests
{
    private const string ChildPath = "JDW_CRASH_TEST_PATH";
    private const string ChildPassword = "JDW_CRASH_TEST_PASSWORD";
    private const string ChildWrite = "JDW_CRASH_TEST_WRITE";
    private const string ChildExplicit = "JDW_CRASH_TEST_EXPLICIT";
    private const string Method = "JetDatabaseWriter.Tests.Writer.TransactionCrashRecoveryTests.InterruptedWrites_RecoverExactOriginalBytes";

    [Fact]
    public async Task InterruptedWrites_RecoverExactOriginalBytes()
    {
        if (Environment.GetEnvironmentVariable(ChildPath) is { } childPath)
        {
            await CrashAsync(childPath);
            Assert.Fail("The requested physical write was never reached.");
            return;
        }

        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            await CheckAsync(format, encrypted: false);
        }

        await CheckAsync(DatabaseFormat.Jet3Mdb, encrypted: true);
        await CheckAsync(DatabaseFormat.Jet4Mdb, encrypted: true);
        await CheckAsync(DatabaseFormat.AceAccdb, encrypted: true);
    }

    [Fact]
    public async Task ExistingReader_PreventsWriterFromStartingPhysicalMutation()
    {
        string path = Path.Combine(Path.GetTempPath(), $"TransactionCrashReader_{Guid.NewGuid():N}.accdb");
        try
        {
            await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.AceAccdb, cancellationToken: TestContext.Current.CancellationToken))
            {
                await created.CreateTableAsync("T", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            }

            byte[] baseline = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                await Assert.ThrowsAnyAsync<IOException>(async () =>
                {
                    await using AccessWriter writer = await AccessWriter.OpenAsync(path, Options(string.Empty), TestContext.Current.CancellationToken);
                });
                Assert.Equal(baseline, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task CheckAsync(DatabaseFormat format, bool encrypted)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"TransactionCrashRecoveryTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string baselinePath = Path.Combine(directory, "baseline.accdb");
        string password = encrypted && format != DatabaseFormat.Jet3Mdb ? "Native123" : string.Empty;
        try
        {
            if (encrypted)
            {
                string fixture = format switch
                {
                    DatabaseFormat.Jet3Mdb => "UpstreamJet3Rc4.mdb",
                    DatabaseFormat.Jet4Mdb => "NativeJet4Rc4.mdb",
                    DatabaseFormat.AceAccdb => "NativeAceAgile.accdb",
                    _ => throw new ArgumentOutOfRangeException(nameof(format)),
                };
                File.Copy(Path.Combine(TestDatabases.EncryptedRoot, fixture), baselinePath);
            }
            else
            {
                await using AccessWriter created = await AccessWriter.CreateDatabaseAsync(baselinePath, format, cancellationToken: TestContext.Current.CancellationToken);
            }

            await using (AccessWriter writer = await AccessWriter.OpenAsync(baselinePath, Options(password), TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("CrashRows", [new("Id", typeof(int)), new("Payload", typeof(string), maxLength: 200)], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("CrashRows", [1, "original"], TestContext.Current.CancellationToken);
            }

            byte[] baseline = await File.ReadAllBytesAsync(baselinePath, TestContext.Current.CancellationToken);
            foreach (bool explicitTransaction in new[] { false, true })
            {
                string expectedPath = Path.Combine(directory, $"expected-{explicitTransaction}.accdb");
                File.Copy(baselinePath, expectedPath);
                await using (AccessWriter expected = await AccessWriter.OpenAsync(expectedPath, Options(password), TestContext.Current.CancellationToken))
                {
                    if (explicitTransaction)
                    {
                        await using JetTransaction transaction = await expected.BeginTransactionAsync(TestContext.Current.CancellationToken);
                        await InsertAsync(expected, earlySpill: !explicitTransaction);
                        await transaction.CommitAsync(TestContext.Current.CancellationToken);
                    }
                    else
                    {
                        await InsertAsync(expected, earlySpill: !explicitTransaction);
                    }
                }

                byte[] committed = await File.ReadAllBytesAsync(expectedPath, TestContext.Current.CancellationToken);
                string committedPath = Path.Combine(directory, $"committed-{explicitTransaction}.accdb");
                File.Copy(baselinePath, committedPath);
                AssertCrashChild(committedPath, password, write: 0, explicitTransaction);
                Assert.False(File.Exists(committedPath + ".jdw-journal"));
                Assert.Equal(committed, await File.ReadAllBytesAsync(committedPath, TestContext.Current.CancellationToken));
                foreach (int write in new[] { 1, 3 })
                {
                    string path = Path.Combine(directory, $"crash-{explicitTransaction}-{write}.accdb");
                    File.Copy(baselinePath, path);
                    AssertCrashChild(path, password, write, explicitTransaction);
                    Assert.True(File.Exists(path + ".jdw-journal"), "The interrupted write must leave durable recovery evidence.");
                    await Assert.ThrowsAnyAsync<IOException>(async () =>
                    {
                        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken);
                    });
                    await using (AccessWriter recovered = await AccessWriter.OpenAsync(path, Options(password), TestContext.Current.CancellationToken))
                    {
                        Assert.NotNull(recovered);
                    }

                    Assert.Equal(baseline, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                    Assert.False(File.Exists(path + ".jdw-journal"));
                    await using (AccessWriter reopened = await AccessWriter.OpenAsync(path, Options(password), TestContext.Current.CancellationToken))
                    {
                        Assert.NotNull(reopened);
                    }

                    Assert.Equal(baseline, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                }
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
        MaxTransactionPageBudget = 512,
        PageCacheSize = 1,
    };

    private static async Task CrashAsync(string path)
    {
        string password = Environment.GetEnvironmentVariable(ChildPassword) ?? string.Empty;
        int write = int.Parse(Environment.GetEnvironmentVariable(ChildWrite)!, CultureInfo.InvariantCulture);
        bool explicitTransaction = Environment.GetEnvironmentVariable(ChildExplicit) == "True";
        await using var stream = new CrashFileStream(path, write);
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, Options(password), leaveOpen: true, TestContext.Current.CancellationToken);
        if (explicitTransaction)
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await InsertAsync(writer, earlySpill: !explicitTransaction);
            stream.Phase = "commit";
            await transaction.CommitAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            await InsertAsync(writer, earlySpill: !explicitTransaction);
        }

        if (write == 0)
        {
            await File.WriteAllTextAsync(path + ".crashed", "commit completed", TestContext.Current.CancellationToken);
            using var process = Process.GetCurrentProcess();
            process.Kill();
            await process.WaitForExitAsync(CancellationToken.None); // Do not unwind before the process is terminated.
        }
    }

    private static async Task InsertAsync(AccessWriter writer, bool earlySpill)
    {
        // Implicit statements exceed the minimum 64-page spill threshold; explicit ones reach final commit.
        object?[][] rows = new object?[earlySpill ? 2000 : 100][];
        for (int index = 0; index < rows.Length; index++)
        {
            rows[index] = [index + 2, new string('x', 180)];
        }

        await writer.InsertRowsAsync("CrashRows", rows, TestContext.Current.CancellationToken);
    }

    private static void AssertCrashChild(string path, string password, int write, bool explicitTransaction)
    {
        static string Quote(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
        string assembly = typeof(TransactionCrashRecoveryTests).Assembly.Location;
        string script = $"$env:{ChildPath}={Quote(path)}; $env:{ChildPassword}={Quote(password)}; $env:{ChildWrite}='{write.ToString(CultureInfo.InvariantCulture)}'; $env:{ChildExplicit}='{explicitTransaction}'; & dotnet {Quote(assembly)} -method {Quote(Method)}; exit $LASTEXITCODE";
        PowerShellRunResult result = PowerShellProcessRunner.Run("pwsh", ["-NoProfile", "-NonInteractive", "-EncodedCommand", Convert.ToBase64String(Encoding.Unicode.GetBytes(script))], TimeSpan.FromSeconds(60));
        Assert.False(result.TimedOut, result.StandardError);
        Assert.True(result.OutputComplete, result.StandardError);
        Assert.NotEqual(0, result.ExitCode);
        Assert.True(File.Exists(path + ".crashed"), $"Child did not reach crash injection: {result.StandardOutput} {result.StandardError}");
        string expectedPhase = explicitTransaction ? "commit" : "insert";
        Assert.Equal(write == 0 ? "commit completed" : expectedPhase, File.ReadAllText(path + ".crashed"));
    }

    private sealed class CrashFileStream(string path, int targetWrite)
        : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        private int writes;

        public string Phase { get; set; } = "insert";

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await base.WriteAsync(buffer, cancellationToken);
#pragma warning disable CA1849 // Crash injection requires durable bytes; FlushAsync does not flush the device.
            this.Flush(flushToDisk: true);
#pragma warning restore CA1849
            if (++this.writes == targetWrite)
            {
                await File.WriteAllTextAsync(this.Name + ".crashed", this.Phase, cancellationToken);
                using var process = Process.GetCurrentProcess();
                process.Kill();
                await process.WaitForExitAsync(CancellationToken.None); // Do not unwind before the process is terminated.
            }
        }
    }
}
