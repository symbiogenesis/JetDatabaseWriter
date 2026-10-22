namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;
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
            CrashRecoveryProcess.MarkReady(childPath);
            string target = Environment.GetEnvironmentVariable(ChildBoundary)!;
            FileShare sharing = OperatingSystem.IsWindows() ? FileShare.Delete : FileShare.None;
            await using var source = new FileStream(childPath, FileMode.Open, FileAccess.Read, sharing);
            using var native = LockFileCoordinator.ForReencrypt(childPath, new AccessWriterOptions());
            native.Acquire();
            using var operation = EncryptionReplacementIntent.Begin(
                childPath,
                source,
                boundary =>
            {
                if (boundary == target)
                {
                    CrashRecoveryProcess.Terminate(childPath, boundary);
                }
            },
                TestContext.Current.CancellationToken);
            operation.CopyReplacement(childPath + ".reenc-owned.tmp");
            operation.Commit(TestContext.Current.CancellationToken);
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
            foreach (string boundary in new[] { "prepared", "renamed", "decision-partial", "committed" })
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
                string old = Assert.Single(Directory.GetFiles(directory, Path.GetFileName(path) + ".jdw-replace-*.old"));
                string next = Assert.Single(Directory.GetFiles(directory, Path.GetFileName(path) + ".jdw-replace-*.new.tmp"));
                Assert.Equal(oldBytes, await File.ReadAllBytesAsync(old, TestContext.Current.CancellationToken));
                Assert.Equal(newBytes, await File.ReadAllBytesAsync(next, TestContext.Current.CancellationToken));
                await Assert.ThrowsAsync<JetOperationException>(async () =>
                {
                    await using AccessReader refused = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
                });
                File.Delete(path);
                await using (AccessWriter recovered = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
                {
                    Assert.NotNull(recovered);
                }

                bool committed = boundary == "committed";
                Assert.Equal(committed ? newBytes : oldBytes, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
                Assert.Equal(committed ? "Replacement" : "Original", Assert.Single(await reader.ListTablesAsync(TestContext.Current.CancellationToken)));
                Assert.False(File.Exists(EncryptionReplacementIntent.IntentPath(path)));
                Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(path)));
                Assert.False(File.Exists(Path.ChangeExtension(path, ".laccdb")));
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Repeated process deaths during recovery retain the selected generation until cleanup finishes.</summary>
    [Fact]
    public async Task Recovery_ProcessKilledTwice_CompletesRecordedDecision()
    {
        if (Environment.GetEnvironmentVariable(ChildPath) is { } childPath)
        {
            CrashRecoveryProcess.MarkReady(childPath);
            string target = Environment.GetEnvironmentVariable(ChildBoundary)!;
            EncryptionReplacementIntent.Recover(
                childPath,
                boundary =>
            {
                if (boundary == target)
                {
                    CrashRecoveryProcess.Terminate(childPath, boundary);
                }
            },
                TestContext.Current.CancellationToken);
            Assert.Fail("The requested recovery boundary was never interrupted.");
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementRecoveryCrashTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            foreach (string boundary in new[] { "publication-ready", "renamed", "cleanup.old", "cleanup.new.tmp", "before-intent-delete" })
            {
                string path = Path.Combine(directory, boundary + ".accdb");
                await CreateImageAsync(path, "Original");
                string temporary = path + ".input.tmp";
                await CreateImageAsync(temporary, "Replacement");
                byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
                Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(temporary, path, phase =>
                {
                    if (phase == "renamed")
                    {
                        throw new IOException("Prepare pending recovery.");
                    }
                }));
                File.Delete(path);
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    if (boundary is "publication-ready" or "renamed")
                    {
                        File.Delete(path);
                    }

                    CrashRecoveryProcess.AssertChild(
                        "JetDatabaseWriter.Tests.Encryption.ReplacementCrashRecoveryTests.Recovery_ProcessKilledTwice_CompletesRecordedDecision",
                        path,
                        boundary,
                        "replacement recovery, boundary=" + boundary,
                        new Dictionary<string, string> { [ChildPath] = path, [ChildBoundary] = boundary });
                    Assert.True(File.Exists(EncryptionReplacementIntent.IntentPath(path)));
                }

                await using (AccessWriter recovered = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
                {
                    Assert.NotNull(recovered);
                }

                Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                Assert.False(File.Exists(EncryptionReplacementIntent.IntentPath(path)));
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

    private static void AssertCrashChild(string path, string boundary) =>
        CrashRecoveryProcess.AssertChild(
            Method,
            path,
            boundary,
            $"replacement, boundary={boundary}",
            new Dictionary<string, string>
            {
                [ChildPath] = path,
                [ChildBoundary] = boundary,
            });
}
