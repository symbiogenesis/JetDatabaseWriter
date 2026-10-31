namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Replays production-order replacement cleanup while retaining destination ownership.</summary>
public sealed class ReplacementCleanupConformanceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Ownership names disappear before intent while the selected destination excludes another opener.</summary>
    /// <param name="committed">Whether the complete decision selects the replacement.</param>
    /// <param name="missingDestination">Whether recovery must first publish the selected image.</param>
    /// <param name="cut">The exact ownership cleanup boundary to interrupt.</param>
    [Theory]
    [InlineData(false, false, "cleanup-native")]
    [InlineData(false, false, "cleanup-receipt")]
    [InlineData(false, false, "ownership-cleanup")]
    [InlineData(false, true, "cleanup-native")]
    [InlineData(false, true, "cleanup-receipt")]
    [InlineData(false, true, "ownership-cleanup")]
    [InlineData(true, false, "cleanup-native")]
    [InlineData(true, false, "cleanup-receipt")]
    [InlineData(true, false, "ownership-cleanup")]
    [InlineData(true, true, "cleanup-native")]
    [InlineData(true, true, "cleanup-receipt")]
    [InlineData(true, true, "ownership-cleanup")]
    public async Task Recover_OwnershipCleanupCuts_RetainSelectedImageAndExcludeSuccessor(bool committed, bool missingDestination, string cut)
    {
        using var directory = new CleanupDirectory();
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5, 6, 7];
        string stage = directory.Path + ".input.tmp";
        string intent = EncryptionReplacementIntent.IntentPath(directory.Path);
        string native = Path.ChangeExtension(directory.Path, ".laccdb");
        string receipt = RecoverableWriterLock.ReceiptPath(directory.Path);
        await File.WriteAllBytesAsync(directory.Path, original, Ct);
        await File.WriteAllBytesAsync(stage, replacement, Ct);
        Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(stage, directory.Path, phase =>
        {
            if (phase == (committed ? "committed" : "renamed"))
            {
                throw new IOException("Capture an admitted replacement decision.");
            }
        }));

        byte[] decision = await File.ReadAllBytesAsync(intent, Ct);
        if (missingDestination)
        {
            File.Delete(directory.Path);
        }

        var observed = new List<string>();
        Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
        directory.Path,
        phase =>
        {
            observed.Add(phase);
            if (phase is "cleanup-native" or "cleanup-receipt" or "ownership-cleanup")
            {
                Assert.True(File.Exists(intent));
                Assert.False(File.Exists(native));
                Assert.Equal(phase == "cleanup-native", File.Exists(receipt));
                Assert.Empty(Directory.GetFiles(directory.DirectoryPath, "*.jdw-replace-*"));
                Assert.Throws<IOException>(() =>
                {
                    using var successor = new FileStream(directory.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
                });
            }

            if (phase == cut)
            {
                throw new IOException("Interrupt production-order ownership cleanup.");
            }
        },
        Ct));

        Assert.True(observed.IndexOf("cleanup.new.tmp") < observed.IndexOf("before-intent-delete"));
        Assert.True(observed.IndexOf("before-intent-delete") < observed.IndexOf("cleanup-native"));
        if (cut != "cleanup-native")
        {
            Assert.True(observed.IndexOf("cleanup-native") < observed.IndexOf("cleanup-receipt"));
        }

        if (cut == "ownership-cleanup")
        {
            Assert.True(observed.IndexOf("cleanup-receipt") < observed.IndexOf("ownership-cleanup"));
        }

        Assert.Equal(committed ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(decision, await File.ReadAllBytesAsync(intent, Ct));
        Assert.False(File.Exists(native));
        Assert.Equal(cut == "cleanup-native", File.Exists(receipt));
        byte[]? retainedReceipt = File.Exists(receipt) ? await File.ReadAllBytesAsync(receipt, Ct) : null;
        int freshBootstrap = 0;
        Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
        directory.Path,
        phase =>
        {
            if (phase == "recovery-lock-created")
            {
                freshBootstrap++;
            }

            if (phase == "validated")
            {
                throw new IOException("Observe recorded reconstruction or fresh bootstrap before cleanup.");
            }
        },
        Ct));
        Assert.Equal(cut == "cleanup-native" ? 0 : 1, freshBootstrap);
        Assert.True(File.Exists(native));
        Assert.True(File.Exists(receipt));
        if (retainedReceipt is not null)
        {
            Assert.Equal(retainedReceipt, await File.ReadAllBytesAsync(receipt, Ct));
        }

        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.False(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(committed ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.False(File.Exists(intent));
        Assert.False(File.Exists(native));
        Assert.False(File.Exists(receipt));
    }

    /// <summary>Repeated recovery transfers surviving authority without changing the selected image or decision.</summary>
    /// <param name="committed">Whether the complete decision selects the replacement.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Recover_RepeatedOwnershipRestarts_PreserveSelectionAndReconstructOnlySurvivingGeneration(bool committed)
    {
        using var directory = new CleanupDirectory();
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5, 6, 7];
        string stage = directory.Path + ".input.tmp";
        string intent = EncryptionReplacementIntent.IntentPath(directory.Path);
        string native = Path.ChangeExtension(directory.Path, ".laccdb");
        string receipt = RecoverableWriterLock.ReceiptPath(directory.Path);
        await File.WriteAllBytesAsync(directory.Path, original, Ct);
        await File.WriteAllBytesAsync(stage, replacement, Ct);
        Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(stage, directory.Path, phase =>
        {
            if (phase == (committed ? "committed" : "renamed"))
            {
                throw new IOException("Capture the decision before repeated recovery.");
            }
        }));

        byte[] decision = await File.ReadAllBytesAsync(intent, Ct);
        File.Delete(directory.Path);
        byte[]? lastReceipt = null;
        foreach (string cut in new[] { "cleanup-native", "validated", "cleanup-native", "cleanup-receipt", "validated", "cleanup-native", "ownership-cleanup" })
        {
            bool recorded = File.Exists(receipt);
            int freshBootstrap = 0;
            bool interrupted = false;
            Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
            directory.Path,
            phase =>
            {
                if (phase == "recovery-lock-created")
                {
                    freshBootstrap++;
                }

                if (phase == cut)
                {
                    interrupted = true;
                    Assert.True(File.Exists(intent));
                    Assert.Throws<IOException>(() =>
                    {
                        using var successor = new FileStream(directory.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
                    });
                    throw new IOException("Interrupt another recovery owner at " + cut);
                }
            },
            Ct));

            Assert.True(interrupted);
            Assert.Equal(recorded ? 0 : 1, freshBootstrap);
            Assert.Equal(committed ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
            Assert.Equal(decision, await File.ReadAllBytesAsync(intent, Ct));
            Assert.Equal(cut == "validated", File.Exists(native));
            Assert.Equal(cut is "validated" or "cleanup-native", File.Exists(receipt));
            Assert.Empty(Directory.GetFiles(directory.DirectoryPath, "*.jdw-replace-*"));
            if (File.Exists(receipt))
            {
                byte[] currentReceipt = await File.ReadAllBytesAsync(receipt, Ct);
                if (lastReceipt is not null)
                {
                    Assert.Equal(recorded, lastReceipt.AsSpan().SequenceEqual(currentReceipt));
                }

                lastReceipt = currentReceipt;
            }
        }

        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.False(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(committed ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.False(File.Exists(intent));
        Assert.False(File.Exists(native));
        Assert.False(File.Exists(receipt));
        using var successorOwner = RecoverableWriterLock.Acquire(directory.Path, "successor", "after-recovery");
        Assert.True(File.Exists(native));
        Assert.True(File.Exists(receipt));
    }

    private sealed class CleanupDirectory : IDisposable
    {
        internal CleanupDirectory()
        {
            this.DirectoryPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "jdw-replacement-cleanup-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(this.DirectoryPath);
            this.Path = System.IO.Path.Combine(this.DirectoryPath, "database.accdb");
        }

        internal string DirectoryPath { get; }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(this.DirectoryPath, recursive: true);
    }
}
