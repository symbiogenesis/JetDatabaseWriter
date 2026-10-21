namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using Xunit;

/// <summary>Exercises replacement and cancellation against the host filesystem.</summary>
public sealed class AtomicReplacementTests
{
    /// <summary>Replacement does not clean up a different operation's temporary file.</summary>
    [Fact]
    public async Task Replace_SucceedsAndPreservesUnrelatedTemporaryFile()
    {
        string directory = CreateDirectory();
        string path = Path.Combine(directory, "database.accdb");
        string otherTemporaryPath = path + ".reenc-other.tmp";
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5, 6, 7];
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
            await File.WriteAllBytesAsync(otherTemporaryPath, original, TestContext.Current.CancellationToken);

            await EncryptionManager.ReplaceFileAtomicAsync(path, replacement, TestContext.Current.CancellationToken);

            Assert.Equal(replacement, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Equal(original, await File.ReadAllBytesAsync(otherTemporaryPath, TestContext.Current.CancellationToken));
            Assert.Equal(otherTemporaryPath, Assert.Single(Directory.GetFiles(directory, "*.tmp")));
            Assert.Empty(Directory.GetFiles(directory, "*.original"));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Cancellation leaves the original intact and removes its temporary file.</summary>
    [Fact]
    public async Task Replace_Canceled_LeavesOriginalAndNoNewTemporaryFile()
    {
        string directory = CreateDirectory();
        string path = Path.Combine(directory, "database.accdb");
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5];
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);

            OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await EncryptionManager.ReplaceFileAtomicAsync(path, replacement, canceled.Token));

            Assert.Equal(canceled.Token, error.CancellationToken);
            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Empty(Directory.GetFiles(directory, "*.tmp"));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>An already canceled operation does not attempt to create its temporary file.</summary>
    [Fact]
    public async Task Replace_CanceledBeforeOpen_DoesNotTouchFilesystem()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"AtomicReplacementTests_{Guid.NewGuid():N}");
        string path = Path.Combine(directory, "database.accdb");
        byte[] replacement = [4, 5];
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await EncryptionManager.ReplaceFileAtomicAsync(path, replacement, canceled.Token));

        Assert.Equal(canceled.Token, error.CancellationToken);
        Assert.False(Directory.Exists(directory));
    }

    /// <summary>Windows refuses a held destination; Unix replaces its name while preserving the old handle.</summary>
    [Fact]
    public async Task Replace_WithOpenReader_ObeysHostRenameSemantics()
    {
        string directory = CreateDirectory();
        string path = Path.Combine(directory, "database.accdb");
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5, 6, 7];
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
            await using (var reader = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.Asynchronous))
            {
                if (OperatingSystem.IsWindows())
                {
                    IOException error = await Assert.ThrowsAsync<IOException>(async () =>
                        await EncryptionManager.ReplaceFileAtomicAsync(path, replacement, TestContext.Current.CancellationToken));
                    string staged = Assert.IsType<string>(error.Data[EncryptionManager.ReplacementFileDataKey]);
                    Assert.Equal(replacement, await File.ReadAllBytesAsync(staged, TestContext.Current.CancellationToken));
                    File.Delete(staged);
                    Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                }
                else
                {
                    await EncryptionManager.ReplaceFileAtomicAsync(path, replacement, TestContext.Current.CancellationToken);
                    Assert.Equal(replacement, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                }

                await using var observed = new MemoryStream();
                await reader.CopyToAsync(observed, TestContext.Current.CancellationToken);
                Assert.Equal(original, observed.ToArray());
                Assert.Empty(Directory.GetFiles(directory, "*.tmp"));
            }

            await EncryptionManager.ReplaceFileAtomicAsync(path, replacement, TestContext.Current.CancellationToken);
            Assert.Equal(replacement, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static string CreateDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"AtomicReplacementTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }
}
