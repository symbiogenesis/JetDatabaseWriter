namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Connects complete production publication exceptions to retained decisions and eventual selected cleanup.</summary>
/// <remarks>Disposed streams may persist buffers; these are exception traces rather than power-loss qualification.</remarks>
public sealed class ReplacementPublicationConformanceTests
{
    /// <summary>Gets read, write, flush, rename and destination-fence cuts through the complete recovery caller.</summary>
    public static TheoryData<bool, bool, string, int> RecoveryCuts
    {
        get
        {
            var cases = new TheoryData<bool, bool, string, int>();
            List<string> cuts = ["publication-created", "publication-read", "publication-before-flush", "publication-flush", "publication-ready", "renamed", "destination-before-flush", "destination-before-directory-flush", "destination-flush", "cleanup-before-flush", "cleanup-flush", "cleanup-before-directory-flush"];
            if (DirectoryPersistence.IsSupported)
            {
                cuts.AddRange(["destination-directory-flush", "cleanup-directory-flush"]);
            }

            foreach (bool committed in new[] { false, true })
            {
                foreach (bool missing in new[] { false, true })
                {
                    foreach (string cut in cuts)
                    {
                        cases.Add(committed, missing, cut, 1);
                    }

                    for (int occurrence = 1; occurrence <= 3; occurrence++)
                    {
                        cases.Add(committed, missing, "publication-written", occurrence);
                    }
                }
            }

            return cases;
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Each complete recovery interruption retains its original selection through another interruption and successful cleanup.</summary>
    /// <param name="committed">Whether the complete decision selects the new copy.</param>
    /// <param name="missing">Whether the authorized destination name was lost.</param>
    /// <param name="cut">The production operation boundary to interrupt.</param>
    /// <param name="occurrence">The selected buffer occurrence.</param>
    [Theory]
    [MemberData(nameof(RecoveryCuts))]
    public async Task Recover_PublicationExceptions_PreserveDecisionThroughRepeatedRecovery(bool committed, bool missing, string cut, int occurrence)
    {
        using var directory = new PublicationDirectory();
        byte[] original = Image(131089, 0);
        byte[] replacement = Image(131089, 17);
        (string old, string next) = await CaptureAsync(directory.Path, original, replacement, committed ? "committed" : "renamed");
        byte[] selected = committed ? replacement : original;
        byte[] before = committed ? original : replacement;
        if (missing)
        {
            File.Delete(directory.Path);
        }
        else
        {
            // Choose the other complete authorized survivor to require actual publication on both decision branches.
            await File.WriteAllBytesAsync(directory.Path, before, Ct);
        }

        string intentPath = EncryptionReplacementIntent.IntentPath(directory.Path);
        byte[] intent = await File.ReadAllBytesAsync(intentPath, Ct);
        string publication = next.Replace(".new.tmp", ".publish.tmp", StringComparison.Ordinal);
        bool renamed = cut is "renamed" or "destination-before-flush" or "destination-before-directory-flush" or "destination-directory-flush" or "destination-flush" or "cleanup-before-flush" or "cleanup-flush" or "cleanup-before-directory-flush" or "cleanup-directory-flush";
        for (int attempt = 0; attempt < 2; attempt++)
        {
            // Once rename has selected the right image, retry skips publication and fences it again in Cleanup.
            string retryCut = renamed && attempt != 0 ? "cleanup-flush" : cut;
            int expectedOccurrence = renamed && attempt != 0 ? 1 : occurrence;
            int seen = 0;
            var observed = new List<string>();
            Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
                directory.Path,
                phase =>
                {
                    observed.Add(phase);
                    if (phase == retryCut && ++seen == expectedOccurrence)
                    {
                        throw new IOException("Interrupt the complete production publication trace.");
                    }
                },
                Ct));
            Assert.Equal(expectedOccurrence, seen);
            Assert.Contains("validated", observed);
            Assert.DoesNotContain("cleanup.old", observed);
            Assert.DoesNotContain("cleanup.new.tmp", observed);
            Assert.Equal(intent, await File.ReadAllBytesAsync(intentPath, Ct));
            Assert.Equal(original, await File.ReadAllBytesAsync(old, Ct));
            Assert.Equal(replacement, await File.ReadAllBytesAsync(next, Ct));
            Assert.True(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
            Assert.True(File.Exists(Path.ChangeExtension(directory.Path, ".laccdb")));
            if (renamed)
            {
                Assert.Equal(selected, await File.ReadAllBytesAsync(directory.Path, Ct));
                Assert.False(File.Exists(publication));
            }
            else
            {
                Assert.Equal(!missing, File.Exists(directory.Path));
                if (!missing)
                {
                    Assert.Equal(before, await File.ReadAllBytesAsync(directory.Path, Ct));
                }

                byte[] prefix = await File.ReadAllBytesAsync(publication, Ct);
                int expectedLength = cut switch
                {
                    "publication-created" or "publication-read" => 0,
                    "publication-written" => Math.Min(occurrence * 65536, selected.Length),
                    _ => selected.Length,
                };
                Assert.Equal(selected.AsSpan(0, expectedLength).ToArray(), prefix);
                Assert.DoesNotContain("renamed", observed);
                if (cut == "publication-before-flush")
                {
                    Assert.DoesNotContain("publication-flush", observed);
                }
            }
        }

        var completed = new List<string>();
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, completed.Add, Ct));
        Assert.Contains("cleanup-before-flush", completed);
        Assert.Contains("cleanup-flush", completed);
        Assert.Equal(DirectoryPersistence.IsSupported, completed.Contains("cleanup-directory-flush"));
        Assert.Contains("cleanup.old", completed);
        Assert.True(completed.IndexOf("cleanup-before-flush") < completed.IndexOf("cleanup-flush"));
        if (DirectoryPersistence.IsSupported)
        {
            Assert.True(completed.IndexOf("cleanup-flush") < completed.IndexOf("cleanup-directory-flush"));
            Assert.True(completed.IndexOf("cleanup-directory-flush") < completed.IndexOf("cleanup.old"));
        }
        else
        {
            Assert.True(completed.IndexOf("cleanup-flush") < completed.IndexOf("cleanup.old"));
        }

        Assert.Equal(selected, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(directory.DirectoryPath)));
        Assert.False(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
    }

    /// <summary>Commit cannot append its decision after a failed publication or destination flush boundary.</summary>
    /// <param name="cut">The actual publication operation boundary.</param>
    [Theory]
    [InlineData("publication-created")]
    [InlineData("publication-read")]
    [InlineData("publication-written")]
    [InlineData("publication-before-flush")]
    [InlineData("publication-flush")]
    [InlineData("destination-before-flush")]
    [InlineData("prepared-before-directory-flush")]
    [InlineData("destination-before-directory-flush")]
    [InlineData("destination-flush")]
    public async Task Commit_PublicationExceptions_RetainPreparedUntilOldSelectionIsRecovered(string cut)
    {
        using var directory = new PublicationDirectory();
        byte[] original = Image(65553, 0);
        byte[] replacement = Image(65553, 17);
        (string old, string next) = await CaptureAsync(directory.Path, original, replacement, cut);
        byte[] intent = await File.ReadAllBytesAsync(EncryptionReplacementIntent.IntentPath(directory.Path), Ct);
        Assert.Equal(256, intent.Length);
        Assert.Equal(original, await File.ReadAllBytesAsync(old, Ct));
        Assert.Equal(replacement, await File.ReadAllBytesAsync(next, Ct));
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(directory.DirectoryPath)));
    }

    /// <summary>A zero-byte selected image still passes both flush gates before any evidence disappears.</summary>
    /// <param name="committed">Whether the empty image is the committed replacement or original.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Recover_EmptySelectedCopy_StillRequiresBothFlushes(bool committed)
    {
        using var directory = new PublicationDirectory();
        byte[] original = committed ? Image(17, 0) : [];
        byte[] replacement = committed ? [] : Image(17, 17);
        (_, string next) = await CaptureAsync(directory.Path, original, replacement, committed ? "committed" : "renamed");
        File.Delete(directory.Path);
        string publication = next.Replace(".new.tmp", ".publish.tmp", StringComparison.Ordinal);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            var observed = new List<string>();
            Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
                directory.Path,
                phase =>
                {
                    observed.Add(phase);
                    if (phase == "publication-before-flush")
                    {
                        throw new IOException("Interrupt the empty publication before its required flush.");
                    }
                },
                Ct));
            Assert.DoesNotContain("publication-read", observed);
            Assert.DoesNotContain("publication-written", observed);
            Assert.DoesNotContain("publication-flush", observed);
            Assert.DoesNotContain("renamed", observed);
            Assert.Equal(0, new FileInfo(publication).Length);
            Assert.False(File.Exists(directory.Path));
        }

        var completed = new List<string>();
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, completed.Add, Ct));
        Assert.Contains("publication-flush", completed);
        Assert.Contains("renamed", completed);
        Assert.Contains("destination-flush", completed);
        Assert.Contains("cleanup-flush", completed);
        Assert.Contains("cleanup.old", completed);
        Assert.True(completed.IndexOf("publication-flush") < completed.IndexOf("renamed"));
        Assert.True(completed.IndexOf("renamed") < completed.IndexOf("destination-flush"));
        Assert.True(completed.IndexOf("destination-flush") < completed.IndexOf("cleanup-flush"));
        Assert.True(completed.IndexOf("cleanup-flush") < completed.IndexOf("cleanup.old"));
        Assert.Empty(await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(directory.DirectoryPath)));
    }

    private static async Task<(string Old, string Next)> CaptureAsync(string path, byte[] original, byte[] replacement, string cut)
    {
        await File.WriteAllBytesAsync(path, original, Ct);
        using var owner = EncryptionReplacementIntent.Begin(
            path,
            boundary: phase =>
            {
                if (phase == cut)
                {
                    throw new IOException("Capture a production publication exception survivor.");
                }
            },
            cancellationToken: Ct);
        await using (FileStream output = owner.CreateReplacement())
        {
            await output.WriteAsync(replacement, Ct);
        }

        Assert.Throws<IOException>(() => owner.Commit(Ct));
        Assert.True(owner.IsPrepared);
        return (owner.OldPath, owner.NewPath);
    }

    private static byte[] Image(int length, int shift)
    {
        byte[] bytes = new byte[length];
        for (int index = 0; index < bytes.Length; index++)
        {
            bytes[index] = (byte)((index + (index / 65536) + shift) % 251);
        }

        return bytes;
    }

    private sealed class PublicationDirectory : IDisposable
    {
        internal PublicationDirectory()
        {
            this.DirectoryPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "jdw-replacement-publication-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(this.DirectoryPath);
            this.Path = System.IO.Path.Combine(this.DirectoryPath, "database.accdb");
        }

        internal string DirectoryPath { get; }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(this.DirectoryPath, recursive: true);
    }
}
