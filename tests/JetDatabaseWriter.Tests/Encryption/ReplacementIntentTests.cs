namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Replacement decisions bind exact retained generations without assuming directory power-loss durability.</summary>
public sealed class ReplacementIntentTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A pending intent determines recovery even when the destination name is absent.</summary>
    /// <param name="boundary">The interrupted replacement boundary.</param>
    [Theory]
    [InlineData("prepared")]
    [InlineData("renamed")]
    [InlineData("committed")]
    public async Task MissingDestination_RecoversOnlyRecordedDecision(string boundary)
    {
        using var directory = new IntentDirectory();
        byte[] original = await ImageAsync("Original");
        byte[] replacement = await ImageAsync("Replacement");
        await CaptureAsync(directory.Path, original, replacement, boundary);
        Assert.True(File.Exists(IntentPath(directory.Path)));
        await Assert.ThrowsAsync<JetOperationException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(directory.Path, new AccessReaderOptions { UseLockFile = false }, Ct);
        });
        File.Delete(directory.Path);
        await using (AccessWriter recovered = await AccessWriter.OpenAsync(directory.Path, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            Assert.NotNull(recovered);
        }

        Assert.Equal(boundary == "committed" ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.False(File.Exists(IntentPath(directory.Path)));
    }

    /// <summary>Discovery preserves canonical identity when the supplied spelling contains relative components.</summary>
    /// <param name="boundary">The interrupted decision boundary.</param>
    /// <param name="kind">The lexical alias spelling.</param>
    [Theory]
    [InlineData("prepared", "prefix")]
    [InlineData("committed", "prefix")]
    [InlineData("prepared", "dot")]
    [InlineData("committed", "dot")]
    [InlineData("prepared", "parent")]
    [InlineData("committed", "parent")]
    public async Task MissingDestination_NormalizedAlias_RecoversRecordedDecision(string boundary, string kind)
    {
        using var directory = new IntentDirectory();
        byte[] original = await ImageAsync("Original");
        byte[] replacement = await ImageAsync("Replacement");
        await CaptureAsync(directory.Path, original, replacement, boundary);
        File.Delete(directory.Path);
        string parent = System.IO.Path.GetDirectoryName(directory.Path)!;
        Directory.CreateDirectory(System.IO.Path.Combine(parent, "alias"));
        string alias = kind switch
        {
            "dot" => System.IO.Path.Combine(directory.Path, "."),
            "parent" => System.IO.Path.Combine(directory.Path, "alias", ".."),
            _ => System.IO.Path.Combine(parent, "alias", "..", System.IO.Path.GetFileName(directory.Path)),
        };
        Assert.Equal(System.IO.Path.GetFullPath(directory.Path), System.IO.Path.GetFullPath(alias));
        await using (AccessWriter writer = await AccessWriter.OpenAsync(alias, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            Assert.NotNull(writer);
        }

        Assert.Equal(boundary == "committed" ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.False(File.Exists(IntentPath(directory.Path)));
    }

    /// <summary>An already opened file must be refused rather than replaced behind its handle.</summary>
    [Fact]
    public async Task SuppliedFileStream_PendingIntent_RefusesWithoutChangingEvidence()
    {
        using var directory = new IntentDirectory();
        byte[] original = await ImageAsync("Original");
        byte[] replacement = await ImageAsync("Replacement");
        await CaptureAsync(directory.Path, original, replacement, "renamed");
        await using var supplied = new FileStream(directory.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        await Assert.ThrowsAsync<JetOperationException>(async () =>
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(supplied, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        });
        Assert.True(File.Exists(IntentPath(directory.Path)));
        supplied.Position = 0;
        byte[] current = new byte[replacement.Length];
        await supplied.ReadExactlyAsync(current, Ct);
        Assert.Equal(replacement, current);
    }

    /// <summary>Every bounded survivor subset either has the selected exact content or is refused unchanged.</summary>
    /// <param name="boundary">The interrupted decision boundary.</param>
    [Theory]
    [InlineData("prepared")]
    [InlineData("renamed")]
    [InlineData("committed")]
    public async Task SurvivorSubsets_SelectOnlyRecordedExactContent(string boundary)
    {
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5, 6, 7];
        for (int survivors = 0; survivors < 8; survivors++)
        {
            using var directory = new IntentDirectory();
            await CaptureAsync(directory.Path, original, replacement, boundary);
            (string old, string next) = Candidates(directory.Path);
            if ((survivors & 1) == 0)
            {
                File.Delete(directory.Path);
            }

            if ((survivors & 2) == 0)
            {
                File.Delete(old);
            }

            if ((survivors & 4) == 0)
            {
                File.Delete(next);
            }

            bool committed = boundary == "committed";
            bool selectedAvailable = (survivors & (committed ? 4 : 2)) != 0
                || ((survivors & 1) != 0 && (committed || boundary == "prepared"));
            Dictionary<string, string> before = Snapshot(directory.Path);
            if (selectedAvailable)
            {
                Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
                Assert.Equal(committed ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
                Assert.False(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
                Assert.False(File.Exists(old));
                Assert.False(File.Exists(next));
            }
            else
            {
                Assert.Throws<JetCorruptDataException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
                Assert.Equal(before, Snapshot(directory.Path));
            }
        }
    }

    /// <summary>Invalid metadata and conflicting candidates refuse before mutating any survivor.</summary>
    /// <param name="damage">The evidence to corrupt.</param>
    [Theory]
    [InlineData("header")]
    [InlineData("torn-header")]
    [InlineData("oversized-intent")]
    [InlineData("identity")]
    [InlineData("version")]
    [InlineData("oversized-length")]
    [InlineData("decision")]
    [InlineData("old")]
    [InlineData("new")]
    [InlineData("destination")]
    [InlineData("publication")]
    public async Task Conflicts_RefuseWithoutChangingSurvivors(string damage)
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "committed");
        (string old, string next) = Candidates(directory.Path);
        string intent = IntentPath(directory.Path);
        byte[] bytes = await File.ReadAllBytesAsync(intent, Ct);
        if (damage is "header" or "identity" or "version" or "oversized-length" or "decision")
        {
            int offset = damage switch { "header" => 0, "identity" => 32, "version" => 8, "oversized-length" => 64, _ => bytes.Length - 1 };
            bytes[offset] ^= 0xFF;
            if (damage == "oversized-length")
            {
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(64), (2L * 1024 * 1024 * 1024) + 1);
            }

            if (damage is "identity" or "version" or "oversized-length")
            {
                SHA256.HashData(bytes.AsSpan(0, 224)).CopyTo(bytes, 224);
            }

            await File.WriteAllBytesAsync(intent, bytes, Ct);
        }
        else if (damage == "oversized-intent")
        {
            await File.WriteAllBytesAsync(intent, new byte[353], Ct);
        }
        else if (damage == "torn-header")
        {
            await File.WriteAllBytesAsync(intent, bytes.AsMemory(0, 255), Ct);
        }
        else
        {
            string changed = damage switch { "old" => old, "new" => next, "destination" => directory.Path, _ => next.Replace(".new.tmp", ".publish.tmp", StringComparison.Ordinal) };
            await File.WriteAllBytesAsync(changed, [0x91, 0x92], Ct);
        }

        Dictionary<string, string> before = Snapshot(directory.Path);
        Assert.Throws<JetCorruptDataException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(before, Snapshot(directory.Path));
    }

    /// <summary>Incomplete decision appends do not select the replacement.</summary>
    [Fact]
    public async Task TornDecision_RestoresOriginalAndRetainsUnrelatedCandidates()
    {
        using var directory = new IntentDirectory();
        byte[] original = [1, 2, 3];
        await CaptureAsync(directory.Path, original, [4, 5, 6, 7], "decision-partial");
        string unrelated = directory.Path + ".reenc-legacy.tmp";
        await File.WriteAllBytesAsync(unrelated, [0xE8], Ct);
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(new byte[] { 0xE8 }, await File.ReadAllBytesAsync(unrelated, Ct));
    }

    /// <summary>Every incomplete decision prefix selects only the old complete generation.</summary>
    /// <param name="length">The number of appended decision bytes that survived.</param>
    [Theory]
    [InlineData(1)]
    [InlineData(48)]
    [InlineData(95)]
    public async Task DecisionPrefixWithoutCompleteCommit_RestoresOriginal(int length)
    {
        using var directory = new IntentDirectory();
        byte[] original = [1, 2, 3];
        await CaptureAsync(directory.Path, original, [4, 5, 6, 7], "committed");
        await using (var intent = new FileStream(IntentPath(directory.Path), FileMode.Open, FileAccess.Write, FileShare.None))
        {
            intent.SetLength(256 + length);
        }

        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
    }

    /// <summary>Repeated cleanup interruption leaves enough exact content to finish the same decision.</summary>
    /// <param name="boundary">The recovery boundary to interrupt repeatedly.</param>
    [Theory]
    [InlineData("publication-ready")]
    [InlineData("renamed")]
    [InlineData("cleanup.old")]
    [InlineData("cleanup.new.tmp")]
    [InlineData("before-intent-delete")]
    public async Task Recovery_InterruptedTwice_CompletesIdempotently(string boundary)
    {
        using var directory = new IntentDirectory();
        byte[] original = [1, 2, 3];
        await CaptureAsync(directory.Path, original, [4, 5, 6, 7], "renamed");
        for (int attempt = 0; attempt < 2; attempt++)
        {
            if (boundary is "publication-ready" or "renamed")
            {
                File.Delete(directory.Path);
            }

            Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
            directory.Path,
            phase =>
            {
                if (phase == boundary)
                {
                    throw new IOException("Interrupted recovery boundary.");
                }
            },
            cancellationToken: Ct));
            Assert.True(File.Exists(IntentPath(directory.Path)));
        }

        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
    }

    /// <summary>Live and foreign lock evidence blocks publication without deleting any evidence.</summary>
    /// <param name="live">Whether another live owner holds a valid generation receipt.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnershipConflict_RefusesBeforePublication(bool live)
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        RecoverableWriterLock? owner = live ? RecoverableWriterLock.Acquire(directory.Path, "test", "test") : null;
        try
        {
            if (!live)
            {
                await File.WriteAllBytesAsync(System.IO.Path.ChangeExtension(directory.Path, ".laccdb"), [0x99], Ct);
            }

            Dictionary<string, string> before = Snapshot(directory.Path);
            Assert.ThrowsAny<IOException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
            Assert.Equal(before, Snapshot(directory.Path));
        }
        finally
        {
            owner?.Dispose();
        }
    }

    /// <summary>An invalid ownership receipt cannot authorize rollback of an already published replacement.</summary>
    [Fact]
    public async Task InvalidOwnershipReceipt_RefusesBeforePublication()
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        await File.WriteAllBytesAsync(RecoverableWriterLock.ReceiptPath(directory.Path), [0x99], Ct);
        Dictionary<string, string> before = Snapshot(directory.Path);
        Assert.Throws<JetLockException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(before, Snapshot(directory.Path));
    }

    /// <summary>Independent transaction evidence cannot be replayed against another replacement generation.</summary>
    /// <param name="preparing">Whether the transaction evidence is only at its preparation name.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConcurrentJournalEvidence_RefusesBeforePublication(bool preparing)
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        string journal = PersistentRollbackJournal.JournalPath(directory.Path) + (preparing ? ".preparing" : string.Empty);
        await File.WriteAllBytesAsync(journal, [0x99], Ct);
        Dictionary<string, string> before = Snapshot(directory.Path);
        Assert.Throws<JetCorruptDataException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(before, Snapshot(directory.Path));
    }

    /// <summary>Static maintenance keeps its live receipt through publication and releases all owned names on success.</summary>
    [Fact]
    public async Task StaticMaintenance_DefaultLocking_CompletesAndCleansOwnedEvidence()
    {
        using var directory = new IntentDirectory();
        await File.WriteAllBytesAsync(directory.Path, await ImageAsync("Original"), Ct);
        await AccessDatabaseEncryption.EncryptAsync(directory.Path, "Intent123".AsMemory(), AccessEncryptionFormat.AccdbAgile, cancellationToken: Ct);
        await AccessDatabaseEncryption.DecryptAsync(directory.Path, "Intent123".AsMemory(), cancellationToken: Ct);
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(System.IO.Path.GetDirectoryName(directory.Path)!)));
        await using AccessReader reader = await AccessReader.OpenAsync(directory.Path, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Equal("Original", Assert.Single(await reader.ListTablesAsync(Ct)));
    }

    /// <summary>Cancellation during a bounded validation pass preserves every recovery survivor.</summary>
    [Fact]
    public async Task Recovery_CanceledDuringValidation_PreservesAllSurvivors()
    {
        using var directory = new IntentDirectory();
        byte[] original = new byte[131073];
        byte[] replacement = (byte[])original.Clone();
        replacement[^1] = 1;
        await CaptureAsync(directory.Path, original, replacement, "renamed");
        Dictionary<string, string> before = Snapshot(directory.Path);
        using var cancellation = new CancellationTokenSource();
        int chunks = 0;
        OperationCanceledException error = Assert.ThrowsAny<OperationCanceledException>(() => EncryptionReplacementIntent.Recover(
            directory.Path,
            phase =>
        {
            if (phase == "validation-chunk" && ++chunks == 2)
            {
                cancellation.Cancel();
            }
        },
            cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Equal(2, chunks);
        Assert.Equal(before, Snapshot(directory.Path));
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
    }

    /// <summary>Commit validation observes cancellation before preparing a decision or changing the source.</summary>
    [Fact]
    public async Task Commit_CanceledDuringValidation_PreservesOriginalAndCleansPreparation()
    {
        using var directory = new IntentDirectory();
        byte[] original = new byte[131073];
        await File.WriteAllBytesAsync(directory.Path, original, Ct);
        using var cancellation = new CancellationTokenSource();
        int chunks = 0;
        using (var owner = EncryptionReplacementIntent.Begin(
            directory.Path,
            boundary: phase =>
        {
            if (phase == "validation-chunk" && ++chunks == 2)
            {
                cancellation.Cancel();
            }
        },
            cancellationToken: Ct))
        {
            await using (FileStream replacement = owner.CreateReplacement())
            {
                await replacement.WriteAsync(original, Ct);
            }

            OperationCanceledException error = Assert.ThrowsAny<OperationCanceledException>(() => owner.Commit(cancellation.Token));
            Assert.Equal(cancellation.Token, error.CancellationToken);
            Assert.False(owner.IsPrepared);
        }

        Assert.Equal(2, chunks);
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(System.IO.Path.GetDirectoryName(directory.Path)!)));
    }

    /// <summary>An already canceled public writer open never performs pending path recovery.</summary>
    [Fact]
    public async Task PublicOpen_PreCanceled_PreservesPendingIntent()
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        Dictionary<string, string> before = Snapshot(directory.Path);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(directory.Path, cancellationToken: cancellation.Token);
        });
        Assert.Equal(before, Snapshot(directory.Path));
    }

    /// <summary>Header detection and the shared reader guard refuse pending content before touching stream position.</summary>
    /// <param name="missing">Whether the destination name has disappeared.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DetectionAndCommonReaderGuard_RefusePendingEvidence(bool missing)
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, await ImageAsync("Original"), await ImageAsync("Replacement"), "renamed");
        if (missing)
        {
            File.Delete(directory.Path);
        }
        else
        {
            await using var stream = new FileStream(directory.Path, FileMode.Open, FileAccess.Read, FileShare.Read);
            stream.Position = 17;
            await Assert.ThrowsAsync<JetOperationException>(async () => await AccessDatabaseEncryption.DetectEncryptionFormatAsync(stream, Ct));
            Assert.Equal(17, stream.Position);
            Assert.Throws<JetOperationException>(() => PersistentRollbackJournal.RejectHotJournal(stream));
            Assert.Equal(17, stream.Position);
            await Assert.ThrowsAsync<JetOperationException>(async () =>
            {
                await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
            });
            Assert.Equal(17, stream.Position);
        }

        Dictionary<string, string> before = Snapshot(directory.Path);
        await Assert.ThrowsAsync<JetOperationException>(async () => await AccessDatabaseEncryption.DetectEncryptionFormatAsync(directory.Path, Ct));
        Assert.Equal(before, Snapshot(directory.Path));
    }

    /// <summary>Fresh creation cannot displace a pending missing destination or write through an already opened stream.</summary>
    [Fact]
    public async Task Creation_PendingIntent_RefusesBeforePublishingOrWriting()
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        File.Delete(directory.Path);
        Dictionary<string, string> before = Snapshot(directory.Path);
        await Assert.ThrowsAsync<JetOperationException>(async () =>
        {
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(directory.Path, DatabaseFormat.AceAccdb, cancellationToken: Ct);
        });
        Assert.False(File.Exists(directory.Path));
        Assert.Equal(before, Snapshot(directory.Path));
        await using (var stream = new FileStream(directory.Path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None))
        {
            await Assert.ThrowsAsync<JetOperationException>(async () =>
            {
                await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: Ct);
            });
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);
        }

        File.Delete(directory.Path);
        Assert.Equal(before, Snapshot(directory.Path));
    }

    /// <summary>Publication rechecks replacement evidence after initialization and before creating the canonical name.</summary>
    [Fact]
    public async Task Creation_IntentAppearsAtPublication_RefusesAndCleansOnlyOwnStage()
    {
        using var directory = new IntentDirectory();
        await Assert.ThrowsAsync<JetOperationException>(async () => await DatabaseCreationPublication.PublishAsync(
            directory.Path,
            static (stream, token) => stream.WriteAsync(new byte[3], token),
            (phase, _) =>
            {
                if (phase == "Publishing")
                {
                    File.WriteAllBytes(IntentPath(directory.Path), [0x99]);
                }
            },
            Ct));
        Assert.False(File.Exists(directory.Path));
        Assert.Equal(IntentPath(directory.Path), Assert.Single(Directory.GetFiles(System.IO.Path.GetDirectoryName(directory.Path)!)));
    }

    /// <summary>Malformed ownership directories cannot authorize publication or cleanup.</summary>
    /// <param name="receipt">Whether the directory occupies the receipt name.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OwnershipDirectory_RefusesBeforePublication(bool receipt)
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        string occupied = receipt ? RecoverableWriterLock.ReceiptPath(directory.Path) : System.IO.Path.ChangeExtension(directory.Path, ".laccdb");
        Directory.CreateDirectory(occupied);
        Dictionary<string, string> before = Snapshot(directory.Path);
        Assert.Throws<JetLockException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        if (receipt)
        {
            Assert.Throws<JetLockException>(() => RecoverableWriterLock.Recover(directory.Path));
        }
        else
        {
            Assert.False(RecoverableWriterLock.Recover(directory.Path));
        }

        Assert.Equal(before, Snapshot(directory.Path));
        Assert.True(Directory.Exists(occupied));
    }

    /// <summary>Native locks and canonical bytes remain exclusively leased through recovery publication.</summary>
    /// <param name="boundary">The point at which another owner attempts to open.</param>
    [Theory]
    [InlineData("validated")]
    [InlineData("publication-ready")]
    public async Task Recovery_RetainsExclusiveDatabaseAndNativeLeases(string boundary)
    {
        using var directory = new IntentDirectory();
        byte[] original = [1, 2, 3];
        await CaptureAsync(directory.Path, original, [4, 5, 6, 7], "renamed");
        bool attempted = false;
        Assert.True(EncryptionReplacementIntent.Recover(
            directory.Path,
            phase =>
        {
            if (phase == boundary)
            {
                attempted = true;
                foreach (string path in new[] { directory.Path, System.IO.Path.ChangeExtension(directory.Path, ".laccdb"), RecoverableWriterLock.ReceiptPath(directory.Path) })
                {
                    Assert.Throws<IOException>(() =>
                    {
                        using var unexpected = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
                    });
                }
            }
        },
            cancellationToken: Ct));
        Assert.True(attempted);
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
    }

    /// <summary>Directories at any owned replacement name refuse before changing current-new content or survivors.</summary>
    /// <param name="suffix">The occupied owned name.</param>
    [Theory]
    [InlineData(".old")]
    [InlineData(".new.tmp")]
    [InlineData(".publish.tmp")]
    [InlineData("intent")]
    public async Task CandidateDirectory_RefusesBeforeAnyRecoveryMutation(string suffix)
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        (_, string next) = Candidates(directory.Path);
        string occupied = suffix == "intent" ? IntentPath(directory.Path) : next[..^8] + suffix;
        File.Delete(occupied);
        Directory.CreateDirectory(occupied);
        Dictionary<string, string> before = Snapshot(directory.Path);
        Assert.Throws<JetCorruptDataException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(before, Snapshot(directory.Path));
        Assert.True(Directory.Exists(occupied));
    }

    /// <summary>Recovery ownership interrupted before its receipt remains unexplained and cannot authorize automatic cleanup.</summary>
    [Fact]
    public async Task RecoveryOwnershipBootstrap_InterruptedBeforeReceipt_RetainsAndRefuses()
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "renamed");
        Dictionary<string, string> before = Snapshot(directory.Path);
        Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
            directory.Path,
            phase =>
            {
                if (phase == "recovery-lock-created")
                {
                    throw new IOException("Interrupted before ownership receipt creation.");
                }
            },
            Ct));
        string native = System.IO.Path.ChangeExtension(directory.Path, ".laccdb");
        Assert.True(File.Exists(native));
        Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
        Dictionary<string, string> interrupted = Snapshot(directory.Path);
        foreach (KeyValuePair<string, string> survivor in before)
        {
            Assert.Equal(survivor.Value, interrupted[survivor.Key]);
        }

        Assert.Throws<JetLockException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(interrupted, Snapshot(directory.Path));
    }

    /// <summary>File-backed creation refuses every occupied recovery name before writing bootstrap bytes.</summary>
    /// <param name="kind">The deterministic evidence name.</param>
    /// <param name="directoryEvidence">Whether a directory occupies the evidence name.</param>
    [Theory]
    [InlineData("journal", false)]
    [InlineData("journal", true)]
    [InlineData("preparing", false)]
    [InlineData("preparing", true)]
    [InlineData("receipt", false)]
    [InlineData("receipt", true)]
    [InlineData("intent", false)]
    [InlineData("intent", true)]
    public async Task Creation_AnyPendingEvidence_PreservesSuppliedStreamAndNames(string kind, bool directoryEvidence)
    {
        using var directory = new IntentDirectory();
        await File.WriteAllBytesAsync(directory.Path, await ImageAsync("Original"), Ct);
        string evidence = kind switch
        {
            "journal" => PersistentRollbackJournal.JournalPath(directory.Path),
            "preparing" => PersistentRollbackJournal.JournalPath(directory.Path) + ".preparing",
            "receipt" => RecoverableWriterLock.ReceiptPath(directory.Path),
            _ => IntentPath(directory.Path),
        };
        if (directoryEvidence)
        {
            Directory.CreateDirectory(evidence);
        }
        else
        {
            await File.WriteAllBytesAsync(evidence, [0x99], Ct);
        }

        Dictionary<string, string> before = Snapshot(directory.Path);
        await using (var stream = new FileStream(directory.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            stream.Position = 17;
            await Assert.ThrowsAsync<JetOperationException>(async () =>
            {
                await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: Ct);
            });
            Assert.Equal(17, stream.Position);
        }

        Assert.Equal(before, Snapshot(directory.Path));
        Assert.True(directoryEvidence ? Directory.Exists(evidence) : File.Exists(evidence));
    }

    /// <summary>Interrupted publication prefixes are owned only when their bytes match a validated generation.</summary>
    /// <param name="replacementPrefix">Whether the retained prefix was copied from the unselected replacement.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublicationPrefix_ReplaysValidatedSelectedGeneration(bool replacementPrefix)
    {
        using var directory = new IntentDirectory();
        byte[] original = [1, 2, 3];
        await CaptureAsync(directory.Path, original, [4, 5, 6, 7], "renamed");
        (_, string next) = Candidates(directory.Path);
        byte[] prefix = replacementPrefix ? [4, 5] : [1, 2];
        await File.WriteAllBytesAsync(next.Replace(".new.tmp", ".publish.tmp", StringComparison.Ordinal), prefix, Ct);
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
    }

    /// <summary>An oversized sparse candidate is rejected before replay buffers or content scans.</summary>
    [Fact]
    public async Task CandidateLengthBound_RefusesSparseContent()
    {
        using var directory = new IntentDirectory();
        await CaptureAsync(directory.Path, [1, 2, 3], [4, 5, 6, 7], "prepared");
        (_, string next) = Candidates(directory.Path);
        await using (var oversized = new FileStream(next, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            oversized.SetLength((2L * 1024 * 1024 * 1024) + 1);
        }

        Assert.Throws<JetCorruptDataException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal((2L * 1024 * 1024 * 1024) + 1, new FileInfo(next).Length);
        Assert.True(File.Exists(IntentPath(directory.Path)));
    }

    private static string IntentPath(string path) => path + ".jdw-replacement";

    private static (string Old, string Next) Candidates(string path)
    {
        byte[] intent = File.ReadAllBytes(IntentPath(path));
        string generation = new Guid(intent.AsSpan(16, 16)).ToString("N");
        string prefix = path + ".jdw-replace-" + generation;
        return (prefix + ".old", prefix + ".new.tmp");
    }

    private static Dictionary<string, string> Snapshot(string path) => Directory.GetFiles(System.IO.Path.GetDirectoryName(path)!)
        .Order(StringComparer.Ordinal).ToDictionary(static file => file, DigestFile, StringComparer.Ordinal);

    private static string DigestFile(string path)
    {
        using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        return Convert.ToHexString(SHA256.HashData(input));
    }

    private static async Task CaptureAsync(string path, byte[] original, byte[] replacement, string boundary)
    {
        string stage = path + ".input.tmp";
        await File.WriteAllBytesAsync(path, original, Ct);
        await File.WriteAllBytesAsync(stage, replacement, Ct);
        Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(stage, path, phase =>
        {
            if (phase == boundary)
            {
                throw new IOException("Interrupted replacement " + boundary);
            }
        }));
    }

    private static async Task<byte[]> ImageAsync(string name)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync(name, [new("Id", typeof(int))], Ct);
            await writer.InsertRowAsync(name, [1], Ct);
        }

        return stream.ToArray();
    }

    private sealed class IntentDirectory : IDisposable
    {
        private readonly string directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "jdw-replacement-intent-" + Guid.NewGuid().ToString("N"));

        internal IntentDirectory()
        {
            Directory.CreateDirectory(this.directory);
            this.Path = System.IO.Path.Combine(this.directory, "database.accdb");
        }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(this.directory, recursive: true);
    }
}
