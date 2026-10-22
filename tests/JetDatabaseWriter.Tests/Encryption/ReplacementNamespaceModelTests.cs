namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Exercises bounded namespace survivor models through actual public open and journal recovery.</summary>
/// <remarks>
/// Tests remove, resurrect or corrupt isolated names after actual replacement/recovery lifecycle boundaries.
/// These are independently selected software fault states, not physical power interruptions or filesystem guarantees.
/// Random replacement candidates have no automatic discovery contract. Preparation journals require complete validation.
/// </remarks>
/// <param name="output">Reports the bounded modeled states and observed discovery limitations.</param>
public sealed class ReplacementNamespaceModelTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ActualReplacementBoundaries_AllNameSurvivorSubsets_PreserveCandidates(DatabaseFormat format)
    {
        using var directory = new ModelDirectory();
        (byte[] oldBytes, byte[] newBytes) = await ImagesAsync(format);
        Assert.NotEqual(Digest(oldBytes), Digest(newBytes));
        int states = 0;
        foreach (string boundary in new[] { "prepared", "renamed", "committed" })
        {
            string captured = directory.Child("capture-" + boundary);
            CapturedReplacement snapshot = await CaptureReplacementAsync(captured, oldBytes, newBytes, boundary);
            int available = snapshot.StageExists ? 7 : 5;
            for (int survivors = 0; survivors <= 7; survivors++)
            {
                if ((survivors & ~available) != 0)
                {
                    continue;
                }

                string replay = directory.Child(boundary + "-" + survivors);
                string path = ReplayReplacement(snapshot, replay, survivors);
                await WriteUnknownCandidatesAsync(path);
                Dictionary<string, string> before = await SnapshotAsync(replay);
                if ((survivors & 1) != 0)
                {
                    await OpenWriterAsync(path);
                    await AssertTableAsync(path, boundary == "prepared" ? "Original" : "Replacement");
                }
                else
                {
                    // A complete .original or staging candidate is retained, not
                    // silently selected as the missing destination's identity.
                    await Assert.ThrowsAsync<FileNotFoundException>(() => OpenWriterAsync(path));
                }

                AssertSnapshot(before, await SnapshotAsync(replay));
                states++;
            }
        }

        Assert.Equal(16, states);
        output.WriteLine($"{format}: {states} bounded replacement name-survivor states; missing destination remains an explicit replacement-discovery gap.");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Documented1176WithoutNativeBackup_MissingDestinationRetainsBothExplicitCopies(DatabaseFormat format)
    {
        using var directory = new ModelDirectory();
        (byte[] oldBytes, byte[] newBytes) = await ImagesAsync(format);
        CapturedReplacement prepared = await CaptureReplacementAsync(directory.Child("prepared"), oldBytes, newBytes, "prepared");
        string replay = directory.Child("1176");
        string path = ReplayReplacement(prepared, replay, 6);
        string stage = Path.Combine(replay, Path.GetFileName(prepared.Stage));
        Assert.Equal(Digest(newBytes), Digest(await File.ReadAllBytesAsync(stage, Ct)));
        Assert.Equal(Digest(oldBytes), Digest(await File.ReadAllBytesAsync(stage + ".original", Ct)));

        // Microsoft documents 1176 with NULL lpBackupFileName as destination
        // absent and replacement still at its original name. Production passes
        // null to File.Replace and makes its own separately flushed backup.
        // This models that documented outcome; no native 1176 injection occurs.
        Dictionary<string, string> before = await SnapshotAsync(replay);
        await Assert.ThrowsAsync<FileNotFoundException>(() => OpenWriterAsync(path));
        IOException retry = Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(stage, path));
        Assert.Equal(stage, retry.Data[EncryptionManager.ReplacementFileDataKey]);
        Assert.Null(retry.Data[EncryptionReplacementCommit.OriginalFileDataKey]);
        AssertSnapshot(before, await SnapshotAsync(replay));
        output.WriteLine($"{format}: modeled ReplaceFileW1176 NULL-backup outcome retains exact old/new hashes; public open cannot discover either candidate.");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConflictingAndTornReplacementCandidates_DoNotAuthorizeAutomaticSelection(bool tornDestination)
    {
        using var directory = new ModelDirectory();
        (byte[] oldBytes, byte[] newBytes) = await ImagesAsync(DatabaseFormat.AceAccdb);
        CapturedReplacement prepared = await CaptureReplacementAsync(directory.Child("prepared"), oldBytes, newBytes, "prepared");
        string replay = directory.Child("conflicting");
        string path = ReplayReplacement(prepared, replay, tornDestination ? 7 : 6);
        await File.WriteAllBytesAsync(path + ".reenc-second.tmp", oldBytes, Ct);
        await File.WriteAllBytesAsync(path + ".reenc-second.tmp.original", newBytes, Ct);
        await File.WriteAllBytesAsync(path + ".reenc-torn.tmp", newBytes.AsMemory(0, 19).ToArray(), Ct);
        if (tornDestination)
        {
            await File.WriteAllBytesAsync(path, oldBytes.AsMemory(0, 17).ToArray(), Ct);
        }

        Dictionary<string, string> before = await SnapshotAsync(replay);
        await Assert.ThrowsAnyAsync<IOException>(() => OpenWriterAsync(path));
        AssertSnapshot(before, await SnapshotAsync(replay));
    }

    [Theory]
    [InlineData("published-full")]
    [InlineData("published-torn")]
    [InlineData("corrupt-journal")]
    [InlineData("conflicting-content")]
    public async Task ActualJournalNamespaceStates_RecoverOrRefuseBeforeDeletingUnknownCopies(string state)
    {
        using var directory = new ModelDirectory();
        (byte[] oldBytes, byte[] newBytes) = await ImagesAsync(DatabaseFormat.AceAccdb);
        string path = Path.Combine(directory.Path, "database.accdb");
        await PrepareJournalAsync(path, oldBytes, newBytes, state == "published-torn", state == "conflicting-content");
        string journal = PersistentRollbackJournal.JournalPath(path);
        string preparing = journal + ".preparing";
        File.Copy(journal, preparing);
        if (state == "corrupt-journal")
        {
            byte[] damaged = await File.ReadAllBytesAsync(journal, Ct);
            damaged[0] ^= 1;
            await File.WriteAllBytesAsync(journal, damaged, Ct);
        }

        await WriteUnknownCandidatesAsync(path);
        Dictionary<string, string> before = await SnapshotAsync(directory.Path);
        if (state is "corrupt-journal" or "conflicting-content")
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(() => OpenWriterAsync(path));
            Assert.Equal(JetErrorCode.CorruptRecoveryJournal, error.ErrorCode);
            AssertSnapshot(before, await SnapshotAsync(directory.Path));
            return;
        }

        await OpenWriterAsync(path);
        Assert.False(File.Exists(journal));
        before.Remove(Path.GetFileName(journal));
        before.Remove(Path.GetFileName(preparing));
        before[Path.GetFileName(path)] = Digest(oldBytes);
        AssertSnapshot(before, await SnapshotAsync(directory.Path));
        await AssertTableAsync(path, "Original");

        Dictionary<string, string> recovered = await SnapshotAsync(directory.Path);
        await OpenWriterAsync(path);
        AssertSnapshot(recovered, await SnapshotAsync(directory.Path));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LostJournalPromotion_RecoversValidatedPreparationBeforeOpen(bool torn)
    {
        using var directory = new ModelDirectory();
        (byte[] oldBytes, byte[] newBytes) = await ImagesAsync(DatabaseFormat.AceAccdb);
        string path = Path.Combine(directory.Path, "database.accdb");
        await PrepareJournalAsync(path, oldBytes, newBytes, torn, conflict: false);
        string journal = PersistentRollbackJournal.JournalPath(path);
        File.Move(journal, journal + ".preparing");
        await WriteUnknownCandidatesAsync(path);
        Dictionary<string, string> before = await SnapshotAsync(directory.Path);
        await Assert.ThrowsAsync<JetOperationException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        });
        await OpenWriterAsync(path);
        before.Remove(Path.GetFileName(journal + ".preparing"));
        before[Path.GetFileName(path)] = Digest(oldBytes);
        AssertSnapshot(before, await SnapshotAsync(directory.Path));
        await AssertTableAsync(path, "Original");
        await OpenWriterAsync(path);
        AssertSnapshot(before, await SnapshotAsync(directory.Path));
    }

    [Theory]
    [InlineData("survives")]
    [InlineData("cleanup-survives")]
    [InlineData("corrupt-decision")]
    public async Task ActualRecoveryDecision_LostCleanupOrName_RemainsIdempotentOrRefuses(string state)
    {
        using var directory = new ModelDirectory();
        (byte[] oldBytes, byte[] newBytes) = await ImagesAsync(DatabaseFormat.Jet4Mdb);
        string path = Path.Combine(directory.Path, "database.mdb");
        await PrepareJournalAsync(path, oldBytes, newBytes, torn: true, conflict: false);
        await using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            IOException interruption = Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database, static () => throw new IOException("Model interrupts recovery after its durable decision, before cleanup.")));
            Assert.Contains("before cleanup", interruption.Message, StringComparison.Ordinal);
        }

        string journal = PersistentRollbackJournal.JournalPath(path);
        Assert.Equal(Digest(oldBytes), Digest(await File.ReadAllBytesAsync(path, Ct)));
        File.Copy(journal, journal + ".preparing");
        if (state == "cleanup-survives")
        {
            File.Delete(journal);
        }
        else if (state == "corrupt-decision")
        {
            byte[] damaged = await File.ReadAllBytesAsync(journal, Ct);
            damaged[^1] ^= 1;
            await File.WriteAllBytesAsync(journal, damaged, Ct);
        }

        await WriteUnknownCandidatesAsync(path);
        Dictionary<string, string> before = await SnapshotAsync(directory.Path);
        if (state == "corrupt-decision")
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(() => OpenWriterAsync(path));
            Assert.Equal(JetErrorCode.CorruptRecoveryJournal, error.ErrorCode);
            AssertSnapshot(before, await SnapshotAsync(directory.Path));
            return;
        }

        await OpenWriterAsync(path);
        Assert.False(File.Exists(journal));
        before.Remove(Path.GetFileName(journal));
        before.Remove(Path.GetFileName(journal + ".preparing"));
        AssertSnapshot(before, await SnapshotAsync(directory.Path));
        await AssertTableAsync(path, "Original");
        await OpenWriterAsync(path);
        AssertSnapshot(before, await SnapshotAsync(directory.Path));
    }

    private static async Task<(byte[] Original, byte[] Replacement)> ImagesAsync(DatabaseFormat format)
    {
        byte[] original = await ImageAsync(format, "Original", 17);
        byte[] replacement = await ImageAsync(format, "Replacement", 29);
        Assert.Equal(original.Length, replacement.Length);
        Assert.NotEqual(Digest(original), Digest(replacement));
        return (original, replacement);
    }

    private static async Task<byte[]> ImageAsync(DatabaseFormat format, string table, int value)
    {
        await using var image = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(image, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync(table, [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.InsertRowAsync(table, [value], Ct);
        }

        return image.ToArray();
    }

    private static async Task<CapturedReplacement> CaptureReplacementAsync(string directory, byte[] original, byte[] replacement, string boundary)
    {
        string path = Path.Combine(directory, "database.accdb");
        string stage = path + ".reenc-" + Guid.NewGuid().ToString("N") + ".tmp";
        await File.WriteAllBytesAsync(path, original, Ct);
        await File.WriteAllBytesAsync(stage, replacement, Ct);
        await using (var staged = new FileStream(stage, FileMode.Open, FileAccess.ReadWrite))
        {
#pragma warning disable CA1849 // Models a boundary after the production staging device flush.
            staged.Flush(flushToDisk: true);
#pragma warning restore CA1849
        }

        IOException error = Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(
            temporary: stage,
            path: path,
            boundary: observed =>
            {
                if (observed == boundary)
                {
                    throw new IOException("Captured actual replacement boundary: " + boundary);
                }
            }));
        Assert.Equal(stage + ".original", error.Data[EncryptionReplacementCommit.OriginalFileDataKey]);
        Assert.Equal(Digest(original), Digest(await File.ReadAllBytesAsync(stage + ".original", Ct)));
        Assert.Equal(Digest(boundary == "prepared" ? original : replacement), Digest(await File.ReadAllBytesAsync(path, Ct)));
        Assert.Equal(boundary == "prepared", File.Exists(stage));
        return new(path, stage, File.Exists(stage));
    }

    private static string ReplayReplacement(CapturedReplacement captured, string directory, int survivors)
    {
        string path = Path.Combine(directory, Path.GetFileName(captured.Destination));
        if ((survivors & 1) != 0)
        {
            File.Copy(captured.Destination, path);
        }

        if ((survivors & 2) != 0)
        {
            File.Copy(captured.Stage, Path.Combine(directory, Path.GetFileName(captured.Stage)));
        }

        if ((survivors & 4) != 0)
        {
            File.Copy(captured.Stage + ".original", Path.Combine(directory, Path.GetFileName(captured.Stage) + ".original"));
        }

        return path;
    }

    private static async Task WriteUnknownCandidatesAsync(string path)
    {
        await File.WriteAllBytesAsync(path + ".reenc-unrelated.tmp", [0x41, 0x17, 0xFF, 0x23], Ct);
        await File.WriteAllBytesAsync(path + ".reenc-unrelated.tmp.original", [0xA1, 0x58, 0x04], Ct);
    }

    private static async Task PrepareJournalAsync(string path, byte[] original, byte[] replacement, bool torn, bool conflict)
    {
        await File.WriteAllBytesAsync(path, original, Ct);
        int pageSize = JetFormat.PageSizeOf(JetFormat.DetectFormat(original));
        await using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        using var journal = PersistentRollbackJournal.Create(database, pageSize);
        bool changed = false;
        for (int offset = 0; offset < original.Length; offset += pageSize)
        {
            if (original.AsSpan(offset, pageSize).SequenceEqual(replacement.AsSpan(offset, pageSize)))
            {
                continue;
            }

            journal.RecordBeforeWrite(offset, replacement.AsSpan(offset, pageSize));
            database.Position = offset;
            await database.WriteAsync(replacement.AsMemory(offset, torn ? pageSize / 2 : pageSize), Ct);
            changed = true;
        }

        Assert.True(changed);
        if (conflict)
        {
            int offset = Enumerable.Range(0, original.Length).First(index => original[index] != replacement[index]);
            byte unknown = unchecked((byte)(original[offset] + 1));
            if (unknown == replacement[offset])
            {
                unknown = unchecked((byte)(unknown + 1));
            }

            database.Position = offset;
            await database.WriteAsync(new byte[] { unknown }, Ct);
        }

#pragma warning disable CA1849 // Models closed, flushed raw old/intended/mixed pages before namespace survival is selected.
        database.Flush(flushToDisk: true);
#pragma warning restore CA1849
    }

    private static async Task OpenWriterAsync(string path)
    {
        await using AccessWriter writer = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false }, Ct);
    }

    private static async Task AssertTableAsync(string path, string expected)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Equal(expected, Assert.Single(await reader.ListTablesAsync(Ct)));
    }

    private static async Task<Dictionary<string, string>> SnapshotAsync(string directory)
    {
        var files = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (string path in Directory.GetFiles(directory))
        {
            files.Add(Path.GetFileName(path), Digest(await File.ReadAllBytesAsync(path, Ct)));
        }

        return files;
    }

    private static void AssertSnapshot(Dictionary<string, string> expected, Dictionary<string, string> actual)
    {
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), actual.Keys.Order(StringComparer.Ordinal));
        foreach (KeyValuePair<string, string> file in expected)
        {
            Assert.Equal(file.Value, actual[file.Key]);
        }
    }

    private static string Digest(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));

    private sealed record CapturedReplacement(string Destination, string Stage, bool StageExists);

    private sealed class ModelDirectory : IDisposable
    {
        internal ModelDirectory()
        {
            this.Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "jdw-replacement-namespace-model-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(this.Path);
        }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(this.Path, recursive: true);

        internal string Child(string name)
        {
            string path = System.IO.Path.Combine(this.Path, name);
            Directory.CreateDirectory(path);
            return path;
        }
    }
}
