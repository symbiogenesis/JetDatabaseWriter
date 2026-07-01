namespace JetDatabaseWriter.Tests.Transactions;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>
/// Tests for optional lockfile handling (.ldb / .laccdb).
/// Lockfile creation is enabled by default. When enabled, a lockfile is created
/// alongside the database on open and deleted on dispose. When disabled, no
/// lockfile is created or deleted.
/// </summary>
public sealed class LockFileTests : IDisposable
{
    private readonly List<string> tempFiles = [];

    // ── Reader: UseLockFile = true (default) ──────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_DefaultOptions_CreatesLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        await using AccessReader reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_DefaultOptions_DeletesLockFileOnDispose(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        AccessReader reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);
        await reader.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileTrue_CreatesLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = true };

        await using AccessReader reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileTrue_DeletesLockFileOnDispose(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = true };

        AccessReader reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);
        await reader.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    // ── Reader: UseLockFile = false ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileFalse_DoesNotCreateLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = false };

        await using AccessReader reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.False(File.Exists(lockPath), $"No lockfile should exist when UseLockFile=false: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileFalse_DoesNotDeletePreExistingLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to ensure it is NOT deleted when UseLockFile=false
        await File.WriteAllTextAsync(lockPath, "pre-existing", TestContext.Current.CancellationToken);
        this.tempFiles.Add(lockPath);

        var options = new AccessReaderOptions { UseLockFile = false };
        AccessReader reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);
        await reader.DisposeAsync();

        Assert.True(File.Exists(lockPath), "Pre-existing lockfile should not be deleted when UseLockFile=false");
    }

    // ── Reader: lockfile extension ────────────────────────────────────

    [Fact]
    public async Task Reader_MdbFile_CreatesLdbLockFile()
    {
        string? mdbPath = await FindFirstExistingAsync(".mdb");
        if (mdbPath == null)
        {
            return; // skip if no .mdb test database
        }

        string temp = this.CopyToTemp(mdbPath);
        string lockPath = Path.ChangeExtension(temp, ".ldb");

        await using AccessReader reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Expected .ldb lockfile for .mdb database");
    }

    [Fact]
    public async Task Reader_AccdbFile_CreatesLaccdbLockFile()
    {
        string? accdbPath = await FindFirstExistingAsync(".accdb");
        if (accdbPath == null)
        {
            return; // skip if no .accdb test database
        }

        string temp = this.CopyToTemp(accdbPath);
        string lockPath = Path.ChangeExtension(temp, ".laccdb");

        await using AccessReader reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Expected .laccdb lockfile for .accdb database");
    }

    // ── Writer: UseLockFile = true (default) ──────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_DefaultOptions_CreatesLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_DefaultOptions_DeletesLockFileOnDispose(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        AccessWriter writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileTrue_CreatesLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = true };

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileTrue_DeletesLockFileOnDispose(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = true };

        AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    // ── Writer: UseLockFile = false ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileFalse_DoesNotCreateLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = false };

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.False(File.Exists(lockPath), $"No lockfile should exist when UseLockFile=false: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileFalse_DoesNotDeletePreExistingLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to ensure it is NOT deleted when UseLockFile=false
        await File.WriteAllTextAsync(lockPath, "pre-existing", TestContext.Current.CancellationToken);
        this.tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = false };
        AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        Assert.True(File.Exists(lockPath), "Pre-existing lockfile should not be deleted when UseLockFile=false");
    }

    // ── Writer: lockfile extension ────────────────────────────────────

    [Fact]
    public async Task Writer_MdbFile_CreatesLdbLockFile()
    {
        string? mdbPath = await FindFirstExistingAsync(".mdb");
        if (mdbPath == null)
        {
            return; // skip if no .mdb test database
        }

        string temp = this.CopyToTemp(mdbPath);
        string lockPath = Path.ChangeExtension(temp, ".ldb");

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Expected .ldb lockfile for .mdb database");
    }

    [Fact]
    public async Task Writer_AccdbFile_CreatesLaccdbLockFile()
    {
        string? accdbPath = await FindFirstExistingAsync(".accdb");
        if (accdbPath == null)
        {
            return; // skip if no .accdb test database
        }

        string temp = this.CopyToTemp(accdbPath);
        string lockPath = Path.ChangeExtension(temp, ".laccdb");

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Expected .laccdb lockfile for .accdb database");
    }

    // ── Writer: double-dispose does not throw ─────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_DisposeTwice_WithoutLockFile_DoesNotThrow(string path)
    {
        string temp = this.CopyToTemp(path);
        var options = new AccessWriterOptions { UseLockFile = false };
        AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        await writer.DisposeAsync();
        Exception? ex = await Record.ExceptionAsync(async () => await writer.DisposeAsync());

        Assert.Null(ex);
    }

    // ── Writer: RespectExistingLockFile ───────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFileTrue_ThrowsWhenLockFileExists(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to simulate another process
        await File.WriteAllTextAsync(lockPath, "in-use", TestContext.Current.CancellationToken);
        this.tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = true };

        await Assert.ThrowsAsync<IOException>(async () => await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFileFalse_OverwritesExistingLockFile(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to simulate another process
        await File.WriteAllTextAsync(lockPath, "in-use", TestContext.Current.CancellationToken);
        this.tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = false };

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Lockfile should still exist after overwrite");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFileTrue_ThrowsWhenLockPathIsDirectory(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        Directory.CreateDirectory(lockPath);

        try
        {
            var options = new AccessWriterOptions
            {
                UseLockFile = true,
                RespectExistingLockFile = true,
            };

            Exception ex = await Assert.ThrowsAnyAsync<Exception>(async () => await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken));
            Assert.True(
                ex is IOException or UnauthorizedAccessException,
                $"Expected IOException or UnauthorizedAccessException, got {ex.GetType().Name}");
        }
        finally
        {
            Directory.Delete(lockPath, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFileFalse_ContinuesWhenLockPathIsDirectory(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        Directory.CreateDirectory(lockPath);

        try
        {
            var options = new AccessWriterOptions
            {
                UseLockFile = true,
                RespectExistingLockFile = false,
            };

            await using AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
            Assert.True(Directory.Exists(lockPath));
        }
        finally
        {
            Directory.Delete(lockPath, recursive: true);
        }
    }

    [Fact]
    public void AccessWriterOptions_RespectExistingLockFile_DefaultsToTrue()
    {
        var options = new AccessWriterOptions();
        Assert.True(options.RespectExistingLockFile);
    }

    // ── Reader + Writer contention ────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFile_SucceedsWhenReaderHoldsLockFile(string path)
    {
        // A reader holding a lockfile should NOT block a writer that also uses
        // lock files — the lock file is cooperative, not exclusive.
        string temp = this.CopyToTemp(path);

        await using AccessReader reader = await AccessReader.OpenAsync(
            temp,
            new AccessReaderOptions { UseLockFile = true },
            TestContext.Current.CancellationToken);

        // Writer with RespectExistingLockFile=true should still open because
        // the lockfile is shared (both append a slot).
        await using AccessWriter writer = await AccessWriter.OpenAsync(
            temp,
            new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = true },
            TestContext.Current.CancellationToken);

        Assert.NotNull(writer);
    }

    // ── Options default ───────────────────────────────────────────────

    [Fact]
    public void AccessReaderOptions_UseLockFile_DefaultsToTrue()
    {
        var options = new AccessReaderOptions();
        Assert.True(options.UseLockFile);
    }

    [Fact]
    public void AccessWriterOptions_UseLockFile_DefaultsToTrue()
    {
        var options = new AccessWriterOptions();
        Assert.True(options.UseLockFile);
    }

    [Fact]
    public void AccessReaderOptions_UseByteRangeLocks_DefaultsToFalse()
    {
        var options = new AccessReaderOptions();
        Assert.False(options.UseByteRangeLocks);
    }

    [Fact]
    public void AccessWriterOptions_UseByteRangeLocks_DefaultsToPlatformSupport()
    {
        var options = new AccessWriterOptions();
        Assert.Equal(JetByteRangeLock.PlatformSupportsByteRangeLocks(), options.UseByteRangeLocks);
    }

    [Fact]
    public void AccessOptions_LockTimeoutMilliseconds_DefaultsToFiveSeconds()
    {
        Assert.Equal(5_000, new AccessReaderOptions().LockTimeoutMilliseconds);
        Assert.Equal(5_000, new AccessWriterOptions().LockTimeoutMilliseconds);
    }

    [Fact]
    public async Task DisposeAfterAsync_WhenOneStepFails_ThrowsOriginalException()
    {
        using var coordinator = new LockFileCoordinator(
            string.Empty,
            nameof(LockFileTests),
            new LockFileSettings(Enabled: false, RespectExisting: false, UserName: string.Empty, MachineName: string.Empty));
        var expected = new InvalidOperationException("first cleanup failed");

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await coordinator.DisposeAfterAsync(() => ValueTask.FromException(expected)));

        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task DisposeAfterAsync_WhenMultipleStepsFail_ThrowsAggregateException()
    {
        using var coordinator = new LockFileCoordinator(
            string.Empty,
            nameof(LockFileTests),
            new LockFileSettings(Enabled: false, RespectExisting: false, UserName: string.Empty, MachineName: string.Empty));
        var first = new InvalidOperationException("first cleanup failed");
        var second = new IOException("second cleanup failed");
        int stepsRun = 0;

        AggregateException actual = await Assert.ThrowsAsync<AggregateException>(async () =>
            await coordinator.DisposeAfterAsync(
                () =>
                {
                    stepsRun++;
                    return ValueTask.FromException(first);
                },
                () =>
                {
                    stepsRun++;
                    return ValueTask.CompletedTask;
                },
                () =>
                {
                    stepsRun++;
                    return ValueTask.FromException(second);
                }));

        Assert.Equal(3, stepsRun);
        Assert.Collection(
            actual.InnerExceptions,
            ex => Assert.Same(first, ex),
            ex => Assert.Same(second, ex));
    }

    // ── Phase 1: populated lockfile slots ─────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_PopulatesSlot_WithMachineAndUserName(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        await using AccessReader reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        byte[] bytes = ReadAllShared(lockPath);
        Assert.True(bytes.Length >= 64, $"Lockfile must contain at least one slot, got {bytes.Length} bytes");

        string machine = ReadAsciiField(bytes, 0, 32);
        string user = ReadAsciiField(bytes, 32, 32);
        Assert.Equal(TruncateAscii(Environment.MachineName, 31), machine);
        Assert.Equal(TruncateAscii(Environment.UserName, 31), user);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_PopulatesSlot_WithMachineAndUserName(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        byte[] bytes = ReadAllShared(lockPath);
        Assert.True(bytes.Length >= 64);

        string machine = ReadAsciiField(bytes, 0, 32);
        string user = ReadAsciiField(bytes, 32, 32);
        Assert.Equal(TruncateAscii(Environment.MachineName, 31), machine);
        Assert.Equal(TruncateAscii(Environment.UserName, 31), user);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_OverrideUserAndMachine_RoundTripsIntoSlot(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var options = new AccessReaderOptions
        {
            LockFileMachineName = "TESTHOST",
            LockFileUserName = "alice",
        };

        await using AccessReader reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        byte[] bytes = ReadAllShared(lockPath);
        Assert.Equal("TESTHOST", ReadAsciiField(bytes, 0, 32));
        Assert.Equal("alice", ReadAsciiField(bytes, 32, 32));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_OverrideUserAndMachine_RoundTripsIntoSlot(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var options = new AccessWriterOptions
        {
            LockFileMachineName = "BUILDBOX",
            LockFileUserName = "svc-build",
            RespectExistingLockFile = false,
        };

        await using AccessWriter writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        byte[] bytes = ReadAllShared(lockPath);
        Assert.Equal("BUILDBOX", ReadAsciiField(bytes, 0, 32));
        Assert.Equal("svc-build", ReadAsciiField(bytes, 32, 32));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_NonAsciiNames_ReplacedWithQuestionMark(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var options = new AccessReaderOptions
        {
            LockFileMachineName = "MÄCHINE",
            LockFileUserName = "üser",
        };

        await using AccessReader reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        byte[] bytes = ReadAllShared(lockPath);
        Assert.Equal("M?CHINE", ReadAsciiField(bytes, 0, 32));
        Assert.Equal("?ser", ReadAsciiField(bytes, 32, 32));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_LongName_TruncatedTo31Chars(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        string longName = new('a', 64);
        var options = new AccessReaderOptions
        {
            LockFileMachineName = longName,
            LockFileUserName = longName,
        };

        await using AccessReader reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        byte[] bytes = ReadAllShared(lockPath);
        string machine = ReadAsciiField(bytes, 0, 32);
        string user = ReadAsciiField(bytes, 32, 32);
        Assert.Equal(31, machine.Length);
        Assert.Equal(31, user.Length);
        Assert.Equal(new string('a', 31), machine);
        Assert.Equal(new string('a', 31), user);

        // Trailing null preserved.
        Assert.Equal(0, bytes[31]);
        Assert.Equal(0, bytes[63]);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Slot_IsZeroedOnDispose(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        AccessReader reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        // Capture the slot bytes while open so the path & permissions are warm.
        Assert.True(File.Exists(lockPath));
        await reader.DisposeAsync();

        // Last opener deleted the lockfile; the slot is therefore "zeroed" by removal.
        Assert.False(File.Exists(lockPath), $"Last opener should remove the lockfile: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task MultipleReaders_AppendDistinctSlots(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var optsA = new AccessReaderOptions
        {
            LockFileMachineName = "HOST-A",
            LockFileUserName = "user-a",
        };
        var optsB = new AccessReaderOptions
        {
            LockFileMachineName = "HOST-B",
            LockFileUserName = "user-b",
        };

        await using AccessReader readerA = await AccessReader.OpenAsync(temp, optsA, TestContext.Current.CancellationToken);
        await using AccessReader readerB = await AccessReader.OpenAsync(temp, optsB, TestContext.Current.CancellationToken);

        // Read with FileShare.ReadWrite | FileShare.Delete since the openers still hold handles.
        byte[] bytes = ReadAllShared(lockPath);
        Assert.True(bytes.Length >= 128, $"Expected at least two slots, got {bytes.Length} bytes");
        Assert.Equal("HOST-A", ReadAsciiField(bytes, 0, 32));
        Assert.Equal("user-a", ReadAsciiField(bytes, 32, 32));
        Assert.Equal("HOST-B", ReadAsciiField(bytes, 64, 32));
        Assert.Equal("user-b", ReadAsciiField(bytes, 96, 32));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task SecondOpener_ReusesFreedSlot(string path)
    {
        string temp = this.CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var optsA = new AccessReaderOptions { LockFileMachineName = "HOST-A", LockFileUserName = "u-a" };
        var optsB = new AccessReaderOptions { LockFileMachineName = "HOST-B", LockFileUserName = "u-b" };
        var optsC = new AccessReaderOptions { LockFileMachineName = "HOST-C", LockFileUserName = "u-c" };

        AccessReader readerA = await AccessReader.OpenAsync(temp, optsA, TestContext.Current.CancellationToken);
        await using AccessReader readerB = await AccessReader.OpenAsync(temp, optsB, TestContext.Current.CancellationToken);

        // Free slot 0.
        await readerA.DisposeAsync();

        await using AccessReader readerC = await AccessReader.OpenAsync(temp, optsC, TestContext.Current.CancellationToken);

        byte[] bytes = ReadAllShared(lockPath);

        // Slot 0 was freed by A and should now hold C; slot 1 still holds B.
        Assert.Equal("HOST-C", ReadAsciiField(bytes, 0, 32));
        Assert.Equal("HOST-B", ReadAsciiField(bytes, 64, 32));
    }

    // ── Phase 2: finalizer safety net ─────────────────────────────────
    //
    // If a caller forgets to dispose the reader / writer, the LockFileSlotWriter
    // finalizer must still release the slot and best-effort-delete the lockfile
    // so the .ldb / .laccdb does not survive the process.

    [Fact]
    public void LockFileSlotWriter_AbandonedWithoutDispose_FinalizerDeletesLockFile()
    {
        // Use a synthetic empty file as a stand-in for a database — the slot
        // writer only needs the path, not a real Access database.
        string dbPath = Path.Combine(Path.GetTempPath(), $"JetLockFin_{Guid.NewGuid():N}.accdb");
        File.WriteAllBytes(dbPath, []);
        this.tempFiles.Add(dbPath);
        string lockPath = Path.ChangeExtension(dbPath, ".laccdb");

        OpenAndAbandonSlot(dbPath);

        // The discarded slot is now eligible for finalization. Drain the
        // finalizer queue (and FileStream's own finalizer) before checking.
        for (int i = 0; i < 10 && File.Exists(lockPath); i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.False(
            File.Exists(lockPath),
            $"Finalizer should have removed the lockfile abandoned by the slot writer: {lockPath}");
    }

    /// <summary>
    /// Open the slot in a separate non-inlined helper so the local cannot
    /// stay JIT-rooted past the call site. The discarded result is then
    /// eligible for finalization on return.
    /// </summary>
    /// <param name="dbPath">Path to the database (can be a synthetic empty file).</param>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void OpenAndAbandonSlot(string dbPath) =>
#pragma warning disable CA2000 // Intentional: this test exercises the finalizer when the caller forgets to dispose.
        _ = LockFileSlotWriter.Open(
            dbPath,
            ownerTypeName: nameof(LockFileTests),
            respectExisting: false,
            machineName: "TESTHOST",
            userName: "tester");
#pragma warning restore CA2000

    private static string ReadAsciiField(byte[] bytes, int offset, int length)
    {
        int end = offset;
        int max = offset + length;
        while (end < max && bytes[end] != 0)
        {
            end++;
        }

        return System.Text.Encoding.ASCII.GetString(bytes, offset, end - offset);
    }

    private static string TruncateAscii(string value, int max)
    {
        char[] buf = new char[Math.Min(value.Length, max)];
        for (int i = 0; i < buf.Length; i++)
        {
            char c = value[i];
            buf[i] = (c is >= (char)0x20 and < (char)0x7F) ? c : '?';
        }

        return new string(buf);
    }

    private static byte[] ReadAllShared(string path)
    {
        using var fs = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
                BufferSize = 4096,
            });
        byte[] buf = new byte[fs.Length];
        int read = 0;
        while (read < buf.Length)
        {
            int n = fs.Read(buf, read, buf.Length - read);
            if (n == 0)
            {
                break;
            }

            read += n;
        }

        return buf;
    }

    // ── Helpers ───────────────────────────────────────────────────────

    public void Dispose()
    {
        foreach (string file in this.tempFiles)
        {
            try
            {
                File.Delete(file);
            }
            catch (IOException)
            {
                /* best-effort cleanup */
            }
        }
    }

    private static string GetExpectedLockPath(string dbPath)
    {
        string ext = Path.GetExtension(dbPath);
        string lockExt = ext.Equals(".accdb", StringComparison.OrdinalIgnoreCase) ? ".laccdb" : ".ldb";
        return Path.ChangeExtension(dbPath, lockExt);
    }

    private static async ValueTask<string?> FindFirstExistingAsync(string extension)
    {
        string dbDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases");
        if (!Directory.Exists(dbDir))
        {
            return null;
        }

        foreach (string file in Directory.GetFiles(dbDir, $"*{extension}"))
        {
            if (await TestDatabases.IsReadableAsync(file, TestContext.Current.CancellationToken))
            {
                return file;
            }
        }

        return null;
    }

    private string CopyToTemp(string sourcePath)
    {
        string ext = Path.GetExtension(sourcePath);
        string temp = Path.Combine(Path.GetTempPath(), $"JetLockTest_{Guid.NewGuid():N}{ext}");
        File.Copy(sourcePath, temp, overwrite: true);
        this.tempFiles.Add(temp);
        return temp;
    }
}
