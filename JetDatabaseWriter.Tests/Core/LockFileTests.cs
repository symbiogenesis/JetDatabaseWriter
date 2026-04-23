namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Tests for optional lockfile handling (.ldb / .laccdb).
/// Lockfile creation is enabled by default. When enabled, a lockfile is created
/// alongside the database on open and deleted on dispose. When disabled, no
/// lockfile is created or deleted.
/// </summary>
public sealed class LockFileTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    // ── Reader: UseLockFile = true (default) ──────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_DefaultOptions_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        await using var reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_DefaultOptions_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);
        await reader.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileTrue_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = true };

        await using var reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileTrue_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = true };

        var reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);
        await reader.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    // ── Reader: UseLockFile = false ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileFalse_DoesNotCreateLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = false };

        await using var reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.False(File.Exists(lockPath), $"No lockfile should exist when UseLockFile=false: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Reader_UseLockFileFalse_DoesNotDeletePreExistingLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to ensure it is NOT deleted when UseLockFile=false
        await File.WriteAllTextAsync(lockPath, "pre-existing", TestContext.Current.CancellationToken);
        _tempFiles.Add(lockPath);

        var options = new AccessReaderOptions { UseLockFile = false };
        var reader = await AccessReader.OpenAsync(temp, options, TestContext.Current.CancellationToken);
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

        string temp = CopyToTemp(mdbPath);
        string lockPath = Path.ChangeExtension(temp, ".ldb");

        await using var reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

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

        string temp = CopyToTemp(accdbPath);
        string lockPath = Path.ChangeExtension(temp, ".laccdb");

        await using var reader = await AccessReader.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Expected .laccdb lockfile for .accdb database");
    }

    // ── Writer: UseLockFile = true (default) ──────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_DefaultOptions_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        await using var writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_DefaultOptions_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileTrue_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = true };

        await using var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileTrue_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = true };

        var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    // ── Writer: UseLockFile = false ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileFalse_DoesNotCreateLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = false };

        await using var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.False(File.Exists(lockPath), $"No lockfile should exist when UseLockFile=false: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_UseLockFileFalse_DoesNotDeletePreExistingLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to ensure it is NOT deleted when UseLockFile=false
        await File.WriteAllTextAsync(lockPath, "pre-existing", TestContext.Current.CancellationToken);
        _tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = false };
        var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
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

        string temp = CopyToTemp(mdbPath);
        string lockPath = Path.ChangeExtension(temp, ".ldb");

        await using var writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

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

        string temp = CopyToTemp(accdbPath);
        string lockPath = Path.ChangeExtension(temp, ".laccdb");

        await using var writer = await AccessWriter.OpenAsync(temp, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Expected .laccdb lockfile for .accdb database");
    }

    // ── Writer: double-dispose does not throw ─────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_DisposeTwice_WithLockFile_DoesNotThrow(string path)
    {
        string temp = CopyToTemp(path);
        var options = new AccessWriterOptions { UseLockFile = false };
        var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        await writer.DisposeAsync();
        var ex = await Record.ExceptionAsync(async () => await writer.DisposeAsync());

        Assert.Null(ex);
    }

    // ── Writer: RespectExistingLockFile ───────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFileTrue_ThrowsWhenLockFileExists(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to simulate another process
        await File.WriteAllTextAsync(lockPath, "in-use", TestContext.Current.CancellationToken);
        _tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = true };

        await Assert.ThrowsAsync<IOException>(async () => await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFileFalse_OverwritesExistingLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to simulate another process
        await File.WriteAllTextAsync(lockPath, "in-use", TestContext.Current.CancellationToken);
        _tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = false };

        await using var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);

        Assert.True(File.Exists(lockPath), "Lockfile should still exist after overwrite");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Writer_RespectExistingLockFileTrue_ThrowsWhenLockPathIsDirectory(string path)
    {
        string temp = CopyToTemp(path);
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
                ex is IOException || ex is UnauthorizedAccessException,
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
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        Directory.CreateDirectory(lockPath);

        try
        {
            var options = new AccessWriterOptions
            {
                UseLockFile = true,
                RespectExistingLockFile = false,
            };

            await using var writer = await AccessWriter.OpenAsync(temp, options, TestContext.Current.CancellationToken);
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

    // ── Helpers ───────────────────────────────────────────────────────

    public void Dispose()
    {
        foreach (string f in _tempFiles)
        {
            try
            {
                File.Delete(f);
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

        string[] files = Directory.GetFiles(dbDir, $"*{extension}");
        foreach (string f in files)
        {
            if (await TestDatabases.IsReadableAsync(f))
            {
                return f;
            }
        }

        return null;
    }

    private string CopyToTemp(string sourcePath)
    {
        string ext = Path.GetExtension(sourcePath);
        string temp = Path.Combine(Path.GetTempPath(), $"JetLockTest_{Guid.NewGuid():N}{ext}");
        File.Copy(sourcePath, temp, overwrite: true);
        _tempFiles.Add(temp);
        return temp;
    }
}
