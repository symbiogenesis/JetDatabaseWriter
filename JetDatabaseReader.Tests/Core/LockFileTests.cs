namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

/// <summary>
/// TDD tests for optional lockfile handling (.ldb / .laccdb).
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
    public void Reader_DefaultOptions_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        using var reader = AccessReader.Open(temp);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Reader_DefaultOptions_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var reader = AccessReader.Open(temp);
        reader.Dispose();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Reader_UseLockFileTrue_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = true };

        using var reader = AccessReader.Open(temp, options);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Reader_UseLockFileTrue_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = true };

        var reader = AccessReader.Open(temp, options);
        reader.Dispose();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    // ── Reader: UseLockFile = false ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Reader_UseLockFileFalse_DoesNotCreateLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessReaderOptions { UseLockFile = false };

        using var reader = AccessReader.Open(temp, options);

        Assert.False(File.Exists(lockPath), $"No lockfile should exist when UseLockFile=false: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Reader_UseLockFileFalse_DoesNotDeletePreExistingLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to ensure it is NOT deleted when UseLockFile=false
        File.WriteAllText(lockPath, "pre-existing");
        _tempFiles.Add(lockPath);

        var options = new AccessReaderOptions { UseLockFile = false };
        var reader = AccessReader.Open(temp, options);
        reader.Dispose();

        Assert.True(File.Exists(lockPath), "Pre-existing lockfile should not be deleted when UseLockFile=false");
    }

    // ── Reader: lockfile extension ────────────────────────────────────

    [Fact]
    public void Reader_MdbFile_CreatesLdbLockFile()
    {
        string? mdbPath = FindFirstExisting(".mdb");
        if (mdbPath == null)
        {
            return; // skip if no .mdb test database
        }

        string temp = CopyToTemp(mdbPath);
        string lockPath = Path.ChangeExtension(temp, ".ldb");

        using var reader = AccessReader.Open(temp);

        Assert.True(File.Exists(lockPath), "Expected .ldb lockfile for .mdb database");
    }

    [Fact]
    public void Reader_AccdbFile_CreatesLaccdbLockFile()
    {
        string? accdbPath = FindFirstExisting(".accdb");
        if (accdbPath == null)
        {
            return; // skip if no .accdb test database
        }

        string temp = CopyToTemp(accdbPath);
        string lockPath = Path.ChangeExtension(temp, ".laccdb");

        using var reader = AccessReader.Open(temp);

        Assert.True(File.Exists(lockPath), "Expected .laccdb lockfile for .accdb database");
    }

    // ── Writer: UseLockFile = true (default) ──────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_DefaultOptions_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        using var writer = AccessWriter.Open(temp);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_DefaultOptions_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        var writer = AccessWriter.Open(temp);
        writer.Dispose();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_UseLockFileTrue_CreatesLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = true };

        using var writer = AccessWriter.Open(temp, options);

        Assert.True(File.Exists(lockPath), $"Expected lockfile at {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_UseLockFileTrue_DeletesLockFileOnDispose(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = true };

        var writer = AccessWriter.Open(temp, options);
        writer.Dispose();

        Assert.False(File.Exists(lockPath), $"Lockfile should be deleted after dispose: {lockPath}");
    }

    // ── Writer: UseLockFile = false ───────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_UseLockFileFalse_DoesNotCreateLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);
        var options = new AccessWriterOptions { UseLockFile = false };

        using var writer = AccessWriter.Open(temp, options);

        Assert.False(File.Exists(lockPath), $"No lockfile should exist when UseLockFile=false: {lockPath}");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_UseLockFileFalse_DoesNotDeletePreExistingLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to ensure it is NOT deleted when UseLockFile=false
        File.WriteAllText(lockPath, "pre-existing");
        _tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = false };
        var writer = AccessWriter.Open(temp, options);
        writer.Dispose();

        Assert.True(File.Exists(lockPath), "Pre-existing lockfile should not be deleted when UseLockFile=false");
    }

    // ── Writer: lockfile extension ────────────────────────────────────

    [Fact]
    public void Writer_MdbFile_CreatesLdbLockFile()
    {
        string? mdbPath = FindFirstExisting(".mdb");
        if (mdbPath == null)
        {
            return; // skip if no .mdb test database
        }

        string temp = CopyToTemp(mdbPath);
        string lockPath = Path.ChangeExtension(temp, ".ldb");

        using var writer = AccessWriter.Open(temp);

        Assert.True(File.Exists(lockPath), "Expected .ldb lockfile for .mdb database");
    }

    [Fact]
    public void Writer_AccdbFile_CreatesLaccdbLockFile()
    {
        string? accdbPath = FindFirstExisting(".accdb");
        if (accdbPath == null)
        {
            return; // skip if no .accdb test database
        }

        string temp = CopyToTemp(accdbPath);
        string lockPath = Path.ChangeExtension(temp, ".laccdb");

        using var writer = AccessWriter.Open(temp);

        Assert.True(File.Exists(lockPath), "Expected .laccdb lockfile for .accdb database");
    }

    // ── Writer: double-dispose does not throw ─────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_DisposeTwice_WithLockFile_DoesNotThrow(string path)
    {
        string temp = CopyToTemp(path);
        var options = new AccessWriterOptions { UseLockFile = false };
        var writer = AccessWriter.Open(temp, options);

        writer.Dispose();
        var ex = Record.Exception(() => writer.Dispose());

        Assert.Null(ex);
    }

    // ── Writer: RespectExistingLockFile ───────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_RespectExistingLockFileTrue_ThrowsWhenLockFileExists(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to simulate another process
        File.WriteAllText(lockPath, "in-use");
        _tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = true };

        Assert.Throws<IOException>(() => AccessWriter.Open(temp, options));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_RespectExistingLockFileFalse_OverwritesExistingLockFile(string path)
    {
        string temp = CopyToTemp(path);
        string lockPath = GetExpectedLockPath(temp);

        // Pre-create a lockfile to simulate another process
        File.WriteAllText(lockPath, "in-use");
        _tempFiles.Add(lockPath);

        var options = new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = false };

        using var writer = AccessWriter.Open(temp, options);

        Assert.True(File.Exists(lockPath), "Lockfile should still exist after overwrite");
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Writer_RespectExistingLockFileTrue_ThrowsWhenLockPathIsDirectory(string path)
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

            Exception ex = Assert.ThrowsAny<Exception>(() => AccessWriter.Open(temp, options));
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
    public void Writer_RespectExistingLockFileFalse_ContinuesWhenLockPathIsDirectory(string path)
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

            using var writer = AccessWriter.Open(temp, options);
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

    private static string? FindFirstExisting(string extension)
    {
        string dbDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases");
        if (!Directory.Exists(dbDir))
        {
            return null;
        }

        string[] files = Directory.GetFiles(dbDir, $"*{extension}");
        foreach (string f in files)
        {
            if (TestDatabases.IsReadable(f))
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
