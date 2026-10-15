namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Test factories preserve the production facades' resource ownership.</summary>
public sealed class TestDatabaseTests
{
    /// <summary>A reader inspection session participates in the requested lock-file lifecycle.</summary>
    [Fact]
    public async Task Reader_UseLockFile_AcquiresAndReleasesSlot()
    {
        string path = Path.Combine(Path.GetTempPath(), $"TestDatabaseTests_{Guid.NewGuid():N}.accdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        try
        {
            await CreateDatabaseAsync(path);
            await using (AccessReader reader = await TestDatabase.OpenReaderAsync(
                path,
                new AccessReaderOptions { UseLockFile = true },
                TestContext.Current.CancellationToken))
            {
                Assert.True(File.Exists(lockPath));
            }

            Assert.False(File.Exists(lockPath));
        }
        finally
        {
            File.Delete(lockPath);
            File.Delete(path);
        }
    }

    /// <summary>A writer inspection session participates in the requested lock-file lifecycle.</summary>
    [Fact]
    public async Task Writer_UseLockFile_AcquiresAndReleasesSlot()
    {
        string path = Path.Combine(Path.GetTempPath(), $"TestDatabaseTests_{Guid.NewGuid():N}.accdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        try
        {
            await CreateDatabaseAsync(path);
            await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(
                path,
                new AccessWriterOptions { UseLockFile = true },
                TestContext.Current.CancellationToken))
            {
                Assert.True(File.Exists(lockPath));
            }

            Assert.False(File.Exists(lockPath));
        }
        finally
        {
            File.Delete(lockPath);
            File.Delete(path);
        }
    }

    /// <summary>A failed reader open disposes only a stream whose ownership was transferred.</summary>
    /// <param name="leaveOpen">Whether the caller retains ownership of the stream.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Reader_TruncatedHeader_RespectsStreamOwnership(bool leaveOpen)
    {
        await using var stream = new MemoryStream();

        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
        {
            await using AccessReader reader = await TestDatabase.OpenReaderAsync(
                stream,
                leaveOpen: leaveOpen,
                cancellationToken: TestContext.Current.CancellationToken);
        });

        Assert.Equal(leaveOpen, stream.CanRead);
    }

    /// <summary>A failed writer open disposes only a stream whose ownership was transferred.</summary>
    /// <param name="leaveOpen">Whether the caller retains ownership of the stream.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Writer_TruncatedHeader_RespectsStreamOwnership(bool leaveOpen)
    {
        await using var stream = new MemoryStream();

        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
        {
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(
                stream,
                leaveOpen: leaveOpen,
                cancellationToken: TestContext.Current.CancellationToken);
        });

        Assert.Equal(leaveOpen, stream.CanRead);
    }

    private static async Task CreateDatabaseAsync(string path)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            path,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);
    }
}
