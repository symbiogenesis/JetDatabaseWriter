namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using Microsoft.EntityFrameworkCore;
using Xunit;

#pragma warning disable EF1001 // These regressions inspect the options passed from EF to the core library.
public sealed class JetOptionsForwardingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Connection_Preserves_Core_Resource_Budgets_And_Recovery_Adapter(bool isStream)
    {
        await using MemoryStream stream = new();
        RecoveryAdapter adapter = new();
        AccessReaderOptions readerTemplate = new()
        {
            MaxActivePageLeases = 37,
            MaxLivePageFrames = 53,
            MaxCachedRowDirectoryEntries = 71,
            PersistentStreamRecoveryAdapter = adapter,
        };
        AccessWriterOptions writerTemplate = new()
        {
            MaxActivePageLeases = 43,
            MaxLivePageFrames = 59,
            MaxCachedRowDirectoryEntries = 73,
            PersistentStreamRecoveryAdapter = adapter,
            UseTransactionalWrites = false,
        };
        DbContextOptionsBuilder options = new();
        if (isStream)
        {
            _ = options.UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, jetOptionsAction: jet => jet.WithReaderOptions(readerTemplate).WithWriterOptions(writerTemplate));
        }
        else
        {
            _ = options.UseJetDatabaseWriter(Path.Combine(Path.GetTempPath(), $"jdw-ef-options-{Guid.NewGuid():N}.accdb"), DatabaseFormat.AceAccdb, jet => jet.WithReaderOptions(readerTemplate).WithWriterOptions(writerTemplate));
        }

        await using JetClientConnection connection = new(options.Options);
        AccessReaderOptions reader = connection.BuildReaderOptions();
        AccessWriterOptions writer = connection.BuildWriterOptions();

        Assert.NotSame(readerTemplate, reader);
        Assert.NotSame(writerTemplate, writer);
        Assert.Equal(37, reader.MaxActivePageLeases);
        Assert.Equal(53, reader.MaxLivePageFrames);
        Assert.Equal(71, reader.MaxCachedRowDirectoryEntries);
        Assert.Same(adapter, reader.PersistentStreamRecoveryAdapter);
        Assert.Equal(43, writer.MaxActivePageLeases);
        Assert.Equal(59, writer.MaxLivePageFrames);
        Assert.Equal(73, writer.MaxCachedRowDirectoryEntries);
        Assert.Same(adapter, writer.PersistentStreamRecoveryAdapter);
        Assert.False(writer.UseTransactionalWrites);
    }

    [Theory]
    [InlineData(false, true, null)]
    [InlineData(false, false, null)]
    [InlineData(false, true, "")]
    [InlineData(false, true, "override")]
    [InlineData(true, true, null)]
    [InlineData(true, false, null)]
    [InlineData(true, true, "")]
    [InlineData(true, true, "override")]
    public async Task Connection_Overrides_Password_And_Stream_Lockfile_Without_Changing_Templates(bool isStream, bool useLockFile, string? password)
    {
        await using MemoryStream stream = new();
        AccessReaderOptions readerTemplate = new("reader") { UseLockFile = useLockFile };
        AccessWriterOptions writerTemplate = new("writer") { UseLockFile = useLockFile };
        DbContextOptionsBuilder options = new();
        if (isStream)
        {
            _ = options.UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, jetOptionsAction: jet => jet.WithReaderOptions(readerTemplate).WithWriterOptions(writerTemplate).WithPassword(password));
        }
        else
        {
            _ = options.UseJetDatabaseWriter(Path.Combine(Path.GetTempPath(), $"jdw-ef-options-{Guid.NewGuid():N}.accdb"), DatabaseFormat.AceAccdb, jet => jet.WithReaderOptions(readerTemplate).WithWriterOptions(writerTemplate).WithPassword(password));
        }

        await using JetClientConnection connection = new(options.Options);
        AccessReaderOptions reader = connection.BuildReaderOptions();
        AccessWriterOptions writer = connection.BuildWriterOptions();

        Assert.Equal(string.IsNullOrEmpty(password) ? "reader" : password, reader.Password.ToString());
        Assert.Equal(string.IsNullOrEmpty(password) ? "writer" : password, writer.Password.ToString());
        Assert.Equal(!isStream && useLockFile, reader.UseLockFile);
        Assert.Equal(!isStream && useLockFile, writer.UseLockFile);
        Assert.Equal("reader", readerTemplate.Password.ToString());
        Assert.Equal("writer", writerTemplate.Password.ToString());
        Assert.Equal(useLockFile, readerTemplate.UseLockFile);
        Assert.Equal(useLockFile, writerTemplate.UseLockFile);
    }

    [Fact]
    public async Task Connection_Defaults_Enable_Transactional_Writes()
    {
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb)
            .Options;
        await using JetClientConnection connection = new(options);

        Assert.True(connection.BuildWriterOptions().UseTransactionalWrites);
    }

    [Fact]
    public async Task EnsureCreated_Refuses_Invalid_Writer_Budget_Before_Changing_Stream()
    {
        await using MemoryStream stream = new();
        DbContextOptions options = new DbContextOptionsBuilder()
            .UseJetDatabaseWriter(
                stream,
                DatabaseFormat.AceAccdb,
                jetOptionsAction: jet => jet.WithWriterOptions(new AccessWriterOptions { MaxActivePageLeases = 0 }))
            .Options;
        await using DbContext context = new(options);

        ArgumentOutOfRangeException exception = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken));

        Assert.Equal("options", exception.ParamName);
        Assert.Equal(0L, stream.Length);
    }

    private sealed class RecoveryAdapter : IPersistentStreamRecoveryAdapter
    {
        public string DatabaseIdentity => "ef-options-forwarding";

        public long Generation => 0;

        public bool HasQuiescentCrashFencing => true;

        public bool HasPendingJournal(Stream databaseStream) => false;

        public IPersistentStreamRecoveryLease AcquireExclusiveLease(Stream databaseStream)
            => throw new InvalidOperationException("Options construction must not acquire a recovery lease.");
    }
}
#pragma warning restore EF1001
