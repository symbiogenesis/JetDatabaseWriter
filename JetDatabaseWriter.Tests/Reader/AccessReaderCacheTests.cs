namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class AccessReaderCacheTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task OpenUncachedAsync_WithPositivePageCacheSize_SuppressesCacheAllocation()
    {
        byte[] bytes = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 256,
            UseLockFile = false,
        };

        using var cachedStream = new MemoryStream(bytes, writable: false);
        await using (var cachedReader = await AccessReader.OpenAsync(
            cachedStream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            Assert.NotNull(ReadPrivateField(cachedReader, "_pageCache"));
            Assert.NotNull(ReadPrivateField(cachedReader, "_rowBoundsCache"));
        }

        using var uncachedStream = new MemoryStream(bytes, writable: false);
        await using var uncachedReader = await AccessReader.OpenUncachedAsync(
            uncachedStream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        Assert.Equal(256, uncachedReader.PageCacheSize);
        Assert.Null(ReadPrivateField(uncachedReader, "_pageCache"));
        Assert.Null(ReadPrivateField(uncachedReader, "_rowBoundsCache"));
        Assert.NotEmpty(await uncachedReader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    private static object? ReadPrivateField(AccessReader reader, string fieldName)
    {
        var field = typeof(AccessReader).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingFieldException(typeof(AccessReader).FullName, fieldName);

        return field.GetValue(reader);
    }
}
