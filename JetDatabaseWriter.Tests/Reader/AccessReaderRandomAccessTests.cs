namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using Xunit;

public sealed class AccessReaderRandomAccessTests : IDisposable
{
    private readonly List<string> _paths = [];

    [Fact]
    public async Task OpenAsync_PathWithParallelPageReads_UsesRandomAccessPageReads()
    {
        string path = await CreateReadableDatabaseAsync();

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
                ParallelPageReadsEnabled = true,
                UseLockFile = false,
            },
            TestContext.Current.CancellationToken);

        Assert.True(reader.UsesRandomAccessPageReads);
        await AssertReadableItemsTableAsync(reader);
    }

    [Fact]
    public async Task OpenAsync_PathWithoutParallelPageReads_UsesSeekReadPageReads()
    {
        string path = await CreateReadableDatabaseAsync();

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        Assert.False(reader.UsesRandomAccessPageReads);
        await AssertReadableItemsTableAsync(reader);
    }

    [Fact]
    public async Task OpenAsync_CallerSuppliedFileStreamWithParallelPageReads_UsesSeekReadPageReads()
    {
        string path = await CreateReadableDatabaseAsync();

        await using FileStream stream = FileStreamFactory.Open(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions
            {
                ParallelPageReadsEnabled = true,
                UseLockFile = false,
            },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        Assert.False(reader.UsesRandomAccessPageReads);
        await AssertReadableItemsTableAsync(reader);
    }

    public void Dispose()
    {
        foreach (string path in _paths)
        {
            TryDeleteFile(path);
        }
    }

    private static async ValueTask AssertReadableItemsTableAsync(AccessReader reader)
    {
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Single(tables);
        Assert.Equal("Items", tables[0]);
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private async ValueTask<string> CreateReadableDatabaseAsync()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ReaderRandomAccess_{Guid.NewGuid():N}.mdb");
        _paths.Add(path);
        _paths.Add(Path.ChangeExtension(path, ".ldb"));

        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            path,
            DatabaseFormat.Jet4Mdb,
            cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        return path;
    }
}
