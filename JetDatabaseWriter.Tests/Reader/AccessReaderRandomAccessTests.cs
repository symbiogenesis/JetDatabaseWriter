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

        await using var reader = await AccessReader.OpenAsync(
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
    public async Task OpenAsync_PathWithParallelPageReads_ReadsMultiPageTableInOrder()
    {
        const int RowCount = 2_000;
        string path = await CreateReadableDatabaseAsync(RowCount);

        await using var reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
                ParallelPageReadsEnabled = true,
                PageCacheSize = 4,
                UseLockFile = false,
            },
            TestContext.Current.CancellationToken);

        int count = 0;
        await foreach (object[] row in reader.Rows("Items", cancellationToken: TestContext.Current.CancellationToken))
        {
            count++;
            Assert.Equal(count, Assert.IsType<int>(row[0]));
        }

        Assert.Equal(RowCount, count);
    }

    [Fact]
    public async Task OpenAsync_PathWithoutParallelPageReads_UsesSeekReadPageReads()
    {
        string path = await CreateReadableDatabaseAsync();

        await using var reader = await AccessReader.OpenAsync(
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

        await using var stream = FileStreamFactory.Open(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
        await using var reader = await AccessReader.OpenAsync(
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
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
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

    private async ValueTask<string> CreateReadableDatabaseAsync(int rowCount = 1)
    {
        string path = Path.Combine(Path.GetTempPath(), $"ReaderRandomAccess_{Guid.NewGuid():N}.mdb");
        _paths.Add(path);
        _paths.Add(Path.ChangeExtension(path, ".ldb"));

        await using var writer = await AccessWriter.CreateDatabaseAsync(
            path,
            DatabaseFormat.Jet4Mdb,
            cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        if (rowCount == 1)
        {
            await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        }
        else
        {
            var rows = new List<object[]>(rowCount);
            for (int id = 1; id <= rowCount; id++)
            {
                rows.Add([id]);
            }

            await writer.InsertRowsAsync("Items", rows, TestContext.Current.CancellationToken);
        }

        return path;
    }
}
