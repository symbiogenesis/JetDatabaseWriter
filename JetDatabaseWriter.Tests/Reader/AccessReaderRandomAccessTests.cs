namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Microsoft.Win32.SafeHandles;
using Xunit;

public sealed class AccessReaderRandomAccessTests : IDisposable
{
    private readonly List<string> paths = [];

    [Fact]
    public async Task OpenAsync_PathWithAutoPageReadOptimization_UsesRandomAccessPageReads()
    {
        string path = await this.CreateReadableDatabaseAsync();

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        Assert.Equal(PageReadOptimizationMode.Auto, reader.PageReadOptimizationMode);

        // The netstandard2.1 build has no RandomAccess API and reads every page through
        // the stream; only the net10.0 build switches to random-access reads.
        Assert.Equal(!LibraryTarget.IsNetStandard, FacadeInternals.Database(reader).UsesRandomAccessPageReads);
        await AssertReadableItemsTableAsync(reader);
    }

    [Fact]
    public async Task OpenAsync_PathWithAutoPageReadOptimization_ReadsMultiPageTableInOrder()
    {
        const int rowCount = 2_000;
        string path = await this.CreateReadableDatabaseAsync(rowCount);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
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

        Assert.Equal(rowCount, count);
    }

    [Fact]
    public async Task OpenAsync_PathWithDisabledPageReadOptimization_UsesSeekReadPageReads()
    {
        string path = await this.CreateReadableDatabaseAsync();

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
                PageReadOptimizationMode = PageReadOptimizationMode.Disabled,
                UseLockFile = false,
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(PageReadOptimizationMode.Disabled, reader.PageReadOptimizationMode);
        Assert.False(FacadeInternals.Database(reader).UsesRandomAccessPageReads);
        await AssertReadableItemsTableAsync(reader);
    }

    [Fact]
    public async Task OpenAsync_CallerSuppliedFileStreamWithEnabledPageReadOptimization_UsesSeekReadPageReads()
    {
        string path = await this.CreateReadableDatabaseAsync();

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
                PageReadOptimizationMode = PageReadOptimizationMode.Enabled,
                UseLockFile = false,
            },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        Assert.False(FacadeInternals.Database(reader).UsesRandomAccessPageReads);
        await AssertReadableItemsTableAsync(reader);
    }

    /// <summary>
    /// <see cref="FileStream.SafeFileHandle"/> flushes the stream and seeks the OS
    /// file pointer on every get, which made each <c>RandomAccess</c> page read
    /// slower than a buffered stream read. The handle is read once, when
    /// random-access reads are enabled, and every page read reuses it.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RandomAccessPageReads_ScanManyPages_DoNotReadTheFileHandleAgain(DatabaseFormat format)
    {
        const int rowCount = 2_000;
        string path = await this.CreateReadableDatabaseAsync(rowCount, format);

        await using var stream = new HandleCountingFileStream(path);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions
            {
                PageCacheSize = 0,
                UseLockFile = false,
            },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
        DatabaseFile db = FacadeInternals.Database(reader);
        db.EnableRandomAccessPageReadsIfSupported();
        Assert.Equal(!LibraryTarget.IsNetStandard, db.UsesRandomAccessPageReads);

        int handleReadsBeforeScan = stream.HandleReads;
        int count = 0;
        await foreach (object[] row in reader.Rows("Items", cancellationToken: TestContext.Current.CancellationToken))
        {
            count++;
            Assert.Equal(count, Assert.IsType<int>(row[0]));
        }

        Assert.Equal(rowCount, count);
        Assert.Equal(handleReadsBeforeScan, stream.HandleReads);
    }

    /// <summary>
    /// A path-opened reader opens the file without <see cref="FileOptions.Asynchronous"/>
    /// and without an access-pattern hint, in every mode. On Windows an overlapped
    /// read completes through the I/O completion port even when the OS cache
    /// already holds the page, which cost several times a synchronous read of a
    /// cached page, and the <see cref="FileOptions.RandomAccess"/> and
    /// <see cref="FileOptions.SequentialScan"/> hints defeated OS read-ahead on
    /// files that were not cached yet.
    /// </summary>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [InlineData(PageReadOptimizationMode.Auto)]
    [InlineData(PageReadOptimizationMode.Disabled)]
    [InlineData(PageReadOptimizationMode.Enabled)]
    public async Task OpenAsync_Path_OpensSynchronousFileHandleWithoutAccessHint(PageReadOptimizationMode mode)
    {
        string path = await this.CreateReadableDatabaseAsync();

        using FileStreamFactory.OpenTracker opens = FileStreamFactory.TrackOpens();
        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
                PageReadOptimizationMode = mode,
                UseLockFile = false,
            },
            TestContext.Current.CancellationToken);

        FileStream stream = Assert.IsType<FileStream>(FacadeInternals.Database(reader).DatabaseStream);
        Assert.False(stream.IsAsync);
        FileStreamFactory.OpenedFile open = Assert.Single(opens.Opens);
        Assert.Equal(path, open.Path);
        Assert.Equal(FileOptions.None, open.Options);
        await AssertReadableItemsTableAsync(reader);
    }

    /// <summary>
    /// A scan cancelled mid-table stops with <see cref="OperationCanceledException"/>
    /// at the next page, and the reader keeps reading afterwards. Every page
    /// read goes to the file.
    /// </summary>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [InlineData(PageReadOptimizationMode.Auto)]
    [InlineData(PageReadOptimizationMode.Disabled)]
    public async Task Rows_CancelledMidScan_ThrowsOperationCanceled(PageReadOptimizationMode mode)
    {
        const int rowCount = 2_000;
        const int rowsBeforeCancel = 10;
        string path = await this.CreateReadableDatabaseAsync(rowCount);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
                PageCacheSize = 0,
                PageReadOptimizationMode = mode,
                UseLockFile = false,
            },
            TestContext.Current.CancellationToken);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        int count = 0;
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (object[] row in reader.Rows("Items", cancellationToken: cancellation.Token))
            {
                Assert.Equal(++count, Assert.IsType<int>(row[0]));
                if (count == rowsBeforeCancel)
                {
                    await cancellation.CancelAsync();
                }
            }
        });

        // Table scans check the token once per data page.
        Assert.InRange(count, rowsBeforeCancel, rowCount - 1);
        Assert.Equal(rowCount, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    public void Dispose()
    {
        foreach (string path in this.paths)
        {
            TryDeleteFile(path);
        }
    }

    private static async ValueTask AssertReadableItemsTableAsync(AccessReader reader)
    {
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
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

    private async ValueTask<string> CreateReadableDatabaseAsync(int rowCount = 1, DatabaseFormat format = DatabaseFormat.Jet4Mdb)
    {
        bool accdb = format == DatabaseFormat.AceAccdb;
        string path = Path.Combine(Path.GetTempPath(), $"ReaderRandomAccess_{Guid.NewGuid():N}{(accdb ? ".accdb" : ".mdb")}");
        this.paths.Add(path);
        this.paths.Add(Path.ChangeExtension(path, accdb ? ".laccdb" : ".ldb"));

        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            path,
            format,
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

    /// <summary>A read-only <see cref="FileStream"/> that counts the gets of <see cref="SafeFileHandle"/>.</summary>
    /// <param name="path">The file to open.</param>
    private sealed class HandleCountingFileStream(string path)
        : FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 4096, FileOptions.None)
    {
        private int handleReads;

        /// <summary>Gets how many times <see cref="SafeFileHandle"/> was read.</summary>
        public int HandleReads => Volatile.Read(ref this.handleReads);

        /// <inheritdoc/>
        public override SafeFileHandle SafeFileHandle
        {
            get
            {
                _ = Interlocked.Increment(ref this.handleReads);
                return base.SafeFileHandle;
            }
        }
    }
}
