namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Concurrent;
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

    /// <summary>
    /// A path-opened reader reads pages synchronously when the caller is already
    /// on a thread-pool thread, instead of handing each read to another pool
    /// thread, so a scan with no page cache completes every row after the first
    /// without waiting: read-ahead included, since its prefetch completes inline.
    /// </summary>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [InlineData(PageReadOptimizationMode.Auto)]
    [InlineData(PageReadOptimizationMode.Disabled)]
    public async Task Rows_OnThreadPool_ReadPagesInline(PageReadOptimizationMode mode)
    {
        const int rowCount = 2_000;
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

        ScanCompletion scan = await Task.Run(() => ScanCountingPendingMovesAsync(reader, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);

        Assert.Equal(rowCount, scan.Rows);
        Assert.Equal(0, scan.PendingMovesAfterFirst);
    }

    /// <summary>
    /// A caller with a <see cref="SynchronizationContext"/>, such as a UI thread,
    /// never waits on the disk: its page reads still run on the thread pool.
    /// That holds when the context runs on a thread-pool thread too, as
    /// Blazor Server's renderer context does, so the context alone keeps the
    /// reads offloaded.
    /// </summary>
    /// <remarks>
    /// Read-ahead is off. With it, the library's <c>ConfigureAwait(false)</c>
    /// continuations start prefetches on pool threads, where they complete
    /// inline, so whether any row still waits depends on timing: under a loaded
    /// full test run, every prefetch finished before the scan reached its page.
    /// Without it, every page the scan moves onto is read from the context's
    /// thread, so the move waits for the offloaded read.
    /// </remarks>
    /// <param name="onThreadPool"><see langword="true"/> to run the context on a thread-pool thread; <see langword="false"/> for a dedicated thread.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Rows_OnThreadWithSynchronizationContext_DoNotReadInline(bool onThreadPool)
    {
        const int rowCount = 2_000;
        string path = await this.CreateReadableDatabaseAsync(rowCount);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
                PageCacheSize = 0,
                PageReadOptimizationMode = PageReadOptimizationMode.Disabled,
                UseLockFile = false,
            },
            TestContext.Current.CancellationToken);

        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        ScanCompletion scan = await SingleThreadSynchronizationContext.RunAsync(
            async () =>
            {
                Assert.Equal(onThreadPool, Thread.CurrentThread.IsThreadPoolThread);
                return await ScanCountingPendingMovesAsync(reader, cancellationToken);
            },
            onThreadPool);

        Assert.Equal(rowCount, scan.Rows);
        Assert.True(scan.PendingMovesAfterFirst > 0, "Every row completed synchronously, so a page read blocked the context's thread.");
    }

    /// <summary>
    /// A caller running under a <see cref="TaskScheduler"/> other than the
    /// default keeps its page reads offloaded, even on a thread-pool thread
    /// with no synchronization context: the scheduler may limit how many of
    /// its tasks run at once, so a blocking read would hold one of its slots.
    /// </summary>
    /// <remarks>
    /// The exclusive scheduler of a <see cref="ConcurrentExclusiveSchedulerPair"/>
    /// runs its tasks on pool threads, so the scheduler is the only thing that
    /// keeps the reads offloaded. Read-ahead is off, as in
    /// <see cref="Rows_OnThreadWithSynchronizationContext_DoNotReadInline"/>.
    /// </remarks>
    [Fact]
    public async Task Rows_OnThreadPoolUnderCustomTaskScheduler_DoNotReadInline()
    {
        const int rowCount = 2_000;
        string path = await this.CreateReadableDatabaseAsync(rowCount);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions
            {
                PageCacheSize = 0,
                PageReadOptimizationMode = PageReadOptimizationMode.Disabled,
                UseLockFile = false,
            },
            TestContext.Current.CancellationToken);

        var schedulers = new ConcurrentExclusiveSchedulerPair();
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        ScanCompletion scan = await Task.Factory.StartNew(
            async () =>
            {
                Assert.True(Thread.CurrentThread.IsThreadPoolThread, "The exclusive scheduler ran the scan off the thread pool.");
                Assert.Null(SynchronizationContext.Current);
                Assert.Same(schedulers.ExclusiveScheduler, TaskScheduler.Current);
                return await ScanCountingPendingMovesAsync(reader, cancellationToken);
            },
            cancellationToken,
            TaskCreationOptions.DenyChildAttach,
            schedulers.ExclusiveScheduler).Unwrap();
        schedulers.Complete();

        Assert.Equal(rowCount, scan.Rows);
        Assert.True(scan.PendingMovesAfterFirst > 0, "Every row completed synchronously, so a page read blocked a thread of the caller's scheduler.");
    }

    /// <summary>
    /// A scan cancelled on a thread-pool thread, where page reads run inline,
    /// stops at the next page, and the reader keeps reading afterwards.
    /// </summary>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [InlineData(PageReadOptimizationMode.Auto)]
    [InlineData(PageReadOptimizationMode.Disabled)]
    public async Task Rows_CancelledMidScanOnThreadPool_ThrowsOperationCanceled(PageReadOptimizationMode mode)
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
        int count = await Task.Run(
            async () =>
            {
                int seen = 0;
                _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                {
                    await foreach (object[] row in reader.Rows("Items", cancellationToken: cancellation.Token))
                    {
                        Assert.Equal(++seen, Assert.IsType<int>(row[0]));
                        if (seen == rowsBeforeCancel)
                        {
                            await cancellation.CancelAsync();
                        }
                    }
                });
                return seen;
            },
            TestContext.Current.CancellationToken);

        Assert.InRange(count, rowsBeforeCancel, rowCount - 1);
        Assert.Equal(rowCount, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Only a path-opened reader reads inline: its handle is synchronous. A
    /// caller's stream may be overlapped, and the writer's handle is, so a
    /// blocking read on either would be slower than the offloaded one.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReadsInlineOnThreadPool_IsSetOnlyForPathOpenedReaders(DatabaseFormat format)
    {
        string path = await this.CreateReadableDatabaseAsync(format: format);

        foreach (PageReadOptimizationMode mode in (PageReadOptimizationMode[])[PageReadOptimizationMode.Auto, PageReadOptimizationMode.Disabled, PageReadOptimizationMode.Enabled])
        {
            await using AccessReader pathReader = await AccessReader.OpenAsync(
                path,
                new AccessReaderOptions { PageReadOptimizationMode = mode, UseLockFile = false },
                TestContext.Current.CancellationToken);
            Assert.True(FacadeInternals.Database(pathReader).ReadsInlineOnThreadPool, $"{mode} path reader");
        }

        await using (FileStream stream = FileStreamFactory.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        await using (AccessReader streamReader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { PageReadOptimizationMode = PageReadOptimizationMode.Enabled, UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            Assert.False(FacadeInternals.Database(streamReader).ReadsInlineOnThreadPool);
        }

        foreach (bool transactional in (bool[])[false, true])
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(
                path,
                new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = transactional },
                TestContext.Current.CancellationToken);
            Assert.False(FacadeInternals.Database(writer).ReadsInlineOnThreadPool);
            await writer.InsertRowAsync("Items", [transactional ? 3 : 2], TestContext.Current.CancellationToken);
            Assert.False(FacadeInternals.Database(writer).ReadsInlineOnThreadPool);

            await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            Assert.False(FacadeInternals.Database(writer).ReadsInlineOnThreadPool);
            await transaction.RollbackAsync(TestContext.Current.CancellationToken);
        }
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
        Assert.Equal("Items", Assert.Single(tables));
    }

    /// <summary>
    /// Scans the 'Items' table by hand and counts the <c>MoveNextAsync</c> calls,
    /// after the first, that were not complete when they returned: those waited
    /// for a page read on another thread.
    /// </summary>
    /// <param name="reader">The reader.</param>
    /// <param name="cancellationToken">A token used to cancel the scan.</param>
    private static async Task<ScanCompletion> ScanCountingPendingMovesAsync(AccessReader reader, CancellationToken cancellationToken)
    {
        int rows = 0;
        int pendingMovesAfterFirst = 0;
        await using IAsyncEnumerator<object[]> scan = reader.Rows("Items", cancellationToken: cancellationToken).GetAsyncEnumerator(cancellationToken);
        while (true)
        {
            // No ConfigureAwait(false): on a synchronization context the next
            // MoveNextAsync must start on the context's thread again.
            ValueTask<bool> move = scan.MoveNextAsync();
            if (rows > 0 && !move.IsCompleted)
            {
                pendingMovesAfterFirst++;
            }

            if (!await move)
            {
                break;
            }

            Assert.Equal(++rows, Assert.IsType<int>(scan.Current[0]));
        }

        return new ScanCompletion(rows, pendingMovesAfterFirst);
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

    /// <summary>The rows a scan returned and how many of its row moves after the first had to wait.</summary>
    /// <param name="Rows">The rows returned.</param>
    /// <param name="PendingMovesAfterFirst">The <c>MoveNextAsync</c> calls after the first that were not complete when they returned.</param>
    private readonly record struct ScanCompletion(int Rows, int PendingMovesAfterFirst);

    /// <summary>
    /// Runs work on one thread whose <see cref="SynchronizationContext"/>
    /// queues every continuation back to that thread, as a UI thread does.
    /// </summary>
    private sealed class SingleThreadSynchronizationContext : SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> queue = [];

        /// <summary>
        /// Runs <paramref name="work"/> on a new thread, or on a thread-pool
        /// thread that it holds until the work completes, with this context
        /// installed, and returns its result.
        /// </summary>
        /// <typeparam name="T">The result type.</typeparam>
        /// <param name="work">The work to start on the thread.</param>
        /// <param name="onThreadPool"><see langword="true"/> to run on a thread-pool thread; <see langword="false"/> for a dedicated thread.</param>
        /// <returns>The work's result.</returns>
        public static async Task<T> RunAsync<T>(Func<Task<T>> work, bool onThreadPool)
        {
            var started = new TaskCompletionSource<Task<T>>(TaskCreationOptions.RunContinuationsAsynchronously);
            void Pump()
            {
                SynchronizationContext? previous = Current;
                using var context = new SingleThreadSynchronizationContext();
                SetSynchronizationContext(context);
                try
                {
                    Task<T> task = work();
                    _ = task.ContinueWith(_ => context.queue.CompleteAdding(), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                    started.SetResult(task);
                    foreach ((SendOrPostCallback callback, object? state) in context.queue.GetConsumingEnumerable())
                    {
                        callback(state);
                    }
                }
                finally
                {
                    // A pool thread goes back to the pool without the context.
                    SetSynchronizationContext(previous);
                }
            }

            if (onThreadPool)
            {
                _ = ThreadPool.QueueUserWorkItem(_ => Pump());
            }
            else
            {
                new Thread(Pump) { IsBackground = true }.Start();
            }

            return await (await started.Task.ConfigureAwait(false)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state) => this.queue.Add((d, state));

        /// <inheritdoc/>
        public override void Send(SendOrPostCallback d, object? state) => throw new NotSupportedException();

        /// <inheritdoc/>
        public override SynchronizationContext CreateCopy() => this;

        /// <inheritdoc/>
        public void Dispose() => this.queue.Dispose();
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
