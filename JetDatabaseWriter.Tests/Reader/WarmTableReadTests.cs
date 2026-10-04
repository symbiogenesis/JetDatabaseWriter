namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// A reader that has read a table keeps what it learned about it: the table's
/// TDEF bytes, its owned data pages and, in the page cache, its data and index
/// pages. Reading the table's first row again, as the warm <c>FirstRow</c>
/// benchmark does, then reads nothing from the file, so the call completes on
/// the caller's thread even where every page read is handed to the thread
/// pool: on a thread with a synchronization context, or on a thread that is
/// not a pool thread, such as the one BenchmarkDotNet blocks on. Listing the
/// table's indexes and seeking through one again read nothing either.
/// </summary>
public sealed class WarmTableReadTests : IDisposable
{
    private const string TableName = "Items";
    private const int RowCount = 300;
    private const string IndexName = "IX_Id";
    private const int SeekId = 42;

    private readonly List<string> paths = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<PageReadOptimizationMode> Modes =>
    [
        PageReadOptimizationMode.Auto,
        PageReadOptimizationMode.Disabled,
        PageReadOptimizationMode.Enabled,
    ];

    /// <summary>
    /// After a full scan, reading the table's first row again reads no byte
    /// of the file in any read mode, read-ahead included.
    /// </summary>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Rows_WarmFirstRow_ReadsNothing(PageReadOptimizationMode mode)
    {
        await using var backing = new MemoryStream(await CreateDatabaseAsync(), writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, Options(mode), leaveOpen: true, Ct);
        Assert.Equal(RowCount, await CountRowsAsync(reader));

        counting.Reset();
        Assert.Equal(1, await ReadFirstIdAsync(reader));

        Assert.True(
            counting.BytesRead == 0,
            $"The warm first-row read read pages {string.Join(", ", counting.PagesRead(reader.PageSize).Order())}.");
    }

    /// <summary>
    /// On a thread with a synchronization context, a path-opened reader hands
    /// each page read to the thread pool and waits for it. A warm first-row
    /// read has no page to read, so its first <c>MoveNextAsync</c> is complete
    /// when it returns.
    /// </summary>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Rows_WarmFirstRowOnThreadWithSynchronizationContext_CompletesSynchronously(PageReadOptimizationMode mode)
    {
        string path = Path.Combine(Path.GetTempPath(), $"WarmTableRead_{Guid.NewGuid():N}.accdb");
        this.paths.Add(path);
        await File.WriteAllBytesAsync(path, await CreateDatabaseAsync(), Ct);
        await using AccessReader reader = await AccessReader.OpenAsync(path, Options(mode), Ct);
        Assert.Equal(RowCount, await CountRowsAsync(reader));

        await using IAsyncEnumerator<object[]> rows = reader.Rows(TableName, cancellationToken: Ct).GetAsyncEnumerator(Ct);
        ValueTask<bool> first = MoveNextOnSynchronizationContext(rows);
        bool completedSynchronously = first.IsCompleted;

        Assert.True(await first);
        Assert.Equal(1, Assert.IsType<int>(rows.Current[0]));
        Assert.True(completedSynchronously, "The warm first-row read waited for a page read on the thread pool.");
    }

    /// <summary>
    /// After a first index listing and seek, listing the table's indexes and
    /// seeking through one of them again read no byte of the file: the index
    /// metadata comes from the TDEF bytes the reader keeps, and the index and
    /// data pages from its page cache.
    /// </summary>
    [Fact]
    public async Task ListIndexesAndSeekRows_Warm_ReadNothing()
    {
        await using var backing = new MemoryStream(await CreateDatabaseAsync(), writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, Options(PageReadOptimizationMode.Auto), leaveOpen: true, Ct);
        Assert.Contains(await reader.ListIndexesAsync(TableName, Ct), index => string.Equals(index.Name, IndexName, StringComparison.Ordinal));
        Assert.Equal(SeekId, await SeekIdAsync(reader));

        counting.Reset();
        Assert.Contains(await reader.ListIndexesAsync(TableName, Ct), index => string.Equals(index.Name, IndexName, StringComparison.Ordinal));
        Assert.Equal(SeekId, await SeekIdAsync(reader));

        Assert.True(
            counting.BytesRead == 0,
            $"The warm index listing and seek read pages {string.Join(", ", counting.PagesRead(reader.PageSize).Order())}.");
    }

    public void Dispose()
    {
        foreach (string path in this.paths)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
            catch (UnauthorizedAccessException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private static AccessReaderOptions Options(PageReadOptimizationMode mode) =>
        new() { PageReadOptimizationMode = mode, UseLockFile = false };

#pragma warning disable RCS1229 // The context must come off when MoveNextAsync returns, not when the move completes: the caller checks whether it completed.
    /// <summary>
    /// Starts the next row with a plain <see cref="SynchronizationContext"/>
    /// installed on this thread, so the reader cannot read a page inline here,
    /// and takes it off again before returning.
    /// </summary>
    /// <param name="rows">The row enumerator.</param>
    /// <returns>The pending or completed move.</returns>
    private static ValueTask<bool> MoveNextOnSynchronizationContext(IAsyncEnumerator<object[]> rows)
    {
        SynchronizationContext? previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
        try
        {
            return rows.MoveNextAsync();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }
#pragma warning restore RCS1229

    private static async Task<int> CountRowsAsync(AccessReader reader)
    {
        int count = 0;
        await foreach (object[] row in reader.Rows(TableName, cancellationToken: Ct))
        {
            Assert.Equal(++count, Assert.IsType<int>(row[0]));
        }

        return count;
    }

    /// <summary>Reads the table's first row and stops, as the <c>FirstRow</c> benchmark does.</summary>
    /// <param name="reader">The reader.</param>
    /// <returns>The first row's Id.</returns>
    /// <exception cref="InvalidOperationException">The table has no rows.</exception>
    private static async Task<int> ReadFirstIdAsync(AccessReader reader)
    {
        await foreach (object[] row in reader.Rows(TableName, cancellationToken: Ct))
        {
            return Assert.IsType<int>(row[0]);
        }

        throw new InvalidOperationException($"'{TableName}' has no rows.");
    }

    /// <summary>Seeks <see cref="SeekId"/> through <see cref="IndexName"/> and returns the Id of the one row found.</summary>
    /// <param name="reader">The reader.</param>
    /// <returns>The Id of the row found.</returns>
    private static async Task<int> SeekIdAsync(AccessReader reader)
    {
        List<int> ids = [];
        await foreach (object[] row in reader.SeekRowsAsync(TableName, IndexName, [SeekId], Ct))
        {
            ids.Add(Assert.IsType<int>(row[0]));
        }

        return Assert.Single(ids);
    }

    /// <summary>
    /// Builds an ACCDB whose <see cref="TableName"/> table holds
    /// <see cref="RowCount"/> rows of an Id, indexed by <see cref="IndexName"/>,
    /// and a 188-character Text value: more than three data pages, so a
    /// path-opened reader's <c>Auto</c> scans and every <c>Enabled</c> scan
    /// read ahead.
    /// </summary>
    /// <returns>The database bytes.</returns>
    private static async Task<byte[]> CreateDatabaseAsync()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Pad", typeof(string), maxLength: 200)],
                [new IndexDefinition(IndexName, "Id")],
                Ct);
            _ = await writer.InsertRowsAsync(
                TableName,
                Enumerable.Range(1, RowCount).Select(id => new object?[] { id, "pad-" + id.ToString("D4", CultureInfo.InvariantCulture) + new string('-', 180) }),
                Ct);
        }

        return stream.ToArray();
    }
}
