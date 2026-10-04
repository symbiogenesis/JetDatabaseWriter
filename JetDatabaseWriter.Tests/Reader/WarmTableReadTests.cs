namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// A reader that has read a table keeps what it learned about it: the table's
/// TDEF bytes, its owned data pages, also when its usage map is rejected and
/// the whole-file owner index answered, and, in the page cache, its data and
/// index pages. Reading the table's first row again, as the warm
/// <c>FirstRow</c> benchmark does, then reads nothing from the file, so the
/// call completes on the caller's thread even where every page read is handed
/// to the thread pool: on a thread with a synchronization context, or on a
/// thread that is not a pool thread, such as the one BenchmarkDotNet blocks
/// on. Listing the table's indexes and seeking through one again read nothing
/// either.
/// </summary>
public sealed class WarmTableReadTests : IDisposable
{
    private const string TableName = "Items";
    private const int RowCount = 300;
    private const string IndexName = "IX_Id";
    private const int SeekId = 42;

    private readonly List<string> paths = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Returns each read mode, with the table's owned-pages usage map valid
    /// and rejected (its TDEF declares more rows than the mapped pages hold,
    /// as when a writer-built map leaves out pages).
    /// </summary>
    /// <returns>The read modes, each with and without a rejected map.</returns>
    public static TheoryData<PageReadOptimizationMode, bool> Cases()
    {
        var data = new TheoryData<PageReadOptimizationMode, bool>();
        foreach (PageReadOptimizationMode mode in (PageReadOptimizationMode[])[PageReadOptimizationMode.Auto, PageReadOptimizationMode.Disabled, PageReadOptimizationMode.Enabled])
        {
            data.Add(mode, false);
            data.Add(mode, true);
        }

        return data;
    }

    /// <summary>
    /// After a full scan, reading the table's first row again reads no byte
    /// of the file in any read mode, read-ahead included, whether its usage
    /// map validated or was rejected.
    /// </summary>
    /// <param name="mode">The page-read optimization mode.</param>
    /// <param name="rejectUsageMap">Whether the table's usage map is rejected.</param>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Rows_WarmFirstRow_ReadsNothing(PageReadOptimizationMode mode, bool rejectUsageMap)
    {
        await using var backing = new MemoryStream(await CreateDatabaseAsync(rejectUsageMap), writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, Options(mode), leaveOpen: true, Ct);
        Assert.Equal(RowCount, await CountRowsAsync(reader));

        if (rejectUsageMap)
        {
            // The rejected map sent the scan to the whole-file owned-page
            // pass, which reads every page from 3 on.
            int pageCount = checked((int)(backing.Length / reader.PageSize));
            Assert.True(
                counting.PagesRead(reader.PageSize).IsSupersetOf(Enumerable.Range(3, pageCount - 3).Select(page => (long)page)),
                "The cold scan did not take the whole-file owned-page pass, so the usage map was not rejected.");
        }

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
    /// <param name="rejectUsageMap">Whether the table's usage map is rejected.</param>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Rows_WarmFirstRowOnThreadWithSynchronizationContext_CompletesSynchronously(PageReadOptimizationMode mode, bool rejectUsageMap)
    {
        string path = Path.Combine(Path.GetTempPath(), $"WarmTableRead_{Guid.NewGuid():N}.accdb");
        this.paths.Add(path);
        await File.WriteAllBytesAsync(path, await CreateDatabaseAsync(rejectUsageMap), Ct);
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
        await using var backing = new MemoryStream(await CreateDatabaseAsync(rejectUsageMap: false), writable: false);
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
    /// read ahead. To have the reader reject the table's owned-pages usage
    /// map, the TDEF's row count is raised past the rows the mapped pages hold.
    /// </summary>
    /// <param name="rejectUsageMap">Whether the reader rejects the table's usage map.</param>
    /// <returns>The database bytes.</returns>
    private static async Task<byte[]> CreateDatabaseAsync(bool rejectUsageMap)
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

        byte[] bytes = stream.ToArray();
        if (rejectUsageMap)
        {
            await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(TableName, Ct));
            int numRows = checked((int)(entry.TDefPage * harness.Database.PageSizeBytes)) + harness.Database.TDef.NumRows;
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(numRows), RowCount + 1_000);
        }

        return bytes;
    }
}
