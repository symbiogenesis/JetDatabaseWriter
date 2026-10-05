namespace JetDatabaseWriter.Tests.Indexes;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // Typed reads instantiate this POCO through compiled expressions.

/// <summary>Early index consumers bound physical reads with the page cache disabled.</summary>
public sealed class IndexReadStreamingTests
{
    /// <summary>Early consumers stop the uncached physical index walk.</summary>
    /// <param name="format">The file format.</param>
    /// <param name="rowCount">The size of the indexed table.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 5000)]
    [InlineData(DatabaseFormat.Jet4Mdb, 5000)]
    [InlineData(DatabaseFormat.AceAccdb, 5000)]
    [InlineData(DatabaseFormat.Jet3Mdb, 25000)]
    [InlineData(DatabaseFormat.Jet4Mdb, 25000)]
    [InlineData(DatabaseFormat.AceAccdb, 25000)]
    public async Task OrderedTakeAndStoppedSeek_ReadAtMostSixteenPages(DatabaseFormat format, int rowCount)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], ct);
            await writer.InsertRowsAsync("Items", Enumerable.Range(0, rowCount).Select(static id => new object?[] { id }), ct);
        }

        stream.Position = 0;
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, ct);
        // Resolve catalog and schema before measuring the index walk itself.
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Items", ct);
        _ = await reader.GetColumnMetadataAsync("Items", ct);
        counting.Reset();
        List<Item> first = await reader.Query<Item>("Items").OrderBy(item => item.Id).Take(10).ToListAsync(ct);
        Assert.Equal(Enumerable.Range(0, 10), first.Select(static item => item.Id));
        int pageSize = format == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        Assert.InRange(counting.BytesRead, 1, 16L * pageSize);

        counting.Reset();
        int count = 0;
        await foreach (Item item in reader.FromIndex<Item>("Items", indexes.Single(static index => index.Kind == IndexKind.PrimaryKey).Name).WhereBetween(100, rowCount - 1).ToRowsAsync(ct))
        {
            Assert.Equal(100 + count, item.Id);
            if (++count == 10)
            {
                break;
            }
        }

        Assert.Equal(10, count);
        Assert.InRange(counting.BytesRead, 1, 16L * pageSize);
    }

    private sealed class Item
    {
        public int Id { get; set; }
    }
}
