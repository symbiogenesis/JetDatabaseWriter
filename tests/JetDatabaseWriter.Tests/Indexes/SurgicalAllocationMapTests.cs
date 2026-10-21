namespace JetDatabaseWriter.Tests.Indexes;

using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Surgical splits reserve their new pages in the global allocation map.</summary>
public sealed class SurgicalAllocationMapTests
{
    /// <summary>Native global maps may already mark future pages free; a split must clear those bits.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Split_ClearsFutureFreeBits(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Key", typeof(string), maxLength: 255)], [new IndexDefinition("IX_Key", "Key")], ct);
            await writer.InsertRowsAsync("Items", Enumerable.Range(0, 30).Select(id => new object?[] { Key(id * 100) }), ct);
            byte[] global = await writer.Database.Pages.ReadPageCopyAsync(1, ct);
            Assert.True(UsageMap.TryGetFirstRowBound(global, writer.Database.Format.DataPage, writer.Database.Format.PageSize, out RowBound bound));
            Assert.Equal(Constants.UsageMap.InlineMapType, global[bound.RowStart]);
            for (long number = writer.Pager.PageCount; number < 300; number++)
            {
                Assert.True(UsageMap.TrySetInlinePageState(global, bound.RowStart, bound.RowSize, number, isMarked: true));
            }

            await writer.Pager.WritePageAsync(1, global, ct);
            await writer.InsertRowsAsync("Items", Enumerable.Range(1, 150).Select(id => new object?[] { Key(id * 17) }), ct);
            global = await writer.Database.Pages.ReadPageCopyAsync(1, ct);
            Assert.True(UsageMap.TryGetFirstRowBound(global, writer.Database.Format.DataPage, writer.Database.Format.PageSize, out bound));
            var free = new List<long>();
            Assert.True(await UsageMap.TryEnumeratePagesAsync(global, bound, writer.Database.Format.PageSize, writer.Pager.PageCount, 2, false, writer.Pager.ReadPageAsync, PageBuffers.Return, free, ct));
            foreach (long number in free)
            {
                byte[] page = await writer.Database.Pages.ReadPageCopyAsync(number, ct);
                Assert.False(page[0] is Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf, $"Live index page {number} is marked free.");
            }
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: ct);
        int count = 0;
        await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
        {
            Assert.IsType<string>(row[0]);
            count++;
        }

        Assert.Equal(180, count);
    }

    private static string Key(int id) => id.ToString("D6", CultureInfo.InvariantCulture) + new string('k', 120);
}
