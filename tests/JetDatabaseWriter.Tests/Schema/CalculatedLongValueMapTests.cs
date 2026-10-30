namespace JetDatabaseWriter.Tests.Schema;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Calculated Memo storage follows the native column identity without changing its Text descriptor.</summary>
public sealed class CalculatedLongValueMapTests
{
    [Theory]
    [InlineData("add", false)]
    [InlineData("rename", false)]
    [InlineData("drop", false)]
    [InlineData("add", true)]
    [InlineData("rename", true)]
    [InlineData("drop", true)]
    public async Task NativeCalculatedMemo_RewriteKeepsDescriptorAndMaintainsCurrentColumnMaps(string operation, bool empty)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.CalcFieldTestV2010, ct), ct);
        stream.Position = 0;
        string columnName = operation == "rename" ? "Names" : "AllNames";
        string first = new('f', 200);
        string last = new('l', 200);
        string fullName = last + ", " + first;
        string expected = fullName + "=" + fullName[..243];
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Table1", ct));
            TableDef before = Assert.IsType<TableDef>(await writer.Database.TableDefs.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo nativeColumn = Assert.Single(before.Columns, column => column.Name == "AllNames");
            Assert.Equal(ColumnType.TextType, nativeColumn.Type);
            Assert.True(nativeColumn.IsCalculated);
            byte[] nativeBytes = Assert.IsType<byte[]>(await writer.Database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, ct));
            Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, nativeBytes, out List<LongValueUsageMap> nativeMaps, out _));
            Assert.Contains(nativeMaps, map => map.ColumnNumber == nativeColumn.ColNum);

            if (empty)
            {
                Assert.Equal(4, await writer.DeleteRowsAsync("Table1", RowCriteria.All(), ct));
            }

            if (operation == "add")
            {
                await writer.AddColumnAsync("Table1", new("Extra", typeof(int)), ct);
            }
            else if (operation == "rename")
            {
                await writer.RenameColumnAsync("Table1", "AllNames", columnName, ct);
            }
            else
            {
                await writer.DropColumnAsync("Table1", "City", ct);
            }

            TableDef rewritten = Assert.IsType<TableDef>(await writer.Database.TableDefs.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo calculated = Assert.Single(rewritten.Columns, column => column.Name == columnName);
            Assert.Equal(ColumnType.TextType, calculated.Type);
            Assert.True(calculated.IsCalculated);
            if (operation == "drop")
            {
                Assert.NotEqual(nativeColumn.ColNum, calculated.ColNum);
            }

            byte[] bytes = Assert.IsType<byte[]>(await writer.Database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, ct));
            Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, bytes, out List<LongValueUsageMap> maps, out _));
            LongValueUsageMap map = Assert.Single(maps, candidate => candidate.ColumnNumber == calculated.ColNum);
            await writer.InsertRowAsync("Table1", new RowValues { ["FirstName"] = first, ["LastName"] = last, ["Salary"] = 1m, ["Popularity"] = 1m }, ct);
            byte[] page = await writer.Pager.ReadPageAsync(map.Owned.PageNumber, ct);
            try
            {
                Assert.True(UsageMap.TryGetRowBound(page, writer.Database.Format.DataPage, writer.Database.Format.PageSize, map.Owned.RowIndex, out RowBound row));
                var owned = new List<long>();
                Assert.True(await UsageMap.TryEnumeratePagesAsync(page, row, writer.Database.Format.PageSize, writer.Pager.PageCount, 1, true, writer.Pager.ReadPageAsync, PageBuffers.Return, owned, ct));
                Assert.NotEmpty(owned);
                foreach (long number in owned)
                {
                    Assert.True(LongValueStore.IsLvalPage(await writer.Database.Pages.ReadPageCopyAsync(number, ct)));
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        using DataTable values = await reader.ReadTableAsync("Table1", cancellationToken: ct);
        Assert.Equal(expected, Assert.Single(values.AsEnumerable(), row => (string)row["FirstName"] == first)[columnName]);
    }
}
