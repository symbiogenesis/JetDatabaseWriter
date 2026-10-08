namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Distinguishes absent tables from empty read results.</summary>
/// <param name="db">The fixture cache.</param>
public class TableLookupTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task UnknownTableReadThrowsStructuredFailure()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        JetObjectNotFoundException error = await Assert.ThrowsAsync<JetObjectNotFoundException>(async () =>
        {
            using DataTable table = await reader.ReadTableAsync("MissingLookupTable", cancellationToken: TestContext.Current.CancellationToken);
        });
        Assert.Equal(JetErrorCode.TableNotFound, error.ErrorCode);
        Assert.Equal("MissingLookupTable", error.ErrorInfo.TableName);
    }

    [Fact]
    public async Task UnknownTableFailsAcrossReadSurfaces()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        const string missing = "MissingLookupTable";
        Func<Task>[] reads =
        [
            async () => _ = await reader.GetTableValidationRuleAsync(missing, TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<LookupRow>(missing).Take(0).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.GetColumnMetadataAsync(missing, TestContext.Current.CancellationToken),
            async () => _ = await reader.GetRealRowCountAsync(missing, TestContext.Current.CancellationToken),
            async () => _ = await reader.ListIndexesAsync(missing, TestContext.Current.CancellationToken),
            async () => _ = await reader.GetComplexColumnsAsync(missing, TestContext.Current.CancellationToken),
            async () => _ = await reader.GetAttachmentsAsync(missing, "MissingColumn", TestContext.Current.CancellationToken),
            async () => _ = await reader.GetMultiValueItemsAsync(missing, "MissingColumn", TestContext.Current.CancellationToken),
            async () => _ = await reader.ReadTableAsync<LookupRow>(missing, 0, TestContext.Current.CancellationToken),
            async () => { using DataTable table = await reader.ReadTableAsStringsAsync(missing, 0, cancellationToken: TestContext.Current.CancellationToken); },
            async () => await ConsumeAsync(reader.Rows(missing, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await ConsumeAsync(reader.RowsAsStrings(missing, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await ConsumeAsync(reader.Rows<LookupRow>(missing, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await ConsumeAsync(reader.Rows<LookupRow>(missing, row => row.Id == 1, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await ConsumeAsync(reader.SeekRowsAsync(missing, "MissingIndex", [1], TestContext.Current.CancellationToken)),
        ];
        foreach (Func<Task> read in reads)
        {
            JetObjectNotFoundException error = await Assert.ThrowsAsync<JetObjectNotFoundException>(read);
            Assert.Equal(JetErrorCode.TableNotFound, error.ErrorCode);
            Assert.Equal(missing, error.ErrorInfo.TableName);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task UnknownTableShortCircuitAndSyntheticQueriesStillThrow(int count)
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        const string missing = "MissingLookupTable";
        Func<Task>[] queries =
        [
            async () => _ = await reader.Query<LookupRow>(missing).Take(count).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<LookupRow>(missing).Select(row => row.Id).Take(count).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<LookupRow>(missing).Take(count).DefaultIfEmpty(new LookupRow()).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<LookupRow>(missing).Prepend(new LookupRow()).Take(1).ToListAsync(TestContext.Current.CancellationToken),
        ];
        foreach (Func<Task> query in queries)
        {
            JetObjectNotFoundException error = await Assert.ThrowsAsync<JetObjectNotFoundException>(query);
            Assert.Equal(JetErrorCode.TableNotFound, error.ErrorCode);
            Assert.Equal(missing, error.ErrorInfo.TableName);
        }
    }

    [Fact]
    public async Task TryLookupDistinguishesAbsentEmptyAndUnopenedLinkedTables()
    {
        await using MemoryStream stream = new();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Empty", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateLinkedTableAsync("UnopenedLink", @"C:\MissingLookupSource\source.accdb", "Source", TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(await reader.TryLookupTableAsync("empty", TestContext.Current.CancellationToken));
        Assert.True(await reader.TryLookupTableAsync("unopenedlink", TestContext.Current.CancellationToken));
        Assert.False(await reader.TryLookupTableAsync("Missing", TestContext.Current.CancellationToken));
        Assert.Equal(0, await reader.GetRealRowCountAsync("Empty", TestContext.Current.CancellationToken));
        Assert.Empty(await reader.ListIndexesAsync("Empty", TestContext.Current.CancellationToken));
        Assert.Empty(await reader.GetComplexColumnsAsync("Empty", TestContext.Current.CancellationToken));
        using DataTable empty = await reader.ReadTableAsync("Empty", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Empty(empty.Rows);
        Assert.Single(empty.Columns);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task KnownUnusableTableThrowsCorruptionInsteadOfAbsence(bool strict, bool linked)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: ct))
        {
            if (linked)
            {
                await writer.CreateLinkedTableAsync("Unusable", @"C:\MissingLookupSource\source.accdb", "Source", ct);
            }
            else
            {
                await writer.CreateTableAsync("Unusable", [new ColumnDefinition("Id", typeof(int))], ct);
            }
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ct))
        {
            TableDef objects = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, ct));
            CatalogRow entry = Assert.Single(await harness.Services.CatalogRows.GetCatalogRowsAsync(objects, ct), row => row.Name == "Unusable");
            long pageNumber = linked ? entry.PageNumber : entry.TDefPage;
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(pageNumber, ct);
            JetFormat format = harness.Database.Format;
            if (linked)
            {
                ColumnInfo source = Assert.IsType<ColumnInfo>(objects.FindColumn("Database"));
                RowBound bound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(format, page), row => row.RowIndex == entry.RowIndex);
                Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, page, bound.RowStart, bound.RowSize, objects.HasVarColumns, out RowLayout layout));
                int offset = bound.RowStart + layout.NullMaskPos + (source.ColNum / 8);
                page[offset] = (byte)(page[offset] & (0xFF ^ (1 << (source.ColNum % 8))));
            }
            else
            {
                JetTypeInfo.Wu16(page, format.TDef.NumCols, 0);
            }

            await harness.Pager.WritePageAsync(pageNumber, page, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetCorruptDataException lookup = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.TryLookupTableAsync("Unusable", ct));
            Assert.Equal(JetErrorCode.CorruptCatalog, lookup.ErrorCode);
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ListIndexesAsync("Unusable", ct));
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Query<LookupRow>("Unusable").Take(0).ToListAsync(ct));
            await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            {
                using DataTable table = await reader.ReadTableAsync("Unusable", cancellationToken: ct);
            });
        }
    }

    private static async Task ConsumeAsync<T>(IAsyncEnumerable<T> rows)
    {
        await foreach (T row in rows)
        {
            _ = row;
        }
    }

#pragma warning disable CA1812 // Created by the row materializer through its generic constructor contract.
    private sealed class LookupRow
    {
        public int Id { get; set; }
    }
#pragma warning restore CA1812
}
