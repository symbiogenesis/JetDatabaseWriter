namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Object visibility flags cannot turn cached ODBC schema into executable local data.</summary>
public sealed class SystemLinkedExecutionTests
{
    [Theory]
    [InlineData(true, 0)]
    [InlineData(false, 0)]
    [InlineData(true, 8)]
    [InlineData(false, 8)]
    [InlineData(true, 2)]
    [InlineData(false, 2)]
    [InlineData(true, int.MinValue)]
    [InlineData(false, int.MinValue)]
    [InlineData(true, unchecked((int)Constants.SystemObjects.SystemTableMask))]
    [InlineData(false, unchecked((int)Constants.SystemObjects.SystemTableMask))]
    public async Task OdbcExecution_RefusesCachedDataRegardlessOfVisibilityFlags(bool strict, int flags)
    {
        await using MemoryStream stream = await CreateCachedOdbcLinkAsync(flags);
        var options = new AccessReaderOptions { UseLockFile = false, StrictParsing = strict };
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, options, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(await reader.TryLookupTableAsync("Victim", TestContext.Current.CancellationToken));
        Assert.Single(await reader.GetColumnMetadataAsync("Victim", TestContext.Current.CancellationToken));
        Assert.Single(await reader.ListIndexesAsync("Victim", TestContext.Current.CancellationToken));
        Assert.Empty(await reader.GetComplexColumnsAsync("Victim", TestContext.Current.CancellationToken));
        if ((unchecked((uint)flags) & Constants.SystemObjects.SystemTableMask) != 0)
        {
            Assert.Empty(await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken));
        }
        else
        {
            Assert.Equal("Victim", Assert.Single(await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken)).Name);
        }

        Func<Task>[] operations =
        [
            async () => _ = await reader.GetRealRowCountAsync("Victim", TestContext.Current.CancellationToken),
            async () => { using DataTable table = await reader.ReadTableAsync("Victim", cancellationToken: TestContext.Current.CancellationToken); },
            async () => { using DataTable table = await reader.ReadTableAsStringsAsync("Victim", cancellationToken: TestContext.Current.CancellationToken); },
            async () => _ = await reader.ReadTableAsync<CachedRow>("Victim", cancellationToken: TestContext.Current.CancellationToken),
            async () => _ = await reader.ReadTableAsync<CachedRow>("Victim", maxRows: 0, TestContext.Current.CancellationToken),
            async () => await DrainAsync(reader.Rows("Victim", cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.RowsAsStrings("Victim", cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.Rows<CachedRow>("Victim", cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.Rows<CachedRow>("Victim", row => row.Id == 7, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.SeekRowsAsync("Victim", "UQ_Id", [7], TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.FromIndex("Victim", "UQ_Id").ToRowsAsync(TestContext.Current.CancellationToken)),
            async () => _ = await reader.GetAttachmentsAsync("Victim", "Files", TestContext.Current.CancellationToken),
            async () => _ = await reader.GetMultiValueItemsAsync("Victim", "Tags", TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CachedRow>("Victim").CountAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CachedRow>("Victim").Where(row => row.Id == 7).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CachedRow>("Victim").OrderBy(row => row.Id).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CachedRow>("Victim").Take(0).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CachedRow>("Victim").Take(0).DefaultIfEmpty().ToListAsync(TestContext.Current.CancellationToken),
        ];
        foreach (Func<Task> operation in operations)
        {
            _ = await Assert.ThrowsAsync<JetNotSupportedException>(operation);
        }
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task SystemLink_RequiresCapabilityAndRemainsReadableWhenAuthorized(bool strict, bool text)
    {
        await using MemoryStream host = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(TestContext.Current.CancellationToken);
        string sourcePath = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "unopened-system-source.accdb");
        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(host, TestContext.Current.CancellationToken))
        {
            if (text)
            {
                await writer.CreateLinkedTextTableAsync("Victim", Path.GetDirectoryName(sourcePath)!, "source.csv", "Text;HDR=YES;FMT=Delimited", TestContext.Current.CancellationToken);
            }
            else
            {
                await writer.CreateLinkedTableAsync("Victim", sourcePath, "Data", TestContext.Current.CancellationToken);
            }
        }

        await RewriteCatalogLinkAsync(host, unchecked((int)Constants.SystemObjects.SystemTableMask), makeOdbc: false);
        var refusedOptions = new AccessReaderOptions { UseLockFile = false, StrictParsing = strict };
        await using (AccessReader refused = await TestDatabase.OpenReaderAsync(host, refusedOptions, cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Empty(await refused.ListLinkedTablesAsync(TestContext.Current.CancellationToken));
            Assert.True(await refused.TryLookupTableAsync("Victim", TestContext.Current.CancellationToken));
            _ = await Assert.ThrowsAsync<JetAccessDeniedException>(async () => await refused.GetRealRowCountAsync("Victim", TestContext.Current.CancellationToken));
        }

        byte[] approved = "Id\r\n7\r\n"u8.ToArray();
        if (!text)
        {
            await using MemoryStream source = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(TestContext.Current.CancellationToken);
            await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(source, TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("Data", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Data", [7], TestContext.Current.CancellationToken);
            }

            approved = source.ToArray();
        }

        var authorizedOptions = new AccessReaderOptions
        {
            UseLockFile = false,
            StrictParsing = strict,
            LinkedSourceResolver = (_, _, _) => new ValueTask<Stream>(new MemoryStream(approved, writable: false)),
        };
        host.Position = 0;
        await using AccessReader authorized = await TestDatabase.OpenReaderAsync(host, authorizedOptions, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Empty(await authorized.ListLinkedTablesAsync(TestContext.Current.CancellationToken));
        Assert.True(await authorized.TryLookupTableAsync("Victim", TestContext.Current.CancellationToken));
        Assert.Equal(1, await authorized.GetRealRowCountAsync("Victim", TestContext.Current.CancellationToken));
        Assert.Single(await authorized.ReadTableAsync<CachedRow>("Victim", cancellationToken: TestContext.Current.CancellationToken));
    }

    private static async ValueTask<MemoryStream> CreateCachedOdbcLinkAsync(int flags)
    {
        MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(TestContext.Current.CancellationToken);
        try
        {
            await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("Victim", [new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Victim", [7], TestContext.Current.CancellationToken);
            }

            await RewriteCatalogLinkAsync(stream, flags, makeOdbc: true);

            stream.Position = 0;
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    private static async ValueTask RewriteCatalogLinkAsync(MemoryStream stream, int flags, bool makeOdbc)
    {
        await using AccessWriter harness = await InMemoryAccessDatabase.OpenWriterAsync(stream, TestContext.Current.CancellationToken);
        TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, TestContext.Current.CancellationToken));
        ColumnInfo name = Assert.IsType<ColumnInfo>(definition.FindColumn("Name"));
        ColumnInfo type = Assert.IsType<ColumnInfo>(definition.FindColumn("Type"));
        ColumnInfo flagsColumn = Assert.IsType<ColumnInfo>(definition.FindColumn("Flags"));
        bool changed = false;
        foreach (RowLocation location in await harness.Database.GetLiveRowLocationsAsync(2, TestContext.Current.CancellationToken))
        {
            byte[] page = await harness.Database.Pages.ReadPageAsync(location.PageNumber, TestContext.Current.CancellationToken);
            if (ScalarColumnReader.DecodeSimpleColumnValue(harness.Database.Format, page, location.RowStart, location.RowSize, name) != "Victim")
            {
                continue;
            }

            int fixedStart = location.RowStart + harness.Database.Format.RowFields.NumCols;
            if (makeOdbc)
            {
                // A native ODBC cache uses a positive TDEF identity even without source metadata.
                BinaryPrimitives.WriteInt16LittleEndian(page.AsSpan(fixedStart + type.FixedOff, 2), Constants.SystemObjects.LinkedOdbcType);
            }

            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(fixedStart + flagsColumn.FixedOff, 4), flags);
            await harness.Pager.WritePageAsync(location.PageNumber, page, TestContext.Current.CancellationToken);
            changed = true;
            break;
        }

        Assert.True(changed);
    }

    private static async Task DrainAsync<T>(IAsyncEnumerable<T> rows)
    {
        await foreach (T row in rows)
        {
            Assert.NotNull(row);
        }
    }

    private sealed class CachedRow
    {
        public int Id { get; set; }
    }
}
