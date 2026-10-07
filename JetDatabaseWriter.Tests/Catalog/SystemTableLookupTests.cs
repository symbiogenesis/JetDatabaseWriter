namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// System tables are found by name through one <c>MSysObjects</c> lookup,
/// <c>CatalogRowReader.FindSystemTableTdefPageAsync</c>, which the reader's
/// <c>CatalogReader</c> and the writer share. <c>MSysObjects</c> itself is
/// always TDEF page 2 (Jackcess <c>PAGE_SYSTEM_CATALOG</c>), so it resolves
/// even in a file whose catalog has no row naming it.
/// </summary>
public sealed class SystemTableLookupTests
{
    private const string CatalogTable = "MSysObjects";

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    /// <summary>
    /// Every read API returns <c>MSysObjects</c> with its columns and rows on a
    /// database the writer created, including Jet3 and Jet4 files whose catalogs
    /// have no row naming <c>MSysObjects</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReadApis_MSysObjects_OnCreatedDatabase_ReturnCatalogRows(DatabaseFormat format)
    {
        byte[] bytes = await CreateDatabaseAsync(format, this.ct);
        (TableDef catalog, long rowCount) = await ReadCatalogTableDefAsync(bytes, this.ct);
        Assert.NotEmpty(catalog.Columns);

        await using AccessReader reader = await OpenReaderAsync(bytes, this.ct);
        using DataTable data = await reader.ReadTableAsync(CatalogTable, cancellationToken: this.ct);
        Assert.Equal(catalog.Columns.Select(c => c.Name), data.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Equal(rowCount, data.Rows.Count);
        Assert.Contains(data.Rows.Cast<DataRow>(), row => Equals(row["Name"], "T1"));

        Assert.Equal(rowCount, await reader.GetRealRowCountAsync(CatalogTable, this.ct));
        Assert.Equal(rowCount, await CountAsync(reader.Rows(CatalogTable, cancellationToken: this.ct)));
        Assert.Equal(rowCount, await CountAsync(reader.RowsAsStrings(CatalogTable, cancellationToken: this.ct)));
        Assert.Equal(catalog.Columns.Count, (await reader.GetColumnMetadataAsync(CatalogTable, this.ct)).Count);
        _ = await reader.ListIndexesAsync(CatalogTable, this.ct);
    }

    /// <summary>
    /// The reader's and the writer's system-table lookups resolve <c>MSysObjects</c>
    /// to TDEF page 2 when its catalog row is absent from a created Jet3 or Jet4 database.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task FindSystemTable_MSysObjects_WithMissingCatalogRow_ResolvesPage2(DatabaseFormat format)
    {
        byte[] bytes = await CreateDatabaseAsync(format, this.ct);
        Assert.False(await CorruptCatalogSelfRowAsync(bytes, this.ct));

        await using (var readStream = new MemoryStream(bytes, writable: false))
        await using (ReaderHarness reader = await ReaderHarness.OpenAsync(readStream, cancellationToken: this.ct))
        {
            Assert.Equal(2, await reader.Services.Catalog.FindSystemTablePageAsync(CatalogTable, this.ct));
            Assert.Equal(2, await reader.Services.Catalog.FindSystemTablePageAsync("msysobjects", this.ct));
        }

        await using var writeStream = new MemoryStream();
        await writeStream.WriteAsync(bytes, this.ct);
        await using WriterHarness writer = await WriterHarness.OpenAsync(writeStream, cancellationToken: this.ct);
        Assert.Equal(2, await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(CatalogTable, this.ct));
    }

    /// <summary>A malformed present catalog self-row cannot trigger the absent-row bootstrap fallback.</summary>
    [Fact]
    public async Task FindSystemTable_MSysObjects_WithMalformedCatalogRow_RefusesCorruption()
    {
        byte[] bytes = await CreateDatabaseAsync(DatabaseFormat.AceAccdb, this.ct);
        Assert.True(await CorruptCatalogSelfRowAsync(bytes, this.ct));

        await using (var readStream = new MemoryStream(bytes, writable: false))
        await using (ReaderHarness reader = await ReaderHarness.OpenAsync(readStream, cancellationToken: this.ct))
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await reader.Services.Catalog.FindSystemTablePageAsync(CatalogTable, this.ct));
            Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
        }

        await using var writeStream = new MemoryStream();
        await writeStream.WriteAsync(bytes, this.ct);
        await using WriterHarness writer = await WriterHarness.OpenAsync(writeStream, cancellationToken: this.ct);
        JetCorruptDataException writerError = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(CatalogTable, this.ct));
        Assert.Equal(JetErrorCode.CorruptCatalog, writerError.ErrorCode);
    }

    /// <summary>
    /// A linked ODBC table (<c>MSysObjects</c> type 4) carries a local TDEF with the
    /// remote table's columns. The reader resolves it to that definition, as it
    /// did before the lookups were merged; the writer only resolves local tables.
    /// </summary>
    [Fact]
    public async Task FindSystemTable_LinkedOdbc_ReaderResolvesLocalDefinition_WriterDoesNot()
    {
        const string linkedOdbcTable = "Ordrar";
        byte[] bytes = await File.ReadAllBytesAsync(TestDatabases.OdbcLinkerTestV2007, this.ct);

        await using (var readStream = new MemoryStream(bytes, writable: false))
        await using (ReaderHarness reader = await ReaderHarness.OpenAsync(readStream, cancellationToken: this.ct))
        {
            long tdefPage = await reader.Services.Catalog.FindSystemTablePageAsync(linkedOdbcTable, this.ct);
            Assert.Equal(127, tdefPage);
            TableDef? definition = await reader.ReadTableDefAsync(tdefPage, this.ct);
            Assert.NotNull(definition);
            Assert.Equal(20, definition.Columns.Count);
        }

        await using var writeStream = new MemoryStream();
        await writeStream.WriteAsync(bytes, this.ct);
        await using WriterHarness writer = await WriterHarness.OpenAsync(writeStream, cancellationToken: this.ct);
        Assert.Equal(0, await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(linkedOdbcTable, this.ct));
    }

    /// <summary>
    /// A system table whose <c>MSysObjects</c> row is an overflow row resolves by
    /// name and reads with its columns.
    /// </summary>
    /// <param name="fixture">The <see cref="TestDatabases"/> field naming the fixture.</param>
    /// <param name="systemTable">The system table whose catalog row is an overflow row.</param>
    [Theory]
    [InlineData(nameof(TestDatabases.TestIndexPropertiesV2010), "MSysAccessXML")]
    [InlineData(nameof(TestDatabases.QueryTestV2007), "MSysNavPaneGroups")]
    public async Task ReadDataTable_SystemTableWithOverflowCatalogRow_HasColumns(string fixture, string systemTable)
    {
        string path = (string)typeof(TestDatabases).GetField(fixture)!.GetValue(null)!;
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: this.ct);

        using DataTable data = await reader.ReadTableAsync(systemTable, cancellationToken: this.ct);
        Assert.NotEmpty(data.Columns);
    }

    /// <summary>
    /// The general catalog predicate lookup resolves each ComplexFields.accdb
    /// flat table by its name suffix to the TDEF the exact name resolves to.
    /// </summary>
    [Fact]
    public async Task FindSystemTable_ByNameSuffix_FindsComplexFlatTables()
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(TestDatabases.ComplexFields, cancellationToken: this.ct);
        await using AccessReader reader = await TestDatabases.OpenAsync(TestDatabases.ComplexFields, cancellationToken: this.ct);

        var flatTables = new List<string>();
        foreach (string table in await reader.ListTablesAsync(this.ct))
        {
            flatTables.AddRange((await reader.GetComplexColumnsAsync(table, this.ct)).Select(c => c.FlatTableName));
        }

        Assert.NotEmpty(flatTables);
        foreach (string flatTable in flatTables)
        {
            long exact = await harness.Services.Catalog.FindSystemTablePageAsync(flatTable, this.ct);
            long bySuffix = await harness.Services.Catalog.FindSystemTablePageAsync(
                name => name.EndsWith(flatTable[flatTable.IndexOf('_', StringComparison.Ordinal)..], StringComparison.OrdinalIgnoreCase),
                this.ct);
            Assert.True(exact > 2, $"'{flatTable}' did not resolve.");
            Assert.Equal(exact, bySuffix);
        }
    }

    private static async ValueTask<bool> CorruptCatalogSelfRowAsync(byte[] bytes, CancellationToken cancellationToken)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: cancellationToken);
        DatabaseFile database = harness.Database;
        TableDef? catalog = await harness.ReadTableDefAsync(2, cancellationToken);
        Assert.NotNull(catalog);
        List<CatalogRow> rows = await new CatalogRowReader(database.Format, database.TableDefs, database.OwnedPages).GetCatalogRowsAsync(catalog, cancellationToken);
        CatalogRow? selfRow = rows.FirstOrDefault(row => row.IsDecoded && string.Equals(row.Name, CatalogTable, StringComparison.OrdinalIgnoreCase));
        if (selfRow is null)
        {
            return false;
        }

        byte[] page = await harness.ReadPageCopyAsync(selfRow.PageNumber, cancellationToken);
        RowBound bound = DataPageRows.EnumerateLiveRowBounds(database.Format, page).Single(row => row.RowIndex == selfRow.RowIndex);
        int rowOffset = checked((int)(selfRow.PageNumber * database.Format.PageSize)) + bound.RowStart;
        bytes.AsSpan(rowOffset, database.Format.RowFields.NumCols).Clear();
        return true;
    }

    private static async ValueTask<byte[]> CreateDatabaseAsync(DatabaseFormat format, CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken))
        {
            await writer.CreateTableAsync("T1", [new ColumnDefinition("Id", typeof(int))], cancellationToken);
        }

        return ms.ToArray();
    }

    private static async ValueTask<(TableDef Definition, long RowCount)> ReadCatalogTableDefAsync(byte[] bytes, CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        TableDef? catalog = await harness.ReadTableDefAsync(2, cancellationToken);
        Assert.NotNull(catalog);
        TableCounters? counters = await harness.Database.TableDefs.ReadTableCountersAsync(2, cancellationToken);
        Assert.NotNull(counters);
        return (catalog, counters.Value.RowCount);
    }

    private static async ValueTask<AccessReader> OpenReaderAsync(byte[] bytes, CancellationToken cancellationToken)
        => await AccessReader.OpenAsync(new MemoryStream(bytes, writable: false), new AccessReaderOptions { UseLockFile = false }, leaveOpen: false, cancellationToken);

    private static async ValueTask<long> CountAsync<T>(IAsyncEnumerable<T> source)
    {
        long count = 0;
        await foreach (T item in source)
        {
            _ = item;
            count++;
        }

        return count;
    }
}
