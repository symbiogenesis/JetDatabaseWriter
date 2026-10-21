namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Catalog traversal budgets apply to the combined schema rather than each table alone.</summary>
public sealed class CatalogSchemaBudgetTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Catalog_RejectsCombinedDefinitionsOverBudgetWithoutCachingPartialScan(DatabaseFormat format, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, int totalBytes, int largestBytes) = await CreateCatalogAsync(format, ct);
        int budget = totalBytes - 1;
        Assert.True(largestBytes <= budget);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, MaxTableDefinitionBytes = largestBytes, MaxSchemaTraversalBytes = budget },
            cancellationToken: ct);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.ListTablesAsync(ct));
            Assert.Equal(JetErrorCode.SchemaTraversalBudgetExceeded, failure.ErrorCode);
            Assert.Equal("MSysObjects", failure.ErrorInfo.TableName);
            Assert.NotNull(failure.ErrorInfo.PageNumber);
            Assert.False(string.IsNullOrEmpty(failure.ErrorInfo.Reason));
            Assert.Empty(reader.LastDiagnostics);
        }
    }

    [Fact]
    public async Task Catalog_InitialDefinitionOverBudgetRefusesBeforeReadingItsPage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, _, _) = await CreateCatalogAsync(DatabaseFormat.Jet4Mdb, ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await AccessReader.OpenAsync(
            counting,
            new AccessReaderOptions { UseLockFile = false, ValidateOnOpen = false, MaxSchemaTraversalBytes = 1 },
            cancellationToken: ct);
        counting.Reset();

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.ListTablesAsync(ct));

        Assert.Equal(JetErrorCode.SchemaTraversalBudgetExceeded, failure.ErrorCode);
        Assert.Equal(2L, failure.ErrorInfo.PageNumber);
        Assert.Empty(counting.PagesRead(reader.PageSize));
    }

    [Fact]
    public async Task Catalog_EntryCountLimitRefusesExcessRowsAndPermitsExactBoundary()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, _, _) = await CreateCatalogAsync(DatabaseFormat.Jet4Mdb, ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: ct);
        TableDef msys = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(2, ct));
        var original = new CatalogRowReader(reader.Database.Format, reader.Database.TableDefs, reader.Database.OwnedPages);
        List<CatalogRow> rows = await original.GetCatalogRowsAsync(msys, ct);
        Assert.True(rows.Count > 1);

        using var limitedDefinitions = new TableDefReader(
            reader.Database.Pages, reader.Database.Format, cacheResults: false, maxSchemaTraversalBytes: (rows.Count - 1) * 64);
        var limited = new CatalogRowReader(reader.Database.Format, limitedDefinitions, reader.Database.OwnedPages);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () => await limited.GetCatalogRowsAsync(msys, ct));
            Assert.Equal(JetErrorCode.SchemaTraversalBudgetExceeded, failure.ErrorCode);
            Assert.Equal("MSysObjects", failure.ErrorInfo.TableName);
            Assert.NotNull(failure.ErrorInfo.PageNumber);
            Assert.Contains("row count", failure.ErrorInfo.Reason, StringComparison.Ordinal);
            Assert.Empty(reader.LastDiagnostics);
        }

        using var permittedDefinitions = new TableDefReader(
            reader.Database.Pages, reader.Database.Format, cacheResults: false, maxSchemaTraversalBytes: rows.Count * 64);
        var permitted = new CatalogRowReader(reader.Database.Format, permittedDefinitions, reader.Database.OwnedPages);
        Assert.Equal(rows.Count, (await permitted.GetCatalogRowsAsync(msys, ct)).Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Definition_RemainingBudgetAppliesBeforeReadAndToCachedImages(bool cached)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, _, _) = await CreateCatalogAsync(DatabaseFormat.Jet4Mdb, ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(counting, cancellationToken: ct);
        using var definitions = new TableDefReader(reader.Database.Pages, reader.Database.Format, cacheResults: true) { VerifyOnHit = false };
        if (cached)
        {
            Assert.NotNull(await definitions.ReadImageAsync(2, ct));
        }

        counting.Reset();
        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await definitions.ReadImageAsync(2, reader.PageSize - 1, ct));

        Assert.Equal(JetErrorCode.SchemaTraversalBudgetExceeded, failure.ErrorCode);
        Assert.Equal(2L, failure.ErrorInfo.PageNumber);
        Assert.Empty(counting.PagesRead(reader.PageSize));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Options_InvalidAggregateBudgetRefusesReaderOpenAndWriterCreation(int budget)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, _, _) = await CreateCatalogAsync(DatabaseFormat.Jet4Mdb, ct);
        await using var source = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                source, new AccessReaderOptions { UseLockFile = false, MaxSchemaTraversalBytes = budget }, leaveOpen: true, ct);
        });

        await using var destination = new MemoryStream();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
                destination, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false, MaxSchemaTraversalBytes = budget }, leaveOpen: true, ct);
        });
        Assert.Equal(0, destination.Length);
    }

    [Fact]
    public async Task Definition_CanceledBudgetedReadPreservesCancellation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, _, _) = await CreateCatalogAsync(DatabaseFormat.Jet4Mdb, ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: ct);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await reader.Database.TableDefs.ReadImageAsync(2, 0, cancellation.Token));
    }

    [Fact]
    public void LinkedOptions_PreserveSchemaTraversalBudget()
    {
        var options = new AccessReaderOptions { MaxSchemaTraversalBytes = 12345 };

        Assert.Equal(options.MaxSchemaTraversalBytes, LinkedTableManager.CreateLinkedSourceOpenOptions(options).MaxSchemaTraversalBytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Catalog_ExactCombinedBudgetPermitsRepeatedCachedTraversals(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, int totalBytes, _) = await CreateCatalogAsync(format, ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false, MaxSchemaTraversalBytes = totalBytes },
            cancellationToken: ct);

        Assert.Equal(2, (await reader.ListTablesAsync(ct)).Count);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            Assert.True(await reader.Services.Catalog.FindSystemTablePageAsync("Second", ct) > 0);
        }
    }

    private static async Task<(byte[] Bytes, int TotalBytes, int LargestBytes)> CreateCatalogAsync(DatabaseFormat format, CancellationToken ct)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("First", [new ColumnDefinition("Value", typeof(int))], ct);
            await writer.CreateTableAsync("Second", [new ColumnDefinition("Value", typeof(int))], ct);
        }

        stream.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(stream, cancellationToken: ct);
        TableDef msys = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(2, ct));
        List<CatalogRow> rows = await new CatalogRowReader(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages).GetCatalogRowsAsync(msys, ct);
        var roots = new HashSet<long> { 2 };
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == Constants.SystemObjects.UserTableType && row.TDefPage >= 2)
            {
                roots.Add(row.TDefPage);
            }
        }

        int totalBytes = 0;
        int largestBytes = 0;
        foreach (long root in roots)
        {
            byte[] definition = Assert.IsType<byte[]>(await harness.Database.TableDefs.ReadTDefBytesAsync(root, ct));
            totalBytes += definition.Length;
            largestBytes = Math.Max(largestBytes, definition.Length);
        }

        return (stream.ToArray(), totalBytes, largestBytes);
    }
}
