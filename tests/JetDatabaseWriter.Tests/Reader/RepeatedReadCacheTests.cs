namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Repeated counts and missing-table lookups retain verified results without page caching.</summary>
public sealed class RepeatedReadCacheTests
{
    [Fact]
    public async Task RepeatedCountAndMissingLookup_ReadNoPagesAfterWarmup()
    {
        await using var database = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(database, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Data", [new("Id", typeof(int))], [new("PK", "Id") { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("Self", "Data", "Id", "Data", "Id") { EnforceReferentialIntegrity = false }, TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync("Data", [[1], [2], [3]], TestContext.Current.CancellationToken);
        }

        database.Position = 0;
        await using var trace = new CountingStream(database);
        await using AccessReader reader = await AccessReader.OpenAsync(trace, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, TestContext.Current.CancellationToken);
        reader.Database.TableDefs.VerifyOnHit = false;
        Assert.Equal(3, await reader.GetRealRowCountAsync("Data", TestContext.Current.CancellationToken));
        Assert.False(await reader.TryLookupTableAsync("Missing", TestContext.Current.CancellationToken));
        _ = await reader.GetTableStatsAsync(TestContext.Current.CancellationToken);
        _ = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);
        _ = await reader.GetColumnMetadataAsync("Data", TestContext.Current.CancellationToken);
        _ = await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken);
        trace.Reset();
        Assert.Equal(3, await reader.GetRealRowCountAsync("data", TestContext.Current.CancellationToken));
        Assert.False(await reader.TryLookupTableAsync("missing", TestContext.Current.CancellationToken));
        Assert.Equal(3, Assert.Single(await reader.GetTableStatsAsync(TestContext.Current.CancellationToken)).RowCount);
        Assert.Equal(3, (await reader.GetStatisticsAsync(TestContext.Current.CancellationToken)).TotalRows);
        Assert.Single(await reader.GetColumnMetadataAsync("Data", TestContext.Current.CancellationToken));
        Assert.Equal("Self", Assert.Single(await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken)).Name);
        Assert.Equal(0, trace.BytesRead);

        var counts = new Task<long>[8];
        for (int index = 0; index < counts.Length; index++)
        {
            counts[index] = reader.GetRealRowCountAsync("Data", TestContext.Current.CancellationToken).AsTask();
        }

        Assert.All(await Task.WhenAll(counts), count => Assert.Equal(3, count));
        Assert.Equal(0, trace.BytesRead);

        reader.Services.TableCatalog.Invalidate();
        Assert.Equal(3, await reader.GetRealRowCountAsync("Data", TestContext.Current.CancellationToken));
        Assert.False(await reader.TryLookupTableAsync("Missing", TestContext.Current.CancellationToken));
        Assert.True(trace.BytesRead > 0);

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.GetRealRowCountAsync("Data", canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.TryLookupTableAsync("Missing", canceled.Token));
    }
}
