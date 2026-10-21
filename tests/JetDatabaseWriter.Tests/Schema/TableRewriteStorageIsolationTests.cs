namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Publishing replacement storage does not walk unrelated table pages.</summary>
public sealed class TableRewriteStorageIsolationTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Rewrite_DoesNotReadUnrelatedDataPages(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var backing = new MemoryStream();
        await using var trace = new CountingStream(backing);
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            trace,
            format,
            new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 },
            leaveOpen: true,
            ct);
        ColumnDefinition[] columns = format == DatabaseFormat.AceAccdb
            ? [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Files", typeof(byte[])) { IsAttachment = true }]
            : [new("Id", typeof(int)) { IsPrimaryKey = true }];
        await writer.CreateTableAsync("Target", columns, ct);
        await writer.InsertRowAsync("Target", format == DatabaseFormat.AceAccdb ? [1, DBNull.Value] : [1], ct);
        await writer.CreateTableAsync("Unrelated", [new("Id", typeof(int)), new("Text", typeof(string), 200)], ct);
        await writer.InsertRowsAsync("Unrelated", Enumerable.Range(1, 256).Select(id => new object[] { id, new string('x', 180) }), ct);
        long unrelatedRoot = (await writer.Services.Catalog.ResolveRequiredTableAsync("Unrelated", ct)).Entry.TDefPage;
        long[] unrelatedPages = [.. await writer.Database.OwnedPages.GetOwnedDataPagesAsync(unrelatedRoot, ct)];
        Assert.True(unrelatedPages.Length > 10);

        // Warm the fallback owner index used by legacy system-table maps.
        _ = await writer.Database.OwnedPages.GetOwnedDataPagesAsync(1, ct);

        // Begin first so the transaction's conflict snapshot is outside this
        // measurement. All pages remain visible to the writer's pager.
        await using JetTransaction transaction = await writer.BeginTransactionAsync(ct);
        trace.Reset();
        await writer.AddColumnAsync("Target", new("Added", typeof(int)), ct);
        Assert.Empty(unrelatedPages.Intersect(trace.PagesRead(writer.Database.Format.PageSize)));
        await transaction.RollbackAsync(ct);
        Assert.Equal(columns.Length, (await writer.Services.Catalog.ResolveRequiredTableAsync("Target", ct)).Definition.Columns.Count);
        Assert.Equal(256, (await writer.Services.Snapshots.ReadRowsAsync(unrelatedRoot, ct)).Count);
    }
}
