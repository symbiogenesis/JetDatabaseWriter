namespace JetDatabaseWriter.Tests.Schema;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks that counter reads stay current and touch only the root of a wide definition.</summary>
public sealed class TableCountersTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReadCounters_ReadsOnlyRootAndSeesPendingValues(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true }], ct);
        }

        await using var counting = new CountingStream(stream);
        await using WriterHarness harness = await WriterHarness.OpenAsync(counting, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, PageCacheSize = 0 }, cancellationToken: ct);
        CatalogEntry entry = await harness.Services.Catalog.GetRequiredCatalogEntryAsync("T", ct);
        _ = await harness.Database.TableDefs.ReadImageAsync(entry.TDefPage, ct);
        counting.Reset();
        TableCounters? before = await harness.Database.TableDefs.ReadTableCountersAsync(entry.TDefPage, ct);
        Assert.NotNull(before);
        Assert.Equal(0u, before.Value.RowCount);
        Assert.Equal([entry.TDefPage], counting.PagesRead(harness.Database.Format.PageSize));
        await using JetTransaction tx = await harness.BeginTransactionAsync(ct);
        await harness.InsertRowAsync("T", [null], ct);
        TableCounters? during = await harness.Database.TableDefs.ReadTableCountersAsync(entry.TDefPage, ct);
        Assert.NotNull(during);
        Assert.Equal(1u, during.Value.RowCount);
        Assert.Equal(1u, during.Value.AutoNumber);
        await tx.RollbackAsync(ct);
        Assert.Equal(before, await harness.Database.TableDefs.ReadTableCountersAsync(entry.TDefPage, ct));
    }
}
