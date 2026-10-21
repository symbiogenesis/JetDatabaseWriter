namespace JetDatabaseWriter.Tests.Pages;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Table removal releases dedicated map storage and retains surviving aliases.</summary>
public sealed class UsageMapStorageReclamationTests
{
    /// <summary>Ownership is proved against surviving definitions, including another table's free map.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="shared">Whether a surviving table aliases the removed map.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task DropTable_ReclaimsOnlyUnreferencedMapStorage(DatabaseFormat format, bool shared)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("Gone", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], ct);
        await writer.CreateTableAsync("Survivor", [new ColumnDefinition("Id", typeof(int))], ct);
        ResolvedTable gone = await writer.Services.Catalog.ResolveRequiredTableAsync("Gone", ct);
        byte[] tdef = await writer.Database.Pages.ReadPageCopyAsync(gone.Entry.TDefPage, ct);
        int map = UsageMap.ReadUInt24(tdef, writer.Database.Format.TDef.UsedPagesPage);
        byte[] original = await writer.Database.Pages.ReadPageCopyAsync(map, ct);
        if (shared)
        {
            ResolvedTable survivor = await writer.Services.Catalog.ResolveRequiredTableAsync("Survivor", ct);
            byte[] survivorTdef = await writer.Database.Pages.ReadPageCopyAsync(survivor.Entry.TDefPage, ct);
            UsageMap.WritePointer(survivorTdef, writer.Database.Format.TDef.FreePages, tdef[writer.Database.Format.TDef.FreePages], map);
            await writer.Pager.WritePageAsync(survivor.Entry.TDefPage, survivorTdef, ct);
        }

        await writer.DropTableAsync("Gone", ct);
        Assert.Equal(!shared, await writer.Services.PageAllocator.IsPageFreeAsync(map, ct));
        if (shared)
        {
            Assert.Equal(original, await writer.Database.Pages.ReadPageCopyAsync(map, ct));
        }

        Assert.Null(await writer.Services.Catalog.GetCatalogEntryAsync("Gone", ct));
        await writer.InsertRowAsync("Survivor", [1], ct);
    }
}
