namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Complete private table comparison composes raw identity, all maps and allocation coverage.</summary>
public sealed class CompactRawTableValidatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Validate_ReopenedRawTableAndEveryMapAgree(DatabaseFormat kind, bool empty)
    {
        CompactRawTableValidatorResult result = await ExerciseAsync(kind, empty);
        Assert.Equal(6, result.Maps);
        Assert.True(result.Pages >= result.Maps);
        Assert.True(result.ReadBytes > 0);
        Assert.False(result.DatabaseValidated);
    }

    [Theory]
    [InlineData("missing-map")]
    [InlineData("swapped-columns")]
    [InlineData("alias-map")]
    [InlineData("patched-image")]
    [InlineData("map-membership")]
    public async Task Validate_ForgedEvidenceOrMapBytesRefuse(string damage)
        => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ExerciseAsync(DatabaseFormat.AceAccdb, false, damage));

    [Fact]
    public async Task Validate_ReadAllowanceIsCumulativeAcrossRawAndMapPhases()
    {
        CompactRawTableValidatorResult complete = await ExerciseAsync(DatabaseFormat.Jet4Mdb, false);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, false, maxReadBytes: complete.ReadBytes - 1));
    }

    [Fact]
    public async Task Validate_MapAllowanceIsCumulativeAcrossEveryColumn()
        => await Assert.ThrowsAsync<JetLimitationException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, true, maxMapPages: 5));

    [Fact]
    public async Task Validate_CancelledComparisonPreservesBothInputs()
        => await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, false, "cancelled"));

    private static async Task<CompactRawTableValidatorResult> ExerciseAsync(DatabaseFormat kind, bool empty, string? damage = null, long maxReadBytes = 1_000_000_000, long maxMapPages = 1024)
    {
        await using var original = new MemoryStream();
        long owner;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Memo", typeof(string), 20_000), new("Binary", typeof(byte[]))], Ct);
            if (!empty)
            {
                await writer.InsertRowAsync("Items", [1, new string('a', 8000), new byte[9000]], Ct);
                await writer.InsertRowAsync("Items", [2, new string('b', 9000), new byte[10000]], Ct);
            }

            owner = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        }

        byte[] sourceBytes = original.ToArray();
        await using var sourceStream = new MemoryStream(sourceBytes, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        TDefImage definition = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        var session = new CompactDiscoverySession(source.Database.Pages, source.Database.Format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(sourceBytes, Ct);
        await using var rowLedger = new MemoryStream();
        await using var allocationLedger = new MemoryStream();
        CompactRawTableBuilderResult staged = await CompactRawTableBuilder.BuildAsync(session, source.Database.Format, owner, definition, destination, rowLedger, allocationLedger, [], new(), Ct);
        byte[] reopenedBytes = destination.ToArray();
        if (damage == "missing-map")
        {
            staged = staged with { LongValueMaps = [staged.LongValueMaps[0]] };
        }
        else if (damage == "swapped-columns")
        {
            staged = staged with { LongValueMaps = [staged.LongValueMaps[1], staged.LongValueMaps[0]] };
        }
        else if (damage == "alias-map")
        {
            staged = staged with { FreePages = staged.OwnedPages };
        }
        else if (damage == "patched-image")
        {
            staged.PatchedDefinition[^1] ^= 0x20;
        }
        else if (damage == "map-membership")
        {
            int start = checked((int)(staged.OwnedPages.FirstPageNumber * source.Database.Format.PageSize)) + source.Database.Format.PageSize - source.Database.Format.UsageMapRowSize;
            reopenedBytes[start + 5] ^= 1;
        }

        await using var reopened = new MemoryStream(reopenedBytes, writable: false);
        await using var rows = new MemoryStream(rowLedger.ToArray(), writable: false);
        await using var allocations = new MemoryStream(allocationLedger.ToArray(), writable: false);
        try
        {
            return await CompactRawTableValidator.ValidateAsync(session, owner, definition, reopened, rows, allocations, staged, [], new() { MaxReadBytes = maxReadBytes, Maps = new() { MaxPages = maxMapPages } }, damage == "cancelled" ? new CancellationToken(canceled: true) : Ct);
        }
        finally
        {
            Assert.Equal(sourceBytes, sourceStream.ToArray());
            Assert.Equal(reopenedBytes, reopened.ToArray());
            Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        }
    }
}
