namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Catalog discovery refuses traversal that would silently omit live objects.</summary>
public sealed class CatalogTraversalCorruptionTests
{
    public static TheoryData<DatabaseFormat, bool, string> Cases()
    {
        var cases = new TheoryData<DatabaseFormat, bool, string>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (bool strict in new[] { false, true })
            {
                foreach (string damage in new[] { "overflow-outside", "overflow-cycle", "overflow-short", "count", "offset" })
                {
                    cases.Add(format, strict, damage);
                }
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task CatalogTraversal_RejectsMissingLiveObjectsWithoutCachingPartialScan(DatabaseFormat format, bool strict, string damage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, long damagedPage) = await CreateDamagedCatalogAsync(format, damage, ct);
        var options = new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, DiagnosticsEnabled = true };
        await using var stream = new MemoryStream(bytes, writable: false);
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, options, leaveOpen: true, ct))
        {
            for (int attempt = 0; attempt < 2; attempt++)
            {
                JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ListTablesAsync(ct));
                AssertCatalogFailure(failure, damagedPage);
                Assert.Empty(reader.LastDiagnostics);
            }
        }

        stream.Position = 0;
        await using ReaderHarness lookup = await ReaderHarness.OpenAsync(stream, options, cancellationToken: ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await lookup.Services.Catalog.FindSystemTablePageAsync("Other", ct));
            AssertCatalogFailure(failure, damagedPage);
        }
    }

    private static void AssertCatalogFailure(JetCorruptDataException failure, long damagedPage)
    {
        Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);
        Assert.Equal("MSysObjects", failure.ErrorInfo.TableName);
        Assert.Equal(damagedPage, failure.ErrorInfo.PageNumber);
        Assert.False(string.IsNullOrEmpty(failure.ErrorInfo.Reason));
    }

    private static async Task<(byte[] Bytes, long DamagedPage)> CreateDamagedCatalogAsync(DatabaseFormat format, string damage, CancellationToken ct)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Victim", [new ColumnDefinition("Value", typeof(int))], ct);
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Value", typeof(int))], ct);
        }

        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct);
        TableDef msys = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(2, ct));
        CatalogRow victim = Assert.Single(
            await new CatalogRowReader(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages).GetCatalogRowsAsync(msys, ct),
            static row => row.Name == "Victim");
        JetFormat layout = harness.Database.Format;
        byte[] page = await harness.ReadPageCopyAsync(victim.PageNumber, ct);
        RowBound bound = Assert.Single(DataPageRows.ComputeRowDirectory(layout, page), row => row.RowIndex == victim.RowIndex);
        byte[] bytes = stream.ToArray();
        int pageStart = checked((int)(victim.PageNumber * layout.PageSize));
        int slotStart = pageStart + layout.DataPage.RowsStart + (victim.RowIndex * sizeof(ushort));
        int rowStart = pageStart + bound.RowStart;
        int rawOffset = bound.RowStart;
        switch (damage)
        {
            case "overflow-outside":
            case "overflow-cycle":
                rawOffset |= Constants.DataPage.OverflowRowFlag;
                long targetPage = damage == "overflow-cycle" ? victim.PageNumber : harness.Database.Pages.PageCount;
                bytes[rowStart] = damage == "overflow-cycle" ? checked((byte)victim.RowIndex) : (byte)0;
                bytes[rowStart + 1] = checked((byte)(targetPage & 0xFF));
                bytes[rowStart + 2] = checked((byte)((targetPage >> 8) & 0xFF));
                bytes[rowStart + 3] = checked((byte)((targetPage >> 16) & 0xFF));
                break;
            case "overflow-short":
                rawOffset |= Constants.DataPage.OverflowRowFlag;
                int rowCount = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(layout.DataPage.NumRows));
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageStart + layout.DataPage.NumRows), checked((ushort)(rowCount + 1)));
                BinaryPrimitives.WriteUInt16LittleEndian(
                    bytes.AsSpan(pageStart + layout.DataPage.RowsStart + (rowCount * sizeof(ushort))),
                    checked((ushort)((bound.RowStart + 3) | Constants.DataPage.DeletedRowFlag)));
                break;
            case "count":
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageStart + layout.DataPage.NumRows), ushort.MaxValue);
                break;
            case "offset":
                rawOffset = layout.DataPage.RowsStart;
                break;
            default:
                throw new ArgumentException("Unknown catalog traversal damage.", nameof(damage));
        }

        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(slotStart), checked((ushort)rawOffset));
        return (bytes, victim.PageNumber);
    }
}
