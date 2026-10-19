namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Native stored Id keys independently identify catalog rows after owner and hint damage.</summary>
public sealed class CompactCatalogRecoveryTests
{
    [Theory]
    [InlineData("Jet3Test.mdb")]
    [InlineData("Jackcess/adox_jet4.mdb")]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb")]
    public async Task NativeIdIndexRecoversDirectCatalogIdentitiesWithoutOwnersOrAllocationHints(string relativePath)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", relativePath), Ct);
        CatalogRow[] expected;
        CatalogRow target;
        JetFormat format;
        await using (DatabaseFile intact = Open(bytes))
        {
            format = intact.Format;
            TDefImage image = Assert.IsType<TDefImage>(await intact.TableDefs.ReadImageAsync(2, Ct));
            var decoder = new CatalogRowReader(format, intact.TableDefs, intact.OwnedPages);
            expected = (await decoder.GetCatalogRowsAsync(image.Definition, Ct, scanPhysicalPages: true)).ToArray();
            target = expected.First(row =>
            {
                int slot = checked(((int)row.PageNumber * format.PageSize) + format.DataPage.RowsStart + (row.RowIndex * 2));
                return (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slot, 2)) & Constants.DataPage.NonLiveRowFlags) == 0;
            });
        }

        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked(((int)target.PageNumber * format.PageSize) + format.DataPage.TDefOff), 4), 0);
        bytes.AsSpan((2 * format.PageSize) + format.TDef.UsedPages, 4).Clear();
        bytes.AsSpan((2 * format.PageSize) + format.TDef.NumRows, 4).Clear();
        byte[] original = bytes.ToArray();
        await using DatabaseFile damaged = Open(bytes);
        var pages = new CompactInventoryPageSource(damaged.Pages, format, 100_000);
        using var definitions = new TableDefReader(pages, format, cacheResults: false);
        using var owned = new OwnedDataPages(pages, format, cacheResults: false);
        var rows = new CatalogRowReader(format, definitions, owned);
        TDefImage catalog = Assert.IsType<TDefImage>(await definitions.ReadImageAsync(2, Ct));
        List<CatalogRow> physical = await rows.GetCatalogRowsAsync(catalog.Definition, Ct, scanPhysicalPages: true);
        Assert.DoesNotContain(physical, row => row.PageNumber == target.PageNumber);
        CompactCatalogRecovery.Result recovered = await CompactCatalogRecovery.ScanAsync(pages, format, catalog, rows, new(), physical, 100_000, 100_000, 512L * 1024 * 1024, Ct);
        Assert.Contains(recovered.Rows, row => row.Identity == target);
        Assert.All(recovered.Rows, row => Assert.Contains(row.Identity, expected));
        Assert.Equal(original, bytes);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);
}
