namespace JetDatabaseWriter.Tests.Catalog;

using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class CatalogCorruptionTests
{
    [Theory]
    [InlineData("Id")]
    [InlineData("Name")]
    [InlineData("Type")]
    [InlineData("Flags")]
    public async Task CatalogRows_RejectsMissingRequiredColumnBeforeScanning(string missing)
    {
        string[] names = ["Id", "Name", "Type", "Flags"];
        var columns = new System.Collections.Generic.List<ColumnInfo>();
        foreach (string name in names)
        {
            if (name != missing)
            {
                columns.Add(new ColumnInfo { Name = name, Type = ColumnType.LongIntegerType });
            }
        }

        var catalog = new CatalogRowReader(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!, null!);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await catalog.GetCatalogRowsAsync(new TableDef { Columns = columns }, TestContext.Current.CancellationToken));

        Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);
    }

    [Fact]
    public async Task PublicListTables_RejectsMissingCatalogInsteadOfEmptyDatabase()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(TestContext.Current.CancellationToken);
        byte[] bytes = stream.ToArray();
        bytes[2 * 4096] = 0;
        await using var corrupted = new MemoryStream(bytes);
        await using AccessReader reader = await AccessReader.OpenAsync(
            corrupted, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.ListTablesAsync(TestContext.Current.CancellationToken));

        Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);

        // A failed scan must not cache an empty list: the same read fails again.
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }
}
