namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Parent snapshot decoding errors are never replaced with foreign-key violations.</summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ParentReadFailureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task Insert_WhenParentRowsCannotBeDecoded_ThrowsTheDecodeError(WriteMode mode)
    {
        await using MemoryStream stream = await db.CopyToStreamAsync(TestDatabases.MdbtoolsNwind, Ct);
        // CompanyName has no covering index, so this additional relationship
        // forces a parent snapshot even when ShipperID can be sought directly.
        await ForeignKeyTestDatabase.PlantRelationshipAsync(stream, "FK_DecodeParent", "Orders", "ShipVia", "Shippers", "CompanyName");
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, ForeignKeyTestDatabase.WriterOptions(WriteMode.Direct), cancellationToken: Ct))
        {
            CatalogEntry entry = await harness.Services.Catalog.GetRequiredCatalogEntryAsync("Shippers", Ct);
            TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(entry.TDefPage, "Shippers", Ct);
            byte[] page = (byte[])(await harness.Pager.ReadPageAsync(entry.TDefPage, Ct)).Clone();
            JetFormat format = harness.Database.Format;
            int start = format.TDef.BlockEnd + (Ri32(page, format.TDef.NumRealIdx) * format.TDef.RealIdxEntrySz);
            int ordinal = definition.FindColumnIndex("CompanyName");
            Assert.True(ordinal >= 0);
            int offset = start + (ordinal * format.ColumnDescriptor.Size) + format.ColumnDescriptor.TypeOff;
            Assert.Equal(0x0A, page[offset]);
            page[offset] = 0x48;
            await harness.Pager.WritePageAsync(entry.TDefPage, page, Ct);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException error = await Assert.ThrowsAnyAsync<InvalidOperationException>(async () => await writer.InsertRowAsync("Orders", new JetDatabaseWriter.Models.RowValues { ["ShipVia"] = 1 }, Ct));
                Assert.IsNotType<JetConstraintException>(error);
                Assert.Contains("CompanyName", error.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, stream.ToArray());
    }
}
