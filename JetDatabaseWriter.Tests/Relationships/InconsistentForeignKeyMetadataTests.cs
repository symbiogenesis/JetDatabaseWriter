namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Repairs and preserves foreign-key metadata with incomplete reciprocal links.</summary>
/// <param name="db">Caches Access-authored fixture databases.</param>
public sealed class InconsistentForeignKeyMetadataTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task DropTable_RemovesOneWayForeignKeyEntryNamingFreedPage(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateDatabaseAsync(format);
        long targetPage = await GetPageAsync(stream, "Target");
        await PatchChildAsync(stream, targetPage, null);
        stream.Position = 0;
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.DropTableAsync("Target", TestContext.Current.CancellationToken);
        }

        Dictionary<long, List<IndexMetadata>> links = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertNoEntryNamesPage(links, targetPage);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb, "add")]
    [InlineData(DatabaseFormat.AceAccdb, "drop")]
    [InlineData(DatabaseFormat.AceAccdb, "rename")]
    [InlineData(DatabaseFormat.Jet4Mdb, "add")]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename")]
    public async Task SchemaRewrite_PreservesPartnerOfUnnumberedBacklink(DatabaseFormat format, string operation)
    {
        await using MemoryStream stream = await this.CreateDatabaseAsync(format);
        long parentPage = await GetPageAsync(stream, "Parent");
        await PatchChildAsync(stream, parentPage, -1);
        stream.Position = 0;
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            switch (operation)
            {
                case "add":
                    await writer.AddColumnAsync("Parent", new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "drop":
                    await writer.DropColumnAsync("Parent", "Note", TestContext.Current.CancellationToken);
                    break;
                default:
                    await writer.RenameColumnAsync("Parent", "Note", "Renamed", TestContext.Current.CancellationToken);
                    break;
            }
        }

        long rewrittenPage = await GetPageAsync(stream, "Parent");
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        IndexMetadata parent = Assert.Single(await reader.ListIndexesAsync("Parent", TestContext.Current.CancellationToken), i => i.Kind == IndexKind.ForeignKey);
        IndexMetadata child = Assert.Single(await reader.ListIndexesAsync("Child", TestContext.Current.CancellationToken), i => i.Kind == IndexKind.ForeignKey);
        Assert.Equal(child.IndexNumber, parent.RelatedIndexNumber);
        Assert.Equal(rewrittenPage, child.RelatedTablePage);
        Assert.Equal(parent.IndexNumber, child.RelatedIndexNumber);
    }

    private static async ValueTask<long> GetPageAsync(MemoryStream stream, string table)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry entry = await harness.GetCatalogEntryAsync(table, TestContext.Current.CancellationToken) ?? throw new InvalidOperationException("Table not found.");
        return entry.TDefPage;
    }

    private static async ValueTask PatchChildAsync(MemoryStream stream, long partnerPage, int? partnerIndex)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry entry = await harness.Services.Catalog.GetCatalogEntryAsync("Child", TestContext.Current.CancellationToken) ?? throw new InvalidOperationException("Table not found.");
        byte[] td = await harness.Database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, TestContext.Current.CancellationToken) ?? throw new InvalidOperationException("TDEF not found.");
        JetFormat format = harness.Database.Format;
        int count = Ri32(td, format.TDef.NumIdx);
        int realCount = Ri32(td, format.TDef.NumRealIdx);
        int start = IndexCatalogReader.LocateRealIdxDescStart(format, td, Ru16(td, format.TDef.NumCols), realCount);
        IndexSectionAnchors anchors = format.Index.GetIndexSection(start, realCount, count);
        int index = IndexCatalogReader.ReadLogicalIdxNames(format, td, anchors.LogIdxNamesStart, count).IndexOf("FK_Child_Parent");
        Assert.True(index >= 0);
        int fields = format.Index.LogicalIdxFieldsOffset(anchors.LogIdxStart, index);
        await harness.Services.TDefWriter.WriteInt32Async(entry.TDefPage, fields + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset, checked((int)partnerPage), TestContext.Current.CancellationToken);
        if (partnerIndex is int number)
        {
            await harness.Services.TDefWriter.WriteInt32Async(entry.TDefPage, fields + Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset, number, TestContext.Current.CancellationToken);
        }
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
        => AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);

    private async ValueTask<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        MemoryStream stream;
        if (format == DatabaseFormat.Jet4Mdb)
        {
            stream = await db.CopyToStreamAsync(TestDatabases.AdventureWorks, TestContext.Current.CancellationToken);
        }
        else
        {
            stream = new MemoryStream();
            await using AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync("Parent", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Note", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Child", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Target", [new("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Child_Parent", "Parent", "Id", "Child", "ParentId"), TestContext.Current.CancellationToken);
        return stream;
    }
}
