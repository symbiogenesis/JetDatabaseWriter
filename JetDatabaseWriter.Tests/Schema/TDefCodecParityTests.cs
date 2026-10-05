namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Compares the codec against the frozen pre-codec parser on Access-authored tables.</summary>
/// <param name="db">Caches the relationship host fixtures.</param>
public sealed class TDefCodecParityTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    public static TheoryData<string> Fixtures => TestDatabases.JackcessAll;

    public static TheoryData<string> ProjectFixtures =>
    [
        TestDatabases.Jet3Test,
        TestDatabases.AdventureWorks,
        TestDatabases.NorthwindTraders,
        TestDatabases.ComplexFields,
        TestDatabases.CompositeTextIndex,
    ];

    [Theory]
    [MemberData(nameof(Fixtures))]
    [MemberData(nameof(ProjectFixtures))]
    public async Task Parse_FixtureTables_MatchesLegacyColumns(string path)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, ct);
        using var reader = new TableDefReader(harness.Database.Pages, harness.Database.Format, cacheResults: false);
        HashSet<long> roots = [];
        HashSet<long> continuations = [];
        for (long page = 1; page < harness.Database.Pages.PageCount; page++)
        {
            byte[] bytes = await harness.Database.Pages.ReadPageAsync(page, ct);
            try
            {
                if (bytes[0] == Constants.PageTypes.TableDefinition)
                {
                    roots.Add(page);
                    continuations.Add(Ru32(bytes, 4));
                }
            }
            finally
            {
                PageBuffers.Return(bytes);
            }
        }

        roots.ExceptWith(continuations);
        Assert.NotEmpty(roots);
        foreach (long root in roots)
        {
            byte[]? bytes = await reader.ReadTDefBytesAsync(root, ct);
            Assert.NotNull(bytes);
            AssertParity(harness.Database.Format, bytes);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Parse_WriterParentChildSelfAndMultiPageDefinitions_MatchesLegacy(DatabaseFormat kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, kind);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Parent", [new ColumnDefinition("Id", typeof(int))], [new IndexDefinition("PK", "Id") { IsPrimaryKey = true }], ct);
            var columns = new List<ColumnDefinition> { new("Id", typeof(int)), new("ParentId", typeof(int)) };
            var indexes = new List<IndexDefinition> { new("PK", "Id") { IsPrimaryKey = true } };
            for (int i = 2; i < 50; i++)
            {
                string name = "C" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
                columns.Add(new ColumnDefinition(name, typeof(int)));
                if (i < 31)
                {
                    indexes.Add(new IndexDefinition("IX_" + name, name));
                }
            }

            await writer.CreateTableAsync("Child", columns, indexes, ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Child", "Parent", "Id", "Child", "ParentId"), ct);
            await writer.CreateTableAsync("Self", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParentId", typeof(int))], [new IndexDefinition("PK", "Id") { IsPrimaryKey = true }], ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Self", "Self", "Id", "Self", "ParentId"), ct);
        }

        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct);
        foreach (string table in new[] { "Parent", "Child", "Self" })
        {
            CatalogEntry? entry = await harness.GetCatalogEntryAsync(table, ct);
            Assert.NotNull(entry);
            byte[]? bytes = await harness.Database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, ct);
            Assert.NotNull(bytes);
            AssertParity(harness.Database.Format, bytes);
            TDefImage? image = TDefCodec.Parse(harness.Database.Format, bytes);
            Assert.NotNull(image);
            Assert.Contains(image.LogicalIndexes, index => index.Entry.IndexType == IndexKind.ForeignKey);
            if (table == "Child")
            {
                Assert.True(bytes.Length > harness.Database.Format.PageSize);
            }
        }
    }

    private static void AssertParity(JetFormat format, byte[] bytes)
    {
        var legacy = new LegacyTDefParsers(format);
        TableDef? expected = legacy.Parse(bytes);
        TDefImage? actual = TDefCodec.Parse(format, bytes);
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }

        Assert.NotNull(actual);
        Assert.Equal(expected.RowCount, actual.Header.Counters.RowCount);
        Assert.Equal(expected.HasDeletedColumns, actual.HasDeletedColumns);
        Assert.Equal(expected.Columns.Count, actual.Columns.Count);
        for (int i = 0; i < expected.Columns.Count; i++)
        {
            ColumnInfo column = actual.Columns[i];
            Assert.Equal(expected.Columns[i], column with { RawDescriptor = [] });
            Assert.Equal(format.ColumnDescriptor.Size, column.RawDescriptor.Count);
        }

        List<IndexMetadata> indexes = LegacyIndexCatalogReader.ReadMetadata(format, bytes, expected.Columns);
        Assert.Equivalent(indexes, IndexCatalogReader.ReadMetadata(format, bytes, actual.Columns), strict: true);
        LegacyIndexCatalogReader.ReadImageIndexes(format, bytes, out List<(RealIdxSlot Slot, uint Root, List<KeyColumn> Columns, byte[] Raw)> physical, out List<(LogicalIdxEntry Entry, string Name, byte[] Raw)> logical);
        Assert.Equal(physical.Count, actual.RealIndexes.Count);
        for (int i = 0; i < physical.Count; i++)
        {
            Assert.Equal(physical[i].Slot, actual.RealIndexes[i].Slot);
            Assert.Equal(physical[i].Columns, actual.RealIndexes[i].Columns);
            Assert.Equal(physical[i].Root, actual.RealIndexes[i].RootPage);
            Assert.Equal(physical[i].Raw, actual.RealIndexes[i].RawDescriptor);
        }

        Assert.Equal(logical.Count, actual.LogicalIndexes.Count);
        for (int i = 0; i < logical.Count; i++)
        {
            Assert.Equal(logical[i].Entry, actual.LogicalIndexes[i].Entry);
            Assert.Equal(logical[i].Name, actual.LogicalIndexes[i].Name);
            Assert.Equal(logical[i].Raw, actual.LogicalIndexes[i].RawDescriptor);
        }
    }
}
