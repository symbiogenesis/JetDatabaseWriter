namespace JetDatabaseWriter.Tests.Queries;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Text joins have the same database comparison under scan and seek plans.</summary>
/// <param name="cache">Caches database fixture bytes.</param>
public sealed class IncludeTextCollationTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    public static TheoryData<string, WriteMode, int> Cases
    {
        get
        {
            var cases = new TheoryData<string, WriteMode, int>();
            foreach (string fixture in new[] { TestDatabases.TestV1997, TestDatabases.TestV2003, TestDatabases.TestV2010 })
            {
                foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
                {
                    cases.Add(fixture, mode, 1);
                    cases.Add(fixture, mode, 20);
                }
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task CaseDifferentForeignKey_CollectionAndReferenceMatchInBothPlans(string fixture, WriteMode mode, int fillerRows)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    await writer.CreateTableAsync("TextJoinParent", [new ColumnDefinition("Code", typeof(string), 40) { IsPrimaryKey = true }], ct);
                    await writer.CreateTableAsync("TextJoinChild", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PCode", typeof(string), 40)], ct);
                    await writer.CreateRelationshipAsync(new RelationshipDefinition("TextJoinRel", "TextJoinParent", "Code", "TextJoinChild", "PCode"), ct);
                    await writer.InsertRowAsync("TextJoinParent", ["ABC"], ct);
                    await writer.InsertRowAsync("TextJoinChild", [1, "abc"], ct);
                    for (int index = 0; index < fillerRows; index++)
                    {
                        string code = $"Other{index}";
                        await writer.InsertRowAsync("TextJoinParent", [code], ct);
                        await writer.InsertRowAsync("TextJoinChild", [100 + index, code], ct);
                    }
                },
                ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        TextJoinParent parent = Assert.Single(await reader.Query<TextJoinParent>("TextJoinParent").Where(row => row.Code == "ABC").Include(row => row.Children).ToListAsync(ct));
        Assert.Equal(1, Assert.Single(parent.Children).Id);
        TextJoinChild child = Assert.Single(await reader.Query<TextJoinChild>("TextJoinChild").Where(row => row.Id == 1).Include(row => row.Parent).ToListAsync(ct));
        Assert.Equal("ABC", Assert.IsType<TextJoinParent>(child.Parent).Code);
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task DifferentStoredCollation_RefusesSeekAndStillJoins(string fixture, WriteMode mode, int fillerRows)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        TextSortOrder stored;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            TextSortOrder header = harness.Database.Format.DefaultTextSortOrder;
            stored = header.HasVersion
                ? new TextSortOrder(0x0409, header.Version == 0 ? (byte)1 : (byte)0, true)
                : new TextSortOrder(0, 0, false);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    await writer.CreateTableAsync("TextJoinParent", [new ColumnDefinition("Code", typeof(string), 40) { IsPrimaryKey = true, TextSortOrderOverride = stored }], ct);
                    await writer.CreateTableAsync("TextJoinChild", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PCode", typeof(string), 40) { TextSortOrderOverride = stored }], ct);
                    await writer.CreateRelationshipAsync(new RelationshipDefinition("TextJoinRel", "TextJoinParent", "Code", "TextJoinChild", "PCode"), ct);
                    await writer.InsertRowAsync("TextJoinParent", ["ABC"], ct);
                    await writer.InsertRowAsync("TextJoinChild", [1, "abc"], ct);
                    for (int index = 0; index < fillerRows; index++)
                    {
                        string code = $"Other{index}";
                        await writer.InsertRowAsync("TextJoinParent", [code], ct);
                        await writer.InsertRowAsync("TextJoinChild", [100 + index, code], ct);
                    }
                },
                ct);
        }

        stream.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            foreach (string table in new[] { "TextJoinParent", "TextJoinChild" })
            {
                string column = table == "TextJoinParent" ? "Code" : "PCode";
                IndexMetadata[] candidates = (await harness.Services.Indexes.ListSeekableIndexesAsync(table, ct))
                    .Where(value => value.Columns.Count == 1 && value.Columns[0].Name == column)
                    .ToArray();
                Assert.NotEmpty(candidates);
                foreach (IndexMetadata index in candidates)
                {
                    Assert.False(await harness.Services.Indexes.CanSeekJoinKeysAsync(table, index, [["ABC"]], ct));
                }
            }
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        TextJoinParent parent = Assert.Single(await reader.Query<TextJoinParent>("TextJoinParent").Where(row => row.Code == "ABC").Include(row => row.Children).ToListAsync(ct));
        Assert.Equal(1, Assert.Single(parent.Children).Id);
        TextJoinChild child = Assert.Single(await reader.Query<TextJoinChild>("TextJoinChild").Where(row => row.Id == 1).Include(row => row.Parent).ToListAsync(ct));
        Assert.Equal("ABC", Assert.IsType<TextJoinParent>(child.Parent).Code);
    }

    [Theory]
    [InlineData(false, (byte)0)]
    [InlineData(true, (byte)0)]
    [InlineData(true, (byte)1)]
    public void LongTextNormalization_KeepsSuffixAndUncappedComparison(bool hasVersion, byte version)
    {
        var order = new TextSortOrder(0x0409, version, hasVersion);
        string lower = new('a', 255);
        Assert.Equal(IncludeLoader.Normalize(lower, order), IncludeLoader.Normalize(lower.ToUpperInvariant(), order));
        Assert.NotEqual(IncludeLoader.Normalize(lower + "x", order), IncludeLoader.Normalize(lower + "y", order));
        string expanding = new('ۗ', 255);
        Assert.NotNull(IncludeLoader.Normalize(expanding, order));
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task LongIndexPrefixCollision_CollectionAndReferenceKeepOnlyFullKeyMatches(string fixture, WriteMode mode, int fillerRows)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string prefix = new('a', 254);
        string wanted = prefix + "x";
        string other = prefix + "y";
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    await writer.CreateTableAsync(
                        "TextJoinParent",
                        [new ColumnDefinition("Code", typeof(string), 255)],
                        [new IndexDefinition("IX_Code", "Code")],
                        ct);
                    await writer.CreateTableAsync(
                        "TextJoinChild",
                        [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PCode", typeof(string), 255)],
                        [new IndexDefinition("IX_PCode", "PCode")],
                        ct);
                    await writer.InsertRowsAsync("TextJoinParent", [[other], [wanted]], ct);
                    await writer.InsertRowsAsync("TextJoinChild", [[2, other], [1, wanted]], ct);
                    for (int index = 0; index < fillerRows; index++)
                    {
                        string code = $"Other{index}";
                        await writer.InsertRowAsync("TextJoinParent", [code], ct);
                        await writer.InsertRowAsync("TextJoinChild", [100 + index, code], ct);
                    }
                },
                ct);
        }

        // Catalog metadata permits a nonunique parent index so colliding
        // index prefixes can exercise the residual on both navigation paths.
        await ForeignKeyTestDatabase.PlantRelationshipAsync(stream, "LongJoinRel", "TextJoinChild", "PCode", "TextJoinParent", "Code");
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        TextJoinParent parent = Assert.Single(await reader.Query<TextJoinParent>("TextJoinParent").Where(row => row.Code == wanted).Include(row => row.Children).ToListAsync(ct));
        Assert.Equal(1, Assert.Single(parent.Children).Id);
        TextJoinChild child = Assert.Single(await reader.Query<TextJoinChild>("TextJoinChild").Where(row => row.Id == 1).Include(row => row.Parent).ToListAsync(ct));
        Assert.Equal(wanted, Assert.IsType<TextJoinParent>(child.Parent).Code);
    }

    [Fact]
    public void CanonicallyEquivalentText_AcrossFormerWindowBoundary_HasSameJoinKey()
    {
        var order = new TextSortOrder(0x0409, 0, true);
        string prefix = new('a', 126);
        Assert.Equal(IncludeLoader.Normalize(prefix + "éZ", order), IncludeLoader.Normalize(prefix + "éZ", order));
    }

#pragma warning disable CA1812 // The query materializer constructs these entity types.
    internal sealed class TextJoinParent
    {
        public string Code { get; set; } = string.Empty;

        public List<TextJoinChild> Children { get; set; } = [];
    }

    internal sealed class TextJoinChild
    {
        public int Id { get; set; }

        public string PCode { get; set; } = string.Empty;

        public TextJoinParent? Parent { get; set; }
    }
#pragma warning restore CA1812
}
