namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Unsupported indexed text is refused before rows, cascades or catalog artifacts change.</summary>
/// <param name="cache">Caches fixture bytes.</param>
public sealed class UnsupportedTextCollationTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    public static TheoryData<DatabaseFormat, WriteMode, string> Cases
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, WriteMode, string>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
                {
                    foreach (string operation in new[] { "insert", "update", "delete", "add", "rename", "dropColumn", "relationship", "cascadeDelete", "cascadeUpdate" })
                    {
                        data.Add(format, mode, operation);
                    }
                }
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task IndexedUnsupportedSortOrder_RefusesWithoutChangingFile(DatabaseFormat format, WriteMode mode, string operation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateAsync(cache, format, [[1, "one"]], []);
        stream.Position = 0;
        await using (AccessWriter seed = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await seed.CreateTableAsync(
                "Bad",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("Note", typeof(string), 40), new ColumnDefinition("Extra", typeof(int))],
                [new IndexDefinition("IX_Note", "Note")],
                ct);
            await seed.InsertRowAsync("Bad", [1, 1, "caf\u00e9", 9], ct);
            if (operation != "relationship")
            {
                await seed.CreateRelationshipAsync(new RelationshipDefinition("BadParent", "P", "Id", "Bad", "ParentId") { CascadeDeletes = true, CascadeUpdates = true }, ct);
            }
        }

        await CollationTestSupport.PatchSortOrderAsync(stream, "Bad", "Note", 0x041D, ct);
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                {
                    switch (operation)
                    {
                        case "insert":
                            await writer.InsertRowAsync("Bad", [2, 1, "other", 9], ct);
                            break;
                        case "update":
                            await writer.UpdateRowsAsync("Bad", "Id", 1, new RowValues { ["Note"] = "other" }, ct);
                            break;
                        case "delete":
                            await writer.DeleteRowsAsync("Bad", "Id", 1, ct);
                            break;
                        case "add":
                            await writer.AddColumnAsync("Bad", new ColumnDefinition("Added", typeof(int)), ct);
                            break;
                        case "rename":
                            await writer.RenameColumnAsync("Bad", "Note", "Renamed", ct);
                            break;
                        case "dropColumn":
                            await writer.DropColumnAsync("Bad", "Extra", ct);
                            break;
                        case "relationship":
                            await writer.CreateRelationshipAsync(new RelationshipDefinition("BadParent", "P", "Id", "Bad", "ParentId"), ct);
                            break;
                        case "cascadeDelete":
                            await writer.DeleteRowsAsync("P", "Id", 1, ct);
                            break;
                        case "cascadeUpdate":
                            await writer.UpdateRowsAsync("P", "Id", 1, new RowValues { ["Id"] = 2 }, ct);
                            break;
                        default:
                            throw new InvalidOperationException(operation);
                    }
                });
                Assert.Contains("Bad", error.Message, StringComparison.Ordinal);
            }, ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task UnsupportedCatalogNameIndex_RefusesCreateTableBeforeArtifacts(WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.TestV2010, ct);
        await CollationTestSupport.PatchSortOrderAsync(stream, "MSysObjects", "Name", 0x041D, ct);
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.CreateTableAsync("NeverCreated", [new ColumnDefinition("Id", typeof(int))], ct));
                Assert.Contains("MSysObjects", error.Message, StringComparison.Ordinal);
            }, ct);
        }

        Assert.Equal(before, stream.ToArray());
    }
    [Theory]
    [InlineData(WriteMode.Direct, false)]
    [InlineData(WriteMode.AutoCommit, false)]
    [InlineData(WriteMode.ExplicitCommit, false)]
    [InlineData(WriteMode.Direct, true)]
    [InlineData(WriteMode.AutoCommit, true)]
    [InlineData(WriteMode.ExplicitCommit, true)]
    public async Task RelationshipTargetSort_IsCheckedOnBothSidesBeforeAddingIndexes(WriteMode mode, bool patchParent)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.TestV2010, ct);
        stream.Position = 0;
        await using (AccessWriter seed = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await seed.CreateTableAsync("TextParent", [new ColumnDefinition("Code", typeof(string), 40) { IsPrimaryKey = true }], ct);
            await seed.CreateTableAsync("TextChild", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParentCode", typeof(string), 40)], ct);
            await seed.InsertRowAsync("TextParent", ["one"], ct);
            await seed.InsertRowAsync("TextChild", [1, "one"], ct);
        }

        await CollationTestSupport.PatchSortOrderAsync(stream, patchParent ? "TextParent" : "TextChild", patchParent ? "Code" : "ParentCode", 0x041D, ct);
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.CreateRelationshipAsync(new RelationshipDefinition("TextRel", "TextParent", "Code", "TextChild", "ParentCode"), ct));
                Assert.Contains(patchParent ? "TextParent" : "TextChild", error.Message, StringComparison.Ordinal);
            }, ct);
        }

        Assert.Equal(before, stream.ToArray());
    }
}
