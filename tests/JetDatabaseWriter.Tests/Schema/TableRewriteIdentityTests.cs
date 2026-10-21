namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Every schema rebuild publishes storage under the original catalog identity.</summary>
public sealed class TableRewriteIdentityTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format, rewrite and write mode.</summary>
    public static TheoryData<DatabaseFormat, string, WriteMode> FormatsAndOperations()
    {
        var data = new TheoryData<DatabaseFormat, string, WriteMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (string operation in new[] { "add", "drop", "rename" })
            {
                foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                {
                    data.Add(format, operation, mode);
                }
            }
        }

        return data;
    }

    /// <summary>Gets complex-column mutations in every write mode.</summary>
    public static TheoryData<string, WriteMode> ComplexOperations()
    {
        var data = new TheoryData<string, WriteMode>();
        foreach (string operation in new[] { "add", "drop", "rename" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(operation, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(FormatsAndOperations))]
    public async Task OrdinaryRewrite_PreservesTableIdentityCatalogAndPermissions(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)) { IsPrimaryKey = true },
            new("Spare", typeof(int)),
            new("Payload", typeof(string)),
        };
        columns.AddRange(Enumerable.Range(0, 100).Select(index => new ColumnDefinition($"ContinuationColumn{index:D3}", typeof(int))));
        string payload = new('x', 12000);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Original", columns, Ct);
            await writer.InsertRowAsync("Original", [7, 9, payload, .. Enumerable.Range(0, 100).Select(index => (object)index)], Ct);
            await writer.CreateTableAsync("Unrelated", [new("Id", typeof(int))], Ct);
            await writer.InsertRowAsync("Unrelated", [99], Ct);
        }

        CatalogSnapshot before = await ReadCatalogAsync(stream, "Original");
        if (format != DatabaseFormat.Jet3Mdb)
        {
            Assert.NotEmpty(before.Permissions);
        }

        await AssertContinuationAsync(stream, before.Page);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            writer.Database.TableDefs.VerifyOnHit = true;
            _ = await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct);
            await WriteModes.RunAsync(writer, mode, async () => await RewriteAsync(writer, "Original", "Spare", operation), Ct);
            ResolvedTable rewritten = await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct);
            Assert.Equal(before.Page, rewritten.Entry.TDefPage);
            Assert.Equal(operation switch { "drop" => 102, "add" => 104, _ => 103 }, rewritten.Definition.Columns.Count);
        }

        CatalogSnapshot after = await ReadCatalogAsync(stream, "Original");
        AssertCatalogUnchanged(before, after);
        await AssertContinuationAsync(stream, before.Page);
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal(["Original", "Unrelated"], (await reader.ListTablesAsync(Ct)).Order(StringComparer.Ordinal));
        using DataTable rows = await reader.ReadTableAsync("Original", cancellationToken: Ct);
        DataRow row = Assert.Single(rows.AsEnumerable());
        Assert.Equal(payload, row["Payload"]);
        Assert.Equal(99, row["ContinuationColumn099"]);
        if (operation != "drop")
        {
            Assert.Equal(9, row[operation == "rename" ? "Renamed" : "Spare"]);
        }

        IndexMetadata primary = Assert.Single(await reader.ListIndexesAsync("Original", Ct), index => index.Kind == IndexKind.PrimaryKey);
        Assert.Single(await reader.SeekRowsAsync("Original", primary.Name, [7], Ct).ToListAsync(Ct));
        using DataTable unrelated = await reader.ReadTableAsync("Unrelated", cancellationToken: Ct);
        Assert.Equal(99, Assert.Single(unrelated.AsEnumerable())["Id"]);
    }

    [Theory]
    [MemberData(nameof(ComplexOperations))]
    public async Task ComplexRewrite_PreservesParentAndSurvivingFlatTableIdentities(string operation, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Original", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Files", typeof(byte[])) { IsAttachment = true }, new("KeepFiles", typeof(byte[])) { IsAttachment = true }], Ct);
            await writer.InsertRowAsync("Original", [1, DBNull.Value, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Original", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("drop-or-rename.txt", [1, 2, 3]), Ct);
            await writer.AddAttachmentAsync("Original", "KeepFiles", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("keep.txt", [5, 8, 13]), Ct);
        }

        CatalogSnapshot before = await ReadCatalogAsync(stream, "Original");
        ComplexColumnInfo[] original;
        stream.Position = 0;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            original = [.. await reader.GetComplexColumnsAsync("Original", Ct)];
        }

        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    if (operation == "add")
                    {
                        await writer.AddColumnAsync("Original", new ColumnDefinition("Added", typeof(byte[])) { IsAttachment = true }, Ct);
                    }
                    else
                    {
                        await RewriteAsync(writer, "Original", "Files", operation);
                    }

                    Assert.Equal(before.Page, (await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct)).Entry.TDefPage);
                },
                Ct);
        }

        AssertCatalogUnchanged(before, await ReadCatalogAsync(stream, "Original"));
        stream.Position = 0;
        await using AccessReader reopened = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal("Original", Assert.Single(await reopened.ListTablesAsync(Ct)));
        IReadOnlyList<ComplexColumnInfo> actual = await reopened.GetComplexColumnsAsync("Original", Ct);
        foreach (ComplexColumnInfo old in original)
        {
            if (operation == "drop" && old.ColumnName == "Files")
            {
                Assert.DoesNotContain(actual, column => column.ComplexId == old.ComplexId);
                using DataTable objects = await reopened.ReadTableAsync("MSysObjects", cancellationToken: Ct);
                Assert.DoesNotContain(objects.AsEnumerable(), row => Equals(row["Id"], old.FlatTableId));
                continue;
            }

            string name = operation == "rename" && old.ColumnName == "Files" ? "Renamed" : old.ColumnName;
            ComplexColumnInfo survivor = Assert.Single(actual, column => column.ColumnName == name);
            Assert.Equal(old.ComplexId, survivor.ComplexId);
            Assert.Equal(old.FlatTableId, survivor.FlatTableId);
            Assert.Equal(old.ConceptualTableId, survivor.ConceptualTableId);
        }

        using DataTable complex = await reopened.ReadTableAsync("MSysComplexColumns", cancellationToken: Ct);
        foreach (ComplexColumnInfo column in actual)
        {
            DataRow metadata = Assert.Single(complex.AsEnumerable(), row => Equals(row["ComplexID"], column.ComplexId));
            Assert.Equal(checked((int)before.Page), metadata["ConceptualTableID"]);
        }

        Assert.Equal([5, 8, 13], Assert.Single(await reopened.GetAttachmentsAsync("Original", "KeepFiles", Ct)).FileData);
        if (operation != "drop")
        {
            Assert.Equal([1, 2, 3], Assert.Single(await reopened.GetAttachmentsAsync("Original", operation == "rename" ? "Renamed" : "Files", Ct)).FileData);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MultiPageIndexRewrite_LeavesNoPageOwnedByFreedReplacementRoot(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Original", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Payload", typeof(string))], Ct);
            await writer.InsertRowsAsync("Original", Enumerable.Range(0, 1000).Select(index => new object?[] { index, "memo" }), Ct);
            await writer.AddColumnAsync("Original", new ColumnDefinition("Added", typeof(int)), Ct);
        }

        stream.Position = 0;
        await using AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        int pageSize = reopened.Database.Format.PageSize;
        int intermediatePages = 0;
        for (long pageNumber = 3; pageNumber < reopened.Pager.PageCount; pageNumber++)
        {
            byte[] page = await reopened.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);
            bool index = page[0] is Constants.PageTypes.IndexLeaf or Constants.PageTypes.IndexIntermediate;
            if (page[0] == Constants.PageTypes.IndexIntermediate)
            {
                intermediatePages++;
            }

            if (index || (page[0] == Constants.PageTypes.Data && !page.AsSpan(4, 4).SequenceEqual("LVAL"u8)))
            {
                int owner = JetTypeInfo.Ri32(page, index ? 4 : reopened.Database.Format.DataPage.TDefOff);
                if (owner > 0)
                {
                    Assert.InRange(owner, 2, checked((int)(reopened.Pager.PageCount - 1)));
                    byte[] ownerPage = await reopened.Database.Pages.ReadPageCopyAsync(owner, Ct);
                    Assert.True(ownerPage[0] == Constants.PageTypes.TableDefinition, $"Page {pageNumber} still names non-TDEF owner page {owner} (type {ownerPage[0]}), page size {pageSize}.");
                }
            }
        }

        Assert.True(intermediatePages > 0, "The fixture must force a multi-page index rebuild.");
    }

    [Fact]
    public async Task FirstAndLastComplexColumnRewrite_KeepsOriginalParentIdentity()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Original", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.InsertRowAsync("Original", [1], Ct);
            long root = (await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct)).Entry.TDefPage;
            await writer.AddColumnAsync("Original", new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }, Ct);
            Assert.Equal(root, (await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct)).Entry.TDefPage);
            await writer.AddAttachmentAsync("Original", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("last.txt", [1]), Ct);
            await writer.DropColumnAsync("Original", "Files", Ct);
            Assert.Equal(root, (await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct)).Entry.TDefPage);
            await writer.InsertRowAsync("Original", [2], Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Empty(await reader.GetComplexColumnsAsync("Original", Ct));
        using DataTable complex = await reader.ReadTableAsync("MSysComplexColumns", cancellationToken: Ct);
        Assert.Empty(complex.AsEnumerable());
        using DataTable rows = await reader.ReadTableAsync("Original", cancellationToken: Ct);
        Assert.Equal([1, 2], rows.AsEnumerable().Select(row => Assert.IsType<int>(row["Id"])).Order());
        Assert.Equal("Original", Assert.Single(await reader.ListTablesAsync(Ct)));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RelatedRewrite_PreservesIdentityWithInboundOutboundAndSelfLinks(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        if (format == DatabaseFormat.Jet4Mdb)
        {
            await stream.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.AdventureWorks, Ct), Ct);
            stream.Position = 0;
        }
        else
        {
            await using AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct);
        }

        long root;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("IdentityParent", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("IdentityMiddle", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int)), new("SelfId", typeof(int)), new("Spare", typeof(int))], Ct);
            await writer.CreateTableAsync("IdentityChild", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("MiddleId", typeof(int))], Ct);
            await writer.InsertRowAsync("IdentityParent", [1], Ct);
            await writer.InsertRowAsync("IdentityMiddle", [2, 1, DBNull.Value, 42], Ct);
            await writer.InsertRowAsync("IdentityMiddle", [3, 1, 2, 43], Ct);
            await writer.InsertRowAsync("IdentityChild", [4, 2], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("IdentityOutbound", "IdentityParent", "Id", "IdentityMiddle", "ParentId"), Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("IdentityInbound", "IdentityMiddle", "Id", "IdentityChild", "MiddleId"), Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("IdentitySelf", "IdentityMiddle", "Id", "IdentityMiddle", "SelfId"), Ct);
            root = (await writer.Services.Catalog.ResolveRequiredTableAsync("IdentityMiddle", Ct)).Entry.TDefPage;
            foreach (string operation in new[] { "add", "rename", "drop" })
            {
                await RewriteAsync(writer, "IdentityMiddle", operation == "drop" ? "Renamed" : "Spare", operation);
                Assert.Equal(root, (await writer.Services.Catalog.ResolveRequiredTableAsync("IdentityMiddle", Ct)).Entry.TDefPage);
            }

            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("IdentityMiddle", [5, 999, DBNull.Value, DBNull.Value], Ct));
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("IdentityMiddle", [5, 1, 999, DBNull.Value], Ct));
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("IdentityChild", [5, 999], Ct));
            await writer.InsertRowAsync("IdentityMiddle", [5, 1, 2, 11], Ct);
            await writer.InsertRowAsync("IdentityChild", [5, 5], Ct);
        }

        Dictionary<long, List<IndexMetadata>> links = await ForeignKeyLinks.ReadForeignKeyEntriesAsync(stream);
        ForeignKeyLinks.AssertConsistent(links);
        Assert.Equal(4, links[root].Count);
        Assert.All(links[root], index => Assert.NotEqual(0, index.RelatedTablePage));
    }

    private static ValueTask RewriteAsync(AccessWriter writer, string table, string column, string operation) => operation switch
    {
        "add" => writer.AddColumnAsync(table, new ColumnDefinition("Added", typeof(int)), Ct),
        "drop" => writer.DropColumnAsync(table, column, Ct),
        _ => writer.RenameColumnAsync(table, column, "Renamed", Ct),
    };

    private static async Task<CatalogSnapshot> ReadCatalogAsync(MemoryStream stream, string name)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        DataRow table = Assert.Single(objects.AsEnumerable(), row => Equals(row["Name"], name));
        int id = Assert.IsType<int>(table["Id"]);
        string[] cells = [.. objects.Columns.Cast<DataColumn>().Where(column => column.ColumnName != "LvProp")
            .Select(column => column.ColumnName + "=" + Cell(table[column])),];
        if (reader.PageSize == Constants.PageSizes.Jet3)
        {
            return new CatalogSnapshot(id, cells, []);
        }

        using DataTable aces = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        string[] permissions = [.. aces.AsEnumerable().Where(row => Equals(row["ObjectId"], id))
            .Select(row => string.Join("|", row.ItemArray.Select(Cell))).Order(StringComparer.Ordinal),];
        return new CatalogSnapshot(id, cells, permissions);
    }

    private static string Cell(object? value) => value switch
    {
        byte[] bytes => Convert.ToHexString(bytes),
        DateTime date => date.Ticks.ToString(CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
    };

    private static void AssertCatalogUnchanged(CatalogSnapshot before, CatalogSnapshot after)
    {
        Assert.Equal(before.Page, after.Page);
        Assert.Equal(before.Cells, after.Cells);
        Assert.Equal(before.Permissions, after.Permissions);
    }

    private static async Task AssertContinuationAsync(MemoryStream stream, long root)
    {
        stream.Position = 0;
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(root, writer.Database.Format.PageSize, writer.Pager.ReadPageAsync, PageBuffers.Return, true, Ct);
        Assert.True(chain.PageNumbers.Count > 1);
    }

    private sealed record CatalogSnapshot(long Page, string[] Cells, string[] Permissions);
}
