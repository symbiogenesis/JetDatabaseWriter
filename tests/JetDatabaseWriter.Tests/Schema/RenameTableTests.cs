namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Table renames retain physical identities, constraints, links and complex values.</summary>
public sealed class RenameTableTests
{
    /// <summary>Gets each format and statement mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Rename_RetainsRootRowsIndexesAndRelationshipEnforcement(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Parents", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Payload", typeof(string)), new("SelfId", typeof(int))], Ct);
            await writer.CreateTableAsync("Children", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.CreateRelationshipAsync(new("FK", "Parents", "Id", "Children", "ParentId") { CascadeUpdates = true, CascadeDeletes = true }, Ct);
            await writer.CreateRelationshipAsync(new("Self", "Parents", "Id", "Parents", "SelfId"), Ct);
            await writer.InsertRowAsync("Parents", [1, new string('x', 5000), DBNull.Value], Ct);
            await writer.InsertRowAsync("Children", [7, 1], Ct);
            ResolvedTable before = await writer.Services.Catalog.ResolveRequiredTableAsync("Parents", Ct);
            byte[] definition = (await writer.Database.TableDefs.ReadTDefChainAsync(before.Entry.TDefPage, Ct)).Bytes.ToArray();
            await WriteModes.RunAsync(writer, mode, async () => await writer.RenameTableAsync("parents", "Renamed Parents", Ct), Ct);
            ResolvedTable after = await writer.Services.Catalog.ResolveRequiredTableAsync("Renamed Parents", Ct);
            Assert.Equal(before.Entry.TDefPage, after.Entry.TDefPage);
            Assert.Equal(definition, (await writer.Database.TableDefs.ReadTDefChainAsync(after.Entry.TDefPage, Ct)).Bytes);
            Assert.Null(await writer.Services.Catalog.GetCatalogEntryAsync("Parents", Ct));
            await writer.RenameTableAsync("Children", "Renamed Children", Ct);
            await writer.InsertRowAsync("Renamed Parents", [2, "second", 1], Ct);
            await writer.InsertRowAsync("Renamed Children", [8, 2], Ct);
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Renamed Children", [9, 3], Ct));
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Renamed Parents", [1, "duplicate", 1], Ct));
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal(["Renamed Children", "Renamed Parents"], (await reader.ListTablesAsync(Ct)).Order(StringComparer.Ordinal));
        using DataTable parents = await reader.ReadTableAsync("Renamed Parents", cancellationToken: Ct);
        Assert.Equal(new string('x', 5000), parents.AsEnumerable().Single(row => (int)row["Id"] == 1)["Payload"]);
        using DataTable relationships = await reader.ReadTableAsync("MSysRelationships", cancellationToken: Ct);
        Assert.All(relationships.AsEnumerable(), row => Assert.Equal("Renamed Parents", row["szReferencedObject"]));
        Assert.Contains(relationships.AsEnumerable(), row => (string)row["szObject"] == "Renamed Parents");
        Assert.Contains(relationships.AsEnumerable(), row => (string)row["szObject"] == "Renamed Children");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Rename_RollbackRestoresBytesAndWarmConstraintState(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)) { IsAutoIncrement = true }, new("Value", typeof(int)) { DefaultValue = 7 }], Ct);
        await writer.InsertRowAsync("T", new RowValues(), Ct);
        byte[] original = stream.ToArray();
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
        {
            await writer.RenameTableAsync("T", new string('N', 64), Ct);
            await writer.InsertRowAsync(new string('N', 64), new RowValues(), Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(original, stream.ToArray());
        await writer.InsertRowAsync("T", new RowValues(), Ct);
        await writer.RenameTableAsync("T", "t", Ct);
        await writer.InsertRowAsync("t", new RowValues(), Ct);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal("t", Assert.Single(await reader.ListTablesAsync(Ct)));
        using DataTable rows = await reader.ReadTableAsync("t", cancellationToken: Ct);
        Assert.Equal([1, 2, 3], rows.AsEnumerable().Select(row => (int)row["Id"]).Order());
        Assert.All(rows.AsEnumerable(), row => Assert.Equal(7, row["Value"]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameLinks_PreservesRemoteMetadataWithoutResolvingSources(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateLinkedTableAsync("AccessLink", "Z:\\JDW_NotPresent\\inert.mdb", "Foreign Table", Ct);
            await writer.CreateLinkedTextTableAsync("TextLink", "Z:\\JDW_NotPresent", "inert.csv", "Text;HDR=YES;FMT=Delimited", Ct);
            await writer.CreateLinkedOdbcTableAsync("OdbcLink", "ODBC;DSN=JDW_Inert_NotInstalled;", "dbo.Foreign", Ct);
        }

        LinkedTableInfo[] original;
        await using (AccessReader before = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            original = (await before.ListLinkedTablesAsync(Ct)).ToArray();
        }

        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            foreach (LinkedTableInfo link in original)
            {
                await writer.RenameTableAsync(link.Name, "Renamed " + link.Name, Ct);
            }
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        LinkedTableInfo[] renamed = (await reader.ListLinkedTablesAsync(Ct)).ToArray();
        Assert.Equal(original.Length, renamed.Length);
        foreach (LinkedTableInfo link in original)
        {
            Assert.Equal(link with { Name = "Renamed " + link.Name }, Assert.Single(renamed, candidate => candidate.Name == "Renamed " + link.Name));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InvalidMissingCollidingAndNoOpNames_RetainExactBytes(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int))], Ct);
        await writer.CreateTableAsync("Other", [new("Id", typeof(int))], Ct);
        await writer.CreateLinkedOdbcTableAsync("Link", "ODBC;DSN=JDW_Inert_NotInstalled;", "dbo.Foreign", Ct);
        byte[] original = stream.ToArray();
        JetOperationException missing = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.RenameTableAsync("Absent", "New", Ct));
        Assert.Equal(JetErrorCode.TableNotFound, missing.ErrorCode);
        await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.RenameTableAsync("T", "other", Ct));
        await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.RenameTableAsync("T", "link", Ct));
        await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.RenameTableAsync("T", "MSysObjects", Ct));
        await Assert.ThrowsAsync<JetArgumentException>(async () => await writer.RenameTableAsync("T", "Bad.Name", Ct));
        await writer.RenameTableAsync("t", "T", Ct);
        Assert.Equal(original, stream.ToArray());
    }

    [Fact]
    public async Task RenameComplexParent_RetainsDescriptorsAndItemIdentities()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new() { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Documents", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Files", typeof(byte[])) { IsAttachment = true }, new("Tags", typeof(string), 50) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
            await writer.InsertRowAsync("Documents", [1, DBNull.Value, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Documents", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("keep.txt", [1, 2, 3]), Ct);
            await writer.AddMultiValueItemAsync("Documents", "Tags", new Dictionary<string, object?> { ["Id"] = 1 }, "first", Ct);
        }

        int itemId;
        ComplexColumnInfo[] original;
        await using (AccessReader before = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            original = (await before.GetComplexColumnsAsync("Documents", Ct)).ToArray();
            itemId = Assert.Single(await before.GetMultiValueItemsAsync("Documents", "Tags", Ct)).Id;
        }

        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.RenameTableAsync("Documents", "Renamed Documents", Ct);
            await writer.AddMultiValueItemAsync("Renamed Documents", "Tags", new Dictionary<string, object?> { ["Id"] = 1 }, "second", Ct);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal(original, await reader.GetComplexColumnsAsync("Renamed Documents", Ct));
        Assert.Equal("keep.txt", Assert.Single(await reader.GetAttachmentsAsync("Renamed Documents", "Files", Ct)).FileName);
        Assert.Contains(await reader.GetMultiValueItemsAsync("Renamed Documents", "Tags", Ct), item => item.Id == itemId && Equals(item.Value, "first"));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameWriteFaults_RestoreBytesAndAllowSameWriterRetry(DatabaseFormat format)
    {
        byte[] original;
        await using (var seed = new MemoryStream())
        {
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(seed, format, new() { UseLockFile = false }, leaveOpen: true, Ct))
            {
                await writer.CreateTableAsync("P", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
                await writer.CreateTableAsync("C", [new("ParentId", typeof(int))], Ct);
                await writer.CreateRelationshipAsync(new("FK", "P", "Id", "C", "ParentId"), Ct);
                await writer.InsertRowAsync("P", [1], Ct);
            }

            original = seed.ToArray();
        }

        int writes;
        await using (WriteFaultStream stream = Copy(original))
        {
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
            int initial = stream.WriteCount;
            await writer.RenameTableAsync("P", new string('N', 64), Ct);
            writes = stream.WriteCount - initial;
        }

        Assert.True(writes > 0);
        for (int point = 1; point <= writes; point++)
        {
            foreach (bool partial in new[] { false, true })
            {
                await using WriteFaultStream stream = Copy(original);
                await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
                _ = await writer.Services.Catalog.ResolveRequiredTableAsync("P", Ct);
                if (partial)
                {
                    stream.FailDuringWrite(point);
                }
                else
                {
                    stream.FailOnWrite(point);
                }

                await Assert.ThrowsAsync<IOException>(async () => await writer.RenameTableAsync("P", new string('N', 64), Ct));
                Assert.True(stream.Faulted);
                Assert.Equal(original, stream.ToArray());
                await writer.RenameTableAsync("P", "Renamed", Ct);
                await writer.InsertRowAsync("C", [1], Ct);
                await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("C", [2], Ct));
            }
        }
    }

    [Theory]
    [InlineData("[T].[Price]", true)]
    [InlineData("[T]![Price]", true)]
    [InlineData("[T].Price", true)]
    [InlineData("T.Price", false)]
    [InlineData("T![Price]", false)]
    public async Task Rename_PreservesNativeSyntaxDependentCalculatedBindings(string reference, bool stale)
    {
        await using var stream = new MemoryStream();
        string expression = reference + " * 2";
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new() { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("T", [new("Price", typeof(int)), new("Calculated", typeof(int)) { IsCalculated = true, CalculationExpression = expression }], Ct);
            await writer.InsertRowAsync("T", [4, DBNull.Value], Ct);
            await writer.RenameTableAsync("T", "Renamed T", Ct);
            await writer.InsertRowAsync("Renamed T", [2, DBNull.Value], Ct);
            await writer.UpdateRowsAsync("Renamed T", "Price", 4, new Dictionary<string, object?> { ["Price"] = 5 }, Ct);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal(expression, (await reader.GetColumnMetadataAsync("Renamed T", Ct))[1].CalculationExpression?.TrimEnd('\0'));
        int count = 0;
        await foreach (object[] row in reader.Rows("Renamed T", cancellationToken: Ct))
        {
            int price = Assert.IsType<int>(row[0]);
            if (stale && price == 2)
            {
                Assert.Null(Assert.IsType<CalculatedError>(row[1]).NativeCode);
            }
            else
            {
                Assert.Equal(stale ? 8 : price * 2, row[1]);
            }

            count++;
        }

        Assert.Equal(2, count);
    }

    private static WriteFaultStream Copy(byte[] bytes)
    {
        var stream = new WriteFaultStream();
        stream.Write(bytes);
        stream.Position = 0;
        return stream;
    }
}
