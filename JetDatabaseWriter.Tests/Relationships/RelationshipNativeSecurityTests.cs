namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Relationship DDL refuses unsafe native security before changing any page.</summary>
public sealed class RelationshipNativeSecurityTests
{
    public static TheoryData<WriteMode, string, string> Cases()
    {
        var cases = new TheoryData<WriteMode, string, string>();
        string[] corruptions = ["owner", "container", "inheritance", "duplicate-sid"];
        string[] operations = ["create", "drop", "rename", "add-column", "drop-column", "rename-column"];
        foreach (WriteMode mode in Enum.GetValues<WriteMode>())
        {
            foreach (string corruption in corruptions)
            {
                foreach (string operation in operations)
                {
                    cases.Add(mode, corruption, operation);
                }
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task MalformedNativeSecurity_RefusesBeforeRelationshipMutation(WriteMode mode, string corruption, string operation)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(
            await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Schema.mdb"), TestContext.Current.CancellationToken),
            TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await OpenAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("SecurityParent", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("SecurityChild", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("Note", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(Relationship("ExistingSecurityRelationship"), TestContext.Current.CancellationToken);
        }

        await CorruptAsync(stream, corruption);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Func<Task> mutation = operation switch
                {
                    "create" => async () => await writer.CreateRelationshipAsync(Relationship("RejectedSecurityRelationship"), TestContext.Current.CancellationToken),
                    "drop" => async () => await writer.DropRelationshipAsync("ExistingSecurityRelationship", TestContext.Current.CancellationToken),
                    "rename" => async () => await writer.RenameRelationshipAsync("ExistingSecurityRelationship", "RenamedSecurityRelationship", TestContext.Current.CancellationToken),
                    "add-column" => async () => await writer.AddColumnAsync("SecurityChild", new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken),
                    "drop-column" => async () => await writer.DropColumnAsync("SecurityChild", "Note", TestContext.Current.CancellationToken),
                    "rename-column" => async () => await writer.RenameColumnAsync("SecurityChild", "Note", "RenamedNote", TestContext.Current.CancellationToken),
                    _ => throw new ArgumentException("Unknown mutation.", nameof(operation)),
                };
                if (corruption == "inheritance")
                {
                    await Assert.ThrowsAsync<NotSupportedException>(mutation);
                }
                else
                {
                    await Assert.ThrowsAsync<JetCorruptDataException>(mutation);
                }

                Assert.Equal(before, stream.ToArray());
            });
        }

        Assert.Equal(before, stream.ToArray());
    }

    private static RelationshipDefinition Relationship(string name)
        => new(name, "SecurityParent", "Id", "SecurityChild", "ParentId");

    private static ValueTask<AccessWriter> OpenAsync(MemoryStream stream, WriteMode mode)
    {
        stream.Position = 0;
        var options = new AccessWriterOptions("Native123")
        {
            UseLockFile = false,
            UseTransactionalWrites = mode == WriteMode.AutoCommit,
        };
        return AccessWriter.OpenAsync(stream, options, leaveOpen: true, TestContext.Current.CancellationToken);
    }

    private static async Task CorruptAsync(MemoryStream stream, string corruption)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(
            stream, new AccessWriterOptions("Native123") { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken);
        string table = corruption is "owner" or "container" ? "MSysObjects" : "MSysACEs";
        long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(table, TestContext.Current.CancellationToken);
        TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(page, table, TestContext.Current.CancellationToken);
        ColumnInfo target = Assert.IsType<ColumnInfo>(definition.FindColumn(table == "MSysObjects" ? "Owner" : "FInheritable"));
        ColumnInfo? name = definition.FindColumn("Name");
        var changed = new Dictionary<long, byte[]>();
        int count = 0;
        await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
            page,
            (row, _) =>
            {
                if (name is not null)
                {
                    string objectName = ScalarColumnReader.DecodeSimpleColumnValue(harness.Database.Format, row.Page, row.Location.RowStart, row.Location.RowSize, name);
                    if (!string.Equals(objectName, corruption == "owner" ? "MSysDb" : "Relationships", StringComparison.Ordinal))
                    {
                        return new ValueTask<bool>(true);
                    }
                }

                Assert.True(RowDecodePlan.TryParseRowLayout(
                    harness.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, hasVarColumns: true, out RowLayout layout));
                if (!changed.TryGetValue(row.Location.PageNumber, out byte[]? bytes))
                {
                    bytes = row.Page.AsSpan(0, harness.Database.Format.PageSize).ToArray();
                    changed.Add(row.Location.PageNumber, bytes);
                }

                int offset = row.Location.RowStart + layout.NullMaskPos + (target.ColNum / 8);
                int mask = 1 << (target.ColNum % 8);
                bytes[offset] = (byte)(corruption == "duplicate-sid" ? bytes[offset] | mask : bytes[offset] & ~mask);
                count++;
                return new ValueTask<bool>(true);
            },
            TestContext.Current.CancellationToken);
        Assert.True(count > 0);
        foreach (KeyValuePair<long, byte[]> pair in changed)
        {
            await harness.Pager.WritePageAsync(pair.Key, pair.Value, TestContext.Current.CancellationToken);
        }
    }
}
