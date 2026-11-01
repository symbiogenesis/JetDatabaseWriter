namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Editing catalog properties retains unrelated native long-value storage.</summary>
public sealed class CatalogLongValuePreservationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets each catalog update, native format, storage form and write mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode, string, int> Cases()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, string, int>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (string operation in new[] { "rule", "column", "relationship" })
                {
                    data.Add(format, mode, operation, 16);
                    data.Add(format, mode, operation, 128);
                    data.Add(format, mode, operation, 5000);
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task PublicSchemaMutation_RetainsUnassignedDescriptorAndPayload(DatabaseFormat format, WriteMode mode, string operation, int payloadLength)
    {
        await using var stream = new MemoryStream();
        string objectName = operation == "relationship" ? "FK_C_P" : "P";
        byte[] payload = Enumerable.Range(0, payloadLength).Select(index => unchecked((byte)index)).ToArray();
        await SeedAsync(stream, format, objectName, payload);
        Snapshot original = await ReadAsync(stream, objectName);
        Assert.Equal(payloadLength > 64, original.Rows.Count > 0);
        stream.Position = 0;
        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            UseTransactionalWrites = mode == WriteMode.AutoCommit,
            SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
        };
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, options, cancellationToken: Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    switch (operation)
                    {
                        case "rule":
                            await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 0"), Ct);
                            break;
                        case "column":
                            await writer.RenameColumnAsync("P", "Label", "Renamed", Ct);
                            break;
                        default:
                            await writer.RenameRelationshipAsync(objectName, "RenamedRelationship", Ct);
                            break;
                    }
                },
                Ct);
        }

        Snapshot actual = await ReadAsync(stream, operation == "relationship" ? "RenamedRelationship" : objectName);
        Assert.Equal(original.Field, actual.Field);
        Assert.Equal(original.Rows.Count, actual.Rows.Count);
        for (int index = 0; index < original.Rows.Count; index++)
        {
            Assert.Equal(original.Rows[index], actual.Rows[index]);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        Assert.Equal(payload, Assert.IsType<byte[]>(Assert.Single(catalog.AsEnumerable(), row => (string)row["Name"] == (operation == "relationship" ? "RenamedRelationship" : objectName))["RmtInfoLong"]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CatalogPropertyMutation_RollbackRestoresEveryByte(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await SeedAsync(stream, format, "P", new byte[5000]);
        byte[] original = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 0"), Ct);
            await transaction.RollbackAsync(Ct);
            Assert.Equal(original, stream.ToArray());
            await writer.InsertRowAsync("P", [1, "retained"], Ct);
            await writer.DeleteRowsAsync("P", "Id", 1, Ct);
        }

        // Check the catalog restoration independently of subsequent row edits.
        Snapshot snapshot = await ReadAsync(stream, "P");
        await using var originalStream = new MemoryStream(original);
        Snapshot expected = await ReadAsync(originalStream, "P");
        Assert.Equal(expected.Field, snapshot.Field);
        Assert.Equal(expected.Rows, snapshot.Rows);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CatalogPropertyMutation_PartialWriteRestoresRetainedStorageAndCanRetry(DatabaseFormat format)
    {
        await using var seed = new MemoryStream();
        await SeedAsync(seed, format, "P", new byte[5000]);
        byte[] original = seed.ToArray();
        Snapshot expected = await ReadAsync(seed, "P");
        int writes;
        await using (WriteFaultStream baseline = Copy(original))
        {
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(baseline, cancellationToken: Ct);
            int initial = baseline.WriteCount;
            await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 0"), Ct);
            writes = baseline.WriteCount - initial;
        }

        Assert.True(writes > 0);
        for (int point = 1; point <= writes; point++)
        {
            await using WriteFaultStream stream = Copy(original);
            await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
            {
                stream.FailDuringWrite(point);
                await Assert.ThrowsAnyAsync<IOException>(async () => await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 0"), Ct));
                Assert.True(stream.Faulted);
                Assert.Equal(original, stream.ToArray());
                await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 0"), Ct);
            }

            Snapshot actual = await ReadAsync(stream, "P");
            Assert.Equal(expected.Field, actual.Field);
            Assert.Equal(expected.Rows, actual.Rows);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CatalogPropertyMutation_SharedRootWithDifferentTokenIsRetained(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await SeedAsync(stream, format, "P", new byte[128]);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 0"), Ct);
            (RowLocation location, ColumnSlice source) = await FindFieldAsync(writer, "P", "LvProp");
            (_, ColumnSlice target) = await FindFieldAsync(writer, "P");
            byte[] page = await writer.Pager.ReadPageAsync(location.DataPageNumber, Ct);
            try
            {
                Assert.Equal(Constants.LongValue.HeaderSize, source.DataLen);
                Assert.Equal(source.DataLen, target.DataLen);
                page.AsSpan(location.RowStart + source.DataStart, source.DataLen).CopyTo(page.AsSpan(location.RowStart + target.DataStart));
                BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(location.RowStart + target.DataStart + 8, 4), 0x98765432);
                await writer.Pager.WritePageAsync(location.DataPageNumber, page, Ct);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        Snapshot expected = await ReadAsync(stream, "P");
        Assert.NotEmpty(expected.Rows);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 1"), Ct);
        }

        Snapshot actual = await ReadAsync(stream, "P");
        Assert.Equal(expected.Field, actual.Field);
        Assert.Equal(expected.Rows, actual.Rows);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CatalogPropertyMutation_SharedInteriorTailSurvivesSecureEraseAndRollback(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await SeedAsync(stream, format, "P", new byte[128], largeProperties: true);
        byte[] expectedPayload;
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            (RowLocation location, ColumnSlice source) = await FindFieldAsync(writer, "P", "LvProp");
            (_, ColumnSlice target) = await FindFieldAsync(writer, "P");
            byte[] page = await writer.Pager.ReadPageAsync(location.DataPageNumber, Ct);
            try
            {
                Assert.True(LongValueDescriptor.TryRead(page.AsSpan(location.RowStart + source.DataStart, source.DataLen), out LongValueDescriptor descriptor));
                Assert.True(descriptor.UsesChainedPages);
                byte[] root = await writer.Database.Pages.ReadPageCopyAsync(descriptor.FirstDp >> 8, Ct);
                Assert.True(DataPageRows.TryGetSlotBound(writer.Database.Format, root, checked((int)(descriptor.FirstDp & 0xFF)), out RowBound bound));
                uint tail = BinaryPrimitives.ReadUInt32LittleEndian(root.AsSpan(bound.RowStart));
                int rootPayload = bound.RowSize - sizeof(uint);
                Assert.NotEqual(0u, tail);
                LongValueDescriptor.Chained(descriptor.Length - rootPayload, tail, 0x98765432).WriteTo(page.AsSpan(location.RowStart + target.DataStart, target.DataLen));
                await writer.Pager.WritePageAsync(location.DataPageNumber, page, Ct);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        Snapshot expected = await ReadAsync(stream, "P");
        byte[] original = stream.ToArray();
        stream.Position = 0;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
            expectedPayload = Assert.IsType<byte[]>(Assert.Single(catalog.AsEnumerable(), row => Equals(row["Name"], "P"))["RmtInfoLong"]);
        }

        var options = new AccessWriterOptions { UseLockFile = false, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages };
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, options, cancellationToken: Ct))
        {
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
            {
                await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 0"), Ct);
                await transaction.RollbackAsync(Ct);
            }

            Assert.Equal(original, stream.ToArray());
            await writer.SetTableValidationRuleAsync("P", new TableValidationRule("[Id] > 1"), Ct);
        }

        Snapshot actual = await ReadAsync(stream, "P");
        Assert.Equal(expected.Field, actual.Field);
        Assert.Equal(expected.Rows, actual.Rows);
        stream.Position = 0;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
            Assert.Equal(expectedPayload, Assert.IsType<byte[]>(Assert.Single(catalog.AsEnumerable(), row => Equals(row["Name"], "P"))["RmtInfoLong"]));
        }
    }

    private static WriteFaultStream Copy(byte[] bytes)
    {
        var stream = new WriteFaultStream();
        stream.Write(bytes);
        stream.Position = 0;
        return stream;
    }

    private static async Task SeedAsync(MemoryStream stream, DatabaseFormat format, string objectName, byte[] payload, bool largeProperties = false)
    {
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string))], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "Id"), Ct);
            TableDef definition = await writer.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
            LocatedRow original = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(2, Ct), row => Equals(row.Values[definition.FindColumnIndex("Name")], objectName));
            object[] replacement = (object[])original.Values.Clone();
            replacement[definition.FindColumnIndex("RmtInfoLong")] = payload;
            if (largeProperties)
            {
                var properties = new ColumnPropertyBlockBuilder();
                properties.GetOrAddTableTarget().Entries.Add(new ColumnPropertyEntryBuilder
                {
                    Name = "Description",
                    DataType = ColumnType.MemoType,
                    Value = writer.Database.Format.EncodeText(new string('d', 5000), compress: false),
                });
                replacement[definition.FindColumnIndex("LvProp")] = Assert.IsType<byte[]>(properties.ToBytes(writer.Database.Format));
            }

            var plan = new MutationPlan();
            plan.ForTable(2, definition, "MSysObjects", MutationPlan.IndexPolicy.CatalogRelocation).Update(original.Location, original.Values, replacement);
            await writer.Services.Mutations.ApplyAsync(plan, Ct);
        }

        // Opaque native descriptor words are not part of the decoded payload.
        // Give them a recognizable value to expose decode/re-encode loss even
        // when the allocator happens to reuse the old bookmark.
        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        (RowLocation location, ColumnSlice slice) = await FindFieldAsync(harness, objectName);
        byte[] page = await harness.Pager.ReadPageAsync(location.DataPageNumber, Ct);
        try
        {
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(location.RowStart + slice.DataStart + 8, 4), 0x12345678);
            await harness.Pager.WritePageAsync(location.DataPageNumber, page, Ct);
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private static async Task<Snapshot> ReadAsync(MemoryStream stream, string objectName)
    {
        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        (RowLocation location, ColumnSlice slice) = await FindFieldAsync(harness, objectName);
        byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.DataPageNumber, Ct);
        byte[] field = page.AsSpan(location.RowStart + slice.DataStart, slice.DataLen).ToArray();
        Assert.True(LongValueDescriptor.TryRead(field, out LongValueDescriptor descriptor));
        var rows = new List<byte[]>();
        uint pointer = descriptor.IsExternal ? descriptor.FirstDp : 0;
        while (pointer != 0)
        {
            Assert.InRange(rows.Count, 0, 3);
            byte[] payloadPage = await harness.Database.Pages.ReadPageCopyAsync(pointer >> 8, Ct);
            Assert.True(DataPageRows.TryGetSlotBound(harness.Database.Format, payloadPage, checked((int)(pointer & 0xFF)), out RowBound bound));
            byte[] row = payloadPage.AsSpan(bound.RowStart, bound.RowSize).ToArray();
            rows.Add(row);
            pointer = descriptor.IsSinglePage ? 0 : BinaryPrimitives.ReadUInt32LittleEndian(row);
        }

        return new Snapshot(field, rows);
    }

    private static async Task<(RowLocation Location, ColumnSlice Slice)> FindFieldAsync(AccessWriter writer, string objectName, string columnName = "RmtInfoLong")
    {
        TableDef definition = await writer.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
        LocatedRow row = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(2, Ct), value => Equals(value.Values[definition.FindColumnIndex("Name")], objectName));
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(row.Location.DataPageNumber, Ct);
        Assert.True(RowDecodePlan.TryParseRowLayout(writer.Database.Format.RowFields, page, row.Location.RowStart, row.Location.RowSize, definition.HasVarColumns, out RowLayout layout));
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(writer.Database.Format.RowFields, page, row.Location.RowStart, row.Location.RowSize, layout, Assert.IsType<ColumnInfo>(definition.FindColumn(columnName)));
        return (row.Location, slice);
    }

    private sealed record Snapshot(byte[] Field, List<byte[]> Rows);
}
