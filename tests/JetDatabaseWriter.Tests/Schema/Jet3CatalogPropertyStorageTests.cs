namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Jet3 catalog properties use native external storage even below the ordinary inline limit.</summary>
public sealed class Jet3CatalogPropertyStorageTests
{
    /// <summary>DAO3.6 Field.Required=true produces this 43-byte payload with an external descriptor.</summary>
    private const string NativeRequiredPayload = "4B4B44001000000080000800526571756972656417000000010008000000020049640900010100000100FF";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task CatalogProperties_CreateRewriteAndRuleEditsRemainExternal(WriteMode mode)
    {
        await using MemoryStream stream = await CreateAsync();
        await EditAsync(stream, mode, writer => writer.CreateTableAsync(
            "T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Spare", typeof(int))], Ct).AsTask());
        PropertySnapshot created = await ReadAsync(stream);
        Assert.Equal(Convert.FromHexString(NativeRequiredPayload), created.Payload);

        await EditAsync(stream, mode, writer => writer.AddColumnAsync("T", new ColumnDefinition("Added", typeof(int)) { IsNullable = false }, Ct).AsTask());
        PropertySnapshot added = await ReadAsync(stream);
        Assert.NotNull(ColumnPropertyBlock.Parse(added.Payload, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb))?.FindTarget("Added"));

        await EditAsync(stream, mode, writer => writer.RenameColumnAsync("T", "Added", "Renamed", Ct).AsTask());
        PropertySnapshot renamed = await ReadAsync(stream);
        Assert.NotNull(ColumnPropertyBlock.Parse(renamed.Payload, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb))?.FindTarget("Renamed"));

        await EditAsync(stream, mode, writer => writer.DropColumnAsync("T", "Renamed", Ct).AsTask());
        Assert.Equal(created.Payload, (await ReadAsync(stream)).Payload);

        await EditAsync(stream, mode, writer => writer.SetTableValidationRuleAsync("T", new TableValidationRule("[Id] > 0", "positive"), Ct).AsTask());
        Assert.NotNull(ColumnPropertyBlock.Parse((await ReadAsync(stream)).Payload, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb))?.FindTableTarget());
        await EditAsync(stream, mode, writer => writer.SetTableValidationRuleAsync("T", null, Ct).AsTask());
        ColumnPropertyBlock cleared = Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse((await ReadAsync(stream)).Payload, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb)));
        Assert.Equal([0xFF], cleared.FindTarget("Id")?.Find(Constants.ColumnPropertyNames.Required)?.Value);
        Assert.All(cleared.Targets, target => Assert.Null(target.Find(Constants.ColumnPropertyNames.ValidationRule)));
    }

    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task CatalogProperties_RefusalAndCallerRollbackPreserveExternalPayload(WriteMode mode)
    {
        await using MemoryStream stream = await CreateAsync();
        await EditAsync(stream, mode, async writer =>
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Spare", typeof(int))], Ct);
            await writer.InsertRowAsync("T", [1, 7], Ct);
        });
        PropertySnapshot original = await ReadAsync(stream);
        byte[] before = stream.ToArray();
        await EditAsync(stream, mode, async writer =>
            await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.SetTableValidationRuleAsync("T", new TableValidationRule("[Id] > 1"), Ct)));
        Assert.Equal(before, stream.ToArray());

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.AddColumnAsync("T", new ColumnDefinition("Added", typeof(int)) { Description = "rolled back" }, Ct);
            await writer.SetTableValidationRuleAsync("T", new TableValidationRule("[Id] > 0"), Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
        Assert.Equal(original.Payload, (await ReadAsync(stream)).Payload);
        await EditAsync(stream, mode, writer => writer.InsertRowAsync("T", [2, 8], Ct).AsTask());
        Assert.Equal(original.Payload, (await ReadAsync(stream)).Payload);
    }

    [Fact]
    public async Task OversizedCatalogProperties_RefuseBeforeDeletingOriginalRow()
    {
        await using MemoryStream stream = await CreateAsync();
        await EditAsync(stream, WriteMode.Direct, writer => writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct).AsTask());
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", Ct));
            byte[] oversized = new byte[Constants.LongValue.MaxPayloadBytes + 1];
            await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("T", entry.TDefPage, oversized, cancellationToken: Ct));
        }

        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<MemoryStream> CreateAsync()
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
        }

        return stream;
    }

    private static async Task EditAsync(MemoryStream stream, WriteMode mode, Func<AccessWriter, Task> edit)
    {
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct);
        await WriteModes.RunAsync(writer, mode, () => edit(writer), Ct);
    }

    private static async Task<PropertySnapshot> ReadAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        JetFormat format = reader.Database.Format;
        TableDef objects = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(2, Ct));
        ColumnInfo name = Assert.IsType<ColumnInfo>(objects.FindColumn("Name"));
        ColumnInfo properties = Assert.IsType<ColumnInfo>(objects.FindColumn("LvProp"));
        var descriptors = new List<byte[]>();
        await reader.Database.OwnedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                RowLocation location = row.Location;
                if (ScalarColumnReader.DecodeSimpleColumnValue(format, row.Page, location.RowStart, location.RowSize, name) == "T")
                {
                    Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, row.Page, location.RowStart, location.RowSize, hasVarColumns: true, out RowLayout layout));
                    ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, row.Page, location.RowStart, location.RowSize, layout, properties);
                    descriptors.Add(row.Page.AsSpan(location.RowStart + slice.DataStart, slice.DataLen).ToArray());
                }

                return new ValueTask<bool>(true);
            },
            Ct);
        byte[] descriptor = Assert.Single(descriptors);
        Assert.Equal(12, descriptor.Length);
        uint lengthAndFlags = BinaryPrimitives.ReadUInt32LittleEndian(descriptor);
        Assert.Equal(0x40000000U, lengthAndFlags & 0xC0000000U);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        byte[] payload = Assert.IsType<byte[]>(Assert.Single(catalog.Select("Name = 'T'"))["LvProp"]);
        Assert.Equal((uint)payload.Length, lengthAndFlags & 0x3FFFFFFFU);
        return new PropertySnapshot(payload);
    }

    private sealed record PropertySnapshot(byte[] Payload);
}
