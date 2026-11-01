namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native AllowZeroLength is independent of the field's Required property.</summary>
public sealed class AllowZeroLengthTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<DatabaseFormat, WriteMode, string> FieldCases()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, string>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (string kind in new[] { "Text", "Memo", "Hyperlink" })
                {
                    data.Add(format, mode, kind);
                }
            }
        }

        return data;
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertEmptyString_DefaultTextField_RefusesWithoutChangingBytes(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Value", typeof(string), 40)], Ct);
        byte[] before = stream.ToArray();
        JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("T", [string.Empty], Ct));
        Assert.Equal(JetErrorCode.ZeroLengthViolation, error.ErrorCode);
        Assert.Equal("T", error.ErrorInfo.TableName);
        Assert.Equal("Value", error.ErrorInfo.ColumnName);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(FieldCases))]
    public async Task EmptyString_DeclarationReopenAndSchemaEdits_KeepNativeConstraint(DatabaseFormat format, WriteMode mode, string kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), Field("Refuse", kind), Field("Accept", kind) with { AllowZeroLength = true, IsNullable = false }], Ct);
                await writer.InsertRowAsync("T", [1, "ok", string.Empty], Ct);
                await AssertRefusedAsync(writer, stream);
            },
                Ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                await AssertRefusedAsync(writer, stream);
                await writer.RenameColumnAsync("T", "Accept", "Accepted", Ct);
                await writer.AddColumnAsync("T", new ColumnDefinition("Spare", typeof(int)), Ct);
                await writer.DropColumnAsync("T", "Spare", Ct);
                await writer.InsertRowAsync("T", [2, null, string.Empty], Ct);
                await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Accepted"] = string.Empty }, Ct);
                await AssertRefusedAsync(writer, stream);
            },
                Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("T", Ct);
        Assert.False(Assert.Single(metadata, c => c.Name == "Refuse").AllowZeroLength);
        ColumnMetadata accept = Assert.Single(metadata, c => c.Name == "Accepted");
        Assert.True(accept.AllowZeroLength);
        Assert.False(accept.IsNullable);
        Assert.Equal(2, (await reader.Rows("T", cancellationToken: Ct).ToListAsync(Ct)).Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EmptyDefault_RefusesAndRestoresBatchAndAutoNumber(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true }, new ColumnDefinition("Value", typeof(string), 40) { DefaultValue = string.Empty }], Ct);
        byte[] before = stream.ToArray();
        object?[][] batch = [[null, "ok"], [null, DbDefault.Value]];
        JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowsAsync("T", batch, Ct));
        Assert.Equal(JetErrorCode.ZeroLengthViolation, error.ErrorCode);
        Assert.Equal(before, stream.ToArray());
        await writer.InsertRowAsync("T", [null, "ok"], Ct);
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal(1, Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToListAsync(Ct))[0]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NonTextDeclaration_RefusesBeforeMutation(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("Invalid", [new ColumnDefinition("Id", typeof(int)) { AllowZeroLength = true }], Ct));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)) { AllowZeroLength = true }, Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Hyperlink_EmptyAndNullRemainDistinctInTypedProjections(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Link", typeof(Hyperlink)) { AllowZeroLength = true }], Ct);
            await writer.InsertRowAsync("T", [1, new Hyperlink()], Ct);
            await writer.InsertRowAsync("T", [2, null], Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        List<object[]> rows = await reader.Rows("T", cancellationToken: Ct).ToListAsync(Ct);
        Assert.Equal(new Hyperlink(), Assert.Single(rows, row => (int)row[0] == 1)[1]);
        Assert.IsType<DBNull>(Assert.Single(rows, row => (int)row[0] == 2)[1]);
        List<LinkRow> mapped = await reader.Rows<LinkRow>("T", cancellationToken: Ct).ToListAsync(Ct);
        Assert.Equal(new Hyperlink(), Assert.Single(mapped, row => row.Id == 1).Link);
        Assert.Null(Assert.Single(mapped, row => row.Id == 2).Link);
        using DataTable data = await reader.ReadTableAsync("T", cancellationToken: Ct);
        Assert.Equal(new Hyperlink(), Assert.Single(data.Select("Id=1"))["Link"]);
        Assert.IsType<DBNull>(Assert.Single(data.Select("Id=2"))["Link"]);
    }

    [Theory]
    [InlineData(ColumnType.ByteType, 1)]
    [InlineData(ColumnType.BooleanType, 0)]
    [InlineData(ColumnType.BooleanType, 2)]
    public async Task MalformedPersistedPolicy_RefusesWithoutChangingBytes(ColumnType type, int length)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Value", typeof(string), 40)], Ct);
        }

        stream.Position = 0;
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            long page = (await harness.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct)).TDefPage;
            ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(page, Ct));
            var builder = ColumnPropertyBlockBuilder.FromBlock(properties);
            ColumnPropertyEntryBuilder policy = Assert.Single(builder.GetOrAddTarget("Value").Entries, entry => entry.Name == Constants.ColumnPropertyNames.AllowZeroLength);
            policy.DataType = type;
            policy.Value = new byte[length];
            await harness.Services.CatalogWriter.UpdateTablePropertiesAsync("T", page, builder.ToBytes(harness.Database.Format), cancellationToken: Ct);
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reopened.InsertRowAsync("T", [string.Empty], Ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, error.ErrorCode);
        Assert.Equal("T", error.ErrorInfo.TableName);
        Assert.Equal("Value", error.ErrorInfo.ColumnName);
        Assert.Equal(before, stream.ToArray());
    }

    private static ColumnDefinition Field(string name, string kind) => kind switch
    {
        "Text" => new ColumnDefinition(name, typeof(string), 40),
        "Memo" => new ColumnDefinition(name, typeof(string)),
        _ => new ColumnDefinition(name, typeof(Hyperlink)),
    };

    private static async Task AssertRefusedAsync(AccessWriter writer, MemoryStream stream)
    {
        byte[] before = stream.ToArray();
        JetConstraintException insert = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("T", [3, string.Empty, "ok"], Ct));
        JetConstraintException update = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Refuse"] = string.Empty }, Ct));
        Assert.Equal(JetErrorCode.ZeroLengthViolation, insert.ErrorCode);
        Assert.Equal(JetErrorCode.ZeroLengthViolation, update.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }

#pragma warning disable CA1812 // Rows<T> constructs this projection through reflection.
    private sealed class LinkRow
    {
        public int Id { get; set; }

        public Hyperlink? Link { get; set; }
    }
#pragma warning restore CA1812
}
