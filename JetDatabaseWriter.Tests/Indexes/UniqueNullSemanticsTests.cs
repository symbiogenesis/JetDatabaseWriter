namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Access Null keys remain consistent across index checks and maintenance.</summary>
public sealed class UniqueNullSemanticsTests
{
    public static TheoryData<DatabaseFormat, WriteMode> Cases => WriteModes.Combine(
        DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task UniqueIndex_AllNullKeysSurviveBatchInsertUpdateAndReopen(DatabaseFormat format, WriteMode mode)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("A", typeof(int)), new ColumnDefinition("B", typeof(int))],
                [new IndexDefinition("UQ", new[] { "A", "B" }) { IsUnique = true }], ct);
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowsAsync("Items", new object[][] { [1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value], [3, 7, DBNull.Value] }, ct);
                await writer.InsertRowAsync("Items", [4, DBNull.Value, DBNull.Value], ct);
                await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 3), new RowValues { ["A"] = DBNull.Value }, ct);
            }, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        var ids = new List<int>();
        await foreach (object[] row in reader.FromIndex("Items", "UQ").ToRowsAsync(ct))
        {
            ids.Add((int)row[0]);
            Assert.IsType<DBNull>(row[1]);
            Assert.IsType<DBNull>(row[2]);
        }

        Assert.Equal(new[] { 1, 2, 3, 4 }, ids.Order());
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task UniqueIndex_PartlyNullDuplicateIsRefusedWithoutMutation(DatabaseFormat format, WriteMode mode)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, ct);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("A", typeof(int)), new ColumnDefinition("B", typeof(int))],
            [new IndexDefinition("UQ", new[] { "A", "B" }) { IsUnique = true }], ct);
        await writer.InsertRowAsync("Items", [7, DBNull.Value], ct);
        byte[] before = stream.ToArray();
        await WriteModes.RunAsync(writer, mode, async () =>
        {
            JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Items", [7, DBNull.Value], ct));
            Assert.Equal(JetErrorCode.UniqueViolation, error.ErrorCode);
        }, ct);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task IgnoreNulls_OmitsAllNullKeysAcrossMutations(DatabaseFormat format, WriteMode mode)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("A", typeof(int)), new ColumnDefinition("B", typeof(int))],
                [new IndexDefinition("IX", new[] { "A", "B" }) { IgnoreNulls = true }], ct);
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowsAsync("Items", new object[][] { [1, DBNull.Value, DBNull.Value], [2, 7, DBNull.Value], [3, 8, 9] }, ct);
                await writer.InsertRowAsync("Items", [4, DBNull.Value, DBNull.Value], ct);
                await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 3), new RowValues { ["A"] = DBNull.Value, ["B"] = DBNull.Value }, ct);
                await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["A"] = 5 }, ct);
                await writer.DeleteRowsAsync("Items", RowCriteria.Where("Id", 4), ct);
            }, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        var ids = new List<int>();
        await foreach (object[] row in reader.FromIndex("Items", "IX").ToRowsAsync(ct))
        {
            ids.Add((int)row[0]);
        }

        Assert.Equal(new[] { 1, 2 }, ids.Order());
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task RequiredIndex_RefusesAnyNullKeyBeforeMutation(DatabaseFormat format, WriteMode mode)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, ct);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("A", typeof(int)), new ColumnDefinition("B", typeof(int))],
            [new IndexDefinition("Required", new[] { "A", "B" }) { IsRequired = true }], ct);
        byte[] before = stream.ToArray();
        await WriteModes.RunAsync(writer, mode, async () =>
        {
            JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Items", [7, DBNull.Value], ct));
            Assert.Equal(JetErrorCode.NotNullViolation, error.ErrorCode);
        }, ct);
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task AccessEmployees_ExistingNullUniqueKeysPermitInsert()
    {
        var ct = TestContext.Current.CancellationToken;
        byte[] fixture = await File.ReadAllBytesAsync(TestDatabases.NorthwindTraders, ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(fixture, ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.InsertRowAsync("Employees", new RowValues { ["FirstName"] = "Ada", ["LastName"] = "Lead", ["WindowsUserName"] = "ada.lead" }, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        int found = 0;
        await foreach (object[] _ in reader.SeekRowsAsync("Employees", "WindowsUserName", new object?[] { "ada.lead" }, ct))
        {
            found++;
        }

        Assert.Equal(1, found);
    }
}
