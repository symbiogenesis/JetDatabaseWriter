namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using Xunit;

/// <summary>Checks Access's limits on generated complex-column schemas.</summary>
public sealed class ComplexColumnSchemaLimitsTests
{
    [Theory]
    [InlineData(0, 255)]
    [InlineData(1, 1)]
    [InlineData(255, 255)]
    public async Task StringItems_UseTextAndEnforceLength(int declaredLength, int effectiveLength)
    {
        await using var stream = new MemoryStream();
        string value = new('x', effectiveLength);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Tags", [new ColumnDefinition("Id", typeof(int)), Items("Items", declaredLength)], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Tags", [1, DBNull.Value], TestContext.Current.CancellationToken);
            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            await writer.AddMultiValueItemAsync("Tags", "Items", key, value, TestContext.Current.CancellationToken);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAnyAsync<ArgumentException>(async () => await writer.AddMultiValueItemAsync("Tags", "Items", key, value + "x", TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        ComplexColumnInfo info = Assert.Single(await reader.GetComplexColumnsAsync("Tags", TestContext.Current.CancellationToken));
        ColumnMetadata column = Assert.Single(await reader.GetColumnMetadataAsync(info.FlatTableName, TestContext.Current.CancellationToken), c => c.Name == "Value");
        Assert.Equal("Text", column.TypeName);
        Assert.Equal(effectiveLength * 2, column.MaxLength);
        Assert.Equal(value, Assert.Single(await reader.GetMultiValueItemsAsync("Tags", "Items", TestContext.Current.CancellationToken)).Value);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(256)]
    public async Task StringItems_RejectMemoLengthBeforeWrites(int maxLength)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await writer.CreateTableAsync("Tags", [Items("Items", maxLength)], TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(256)]
    public async Task AddColumn_StringItemsRejectMemoLengthBeforeWrites(int maxLength)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Tags", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Tags", [1], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await writer.AddColumnAsync("Tags", Items("Items", maxLength), TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task LongNames_GeneratedNamesRemainValidAndItemsRoundTrip(bool surrogateBoundary, bool attachment)
    {
        string columnName = surrogateBoundary ? new string('c', 28) + "\U0001F600" + new string('c', 34) : new string('c', 64);
        string tableName = "_" + columnName[..63];
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, [new ColumnDefinition("Id", typeof(int)), attachment ? new ColumnDefinition(columnName, typeof(byte[])) { IsAttachment = true } : Items(columnName, 0)], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, DBNull.Value], TestContext.Current.CancellationToken);
            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            if (attachment)
            {
                await writer.AddAttachmentAsync(tableName, columnName, key, new AttachmentInput("tag.txt", [1]), TestContext.Current.CancellationToken);
            }
            else
            {
                await writer.AddMultiValueItemAsync(tableName, columnName, key, "tag", TestContext.Current.CancellationToken);
            }
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        ComplexColumnInfo info = Assert.Single(await reader.GetComplexColumnsAsync(tableName, TestContext.Current.CancellationToken));
        Assert.Null(AccessObjectName.FindViolation(info.FlatTableName));
        Assert.False(char.IsHighSurrogate(info.FlatTableName[^1]));
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(info.FlatTableName, TestContext.Current.CancellationToken);
        Assert.Equal(columns.Count, columns.Select(c => c.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
        Assert.All(columns, c => Assert.Null(AccessObjectName.FindViolation(c.Name)));
        Assert.All(await reader.ListIndexesAsync(info.FlatTableName, TestContext.Current.CancellationToken), i => Assert.Null(AccessObjectName.FindViolation(i.Name)));
        if (attachment)
        {
            Assert.Equal("tag.txt", Assert.Single(await reader.GetAttachmentsAsync(tableName, columnName, TestContext.Current.CancellationToken)).FileName);
        }
        else
        {
            Assert.Equal("tag", Assert.Single(await reader.GetMultiValueItemsAsync(tableName, columnName, TestContext.Current.CancellationToken)).Value);
        }
    }

    private static ColumnDefinition Items(string name, int maxLength) => new(name, typeof(object), maxLength) { IsMultiValue = true, MultiValueElementType = typeof(string) };
}