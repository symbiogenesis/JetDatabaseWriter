namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Lookup and schema refusals expose stable codes and preserve messages.</summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class LookupErrorTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the format and write-mode combinations.</summary>
    /// <returns>The combinations.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task TableAndColumnRefusals_HaveContext(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], Ct);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
        {
            JetOperationException missing = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.InsertRowAsync("Missing", [1], Ct));
            Assert.Equal(JetErrorCode.TableNotFound, missing.ErrorCode);
            Assert.Equal("Missing", missing.ErrorInfo.TableName);
            Assert.Equal("Table 'Missing' was not found.", missing.Message);
            JetObjectExistsException exists = await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], Ct));
            Assert.Equal(JetErrorCode.TableExists, exists.ErrorCode);
            Assert.Equal("T", exists.ErrorInfo.TableName);
            JetObjectNotFoundException column = await Assert.ThrowsAsync<JetObjectNotFoundException>(async () => await writer.DropColumnAsync("T", "Missing", Ct));
            Assert.Equal(JetErrorCode.ColumnNotFound, column.ErrorCode);
            Assert.Equal("T", column.ErrorInfo.TableName);
            Assert.Equal("Missing", column.ErrorInfo.ColumnName);
            Assert.Equal(new ArgumentException("Column 'Missing' was not found in table 'T'.", "columnName").Message, column.Message);
            JetOperationException last = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("T", "Id", Ct));
            Assert.Equal(JetErrorCode.LastColumn, last.ErrorCode);
            Assert.Equal("Cannot drop the last remaining column from table 'T'.", last.Message);
        });
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task RelationshipRefusals_HaveContext(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], []);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), Ct);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
        {
            JetOperationException table = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropTableAsync("P", Ct));
            Assert.Equal(JetErrorCode.TableInRelationship, table.ErrorCode);
            Assert.Equal("P", table.ErrorInfo.TableName);
            JetOperationException key = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("P", "Id", Ct));
            Assert.Equal(JetErrorCode.KeyColumnInRelationship, key.ErrorCode);
            Assert.Equal("Id", key.ErrorInfo.ColumnName);
            JetOperationException missing = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropRelationshipAsync("Missing", Ct));
            Assert.Equal(JetErrorCode.RelationshipNotFound, missing.ErrorCode);
            Assert.Equal("Missing", missing.ErrorInfo.RelationshipName);
            JetObjectExistsException exists = await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), Ct));
            Assert.Equal(JetErrorCode.RelationshipExists, exists.ErrorCode);
        });
    }
}
