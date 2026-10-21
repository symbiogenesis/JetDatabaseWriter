namespace JetDatabaseWriter.Tests.Relationships;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Constraint failures retain their codes, context and original messages.</summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ConstraintViolationErrorTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the format and write-mode combinations.</summary>
    /// <returns>The combinations.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task UniqueViolation_NamesThePrimaryIndex(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], []);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), Ct);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
        {
            JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("P", [1, "duplicate"], Ct));
            Assert.Equal(JetErrorCode.UniqueViolation, error.ErrorCode);
            Assert.Equal("P", error.ErrorInfo.TableName);
            Assert.Equal("PrimaryKey", error.ErrorInfo.IndexName);
            Assert.Equal("Unique index violation on table 'P': duplicate key for index 'PrimaryKey'. The conflict was detected before any row was written; the table is unchanged.", error.Message);
        });
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task MissingParent_NamesTheRelationship(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], []);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId"), Ct);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
        {
            JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("C", [1, 99, "bad"], Ct));
            Assert.Equal(JetErrorCode.ForeignKeyMissingParent, error.ErrorCode);
            Assert.Equal("C", error.ErrorInfo.TableName);
            Assert.Equal("FK_C_P", error.ErrorInfo.RelationshipName);
            Assert.Equal("INSERT into 'C' violates foreign-key constraint 'FK_C_P': no matching row in 'P' for the supplied ParentId value(s).", error.Message);
        });
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task NotNullAndRuleFailures_NameTheColumn(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode);
        await writer.CreateTableAsync("Required", [new ColumnDefinition("Id", typeof(int)) { IsNullable = false }, new ColumnDefinition("Score", typeof(int)) { ValidationRuleExpression = ">0" }], Ct);
        await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
        {
            JetConstraintException notNull = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Required", [null, 1], Ct));
            Assert.Equal(JetErrorCode.NotNullViolation, notNull.ErrorCode);
            Assert.Equal("Required", notNull.ErrorInfo.TableName);
            Assert.Equal("Id", notNull.ErrorInfo.ColumnName);
            Assert.Equal("Column 'Id' on table 'Required' is marked NOT NULL and cannot be set to null.", notNull.Message);
            JetValidationRuleException rule = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("Required", [1, -1], Ct));
            Assert.Equal(JetErrorCode.ValidationRuleViolation, rule.ErrorCode);
            Assert.Equal("Score", rule.ErrorInfo.ColumnName);
            Assert.Equal("Validation rule '>0' for column 'Score' on table 'Required' rejected value '-1'.", rule.Message);
        });
    }
}
