namespace JetDatabaseWriter.Tests.Schema;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Stored table rules remain effective through reopen and schema changes.</summary>
/// <param name="db">The database fixture cache.</param>
public sealed class TableValidationRuleTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every database format and write mode.</summary>
    /// <returns>The format and mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>Rules persist, reject invalid rows, follow renames and protect dependencies.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task TableRule_PersistsAndFollowsSchemaChanges(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Rules", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Other", typeof(int))], Ct);
            Assert.Equal(1, await writer.InsertRowsAsync("Rules", [[1, 0]], Ct));
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id] > 0", "positive identifier"), Ct);
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(new TableValidationRule("[Id] > 0", "positive identifier"), await reader.GetTableValidationRuleAsync("Rules", Ct));
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowsAsync("Rules", [[-1, 0]], Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("Rules", RowCriteria.Where("Id", 1), new RowValues { ["Other"] = 2 }, Ct));
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.UpdateRowsAsync("Rules", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = -1 }, Ct));
                await writer.RenameColumnAsync("Rules", "Id", "Value", Ct);
                _ = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("Rules", "Value", Ct));
            });
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(new TableValidationRule("[Value] > 0", "positive identifier"), await reader.GetTableValidationRuleAsync("Rules", Ct));
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Value] < 0"), Ct)));
        }

        Assert.Equal(before, stream.ToArray());
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await writer.SetTableValidationRuleAsync("Rules", null, Ct);
                Assert.Equal(1, await writer.InsertRowsAsync("Rules", [[-1, 0]], Ct));
            });
        }

        stream.Position = 0;
        await using AccessReader finalReader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        Assert.Null(await finalReader.GetTableValidationRuleAsync("Rules", Ct));
    }
}
