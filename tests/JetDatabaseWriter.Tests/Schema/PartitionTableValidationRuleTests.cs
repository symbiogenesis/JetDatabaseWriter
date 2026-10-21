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

/// <summary>Checks Partition in persisted table validation rules.</summary>
/// <param name="db">The database fixture cache.</param>
public sealed class PartitionTableValidationRuleTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task PartitionRule_PersistsEvaluatesAndRejectsInvalidRows(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Rules", [new ColumnDefinition("Id", typeof(int))], ct);
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("Partition([Id],0,10,2)<>\"\""), ct);
            Assert.Equal(1, await writer.InsertRowsAsync("Rules", [[1]], ct));
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Rules", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 2 }, ct));
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("Partition([Id],0,10,2)=\" 2: 3\""), ct);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowsAsync("Rules", [[4]], ct));
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.UpdateRowsAsync("Rules", RowCriteria.Where("Id", 2), new RowValues { ["Id"] = 4 }, ct));
        }

        Assert.Equal(before, stream.ToArray());
    }
}
