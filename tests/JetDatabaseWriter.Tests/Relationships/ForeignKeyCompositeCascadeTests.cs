namespace JetDatabaseWriter.Tests.Relationships;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>A composite cascade copies physical values even when one component compares equal.</summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ForeignKeyCompositeCascadeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format and write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> Cases() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>A numeric key change carries the parent's text spelling through every generation.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The transaction mode.</param>
    /// <returns>The asynchronous regression.</returns>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Update_CompositeKey_CopiesCaseEquivalentComponents(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            foreach ((string table, string code) in new[] { ("A", "Alpha"), ("B", "aLPHa"), ("C", "ALpha") })
            {
                await writer.CreateTableAsync(table, [new ColumnDefinition("Code", typeof(string), maxLength: 40) { IsPrimaryKey = true }, new ColumnDefinition("Part", typeof(int)) { IsPrimaryKey = true }], Ct);
                Assert.Equal(2, await writer.InsertRowsAsync(table, [[code, 1], [code, 2]], Ct));
            }

            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_A_B", "A", ["Code", "Part"], "B", ["Code", "Part"]) { CascadeUpdates = true }, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_B_C", "B", ["Code", "Part"], "C", ["Code", "Part"]) { CascadeUpdates = true }, Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("A", RowCriteria.Where("Part", 1), new RowValues { ["Code"] = "ALPHA", ["Part"] = 5 }, Ct)));
        }

        foreach ((string table, string unchangedCode) in new[] { ("A", "Alpha"), ("B", "aLPHa"), ("C", "ALpha") })
        {
            Assert.Equal(["ALPHA|5", unchangedCode + "|2"], await ForeignKeyTestDatabase.ReadRowsAsync(stream, table));
        }

        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(stream, "A", "B", "C");
    }
}
