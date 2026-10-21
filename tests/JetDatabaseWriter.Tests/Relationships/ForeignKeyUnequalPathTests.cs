namespace JetDatabaseWriter.Tests.Relationships;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Converging paths distinguish unequal depths from simultaneous diamond arrivals.</summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ForeignKeyUnequalPathTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets each format and write mode with both declaration orders and convergence shapes.</summary>
    /// <returns>The format, write mode, declaration order and presence of a later diamond.</returns>
    public static TheoryData<DatabaseFormat, WriteMode, bool, bool> Cases()
    {
        var cases = new TheoryData<DatabaseFormat, WriteMode, bool, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                cases.Add(format, mode, false, false);
                cases.Add(format, mode, true, false);
                cases.Add(format, mode, false, true);
                cases.Add(format, mode, true, true);
            }
        }

        return cases;
    }

    /// <summary>A shorter path establishes the same terminal key before the longer path reaches it.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The transaction mode.</param>
    /// <param name="reverse">Whether the direct relationship is declared first.</param>
    /// <param name="lateDiamond">Whether two later parents reach the terminal key at the same depth.</param>
    /// <returns>The asynchronous regression.</returns>
    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Update_ConvergingPaths_RespectFirstArrivalDepth(DatabaseFormat format, WriteMode mode, bool reverse, bool lateDiamond)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            foreach (string table in new[] { "A", "B", "C", "D" })
            {
                await writer.CreateTableAsync(table, [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
                Assert.Equal(2, await writer.InsertRowsAsync(table, [[1], [2]], Ct));
            }

            string middleEdge = lateDiamond ? "A,C" : "B,C";
            foreach (string edge in reverse ? (string[])["A,D", "C,D", middleEdge, "B,D", "A,B"] : ["A,B", "B,D", middleEdge, "C,D", "A,D"])
            {
                if (!lateDiamond && edge == "B,D")
                {
                    continue;
                }

                string[] tables = edge.Split(',');
                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_" + tables[0] + "_" + tables[1], tables[0], "Id", tables[1], "Id") { CascadeUpdates = true }, Ct);
            }
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                if (lateDiamond)
                {
                    JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                        await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
                    Assert.Equal(JetErrorCode.ForeignKeyRestrictUpdate, exception.ErrorCode);
                }
                else
                {
                    Assert.Equal(1, await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
                }
            });
        }

        if (lateDiamond)
        {
            Assert.Equal(before, stream.ToArray());
        }

        foreach (string table in new[] { "A", "B", "C", "D" })
        {
            Assert.Equal(lateDiamond ? ["1", "2"] : ["2", "5"], await ForeignKeyTestDatabase.ReadRowsAsync(stream, table));
        }

        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(stream, "A", "B", "C", "D");
    }
}
