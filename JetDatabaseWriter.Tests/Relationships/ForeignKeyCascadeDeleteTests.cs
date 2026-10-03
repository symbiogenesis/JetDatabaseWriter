namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using WriteMode = JetDatabaseWriter.Tests.Writer.TransactionReadVisibilityTests.WriteMode;

/// <summary>
/// A delete finds every row it cascades to, through every level, and makes
/// every refusal before it deletes any row: a refused delete changes nothing,
/// even when another relationship cascades first, and a row two cascades of
/// one delete reach is deleted and counted once.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ForeignKeyCascadeDeleteTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>
    /// A delete that a relationship without cascading deletes refuses deletes
    /// no row, even though a relationship read before it cascades deletes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the delete.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Delete_CascadingRelationshipBeforeRefusingOne_DeletesNoRow(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateCascadingAsync(
            db,
            format,
            secondChild: new RelationshipDefinition("FK_D_P", "P", "Id", "D", "ParentId"));

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                Assert.Contains(
                    "DELETE on 'P' violates foreign-key constraint 'FK_D_P': 1 dependent row(s) in 'D' reference the deleted key(s)",
                    ex.Message,
                    StringComparison.Ordinal);
            });
        }

        Assert.Equal(["1|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1|a"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        Assert.Equal(["1|1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "D"));
    }

    /// <summary>
    /// Deleting root 1 cascades to its tree rows 1 and 2, and from row 1 again
    /// to row 2, its child. Row 2 is deleted once and the table's stored row
    /// count drops by two, leaving row 3 of root 2 counted.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the delete.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Delete_RowReachedByTwoCascades_IsCountedOnce(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Root", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync(
                "Tree",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("RootId", typeof(int)),
                    new ColumnDefinition("ParentId", typeof(int)),
                ],
                Ct);
            Assert.Equal(2, await writer.InsertRowsAsync("Root", [[1], [2]], Ct));
            Assert.Equal(3, await writer.InsertRowsAsync("Tree", [[1, 1, null], [2, 1, 1], [3, 2, null]], Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Tree_Root", "Root", "Id", "Tree", "RootId") { CascadeDeletes = true }, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Tree_Self", "Tree", "Id", "Tree", "ParentId") { CascadeDeletes = true }, Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.DeleteRowsAsync("Root", RowCriteria.Where("Id", 1), Ct)));
        }

        Assert.Equal(["2"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Root"));
        Assert.Equal(["3|2|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
        Assert.Equal(1, stats.Single(stat => stat.Name == "Tree").RowCount);
        Assert.Equal(1, stats.Single(stat => stat.Name == "Root").RowCount);
    }
}
