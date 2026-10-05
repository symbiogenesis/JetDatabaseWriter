namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

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

    /// <summary>Secure cascade deletes scrub the child's long-value payload.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Delete_SecureCascade_ScrubsChildLongValues(DatabaseFormat format, WriteMode mode)
    {
        byte[] payload = Enumerable.Repeat((byte)0x5A, 9000).ToArray();
        byte[] marker = payload.AsSpan(0, 64).ToArray();
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("Blob", typeof(byte[]))], Ct);
            Assert.Equal(1, await writer.InsertRowsAsync("P", [[1]], Ct));
            Assert.Equal(1, await writer.InsertRowsAsync("C", [[1, 1, payload]], Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeDeletes = true }, Ct);
        }

        Assert.True(ms.ToArray().AsSpan().IndexOf(marker) >= 0);
        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            UseTransactionalWrites = mode == WriteMode.AutoCommit,
            SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
        }, leaveOpen: true, Ct))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct)));
        }

        Assert.True(ms.ToArray().AsSpan().IndexOf(marker) < 0);
        Assert.Empty(await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>A statement's own self-related rows are deleted and counted once.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Delete_OwnRowsReachedThroughSelfRelationship_AreCountedOnce(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Tree", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))], Ct);
            Assert.Equal(3, await writer.InsertRowsAsync("Tree", [[1, null], [2, 1], [3, null]], Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Tree_Self", "Tree", "Id", "Tree", "ParentId") { CascadeDeletes = true }, Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(2, await writer.DeleteRowsAsync("Tree", RowCriteria.Where(ColumnPredicate.LessThan("Id", 3)), Ct)));
        }

        Assert.Equal(["3|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
        Assert.Equal(1, stats.Single(stat => stat.Name == "Tree").RowCount);
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree");
    }

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
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                Assert.Equal(JetErrorCode.ForeignKeyRestrictDelete, ex.ErrorCode);
                Assert.Equal("P", ex.ErrorInfo.TableName);
                Assert.Equal("FK_D_P", ex.ErrorInfo.RelationshipName);
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
