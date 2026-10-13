namespace JetDatabaseWriter.Tests.Relationships;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Explicit caller foreign-key assignments take precedence over self-update cascades.</summary>
/// <param name="db">Caches the fixture databases.</param>
public sealed class ForeignKeySelfAssignmentTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every database format and writer mode.</summary>
    /// <returns>The format and mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>Gets every database format.</summary>
    /// <returns>The database formats.</returns>
    public static TheoryData<DatabaseFormat> Formats() => [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    /// <summary>An explicitly retained old foreign key refuses without changing bytes or indexes.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ExplicitOldSelfForeignKey_IsRefusedWithoutChangingBytes(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, composite: false);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentId"] = 1 }, Ct));
                Assert.Equal(JetErrorCode.ForeignKeyMissingParent, exception.ErrorCode);
            });
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["1|1", "2|1", "3|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree");
    }

    /// <summary>Assigning one composite foreign-key component preserves the entire caller key.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ExplicitCompositeSelfForeignKeyComponent_IsRefusedWithoutChangingBytes(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, composite: true);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentPart"] = 1 }, Ct));
                Assert.Equal(JetErrorCode.ForeignKeyMissingParent, exception.ErrorCode);
            });
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["1|1|1|1", "2|1|1|1", "3|1||"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree");
    }

    /// <summary>A refused self-update leaves an explicit transaction usable and rollback restores all bytes.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task Update_RefusedExplicitSelfAssignment_ThenSuccessfulUpdate_RollsBack(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, composite: false);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
            _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentId"] = 1 }, Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentId"] = 3 }, Ct));
            await tx.RollbackAsync(Ct);
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["1|1", "2|1", "3|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree");
    }

    /// <summary>A moved self-related row retains an unchanged orphan key that this statement did not invalidate.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfForeignKeyWasAlreadyOrphaned_RemainsUnchecked(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Tree", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))], Ct);
            Assert.Equal(2, await writer.InsertRowsAsync("Tree", [[1, null], [4, 99]], Ct));
        }

        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_Tree_Self", "Tree", "ParentId", "Tree", "Id");
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 4), new RowValues { ["Id"] = 5, ["ParentId"] = 99 }, Ct)));
        }

        Assert.Equal(["1|", "5|99"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree");
    }

    private async Task<MemoryStream> CreateTreeAsync(DatabaseFormat format, bool composite)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            ColumnDefinition[] columns = composite
                ? [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Part", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("ParentPart", typeof(int))]
                : [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))];
            await writer.CreateTableAsync("Tree", columns, Ct);
            object?[][] rows = composite ? [[1, 1, 1, 1], [2, 1, 1, 1], [3, 1, null, null]] : [[1, 1], [2, 1], [3, null]];
            Assert.Equal(3, await writer.InsertRowsAsync("Tree", rows, Ct));
            RelationshipDefinition relationship = composite
                ? new RelationshipDefinition("FK_Tree_Self", "Tree", ["Id", "Part"], "Tree", ["ParentId", "ParentPart"])
                : new RelationshipDefinition("FK_Tree_Self", "Tree", "Id", "Tree", "ParentId");
            await writer.CreateRelationshipAsync(relationship with { CascadeUpdates = true }, Ct);
        }

        return ms;
    }
}
