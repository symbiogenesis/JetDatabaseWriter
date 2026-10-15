namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Only an entirely Null foreign key is exempt from matching a parent.</summary>
/// <param name="db">Caches fixture databases.</param>
public sealed class ForeignKeyPartialNullTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format and writer mode.</summary>
    /// <returns>The format and mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>Gets nullable-parent shapes, duplicate keys and dependency policies.</summary>
    /// <returns>The database, writer and operation cases.</returns>
    public static TheoryData<DatabaseFormat, WriteMode, bool, bool, bool, bool> AllNullParents()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, bool, bool, bool, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (bool composite in new[] { false, true })
                {
                    foreach (bool duplicate in new[] { false, true })
                    {
                        foreach (bool cascade in new[] { false, true })
                        {
                            foreach (bool delete in new[] { false, true })
                            {
                                data.Add(format, mode, composite, duplicate, cascade, delete);
                            }
                        }
                    }
                }
            }
        }

        return data;
    }

    /// <summary>Even duplicate entirely Null parent keys have dependents during update and delete.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <param name="composite">Whether the relationship has two components.</param>
    /// <param name="duplicate">Whether another entirely Null parent remains.</param>
    /// <param name="cascade">Whether updates and deletes cascade.</param>
    /// <param name="delete">Whether to delete rather than update the parent.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllNullParents))]
    public Task AllNullParentKeys_HaveNativeDependencySemantics(DatabaseFormat format, WriteMode mode, bool composite, bool duplicate, bool cascade, bool delete)
        => this.VerifyAllNullParentAsync(format, mode, composite, duplicate, cascade, delete, ignoreNulls: false);

    /// <summary>Ignored Null index entries cannot hide dependents of an entirely Null parent.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <param name="composite">Whether the relationship has two components.</param>
    /// <param name="duplicate">Whether another entirely Null parent remains.</param>
    /// <param name="cascade">Whether updates and deletes cascade.</param>
    /// <param name="delete">Whether to delete rather than update the parent.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AllNullParents))]
    public Task IgnoreNullsChildIndex_DoesNotHideAllNullDependents(DatabaseFormat format, WriteMode mode, bool composite, bool duplicate, bool cascade, bool delete)
        => this.VerifyAllNullParentAsync(format, mode, composite, duplicate, cascade, delete, ignoreNulls: true);

    /// <summary>Unmatched partial keys refuse before changing bytes or the caller transaction.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task UnmatchedPartialNullInsertAndUpdate_PreserveBytesAndTransaction(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await using JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(Ct) : null;
            int pages = transaction?.JournaledPageCount ?? 0;
            JetConstraintException insert = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.InsertRowsAsync("C", [[20, null, null], [21, null, 9]], Ct));
            Assert.Equal(JetErrorCode.ForeignKeyMissingParent, insert.ErrorCode);
            JetConstraintException update = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 10), new RowValues { ["PA"] = null, ["PB"] = 9 }, Ct));
            Assert.Equal(JetErrorCode.ForeignKeyMissingParent, update.ErrorCode);
            Assert.Equal(before, ms.ToArray());
            if (transaction is not null)
            {
                Assert.Equal(pages, transaction.JournaledPageCount);
                Assert.False(transaction.IsCommitted);
                Assert.False(transaction.IsRolledBack);
            }

            Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 10), new RowValues { ["PA"] = null, ["PB"] = null }, Ct));
            if (transaction is not null)
            {
                await transaction.CommitAsync(Ct);
            }
        }

        Assert.Equal(["10||", "11||", "12||1", "13|1|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>Nullable unique parent keys are real references for insertion, update and deletion.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task NullableUniqueParentKeys_ParticipateInCascades(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowsAsync("C", [[14, null, 1], [15, 1, null]], Ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["B"] = 3 }, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 3), new RowValues { ["A"] = 5 }, Ct));
                Assert.Equal(1, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 2), Ct));
                Assert.Equal(1, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 4), Ct));
            });
        }

        Assert.Equal(["10|1|1", "13|5|", "15|5|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
    }

    /// <summary>Restrictive relationships retain dependents of partial Null unique keys.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task PartialNullParentWithoutCascade_RefusesUpdateAndDelete(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: false);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["B"] = 3 }, Ct));
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 3), Ct));
            });
        }

        Assert.Equal(before, ms.ToArray());
    }

    /// <summary>Relationship creation checks partial keys while accepting matching nullable unique keys.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task ExistingUnmatchedPartialNullKey_RefusesRelationshipCreation(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.DropRelationshipAsync("FK_C_P", Ct);
            await writer.InsertRowAsync("C", [20, null, 9], Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", ["A", "B"], "C", ["PA", "PB"]), Ct)));
        }

        Assert.Equal(before, ms.ToArray());
    }

    /// <summary>The native self-update reproduction refuses a partial clear and accepts clearing both components.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task CompositeSelfUpdate_RequiresEveryRemainingComponentToMatch(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter setup = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await setup.CreateTableAsync("Tree", [new ColumnDefinition("A", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("B", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PA", typeof(int)), new ColumnDefinition("PB", typeof(int))], Ct);
            await setup.InsertRowsAsync("Tree", [[1, 1, 1, 1], [2, 1, 1, 1], [3, 1, null, null]], Ct);
            await setup.CreateRelationshipAsync(new RelationshipDefinition("FK_Tree_Self", "Tree", ["A", "B"], "Tree", ["PA", "PB"]) { CascadeUpdates = true }, Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("A", 1), new RowValues { ["A"] = 5, ["PA"] = null }, Ct));
                Assert.Equal(before, ms.ToArray());
                Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("A", 1), new RowValues { ["A"] = 5, ["PA"] = null, ["PB"] = null }, Ct));
            });
        }

        Assert.Equal(["2|1|5|1", "3|1||", "5|1||"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
    }

    /// <summary>Unchanged partial-Null orphans do not block unrelated edits or another row's cascade.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task UnchangedPartialNullOrphan_IsNotRechecked(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true);
        await using (AccessWriter setup = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await setup.DropRelationshipAsync("FK_C_P", Ct);
            await setup.InsertRowAsync("C", [20, null, 9], Ct);
        }

        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(ms, cancellationToken: Ct))
        {
            long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, Ct);
            TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(page, Constants.SystemTableNames.Relationships, Ct);
            for (int index = 0; index < 2; index++)
            {
                object[] row = definition.CreateNullValueRow();
                definition.SetValueByName(row, "ccolumn", 2);
                definition.SetValueByName(row, "grbit", 256);
                definition.SetValueByName(row, "icolumn", index);
                definition.SetValueByName(row, "szColumn", index == 0 ? "PA" : "PB");
                definition.SetValueByName(row, "szObject", "C");
                definition.SetValueByName(row, "szReferencedColumn", index == 0 ? "A" : "B");
                definition.SetValueByName(row, "szReferencedObject", "P");
                definition.SetValueByName(row, "szRelationship", "FK_C_P");
                await harness.Services.Indexes.InsertSystemRowAndMaintainAsync(page, definition, Constants.SystemTableNames.Relationships, row, cancellationToken: Ct);
            }
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 20), new RowValues { ["Id"] = 21, ["PA"] = null }, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["B"] = 2 }, Ct));
            });
        }

        Assert.Equal(["10|1|2", "11||", "12||1", "13|1|", "21||9"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    private async Task VerifyAllNullParentAsync(DatabaseFormat format, WriteMode mode, bool composite, bool duplicate, bool cascade, bool delete, bool ignoreNulls)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter setup = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            ColumnDefinition[] parent = composite
                ? [new("Id", typeof(int)) { IsPrimaryKey = true }, new("A", typeof(int)), new("B", typeof(int))]
                : [new("Id", typeof(int)) { IsPrimaryKey = true }, new("A", typeof(int))];
            ColumnDefinition[] child = composite
                ? [new("Id", typeof(int)) { IsPrimaryKey = true }, new("PA", typeof(int)), new("PB", typeof(int))]
                : [new("Id", typeof(int)) { IsPrimaryKey = true }, new("PA", typeof(int))];
            await setup.CreateTableAsync("P", parent, Ct);
            await setup.CreateTableAsync("C", child, ignoreNulls ? [new IndexDefinition("C_Parent", composite ? ["PA", "PB"] : ["PA"]) { IgnoreNulls = true }] : [], Ct);
            await setup.InsertRowAsync("P", composite ? [1, null, null] : [1, null], Ct);
            if (duplicate)
            {
                await setup.InsertRowAsync("P", composite ? [2, null, null] : [2, null], Ct);
            }

            await setup.InsertRowAsync("C", composite ? [10, null, null] : [10, null], Ct);
            RelationshipDefinition relationship = composite
                ? new("FK_C_P", "P", ["A", "B"], "C", ["PA", "PB"])
                : new("FK_C_P", "P", "A", "C", "PA");
            await setup.CreateRelationshipAsync(relationship with { CascadeUpdates = cascade, CascadeDeletes = cascade }, Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await using JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(Ct) : null;
            async Task MutateAsync()
            {
                if (delete)
                {
                    Assert.Equal(1, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                }
                else
                {
                    Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["A"] = 8 }, Ct));
                }
            }

            if (cascade)
            {
                await MutateAsync();
            }
            else
            {
                _ = await Assert.ThrowsAsync<JetConstraintException>(MutateAsync);
                Assert.Equal(before, ms.ToArray());
                if (transaction is not null)
                {
                    Assert.Equal(0, transaction.JournaledPageCount);
                    Assert.False(transaction.IsCommitted);
                    Assert.False(transaction.IsRolledBack);
                }
            }

            if (transaction is not null)
            {
                await transaction.CommitAsync(Ct);
            }
        }

        if (cascade && delete)
        {
            Assert.Empty(await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        }
        else
        {
            string expected = cascade ? "10|8" : "10|";
            Assert.Equal([expected + (composite ? "|" : string.Empty)], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        }
    }

    private async Task<MemoryStream> CreateAsync(DatabaseFormat format, bool cascade)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("A", typeof(int)), new ColumnDefinition("B", typeof(int))], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PA", typeof(int)), new ColumnDefinition("PB", typeof(int))], Ct);
            await writer.InsertRowsAsync("P", [[1, 1, 1], [2, null, 1], [3, 1, null], [4, null, null]], Ct);
            await writer.InsertRowsAsync("C", [[10, 1, 1], [11, null, null], [12, null, 1], [13, 1, null]], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", ["A", "B"], "C", ["PA", "PB"]) { CascadeUpdates = cascade, CascadeDeletes = cascade }, Ct);
        }

        return ms;
    }
}
