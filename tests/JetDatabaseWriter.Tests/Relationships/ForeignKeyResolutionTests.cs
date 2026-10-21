namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Data;
using System.Globalization;
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
/// An enforced relationship whose table or key column the writer cannot find
/// fails closed: every write it would have to check throws
/// <see cref="InvalidOperationException"/> naming the relationship and what is
/// missing, instead of reporting a missing parent row or skipping the check.
/// A write the relationship does not constrain (a null foreign key, an update
/// that leaves the key alone, a delete that matches nothing) still succeeds,
/// and a write it refuses changes no row, even when another relationship
/// cascades first. The relationships are planted straight into
/// <c>MSysRelationships</c>, because <c>CreateRelationshipAsync</c> and
/// <c>DropTableAsync</c> never leave one dangling. The class also covers
/// parent rows inserted earlier in the same batch. Jet3 resolves the parent
/// through its key set; Jet4 and ACCDB try the index seek first.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ForeignKeyResolutionTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Insert_EnforcedRelationshipToMissingTable_ThrowsNamingTheTable(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], []);
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_Dangling", "C", "ParentId", "NoSuchParent", "Id");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetOperationException ex = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.InsertRowAsync("C", [3, 1, "x"], Ct));
                AssertCannotBeEnforced(ex, "FK_Dangling", "primary table 'NoSuchParent' was not found");

                await writer.InsertRowAsync("C", [4, null, "y"], Ct);
            });

            await writer.DropRelationshipAsync("FK_Dangling", Ct);
            await writer.InsertRowAsync("C", [3, 1, "x"], Ct);
        }

        Assert.Equal(["3|1|x", "4||y"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_EnforcedRelationshipToMissingTable_ThrowsOnlyWhenTheKeyChanges(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], [[1, 1, "a"], [2, 99, "b"]]);
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_Dangling", "C", "ParentId", "NoSuchParent", "Id");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Assert.Equal(2, await writer.UpdateRowsAsync("C", RowCriteria.All(), new RowValues { ["Note"] = "z" }, Ct));

                JetOperationException ex = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 2), new RowValues { ["ParentId"] = 5 }, Ct));
                AssertCannotBeEnforced(ex, "FK_Dangling", "primary table 'NoSuchParent' was not found");

                Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 1), new RowValues { ["ParentId"] = null }, Ct));
            });
        }

        Assert.Equal(["1||z", "2|99|z"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertAndDelete_EnforcedRelationshipToMissingPrimaryColumn_ThrowNamingTheColumn(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], []);
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_NoColumn", "C", "ParentId", "P", "NoSuchColumn");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetOperationException insert = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.InsertRowAsync("C", [3, 1, "x"], Ct));
                AssertCannotBeEnforced(insert, "FK_NoColumn", "table 'P' has no column 'NoSuchColumn'");

                JetOperationException delete = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                AssertCannotBeEnforced(delete, "FK_NoColumn", "table 'P' has no column 'NoSuchColumn'");

                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 2, ["Name"] = "two" }, Ct));
                Assert.Equal(0, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 99), Ct));
            });
        }

        Assert.Equal(["2|two"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Empty(await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertAndDelete_EnforcedRelationshipFromMissingForeignColumn_ThrowNamingTheColumn(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], [[1, 1, "a"]]);
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_NoFkColumn", "C", "NoSuchFk", "P", "Id");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetOperationException insert = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.InsertRowAsync("C", [3, 1, "x"], Ct));
                AssertCannotBeEnforced(insert, "FK_NoFkColumn", "table 'C' has no column 'NoSuchFk'");

                JetOperationException delete = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                AssertCannotBeEnforced(delete, "FK_NoFkColumn", "table 'C' has no column 'NoSuchFk'");

                // An update cannot assign a column the table does not have,
                // so it never changes the relationship's key.
                Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.All(), new RowValues { ["ParentId"] = null, ["Note"] = "z" }, Ct));
            });
        }

        Assert.Equal(["1|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1||z"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task DeleteAndKeyUpdate_EnforcedRelationshipFromMissingChildTable_ThrowNamingTheTable(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], []);
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_NoChild", "NoSuchChild", "ParentId", "P", "Id");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetOperationException delete = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                AssertCannotBeEnforced(delete, "FK_NoChild", "foreign table 'NoSuchChild' was not found");

                JetOperationException update = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 8 }, Ct));
                AssertCannotBeEnforced(update, "FK_NoChild", "foreign table 'NoSuchChild' was not found");

                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Name"] = "uno" }, Ct));
                Assert.Equal(0, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 99), Ct));
            });
        }

        Assert.Equal(["1|uno"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
    }

    /// <summary>
    /// A key update that an unresolvable relationship refuses changes no row,
    /// even though a relationship read before it cascades updates: the
    /// relationships are resolved before any child row is rewritten.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task KeyUpdate_CascadingRelationshipBeforeUnresolvableOne_ChangesNoRow(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateCascadingAsync(db, format, secondChild: null);
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_NoFkColumn", "C", "NoSuchFk", "P", "Id");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetOperationException ex = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 8 }, Ct));
                AssertCannotBeEnforced(ex, "FK_NoFkColumn", "table 'C' has no column 'NoSuchFk'");
            });
        }

        Assert.Equal(["1|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1|a"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>
    /// A delete that an unresolvable relationship refuses deletes no row, even
    /// though a relationship read before it cascades deletes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the delete.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Delete_CascadingRelationshipBeforeUnresolvableOne_DeletesNoRow(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateCascadingAsync(db, format, secondChild: null);
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_NoColumn", "C", "ParentId", "P", "NoSuchColumn");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetOperationException ex = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                AssertCannotBeEnforced(ex, "FK_NoColumn", "table 'P' has no column 'NoSuchColumn'");
            });
        }

        Assert.Equal(["1|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1|a"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>
    /// A delete deletes no row when the relationships of a table it cascades
    /// into cannot be resolved, even though another relationship of the
    /// deleted table cascades first: <c>P</c> cascades to <c>C</c> and then to
    /// <c>D</c>, whose relationship to a missing table refuses the delete.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the delete.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Delete_UnresolvableRelationshipOfACascadedTable_DeletesNoRow(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateCascadingAsync(
            db,
            format,
            secondChild: new RelationshipDefinition("FK_D_P", "P", "Id", "D", "ParentId") { CascadeDeletes = true });
        await ForeignKeyTestDatabase.PlantRelationshipAsync(ms, "FK_NoChild", "NoSuchChild", "ParentId", "D", "Id");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetOperationException ex = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                AssertCannotBeEnforced(ex, "FK_NoChild", "foreign table 'NoSuchChild' was not found");
            });
        }

        Assert.Equal(["1|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1|a"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        Assert.Equal(["1|1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "D"));
    }

    /// <summary>
    /// A row of an <c>InsertRowsAsync</c> batch resolves its foreign key
    /// against a row inserted earlier in the same batch, even when no row
    /// before it had a non-null key: on Jet4 and ACCDB the parent's index is
    /// rebuilt only after the batch, so the seek cannot see that row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the insert.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertRows_SelfReferentialBatch_ChildOfEarlierRowWithNullParent_IsAccepted(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeDatabaseAsync(format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Assert.Equal(3, await writer.InsertRowsAsync("Tree", [[5, null], [6, 5], [7, 6]], Ct));
                Assert.Equal(2, await writer.InsertRowsAsync("Tree", [[8, 7], [9, 8]], Ct));
            });
        }

        Assert.Equal(["5|", "6|5", "7|6", "8|7", "9|8"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertRows_SelfReferentialBatch_ChildOfMissingParent_IsRejectedAndRollsBack(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeDatabaseAsync(format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.InsertRowsAsync("Tree", [[5, null], [6, 5], [7, 99]], Ct));
                Assert.Contains("violates foreign-key constraint 'FK_Tree_Self': no matching row in 'Tree'", ex.Message, StringComparison.Ordinal);

                Assert.Equal(2, await writer.InsertRowsAsync("Tree", [[5, null], [6, 5]], Ct));
            });
        }

        Assert.Equal(["5|", "6|5"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
    }

    /// <summary>
    /// The Access-authored relationships of NorthwindTraders.accdb resolve
    /// their tables, including Orders, whose <c>MSysObjects</c> row is an
    /// overflow row: an update moves an order line to another existing order
    /// and is refused for a missing one, a delete of an order status that
    /// orders use is refused, and a delete of an order cascades to its lines.
    /// </summary>
    /// <param name="mode">How the writer runs the operations.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task AccessAuthoredForeignKeys_Northwind_ResolveTheirTables(WriteMode mode)
    {
        if (!File.Exists(TestDatabases.NorthwindTraders))
        {
            Assert.Skip("NorthwindTraders.accdb is unavailable on this machine.");
        }

        await using MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, Ct);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("OrderDetails", RowCriteria.Where("OrderDetailID", 1), new RowValues { ["OrderID"] = 2 }, Ct));

                JetConstraintException update = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("OrderDetails", RowCriteria.Where("OrderDetailID", 1), new RowValues { ["OrderID"] = 99999 }, Ct));
                Assert.Contains(
                    "UPDATE of 'OrderDetails' violates foreign-key constraint 'New_New_OrdersOrderDetails': no matching row in 'Orders'",
                    update.Message,
                    StringComparison.Ordinal);

                JetConstraintException delete = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.DeleteRowsAsync("OrderStatus", RowCriteria.Where("OrderStatusID", 3), Ct));
                Assert.Contains("DELETE on 'OrderStatus' violates foreign-key constraint 'New_New_OrdersStatusOrders'", delete.Message, StringComparison.Ordinal);
                Assert.Contains("dependent row(s) in 'Orders'", delete.Message, StringComparison.Ordinal);

                // New_New_OrdersOrderDetails cascades deletes, including to
                // the line just moved to order 2.
                Assert.Equal(1, await writer.DeleteRowsAsync("Orders", RowCriteria.Where("OrderID", 2), Ct));
            });
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        using DataTable details = await reader.ReadTableAsync("OrderDetails", cancellationToken: Ct);
        Assert.Empty(details.Select("OrderDetailID = 1 OR OrderID = 2"));
        Assert.NotEmpty(details.Select("OrderID = 1"));
        using DataTable orders = await reader.ReadTableAsync("Orders", cancellationToken: Ct);
        Assert.Empty(orders.Select("OrderID = 2"));
    }

    /// <summary>
    /// The Access 97 relationship 'Table3Table1' of indexTestV1997.mdb resolves
    /// Table3, which the catalog once missed: an update of Table1.otherfk2 to
    /// another Table3 id succeeds and one to a missing id is refused.
    /// </summary>
    /// <param name="mode">How the writer runs the operations.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task AccessAuthoredForeignKeys_Jet3IndexTest_ResolveTheirTables(WriteMode mode)
    {
        if (!File.Exists(TestDatabases.IndexTestV1997))
        {
            Assert.Skip("Jet3 index fixture is unavailable on this machine.");
        }

        await using MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.IndexTestV1997, Ct);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            Assert.Equal(DatabaseFormat.Jet3Mdb, writer.DatabaseFormat);
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("Table1", RowCriteria.Where("otherfk2", 10), new RowValues { ["otherfk2"] = 13 }, Ct));

                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Table1", RowCriteria.Where("otherfk2", 13), new RowValues { ["otherfk2"] = 999 }, Ct));
                Assert.Contains(
                    "UPDATE of 'Table1' violates foreign-key constraint 'Table3Table1': no matching row in 'Table3'",
                    ex.Message,
                    StringComparison.Ordinal);
            });
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        using DataTable table1 = await reader.ReadTableAsync("Table1", cancellationToken: Ct);
        Assert.Equal(
            [11, 11, 13, 13],
            table1.AsEnumerable().Select(row => Convert.ToInt32(row["otherfk2"], CultureInfo.InvariantCulture)).Order());
    }

    /// <summary>
    /// Checks that <paramref name="ex"/> says the relationship cannot be
    /// enforced and names what is missing, rather than a missing parent row.
    /// </summary>
    /// <param name="ex">The exception.</param>
    /// <param name="relationship">The relationship name.</param>
    /// <param name="missing">The text naming the missing table or column.</param>
    private static void AssertCannotBeEnforced(InvalidOperationException ex, string relationship, string missing)
    {
        Assert.Contains($"Foreign-key constraint '{relationship}' cannot be enforced", ex.Message, StringComparison.Ordinal);
        Assert.Contains(missing, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("no matching row", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Returns a database with an empty table <c>Tree</c> (<c>Id</c> primary
    /// key, <c>ParentId</c>) and the enforced relationship <c>FK_Tree_Self</c>
    /// from <c>Tree.ParentId</c> to <c>Tree.Id</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    private async Task<MemoryStream> CreateTreeDatabaseAsync(DatabaseFormat format)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "Tree",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))],
                Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Tree_Self", "Tree", "Id", "Tree", "ParentId"), Ct);
        }

        ms.Position = 0;
        return ms;
    }
}
