namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using WriteMode = JetDatabaseWriter.Tests.Writer.TransactionReadVisibilityTests.WriteMode;

/// <summary>
/// An enforced relationship whose table or key column the writer cannot find
/// fails closed: every write it would have to check throws
/// <see cref="InvalidOperationException"/> naming the relationship and what is
/// missing, instead of reporting a missing parent row or skipping the check.
/// A write the relationship does not constrain (a null foreign key, an update
/// that leaves the key alone, a delete that matches nothing) still succeeds.
/// The relationships are planted straight into <c>MSysRelationships</c>,
/// because <c>CreateRelationshipAsync</c> and <c>DropTableAsync</c> never
/// leave one dangling. Jet3 resolves the parent through its key set; Jet4 and
/// ACCDB try the index seek first.
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
                InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
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

                InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
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
                InvalidOperationException insert = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.InsertRowAsync("C", [3, 1, "x"], Ct));
                AssertCannotBeEnforced(insert, "FK_NoColumn", "table 'P' has no column 'NoSuchColumn'");

                InvalidOperationException delete = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
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
                InvalidOperationException insert = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.InsertRowAsync("C", [3, 1, "x"], Ct));
                AssertCannotBeEnforced(insert, "FK_NoFkColumn", "table 'C' has no column 'NoSuchFk'");

                InvalidOperationException delete = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
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
                InvalidOperationException delete = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                AssertCannotBeEnforced(delete, "FK_NoChild", "foreign table 'NoSuchChild' was not found");

                InvalidOperationException update = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 8 }, Ct));
                AssertCannotBeEnforced(update, "FK_NoChild", "foreign table 'NoSuchChild' was not found");

                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Name"] = "uno" }, Ct));
                Assert.Equal(0, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 99), Ct));
            });
        }

        Assert.Equal(["1|uno"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
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
}
