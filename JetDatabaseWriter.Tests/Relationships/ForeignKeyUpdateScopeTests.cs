namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using WriteMode = JetDatabaseWriter.Tests.Writer.TransactionReadVisibilityTests.WriteMode;

/// <summary>
/// <c>UpdateRowsAsync</c> checks a relationship's foreign key only on the rows
/// whose key it changes, as Access does: an update of other columns, or one
/// that assigns a key its current value, leaves the key unchecked even when it
/// has no parent. A key changed to a value with no parent row is still
/// rejected before anything is written. The child row with <c>Id</c> 2 is
/// planted with the orphaned key 99 before the relationship is created, which
/// <c>CreateRelationshipAsync</c> does not check.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ForeignKeyUpdateScopeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Relationship = "FK_C_P";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>Gets the Access-authored fixtures in every write mode.</summary>
    /// <returns>The fixture names and write modes.</returns>
    public static TheoryData<string, WriteMode> AccessAuthoredFixturesAndModes()
    {
        var data = new TheoryData<string, WriteMode>();
        foreach (string fixture in new[] { "NorthwindTraders", "indexTestV1997" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(fixture, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_UnassignedForeignKey_OnOrphanedRow_IsNotChecked(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(2, await writer.UpdateRowsAsync("C", RowCriteria.All(), new RowValues { ["Note"] = "z" }, Ct)));
        }

        Assert.Equal(["1|1|z", "2|99|z"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ForeignKeyReassignedSameValue_IsNotChecked(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 2), new RowValues { ["ParentId"] = 99, ["Note"] = "y" }, Ct)));
        }

        Assert.Equal(["1|1|a", "2|99|y"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ForeignKeyChangedToMissingParent_IsRejectedAndChangesNothing(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.UpdateRowsAsync("C", RowCriteria.All(), new RowValues { ["ParentId"] = 42, ["Note"] = "z" }, Ct));
                Assert.Contains($"UPDATE of 'C' violates foreign-key constraint '{Relationship}'", ex.Message, StringComparison.Ordinal);
                Assert.Contains("no matching row in 'P' for the new ParentId value(s)", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(["1|1|a", "2|99|b"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ForeignKeyChangedToExistingParentOrNull_Succeeds(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("P", [2, "two"], Ct);
                Assert.Equal(2, await writer.UpdateRowsAsync("C", RowCriteria.All(), new RowValues { ["ParentId"] = 2 }, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 1), new RowValues { ["ParentId"] = null }, Ct));
            });
        }

        Assert.Equal(["1||a", "2|2|b"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>
    /// When <c>P</c> is the primary table of two relationships, on different
    /// columns, an update that changes <c>P.Id</c> checks only the relationship
    /// on <c>Id</c>: the children of <c>P.Name</c>, which the update leaves
    /// alone, neither block it nor change.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_PrimaryKeyOfOneRelationship_DoesNotCheckAnotherOnUnchangedColumn(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "D",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PName", typeof(string), maxLength: 20)],
                Ct);
            await writer.InsertRowAsync("P", [2, "two"], Ct);
            await writer.InsertRowAsync("D", [1, "two"], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_D_P", "P", "Name", "D", "PName"), Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Id"] = 5 }, Ct)));
        }

        Assert.Equal(["1|one", "5|two"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|two"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "D"));
    }

    /// <summary>
    /// An update of a column outside every relationship of an Access-authored
    /// child table succeeds and changes every row: OrderDetails in
    /// NorthwindTraders.accdb (three enforced relationships, on ACCDB) and
    /// Table1 in indexTestV1997.mdb (two, on Jet3).
    /// </summary>
    /// <param name="fixture">The fixture.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AccessAuthoredFixturesAndModes))]
    public async Task AccessAuthoredChild_UpdateOfNonKeyColumn_Succeeds(string fixture, WriteMode mode)
    {
        (string path, string table, string column, object value, int rows) = fixture == "NorthwindTraders"
            ? (TestDatabases.NorthwindTraders, "OrderDetails", "Discount", (object)0.25, 130)
            : (TestDatabases.IndexTestV1997, "Table1", "data", "q", 4);
        if (!File.Exists(path))
        {
            Assert.Skip($"{fixture} is unavailable on this machine.");
        }

        await using MemoryStream ms = await db.CopyToStreamAsync(path, Ct);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(rows, await writer.UpdateRowsAsync(table, RowCriteria.All(), new RowValues { [column] = value }, Ct)));
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        using DataTable after = await reader.ReadDataTableAsync(table, cancellationToken: Ct);
        Assert.Equal(rows, after.Rows.Count);
        Assert.All(
            after.AsEnumerable(),
            row => Assert.Equal(Convert.ToString(value, CultureInfo.InvariantCulture), Convert.ToString(row[column], CultureInfo.InvariantCulture)));
    }

    /// <summary>
    /// Returns <c>P</c> with row 1 and <c>C</c> with rows (1, 1, 'a') and the
    /// orphan (2, 99, 'b'), related by the enforced <see cref="Relationship"/>
    /// from <c>C.ParentId</c> to <c>P.Id</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    private async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], [[1, 1, "a"], [2, 99, "b"]]);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, "P", "Id", "C", "ParentId"), Ct);
        }

        ms.Position = 0;
        return ms;
    }
}
