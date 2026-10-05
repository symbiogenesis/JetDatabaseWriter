namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Referenced nullable keys are checked and cascaded when cleared.</summary>
/// <param name="db">Caches fixture databases.</param>
public sealed class ForeignKeyNullReferenceUpdateTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets each database format and write mode.</summary>
    /// <returns>The format and mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>Gets each write mode for calculated-column ACCDB coverage.</summary>
    /// <returns>The write modes.</returns>
    public static TheoryData<WriteMode> AccdbModes()
    {
        var data = new TheoryData<WriteMode>();
        foreach (WriteMode mode in Enum.GetValues<WriteMode>())
        {
            data.Add(mode);
        }

        return data;
    }

    /// <summary>A restrictive relationship refuses clearing an alternate key before writing.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ReferencedKeyToNullWithoutCascade_LeavesBothTablesUnchanged(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: false, requiredChild: false, composite: false, self: false);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = null }, Ct));
                Assert.Contains("cascade-update is not enabled", error.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["2|two|7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|two|7", "3||7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>A cascade copies null and preserves the other component of a composite key.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_CompositeReferencedKeyToPartialNull_CascadesOnlyMatchingChild(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true, requiredChild: false, composite: true, self: false);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = null, ["Code"] = 8 }, Ct)));
        }

        Assert.Equal(["2||8"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1||8", "3||7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
    }

    /// <summary>A cascade into a required column refuses before either table changes.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ReferencedKeyToNullWithRequiredChild_LeavesBothTablesUnchanged(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true, requiredChild: true, composite: false, self: false);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = null }, Ct));
                Assert.Contains("NOT NULL", error.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["2|two|7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|two|7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>A self-related row takes a null cascade in its own rewrite.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfReferencedKeyToNull_RewritesRowOnce(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true, requiredChild: false, composite: false, self: true);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = null }, Ct)));
        }

        Assert.Equal(["2|||7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P");
    }

    /// <summary>A self-related required foreign key also refuses a null cascade before writing.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfReferencedKeyToNullWithRequiredChild_LeavesRowUnchanged(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true, requiredChild: true, composite: false, self: true);
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = null }, Ct));
                Assert.Contains("NOT NULL", error.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["2|two|two|7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
    }

    /// <summary>A key with a null component has no dependents to move when populated again.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_PartialNullReferencedKeyToNonNull_LeavesNullChildKeysAlone(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateAsync(format, cascade: true, requiredChild: false, composite: true, self: false);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = null }, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = "three", ["Code"] = 8 }, Ct));
            });
        }

        Assert.Equal(["2|three|8"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1||7", "3||7"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
    }

    /// <summary>A null cascade refreshes a child calculated column that reads the foreign key.</summary>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AccdbModes))]
    public async Task Update_ReferencedKeyToNull_RecomputesChildCalculatedValue(WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "P",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Name", typeof(string), maxLength: 20)],
                Ct);
            await writer.CreateTableAsync(
                "C",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentName", typeof(string), maxLength: 20),
                    new ColumnDefinition("Label", typeof(string), maxLength: 20)
                    {
                        IsCalculated = true,
                        CalculationExpression = "Nz([ParentName], \"missing\")",
                    },
                ],
                Ct);
            await writer.InsertRowAsync("P", [2, "two"], Ct);
            await writer.InsertRowAsync("C", [1, "two", null], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Name", "C", "ParentName") { CascadeUpdates = true }, Ct);
        }

        Assert.Equal(["1|two|two"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = null }, Ct)));
        }

        Assert.Equal(["1||missing"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
    }

    private async Task<MemoryStream> CreateAsync(DatabaseFormat format, bool cascade, bool requiredChild, bool composite, bool self)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            if (self)
            {
                await writer.CreateTableAsync(
                    "P",
                    [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Name", typeof(string), maxLength: 20), new ColumnDefinition("ParentName", typeof(string), maxLength: 20) { IsNullable = !requiredChild }, new ColumnDefinition("Code", typeof(int))],
                    Ct);
                await writer.InsertRowAsync("P", [2, "two", "two", 7], Ct);
                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_P_P", "P", "Name", "P", "ParentName") { CascadeUpdates = cascade }, Ct);
            }
            else
            {
                await writer.CreateTableAsync(
                    "P",
                    [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Name", typeof(string), maxLength: 20), new ColumnDefinition("Code", typeof(int))],
                    Ct);
                await writer.CreateTableAsync(
                    "C",
                    [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentName", typeof(string), maxLength: 20) { IsNullable = !requiredChild }, new ColumnDefinition("ParentCode", typeof(int))],
                    Ct);
                await writer.InsertRowAsync("P", [2, "two", 7], Ct);
                await writer.InsertRowAsync("C", [1, "two", 7], Ct);
                if (!requiredChild)
                {
                    await writer.InsertRowAsync("C", [3, null, 7], Ct);
                }

                RelationshipDefinition relationship = composite
                    ? new RelationshipDefinition("FK_C_P", "P", ["Name", "Code"], "C", ["ParentName", "ParentCode"])
                    : new RelationshipDefinition("FK_C_P", "P", "Name", "C", "ParentName");
                await writer.CreateRelationshipAsync(relationship with { CascadeUpdates = cascade }, Ct);
            }
        }

        ms.Position = 0;
        return ms;
    }
}
