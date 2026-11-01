namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Creates indexes over existing rows without losing the table's schema or identities.</summary>
public sealed class CreateIndexTests
{
    /// <summary>Gets every supported format and statement mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task PopulatedTable_BuildsCompositeIndexAndRetainsExistingIndex(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Category", typeof(int)), new("Score", typeof(int))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1, 2, 7], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [2, 1, 5], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [3, 2, 9], TestContext.Current.CancellationToken);
                await writer.CreateIndexAsync("Items", new("IX_Category_Score", ["Category", "Score"]) { DescendingColumns = ["Score"], IsUnique = true }, TestContext.Current.CancellationToken);
            },
                TestContext.Current.CancellationToken);
        }

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken);
            Assert.Equal(2, indexes.Count);
            Assert.Single(indexes, index => index.Kind == IndexKind.PrimaryKey);
            IndexMetadata added = Assert.Single(indexes, index => index.Name == "IX_Category_Score");
            Assert.True(added.EnforcesUniqueness);
            Assert.Equal(["Category", "Score"], added.Columns.Select(column => column.Name));
            Assert.Equal([true, false], added.Columns.Select(column => column.IsAscending));
            using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal([1, 2, 3], rows.Rows.Cast<DataRow>().Select(row => (int)row["Id"]).Order());
        }

        await using AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<JetConstraintException>(async () => await reopened.InsertRowAsync("Items", [4, 2, 7], TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<JetConstraintException>(async () => await reopened.InsertRowAsync("Items", [1, 3, 8], TestContext.Current.CancellationToken));
        await WriteModes.RunAsync(reopened, mode, async () => await reopened.InsertRowAsync("Items", [4, 2, 8], TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task DuplicateExistingKeys_RefusesWithoutChangingDatabase(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Value", typeof(int))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1, 7], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [2, 7], TestContext.Current.CancellationToken);
            },
                TestContext.Current.CancellationToken);

            byte[] before = stream.ToArray();
            await WriteModes.RunAsync(writer, mode, async () => await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.CreateIndexAsync("Items", new("UniqueValue", "Value") { IsUnique = true }, TestContext.Current.CancellationToken)), TestContext.Current.CancellationToken);
            Assert.Equal(before, stream.ToArray());
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Empty(await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task PrimaryKeyAndInvalidDeclarations_AreAtomic(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        await writer.CreateIndexAsync("Items", new("PK_Items", "Id") { IsPrimaryKey = true }, TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateIndexAsync("Items", new("pk_items", "Id"), TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateIndexAsync("Items", new("SecondPrimary", "Id") { IsPrimaryKey = true }, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateIndexAsync("Items", new("Missing", "Absent"), TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
        await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Items", [DBNull.Value], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExplicitRollback_RemovesIndexAndRestoresRows()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new() { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken))
        {
            await writer.CreateIndexAsync("Items", new("UniqueId", "Id") { IsUnique = true }, TestContext.Current.CancellationToken);
            await transaction.RollbackAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(before, stream.ToArray());
        await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task NullKeys_RequiredRefusesAndIgnoreNullsAdmits(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Value", typeof(int))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1, DBNull.Value], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [2, DBNull.Value], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [3, 7], TestContext.Current.CancellationToken);
            },
                TestContext.Current.CancellationToken);

            byte[] before = stream.ToArray();
            await WriteModes.RunAsync(writer, mode, async () => await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.CreateIndexAsync("Items", new("RequiredValue", "Value") { IsRequired = true }, TestContext.Current.CancellationToken)), TestContext.Current.CancellationToken);
            Assert.Equal(before, stream.ToArray());
            await WriteModes.RunAsync(writer, mode, async () => await writer.CreateIndexAsync("Items", new("UniqueValue", "Value") { IsUnique = true, IgnoreNulls = true }, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        IndexMetadata index = Assert.Single(await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken));
        Assert.True(index.IgnoreNulls);
        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Rows.Count);
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task IndexOnlyRewrite_RetainsOpaqueNameMap(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, TestContext.Current.CancellationToken);
        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                await writer.CreateTableAsync("Items", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
                ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("Items", TestContext.Current.CancellationToken);
                ColumnPropertyBlock? properties = await writer.Services.Snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, TestContext.Current.CancellationToken);
                ColumnPropertyBlockBuilder builder = properties is null ? new ColumnPropertyBlockBuilder() : ColumnPropertyBlockBuilder.FromBlock(properties);
                builder.GetOrAddTableTarget().Entries.Add(new ColumnPropertyEntryBuilder { Name = "NameMap", DataType = ColumnType.OleType, DdlFlag = 0x41, Value = [0x00, 0x80, 0xFF, 0x2A] });
                await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("Items", table.Entry.TDefPage, builder.ToBytes(writer.Database.Format), cancellationToken: TestContext.Current.CancellationToken);
                await writer.CreateIndexAsync("Items", new("IX_Id", "Id"), TestContext.Current.CancellationToken);
                ColumnPropertyBlock stored = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, TestContext.Current.CancellationToken));
                ColumnPropertyEntry map = Assert.Single(Assert.IsType<ColumnPropertyTarget>(stored.FindTableTarget()).Entries, entry => entry.Name == "NameMap");
                Assert.Equal(ColumnType.OleType, map.DataType);
                Assert.Equal(0x41, map.DdlFlag);
                Assert.Equal([0x00, 0x80, 0xFF, 0x2A], map.Value);
            },
            TestContext.Current.CancellationToken);
    }
}
