namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Constraint lifetimes follow schema operations even when physical storage is reused.</summary>
public sealed class ConstraintReplacementTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A new table at the same physical location receives only its own constraints.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="automatic">Whether each operation uses an automatic transaction.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task RecreateTable_ReusedDefinitionHasNewConstraints(DatabaseFormat format, bool automatic)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, Options(automatic), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Rules", Columns(7, oldRule: true), Ct);
            (long originalPage, byte[] originalDefinition, TableDef original) = await DefinitionAsync(stream);
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Score] < 10"), Ct);
            await writer.InsertRowAsync("Rules", [40, DbDefault.Value], Ct);

            await writer.DropTableAsync("Rules", Ct);
            await writer.CreateTableAsync("Rules", Columns(19, oldRule: false), Ct);
            (long replacementPage, byte[] replacementDefinition, TableDef replacement) = await DefinitionAsync(stream);
            Assert.Equal(originalPage, replacementPage);
            Assert.Equal(original.Columns.Count, replacement.Columns.Count);
            for (int index = 0; index < original.Columns.Count; index++)
            {
                Assert.Equal(original.Columns[index].ColNum, replacement.Columns[index].ColNum);
                Assert.Equal(original.Columns[index].RawDescriptor, replacement.Columns[index].RawDescriptor);
            }

            // Jet3 retains shared map storage and allocates different map rows; those
            // pointers are not column identity. Jet4/ACE reuse the entire TDEF image.
            if (format != DatabaseFormat.Jet3Mdb)
            {
                Assert.Equal(originalDefinition, replacementDefinition);
            }

            // Creation must also forget the old table rule before a new one is set.
            await writer.InsertRowAsync("Rules", [DBNull.Value, DbDefault.Value], Ct);
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Score] < 30"), Ct);
            await AssertRejectedAsync(writer, stream, 7);
            await AssertRejectedAsync(writer, stream, 31);
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowAsync("Rules", [DBNull.Value, 20], Ct);
                await AssertRejectedAsync(writer, stream, 7);
                await AssertRejectedAsync(writer, stream, 31);
                Assert.False(transaction.IsCommitted);
                Assert.False(transaction.IsRolledBack);
                await transaction.CommitAsync(Ct);
            }
        }

        await using (AccessWriter reopened = await OpenAsync(stream, automatic))
        {
            await AssertRejectedAsync(reopened, stream, 7);
            await AssertRejectedAsync(reopened, stream, 31);
            await reopened.InsertRowAsync("Rules", [DBNull.Value, DbDefault.Value], Ct);
        }

        await AssertRowsAsync(stream, [1, 2, 3], [19, 20, 19]);
    }

    /// <summary>Rolling back replacement restores the original cached defaults, rules and counter.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RecreateTable_RollbackRestoresOriginalConstraints(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, Options(false), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Rules", Columns(7, oldRule: true), Ct);
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Score] < 10"), Ct);
            await writer.InsertRowAsync("Rules", [40, DbDefault.Value], Ct);
            byte[] before = stream.ToArray();
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
            {
                await writer.DropTableAsync("Rules", Ct);
                await writer.CreateTableAsync("Rules", Columns(19, oldRule: false), Ct);
                await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Score] >= 10"), Ct);
                await writer.InsertRowAsync("Rules", [DBNull.Value, DbDefault.Value], Ct);
                await AssertRejectedAsync(writer, stream, 7);
                await transaction.RollbackAsync(Ct);
            }

            Assert.Equal(before, stream.ToArray());
            await AssertRejectedAsync(writer, stream, 19);
            await writer.InsertRowAsync("Rules", [DBNull.Value, DbDefault.Value], Ct);
        }

        await AssertRowsAsync(stream, [40, 41], [7, 7]);
    }

    /// <summary>Dropping and readding a column must not resurrect its CLR rule or default.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RecreateColumn_ReusedDescriptorHasNewConstraints(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, Options(false), leaveOpen: true, Ct))
        {
            ColumnDefinition[] columns = Columns(7, oldRule: true);
            if (format == DatabaseFormat.AceAccdb)
            {
                // A surviving complex column requires the rewrite to retain its TDEF root.
                columns = [columns[0], new("Files", typeof(byte[])) { IsAttachment = true }, columns[1]];
            }

            await writer.CreateTableAsync("Rules", columns, Ct);
            (long originalPage, ColumnInfo original) = await ScoreColumnAsync(stream);
            await writer.InsertRowAsync("Rules", new RowValues { ["Id"] = 40, ["Score"] = DbDefault.Value }, Ct);
            await writer.DropColumnAsync("Rules", "Score", Ct);
            await writer.AddColumnAsync("Rules", Columns(19, oldRule: false)[1], Ct);
            (long replacementPage, ColumnInfo replacement) = await ScoreColumnAsync(stream);
            if (format == DatabaseFormat.AceAccdb)
            {
                Assert.Equal(originalPage, replacementPage);
            }

            Assert.Equal(original.ColNum, replacement.ColNum);
            Assert.Equal(original.RawDescriptor, replacement.RawDescriptor);
            await AssertRejectedAsync(writer, stream, 7);
            await writer.InsertRowAsync("Rules", new RowValues { ["Score"] = DbDefault.Value }, Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("Rules", "Id", 40, new Dictionary<string, object?> { ["Score"] = 20 }, Ct));
        }

        await AssertRowsAsync(stream, [40, 41], [20, 19]);
    }

    private static ColumnDefinition[] Columns(int defaultValue, bool oldRule) =>
    [
        new("Id", typeof(int)) { IsAutoIncrement = true },
        new("Score", typeof(int))
        {
            DefaultValue = defaultValue,
            ValidationRule = oldRule ? static value => value is int score && score < 10 : static value => value is int score && score >= 10,
            ValidationRuleExpression = oldRule ? "< 10" : ">= 10",
        },
    ];

    private static AccessWriterOptions Options(bool automatic) => new() { UseLockFile = false, UseTransactionalWrites = automatic };

    private static ValueTask<AccessWriter> OpenAsync(MemoryStream stream, bool automatic)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, Options(automatic), leaveOpen: true, Ct);
    }

    private static async Task AssertRejectedAsync(AccessWriter writer, MemoryStream stream, int score)
    {
        byte[] before = stream.ToArray();
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("Rules", new RowValues { ["Score"] = score }, Ct));
        Assert.Equal(before, stream.ToArray());
        _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
            await writer.UpdateRowsAsync("Rules", RowCriteria.All(), new RowValues { ["Score"] = score }, Ct));
        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<(long Page, byte[] Bytes, TableDef Definition)> DefinitionAsync(MemoryStream stream)
    {
        await using var copy = new MemoryStream(stream.ToArray());
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(copy, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Rules", Ct));
        TableDef definition = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(entry.TDefPage, Ct));
        return (entry.TDefPage, await reader.ReadPageCopyAsync(entry.TDefPage, Ct), definition);
    }

    private static async Task<(long Page, ColumnInfo Column)> ScoreColumnAsync(MemoryStream stream)
    {
        await using var copy = new MemoryStream(stream.ToArray());
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(copy, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Rules", Ct));
        TableDef definition = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(entry.TDefPage, Ct));
        return (entry.TDefPage, Assert.Single(definition.Columns, column => column.Name == "Score"));
    }

    private static async Task AssertRowsAsync(MemoryStream stream, int[] ids, int[] scores)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        DataTable table = await reader.ReadTableAsync("Rules", cancellationToken: Ct);
        DataRow[] rows = table.AsEnumerable().OrderBy(row => (int)row["Id"]).ToArray();
        Assert.Equal(ids, rows.Select(row => (int)row["Id"]));
        Assert.Equal(scores, rows.Select(row => (int)row["Score"]));
    }
}
