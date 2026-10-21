namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Ordinary append-only Memo changes maintain their protected native history.</summary>
public sealed class AppendOnlyHistoryMutationTests
{
    private const string History = "VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>An unrelated cascade preserves history; an assigned Memo adds its version without changing the reference.</summary>
    /// <param name="mode">The write transaction mode.</param>
    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task Update_RecordsChangedMemoAndPreservesParentReference(WriteMode mode)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        RawTable before = await ReadRawTableAsync(stream, "Table1");
        int reference = Assert.NotNull(Slot(before, Assert.Single(before.Rows, row => Equals(row[0], "row3")), History));
        DateTime earliest = DateTimeOffset.Now.DateTime.AddSeconds(-2);
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync("HistoryParents", [new ColumnDefinition("Id", typeof(string), before.Definition.Columns[0].Size / 2) { IsPrimaryKey = true }], Ct);
                await writer.InsertRowsAsync("HistoryParents", before.Rows.Select(row => new[] { row[0] }), Ct);
                await writer.CreateRelationshipAsync(new RelationshipDefinition("HistoryParent", "HistoryParents", "Id", "Table1", "id") { CascadeUpdates = true }, Ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("HistoryParents", RowCriteria.Where("Id", "row3"), new RowValues { ["Id"] = "moved3" }, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "moved3"), new RowValues { ["append-memo-data"] = "library revision" }, Ct));
            });
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        MultiValueItem[] versions = [.. (await reader.GetMultiValueItemsAsync("Table1", History, Ct)).Where(item => item.ConceptualTableId == reference)];
        Assert.Equal(["row3-memo", "row3-memo-revised", "row3-memo-again", "library revision"], versions.Select(item => item.Value));
        Assert.InRange(Assert.NotNull(versions[^1].Modified), earliest, DateTimeOffset.Now.DateTime.AddSeconds(2));
    }

    /// <summary>Rolling back also restores the history rows and their generated IDs.</summary>
    [Fact]
    public async Task Update_RollbackRestoresMemoHistoryAndCounters()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream, WriteMode.Direct))
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
        {
            await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row3"), new RowValues { ["append-memo-data"] = "rolled back" }, Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    /// <summary>After rollback, the same writer reuses the uncommitted history identity.</summary>
    [Fact]
    public async Task Rollback_SubsequentVersionReusesUncommittedIdentity()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        int previousId;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            previousId = (await reader.GetMultiValueItemsAsync("Table1", History, Ct)).Max(item => item.Id);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream, WriteMode.Direct))
        {
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
            {
                await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row3"), new RowValues { ["append-memo-data"] = "rolled back" }, Ct);
                await transaction.RollbackAsync(Ct);
            }

            await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row3"), new RowValues { ["append-memo-data"] = "retained" }, Ct);
        }

        await using AccessReader after = await OpenReaderAsync(stream);
        MultiValueItem[] versions = [.. await after.GetMultiValueItemsAsync("Table1", History, Ct)];
        Assert.DoesNotContain(versions, item => Equals(item.Value, "rolled back"));
        Assert.Equal(previousId + 1, Assert.Single(versions, item => Equals(item.Value, "retained")).Id);
    }

    /// <summary>Native ACE records explicit identical, empty and Null assignments, but no unrelated edit.</summary>
    /// <param name="fixture">The Microsoft-authored source file.</param>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task Update_AssignmentSemanticsMatchNativeAce(string fixture)
    {
        await using MemoryStream stream = await CopyFixtureAsync(fixture);
        RawTable before = await ReadRawTableAsync(stream, "Table1");
        int reference = Assert.NotNull(Slot(before, Assert.Single(before.Rows, row => Equals(row[0], "row1")), History));
        await using (AccessWriter writer = await OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row1"), new RowValues { ["memo-data"] = "unrelated" }, Ct);
            foreach (object value in new object[] { "same", "same", string.Empty, DBNull.Value })
            {
                await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row1"), new RowValues { ["append-memo-data"] = value }, Ct);
            }
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        MultiValueItem[] versions = [.. (await reader.GetMultiValueItemsAsync("Table1", History, Ct)).Where(item => item.ConceptualTableId == reference)];
        Assert.Equal(["same", "same", "\0", "\0"], versions.Select(item => item.Value));
        Assert.All(versions, item => Assert.NotNull(item.Modified));
    }

    /// <summary>Native ACE versions explicit empty and Null Memo inserts, while an omitted Null has no version.</summary>
    [Fact]
    public async Task Insert_ExplicitMemoAssignmentsCreateVersions()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        await using (AccessWriter writer = await OpenWriterAsync(stream, WriteMode.Direct))
        {
            var insertedRows = new RowValues[]
            {
                new() { ["id"] = "new-text", ["append-memo-data"] = "inserted" },
                new() { ["id"] = "new-empty", ["append-memo-data"] = string.Empty },
                new() { ["id"] = "new-null", ["append-memo-data"] = null },
                new() { ["id"] = "new-omitted" },
            };
            await writer.InsertRowsAsync("Table1", insertedRows, Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        MultiValueItem[] added = [.. (await reader.GetMultiValueItemsAsync("Table1", History, Ct)).Where(item => item.ConceptualTableId > 5)];
        Assert.Equal(["inserted", "\0", "\0"], added.Select(item => item.Value));
        Assert.All(added, item => Assert.NotNull(item.Modified));
    }

    /// <summary>A public flat-table name cannot bypass protected version history.</summary>
    [Fact]
    public async Task FlatTable_DirectDmlIsRefusedWithoutMutation()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        string flatName;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            flatName = Assert.Single(await reader.GetComplexColumnsAsync("Table1", Ct), column => column.ColumnName == History).FlatTableName;
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream, WriteMode.Direct))
        {
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.InsertRowAsync(flatName, new RowValues(), Ct));
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.UpdateRowsAsync(flatName, RowCriteria.All(), new RowValues { ["append-memo-data"] = "forged" }, Ct));
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DeleteRowsAsync(flatName, RowCriteria.All(), Ct));
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData("Value")]
    [InlineData("FileData")]
    public async Task PayloadNameCannotBypassProtectedHistory(string memoName)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.RenameColumnAsync("Table1", "append-memo-data", memoName, Ct);
        }

        byte[] before = stream.ToArray();
        var key = new Dictionary<string, object?> { ["id"] = "row2" };
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            JetNotSupportedException add = await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.AddMultiValueItemAsync("Table1", History, key, "forged", Ct));
            JetNotSupportedException replace = await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.ReplaceMultiValueItemAsync("Table1", History, key, 1, "forged", Ct));
            JetNotSupportedException remove = await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.RemoveMultiValueItemAsync("Table1", History, key, 1, Ct));
            Assert.Equal(JetErrorCode.VersionHistoryNotEditable, add.ErrorCode);
            Assert.Equal(JetErrorCode.VersionHistoryNotEditable, replace.ErrorCode);
            Assert.Equal(JetErrorCode.VersionHistoryNotEditable, remove.ErrorCode);
        }

        Assert.Equal(before, stream.ToArray());
    }

    /// <summary>Renaming an append-only Memo keeps its history binding and permits the next ordinary update.</summary>
    [Fact]
    public async Task RenameMemo_SubsequentUpdateRetainsHistoricalVersions()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        await using (AccessWriter writer = await OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.RenameColumnAsync("Table1", "append-memo-data", "RenamedMemo", Ct);
            await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row3"), new RowValues { ["RenamedMemo"] = "after rename" }, Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Equal(["row3-memo", "row3-memo-revised", "row3-memo-again", "after rename"], (await reader.GetMultiValueItemsAsync("Table1", History, Ct)).Where(item => item.ConceptualTableId == 3).Select(item => item.Value));
    }
}
