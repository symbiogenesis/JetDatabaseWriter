namespace JetDatabaseWriter.Tests.Relationships;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Validates that pre-existing Access-authored FK surrogate indexes survive
/// writer mutations (row inserts into unrelated tables, new table creation).
/// Closes the remaining §1.4 gap: "emitting or preserving Access-authored
/// surrogate indexes on write.".
/// </summary>
public sealed class ForeignKeySurrogateIndexPreservationTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// After inserting a row into a new scratch table (no FK relationship),
    /// the pre-existing FK indexes on NorthwindTraders tables remain
    /// readable and unchanged.
    /// </summary>
    [Fact]
    public async Task FkSurrogateIndexes_SurviveUnrelatedTableInsert()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        // Snapshot FK index metadata before mutation.
        var before = await SnapshotFkIndexesAsync(path, TestContext.Current.CancellationToken);
        Assert.True(before.Count > 0, "NorthwindTraders should have FK indexes.");

        // Mutate: copy the database, add a scratch table with rows.
        var temp = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);
        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "ScratchPreserve",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Val", typeof(string), maxLength: 50)],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("ScratchPreserve", [1, "hello"], TestContext.Current.CancellationToken);
        }

        // Re-read FK indexes after mutation.
        var after = await SnapshotFkIndexesFromStreamAsync(temp, TestContext.Current.CancellationToken);

        // Every FK index that existed before should still be present.
        Assert.Equal(before.Count, after.Count);
        foreach (var (table, indexName) in before)
        {
            Assert.Contains((table, indexName), after);
        }
    }

    /// <summary>
    /// After creating a new relationship (which adds FK surrogate indexes),
    /// the pre-existing FK indexes from other tables are not corrupted.
    /// </summary>
    [Fact]
    public async Task FkSurrogateIndexes_SurviveNewRelationshipCreation()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        var before = await SnapshotFkIndexesAsync(path, TestContext.Current.CancellationToken);
        Assert.True(before.Count > 0, "NorthwindTraders should have FK indexes.");

        var temp = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);
        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("PresPar", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("PresChi", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_PresChi_PresPar", "PresPar", "Id", "PresChi", "ParId"),
                TestContext.Current.CancellationToken);
        }

        var after = await SnapshotFkIndexesFromStreamAsync(temp, TestContext.Current.CancellationToken);

        // The new relationship adds 2 FK entries (one per side); all old ones remain.
        Assert.True(after.Count >= before.Count + 2, $"Expected at least {before.Count + 2} FK indexes after relationship, got {after.Count}.");
        foreach (var (table, indexName) in before)
        {
            Assert.Contains((table, indexName), after);
        }
    }

    /// <summary>
    /// FK surrogate indexes that existed before a write still report valid
    /// column names (non-empty) and non-zero RelatedTablePage.
    /// </summary>
    [Fact]
    public async Task FkSurrogateIndexes_RetainValidMetadataAfterWrite()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        var temp = await db.CopyToStreamAsync(path, TestContext.Current.CancellationToken);
        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "MetaPreserve",
                [new ColumnDefinition("X", typeof(int))],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("MetaPreserve", [42], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        int fkCount = 0;
        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(table, TestContext.Current.CancellationToken);
            foreach (IndexMetadata idx in indexes.Where(i => i.IsForeignKey))
            {
                fkCount++;
                Assert.NotEmpty(idx.Columns);
                Assert.All(idx.Columns, c =>
                    Assert.False(string.IsNullOrEmpty(c.Name), $"FK index '{idx.Name}' on '{table}' has an unresolved column."));
                Assert.True(idx.RelatedTablePage > 0, $"FK index '{idx.Name}' on '{table}' has RelatedTablePage=0.");
            }
        }

        Assert.True(fkCount > 0, "Expected FK indexes to survive the write.");
    }

    private static async Task<List<(string Table, string IndexName)>> SnapshotFkIndexesFromStreamAsync(MemoryStream stream, CancellationToken ct)
    {
        await using var reader = await OpenReaderAsync(stream, ct);
        return await CollectFkIndexesAsync(reader, ct);
    }

    private static async Task<List<(string Table, string IndexName)>> CollectFkIndexesAsync(AccessReader reader, CancellationToken ct)
    {
        List<string> tables = await reader.ListTablesAsync(ct);
        var result = new List<(string, string)>();
        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(table, ct);
            foreach (IndexMetadata idx in indexes.Where(i => i.IsForeignKey))
            {
                result.Add((table, idx.Name));
            }
        }

        return result;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private async Task<List<(string Table, string IndexName)>> SnapshotFkIndexesAsync(string path, CancellationToken ct)
    {
        var reader = await db.GetReaderAsync(path, ct);
        return await CollectFkIndexesAsync(reader, ct);
    }
}
