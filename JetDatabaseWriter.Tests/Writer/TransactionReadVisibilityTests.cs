namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Writer workflows that read the table they are about to change (update,
/// delete, cascade, index rebuild, schema rewrite, relationship enforcement)
/// must see the rows the same writer has already written, including the
/// pending writes of an explicit or auto-commit transaction, and must mutate
/// exactly the rows they decoded.
/// </summary>
public sealed class TransactionReadVisibilityTests
{
    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the relationship test cases: Jet4 (from an Access-authored file) and ACCDB, in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> RelationshipFormatsAndModes() =>
        WriteModes.Combine(DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    /// <summary>Gets every writer-created format (Jet3, Jet4, ACCDB) in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> AllFormatsAndModes() =>
        WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task DeleteThenUpdate_UpdatesOnlyTheMatchingRow(DatabaseFormat format, WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, mode))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 50)], Ct);
            for (int id = 1; id <= 3; id++)
            {
                await writer.InsertRowAsync("T", [id, "r" + id], Ct);
            }

            await RunAsync(writer, mode, async () =>
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 2, new Dictionary<string, object?> { ["Name"] = "updated" }, Ct));
            });
        }

        Assert.Equal(["2|updated", "3|r3"], await ReadRowsAsync(ms, "T"));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DeleteThenUpdate_RolledBack_LeavesTableUnchanged(DatabaseFormat format)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 50)], Ct);
            for (int id = 1; id <= 3; id++)
            {
                await writer.InsertRowAsync("T", [id, "r" + id], Ct);
            }

            await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 2, new Dictionary<string, object?> { ["Name"] = "updated" }, Ct));
            await tx.RollbackAsync(Ct);
        }

        Assert.Equal(["1|r1", "2|r2", "3|r3"], await ReadRowsAsync(ms, "T"));
    }

    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task InsertDeleteUpdate_AcrossSeveralDataPages_ChangesExactlyTheMatchingRows(DatabaseFormat format, WriteMode mode)
    {
        // ~200-byte rows put a few dozen rows on each data page, so the
        // table spans several pages and the deletes leave holes on each.
        const int rowCount = 150;
        string padding = new('x', 180);

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, mode))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 255)], Ct);
            await writer.InsertRowsAsync("T", Enumerable.Range(1, rowCount).Select(id => new object[] { id, "r" + id + padding }), Ct);

            await RunAsync(writer, mode, async () =>
            {
                for (int id = 1; id <= rowCount; id += 10)
                {
                    Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", id, Ct));
                }

                await writer.InsertRowAsync("T", [rowCount + 1, "late"], Ct);
                for (int id = 2; id <= rowCount + 1; id += 7)
                {
                    if (id % 10 == 1)
                    {
                        continue;
                    }

                    Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", id, new Dictionary<string, object?> { ["Name"] = "u" + id }, Ct));
                }
            });
        }

        var expected = new List<string>();
        for (int id = 1; id <= rowCount + 1; id++)
        {
            if (id <= rowCount && id % 10 == 1)
            {
                continue;
            }

            string name = "r" + id + padding;
            if (id == rowCount + 1)
            {
                name = "late";
            }
            else if (id % 7 == 2)
            {
                name = "u" + id;
            }

            expected.Add(id.ToString(CultureInfo.InvariantCulture) + "|" + name);
        }

        Assert.Equal(expected, await ReadRowsAsync(ms, "T"));
    }

    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task CreateTableInsertAddColumn_InOneTransaction_KeepsEveryRow(DatabaseFormat format, WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync("T", [new("Id", typeof(int))], Ct);
                for (int id = 1; id <= 3; id++)
                {
                    await writer.InsertRowAsync("T", [id], Ct);
                }

                await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), Ct);
            });
        }

        Assert.Equal(["1|", "2|", "3|"], await ReadRowsAsync(ms, "T"));
    }

    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task InsertThenAddColumn_OnCommittedTable_KeepsTheNewRow(DatabaseFormat format, WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, mode))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int))], Ct);
            for (int id = 1; id <= 3; id++)
            {
                await writer.InsertRowAsync("T", [id], Ct);
            }

            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("T", [4], Ct);
                await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), Ct);
                await writer.UpdateRowsAsync("T", "Id", 4, new Dictionary<string, object?> { ["Extra"] = 40 }, Ct);
            });
        }

        Assert.Equal(["1|", "2|", "3|", "4|40"], await ReadRowsAsync(ms, "T"));
    }

    [Theory]
    [MemberData(nameof(RelationshipFormatsAndModes))]
    public async Task CreateRelationship_ThenViolatingInsert_IsRejected(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateRelationshipCapableDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await writer.CreateTableAsync("VisParent", [new("Id", typeof(int))], Ct);
            await writer.CreateTableAsync("VisChild", [new("Id", typeof(int)), new("ParentId", typeof(int))], Ct);
            await writer.InsertRowAsync("VisParent", [7], Ct);

            await RunAsync(writer, mode, async () =>
            {
                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_VisChild_VisParent", "VisParent", "Id", "VisChild", "ParentId"), Ct);

                InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.InsertRowAsync("VisChild", [1, 999], Ct));
                Assert.Contains("FK_VisChild_VisParent", ex.Message, StringComparison.Ordinal);

                await writer.InsertRowAsync("VisChild", [2, 7], Ct);
            });
        }

        Assert.Equal(["2|7"], await ReadRowsAsync(ms, "VisChild"));
    }

    [Theory]
    [MemberData(nameof(RelationshipFormatsAndModes))]
    public async Task CascadeDelete_LeavesChildIndexesPointingAtTheSurvivingRows(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateRelationshipCapableDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await writer.CreateTableAsync("VisParent", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("VisChild", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.InsertRowAsync("VisParent", [7], Ct);
            await writer.InsertRowAsync("VisParent", [8], Ct);
            await writer.InsertRowsAsync("VisChild", [[1, 7], [2, 8], [3, 7], [4, 8]], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_VisChild_VisParent", "VisParent", "Id", "VisChild", "ParentId") { CascadeDeletes = true }, Ct);

            await RunAsync(writer, mode, async () => Assert.Equal(1, await writer.DeleteRowsAsync("VisParent", "Id", 7, Ct)));
        }

        Assert.Equal(["2|8", "4|8"], await ReadRowsAsync(ms, "VisChild"));

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: Ct);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("VisChild", Ct);
        IndexMetadata primaryKey = Assert.Single(indexes, ix => ix.Kind == IndexKind.PrimaryKey);
        IndexMetadata foreignKey = Assert.Single(indexes, ix => ix.Kind != IndexKind.PrimaryKey && ix.Columns.Any(c => c.Name == "ParentId"));

        Assert.Empty(await SeekAsync(reader, "VisChild", primaryKey.Name, 1));
        Assert.Equal(["2|8"], await SeekAsync(reader, "VisChild", primaryKey.Name, 2));
        Assert.Empty(await SeekAsync(reader, "VisChild", primaryKey.Name, 3));
        Assert.Equal(["4|8"], await SeekAsync(reader, "VisChild", primaryKey.Name, 4));
        Assert.Empty(await SeekAsync(reader, "VisChild", foreignKey.Name, 7));
        Assert.Equal(["2|8", "4|8"], await SeekAsync(reader, "VisChild", foreignKey.Name, 8));
    }

    [Theory]
    [MemberData(nameof(RelationshipFormatsAndModes))]
    public async Task BulkInsert_ThenIndexSeek_FindsEveryInsertedRow(DatabaseFormat format, WriteMode mode)
    {
        // Index seeks are Jet4 / ACE only, so this uses the relationship formats.
        const int rowCount = 3000;
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, mode))
        {
            await writer.CreateTableAsync(
                "T",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("LastName", typeof(string), maxLength: 50)],
                [new IndexDefinition("IX_LastName", "LastName")],
                Ct);

            await RunAsync(writer, mode, async () =>
                await writer.InsertRowsAsync("T", Enumerable.Range(1, rowCount).Select(id => new object[] { id, "Name" + id }), Ct));
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: Ct);
        foreach (int id in new[] { 1, 777, 1500, rowCount - 1, rowCount })
        {
            var hits = new List<string>();
            await foreach (object[] row in reader.SeekRowsAsync("T", "IX_LastName", ["Name" + id], Ct))
            {
                hits.Add(Format(row));
            }

            Assert.Equal([id + "|Name" + id], hits);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.Jet4Rc4)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbLegacyPassword)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAesCfbWrapped)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbStandard)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgileCfb)]
    public async Task EncryptedDatabase_TransactionReadsSeeTheirOwnWrites(DatabaseFormat format, AccessEncryptionFormat encryption)
    {
        const string password = "visibility";

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 50)], Ct);
            for (int id = 1; id <= 3; id++)
            {
                await writer.InsertRowAsync("T", [id, "r" + id], Ct);
            }
        }

        ms.Position = 0;
        await AccessWriter.EncryptAsync(ms, password.AsMemory(), encryption, Ct);

        ms.Position = 0;
        var writerOptions = new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, Password = password.AsMemory() };
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, writerOptions, leaveOpen: true, cancellationToken: Ct))
        {
            await RunAsync(writer, WriteMode.ExplicitCommit, async () =>
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 2, new Dictionary<string, object?> { ["Name"] = "updated" }, Ct));
                await writer.InsertRowAsync("T", [4, "r4"], Ct);
                await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), Ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 4, new Dictionary<string, object?> { ["Extra"] = 40 }, Ct));
            });
        }

        Assert.Equal(["2|updated|", "3|r3|", "4|r4|40"], await ReadRowsAsync(ms, "T", new AccessReaderOptions { UseLockFile = false, Password = password.AsMemory() }));
    }

    [Theory]
    [MemberData(nameof(AllFormatsAndModes))]
    public async Task DeleteAndUpdate_PastARowTheDecoderSkips_ChangeOnlyTheMatchingRows(DatabaseFormat format, WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, format, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50)], Ct);
            for (int id = 1; id <= 4; id++)
            {
                await writer.InsertRowAsync("T", [id, "r" + id], Ct);
            }
        }

        // Zero row 1's column count. The row stays live on its page, but no
        // decoder can read it, so every snapshot of the table skips it.
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, WriterOptions(WriteMode.Direct), cancellationToken: Ct))
        {
            CatalogEntry entry = await harness.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct);
            TableDef tableDef = await harness.Database.TableDefs.ReadRequiredTableDefAsync(entry.TDefPage, "T", Ct);
            List<RowLocation> locations = await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, Ct);
            RowLocation first = locations[0];
            Assert.Equal(1, (await PartialColumnReader.TryReadColumnValuesTypedAsync(harness.Database.Format, harness.Database.Pages, first, tableDef, [0], Ct))?[0]);

            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(first.PageNumber, Ct);
            page.AsSpan(first.RowStart, harness.Database.Format.RowFields.NumCols).Clear();
            await harness.Pager.WritePageAsync(first.PageNumber, page, Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 3, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 4, new Dictionary<string, object?> { ["Name"] = "u4" }, Ct));
            });
        }

        Assert.Equal(["2|r2", "4|u4"], await ReadRowsAsync(ms, "T"));
        if (format == DatabaseFormat.Jet3Mdb)
        {
            return;
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: Ct);
        IndexMetadata primaryKey = Assert.Single(await reader.ListIndexesAsync("T", Ct), ix => ix.Kind == IndexKind.PrimaryKey);
        Assert.Equal(["2|r2"], await SeekAsync(reader, "T", primaryKey.Name, 2));
        Assert.Empty(await SeekAsync(reader, "T", primaryKey.Name, 3));
        Assert.Equal(["4|u4"], await SeekAsync(reader, "T", primaryKey.Name, 4));
    }

    private static AccessWriterOptions WriterOptions(WriteMode mode) => WriteModes.WriterOptions(mode);

    private static async Task<AccessWriter> CreateWriterAsync(MemoryStream ms, DatabaseFormat format, WriteMode mode) =>
        await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);

    private static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);
    }

    /// <summary>
    /// Returns a database that has an <c>MSysRelationships</c> table: a
    /// writer-created full-catalog ACCDB, or a copy of an Access-authored Jet4
    /// <c>.mdb</c> (writer-created <c>.mdb</c> files have none).
    /// </summary>
    /// <param name="format">The database format.</param>
    private static async Task<MemoryStream> CreateRelationshipCapableDatabaseAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        if (format == DatabaseFormat.Jet4Mdb)
        {
            byte[] bytes = await File.ReadAllBytesAsync(TestDatabases.TestV2003, Ct);
            await ms.WriteAsync(bytes, Ct);
        }
        else
        {
            await using AccessWriter writer = await CreateWriterAsync(ms, format, WriteMode.Direct);
        }

        ms.Position = 0;
        return ms;
    }

    private static Task RunAsync(AccessWriter writer, WriteMode mode, Func<Task> work) => WriteModes.RunAsync(writer, mode, work, Ct);

    private static async Task<List<string>> ReadRowsAsync(MemoryStream ms, string tableName, AccessReaderOptions? options = null)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, options ?? ReaderOptions, leaveOpen: true, cancellationToken: Ct);
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: Ct))
        {
            rows.Add(row);
        }

        return [.. rows.OrderBy(row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture)).Select(Format)];
    }

    private static async Task<List<string>> SeekAsync(AccessReader reader, string tableName, string indexName, int key)
    {
        var hits = new List<string>();
        await foreach (object[] row in reader.SeekRowsAsync(tableName, indexName, [key], Ct))
        {
            hits.Add(Format(row));
        }

        hits.Sort(StringComparer.Ordinal);
        return hits;
    }

    private static string Format(object[] row) =>
        string.Join("|", row.Select(value => value is DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture)));
}
