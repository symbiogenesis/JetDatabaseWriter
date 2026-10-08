namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
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
/// The writer loads the enforced relationships once per session and reuses
/// them for every insert, update and delete. These tests check that the set
/// is loaded again after each change to <c>MSysRelationships</c> (create,
/// drop and rename a relationship, rename either key column), after a
/// rollback and after a commit that fails before replay, and that a warm
/// writer no longer reads the catalog pages on each operation. Jet3 runs on a
/// copy of Jet3Test.mdb and Jet4 on a copy of AdventureLT2008.mdb, because
/// writer-created <c>.mdb</c> files have no <c>MSysRelationships</c>; ACCDB
/// runs on a writer-created full-catalog file.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class RelationshipCatalogCacheTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Relationship = "PC";

    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() =>
        Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    /// <summary>Gets Jet4 and ACCDB in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> Jet4AndAccdbModes() =>
        Combine(DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    /// <summary>Gets every format.</summary>
    /// <returns>The formats.</returns>
    public static TheoryData<DatabaseFormat> Formats() =>
        [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Insert_AfterCreateRelationshipInSameSession_IsEnforced(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: false);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                // Not yet enforced; this also loads the relationship set.
                await writer.InsertRowAsync("C", [1, 99], Ct);
                Assert.Equal(1, await writer.DeleteRowsAsync("C", "Id", 1, Ct));

                await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, "P", "Id", "C", "PId"), Ct);

                await AssertOrphanInsertRejectedAsync(writer, "PId");
                await writer.InsertRowAsync("C", [3, 1], Ct);
            });
        }

        Assert.Equal(["3|1"], await ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Insert_AfterRenamingForeignKeyColumn_EnforcesRenamedColumn(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: true);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("C", [1, 1], Ct);
                await writer.RenameColumnAsync("C", "PId", "ParentId", Ct);

                await writer.InsertRowAsync("C", [2, 2], Ct);
                await AssertOrphanInsertRejectedAsync(writer, "ParentId");
            });
        }

        Assert.Equal(["1|1", "2|2"], await ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task UpdatePrimaryKey_AfterRenamingPrimaryKeyColumn_StillRejected(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: true);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("C", [1, 1], Ct);
                await writer.RenameColumnAsync("P", "Id", "PKey", Ct);

                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("P", "PKey", 1, new Dictionary<string, object?> { ["PKey"] = 5 }, Ct));
                Assert.Contains($"foreign-key constraint '{Relationship}'", ex.Message, StringComparison.Ordinal);
                Assert.Contains("cascade-update is not enabled", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(["1|one", "2|two"], await ReadRowsAsync(ms, "P"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Delete_AfterRenameRelationship_ReportsNewName(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: true);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("C", [1, 1], Ct);
                await writer.RenameRelationshipAsync(Relationship, "PC2", Ct);

                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.DeleteRowsAsync("P", "Id", 1, Ct));
                Assert.Contains("foreign-key constraint 'PC2'", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(["1|one", "2|two"], await ReadRowsAsync(ms, "P"));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Insert_AfterDropRelationship_IsNotEnforced(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: true);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await AssertOrphanInsertRejectedAsync(writer, "PId");
                await writer.DropRelationshipAsync(Relationship, Ct);

                await writer.InsertRowAsync("C", [1, 99], Ct);
            });
        }

        Assert.Equal(["1|99"], await ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task CreateRelationship_RolledBack_IsNotEnforced(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: false);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.InsertRowAsync("C", [1, 1], Ct);

            await using (JetTransaction tx = await writer.BeginTransactionAsync(Ct))
            {
                await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, "P", "Id", "C", "PId"), Ct);
                await AssertOrphanInsertRejectedAsync(writer, "PId");
                await tx.RollbackAsync(Ct);
            }

            await writer.InsertRowAsync("C", [2, 99], Ct);
        }

        Assert.Equal(["1|1", "2|99"], await ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task DropRelationship_RolledBack_IsEnforcedAgain(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: true);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.InsertRowAsync("C", [1, 1], Ct);

            await using (JetTransaction tx = await writer.BeginTransactionAsync(Ct))
            {
                await writer.DropRelationshipAsync(Relationship, Ct);
                await writer.InsertRowAsync("C", [2, 99], Ct);
                await tx.RollbackAsync(Ct);
            }

            await AssertOrphanInsertRejectedAsync(writer, "PId");
        }

        Assert.Equal(["1|1"], await ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task CreateRelationship_WhenCommitFailsBeforeReplay_IsNotEnforced(DatabaseFormat format)
    {
        if (!OperatingSystem.IsWindows())
        {
            // Same-process byte-range lock contention needs Windows; POSIX
            // record locks are process-scoped.
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"RelationshipCatalogCacheTests_{Guid.NewGuid():N}{(format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb")}");
        var options = new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = true, LockTimeoutMilliseconds = 100 };

        // The commit-lock sentinel byte: 0xFFFFFFFC on ACE, 0xFFFFFFFE on Jet3 and Jet4.
        long commitLockOffset = format == DatabaseFormat.AceAccdb ? 0xFFFFFFFCL : 0xFFFFFFFEL;
        try
        {
            await using (MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: false))
            {
                await File.WriteAllBytesAsync(path, ms.ToArray(), Ct);
            }

            // Explicit sharing permits the competing test handle; path writers are exclusive.
            await using (var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, Ct))
            {
                await writer.InsertRowAsync("C", [1, 1], Ct);

                JetTransaction tx = await writer.BeginTransactionAsync(Ct);
                await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, "P", "Id", "C", "PId"), Ct);
                await writer.InsertRowAsync("C", [2, 2], Ct);

                await using (var holder = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    holder.Lock(commitLockOffset, 1);
                    JetLockException error = await Assert.ThrowsAsync<JetLockException>(async () => await tx.CommitAsync(Ct));
                    Assert.Equal(JetErrorCode.LockTimeout, error.ErrorCode);
                    holder.Unlock(commitLockOffset, 1);
                }

                Assert.True(tx.IsRolledBack);
                await tx.DisposeAsync();

                await writer.InsertRowAsync("C", [3, 99], Ct);
            }

            await using AccessReader reader = await AccessReader.OpenAsync(path, ReaderOptions, Ct);
            Assert.Equal(["1|1", "3|99"], await ReadRowsAsync(reader, "C"));
        }
        finally
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    /// <summary>
    /// A warm writer checks relationships on every insert, update and delete
    /// without reading <c>MSysObjects</c> or <c>MSysRelationships</c>. Jet4 and
    /// ACCDB only: on Jet3 the writer keeps no usage map for the tables it
    /// creates, so every read of P or C walks every page of the file anyway;
    /// the nwind case of <see cref="DeleteRows_NoMatch_AfterWarmUp_ReadsFewBytes"/>
    /// covers Jet3.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the operations.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(Jet4AndAccdbModes))]
    public async Task Dml_AfterWarmUp_ReadsNoMSysObjectsOrMSysRelationshipsPages(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateDatabaseAsync(format, withRelationship: true);
        HashSet<long> catalogPages = await ReadCatalogDataPagesAsync(ms);

        ms.Position = 0;
        await using var counting = new CountingStream(ms);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(counting, WriterOptions(mode), leaveOpen: true, Ct))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("C", [1, 1], Ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("C", "Id", 1, new Dictionary<string, object?> { ["PId"] = 2 }, Ct));
                Assert.Equal(0, await writer.DeleteRowsAsync("C", "Id", 99, Ct));
                Assert.Equal(0, await writer.DeleteRowsAsync("P", "Id", 99, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync("P", "Id", 1, new Dictionary<string, object?> { ["N"] = "uno" }, Ct));
            });

            // An update re-inserts its rows. When the insert-page hint points at
            // another table, the re-insert looks for room through the table's
            // usage map, which fails validation while the old rows are deleted,
            // and falls back to reading every page of the file. So each update
            // here runs while the hint points at its own table.
            counting.Reset();
            await RunAsync(writer, mode, async () =>
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("P", "Id", 2, new Dictionary<string, object?> { ["N"] = "dos" }, Ct));
                await writer.InsertRowAsync("C", [2, 1], Ct);
                Assert.Equal(1, await writer.UpdateRowsAsync("C", "Id", 2, new Dictionary<string, object?> { ["PId"] = 2 }, Ct));
                Assert.Equal(1, await writer.DeleteRowsAsync("C", "Id", 1, Ct));
                Assert.Equal(0, await writer.DeleteRowsAsync("C", "Id", 99, Ct));
                Assert.Equal(0, await writer.DeleteRowsAsync("P", "Id", 99, Ct));
            });

            long[] catalogPagesRead = [.. counting.PagesRead(Constants.PageSizes.Jet4).Intersect(catalogPages).Order()];
            Assert.True(catalogPagesRead.Length == 0, $"The warm writer read catalog pages {string.Join(", ", catalogPagesRead)}.");
        }

        Assert.Equal(["2|2"], await ReadRowsAsync(ms, "C"));
    }

    [Theory]
    [InlineData("nwind", "Shippers", "ShipperID")]
    [InlineData("Northwind", "States", "StateAbbrev")]
    public async Task DeleteRows_NoMatch_AfterWarmUp_ReadsFewBytes(string fixture, string table, string column)
    {
        const long maxBytes = 64 * 1024;
        string path = fixture == "nwind" ? TestDatabases.MdbtoolsNwind : TestDatabases.NorthwindTraders;
        await using MemoryStream ms = await db.CopyToStreamAsync(path, Ct);
        object missing = column == "ShipperID" ? -1 : "??";

        await using var counting = new CountingStream(ms);
        await using AccessWriter writer = await AccessWriter.OpenAsync(counting, WriterOptions(WriteMode.Direct), leaveOpen: true, Ct);
        Assert.Equal(0, await writer.DeleteRowsAsync(table, column, missing, Ct));

        counting.Reset();
        Assert.Equal(0, await writer.DeleteRowsAsync(table, column, missing, Ct));

        Assert.True(
            counting.BytesRead < maxBytes,
            $"A second no-match delete on {table} read {counting.BytesRead} bytes of a {ms.Length}-byte file.");
    }

    private static TheoryData<DatabaseFormat, WriteMode> Combine(params DatabaseFormat[] formats)
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in formats)
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    private static AccessWriterOptions WriterOptions(WriteMode mode) => new()
    {
        UseLockFile = false,
        UseByteRangeLocks = false,
        UseTransactionalWrites = mode == WriteMode.AutoCommit,
    };

    private static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriterOptions(mode), leaveOpen: true, Ct);
    }

    private static async Task RunAsync(AccessWriter writer, WriteMode mode, Func<Task> work)
    {
        if (mode != WriteMode.ExplicitCommit)
        {
            await work();
            return;
        }

        await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
        await work();
        await tx.CommitAsync(Ct);
    }

    /// <summary>Checks that inserting a child whose parent does not exist is rejected by the relationship.</summary>
    /// <param name="writer">The writer.</param>
    /// <param name="foreignColumn">The child key column the error names.</param>
    private static async Task AssertOrphanInsertRejectedAsync(AccessWriter writer, string foreignColumn)
    {
        JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
            await writer.InsertRowAsync("C", [42, 99], Ct));
        Assert.Contains($"violates foreign-key constraint '{Relationship}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(foreignColumn, ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Returns the data pages of <c>MSysObjects</c> and <c>MSysRelationships</c>
    /// in the database held by <paramref name="ms"/>.
    /// </summary>
    /// <param name="ms">The database.</param>
    private static async Task<HashSet<long>> ReadCatalogDataPagesAsync(MemoryStream ms)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, cancellationToken: Ct);
        long relationshipsPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, Ct);
        Assert.True(relationshipsPage > 0, "The database has no MSysRelationships table.");

        var pages = new HashSet<long>(await harness.Database.OwnedPages.GetOwnedDataPagesAsync(2, Ct));
        pages.UnionWith(await harness.Database.OwnedPages.GetOwnedDataPagesAsync(relationshipsPage, Ct));
        Assert.NotEmpty(pages);
        return pages;
    }

    private static async Task<List<string>> ReadRowsAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        return await ReadRowsAsync(reader, tableName);
    }

    private static async Task<List<string>> ReadRowsAsync(AccessReader reader, string tableName)
    {
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: Ct))
        {
            rows.Add(string.Join("|", row.Select(value => value is DBNull ? string.Empty : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /// <summary>
    /// Returns a database with an <c>MSysRelationships</c> table and the tables
    /// <c>P</c> (<c>Id</c> primary key, <c>N</c>; rows 1 and 2) and <c>C</c>
    /// (<c>Id</c> primary key, <c>PId</c>; empty), and when
    /// <paramref name="withRelationship"/> is set, the enforced relationship
    /// <see cref="Relationship"/> from <c>C.PId</c> to <c>P.Id</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="withRelationship">Whether to create the relationship.</param>
    private async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, bool withRelationship)
    {
        MemoryStream ms;
        if (format == DatabaseFormat.AceAccdb)
        {
            ms = new MemoryStream();
            await using AccessWriter created = await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(WriteMode.Direct), leaveOpen: true, Ct);
        }
        else
        {
            ms = await db.CopyToStreamAsync(format == DatabaseFormat.Jet3Mdb ? TestDatabases.Jet3Test : TestDatabases.AdventureWorks, Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "P",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("N", typeof(string), maxLength: 20)],
                Ct);
            await writer.CreateTableAsync(
                "C",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PId", typeof(int))],
                Ct);
            await writer.InsertRowsAsync("P", [[1, "one"], [2, "two"]], Ct);
            if (withRelationship)
            {
                await writer.CreateRelationshipAsync(new RelationshipDefinition(Relationship, "P", "Id", "C", "PId"), Ct);
            }
        }

        ms.Position = 0;
        return ms;
    }
}
