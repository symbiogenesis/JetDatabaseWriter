namespace JetDatabaseWriter.Tests.Relationships;

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
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using WriteMode = JetDatabaseWriter.Tests.Writer.TransactionReadVisibilityTests.WriteMode;

/// <summary>
/// Builds the parent table <c>P</c> (<c>Id</c> primary key, <c>Name</c>) and the
/// child table <c>C</c> (<c>Id</c> primary key, <c>ParentId</c>, <c>Note</c>)
/// that the foreign-key enforcement tests run on, and drives a writer in each
/// write mode. Jet3 and Jet4 start from copies of the Access-authored
/// testV1997.mdb and testV2003.mdb, because writer-created <c>.mdb</c> files
/// have no <c>MSysRelationships</c> table; ACCDB starts from a writer-created
/// full-catalog file.
/// </summary>
internal static class ForeignKeyTestDatabase
{
    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    /// <summary>
    /// Returns a database with an <c>MSysRelationships</c> table and none of
    /// the tables the tests create: a copy of testV1997.mdb or testV2003.mdb,
    /// or a new full-catalog ACCDB.
    /// </summary>
    /// <param name="db">Caches the fixture files.</param>
    /// <param name="format">The database format.</param>
    /// <returns>The database, positioned at 0.</returns>
    public static async Task<MemoryStream> CreateEmptyAsync(DatabaseCache db, DatabaseFormat format)
    {
        MemoryStream ms;
        if (format == DatabaseFormat.AceAccdb)
        {
            ms = new MemoryStream();
            await using AccessWriter created = await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(WriteMode.Direct), leaveOpen: true, Ct);
        }
        else
        {
            ms = await db.CopyToStreamAsync(format == DatabaseFormat.Jet3Mdb ? TestDatabases.TestV1997 : TestDatabases.TestV2003, Ct);
        }

        ms.Position = 0;
        return ms;
    }

    /// <summary>
    /// Returns a database holding <c>P</c> and <c>C</c>, with the rows given,
    /// written with no transaction and no relationship.
    /// </summary>
    /// <param name="db">Caches the fixture files.</param>
    /// <param name="format">The database format.</param>
    /// <param name="parentRows">The rows of <c>P</c> (<c>Id</c>, <c>Name</c>).</param>
    /// <param name="childRows">The rows of <c>C</c> (<c>Id</c>, <c>ParentId</c>, <c>Note</c>).</param>
    /// <returns>The database, positioned at 0.</returns>
    public static async Task<MemoryStream> CreateAsync(DatabaseCache db, DatabaseFormat format, object?[][] parentRows, object?[][] childRows)
    {
        MemoryStream ms = await CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            Assert.Equal(format, writer.DatabaseFormat);
            await writer.CreateTableAsync(
                "P",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Name", typeof(string), maxLength: 20)],
                Ct);
            await writer.CreateTableAsync(
                "C",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Note", typeof(string), maxLength: 20),
                ],
                Ct);
            Assert.Equal(parentRows.Length, await writer.InsertRowsAsync("P", parentRows, Ct));
            Assert.Equal(childRows.Length, await writer.InsertRowsAsync("C", childRows, Ct));
        }

        ms.Position = 0;
        return ms;
    }

    /// <summary>
    /// Returns <c>P</c> with row (1, 'one') and <c>C</c> with row (1, 1, 'a'),
    /// related by <c>FK_C_P</c> from <c>C.ParentId</c> to <c>P.Id</c>, which
    /// cascades updates and deletes. When <paramref name="secondChild"/> is
    /// given, the database also holds <c>D</c> (<c>Id</c> primary key,
    /// <c>ParentId</c>) with row (1, 1), and that relationship, created after
    /// <c>FK_C_P</c>.
    /// </summary>
    /// <param name="db">Caches the fixture files.</param>
    /// <param name="format">The database format.</param>
    /// <param name="secondChild">The relationship from <c>D</c> to <c>P</c>, or <see langword="null"/> for no table <c>D</c>.</param>
    /// <returns>The database, positioned at 0.</returns>
    public static async Task<MemoryStream> CreateCascadingAsync(DatabaseCache db, DatabaseFormat format, RelationshipDefinition? secondChild)
    {
        MemoryStream ms = await CreateAsync(db, format, [[1, "one"]], [[1, 1, "a"]]);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true, CascadeDeletes = true },
                Ct);
            if (secondChild != null)
            {
                await writer.CreateTableAsync(
                    "D",
                    [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))],
                    Ct);
                await writer.InsertRowAsync("D", [1, 1], Ct);
                await writer.CreateRelationshipAsync(secondChild, Ct);
            }
        }

        ms.Position = 0;
        return ms;
    }

    /// <summary>Gets the writer options for <paramref name="mode"/>.</summary>
    /// <param name="mode">How the writer runs the operations.</param>
    /// <returns>The options.</returns>
    public static AccessWriterOptions WriterOptions(WriteMode mode) => new()
    {
        UseLockFile = false,
        UseByteRangeLocks = false,
        UseTransactionalWrites = mode == WriteMode.AutoCommit,
    };

    /// <summary>Opens a writer over <paramref name="ms"/> for <paramref name="mode"/>.</summary>
    /// <param name="ms">The database.</param>
    /// <param name="mode">How the writer runs the operations.</param>
    /// <returns>The writer, which leaves <paramref name="ms"/> open.</returns>
    public static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriterOptions(mode), leaveOpen: true, Ct);
    }

    /// <summary>
    /// Runs <paramref name="work"/> directly, or inside one explicit transaction
    /// that is committed afterwards when <paramref name="mode"/> is
    /// <see cref="WriteMode.ExplicitCommit"/>.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="mode">How the writer runs the operations.</param>
    /// <param name="work">The operations.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    public static async Task RunAsync(AccessWriter writer, WriteMode mode, Func<Task> work)
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

    /// <summary>
    /// Writes an enforced one-column relationship straight into
    /// <c>MSysRelationships</c>, without the checks
    /// <c>CreateRelationshipAsync</c> makes, so it can name a table or column
    /// that does not exist. No foreign-key index entries are written.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="name">The relationship name.</param>
    /// <param name="foreignTable">The foreign (child) table.</param>
    /// <param name="foreignColumn">The foreign-key column.</param>
    /// <param name="primaryTable">The primary (parent) table.</param>
    /// <param name="primaryColumn">The referenced column.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    public static async Task PlantRelationshipAsync(MemoryStream ms, string name, string foreignTable, string foreignColumn, string primaryTable, string primaryColumn)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        long relationshipsPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, Ct);
        Assert.True(relationshipsPage > 0, "The database has no MSysRelationships table.");
        TableDef relationshipsDef = await harness.Database.ReadRequiredTableDefAsync(relationshipsPage, Constants.SystemTableNames.Relationships, Ct);

        object[] row = relationshipsDef.CreateNullValueRow();
        relationshipsDef.SetValueByName(row, "ccolumn", 1);
        relationshipsDef.SetValueByName(row, "grbit", 0);
        relationshipsDef.SetValueByName(row, "icolumn", 0);
        relationshipsDef.SetValueByName(row, "szColumn", foreignColumn);
        relationshipsDef.SetValueByName(row, "szObject", foreignTable);
        relationshipsDef.SetValueByName(row, "szReferencedColumn", primaryColumn);
        relationshipsDef.SetValueByName(row, "szReferencedObject", primaryTable);
        relationshipsDef.SetValueByName(row, "szRelationship", name);
        await harness.Services.Indexes.InsertSystemRowAndMaintainAsync(
            relationshipsPage,
            relationshipsDef,
            Constants.SystemTableNames.Relationships,
            row,
            cancellationToken: Ct);
        ms.Position = 0;
    }

    /// <summary>
    /// Asserts that every real index of each of <paramref name="tableNames"/>
    /// holds exactly one entry for each live row of its table, pointing at
    /// that row.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableNames">The tables.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous check.</returns>
    public static async Task AssertIndexesCoverRowsAsync(MemoryStream ms, params string[] tableNames)
    {
        ms.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(ms, ReaderOptions, cancellationToken: Ct))
        {
            foreach (string tableName in tableNames)
            {
                CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, Ct);
                Assert.NotNull(entry);
                foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, entry.TDefPage, Ct))
                {
                    await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, entry.TDefPage, root, Ct);
                }
            }
        }

        ms.Position = 0;
    }

    /// <summary>
    /// Reads every row of <paramref name="tableName"/> as its values joined by
    /// <c>|</c>, with database null as an empty string, sorted ordinally.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableName">The table.</param>
    /// <returns>The rows.</returns>
    public static async Task<List<string>> ReadRowsAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: Ct))
        {
            rows.Add(string.Join("|", row.Select(value => value is DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
