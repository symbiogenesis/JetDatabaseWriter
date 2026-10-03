namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
