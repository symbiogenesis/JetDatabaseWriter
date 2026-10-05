namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>
/// <c>UpdateRowsAsync</c> and <c>DeleteRowsAsync</c> honour their token through
/// every read and check, and ignore it from their first page write. Without a
/// transaction, a statement cancelled after it has started writing runs to
/// completion and returns its count, and leaves the file exactly as an
/// uncancelled run does, through cascades, complex-column items and the
/// AutoNumber high-water value too: no row is lost, the indexes and TDEF row
/// counts of the tables it names or cascades into agree with their rows, and
/// the hidden flat tables' row counts with theirs. A token cancelled before
/// the call, or while the statement is still reading, throws
/// <see cref="OperationCanceledException"/> and leaves the file unchanged.
/// With <see cref="AccessWriterOptions.UseTransactionalWrites"/> the commit
/// checks the token before its first page write, so a cancel during the
/// statement always throws and leaves the file unchanged; in an explicit
/// transaction the statement either throws before it journals a page or
/// completes.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class UpdateDeleteCancellationTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const int TableRows = 150;
    private const int ParentRows = 5;
    private const int ChildRows = 300;
    private const int DocumentRows = 10;
    private const int AssignedAutoNumber = 1000;

    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static readonly DatabaseFormat[] Formats = [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    /// <summary>The statement a test cancels.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum Statement
    {
        /// <summary><c>UpdateRowsAsync</c> of every row of <c>T</c>, which spans several data pages, assigning its indexed <c>Grp</c> column and <c>Name</c>.</summary>
        UpdateEveryRow = 0,

        /// <summary><c>DeleteRowsAsync</c> of the 99 rows of <c>T</c> whose <c>Id</c> is below 100.</summary>
        DeleteManyRows = 1,

        /// <summary><c>UpdateRowsAsync</c> of <c>P.Id</c> 1 to 100, which cascades to the 60 rows of <c>C</c> under it.</summary>
        CascadingUpdate = 2,

        /// <summary><c>DeleteRowsAsync</c> of <c>P</c> rows 1 to 3, which cascades to the 180 rows of <c>C</c> under them.</summary>
        CascadingDelete = 3,

        /// <summary>ACCDB only: <c>DeleteRowsAsync</c> of <c>Docs</c> rows 1 to 6, which removes their attachment and multi-value rows from two hidden flat tables.</summary>
        DeleteRowsWithComplexItems = 4,

        /// <summary><c>UpdateRowsAsync</c> of row 1 of <c>A</c>, setting its AutoNumber primary key to 1000, above the TDEF high-water value of 150, which the update then raises as its last write.</summary>
        UpdateAutoNumberKey = 5,
    }

    /// <summary>Where in the statement's page writes the token is cancelled.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum CancelPoint
    {
        /// <summary>After the statement's first page write.</summary>
        AfterFirstWrite = 0,

        /// <summary>After half of the statement's page writes.</summary>
        AfterHalfTheWrites = 1,

        /// <summary>After every page write of the statement but its last.</summary>
        BeforeLastWrite = 2,
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every statement in every format it runs on.</summary>
    /// <returns>The format and statement pairs.</returns>
    public static TheoryData<DatabaseFormat, Statement> Statements()
    {
        var data = new TheoryData<DatabaseFormat, Statement>();
        foreach ((DatabaseFormat format, Statement statement) in StatementCases())
        {
            data.Add(format, statement);
        }

        return data;
    }

    /// <summary>Gets every statement in every format it runs on, at every cancellation point.</summary>
    /// <returns>The format, statement and cancellation-point triples.</returns>
    public static TheoryData<DatabaseFormat, Statement, CancelPoint> StatementsAndWritePoints()
    {
        var data = new TheoryData<DatabaseFormat, Statement, CancelPoint>();
        foreach ((DatabaseFormat format, Statement statement) in StatementCases())
        {
            foreach (CancelPoint point in Enum.GetValues<CancelPoint>())
            {
                data.Add(format, statement, point);
            }
        }

        return data;
    }

    /// <summary>Gets every statement in every format it runs on, in every write mode.</summary>
    /// <returns>The format, statement and write-mode triples.</returns>
    public static TheoryData<DatabaseFormat, Statement, WriteMode> StatementsAndModes()
    {
        var data = new TheoryData<DatabaseFormat, Statement, WriteMode>();
        foreach ((DatabaseFormat format, Statement statement) in StatementCases())
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, statement, mode);
            }
        }

        return data;
    }

    /// <summary>
    /// Without a transaction, a statement whose token is cancelled after one
    /// of its page writes, from the first to the one before its last, still
    /// completes: it returns the count an uncancelled run returns, and the
    /// file is byte for byte what that run leaves, with every row changed,
    /// the TDEF row counts lowered by exactly the deleted rows, and every
    /// index entry pointing at a live row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="statement">The statement.</param>
    /// <param name="point">After which page write the token is cancelled.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(StatementsAndWritePoints))]
    public async Task Statement_CancelledAfterAPageWrite_RunsToCompletion(DatabaseFormat format, Statement statement, CancelPoint point)
    {
        await using MemoryStream source = await this.CreateDatabaseAsync(format, statement);
        UncancelledRun uncancelled = await RunUncancelledAsync(source, statement, WriteMode.Direct);
        Assert.True(uncancelled.Writes >= 3, $"The statement made only {uncancelled.Writes} page writes.");
        await using WriteFaultStream stream = CopyToFaultStream(source);
        using var cancellation = new CancellationTokenSource();
        await using (AccessWriter writer = await OpenWarmWriterAsync(stream, WriteMode.Direct, statement))
        {
            stream.CancelAfterWrites(WriteToCancelAfter(point, uncancelled.Writes), cancellation);
            int count = await RunStatementAsync(writer, statement, cancellation.Token);

            Assert.True(cancellation.IsCancellationRequested);
            Assert.Equal(uncancelled.Count, count);
        }

        Assert.True(uncancelled.Bytes.AsSpan().SequenceEqual(stream.ToArray()), "The cancelled statement left the file different from an uncancelled run.");
        await AssertCompletedAsync(stream, format, statement);
    }

    /// <summary>
    /// A token cancelled after a page read of the statement, at points spread
    /// over all of its reads, ends the statement in one of two ways: it throws
    /// <see cref="OperationCanceledException"/> and leaves the file unchanged,
    /// which it always does when it is still reading, or it completes and
    /// leaves the file as an uncancelled run does, which it always does after
    /// its last read, since that follows its first write. In an explicit
    /// transaction a statement that throws has journaled nothing, so
    /// committing the transaction anyway changes nothing. With
    /// <see cref="AccessWriterOptions.UseTransactionalWrites"/> it always
    /// throws, because the commit checks the token before it writes a page.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="statement">The statement.</param>
    /// <param name="mode">How the writer runs the statement.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(StatementsAndModes))]
    public async Task Statement_CancelledAfterAPageRead_ThrowsUnchangedOrCompletes(DatabaseFormat format, Statement statement, WriteMode mode)
    {
        await using MemoryStream source = await this.CreateDatabaseAsync(format, statement);
        byte[] original = source.ToArray();
        // This fault injector cancels on stream reads, so every read must reach the stream.
        UncancelledRun uncancelled = await RunUncancelledAsync(source, statement, mode, physicalReads: true);
        Assert.True(uncancelled.Reads >= 5, $"The statement made only {uncancelled.Reads} page reads.");

        foreach (int nthRead in Enumerable.Range(0, 6).Select(fifth => Math.Max(1, uncancelled.Reads * fifth / 5)).Distinct())
        {
            await using WriteFaultStream stream = CopyToFaultStream(source);
            using var cancellation = new CancellationTokenSource();
            int? count;
            await using (AccessWriter writer = await OpenWarmWriterAsync(stream, mode, statement, physicalReads: true))
            {
                stream.CancelAfterReads(nthRead, cancellation);
                try
                {
                    count = await RunInModeAsync(writer, mode, statement, cancellation.Token);
                }
                catch (OperationCanceledException)
                {
                    count = null;
                }
            }

            Assert.True(cancellation.IsCancellationRequested, $"The token was not cancelled after read {nthRead} of {uncancelled.Reads}.");
            byte[] actual = stream.ToArray();
            if (count is null)
            {
                Assert.True(original.AsSpan().SequenceEqual(actual), $"A cancel after read {nthRead} of {uncancelled.Reads} threw but changed the file.");

                // The statement's last read follows its first write, so a
                // cancel there is past the point where it is honoured.
                Assert.True(mode == WriteMode.AutoCommit || nthRead < uncancelled.Reads, "A cancel after the statement's last read stopped it.");
            }
            else
            {
                Assert.True(mode != WriteMode.AutoCommit, $"A cancel after read {nthRead} of {uncancelled.Reads} did not stop the auto-commit.");
                Assert.True(nthRead > 1, "A cancel after the statement's first read did not stop it.");
                Assert.Equal(uncancelled.Count, count);
                Assert.True(uncancelled.Bytes.AsSpan().SequenceEqual(actual), $"A cancel after read {nthRead} of {uncancelled.Reads} completed but left the file different from an uncancelled run.");
            }
        }
    }

    /// <summary>
    /// A token that is already cancelled when the statement is called throws
    /// <see cref="OperationCanceledException"/> before any page is written.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="statement">The statement.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(Statements))]
    public async Task Statement_TokenCancelledBeforeTheCall_ThrowsAndChangesNothing(DatabaseFormat format, Statement statement)
    {
        await using MemoryStream source = await this.CreateDatabaseAsync(format, statement);
        byte[] original = source.ToArray();

        await using WriteFaultStream stream = CopyToFaultStream(source);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await using (AccessWriter writer = await OpenWarmWriterAsync(stream, WriteMode.Direct, statement))
        {
            int writesBefore = stream.WriteCount;
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await RunStatementAsync(writer, statement, cancellation.Token));
            Assert.Equal(writesBefore, stream.WriteCount);
        }

        Assert.True(original.AsSpan().SequenceEqual(stream.ToArray()), "A statement called with a cancelled token changed the file.");
    }

    private static IEnumerable<(DatabaseFormat Format, Statement Statement)> StatementCases()
    {
        foreach (DatabaseFormat format in Formats)
        {
            foreach (Statement statement in Enum.GetValues<Statement>())
            {
                if (statement != Statement.DeleteRowsWithComplexItems || format == DatabaseFormat.AceAccdb)
                {
                    yield return (format, statement);
                }
            }
        }
    }

    private static int WriteToCancelAfter(CancelPoint point, int statementWrites) => point switch
    {
        CancelPoint.AfterFirstWrite => 1,
        CancelPoint.AfterHalfTheWrites => statementWrites / 2,
        CancelPoint.BeforeLastWrite => statementWrites - 1,
        _ => throw new ArgumentOutOfRangeException(nameof(point), point, null),
    };

    private static string Name(int id) => $"row-{id:D4}-" + new string('x', 50);

    private static string Note(int id) => $"note-{id:D4}-xxxxxxxxxx";

    private static int ParentOf(int childId) => (childId % ParentRows) + 1;

    private static WriteFaultStream CopyToFaultStream(MemoryStream source)
    {
        var stream = new WriteFaultStream();
        source.WriteTo(stream);
        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Opens a writer over <paramref name="ms"/> and runs a delete that matches
    /// no row of the statement's table, which loads the enforced relationships
    /// and writes nothing, so the statement's own reads are counted alone.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="mode">How the writer runs the operations.</param>
    /// <param name="statement">The statement the writer will run.</param>
    /// <param name="physicalReads">Whether reads must reach the fault-injection stream.</param>
    /// <returns>The writer.</returns>
    private static async Task<AccessWriter> OpenWarmWriterAsync(MemoryStream ms, WriteMode mode, Statement statement, bool physicalReads = false)
    {
        ms.Position = 0;
        AccessWriter writer = physicalReads
            ? await AccessWriter.OpenAsync(
                ms,
                new AccessWriterOptions { PageCacheSize = 0, UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = mode == WriteMode.AutoCommit },
                leaveOpen: true,
                Ct)
            : await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode);
        try
        {
            Assert.Equal(0, await writer.DeleteRowsAsync(TableOf(statement), RowCriteria.Where("Id", -1), Ct));
            return writer;
        }
        catch
        {
            await writer.DisposeAsync();
            throw;
        }
    }

    private static string TableOf(Statement statement) => statement switch
    {
        Statement.UpdateEveryRow or Statement.DeleteManyRows => "T",
        Statement.CascadingUpdate or Statement.CascadingDelete => "P",
        Statement.DeleteRowsWithComplexItems => "Docs",
        Statement.UpdateAutoNumberKey => "A",
        _ => throw new ArgumentOutOfRangeException(nameof(statement), statement, null),
    };

    private static ValueTask<int> RunStatementAsync(AccessWriter writer, Statement statement, CancellationToken cancellationToken) => statement switch
    {
        Statement.UpdateEveryRow => writer.UpdateRowsAsync("T", RowCriteria.All(), new RowValues { ["Grp"] = 99, ["Name"] = "updated" }, cancellationToken),
        Statement.DeleteManyRows => writer.DeleteRowsAsync("T", RowCriteria.Where(ColumnPredicate.LessThan("Id", 100)), cancellationToken),
        Statement.CascadingUpdate => writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 100 }, cancellationToken),
        Statement.CascadingDelete => writer.DeleteRowsAsync("P", RowCriteria.Where(ColumnPredicate.LessThanOrEqual("Id", 3)), cancellationToken),
        Statement.DeleteRowsWithComplexItems => writer.DeleteRowsAsync("Docs", RowCriteria.Where(ColumnPredicate.LessThanOrEqual("Id", 6)), cancellationToken),
        Statement.UpdateAutoNumberKey => writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = AssignedAutoNumber }, cancellationToken),
        _ => throw new ArgumentOutOfRangeException(nameof(statement), statement, null),
    };

    /// <summary>
    /// Runs the statement directly, or for <see cref="WriteMode.ExplicitCommit"/>
    /// in a transaction that is committed with the test's token afterwards,
    /// also when the statement throws <see cref="OperationCanceledException"/>,
    /// so whatever a cancelled statement journaled reaches the file.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="mode">How the writer runs the statement.</param>
    /// <param name="statement">The statement.</param>
    /// <param name="cancellationToken">The token the statement is given.</param>
    /// <returns>The statement's count.</returns>
    private static async Task<int> RunInModeAsync(AccessWriter writer, WriteMode mode, Statement statement, CancellationToken cancellationToken)
    {
        if (mode != WriteMode.ExplicitCommit)
        {
            return await RunStatementAsync(writer, statement, cancellationToken);
        }

        await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
        int count;
        try
        {
            count = await RunStatementAsync(writer, statement, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            await tx.CommitAsync(Ct);
            throw;
        }

        await tx.CommitAsync(Ct);
        return count;
    }

    private static async Task<UncancelledRun> RunUncancelledAsync(MemoryStream source, Statement statement, WriteMode mode, bool physicalReads = false)
    {
        await using WriteFaultStream twin = CopyToFaultStream(source);
        int count;
        int writes;
        int reads;
        await using (AccessWriter writer = await OpenWarmWriterAsync(twin, mode, statement, physicalReads))
        {
            int writesBefore = twin.WriteCount;
            int readsBefore = twin.ReadCount;
            count = await RunInModeAsync(writer, mode, statement, Ct);
            writes = twin.WriteCount - writesBefore;
            reads = twin.ReadCount - readsBefore;
        }

        return new UncancelledRun(twin.ToArray(), count, writes, reads);
    }

    private static async Task AssertCompletedAsync(MemoryStream ms, DatabaseFormat format, Statement statement)
    {
        switch (statement)
        {
            case Statement.UpdateEveryRow:
                Assert.Equal(
                    Sorted(Enumerable.Range(1, TableRows).Select(id => $"{id}|99|updated")),
                    await ForeignKeyTestDatabase.ReadRowsAsync(ms, "T"));
                if (format != DatabaseFormat.Jet3Mdb)
                {
                    List<int> groups = await ReadIndexColumnAsync(ms, "T", "IX_Grp", column: 1);
                    Assert.Equal(Enumerable.Repeat(99, TableRows), groups);
                }

                int[] updatedCounts = await ReadRowCountsAsync(ms, "T");
                Assert.Equal([TableRows], updatedCounts);
                await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "T");
                break;

            case Statement.DeleteManyRows:
                Assert.Equal(
                    Sorted(Enumerable.Range(100, TableRows - 99).Select(id => $"{id}|{id % 7}|{Name(id)}")),
                    await ForeignKeyTestDatabase.ReadRowsAsync(ms, "T"));
                if (format != DatabaseFormat.Jet3Mdb)
                {
                    List<int> ids = await ReadIndexColumnAsync(ms, "T", "PK", column: 0);
                    Assert.Equal(Enumerable.Range(100, TableRows - 99), ids);
                }

                int[] deletedCounts = await ReadRowCountsAsync(ms, "T");
                Assert.Equal([TableRows - 99], deletedCounts);
                await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "T");
                break;

            case Statement.CascadingUpdate:
                Assert.Equal(
                    Sorted(Enumerable.Range(2, ParentRows - 1).Select(id => $"{id}|parent-{id}").Append("100|parent-1")),
                    await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
                Assert.Equal(
                    Sorted(Enumerable.Range(1, ChildRows).Select(id => $"{id}|{(ParentOf(id) == 1 ? 100 : ParentOf(id))}|{Note(id)}")),
                    await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
                int[] cascadedUpdateCounts = await ReadRowCountsAsync(ms, "P", "C");
                Assert.Equal([ParentRows, ChildRows], cascadedUpdateCounts);
                await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
                break;

            case Statement.CascadingDelete:
                Assert.Equal(
                    Sorted(Enumerable.Range(4, ParentRows - 3).Select(id => $"{id}|parent-{id}")),
                    await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
                int[] keptChildren = [.. Enumerable.Range(1, ChildRows).Where(id => ParentOf(id) > 3)];
                Assert.Equal(
                    Sorted(keptChildren.Select(id => $"{id}|{ParentOf(id)}|{Note(id)}")),
                    await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
                int[] cascadedDeleteCounts = await ReadRowCountsAsync(ms, "P", "C");
                Assert.Equal([ParentRows - 3, keptChildren.Length], cascadedDeleteCounts);
                await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
                break;

            case Statement.DeleteRowsWithComplexItems:
                await AssertDocumentsAfterDeleteAsync(ms);
                break;

            case Statement.UpdateAutoNumberKey:
                Assert.Equal(
                    Sorted(Enumerable.Range(2, TableRows - 1).Select(id => $"{id}|{id % 7}|{Name(id)}").Append($"{AssignedAutoNumber}|1|{Name(1)}")),
                    await ForeignKeyTestDatabase.ReadRowsAsync(ms, "A"));
                if (format != DatabaseFormat.Jet3Mdb)
                {
                    List<int> ids = await ReadIndexColumnAsync(ms, "A", "PK", column: 0);
                    Assert.Equal(Enumerable.Range(2, TableRows - 1).Append(AssignedAutoNumber), ids);
                }

                int[] autoNumberCounts = await ReadRowCountsAsync(ms, "A");
                Assert.Equal([TableRows], autoNumberCounts);
                Assert.Equal((uint)AssignedAutoNumber, await ReadAutoNumberHighWaterAsync(ms, "A"));
                await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "A");
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(statement), statement, null);
        }
    }

    private static async Task AssertDocumentsAfterDeleteAsync(MemoryStream ms)
    {
        int[] kept = [.. Enumerable.Range(7, DocumentRows - 6)];
        ms.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct))
        {
            var ids = new List<int>();
            await foreach (object[] row in reader.Rows("Docs", cancellationToken: Ct))
            {
                ids.Add((int)row[0]);
            }

            Assert.Equal(kept, ids.Order());
            IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Docs", "Files", Ct);
            Assert.Equal(kept.Select(id => $"doc-{id}.txt").Order(StringComparer.Ordinal), attachments.Select(attachment => attachment.FileName).Order(StringComparer.Ordinal));
            IReadOnlyList<MultiValueItem> tags = await reader.GetMultiValueItemsAsync("Docs", "Tags", Ct);
            Assert.Equal(
                kept.SelectMany(id => new[] { (id * 10) + 1, (id * 10) + 2 }),
                tags.Select(tag => Convert.ToInt32(tag.Value, CultureInfo.InvariantCulture)).Order());
        }

        int[] counts = await ReadRowCountsAsync(ms, "Docs");
        Assert.Equal([kept.Length], counts);
        Assert.Equal((kept.Length, kept.Length), await FlatTableRowCounts.ReadAsync(ms, "Docs", "Files", Ct));
        Assert.Equal((kept.Length * 2, kept.Length * 2), await FlatTableRowCounts.ReadAsync(ms, "Docs", "Tags", Ct));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Docs");
    }

    private static List<string> Sorted(IEnumerable<string> rows)
    {
        List<string> sorted = [.. rows];
        sorted.Sort(StringComparer.Ordinal);
        return sorted;
    }

    /// <summary>
    /// Reads one Long Integer column of every row through
    /// <see cref="AccessReader.FromIndex(string, string)"/>, in index order.
    /// Jet3 has no index reads, so the callers check its indexes only by
    /// <see cref="ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync"/>.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableName">The table.</param>
    /// <param name="indexName">The index.</param>
    /// <param name="column">The ordinal of the column to read.</param>
    /// <returns>The column's values, in index order.</returns>
    private static async Task<List<int>> ReadIndexColumnAsync(MemoryStream ms, string tableName, string indexName, int column)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        var values = new List<int>();
        await foreach (object[] row in reader.FromIndex(tableName, indexName).ToRowsAsync(Ct))
        {
            values.Add((int)row[column]);
        }

        ms.Position = 0;
        return values;
    }

    private static async Task<int[]> ReadRowCountsAsync(MemoryStream ms, params string[] tableNames)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
        ms.Position = 0;
        return [.. tableNames.Select(name => checked((int)stats.Single(stat => stat.Name == name).RowCount))];
    }

    /// <summary>
    /// Reads the AutoNumber high-water value from the TDEF header of
    /// <paramref name="tableName"/>, which Access continues numbering from.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableName">The table.</param>
    /// <returns>The TDEF's AutoNumber counter.</returns>
    private static async Task<uint> ReadAutoNumberHighWaterAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, ReaderOptions, cancellationToken: Ct);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        byte[] tdef = await harness.ReadPageCopyAsync(entry.TDefPage, Ct);
        ms.Position = 0;
        return BinaryPrimitives.ReadUInt32LittleEndian(tdef.AsSpan(harness.Database.Format.TDef.AutoNumber));
    }

    /// <summary>
    /// Builds the database the statement runs on, with no transaction:
    /// <c>T</c> (<c>Id</c> primary key <c>PK</c>, <c>Grp</c> with index
    /// <c>IX_Grp</c>, <c>Name</c>) with 150 rows; <c>P</c> with 5 rows and
    /// <c>C</c> with 300 rows, 60 under each parent, related by <c>FK_C_P</c>,
    /// which cascades updates and deletes; on ACCDB, <c>Docs</c>
    /// (<c>Id</c> primary key, <c>Files</c> attachment, <c>Tags</c> multi-value)
    /// with 10 rows, each with one attachment and two tags; or <c>A</c>, laid
    /// out as <c>T</c> but with an AutoNumber <c>Id</c>, numbered 1 to 150.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="statement">The statement.</param>
    /// <returns>The database, positioned at 0.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="statement"/> is not a <see cref="Statement"/> value.</exception>
    private async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, Statement statement)
    {
        switch (statement)
        {
            case Statement.UpdateEveryRow or Statement.DeleteManyRows:
            {
                MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
                await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
                {
                    await writer.CreateTableAsync(
                        "T",
                        [
                            new ColumnDefinition("Id", typeof(int)),
                            new ColumnDefinition("Grp", typeof(int)),
                            new ColumnDefinition("Name", typeof(string), maxLength: 100),
                        ],
                        [new IndexDefinition("PK", "Id") { IsPrimaryKey = true }, new IndexDefinition("IX_Grp", "Grp")],
                        Ct);
                    Assert.Equal(TableRows, await writer.InsertRowsAsync("T", Enumerable.Range(1, TableRows).Select(id => new object?[] { id, id % 7, Name(id) }), Ct));
                }

                ms.Position = 0;
                return ms;
            }

            case Statement.CascadingUpdate or Statement.CascadingDelete:
            {
                MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(
                    db,
                    format,
                    [.. Enumerable.Range(1, ParentRows).Select(id => new object?[] { id, $"parent-{id}" })],
                    [.. Enumerable.Range(1, ChildRows).Select(id => new object?[] { id, ParentOf(id), Note(id) })]);
                await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
                {
                    await writer.CreateRelationshipAsync(
                        new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true, CascadeDeletes = true },
                        Ct);
                }

                ms.Position = 0;
                return ms;
            }

            case Statement.DeleteRowsWithComplexItems:
            {
                MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
                await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
                {
                    await writer.CreateTableAsync(
                        "Docs",
                        [
                            new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                            new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                            new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
                        ],
                        Ct);
                    Assert.Equal(DocumentRows, await writer.InsertRowsAsync("Docs", Enumerable.Range(1, DocumentRows).Select(id => new object?[] { id, DBNull.Value, DBNull.Value }), Ct));
                    for (int id = 1; id <= DocumentRows; id++)
                    {
                        var key = new Dictionary<string, object?> { ["Id"] = id };
                        await writer.AddAttachmentAsync("Docs", "Files", key, new AttachmentInput($"doc-{id}.txt", Encoding.UTF8.GetBytes($"document {id}")), Ct);
                        await writer.AddMultiValueItemAsync("Docs", "Tags", key, (id * 10) + 1, Ct);
                        await writer.AddMultiValueItemAsync("Docs", "Tags", key, (id * 10) + 2, Ct);
                    }
                }

                ms.Position = 0;
                return ms;
            }

            case Statement.UpdateAutoNumberKey:
            {
                MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
                await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
                {
                    await writer.CreateTableAsync(
                        "A",
                        [
                            new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true },
                            new ColumnDefinition("Grp", typeof(int)),
                            new ColumnDefinition("Name", typeof(string), maxLength: 100),
                        ],
                        [new IndexDefinition("PK", "Id") { IsPrimaryKey = true }, new IndexDefinition("IX_Grp", "Grp")],
                        Ct);
                    Assert.Equal(TableRows, await writer.InsertRowsAsync("A", Enumerable.Range(1, TableRows).Select(id => new object?[] { DBNull.Value, id % 7, Name(id) }), Ct));
                }

                Assert.Equal((uint)TableRows, await ReadAutoNumberHighWaterAsync(ms, "A"));
                ms.Position = 0;
                return ms;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(statement), statement, null);
        }
    }

    /// <summary>What an uncancelled run of a statement leaves and costs.</summary>
    /// <param name="Bytes">The file after the run.</param>
    /// <param name="Count">The statement's count.</param>
    /// <param name="Writes">The page writes the statement made.</param>
    /// <param name="Reads">The page reads the statement made.</param>
    private sealed record UncancelledRun(byte[] Bytes, int Count, int Writes, int Reads);
}
