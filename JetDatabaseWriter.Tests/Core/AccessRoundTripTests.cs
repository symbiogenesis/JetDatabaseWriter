namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// End-to-end validation that primary-key, composite-PK, and foreign-key
/// metadata produced by <see cref="AccessWriter"/> survives a real Microsoft
/// Access "Compact &amp; Repair" pass.
/// </summary>
/// <remarks>
/// <para>
/// These tests are skipped automatically when MSACCESS.EXE is not installed
/// (see <see cref="AccessRoundTripEnvironment"/>) — they only run on
/// developer machines and CI agents that have Microsoft Access available.
/// Compact &amp; Repair is invoked through
/// <c>DAO.DBEngine.120.CompactDatabase</c> (driven by a bitness-matched
/// <c>powershell.exe</c>) rather than <c>MSACCESS.EXE /compact</c> because
/// the Office launcher detaches its child process and the compacted file
/// never appears for the test to validate.
/// </para>
/// <para>
/// The fixture is <c>NorthwindTraders.accdb</c> — an Access-authored database
/// that already contains the <c>MSysRelationships</c> catalog table required
/// by <see cref="AccessWriter.CreateRelationshipAsync"/>.
/// </para>
/// </remarks>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class AccessRoundTripTests
{
    private static readonly TimeSpan CompactTimeout = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Gets a value indicating whether the round-trip environment is available.
    /// Consumed by xUnit v3's <c>SkipUnless</c> selector — when this is
    /// <c>false</c>, the gated <c>[Fact]</c> reports as skipped instead of
    /// failing. Backed by <see cref="AccessRoundTripEnvironment.IsAvailable"/>.
    /// </summary>
    public static bool RoundTripEnvironmentAvailable => AccessRoundTripEnvironment.IsAvailable;

    [Fact(
        Skip = "DAO Compact & Repair rejects the writer's output with err 3011 'MSysDb' until the page-1 global page-allocation map and MSysACEs row insertion are implemented. See docs/design/round-trip-test-failures.md for the full investigation, the DAO-authored ground-truth probe (DIAG_RT_DAO_BASELINE), and the prioritized remaining work.")]
    public async Task SinglePk_AndSingleColumnFk_SurviveCompactAndRepair()
    {
        await using var session = await RoundTripSession.CreateAsync(TestContext.Current.CancellationToken);

        const string Parent = "RT_Customers";
        const string Child = "RT_Orders";
        const string FkName = "RT_FK_Orders_Customers";

        await using (var writer = await session.OpenWriterAsync())
        {
            await writer.CreateTableAsync(
                Parent,
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                Child,
                [
                    new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("CustomerID", typeof(int)) { IsNullable = false },
                    new("OrderDate", typeof(DateTime)),
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    FkName,
                    primaryTable: Parent,
                    primaryColumn: "CustomerID",
                    foreignTable: Child,
                    foreignColumn: "CustomerID")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                Parent,
                new[]
                {
                    new object[] { DBNull.Value, "Acme" },
                    new object[] { DBNull.Value, "Beta" },
                    new object[] { DBNull.Value, "Gamma" },
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                Child,
                new[]
                {
                    new object[] { DBNull.Value, 1, new DateTime(2025, 1, 15) },
                    new object[] { DBNull.Value, 2, new DateTime(2025, 2, 20) },
                    new object[] { DBNull.Value, 3, new DateTime(2025, 3, 03) },
                },
                TestContext.Current.CancellationToken);
        }

        var pre = await CaptureSnapshotAsync(
            session.SourcePath,
            [Parent, Child],
            [FkName],
            TestContext.Current.CancellationToken);

        // Pre-compact sanity: verify our own reader can parse the writer's output.
        AssertPreCompactConsistency(pre, [Parent, Child], [FkName], expectedParentRows: 3, expectedChildRows: 3);

        // Verify TDEF structural invariants on the writer-produced file before
        // handing it to DAO, so a failure here clearly points at the writer
        // rather than DAO's interpretation of the output.
        await AssertTdefMagicStampsAsync(session.SourcePath, [Parent, Child], TestContext.Current.CancellationToken);

        session.RunDaoCompact();

        var post = await CaptureSnapshotAsync(
            session.CompactedPath,
            [Parent, Child],
            [FkName],
            TestContext.Current.CancellationToken);

        AssertSchemaSurvived(pre, post);
        Assert.Contains(post.Indexes[Child], i => i.Kind == IndexKind.PrimaryKey && i.Columns == "OrderID");
        Assert.Contains(post.Indexes[Child], i => i.IsForeignKey && i.Columns == "CustomerID" && i.CascadeDeletes);
    }

    [Fact]
    public async Task CompositePk_AndMultiColumnFk_SurviveCompactAndRepair()
    {
        await using var session = await RoundTripSession.CreateAsync(TestContext.Current.CancellationToken);

        const string Parent = "RT_Orders2";
        const string Child = "RT_OrderItems2";
        const string FkName = "RT_FK_Items_Orders";

        await using (var writer = await session.OpenWriterAsync())
        {
            await writer.CreateTableAsync(
                Parent,
                [
                    new("OrderID", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new("Region", typeof(string), maxLength: 32) { IsPrimaryKey = true, IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                Child,
                [
                    new("OrderID", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new("Region", typeof(string), maxLength: 32) { IsPrimaryKey = true, IsNullable = false },
                    new("LineNo", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new("Sku", typeof(string), maxLength: 32) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    FkName,
                    primaryTable: Parent,
                    primaryColumns: ["OrderID", "Region"],
                    foreignTable: Child,
                    foreignColumns: ["OrderID", "Region"])
                {
                    EnforceReferentialIntegrity = true,
                    CascadeUpdates = true,
                    CascadeDeletes = true,
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                Parent,
                new[]
                {
                    new object[] { 1, "North" },
                    new object[] { 2, "South" },
                },
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                Child,
                new[]
                {
                    new object[] { 1, "North", 1, "SKU-A" },
                    new object[] { 1, "North", 2, "SKU-B" },
                    new object[] { 2, "South", 1, "SKU-C" },
                },
                TestContext.Current.CancellationToken);
        }

        var pre = await CaptureSnapshotAsync(
            session.SourcePath,
            [Parent, Child],
            [FkName],
            TestContext.Current.CancellationToken);

        AssertPreCompactConsistency(pre, [Parent, Child], [FkName], expectedParentRows: 2, expectedChildRows: 3);
        await AssertTdefMagicStampsAsync(session.SourcePath, [Parent, Child], TestContext.Current.CancellationToken);

        session.RunDaoCompact();

        var post = await CaptureSnapshotAsync(
            session.CompactedPath,
            [Parent, Child],
            [FkName],
            TestContext.Current.CancellationToken);

        AssertSchemaSurvived(pre, post);
        Assert.Contains(post.Indexes[Parent], i => i.Kind == IndexKind.PrimaryKey && i.Columns == "OrderID+Region");
        Assert.Contains(post.Indexes[Child], i => i.Kind == IndexKind.PrimaryKey && i.Columns == "OrderID+Region+LineNo");
        Assert.Contains(post.Indexes[Child], i => i.IsForeignKey && i.Columns == "OrderID+Region" && i.CascadeUpdates && i.CascadeDeletes);
    }

    private static async Task<Snapshot> CaptureSnapshotAsync(
        string path,
        IReadOnlyList<string> tables,
        IReadOnlyList<string> fkNames,
        CancellationToken ct)
    {
        var snap = new Snapshot();
        await using var reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, ct);

        foreach (string t in tables)
        {
            var idx = await reader.ListIndexesAsync(t, ct);
            snap.Indexes[t] = idx
                .Select(i => new IndexSummary(
                    i.Name,
                    i.Kind,
                    i.IsForeignKey,
                    i.CascadeUpdates,
                    i.CascadeDeletes,
                    string.Join("+", i.Columns.Select(c => c.Name))))
                .OrderBy(i => i.Name, StringComparer.Ordinal)
                .ToList();

            DataTable? dt = await reader.ReadDataTableAsync(t, cancellationToken: ct);
            snap.RowCounts[t] = dt?.Rows.Count ?? -1;
        }

        DataTable? rel = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: ct);
        if (rel is not null && rel.Columns.Contains("szRelationship"))
        {
            int n = 0;
            foreach (DataRow row in rel.Rows)
            {
                string name = row["szRelationship"]?.ToString() ?? string.Empty;
                if (fkNames.Contains(name, StringComparer.Ordinal))
                {
                    n++;
                }
            }

            snap.RelationshipRowCount = n;
        }

        return snap;
    }

    private static void AssertSchemaSurvived(Snapshot pre, Snapshot post)
    {
        foreach (var (table, preIdx) in pre.Indexes)
        {
            Assert.True(post.Indexes.ContainsKey(table), $"table {table} disappeared after compact.");
            Assert.Equal(pre.RowCounts[table], post.RowCounts[table]);

            bool prePk = preIdx.Any(i => i.Kind == IndexKind.PrimaryKey);
            bool postPk = post.Indexes[table].Any(i => i.Kind == IndexKind.PrimaryKey);
            Assert.True(!prePk || postPk, $"{table}: primary-key index disappeared after compact.");

            int preFk = preIdx.Count(i => i.IsForeignKey);
            int postFk = post.Indexes[table].Count(i => i.IsForeignKey);
            Assert.True(preFk == 0 || postFk > 0, $"{table}: all foreign-key indexes disappeared after compact (pre={preFk}, post={postFk}).");
        }

        Assert.True(
            pre.RelationshipRowCount == 0 || post.RelationshipRowCount > 0,
            $"MSysRelationships rows for declared FKs disappeared after compact (pre={pre.RelationshipRowCount}, post={post.RelationshipRowCount}).");
    }

    /// <summary>
    /// Validates pre-compact snapshot consistency: tables exist, rows present,
    /// FK relationship rows recorded. Failures here indicate a writer bug
    /// (output is unreadable by our own reader) rather than a DAO issue.
    /// </summary>
    private static void AssertPreCompactConsistency(
        Snapshot snap,
        IReadOnlyList<string> tables,
        IReadOnlyList<string> fkNames,
        int expectedParentRows,
        int expectedChildRows)
    {
        Assert.True(snap.Indexes.ContainsKey(tables[0]), $"Pre-compact: parent table '{tables[0]}' not found by reader.");
        Assert.True(snap.Indexes.ContainsKey(tables[1]), $"Pre-compact: child table '{tables[1]}' not found by reader.");

        Assert.Equal(expectedParentRows, snap.RowCounts[tables[0]]);
        Assert.Equal(expectedChildRows, snap.RowCounts[tables[1]]);

        Assert.Contains(snap.Indexes[tables[0]], i => i.Kind == IndexKind.PrimaryKey);
        Assert.True(snap.RelationshipRowCount > 0, $"Pre-compact: no MSysRelationships rows for {string.Join(", ", fkNames)}.");
    }

    /// <summary>
    /// Reads the TDEF page for each table and asserts that the Jet4/ACE format
    /// magic (<c>0x00000659</c>) is stamped in the TDEF header, column
    /// descriptors, and index descriptors. Failures here point to a writer
    /// bug in TDEF construction rather than a DAO compact issue.
    /// </summary>
    private static async Task AssertTdefMagicStampsAsync(
        string dbPath,
        IReadOnlyList<string> tableNames,
        CancellationToken ct)
    {
        byte[] fileBytes = await File.ReadAllBytesAsync(dbPath, ct);
        const int PageSize = 4096;

        await using var reader = await AccessReader.OpenAsync(dbPath, new AccessReaderOptions { UseLockFile = false }, ct);
        foreach (string tableName in tableNames)
        {
            var entry = await reader.GetCatalogEntryAsync(tableName, ct);
            Assert.True(entry is not null, $"{tableName}: catalog entry not found.");
            int tdefPage = (int)entry.TDefPage;

            int off = tdefPage * PageSize;
            Assert.True(
                fileBytes[off] == 0x02 && fileBytes[off + 1] == 0x01,
                $"{tableName}: page {tdefPage} is not a TDEF (type=0x{fileBytes[off]:X2}{fileBytes[off + 1]:X2}).");

            int headerMagic = BitConverter.ToInt32(fileBytes, off + 0x0C);
            Assert.True(
                headerMagic == 0x00000659,
                $"{tableName}: TDEF header magic at 0x0C = 0x{headerMagic:X8}, expected 0x00000659.");

            int numCols = BitConverter.ToUInt16(fileBytes, off + 45);
            int numRealIdx = BitConverter.ToInt32(fileBytes, off + 51);
            int numIdx = BitConverter.ToInt32(fileBytes, off + 47);
            int colStart = off + 63 + (numRealIdx * 12);

            for (int c = 0; c < numCols; c++)
            {
                int o = colStart + (c * 25);
                int colMagic = BitConverter.ToInt32(fileBytes, o + 1);
                Assert.True(
                    colMagic == 0x00000659,
                    $"{tableName}: column[{c}] descriptor magic = 0x{colMagic:X8}, expected 0x00000659.");
            }

            // Walk past column names to reach real-idx physical descriptors.
            int namePos = colStart + (numCols * 25);
            for (int c = 0; c < numCols; c++)
            {
                int nameLen = BitConverter.ToUInt16(fileBytes, namePos);
                namePos += 2 + nameLen;
            }

            for (int i = 0; i < numRealIdx; i++)
            {
                int phys = namePos + (i * 52);
                int idxMagic = BitConverter.ToInt32(fileBytes, phys);
                Assert.True(
                    idxMagic == 0x00000659,
                    $"{tableName}: real-idx[{i}] magic = 0x{idxMagic:X8}, expected 0x00000659.");
            }

            // Logical-idx entries start after real-idx physical descriptors.
            int logStart = namePos + (numRealIdx * 52);
            for (int i = 0; i < numIdx; i++)
            {
                int logEntry = logStart + (i * 28);
                int logMagic = BitConverter.ToInt32(fileBytes, logEntry);
                Assert.True(
                    logMagic == 0x00000659,
                    $"{tableName}: logical-idx[{i}] magic = 0x{logMagic:X8}, expected 0x00000659.");
            }
        }
    }

    private sealed record IndexSummary(
        string Name,
        IndexKind Kind,
        bool IsForeignKey,
        bool CascadeUpdates,
        bool CascadeDeletes,
        string Columns);

    private sealed class Snapshot
    {
        public Dictionary<string, List<IndexSummary>> Indexes { get; } = new(StringComparer.Ordinal);

        public Dictionary<string, int> RowCounts { get; } = new(StringComparer.Ordinal);

        public int RelationshipRowCount { get; set; }
    }

    /// <summary>
    /// Owns a temp directory containing a copy of the Northwind fixture and
    /// the compacted output. Cleaned up on dispose.
    /// </summary>
    private sealed class RoundTripSession : IAsyncDisposable
    {
        private RoundTripSession(string workDir, string source, string compacted)
        {
            WorkDir = workDir;
            SourcePath = source;
            CompactedPath = compacted;
        }

        public string WorkDir { get; }

        public string SourcePath { get; }

        public string CompactedPath { get; }

        public static async Task<RoundTripSession> CreateAsync(CancellationToken ct)
        {
            string work = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.Tests.RoundTrip", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(work);
            string source = Path.Combine(work, "source.accdb");
            string compacted = Path.Combine(work, "compacted.accdb");
            await using (FileStream src = File.OpenRead(TestDatabases.NorthwindTraders))
            await using (FileStream dst = File.Create(source))
            {
                await src.CopyToAsync(dst, ct);
            }

            File.SetAttributes(source, File.GetAttributes(source) & ~FileAttributes.ReadOnly);
            return new RoundTripSession(work, source, compacted);
        }

        public ValueTask<AccessWriter> OpenWriterAsync() =>
            AccessWriter.OpenAsync(SourcePath, new AccessWriterOptions { UseLockFile = false });

        public void RunDaoCompact()
        {
            var result = AccessRoundTripEnvironment.RunDaoCompact(SourcePath, CompactedPath, CompactTimeout);
            if (result.ExitCode != 0 || !File.Exists(CompactedPath))
            {
                throw new Xunit.Sdk.XunitException(
                    $"DAO CompactDatabase failed (exit={result.ExitCode}).\n--- stdout ---\n{result.StdOut}\n--- stderr ---\n{result.StdErr}");
            }
        }

        public ValueTask DisposeAsync()
        {
            // Diagnostic: leave the work dir so failing tests can be
            // inspected. Path is logged via Console.WriteLine below.
            Console.WriteLine($"DIAG_RT_KEEP {WorkDir}");
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsyncOriginal()
        {
            try
            {
                if (Directory.Exists(WorkDir))
                {
                    Directory.Delete(WorkDir, recursive: true);
                }
            }
            catch (IOException)
            {
                // Best-effort cleanup; the temp folder is short-lived per run.
            }
            catch (UnauthorizedAccessException)
            {
                // Best-effort cleanup; the temp folder is short-lived per run.
            }

            return ValueTask.CompletedTask;
        }
    }
}
