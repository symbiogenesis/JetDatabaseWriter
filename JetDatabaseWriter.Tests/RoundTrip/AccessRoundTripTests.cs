namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
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
/// that keeps Compact &amp; Repair relationship metadata behavior representative.
/// Both relationship scenarios are built into one fixture copy and compacted
/// once, so the tests keep separate assertions without paying duplicate DAO
/// compact cost.
/// Do not replace this host with a fresh writer-created database: the writer
/// output is the subject under test, not the fixture oracle.
/// </para>
/// </remarks>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class AccessRoundTripTests
{
    private const string CompositeChild = "RT_OrderItems2";
    private const string CompositeFkName = "RT_FK_Items_Orders";
    private const string CompositeParent = "RT_Orders2";
    private const string SingleChild = "RT_Orders";
    private const string SingleFkName = "RT_FK_Orders_Customers";
    private const string SingleParent = "RT_Customers";

    private static readonly TimeSpan CompactTimeout = TimeSpan.FromMinutes(2);
#if NET8_0_OR_GREATER
    private static readonly Lock relationshipRoundTripSync = new();
#else
    private static readonly object relationshipRoundTripSync = new();
#endif
    private static Task<RelationshipRoundTripResult>? relationshipRoundTripTask;

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task SinglePk_AndSingleColumnFk_SurviveCompactAndRepair()
    {
        RelationshipRoundTripResult result = await GetRelationshipRoundTripResultAsync(TestContext.Current.CancellationToken);

        AssertSchemaSurvived(result.SinglePre, result.SinglePost);
        Assert.Contains(result.SinglePost.Indexes[SingleChild], i => i.Kind == IndexKind.PrimaryKey && i.Columns == "OrderID");
        Assert.Contains(result.SinglePost.Indexes[SingleChild], i => i.IsForeignKey && i.Columns == "CustomerID" && i.CascadeDeletes);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task CompositePk_AndMultiColumnFk_SurviveCompactAndRepair()
    {
        RelationshipRoundTripResult result = await GetRelationshipRoundTripResultAsync(TestContext.Current.CancellationToken);

        AssertSchemaSurvived(result.CompositePre, result.CompositePost);
        Assert.Contains(result.CompositePost.Indexes[CompositeParent], i => i.Kind == IndexKind.PrimaryKey && i.Columns == "OrderID+Region");
        Assert.Contains(result.CompositePost.Indexes[CompositeChild], i => i.Kind == IndexKind.PrimaryKey && i.Columns == "OrderID+Region+LineNo");
        Assert.Contains(result.CompositePost.Indexes[CompositeChild], i => i.IsForeignKey && i.Columns == "OrderID+Region" && i.CascadeUpdates && i.CascadeDeletes);
    }

    private static Task<RelationshipRoundTripResult> GetRelationshipRoundTripResultAsync(CancellationToken cancellationToken)
    {
        lock (relationshipRoundTripSync)
        {
            relationshipRoundTripTask ??= BuildRelationshipRoundTripResultAsync(cancellationToken);
            return relationshipRoundTripTask;
        }
    }

    private static async Task<RelationshipRoundTripResult> BuildRelationshipRoundTripResultAsync(CancellationToken cancellationToken)
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(
            cancellationToken,
            compactTimeout: CompactTimeout);

        await using (AccessWriter writer = await session.OpenWriterAsync(cancellationToken))
        {
            await writer.CreateTableAsync(
                SingleParent,
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                ],
                cancellationToken);

            await writer.CreateTableAsync(
                SingleChild,
                [
                    new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("CustomerID", typeof(int)) { IsNullable = false },
                    new("OrderDate", typeof(DateTime)),
                ],
                cancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    SingleFkName,
                    primaryTable: SingleParent,
                    primaryColumn: "CustomerID",
                    foreignTable: SingleChild,
                    foreignColumn: "CustomerID")
                {
                    EnforceReferentialIntegrity = true,
                    CascadeDeletes = true,
                },
                cancellationToken);

            await writer.InsertRowsAsync(
                SingleParent,
                [
                    [DBNull.Value, "Acme"],
                    [DBNull.Value, "Beta"],
                    [DBNull.Value, "Gamma"],
                ],
                cancellationToken);

            await writer.InsertRowsAsync(
                SingleChild,
                [
                    [DBNull.Value, 1, new DateTime(2025, 1, 15)],
                    [DBNull.Value, 2, new DateTime(2025, 2, 20)],
                    [DBNull.Value, 3, new DateTime(2025, 3, 03)],
                ],
                cancellationToken);

            await writer.CreateTableAsync(
                CompositeParent,
                [
                    new("OrderID", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new("Region", typeof(string), maxLength: 32) { IsPrimaryKey = true, IsNullable = false },
                ],
                cancellationToken);

            await writer.CreateTableAsync(
                CompositeChild,
                [
                    new("OrderID", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new("Region", typeof(string), maxLength: 32) { IsPrimaryKey = true, IsNullable = false },
                    new("LineNo", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new("Sku", typeof(string), maxLength: 32) { IsNullable = false },
                ],
                cancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    CompositeFkName,
                    primaryTable: CompositeParent,
                    primaryColumns: ["OrderID", "Region"],
                    foreignTable: CompositeChild,
                    foreignColumns: ["OrderID", "Region"])
                {
                    EnforceReferentialIntegrity = true,
                    CascadeUpdates = true,
                    CascadeDeletes = true,
                },
                cancellationToken);

            await writer.InsertRowsAsync(
                CompositeParent,
                [
                    [1, "North"],
                    [2, "South"],
                ],
                cancellationToken);

            await writer.InsertRowsAsync(
                CompositeChild,
                [
                    [1, "North", 1, "SKU-A"],
                    [1, "North", 2, "SKU-B"],
                    [2, "South", 1, "SKU-C"],
                ],
                cancellationToken);
        }

        Snapshot singlePre = await CaptureSnapshotAsync(
            session.SourcePath,
            [SingleParent, SingleChild],
            [SingleFkName],
            cancellationToken);
        AssertPreCompactConsistency(singlePre, [SingleParent, SingleChild], [SingleFkName], expectedParentRows: 3, expectedChildRows: 3);

        Snapshot compositePre = await CaptureSnapshotAsync(
            session.SourcePath,
            [CompositeParent, CompositeChild],
            [CompositeFkName],
            cancellationToken);
        AssertPreCompactConsistency(compositePre, [CompositeParent, CompositeChild], [CompositeFkName], expectedParentRows: 2, expectedChildRows: 3);

        await AssertTdefMagicStampsAsync(
            session.SourcePath,
            [SingleParent, SingleChild, CompositeParent, CompositeChild],
            cancellationToken);

        session.RunDaoCompact();

        Snapshot singlePost = await CaptureSnapshotAsync(
            session.CompactedPath,
            [SingleParent, SingleChild],
            [SingleFkName],
            cancellationToken);
        Snapshot compositePost = await CaptureSnapshotAsync(
            session.CompactedPath,
            [CompositeParent, CompositeChild],
            [CompositeFkName],
            cancellationToken);

        return new RelationshipRoundTripResult(singlePre, singlePost, compositePre, compositePost);
    }

    private static async Task<Snapshot> CaptureSnapshotAsync(
        string path,
        IReadOnlyList<string> tables,
        IReadOnlyList<string> fkNames,
        CancellationToken ct)
    {
        var snap = new Snapshot();
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, ct);

        foreach (string t in tables)
        {
            IReadOnlyList<IndexMetadata> idx = await reader.ListIndexesAsync(t, ct);
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

            DataTable dt = await reader.ReadDataTableAsync(t, cancellationToken: ct);
            snap.RowCounts[t] = dt?.Rows.Count ?? -1;
        }

        DataTable rel = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: ct);
        if (rel?.Columns.Contains("szRelationship") == true)
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
        foreach ((string? table, List<IndexSummary>? preIdx) in pre.Indexes)
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
    /// <param name="snap">The row or relationship snapshot.</param>
    /// <param name="tables">The tables.</param>
    /// <param name="fkNames">The foreign key names.</param>
    /// <param name="expectedParentRows">The expected parent rows.</param>
    /// <param name="expectedChildRows">The expected child rows.</param>
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
    /// Reads the TDEF page for each table and asserts that the Jet4/ACE
    /// format-wide magic (<c>0x00000659</c>) is stamped in the TDEF header,
    /// every column descriptor, and every logical-idx entry, AND that the
    /// distinct real-idx physical-descriptor magic (<c>0x00000783</c>,
    /// <see cref="Constants.TableDefinition.Jet4.RealIdx.LeadingMagic"/>) is
    /// stamped in every real-idx physical descriptor. Failures here point to a
    /// writer bug in TDEF construction rather than a DAO compact issue.
    /// </summary>
    /// <param name="dbPath">The db path.</param>
    /// <param name="tableNames">The table names.</param>
    /// <param name="ct">The cancellation token.</param>
    private static async Task AssertTdefMagicStampsAsync(
        string dbPath,
        IReadOnlyList<string> tableNames,
        CancellationToken ct)
    {
        byte[] fileBytes = await File.ReadAllBytesAsync(dbPath, ct);
        await using AccessReader reader = await AccessReader.OpenAsync(dbPath, new AccessReaderOptions { UseLockFile = false }, ct);
        foreach (string tableName in tableNames)
        {
            CatalogEntry? entry = await reader.GetCatalogEntryAsync(tableName, ct);
            Assert.True(entry is not null, $"{tableName}: catalog entry not found.");
            int tdefPage = (int)entry.TDefPage;

            int off = tdefPage * Constants.PageSizes.Jet4;
            Assert.True(
                fileBytes[off] == 0x02 && fileBytes[off + 1] == 0x01,
                $"{tableName}: page {tdefPage} is not a TDEF (type=0x{fileBytes[off]:X2}{fileBytes[off + 1]:X2}).");

            int headerMagic = BitConverter.ToInt32(fileBytes, off + 0x0C);
            Assert.True(
                headerMagic == Constants.TableDefinition.Jet4.FormatMagic,
                $"{tableName}: TDEF header magic at 0x0C = 0x{headerMagic:X8}, expected 0x{Constants.TableDefinition.Jet4.FormatMagic:X8}.");

            int numCols = BitConverter.ToUInt16(fileBytes, off + 45);
            int numRealIdx = BitConverter.ToInt32(fileBytes, off + 51);
            int numIdx = BitConverter.ToInt32(fileBytes, off + 47);
            int colStart = off + 63 + (numRealIdx * 12);

            for (int c = 0; c < numCols; c++)
            {
                int o = colStart + (c * 25);
                int colMagic = BitConverter.ToInt32(fileBytes, o + 1);
                Assert.True(
                    colMagic == Constants.TableDefinition.Jet4.FormatMagic,
                    $"{tableName}: column[{c}] descriptor magic = 0x{colMagic:X8}, expected 0x{Constants.TableDefinition.Jet4.FormatMagic:X8}.");
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
                    idxMagic == Constants.TableDefinition.Jet4.RealIdx.LeadingMagic,
                    $"{tableName}: real-idx[{i}] magic = 0x{idxMagic:X8}, expected 0x{Constants.TableDefinition.Jet4.RealIdx.LeadingMagic:X8}.");
            }

            // Logical-idx entries start after real-idx physical descriptors.
            int logStart = namePos + (numRealIdx * 52);
            for (int i = 0; i < numIdx; i++)
            {
                int logEntry = logStart + (i * 28);
                int logMagic = BitConverter.ToInt32(fileBytes, logEntry);
                Assert.True(
                    logMagic == Constants.TableDefinition.Jet4.FormatMagic,
                    $"{tableName}: logical-idx[{i}] magic = 0x{logMagic:X8}, expected 0x{Constants.TableDefinition.Jet4.FormatMagic:X8}.");
            }
        }
    }

    private sealed record RelationshipRoundTripResult(
        Snapshot SinglePre,
        Snapshot SinglePost,
        Snapshot CompositePre,
        Snapshot CompositePost);

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
}
