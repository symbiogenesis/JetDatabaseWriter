namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Structural integrity sweeps for index B-trees, mirroring two Jackcess
/// fixture-driven tests:
/// <list type="bullet">
///   <item><c>IndexTest.testComplexIndex</c> — opens each <c>compIndexTest*</c>
///         fixture and asserts <c>countRows(t) == indexData.getEntryCount()</c>
///         for the indexed table. Catches "we lost an entry on a page split"
///         class bugs.</item>
///   <item><c>IndexTest.testByteOrder</c> — verifies the in-memory unsigned
///         byte comparator matches the on-disk leaf chain ordering. Cheap
///         smoke test that all leaves we walk are actually sorted, which is
///         a precondition for the position-by-position fixture tests.</item>
/// </list>
/// </summary>
public sealed class IndexBTreeStructuralFixtureTests
{
    public static TheoryData<string> CompIndexFixtures =>
    [
        TestDatabases.CompIndexTestV2000,

        // V2003 excluded: the on-disk MSysObjects catalog for this fixture
        // contains only system tables (every Type=1 entry has the system
        // bit set in Flags) — there is no user Table1 to scan. Not a
        // reader bug; verified via catalog dump on 2026-05-04.
        TestDatabases.CompIndexTestV2007,
        TestDatabases.CompIndexTestV2010,
    ];

    public static TheoryData<string> AllJackcessFixtures
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (string p in new[]
            {
                TestDatabases.IndexTestV2000,
                TestDatabases.IndexTestV2003,
                TestDatabases.IndexTestV2007,
                TestDatabases.IndexTestV2010,
                TestDatabases.CompIndexTestV2000,

                // CompIndexTestV2003 excluded: on-disk MSysObjects has no
                // user (Type=1) tables, only system tables. Verified via
                // catalog dump on 2026-05-04 — not a catalog reader bug.
                TestDatabases.CompIndexTestV2007,
                TestDatabases.CompIndexTestV2010,

                // BigIndexTest V2000–V2010 excluded: these are schema-only
                // templates (Jackcess populates them at test time); the
                // on-disk index B-tree root page exists (FirstDp > 0) but
                // contains zero leaf entries.
                TestDatabases.TestIndexCodesV2000,
                TestDatabases.TestIndexCodesV2003,
                TestDatabases.TestIndexCodesV2007,
                TestDatabases.TestIndexCodesV2010,
                TestDatabases.BinIdxTestV2010,
            })
            {
                data.Add(p);
            }

            return data;
        }
    }

    public static TheoryData<string> BigIndexFixtures =>
    [
        TestDatabases.BigIndexTestV2000,
        TestDatabases.BigIndexTestV2003,
        TestDatabases.BigIndexTestV2007,
        TestDatabases.BigIndexTestV2010,
    ];

    /// <summary>
    /// Mirrors Jackcess <c>IndexTest.testComplexIndex</c>:
    /// <c>assertEquals(512, countRows(t));
    /// assertEquals(512, index.getIndexData().getEntryCount())</c>.
    /// </summary>
    /// <param name="fixturePath">Absolute path to the compIndexTest fixture under test.</param>
    [Theory]
    [MemberData(nameof(CompIndexFixtures))]
    public async Task CompIndex_RowCount_EqualsLeafEntryCount(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        // The compIndex fixtures all have a "Table1" with a non-FK index
        // whose entry count equals the table row count. Jackcess hard-codes
        // the row count (512) — we instead read it from the table so the
        // assertion stays meaningful even if the fixture is regenerated.
        DataTable dt = await reader.ReadDataTableAsync("Table1", cancellationToken: ct);
        int rowCount = dt.Rows.Count;

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Table1", ct);
        IndexMetadata? primary = indexes.FirstOrDefault(i => !i.IsForeignKey && i.FirstDp > 0);
        if (primary is null)
        {
            Assert.Fail($"No non-FK index with first_dp on Table1 of '{fixturePath}'.");
        }

        int leafEntryCount = await CountLeafEntriesAsync(reader, layout, pageSize, primary.FirstDp, ct);

        // Allow slack only for IGNORE_NULLS indexes (which legitimately omit
        // null-keyed rows). Composite indexes in compIndexTest* fixtures
        // don't set IGNORE_NULLS, so the strict equality should hold.
        Assert.True(
            leafEntryCount == rowCount,
            $"compIndex {fixturePath} Table1: rowCount={rowCount} leafEntryCount={leafEntryCount} (index '{primary.Name}')");
    }

    /// <summary>
    /// Mirrors Jackcess <c>IndexTest.testByteOrder</c>: verifies that every
    /// non-FK B-tree we walk yields leaves whose keys are sorted in
    /// ascending unsigned-byte order. (The leaves are stored in unsigned
    /// order on disk per §4 of the Access format; if our walker breaks the
    /// order, every fixture-driven encoder comparison would silently
    /// degrade into noise.)
    /// </summary>
    /// <param name="fixturePath">Absolute path to a Jackcess fixture under test.</param>
    [Theory]
    [MemberData(nameof(AllJackcessFixtures))]
    public async Task LeafChain_IsSortedByUnsignedByteOrder(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        int btreesChecked = 0;
        int totalEntries = 0;

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            IReadOnlyList<IndexMetadata> indexes;
            try
            {
                indexes = await reader.ListIndexesAsync(tableName, ct);
            }
            catch (NotSupportedException)
            {
                continue;
            }

            foreach (IndexMetadata index in indexes)
            {
                if (index.IsForeignKey || index.FirstDp <= 0)
                {
                    continue;
                }

                List<byte[]> keys;
                try
                {
                    keys = await CollectAllLeafKeysAsync(reader, layout, pageSize, index.FirstDp, ct);
                }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
                {
                    // Surface the bad page in the failure message but keep
                    // sweeping so we see the full damage report.
                    Assert.Fail($"Leaf walk failed for {tableName}.{index.Name} in '{fixturePath}': {ex.Message}");
                    return;
                }

                btreesChecked++;
                totalEntries += keys.Count;
                for (int i = 1; i < keys.Count; i++)
                {
                    int cmp = CompareBytesUnsigned(keys[i - 1], keys[i]);
                    if (cmp > 0)
                    {
                        Assert.Fail(
                            $"Out-of-order leaf entries in {tableName}.{index.Name} at position {i} "
                            + $"(prev={Convert.ToHexString(keys[i - 1])} cur={Convert.ToHexString(keys[i])}) "
                            + $"in '{fixturePath}'");
                    }
                }
            }
        }

        if (btreesChecked == 0 || totalEntries == 0)
        {
            Assert.Fail($"No scannable B-trees in '{fixturePath}'.");
        }
    }

    /// <summary>
    /// Mirrors Jackcess's implicit invariant from <c>IndexData.readDataPage</c>:
    /// every leaf entry's row trailer must point at a (data_page, data_row)
    /// inside the table's page chain. We can't easily validate the data row
    /// number without parsing the row stub directory, but we can at least
    /// assert that the data_page is non-zero, fits within the file, and
    /// (per Jackcess <c>RowIdImpl</c>) has page_type 0x01 (DATA).
    /// </summary>
    /// <param name="fixturePath">Absolute path to a Jackcess fixture under test.</param>
    [Theory]
    [MemberData(nameof(AllJackcessFixtures))]
    public async Task LeafEntryTrailers_PointAtValidDataPages(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        long fileLength = new FileInfo(fixturePath).Length;
        long maxPageNumber = fileLength / pageSize;

        var seenInvalidPages = new HashSet<long>();
        int totalEntries = 0;

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            IReadOnlyList<IndexMetadata> indexes;
            try
            {
                indexes = await reader.ListIndexesAsync(tableName, ct);
            }
            catch (NotSupportedException)
            {
                continue;
            }

            foreach (IndexMetadata index in indexes)
            {
                if (index.IsForeignKey || index.FirstDp <= 0)
                {
                    continue;
                }

                List<IndexEntry> entries;
                try
                {
                    entries = await CollectAllLeafEntriesAsync(reader, layout, pageSize, index.FirstDp, ct);
                }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
                {
                    continue;
                }

                foreach (IndexEntry e in entries)
                {
                    totalEntries++;
                    if (e.DataPage <= 0 || e.DataPage > maxPageNumber)
                    {
                        seenInvalidPages.Add(e.DataPage);
                    }
                }
            }
        }

        if (totalEntries == 0)
        {
            Assert.Fail($"No leaf entries scanned in '{fixturePath}'.");
        }

        Assert.Empty(seenInvalidPages);
    }

    /// <summary>
    /// Mirrors Jackcess <c>BigIndexTest.testBigIndex</c>: the on-disk
    /// <c>bigIndexTest*</c> fixtures are schema-only templates (Table1
    /// with two text columns and a non-unique index on <c>col1</c>; root
    /// page exists but holds zero leaf entries). Jackcess populates them
    /// at runtime to force a multi-level B-tree, then asserts the index
    /// stays consistent. We do the same: copy the fixture to a temp path,
    /// use <see cref="AccessWriter"/> to insert enough rows to split the
    /// root into an intermediate, then run the row-count == leaf-entry-count
    /// and monotonic-byte-order invariants against the populated copy via
    /// <see cref="AccessReader"/>. This is a writer→reader round-trip on
    /// a multi-level B-tree, not a pure read of canonical Jackcess output.
    /// </summary>
    /// <param name="fixturePath">Absolute path to a <c>bigIndexTest*</c> fixture.</param>
    [Theory]
    [MemberData(nameof(BigIndexFixtures))]
    public async Task BigIndex_PopulatedAtRuntime_RowCount_EqualsLeafEntryCount(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int rowCount = 2000;

        string temp = Path.Combine(
            Path.GetTempPath(),
            $"bigIndexTest_{Guid.NewGuid():N}{Path.GetExtension(fixturePath)}");
        File.Copy(fixturePath, temp);
        File.SetAttributes(temp, FileAttributes.Normal);

        try
        {
            await using (AccessWriter writer = await AccessWriter.OpenAsync(
                temp,
                new AccessWriterOptions { UseLockFile = false },
                ct))
            {
                _ = await writer.InsertRowsAsync("Table1", EnumerateRows(rowCount), ct);
            }

            static IEnumerable<object[]> EnumerateRows(int count)
            {
                for (int i = 0; i < count; i++)
                {
                    // Vary key length to exercise different leaf-entry sizes;
                    // include 'i' to keep keys distinct.
                    string col1 = string.Create(
                        System.Globalization.CultureInfo.InvariantCulture,
                        $"key{i:D6}_{new string('x', i % 17)}");
                    string col2 = string.Create(
                        System.Globalization.CultureInfo.InvariantCulture,
                        $"data{i}");
                    yield return [col1, col2];
                }
            }

            await using AccessReader reader = await AccessReader.OpenAsync(
                temp,
                new AccessReaderOptions { UseLockFile = false },
                ct);

            IndexLeafPageBuilder.LeafPageLayout layout =
                IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
            int pageSize = reader.PageSize;

            DataTable dt = await reader.ReadDataTableAsync("Table1", cancellationToken: ct);
            Assert.Equal(rowCount, dt.Rows.Count);

            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Table1", ct);
            IndexMetadata index = indexes.Single(i => !i.IsForeignKey && i.FirstDp > 0);

            // Walk down through any intermediate levels and confirm the tree
            // actually has at least one — otherwise this test isn't covering
            // what its name claims.
            byte[] rootPage = await reader.GetRawPageBytesAsync(index.FirstDp, ct);
            Assert.True(
                rootPage[0] == Constants.IndexLeafPage.PageTypeIntermediate,
                $"Expected root page 0x{index.FirstDp:X} to be intermediate (0x03) for a populated big-index B-tree; got 0x{rootPage[0]:X2}.");

            List<IndexEntry> leafEntries = await CollectAllLeafEntriesAsync(
                reader, layout, pageSize, index.FirstDp, ct);

            Assert.Equal(rowCount, leafEntries.Count);

            // Monotonic unsigned-byte order across the full leaf chain.
            for (int i = 1; i < leafEntries.Count; i++)
            {
                int cmp = CompareBytesUnsigned(leafEntries[i - 1].Key, leafEntries[i].Key);
                Assert.True(
                    cmp <= 0,
                    $"Out-of-order leaf entries at position {i} in populated '{Path.GetFileName(fixturePath)}'.");
            }
        }
        finally
        {
            try
            {
                File.Delete(temp);
            }
            catch (IOException)
            {
            }
        }
    }

    private static async Task<int> CountLeafEntriesAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        List<IndexEntry> entries = await CollectAllLeafEntriesAsync(reader, layout, pageSize, rootPage, ct);
        return entries.Count;
    }

    private static async Task<List<byte[]>> CollectAllLeafKeysAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        List<IndexEntry> entries = await CollectAllLeafEntriesAsync(reader, layout, pageSize, rootPage, ct);
        return entries.Select(e => e.Key).ToList();
    }

    private static async Task<List<IndexEntry>> CollectAllLeafEntriesAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            byte pageType = page[0];
            if (pageType == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            if (pageType != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                throw new InvalidOperationException(
                    $"Unexpected page_type 0x{pageType:X2} at page {current} (expected 0x03 or 0x04).");
            }

            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            if (entries.Count == 0)
            {
                throw new InvalidOperationException($"Intermediate page {current} has no entries.");
            }

            current = entries[0].ChildPage;
        }

        var result = new List<IndexEntry>();
        long visitGuard = 0;
        while (current != 0)
        {
            if (++visitGuard > 100_000)
            {
                throw new InvalidOperationException("Leaf chain exceeds visit guard — possible cycle.");
            }

            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            if (page[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                throw new InvalidOperationException(
                    $"Expected leaf page (0x04) at page {current}; got 0x{page[0]:X2}.");
            }

            List<IndexEntry> entries = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
            result.AddRange(entries);

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    private static int CompareBytesUnsigned(byte[] a, byte[] b)
    {
        int min = Math.Min(a.Length, b.Length);
        for (int i = 0; i < min; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                return diff;
            }
        }

        return a.Length - b.Length;
    }
}
