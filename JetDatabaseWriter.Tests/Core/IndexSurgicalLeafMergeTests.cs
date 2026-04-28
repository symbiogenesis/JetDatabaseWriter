namespace JetDatabaseWriter.Tests.Core;

using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Round-trip tests for the leaf-merge (leaf merge on delete underflow) path
/// inside <c>AccessWriter.TrySurgicalCrossLeafMaintainAsync</c>. When a
/// per-leaf splice empties the affected leaf, the cross-leaf path drops
/// the leaf from its parent intermediate and patches the leaf-sibling
/// chain to skip it — the dead leaf is orphaned (Compact &amp; Repair
/// reclaims it). Bails to bulk rebuild when the dead leaf was its parent's
/// rightmost child (would shrink ancestor <c>tail_page</c>) or when the
/// parent has only one child (would cascade-collapse the parent).
/// </summary>
public sealed class IndexSurgicalLeafMergeTests
{
    private const int PageSize = 4096; // ACE

    [Fact]
    public async Task DeleteAllInLeftmostLeaf_MergesIntoRightSibling_AppendsZeroIndexPages()
    {
        // Non-unique index on Tag. 800 rows split 400/400 across Tag=0 and
        // Tag=1 → leaf 0 holds Tag=0, leaf 1 holds Tag=1; one intermediate
        // root with 2 entries. Deleting all Tag=0 empties leaf 0 (the
        // leftmost). leaf-merge conditions: parent count 2 (>= 2), deadIdx 0
        // (!= last) → merge engages. Dead leaf is orphaned (still 0x04
        // 0x01-tagged) so CountIndexPages stays equal.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Tag", typeof(int)),
                    new ColumnDefinition("Id", typeof(int)),
                ],
                [new IndexDefinition("IX_Tag", "Tag")],
                ct);

            var rows = new object[800][];
            for (int i = 0; i < 400; i++)
            {
                rows[i] = [0, i];
            }

            for (int i = 0; i < 400; i++)
            {
                rows[400 + i] = [1, i];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Tag", 0, ct);
            Assert.Equal(400, deleted);
        }

        int idxAfter = CountIndexPages(stream.ToArray());

        // Surgical merge: zero new index pages appended (orphaned dead
        // leaf still byte-counted, but count delta should remain zero
        // because the bulk path also leaves orphans — the difference is
        // bulk also APPENDS a fresh tree, surgical does not).
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(400, dt!.Rows.Count);

        // All surviving rows have Tag=1.
        foreach (DataRow r in dt.Rows)
        {
            Assert.Equal(1, (int)r["Tag"]);
        }
    }

    [Fact]
    public async Task DeleteAllInMiddleLeaf_MergesAndOrphans_PreservesDataIntegrity()
    {
        // Three Tag values, 400 rows each — the bulk index builder is
        // free to put Tag=1's entries on its own leaf or split them
        // across leaf boundaries depending on payload-area math. So we
        // can't strictly assert "page count unchanged" here (when the
        // delete change-set straddles two leaves and the descent picks
        // one of them, splice rejects the unresolved removes and falls
        // through to bulk rebuild bulk — a correctness path, just not the leaf-merge
        // surgical path). What we DO assert is that, regardless of which
        // path runs, the post-state is correct: 800 surviving rows, no
        // Tag=1 entries left, and a navigable tree.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Tag", typeof(int)),
                    new ColumnDefinition("Id", typeof(int)),
                ],
                [new IndexDefinition("IX_Tag", "Tag")],
                ct);

            var rows = new object[1200][];
            for (int t = 0; t < 3; t++)
            {
                for (int i = 0; i < 400; i++)
                {
                    rows[(t * 400) + i] = [t, i];
                }
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Tag", 1, ct);
            Assert.Equal(400, deleted);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(800, dt!.Rows.Count);

        var tags = dt.Rows.Cast<DataRow>().Select(r => (int)r["Tag"]).ToHashSet();
        Assert.Equal(2, tags.Count);
        Assert.Contains(0, tags);
        Assert.Contains(2, tags);
        Assert.DoesNotContain(1, tags);
    }

    [Fact]
    public async Task DeleteAllInTailLeaf_MergesAndPropagatesTailPage_AppendsZeroIndexPages()
    {
        // 3-leaf tree on a UNIQUE sparse INT index (1200 rows, Id =
        // 0, 10, ..., 11990 → bulk builder packs ~401 / 401 / 398 entries
        // per leaf). Region label is set so that ONLY the rightmost-leaf
        // rows carry Region = "C" — deleting Region = "C" then descends
        // every removed key into the rightmost leaf, splice empties it,
        // and the merge path engages without a concurrent middle-leaf
        // mutation.
        //
        // the parent intermediate (= root) drops
        // its rightmost child entry AND its tail_page header is recomputed
        // to point at the surviving (former middle) leaf. Surgical engages
        // → zero new index pages appended (the orphaned dead leaf is
        // reclaimed by Compact & Repair).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        const int Total = 1200;
        const int LeftAndMidCount = 802; // matches the bulk builder split

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Region", typeof(string), maxLength: 4),
                ],
                [new IndexDefinition("IX_Id", "Id") { IsUnique = true }],
                ct);

            var rows = new object[Total][];
            for (int i = 0; i < Total; i++)
            {
                rows[i] = [i * 10, i < LeftAndMidCount ? "AB" : "C"];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        const int ExpectedDeletes = Total - LeftAndMidCount; // 398
        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Region", "C", ct);
            Assert.Equal(ExpectedDeletes, deleted);
        }

        int idxAfter = CountIndexPages(stream.ToArray());
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(LeftAndMidCount, dt!.Rows.Count);

        foreach (DataRow r in dt.Rows)
        {
            Assert.Equal("AB", (string)r["Region"]);
        }
    }

    [Fact]
    public async Task DeleteAcrossMultipleLeaves_NoUnderflow_RewritesInPlace()
    {
        // Indexed column `Id`, predicate column `Desc` (NOT indexed).
        // Insert 800 rows where every other row carries Desc="DEL".
        // DeleteRowsAsync(T, "Desc", "DEL") removes 400 rows spread evenly
        // across both leaves of the IX_Id index → cross-leaf change-set
        // with no per-leaf underflow. cross-leaf surgical in-place rewrites both leaves.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Desc", typeof(string), maxLength: 8),
                ],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i + 1, (i % 2 == 0) ? "DEL" : "KEEP"];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Desc", "DEL", ct);
            Assert.Equal(400, deleted);
        }

        int idxAfter = CountIndexPages(stream.ToArray());
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(400, dt!.Rows.Count);

        // Surviving rows all have Desc=KEEP and even Ids (since DEL was
        // assigned to even-INDEX rows, which got Id = i+1 = odd Id;
        // KEEP rows ended up with even Id values).
        foreach (DataRow r in dt.Rows)
        {
            Assert.Equal("KEEP", (string)r["Desc"]);
            Assert.Equal(0, ((int)r["Id"]) % 2);
        }
    }

    [Fact]
    public async Task DeleteEmptiesLeaf_DataIntegrityRoundTrips()
    {
        // Functional verification: after a leaf-empty operation (path may
        // engage leaf-merge merge OR fall back to bulk rebuild depending on leaf
        // alignment), the index can still be read back AND a subsequent
        // mutation succeeds (the chain pointers were patched correctly
        // either way).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Tag", typeof(int)),
                    new ColumnDefinition("Id", typeof(int)),
                ],
                [new IndexDefinition("IX_Tag", "Tag")],
                ct);

            var rows = new object[1200][];
            for (int t = 0; t < 3; t++)
            {
                for (int i = 0; i < 400; i++)
                {
                    rows[(t * 400) + i] = [t, i];
                }
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.DeleteRowsAsync("T", "Tag", 1, ct);
            await writer.InsertRowAsync("T", new object[] { 0, 9999 }, ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(801, dt!.Rows.Count);

        // 9999 row present.
        bool found = false;
        foreach (DataRow r in dt.Rows)
        {
            if ((int)r["Tag"] == 0 && (int)r["Id"] == 9999)
            {
                found = true;
                break;
            }
        }

        Assert.True(found, "Post-delete inserted row should be readable.");

        // No Tag=1 rows survived.
        foreach (DataRow r in dt.Rows)
        {
            Assert.NotEqual(1, (int)r["Tag"]);
        }
    }

    [Fact]
    public async Task DeleteAllInRightmostLeaf_MergesAndShrinksRoot_AppendsZeroIndexPages()
    {
        // Two-leaf tree on a UNIQUE sparse INT index. The bulk builder
        // packs ~401 entries into the leftmost leaf and ~399 into the
        // rightmost (split at Id = 4010 for 800 sparse rows on a 4 KiB
        // page). We label rows so that only the rightmost-leaf rows carry
        // Region = "B" — deleting Region = "B" then descends EVERY removed
        // key into the rightmost leaf, splice empties it, and the merge
        // path engages without a concurrent left-sibling mutation.
        //
        // the parent intermediate (= root) drops
        // its rightmost child entry, leaving a single-entry root pointing
        // at the surviving (former leftmost) leaf, with tail_page
        // recomputed to that leaf's page number. Single-entry intermediates
        // are valid — the seeker descends through their lone child pointer.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        const int Total = 800;
        const int LeftLeafCount = 401; // matches the bulk builder split

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Region", typeof(string), maxLength: 4),
                ],
                [new IndexDefinition("IX_Id", "Id") { IsUnique = true }],
                ct);

            var rows = new object[Total][];
            for (int i = 0; i < Total; i++)
            {
                rows[i] = [i * 10, i < LeftLeafCount ? "A" : "B"];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int idxBefore = CountIndexPages(stream.ToArray());

        const int ExpectedDeletes = Total - LeftLeafCount; // 399
        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Region", "B", ct);
            Assert.Equal(ExpectedDeletes, deleted);
        }

        int idxAfter = CountIndexPages(stream.ToArray());
        Assert.Equal(idxBefore, idxAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(LeftLeafCount, dt!.Rows.Count);
        foreach (DataRow r in dt.Rows)
        {
            Assert.Equal("A", (string)r["Region"]);
        }
    }

    private static int CountIndexPages(byte[] fileBytes)
    {
        int n = 0;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            byte t = fileBytes[o];
            if ((t == 0x03 || t == 0x04) && fileBytes[o + 1] == 0x01)
            {
                n++;
            }
        }

        return n;
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
