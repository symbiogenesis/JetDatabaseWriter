namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Mirrors of high-value Jackcess writer tests that previously had no
/// counterpart in this suite (see <c>BigIndexTest</c>, <c>IndexTest.testConstraintViolation</c>,
/// and <c>IndexTest.testAutoNumberRecover</c> in the Jackcess source). Each
/// test exercises a code path the existing round-trip suite does not
/// reach:
/// <list type="bullet">
///   <item><description>A bulk insert large enough to force a multi-level B-tree (intermediate index pages, not just a single leaf).</description></item>
///   <item><description>The bulk-rebuild uniqueness check on a duplicate key inside an <see cref="IAccessWriter.InsertRowsAsync(string, System.Collections.Generic.IEnumerable{object[]}, System.Threading.CancellationToken)"/> batch.</description></item>
///   <item><description>The auto-increment counter must <b>not</b> skip a value when a prior row was rejected by a unique-index violation.</description></item>
/// </list>
/// </summary>
public sealed class BigIndexStressTests
{
    private const int PageSize = 4096; // ACE
    private const int LeafBitmaskOffset = 0x1B;
    private const int LeafFirstEntryOffset = 0x1E0;

    /// <summary>
    /// Bulk-inserts enough unique integer keys to force the B-tree rebuild
    /// to emit at least one intermediate (non-leaf) index page in addition
    /// to the leafs. Verifies the data round-trips through
    /// <see cref="AccessReader.ReadDataTableAsync"/>.
    /// </summary>
    /// <remarks>
    /// One Jet4 leaf page holds roughly <c>(0x1000 - 0x1E0) / 5 ≈ 612</c>
    /// fixed-int entries, so 1500 rows guarantees the builder must produce
    /// multiple leafs and a parent intermediate page (see
    /// <c>IndexBTreeBuilder</c>). The Jackcess <c>BigIndexTest</c> covers
    /// the same scenario at 2000 rows over text keys.
    /// </remarks>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Fact]
    public async Task BulkInsert_UniqueIntIndex_ManyRows_ProducesMultiLevelBTree()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;
        const int RowCount = 1500;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Big",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
                ct);

            var rows = new object[RowCount][];
            for (int i = 0; i < RowCount; i++)
            {
                rows[i] = [i + 1];
            }

            int inserted = await writer.InsertRowsAsync("Big", rows, ct);
            Assert.Equal(RowCount, inserted);
        }

        // Reader must surface every row. (Round-trip via ReadDataTableAsync
        // confirms the data pages remain readable; the index B-tree was
        // exercised purely on the write side.)
        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync("Big", cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(RowCount, dt.Rows.Count);
        var seen = new HashSet<int>();
        foreach (DataRow r in dt.Rows)
        {
            seen.Add((int)r["Id"]);
        }

        Assert.Equal(RowCount, seen.Count);
        Assert.Equal(1, seen.Min());
        Assert.Equal(RowCount, seen.Max());

        // Sanity-check on the file structure: at least two index leaf pages
        // were emitted by the rebuild — that's the hallmark of a multi-leaf
        // (and therefore multi-level) B-tree.
        Assert.True(
            CountLeafPages(stream.ToArray()) >= 2,
            "Expected the bulk rebuild to emit multiple index leaf pages for a 1500-row unique index.");
    }

    /// <summary>
    /// Mirror of Jackcess <c>IndexTest.testConstraintViolation</c>: feeding
    /// a duplicate key into <see cref="IAccessWriter.InsertRowsAsync(string, System.Collections.Generic.IEnumerable{object[]}, System.Threading.CancellationToken)"/>
    /// must surface a uniqueness failure. Our implementation defers the
    /// check until the bulk B-tree rebuild that runs after every row has
    /// been written (see <c>AccessWriter.MaintainIndexesAsync</c>), so the
    /// throw appears at the end of the call rather than mid-iteration.
    /// </summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Fact]
    public async Task BulkInsert_UniqueIndex_DuplicateKeyInBatch_ThrowsOnRebuild()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int))],
            [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
            ct);

        var batch = new[]
        {
            new object[] { 1 },
            [2],
            [3],
            [2], // duplicate
            [4],
        };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowsAsync("T", batch, ct));
    }

    /// <summary>
    /// Mirror of Jackcess <c>IndexTest.testAutoNumberRecover</c>: when an
    /// insert is rejected before any data is written, the next successful
    /// auto-increment value must <b>not</b> skip the rejected row's slot.
    /// </summary>
    /// <remarks>
    /// The Jackcess test rejects via a unique-index violation, but our
    /// uniqueness check is a post-write check (the offending row is already
    /// committed by the time the index rebuild detects the duplicate, see
    /// <c>AccessWriter.MaintainIndexesAsync</c>'s <c>IsUnique</c> branch).
    /// To exercise the autonumber-recovery semantic on its own, this test
    /// uses <see cref="ColumnDefinition.ValidationRule"/> instead, which
    /// fires <i>before</i> any row data is written and therefore leaves the
    /// counter in a consistent state.
    /// </remarks>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Fact]
    public async Task AutoIncrement_AfterRejectedInsert_DoesNotSkipNextValue()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true },
                    new ColumnDefinition("Data", typeof(string), maxLength: 50)
                    {
                        ValidationRule = v => !string.Equals(v as string, "REJECT", StringComparison.Ordinal),
                    },
                ],
                ct);

            // Two distinct values land cleanly: Id auto-assigns 1, 2.
            await writer.InsertRowAsync("T", [DBNull.Value, "row1"], ct);
            await writer.InsertRowAsync("T", [DBNull.Value, "row2"], ct);

            // The validation predicate rejects this row before any data is
            // written — the autonumber counter must NOT advance for it.
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync("T", [DBNull.Value, "REJECT"], ct));

            // Next successful insert must use Id == 3, not 4.
            await writer.InsertRowAsync("T", [DBNull.Value, "row3"], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken))!;

        var rows = dt.AsEnumerable()
            .Select(r => (Id: (int)r["Id"], Data: (string)r["Data"]))
            .OrderBy(t => t.Id)
            .ToArray();

        Assert.Equal(3, rows.Length);
        Assert.Equal((1, "row1"), rows[0]);
        Assert.Equal((2, "row2"), rows[1]);

        // The key assertion: Id == 3, not 4.
        Assert.Equal((3, "row3"), rows[2]);
    }

    /// <summary>
    /// Companion to <see cref="BulkInsert_UniqueIntIndex_ManyRows_ProducesMultiLevelBTree"/>:
    /// after a large bulk insert, deleting a slice in the middle and at the
    /// tail must leave the index consistent with the surviving row set.
    /// Adapted from the second half of Jackcess <c>BigIndexTest.testBigIndex</c>.
    /// </summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Fact]
    public async Task BulkInsert_ThenDeleteRange_LeavesConsistentIndex()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;
        const int RowCount = 800;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Big",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Note", typeof(string), maxLength: 32),
                ],
                [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
                ct);

            var rows = new object[RowCount][];
            for (int i = 0; i < RowCount; i++)
            {
                rows[i] = [i + 1, "n" + i];
            }

            await writer.InsertRowsAsync("Big", rows, ct);

            // Delete two specific rows (mid-range and near the tail). Each
            // delete triggers a full bulk rebuild via MaintainIndexesAsync.
            int d1 = await writer.DeleteRowsAsync("Big", "Id", 400, ct);
            int d2 = await writer.DeleteRowsAsync("Big", "Id", 799, ct);

            Assert.Equal(1, d1);
            Assert.Equal(1, d2);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync("Big", cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(RowCount - 2, dt.Rows.Count);

        var ids = dt.AsEnumerable().Select(r => (int)r["Id"]).ToHashSet();
        Assert.DoesNotContain(400, ids);
        Assert.DoesNotContain(799, ids);
        Assert.Contains(1, ids);
        Assert.Contains(RowCount, ids);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private static int CountLeafPages(byte[] fileBytes)
    {
        int n = 0;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;

            // page_type=0x04 (leaf), page_flag=0x01 (live, not orphaned).
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                // Count only leafs that actually hold real entries (more than
                // the implicit empty placeholder).
                if (CountLeafEntries(fileBytes, o) > 1)
                {
                    n++;
                }
            }
        }

        return n;
    }

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        int count = 1;
        for (int i = LeafBitmaskOffset; i < LeafFirstEntryOffset; i++)
        {
            byte b = fileBytes[leafOffset + i];
            for (int bit = 0; bit < 8; bit++)
            {
                if ((b & (1 << bit)) != 0)
                {
                    count++;
                }
            }
        }

        return count;
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
