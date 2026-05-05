namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Round-trip tests for the index rebalance / borrow path that Jackcess
/// exercises in <c>testCursors</c> / <c>testIndexCreation</c>. Unlike
/// <see cref="IndexSurgicalCascadingIntermediateCollapseTests"/> (which
/// focuses on collapsing entire subtrees), this class exercises:
/// <list type="bullet">
///   <item>Scattered deletes across a multi-level B-tree that leave some
///         intermediate pages with fewer children without emptying them
///         entirely — the "borrow" / underflow-without-collapse path.</item>
///   <item>Interleaved inserts after scattered deletes, forcing the surgical
///         path to handle splits within a previously-thinned tree.</item>
///   <item>Multiple rounds of delete + insert on the same index, verifying
///         the tree remains consistent after repeated structural mutations.</item>
/// </list>
/// </summary>
public sealed class IndexRebalanceBorrowPathTests
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    /// <summary>
    /// Creates a 3-level B-tree (1500 rows / int key ≈ 3 leaf pages +
    /// intermediate), deletes every 3rd row in a scattered pattern (leaving
    /// leaves underfull but not empty), then inserts new rows into the gaps.
    /// Verifies all surviving + newly inserted data round-trips.
    /// </summary>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ScatteredDeletes_ThenInserts_InMultiLevelTree_DataRoundTrips(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        const int initialRows = 1500;

        // Phase 1: Build a multi-level B-tree.
        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Val", typeof(string), maxLength: 50),
                ],
                [new IndexDefinition("IX_Id", "Id") { IsUnique = true }],
                ct);

            var rows = new object[initialRows][];
            for (int i = 0; i < initialRows; i++)
            {
                rows[i] = [i * 10, string.Create(CultureInfo.InvariantCulture, $"v{i}")];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        // Phase 2: Scattered deletes — remove every 3rd row. This thins
        // each leaf without emptying any, exercising the underflow-
        // without-collapse path.
        var deletedIds = new HashSet<int>();
        await using (var writer = await OpenWriterAsync(stream))
        {
            for (int i = 0; i < initialRows; i += 3)
            {
                int id = i * 10;
                int d = await writer.DeleteRowsAsync("T", "Id", id, ct);
                Assert.Equal(1, d);
                deletedIds.Add(id);
            }
        }

        // Phase 3: Insert new rows into gaps left by deletes. These keys
        // interleave with survivors, forcing splits on leaves that the
        // surgical path may have already thinned.
        int insertCount = 200;
        var insertedIds = new HashSet<int>();
        await using (var writer = await OpenWriterAsync(stream))
        {
            var rows = new object[insertCount][];
            for (int i = 0; i < insertCount; i++)
            {
                // Keys that fall between existing survivors: id*10 + 5
                int id = (i * 3 * 10) + 5;
                rows[i] = [id, string.Create(CultureInfo.InvariantCulture, $"new{i}")];
                insertedIds.Add(id);
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        // Verify: all survivors + new inserts are present.
        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync("T", cancellationToken: ct))!;

        int expectedCount = initialRows - deletedIds.Count + insertCount;
        Assert.Equal(expectedCount, dt.Rows.Count);

        var allIds = dt.AsEnumerable().Select(r => (int)r["Id"]).ToHashSet();
        Assert.Equal(expectedCount, allIds.Count);

        // No deleted ID should be present.
        foreach (int id in deletedIds)
        {
            Assert.DoesNotContain(id, allIds);
        }

        // All inserted IDs should be present.
        foreach (int id in insertedIds)
        {
            Assert.Contains(id, allIds);
        }
    }

    /// <summary>
    /// Performs multiple rounds of delete + insert on a large index,
    /// exercising the scenario where intermediate pages are repeatedly
    /// restructured. After each round the data must still round-trip.
    /// </summary>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task MultipleRounds_DeleteInsert_IntermediateRestructure_DataRoundTrips(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        const int initialRows = 1200;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Round", typeof(int)),
                ],
                [new IndexDefinition("IX_Id", "Id") { IsUnique = true }],
                ct);

            var rows = new object[initialRows][];
            for (int i = 0; i < initialRows; i++)
            {
                rows[i] = [i, 0];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        int nextId = initialRows;
        var expected = Enumerable.Range(0, initialRows).ToHashSet();

        // Three rounds of scattered delete + insert.
        for (int round = 1; round <= 3; round++)
        {
            // Delete every 5th remaining row.
            var toDelete = expected.Where(id => id % (5 * round) == 0).ToList();
            await using (var writer = await OpenWriterAsync(stream))
            {
                foreach (int id in toDelete)
                {
                    await writer.DeleteRowsAsync("T", "Id", id, ct);
                    expected.Remove(id);
                }
            }

            // Insert half as many new rows.
            int toInsert = toDelete.Count / 2;
            await using (var writer = await OpenWriterAsync(stream))
            {
                var rows = new object[toInsert][];
                for (int i = 0; i < toInsert; i++)
                {
                    rows[i] = [nextId, round];
                    expected.Add(nextId);
                    nextId++;
                }

                await writer.InsertRowsAsync("T", rows, ct);
            }

            // Verify after each round.
            await using var reader = await OpenReaderAsync(stream);
            DataTable dt = (await reader.ReadDataTableAsync("T", cancellationToken: ct))!;
            Assert.Equal(expected.Count, dt.Rows.Count);

            var actual = dt.AsEnumerable().Select(r => (int)r["Id"]).ToHashSet();
            Assert.Equal(expected.Count, actual.Count);
            string msg = FormattableString.Invariant(
                $"Round {round}: expected {expected.Count} IDs, got {actual.Count}");
            Assert.True(expected.SetEquals(actual), msg);
        }
    }

    /// <summary>
    /// Deletes rows from both extremes of a wide B-tree (low and high keys)
    /// in a single session, forcing the maintainer to handle both the
    /// leftmost and rightmost intermediate page boundaries simultaneously.
    /// This exercises tail_page recomputation under multi-group delete.
    /// </summary>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DeleteFromBothExtremes_TailPageRecomputes_DataRoundTrips(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        const int rowCount = 1500;
        const int deletePerSide = 100;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Zone", typeof(string), maxLength: 10),
                ],
                [new IndexDefinition("IX_Id", "Id") { IsUnique = true }],
                ct);

            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                string zone = i < deletePerSide ? "low"
                    : i >= rowCount - deletePerSide ? "high"
                    : "mid";
                rows[i] = [i, zone];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        // Delete low-end rows.
        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.DeleteRowsAsync("T", "Zone", "low", ct);
        }

        // Delete high-end rows.
        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.DeleteRowsAsync("T", "Zone", "high", ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync("T", cancellationToken: ct))!;

        int expectedCount = rowCount - (2 * deletePerSide);
        Assert.Equal(expectedCount, dt.Rows.Count);

        var ids = dt.AsEnumerable().Select(r => (int)r["Id"]).ToHashSet();
        for (int i = deletePerSide; i < rowCount - deletePerSide; i++)
        {
            Assert.Contains(i, ids);
        }

        for (int i = 0; i < deletePerSide; i++)
        {
            Assert.DoesNotContain(i, ids);
        }

        for (int i = rowCount - deletePerSide; i < rowCount; i++)
        {
            Assert.DoesNotContain(i, ids);
        }
    }

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
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
