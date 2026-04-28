namespace JetDatabaseWriter.Tests.Core;

using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Round-trip tests for the surgical cascading-underflow path that handles
/// recursive intermediate collapse. The previous implementation routed
/// <c>TryStageIntermediateRewritesAsync</c> to the bulk-rebuild
/// fallback whenever a multi-group delete batch left an intermediate
/// page with zero surviving children; the current code handles three
/// shapes surgically (single-leaf collapse, contiguous run-of-leaves
/// collapse, and full-subtree cascade up to the grandparent) plus
/// recomputes the captured ancestor's <c>tail_page</c> via
/// <c>GetEffectiveTailPageAsync</c>. Data must round-trip
/// regardless of whether the surgical path or the rebuild fallback
/// actually runs.
/// </summary>
public sealed class IndexSurgicalCascadingIntermediateCollapseTests
{
    private static readonly string[] CompositeKeyColumns = ["K1", "K2"];

    [Fact]
    public async Task DeepTree_DeleteEmptiesEntireMidIntermediateSubtree_DataRoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        const int rowCount = 600;
        const int leftSubtreeRows = 60;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("Tag", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                int tag = i < leftSubtreeRows ? 9 : (i % 5);
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), tag];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Tag", 9, ct);
            Assert.Equal(leftSubtreeRows, deleted);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(rowCount - leftSubtreeRows, dt!.Rows.Count);

        var keyPairs = dt.Rows.Cast<DataRow>()
            .Select(r => ((string)r["K1"], (string)r["K2"]))
            .ToHashSet();
        Assert.Equal(rowCount - leftSubtreeRows, keyPairs.Count);
        for (int i = leftSubtreeRows; i < rowCount; i++)
        {
            Assert.Contains((BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M')), keyPairs);
        }

        foreach (DataRow r in dt.Rows)
        {
            Assert.NotEqual(9, (int)r["Tag"]);
        }
    }

    [Fact]
    public async Task DeepTree_BulkDeleteAcrossEntireSubtree_DataRoundTrips()
    {
        // 1500 rows / ~7 entries-per-leaf gives a 3-level tree. We bulk-
        // delete the dominant Tag value (~1475 rows) leaving only 25
        // scattered survivors. This exercises the cascading-underflow
        // shape: every leaf either empties completely or loses most of
        // its entries, so the surgical maintenance path needs to either
        // collapse entire subtrees in-place or fall back to the bulk
        // rebuild — either way the resulting index must be readable.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("Tag", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var rows = new object[1500][];
            for (int i = 0; i < 1500; i++)
            {
                int tag = (i % 60 == 0) ? 2 : 1;
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), tag];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.DeleteRowsAsync("T", "Tag", 1, ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(25, dt!.Rows.Count);

        var keyPairs = dt.Rows.Cast<DataRow>()
            .Select(r => ((string)r["K1"], (string)r["K2"]))
            .ToHashSet();
        for (int i = 0; i < 1500; i += 60)
        {
            Assert.Contains((BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M')), keyPairs);
        }
    }

    [Fact]
    public async Task DeepTree_DeleteThenReinsertAcrossCollapsedSubtree_DataRoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        const int rowCount = 600;
        const int leftSubtreeRows = 60;
        const int reinsertCount = 25;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("K1", typeof(string), maxLength: 255),
                    new ColumnDefinition("K2", typeof(string), maxLength: 255),
                    new ColumnDefinition("Tag", typeof(int)),
                ],
                [new IndexDefinition("IX_K", CompositeKeyColumns)],
                ct);

            var rows = new object[rowCount][];
            for (int i = 0; i < rowCount; i++)
            {
                int tag = i < leftSubtreeRows ? 9 : (i % 5);
                rows[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), tag];
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.DeleteRowsAsync("T", "Tag", 9, ct);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            var reins = new object[reinsertCount][];
            for (int i = 0; i < reinsertCount; i++)
            {
                reins[i] = [BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M'), 7];
            }

            await writer.InsertRowsAsync("T", reins, ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(rowCount - leftSubtreeRows + reinsertCount, dt!.Rows.Count);

        var keyPairs = dt.Rows.Cast<DataRow>()
            .Select(r => ((string)r["K1"], (string)r["K2"]))
            .ToHashSet();

        for (int i = 0; i < reinsertCount; i++)
        {
            Assert.Contains((BuildKey(i, prefix: 'A'), BuildKey(i, prefix: 'M')), keyPairs);
        }

        foreach (DataRow r in dt.Rows)
        {
            Assert.NotEqual(9, (int)r["Tag"]);
        }
    }

    private static string BuildKey(int n, char prefix)
    {
        var sb = new StringBuilder(255);
        sb.Append(prefix);
        sb.Append(n.ToString("D8", System.Globalization.CultureInfo.InvariantCulture));
        for (int i = 0; i < 246; i++)
        {
            sb.Append((char)('A' + ((n + i) % 26)));
        }

        return sb.ToString();
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
