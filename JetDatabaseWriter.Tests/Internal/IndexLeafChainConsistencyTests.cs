namespace JetDatabaseWriter.Tests.Internal;

using System.Collections.Generic;
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
/// Validates doubly-linked list consistency of index leaf page chains.
/// Mirrors §1.3 gap: "Compare our next_page/prev_page chain against an
/// independent implementation." Rather than diffing against mdbtools, we
/// verify the internal invariant that <c>prev(next(page)) == page</c> for
/// every link in the chain — a bidirectional consistency check that catches
/// broken link updates and page corruption.
/// </summary>
public sealed class IndexLeafChainConsistencyTests
{
    public static TheoryData<string> Fixtures => new()
    {
        TestDatabases.IndexTestV2000,
        TestDatabases.IndexTestV2003,
        TestDatabases.IndexTestV2007,
        TestDatabases.IndexTestV2010,
        TestDatabases.CompIndexTestV2000,
        TestDatabases.CompIndexTestV2007,
        TestDatabases.CompIndexTestV2010,
        TestDatabases.TestIndexCodesV2000,
        TestDatabases.TestIndexCodesV2003,
        TestDatabases.TestIndexCodesV2007,
        TestDatabases.TestIndexCodesV2010,
    };

    /// <summary>
    /// For every scannable B-tree in the fixture, walks the leaf chain and
    /// asserts that <c>prev_page</c> of the next leaf page equals the
    /// current leaf page number. This validates doubly-linked list integrity
    /// which the existing unsigned-byte-order test does not check.
    /// </summary>
    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task LeafChain_PrevOfNext_EqualsCurrentPage(string fixturePath)
    {
        if (!File.Exists(fixturePath))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        int chainsChecked = 0;
        int linksChecked = 0;

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            IReadOnlyList<IndexMetadata> indexes;
            try
            {
                indexes = await reader.ListIndexesAsync(tableName, ct);
            }
            catch (System.NotSupportedException)
            {
                continue;
            }

            foreach (IndexMetadata index in indexes)
            {
                if (index.IsForeignKey || index.FirstDp <= 0)
                {
                    continue;
                }

                long firstLeaf = await FindFirstLeafPageAsync(reader, layout, pageSize, index.FirstDp, ct);
                if (firstLeaf <= 0)
                {
                    continue;
                }

                chainsChecked++;
                long current = firstLeaf;
                long visitGuard = 0;

                while (current != 0)
                {
                    if (++visitGuard > 100_000)
                    {
                        Assert.Fail($"Leaf chain exceeds visit guard in {tableName}.{index.Name} — possible cycle.");
                        return;
                    }

                    byte[] page = await reader.GetRawPageBytesAsync(current, ct);
                    (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);

                    if (next != 0)
                    {
                        byte[] nextPage = await reader.GetRawPageBytesAsync(next, ct);
                        (long prevOfNext, long _, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, nextPage);

                        string msg = $"Broken doubly-linked chain in {tableName}.{index.Name}: page {current} -> next {next}, but next's prev = {prevOfNext} (expected {current}). Fixture: '{fixturePath}'";
                        Assert.True(prevOfNext == current, msg);

                        linksChecked++;
                    }

                    current = next;
                }
            }
        }

        // At least one chain should have been checked — otherwise the fixture
        // has no scannable B-trees and this test is vacuously true.
        if (chainsChecked == 0)
        {
            Assert.Fail($"No scannable B-trees in '{fixturePath}'.");
        }
    }

    private static async Task<long> FindFirstLeafPageAsync(
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
                return current;
            }

            if (pageType != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                return 0;
            }

            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            if (entries.Count == 0)
            {
                return 0;
            }

            current = entries[0].ChildPage;
        }

        return 0;
    }
}
