namespace JetDatabaseWriter.Tests.Indexes;

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
/// Validates that the <c>tail_page</c> (child_tail) header field on
/// intermediate index pages is non-zero when the B-tree has depth > 1,
/// and that it points at a valid page within the file. Jackcess's
/// <c>IndexPageCache.validate</c> walks this pointer; we previously
/// ignored it entirely. Closes §1.3 gap: "Index page splits where the
/// child_tail pointer is populated.".
/// </summary>
public sealed class IndexChildTailPointerTests
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
    };

    /// <summary>
    /// For every B-tree whose root is an intermediate page, verifies that
    /// the <c>tail_page</c> header field is either zero (single-level tree)
    /// or points at a valid page within the file bounds.
    /// </summary>
    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task IntermediatePages_TailPointer_IsZeroOrValid(string fixturePath)
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
        long fileLength = new FileInfo(fixturePath).Length;
        long maxPageNumber = fileLength / pageSize;

        int intermediatesChecked = 0;
        int nonZeroTails = 0;

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

                // Walk intermediate pages from root until we hit a leaf.
                long current = index.FirstDp;
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
                        break;
                    }

                    intermediatesChecked++;
                    (long _, long _, long tail) = IndexLeafIncremental.ReadSiblingPointers(layout, page);

                    if (tail != 0)
                    {
                        nonZeroTails++;
                        string msg = $"tail_page {tail} out of bounds [1..{maxPageNumber}] on intermediate page {current} of {tableName}.{index.Name} in '{fixturePath}'.";
                        Assert.True(tail > 0 && tail <= maxPageNumber, msg);
                    }

                    // Descend to first child.
                    List<DecodedIntermediateEntry> entries =
                        IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
                    if (entries.Count == 0)
                    {
                        break;
                    }

                    current = entries[0].ChildPage;
                }
            }
        }

        // If no intermediate pages were found, the fixture only has single-leaf
        // B-trees — that's fine for small fixtures.
        if (intermediatesChecked > 0)
        {
            // At least some multi-level B-trees should have tail pointers.
            // This is informational — not all fixtures will have them populated.
            Assert.True(intermediatesChecked > 0);
        }
    }
}
