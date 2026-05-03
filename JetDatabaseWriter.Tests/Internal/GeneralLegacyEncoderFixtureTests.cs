namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-driven validation of <see cref="GeneralLegacyTextIndexEncoder"/>.
/// For each Jackcess <c>testIndexCodes*</c> fixture, walks every Text/Memo
/// single-column index B-tree on disk, then re-encodes each indexed value
/// with our encoder and asserts byte equality against the on-disk leaf keys.
/// <para>
/// This is the closest analogue in this repository of Jackcess's own
/// <c>IndexCodesTest</c>: the upstream test reads index entries from these
/// same fixtures via reflection on private cursor state and compares them to
/// keys it constructs by re-running its encoder. The fixtures were authored
/// by Microsoft Access itself, so byte-exact agreement here proves the
/// encoder's per-codepoint tables AND the framing/extra/unprintable/crazy
/// state machine match the on-disk Access format — closing the validation
/// gap that <c>GeneralLegacyTextIndexEncoder</c>'s class doc-comment calls
/// out.
/// </para>
/// </summary>
public sealed class GeneralLegacyEncoderFixtureTests
{
    public static TheoryData<string> Fixtures => new()
    {
        // Scope:
        //  • V1997 (Jet3) excluded: this helper's B-tree descent walks
        //    Jet4-layout pages only.
        //  • V2010 excluded: default text-index sort order is "General"
        //    (post-2010), not "General Legacy" — the encoder under test.
        TestDatabases.TestIndexCodesV2000,
        TestDatabases.TestIndexCodesV2003,
        TestDatabases.TestIndexCodesV2007,
    };

    // Tables intentionally containing values whose encoded prefix exceeds
    // the indexed-text length cap (127 chars in Jackcess / our encoder).
    // Upstream Jackcess explicitly skips them too (see
    // `IndexCodesTest.findRow`'s "TODO long rows not handled completely"
    // workaround). Tracked in <c>docs/design/test-coverage-gaps.md</c> §1.1
    // (canonical home for the upstream long-row TODO cross-references).
    private static readonly HashSet<string> LongRowStressTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "Table11",
        "Table11_desc",
    };

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task TextSingleColumnIndexes_OnDiskLeavesMatchEncoderOutput(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        int totalIndexesValidated = 0;
        int totalKeysValidated = 0;

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            if (LongRowStressTables.Contains(tableName))
            {
                continue;
            }

            List<ColumnMetadata> cols = await reader.GetColumnMetadataAsync(tableName, ct);
            var colByName = cols.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
            foreach (IndexMetadata index in indexes)
            {
                // Restrict to single-column Text/Memo indexes so the leaf key
                // is exactly the per-column entry block produced by the
                // encoder under test (no composite-key concatenation to factor
                // in). Skip foreign-key surrogates and indexes whose backing
                // real-idx slot was empty (FirstDp == 0).
                if (index.Columns.Count != 1 || index.IsForeignKey || index.FirstDp <= 0)
                {
                    continue;
                }

                IndexColumnReference keyCol = index.Columns[0];
                if (!colByName.TryGetValue(keyCol.Name, out ColumnMetadata? colMeta)
                    || colMeta.ClrType != typeof(string))
                {
                    continue;
                }

                List<byte[]> onDiskKeys = await CollectAllLeafKeysAsync(reader, layout, pageSize, index.FirstDp, ct);

                // Read the table and pull this column's values.
                DataTable dt = await reader.ReadDataTableAsync(tableName, cancellationToken: ct);
                var values = new List<string?>(dt.Rows.Count);
                foreach (DataRow row in dt.Rows)
                {
                    object boxed = row[keyCol.Name];
                    string? v = boxed is DBNull ? null : (string?)boxed;

                    // Indexes with the IGNORE_NULLS flag (`flags & 0x02`) omit
                    // null-keyed rows from their B-tree; mirror that filter
                    // here so the encoded set matches the on-disk leaf set.
                    if (v is null && index.IgnoreNulls)
                    {
                        continue;
                    }

                    values.Add(v);
                }

                // Encode each value the way IndexKeyEncoder routes Text/Memo,
                // pair with its source value for diagnostics, then sort by
                // unsigned byte order — the on-disk leaves are also stored in
                // unsigned-byte sort order, so a positional compare validates
                // byte equality for every entry without needing a
                // (dataPage, dataRow) → row reverse-lookup.
                var encoded = values
                    .Select(v => (Value: v, Key: GeneralLegacyTextIndexEncoder.Encode(v, keyCol.IsAscending)))
                    .ToList();
                encoded.Sort((a, b) => CompareBytesUnsigned(a.Key, b.Key));

                Assert.Equal(encoded.Count, onDiskKeys.Count);
                for (int i = 0; i < encoded.Count; i++)
                {
                    if (!encoded[i].Key.SequenceEqual(onDiskKeys[i]))
                    {
                        Assert.Fail(
                            $"Encoder/leaf byte mismatch at position {i} in "
                            + $"{tableName}.{index.Name} (column '{keyCol.Name}', "
                            + $"ascending={keyCol.IsAscending}, fixture='{fixturePath}'). "
                            + $"value=\"{encoded[i].Value}\". "
                            + $"expected={Hex(onDiskKeys[i])} actual={Hex(encoded[i].Key)}");
                    }
                }

                totalIndexesValidated++;
                totalKeysValidated += encoded.Count;
            }
        }

        // Sanity guard so a future fixture path / filter regression doesn't
        // silently turn this into a no-op.
        string noIndexesMsg = $"No single-column Text/Memo indexes found in '{fixturePath}'. Fixture path or index-filter changed?";
        Assert.True(totalIndexesValidated > 0, noIndexesMsg);
        string noKeysMsg = $"No leaf keys validated in '{fixturePath}'.";
        Assert.True(totalKeysValidated > 0, noKeysMsg);
    }

    /// <summary>
    /// Descends from <paramref name="rootPage"/> through Jet4/ACE intermediate
    /// (<c>0x03</c>) pages to the leftmost leaf (<c>0x04</c>) by following the
    /// first child pointer at each level, then sweeps <c>next_page</c> to the
    /// end of the leaf-sibling chain, decoding every entry's canonical key.
    /// </summary>
    private static async Task<List<byte[]>> CollectAllLeafKeysAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        // Descend to leftmost leaf.
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

            // First entry's child pointer = first 4 bytes after the entry's
            // key + 3-byte BE data page + 1-byte data row. To find the
            // entry's end we walk the §4.2 entry-start bitmask via
            // IndexLeafIncremental's intermediate decoder.
            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            if (entries.Count == 0)
            {
                throw new InvalidOperationException($"Intermediate page {current} has no entries.");
            }

            current = entries[0].ChildPage;
        }

        // Sweep leaf chain.
        var result = new List<byte[]>();
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
            foreach (IndexEntry e in entries)
            {
                result.Add(e.Key);
            }

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

    private static string Hex(byte[] b) => Convert.ToHexString(b);
}
