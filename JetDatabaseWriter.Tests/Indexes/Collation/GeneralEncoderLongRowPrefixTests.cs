namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Partial-validation companion to <see cref="GeneralEncoderFixtureTests"/>
/// for the V2010 long-row stress tables (<c>Table11</c> / <c>Table11_desc</c>).
/// <para>
/// The full byte-exact assertion in <see cref="GeneralEncoderFixtureTests"/>
/// skips these two tables: the V2010 "General" sort-order long-row encoder
/// produces 510-byte entries whose first 508 bytes match the on-disk leaves
/// but whose final 2-byte suffix is computed by an algorithm that has so far
/// resisted reverse-engineering (see
/// <c>docs/design/long-row-index-encoding.md</c>).
/// </para>
/// <para>
/// This test locks in the partial result we <em>do</em> have: each leaf key
/// is exactly 510 bytes long and bytes <c>[0..507]</c> agree with the
/// encoder. This catches regressions in the body of the long-row encoder
/// (the part we understand) and will trip naturally if the suffix algorithm
/// is ever discovered — at which point this test should be deleted in favour
/// of removing <c>Table11</c> / <c>Table11_desc</c> from the
/// <c>LongRowStressTables</c> skip set in
/// <see cref="GeneralEncoderFixtureTests"/>.
/// </para>
/// </summary>
public sealed class GeneralEncoderLongRowPrefixTests
{
    /// <summary>
    /// Number of bytes at the head of each long-row entry that the V2010
    /// "General" sort-order encoder reproduces byte-exact. The remaining
    /// <c>510 - PrefixMatchLength</c> bytes carry the unknown suffix.
    /// </summary>
    private const int PrefixMatchLength = 508;

    /// <summary>
    /// Total fixed size, in bytes, of a V2010 long-row index entry (the hard
    /// cap applied by Access). See
    /// <see cref="GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010"/>.
    /// </summary>
    private const int LongRowEntryLength = 510;

    public static TheoryData<string, string> LongRowTables => new()
    {
        { TestDatabases.TestIndexCodesV2010, "Table11" },
        { TestDatabases.TestIndexCodesV2010, "Table11_desc" },
    };

    [Theory]
    [MemberData(nameof(LongRowTables))]
    public async Task LongRowStressTable_FirstPrefixBytesMatchEncoderOutput(
        string fixturePath,
        string tableName)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout =
            IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        List<ColumnMetadata> cols = await reader.GetColumnMetadataAsync(tableName, ct);
        var colByName = cols.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);

        int indexesValidated = 0;
        int keysValidated = 0;
        int longRowKeysSeen = 0;

        foreach (IndexMetadata index in indexes)
        {
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

            List<byte[]> onDiskKeys = await CollectAllLeafKeysAsync(
                reader, layout, pageSize, index.FirstDp, ct);

            DataTable dt = await reader.ReadDataTableAsync(tableName, cancellationToken: ct);
            var values = new List<string?>(dt.Rows.Count);
            foreach (DataRow row in dt.Rows)
            {
                object boxed = row[keyCol.Name];
                string? v = boxed is DBNull ? null : (string?)boxed;
                if (v is null && index.IgnoreNulls)
                {
                    continue;
                }

                values.Add(v);
            }

            var encoded = values
                .Select(v => (Value: v, Key: GeneralTextIndexEncoder.Encode(v, keyCol.IsAscending)))
                .ToList();
            encoded.Sort((a, b) => CompareBytesUnsignedPrefix(a.Key, b.Key));

            Assert.Equal(encoded.Count, onDiskKeys.Count);

            int longRowKeysSeenInIndex = 0;
            for (int i = 0; i < encoded.Count; i++)
            {
                byte[] expected = onDiskKeys[i];
                byte[] actual = encoded[i].Key;

                // Tables 11 / 11_desc contain a mix of NULL / short-text rows
                // (whose entries fit under the cap and validate byte-exact)
                // and long-row entries pinned at 510 bytes. We assert
                // byte-exact for the former and prefix-only for the latter,
                // which is where the unknown 2-byte suffix lives.
                if (expected.Length < LongRowEntryLength)
                {
                    if (!actual.SequenceEqual(expected))
                    {
                        Assert.Fail(
                            $"Encoder/leaf byte mismatch on short entry at position {i} "
                            + $"in {tableName}.{index.Name} (column '{keyCol.Name}', "
                            + $"ascending={keyCol.IsAscending}, fixture='{fixturePath}'). "
                            + $"value=\"{encoded[i].Value}\". "
                            + $"expected={Convert.ToHexString(expected)} "
                            + $"actual={Convert.ToHexString(actual)}");
                    }

                    continue;
                }

                string actualLenMsg =
                    $"Encoder output at position {i} in {tableName}.{index.Name} "
                    + $"has length {actual.Length}, expected {LongRowEntryLength}-byte "
                    + $"long-row entry to match the on-disk leaf. "
                    + $"value=\"{encoded[i].Value}\".";
                Assert.True(actual.Length == LongRowEntryLength, actualLenMsg);

                if (!actual.AsSpan(0, PrefixMatchLength)
                    .SequenceEqual(expected.AsSpan(0, PrefixMatchLength)))
                {
                    Assert.Fail(
                        $"Encoder/leaf prefix mismatch in first {PrefixMatchLength} bytes "
                        + $"at position {i} in {tableName}.{index.Name} "
                        + $"(column '{keyCol.Name}', ascending={keyCol.IsAscending}, "
                        + $"fixture='{fixturePath}'). value=\"{encoded[i].Value}\". "
                        + $"expected={Convert.ToHexString(expected.AsSpan(0, PrefixMatchLength))} "
                        + $"actual={Convert.ToHexString(actual.AsSpan(0, PrefixMatchLength))}");
                }

                longRowKeysSeenInIndex++;
            }

            indexesValidated++;
            keysValidated += encoded.Count;
            longRowKeysSeen += longRowKeysSeenInIndex;
        }

        string noIndexesMsg =
            $"No single-column Text/Memo indexes found on '{tableName}' in '{fixturePath}'. "
            + "Fixture or table layout changed?";
        Assert.True(indexesValidated > 0, noIndexesMsg);
        string noKeysMsg = $"No leaf keys validated on '{tableName}' in '{fixturePath}'.";
        Assert.True(keysValidated > 0, noKeysMsg);

        string noLongRowMsg =
            $"No 510-byte long-row entries observed across any index on '{tableName}' "
            + $"(fixture='{fixturePath}'); this test exists to lock in the partial "
            + "long-row encoder result and is meaningless without any.";
        Assert.True(longRowKeysSeen > 0, noLongRowMsg);
    }

    private static async Task<List<byte[]>> CollectAllLeafKeysAsync(
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

    /// <summary>
    /// Sorts encoder outputs by the prefix that we know matches Access on disk
    /// (the unknown suffix at <c>[508..509]</c> would otherwise perturb the
    /// order). Ties on the prefix fall back to full-length unsigned compare,
    /// keeping the sort total — the on-disk keys break ties identically since
    /// Access's own suffix is a deterministic function of the entry body.
    /// </summary>
    private static int CompareBytesUnsignedPrefix(byte[] a, byte[] b)
    {
        int prefix = Math.Min(Math.Min(a.Length, b.Length), PrefixMatchLength);
        for (int i = 0; i < prefix; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                return diff;
            }
        }

        int min = Math.Min(a.Length, b.Length);
        for (int i = prefix; i < min; i++)
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
