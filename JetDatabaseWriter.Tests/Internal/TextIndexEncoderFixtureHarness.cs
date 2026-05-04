namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Shared driver for fixture-driven byte-exact validation of the three
/// text-index sort-order encoders (General Legacy, General, General 97).
/// All three tests share the same per-fixture sweep — they differ only in
/// (a) which fixture set they run against and (b) which encoder produces
/// the comparison key.
/// </summary>
internal static class TextIndexEncoderFixtureHarness
{
    internal delegate byte[] EncodeText(string? value, bool ascending);

    /// <summary>
    /// Opens <paramref name="fixturePath"/>, walks every single-column
    /// non-FK Text/Memo index B-tree on disk, then re-encodes each indexed
    /// value with <paramref name="encode"/> and asserts byte equality
    /// against the on-disk leaf keys (sorted unsigned). Mirrors the original
    /// <c>GeneralLegacyEncoderFixtureTests</c> logic.
    /// </summary>
    public static async Task ValidateAsync(
        string fixturePath,
        EncodeText encode,
        IReadOnlyCollection<string>? skipTables = null,
        CancellationToken ct = default)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        int totalIndexesValidated = 0;
        int totalKeysValidated = 0;
        var skip = skipTables ?? Array.Empty<string>();

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            if (skip.Contains(tableName, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }

            List<ColumnMetadata> cols = await reader.GetColumnMetadataAsync(tableName, ct);
            var colByName = cols.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
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
                    .Select(v => (Value: v, Key: encode(v, keyCol.IsAscending)))
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
                            + $"expected={Convert.ToHexString(onDiskKeys[i])} "
                            + $"actual={Convert.ToHexString(encoded[i].Key)}");
                    }
                }

                totalIndexesValidated++;
                totalKeysValidated += encoded.Count;
            }
        }

        string noIndexesMsg =
            $"No single-column Text/Memo indexes found in '{fixturePath}'. "
            + "Fixture path or index-filter changed?";
        Assert.True(totalIndexesValidated > 0, noIndexesMsg);
        string noKeysMsg = $"No leaf keys validated in '{fixturePath}'.";
        Assert.True(totalKeysValidated > 0, noKeysMsg);
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
