namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
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
/// Composite (multi-column) text-index encoder validation, mirroring the
/// shape of <see cref="GeneralLegacyEncoderFixtureTests"/> but targeting
/// Jackcess's <c>compIndexTest*</c> fixtures. For each multi-column index
/// whose key columns are <em>all</em> Text/Memo, this test re-encodes each
/// row's key tuple via <see cref="GeneralLegacyTextIndexEncoder"/>, sorts by
/// unsigned byte order, and compares positionally against the on-disk leaf
/// keys.
/// <para>
/// On a passing run this proves that the encoder concatenates per-column
/// key blocks in the expected order. On a failing run it surfaces — like the
/// aggregate diagnostic — the first <see cref="MaxDetailRowsPerIndex"/>
/// mismatches per index so the failure isn't lost behind a bail-on-first
/// assertion.
/// </para>
/// </summary>
public sealed class CompositeTextIndexFixtureTests
{
    private const int MaxDetailRowsPerIndex = 5;

    public static TheoryData<string> Fixtures => new()
    {
        // V1997 Jet3 layout excluded — descent helper is hard-coded for the
        // Jet4 layout (see GeneralLegacyEncoderFixtureTests for the same
        // exclusion rationale).
        TestDatabases.CompIndexTestV2000,
        TestDatabases.CompIndexTestV2003,
        TestDatabases.CompIndexTestV2007,
        TestDatabases.CompIndexTestV2010,
    };

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task CompositeTextIndexes_LeafMatchesEncoder(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        var failures = new StringBuilder();
        int indexesValidated = 0;
        int keysValidated = 0;

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            List<ColumnMetadata> cols = await reader.GetColumnMetadataAsync(tableName, ct);
            var colByName = cols.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
            foreach (IndexMetadata index in indexes)
            {
                if (index.Columns.Count < 2 || index.IsForeignKey || index.FirstDp <= 0)
                {
                    continue;
                }

                if (!index.Columns.All(c => colByName.TryGetValue(c.Name, out ColumnMetadata? cm)
                                            && cm.ClrType == typeof(string)))
                {
                    continue;
                }

                List<byte[]> onDiskKeys = await CollectAllLeafKeysAsync(reader, layout, pageSize, index.FirstDp, ct);

                DataTable dt = await reader.ReadDataTableAsync(tableName, cancellationToken: ct);
                var encoded = new List<(string Repr, byte[] Key)>(dt.Rows.Count);
                foreach (DataRow row in dt.Rows)
                {
                    bool allNull = true;
                    var perCol = new List<byte[]>(index.Columns.Count);
                    var repr = new StringBuilder();
                    foreach (IndexColumnReference keyCol in index.Columns)
                    {
                        object boxed = row[keyCol.Name];
                        string? v = boxed is DBNull ? null : (string?)boxed;
                        if (v is not null)
                        {
                            allNull = false;
                        }

                        if (repr.Length > 0)
                        {
                            repr.Append('|');
                        }

                        repr.Append(v ?? "<null>");
                        perCol.Add(GeneralLegacyTextIndexEncoder.Encode(v, keyCol.IsAscending));
                    }

                    if (allNull && index.IgnoreNulls)
                    {
                        continue;
                    }

                    int total = perCol.Sum(p => p.Length);
                    byte[] full = new byte[total];
                    int off = 0;
                    foreach (byte[] p in perCol)
                    {
                        Buffer.BlockCopy(p, 0, full, off, p.Length);
                        off += p.Length;
                    }

                    encoded.Add((repr.ToString(), full));
                }

                encoded.Sort((a, b) => CompareBytesUnsigned(a.Key, b.Key));

                int detailReported = 0;
                if (encoded.Count != onDiskKeys.Count)
                {
                    string countLine = FormattableString.Invariant(
                        $"  {tableName}.{index.Name}: count mismatch encoded={encoded.Count} onDisk={onDiskKeys.Count}\n");
                    failures.Append(countLine);
                }

                int compareCount = Math.Min(encoded.Count, onDiskKeys.Count);
                for (int i = 0; i < compareCount; i++)
                {
                    if (encoded[i].Key.SequenceEqual(onDiskKeys[i]))
                    {
                        keysValidated++;
                        continue;
                    }

                    if (detailReported < MaxDetailRowsPerIndex)
                    {
                        string keyPreview = Truncate(encoded[i].Repr, 80);
                        string expHex = Convert.ToHexString(onDiskKeys[i]);
                        string actHex = Convert.ToHexString(encoded[i].Key);
                        string detailLine = FormattableString.Invariant(
                            $"  {tableName}.{index.Name}[{i}] key=\"{keyPreview}\" expected={expHex} actual={actHex}\n");
                        failures.Append(detailLine);
                        detailReported++;
                    }
                }

                indexesValidated++;
            }
        }

        if (indexesValidated == 0)
        {
            // The Jackcess compIndex fixtures historically use mixed-type
            // composites (text + numeric) rather than text-only composites,
            // so an all-text filter may legitimately match zero indexes in
            // some fixtures. Surface this as Skip rather than fail so the
            // test stays informative without flagging a false positive.
            Assert.Skip($"No multi-column text-only indexes in '{fixturePath}'.");
        }

        if (failures.Length > 0)
        {
            Assert.Fail($"Composite text-index encoder mismatches in '{fixturePath}':\n{failures}");
        }

        Assert.True(keysValidated > 0, $"No leaf keys validated in '{fixturePath}'.");
    }

    private static string Truncate(string s, int max)
        => s.Length <= max ? s : s[..max] + "...(+" + (s.Length - max).ToString(CultureInfo.InvariantCulture) + ")";

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
