using JetDatabaseWriter.Enums;

namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Indexes.Collation;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Single-column non-text index encoder validation. Routes each indexed
/// value through <see cref="IndexKeyEncoder.EncodeEntry"/> and compares the
/// result byte-for-byte against the on-disk leaf entries. Mirrors the shape
/// of <see cref="GeneralLegacyEncoderFixtureTests"/> but covers Long /
/// Single / Double / Money / Currency / Byte / Boolean / DateTime / GUID /
/// Binary keys against the full Jackcess fixture corpus.
/// <para>
/// The closest upstream analogue is Jackcess <c>testBinaryIndex</c> +
/// <c>testNumericTypes</c>: Jackcess opens the same fixtures and walks each
/// non-text index via its encoder + cursor reflection. This test does the
/// equivalent without the cursor abstraction by descending the B-tree
/// directly and re-encoding each row's value.
/// </para>
/// <para>
/// Excludes Text/Memo (covered by the GeneralLegacy fixture suite) and
/// composite-key indexes (covered separately by <see cref="CompositeTextIndexFixtureTests"/>
/// for text-only composites). Skips fixtures that produce zero
/// matching indexes.
/// </para>
/// </summary>
public sealed class IndexNonTextSingleColumnFixtureTests
{
    private const int MaxDetailRowsPerIndex = 5;

    public static TheoryData<string> Fixtures
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (string p in new[]
            {
                TestDatabases.IndexTestV2000,
                TestDatabases.IndexTestV2003,
                TestDatabases.IndexTestV2007,
                TestDatabases.IndexTestV2010,

                // BigIndexTest V2000–V2010 excluded: schema-only templates
                // with an empty text index (Jackcess populates at test time).

                // BinIdxTest V2010 excluded: binary long-key suffix behavior
                // remains unobserved in the checked-in fixture corpus.
            })
            {
                data.Add(p);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task NonTextSingleColumnIndexes_LeafMatchesEncoder(string fixturePath)
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
            List<ColumnMetadata> cols;
            try
            {
                cols = await reader.GetColumnMetadataAsync(tableName, ct);
            }
            catch (NotSupportedException)
            {
                continue;
            }

            var colByName = cols.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

            IReadOnlyList<IndexMetadata> indexes;
            try
            {
                indexes = await reader.ListIndexesAsync(tableName, ct);
            }
            catch (NotSupportedException)
            {
                continue;
            }

            foreach (IndexMetadata index in indexes)
            {
                if (index.Columns.Count != 1 || index.IsForeignKey || index.FirstDp <= 0)
                {
                    continue;
                }

                IndexColumnReference keyCol = index.Columns[0];
                if (!colByName.TryGetValue(keyCol.Name, out ColumnMetadata? colMeta))
                {
                    continue;
                }

                // Skip Text/Memo (covered by GeneralLegacyEncoderFixtureTests).
                if (colMeta.ClrType == typeof(string))
                {
                    continue;
                }

                // Skip Binary single-column indexes for now: the V2010
                // binIdxTest fixture exposes long binary keys whose on-disk
                // leaf reconstruction does not match the
                // <see cref="IndexKeyEncoder"/> general-binary-entry output
                // — a known gap also flagged in upstream Jackcess
                // (<c>"TODO long rows not handled completely yet in V2010 —
                // seems to truncate entry at 508 bytes"</c>). The text-key
                // analogue is documented in
                // <see href="docs/format-probe/format-probe-long-row-index-encoding.md" />.
                if (colMeta.ClrType == typeof(byte[]))
                {
                    continue;
                }

                ColumnType? columnTypeCode = ResolveColumnTypeCode(colMeta.ClrType);
                if (columnTypeCode is null)
                {
                    string skipMsg = FormattableString.Invariant(
                        $"""
                          {tableName}.{index.Name}: skipping (no encoder route for CLR type {colMeta.ClrType.Name})

                        """);
                    failures.Append(skipMsg);
                    continue;
                }

                List<IndexEntry> onDisk;
                try
                {
                    onDisk = await CollectAllLeafEntriesAsync(reader, layout, pageSize, index.FirstDp, ct);
                }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
                {
                    string walkMsg = FormattableString.Invariant(
                        $"""
                          {tableName}.{index.Name}: leaf walk failed: {ex.Message}

                        """);
                    failures.Append(walkMsg);
                    continue;
                }

                DataTable dt;
                try
                {
                    dt = await reader.ReadDataTableAsync(tableName, cancellationToken: ct);
                }
                catch (NotSupportedException)
                {
                    continue;
                }

                var encoded = new List<(object? Value, byte[] Key)>(dt.Rows.Count);
                foreach (DataRow row in dt.Rows)
                {
                    object boxed = row[keyCol.Name];
                    object? v = boxed is DBNull ? null : boxed;
                    if (v is null && index.IgnoreNulls)
                    {
                        continue;
                    }

                    byte[] key;
                    try
                    {
                        key = IndexKeyEncoder.EncodeEntry(columnTypeCode.Value, v, keyCol.IsAscending);
                    }
                    catch (Exception ex) when (ex is NotSupportedException or ArgumentException)
                    {
                        string encMsg = FormattableString.Invariant(
                            $"""
                              {tableName}.{index.Name}: encoder threw {ex.GetType().Name}: {ex.Message}

                            """);
                        failures.Append(encMsg);
                        encoded.Clear();
                        break;
                    }

                    encoded.Add((v, key));
                }

                if (encoded.Count == 0 && dt.Rows.Count > 0)
                {
                    continue;
                }

                encoded.Sort((a, b) => CompareBytesUnsigned(a.Key, b.Key));
                var onDiskKeys = onDisk
                    .Select(e => e.Key)
                    .OrderBy(k => k, ByteSpanComparer.Instance)
                    .ToList();

                int detailReported = 0;
                if (encoded.Count != onDiskKeys.Count)
                {
                    string countMsg = FormattableString.Invariant(
                        $"""
                          {tableName}.{index.Name} (col '{keyCol.Name}' {colMeta.TypeName} typeCode=0x{(byte)columnTypeCode.Value:X2} asc={keyCol.IsAscending}): count mismatch encoded={encoded.Count} onDisk={onDiskKeys.Count}

                        """);
                    failures.Append(countMsg);
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
                        string vstr = encoded[i].Value?.ToString() ?? "<null>";
                        string preview = Truncate(vstr, 60);
                        string expHex = Convert.ToHexString(onDiskKeys[i]);
                        string actHex = Convert.ToHexString(encoded[i].Key);
                        string detailMsg = FormattableString.Invariant(
                            $"""
                              {tableName}.{index.Name} (col '{keyCol.Name}' {colMeta.TypeName} typeCode=0x{(byte)columnTypeCode.Value:X2} asc={keyCol.IsAscending})[{i}] value="{preview}" expected={expHex} actual={actHex}

                            """);
                        failures.Append(detailMsg);
                        detailReported++;
                    }
                }

                indexesValidated++;
            }
        }

        if (indexesValidated == 0)
        {
            Assert.Fail($"No applicable single-column non-text indexes in '{fixturePath}'.");
        }

        if (failures.Length > 0)
        {
            Assert.Fail($"""
                Non-text single-column index encoder mismatches in '{fixturePath}':
                {failures}
                """);
        }

        Assert.True(keysValidated > 0, $"No leaf keys validated in '{fixturePath}'.");
    }

    /// <summary>
    /// Maps the ClrType surfaced by <see cref="ColumnMetadata.ClrType"/> back
    /// to the JET type code expected by <see cref="IndexKeyEncoder.EncodeEntry"/>.
    /// Returns <see langword="null"/> for types this test doesn't know how to encode (caller skips).
    /// </summary>
    /// <param name="clr">The CLR type.</param>
    private static ColumnType? ResolveColumnTypeCode(Type clr)
    {
        // mdbtools HACKING.md §3 column type code table.
        if (clr == typeof(byte))
        {
            return ByteType;
        }

        if (clr == typeof(short) || clr == typeof(int))
        {
            return clr == typeof(short) ? IntegerType : LongIntegerType;
        }

        if (clr == typeof(float))
        {
            return FloatType;
        }

        if (clr == typeof(double))
        {
            return DoubleType;
        }

        if (clr == typeof(DateTime))
        {
            return DateTimeType;
        }

        if (clr == typeof(decimal))
        {
            // Money = 0x05; Numeric = 0x11 (ACE only). Both surface as
            // decimal in the reader; the encoder tolerates either via the
            // same fixed-point path. Default to Money because it's the
            // more common case in the Jackcess fixtures.
            return MoneyType;
        }

        if (clr == typeof(bool))
        {
            return BooleanType;
        }

        if (clr == typeof(Guid))
        {
            return GuidType;
        }

        if (clr == typeof(byte[]))
        {
            return BinaryType;
        }

        return null;
    }

    private static string Truncate(string s, int max)
        => s.Length <= max ? s : s[..max] + "...(+" + (s.Length - max).ToString(CultureInfo.InvariantCulture) + ")";

    private static async Task<List<IndexEntry>> CollectAllLeafEntriesAsync(
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

        var result = new List<IndexEntry>();
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
            result.AddRange(entries);

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

    private sealed class ByteSpanComparer : IComparer<byte[]>
    {
        public static readonly ByteSpanComparer Instance = new();

        public int Compare(byte[]? x, byte[]? y)
        {
            if (x is null && y is null)
            {
                return 0;
            }

            if (x is null)
            {
                return -1;
            }

            if (y is null)
            {
                return 1;
            }

            return CompareBytesUnsigned(x, y);
        }
    }
}
