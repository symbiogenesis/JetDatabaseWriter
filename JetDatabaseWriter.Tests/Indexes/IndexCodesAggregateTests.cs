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
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Indexes.Collation;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Sibling of <see cref="GeneralLegacyEncoderFixtureTests"/> that <em>does
/// not</em> bail on the first encoder/leaf mismatch. Instead it sweeps every
/// single-column Text/Memo index in every <c>testIndexCodes*</c> fixture and
/// produces a structured per-(table, index) summary of:
/// <list type="bullet">
///   <item>how many encoded values matched the on-disk leaf bytes</item>
///   <item>how many were length-mismatches vs byte-mismatches</item>
///   <item>up to <see cref="MaxDetailRowsPerIndex"/> example mismatches with
///         the offending value, expected hex, and actual hex</item>
///   <item>per-column metadata (<see cref="ColumnMetadata.MaxLength"/>,
///         <see cref="ColumnMetadata.TypeName"/>) — useful when triaging
///         whether a failure is sort-order routing (Text vs Memo, length
///         hints), composite-key, or surrogate-handler related</item>
/// </list>
/// <para>
/// The point is diagnostic coverage: when the per-fixture test in
/// <c>GeneralLegacyEncoderFixtureTests</c> fails on its first mismatch, this
/// test lets us see the <em>shape</em> of the failure (one column? one
/// fixture? one Memo table?) without 4 round-trips through the test runner.
/// </para>
/// </summary>
public sealed class IndexCodesAggregateTests
{
    private const int MaxDetailRowsPerIndex = 5;

    // V2010+ stores text-index keys via the General (full Unicode collation)
    // encoder, not GeneralLegacy. Validating those would require
    // GeneralIndexEncoder, which lives in a separate diagnostic test.
    public static TheoryData<string> Fixtures => new()
    {
        TestDatabases.TestIndexCodesV2000,
        TestDatabases.TestIndexCodesV2003,
        TestDatabases.TestIndexCodesV2007,
    };

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task AllSingleColumnTextIndexes_AggregateMatch(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        var report = new List<IndexReport>();

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
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

                List<byte[]> onDiskKeys = await CollectAllLeafKeysAsync(reader, layout, pageSize, index.FirstDp, ct);

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
                    .Select(v => (Value: v, Key: GeneralLegacyTextIndexEncoder.Encode(v, keyCol.IsAscending)))
                    .ToList();
                encoded.Sort((a, b) => CompareBytesUnsigned(a.Key, b.Key));

                IndexReport ir = new()
                {
                    Table = tableName,
                    Index = index.Name,
                    Column = keyCol.Name,
                    ColumnTypeName = colMeta.TypeName,
                    ColumnMaxLength = colMeta.MaxLength,
                    Ascending = keyCol.IsAscending,
                    OnDiskCount = onDiskKeys.Count,
                    EncodedCount = encoded.Count,
                };

                int compareCount = Math.Min(encoded.Count, onDiskKeys.Count);
                for (int i = 0; i < compareCount; i++)
                {
                    if (encoded[i].Key.SequenceEqual(onDiskKeys[i]))
                    {
                        ir.Matched++;
                        continue;
                    }

                    ir.Mismatched++;
                    if (encoded[i].Key.Length != onDiskKeys[i].Length)
                    {
                        ir.LengthMismatched++;
                    }

                    if (ir.Details.Count < MaxDetailRowsPerIndex)
                    {
                        ir.Details.Add(new MismatchDetail
                        {
                            Position = i,
                            Value = encoded[i].Value,
                            Expected = onDiskKeys[i],
                            Actual = encoded[i].Key,
                        });
                    }
                }

                report.Add(ir);
            }
        }

        // Surface a structured report whether or not anything failed; xUnit
        // captures stdout per test so the report is visible in test logs.
        TestContext.Current.SendDiagnosticMessage(BuildReport(fixturePath, report));

        // Sanity: must have exercised something so a future filter regression
        // can't silently neutralise this.
        Assert.NotEmpty(report);

        // Ignore mismatches on Memo-keyed indexes: Jackcess upstream has a
        // standing TODO ("long rows not handled completely yet … seems to
        // truncate entry at 508 bytes") for long-value index keys, and our
        // text encoder mirrors that limitation. Tracked in
        // <c>docs/design/test-coverage-gaps.md</c> §1.1 (canonical home for
        // the upstream long-row TODO cross-references).
        int totalMismatched = report
            .Where(r => !string.Equals(r.ColumnTypeName, "Memo", StringComparison.OrdinalIgnoreCase))
            .Sum(r => r.Mismatched);
        int totalCountMismatch = report
            .Where(r => !string.Equals(r.ColumnTypeName, "Memo", StringComparison.OrdinalIgnoreCase))
            .Count(r => r.OnDiskCount != r.EncodedCount);

        // Single aggregate assertion with the full report inline so the
        // failure message itself is the diagnostic.
        Assert.True(
            totalMismatched == 0 && totalCountMismatch == 0,
            BuildReport(fixturePath, report));
    }

    private static string BuildReport(string fixturePath, List<IndexReport> report)
    {
        var sb = new StringBuilder();
        sb.Append(CultureInfo.InvariantCulture, $"Encoder/leaf aggregate report for '{fixturePath}':\n");
        sb.Append(CultureInfo.InvariantCulture, $"  indexes scanned: {report.Count}\n");
        sb.Append(CultureInfo.InvariantCulture, $"  fully matched: {report.Count(r => r.Mismatched == 0 && r.OnDiskCount == r.EncodedCount)}\n");
        sb.Append(CultureInfo.InvariantCulture, $"  with byte mismatches: {report.Count(r => r.Mismatched > 0)}\n");
        sb.Append(CultureInfo.InvariantCulture, $"  with count mismatch: {report.Count(r => r.OnDiskCount != r.EncodedCount)}\n");
        foreach (IndexReport r in report.OrderByDescending(x => x.Mismatched).ThenBy(x => x.Table, StringComparer.Ordinal))
        {
            string maxLen = r.ColumnMaxLength?.ToString(CultureInfo.InvariantCulture) ?? "-";
            string indexLine = FormattableString.Invariant(
                $"  - {r.Table}.{r.Index} (col '{r.Column}' {r.ColumnTypeName} max={maxLen}, asc={r.Ascending}): matched={r.Matched} mismatched={r.Mismatched} (len-mismatch={r.LengthMismatched}) onDisk={r.OnDiskCount} encoded={r.EncodedCount}\n");
            sb.Append(indexLine);
            foreach (MismatchDetail d in r.Details)
            {
                string preview = d.Value is null ? "<null>" : Truncate(d.Value, 40);
                string expHex = Convert.ToHexString(d.Expected);
                string actHex = Convert.ToHexString(d.Actual);
                string detailLine = FormattableString.Invariant(
                    $"      [{d.Position}] value=\"{preview}\" expected({d.Expected.Length})={expHex} actual({d.Actual.Length})={actHex}\n");
                sb.Append(detailLine);
            }
        }

        return sb.ToString();
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

    private sealed class IndexReport
    {
        public string Table { get; init; } = string.Empty;

        public string Index { get; init; } = string.Empty;

        public string Column { get; init; } = string.Empty;

        public string ColumnTypeName { get; init; } = string.Empty;

        public int? ColumnMaxLength { get; init; }

        public bool Ascending { get; init; }

        public int OnDiskCount { get; init; }

        public int EncodedCount { get; init; }

        public int Matched { get; set; }

        public int Mismatched { get; set; }

        public int LengthMismatched { get; set; }

        public List<MismatchDetail> Details { get; } = [];
    }

    private sealed class MismatchDetail
    {
        public int Position { get; init; }

        public string? Value { get; init; }

        public byte[] Expected { get; init; } = [];

        public byte[] Actual { get; init; } = [];
    }
}
