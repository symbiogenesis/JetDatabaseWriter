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
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Focused validation companion to <see cref="GeneralEncoderFixtureTests"/>
/// for the V2010 long-row stress tables (<c>Table11</c> / <c>Table11_desc</c>).
/// <para>
/// The aggregate fixture test now validates these tables byte-exactly. This
/// class keeps the long-row-specific invariants and expected suffix order close
/// to the reverse-engineering notes.
/// </para>
/// </summary>
public sealed class GeneralEncoderLongRowPrefixTests
{
    private enum LongRowValidationMode
    {
        PrefixOnly,
        FullKey,
    }

    /// <summary>
    /// Number of prefix bytes used by the historical partial-regression test.
    /// The remaining <c>510 - PrefixMatchLength</c> bytes carry the ACE suffix.
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

    public static TheoryData<string, string[]> LongRowSuffixExpectations => new()
    {
        { "Table11", ["43EC", "1DAC", "A22D"] },
        { "Table11_desc", ["37DD", "C1A1", "9A4E"] },
    };

    public static TheoryData<string, bool, char, char, char, string> DaoDerivedLongRowSuffixSamples => new()
    {
        { "plain", true, 'a', 'a', 'd', "77A5" },
        { "plain", false, 'b', ' ', ' ', "FF00" },
        { "auxiliary", true, 'a', ' ', ' ', "3404" },
        { "auxiliary", false, 'a', 'a', ' ', "CAC9" },
        { "row10", true, 'j', ' ', 'b', "DF46" },
        { "row10", true, ' ', ' ', ' ', "173E" },
        { "row11", false, 'j', ' ', 'c', "01F9" },
        { "row12", true, ' ', ' ', ' ', "1D58" },
        { "row12", false, 'j', 'a', ' ', "B2B4" },
    };

    [Theory]
    [MemberData(nameof(LongRowTables))]
    public async Task LongRowStressTable_FirstPrefixBytesMatchEncoderOutput(
        string fixturePath,
        string tableName)
        => await ValidateLongRowStressTableAsync(
            fixturePath,
            tableName,
            LongRowValidationMode.PrefixOnly);

    [Theory]
    [MemberData(nameof(LongRowTables))]
    public async Task LongRowStressTable_AllBytesMatchEncoderOutput_WhenSuffixAlgorithmIsImplemented(
        string fixturePath,
        string tableName)
        => await ValidateLongRowStressTableAsync(
            fixturePath,
            tableName,
            LongRowValidationMode.FullKey);

    [Theory]
    [MemberData(nameof(LongRowSuffixExpectations))]
    public async Task LongRowStressTable_OnDiskSuffixBytesMatchKnownFixtureValues(
        string tableName,
        string[] expectedSuffixes)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var reader = await AccessReader.OpenAsync(
            TestDatabases.TestIndexCodesV2010,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        var layout =
            IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);

        var indexes = await reader.ListIndexesAsync(tableName, ct);
        var dataIndex = Assert.Single(indexes, candidateIndex =>
            candidateIndex.Columns.Count == 1
            && !candidateIndex.IsForeignKey
            && candidateIndex.FirstDp > 0
            && candidateIndex.Columns[0].Name.Equals("data", StringComparison.OrdinalIgnoreCase));

        var onDiskKeys = await CollectAllLeafKeysAsync(
            reader,
            layout,
            reader.PageSize,
            dataIndex.FirstDp,
            ct);

        List<string> actualSuffixes = onDiskKeys
            .Where(key => key.Length == LongRowEntryLength)
            .Select(key => Convert.ToHexString(
                key.AsSpan(PrefixMatchLength, LongRowEntryLength - PrefixMatchLength)))
            .ToList();

        Assert.Equal(expectedSuffixes, actualSuffixes);
    }

    [Theory]
    [MemberData(nameof(DaoDerivedLongRowSuffixSamples))]
    public async Task LongRowSuffix_DaoDerivedContributionTableSamples_MatchAccessSuffix(
        string context,
        bool ascending,
        char precedingBoundaryChar,
        char previousBoundaryChar,
        char boundaryChar,
        string expectedSuffix)
    {
        string text = await BuildDaoDerivedSampleTextAsync(
            context,
            precedingBoundaryChar,
            previousBoundaryChar,
            boundaryChar);

        byte[] key = GeneralTextIndexEncoder.Encode(text, ascending);
        string actualSuffix = Convert.ToHexString(key.AsSpan(PrefixMatchLength, 2));

        Assert.Equal(LongRowEntryLength, key.Length);
        Assert.Equal(expectedSuffix, actualSuffix);
    }

    private static async Task ValidateLongRowStressTableAsync(
        string fixturePath,
        string tableName,
        LongRowValidationMode validationMode)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        var layout =
            IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        var cols = await reader.GetColumnMetadataAsync(tableName, ct);
        var colByName = cols.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

        var indexes = await reader.ListIndexesAsync(tableName, ct);

        int indexesValidated = 0;
        int keysValidated = 0;
        int longRowKeysSeen = 0;

        foreach (var index in indexes)
        {
            if (index.Columns.Count != 1 || index.IsForeignKey || index.FirstDp <= 0)
            {
                continue;
            }

            var keyCol = index.Columns[0];
            if (!colByName.TryGetValue(keyCol.Name, out var colMeta)
                || colMeta.ClrType != typeof(string))
            {
                continue;
            }

            var onDiskKeys = await CollectAllLeafKeysAsync(
                reader, layout, pageSize, index.FirstDp, ct);

            var dt = await reader.ReadDataTableAsync(tableName, cancellationToken: ct);
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
                // and long-row entries pinned at 510 bytes.
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

                if (validationMode == LongRowValidationMode.FullKey)
                {
                    if (!actual.SequenceEqual(expected))
                    {
                        Assert.Fail(
                            $"Encoder/leaf byte mismatch on long-row entry at position {i} "
                            + $"in {tableName}.{index.Name} (column '{keyCol.Name}', "
                            + $"ascending={keyCol.IsAscending}, fixture='{fixturePath}'). "
                            + $"value=\"{encoded[i].Value}\". "
                            + $"expected={Convert.ToHexString(expected)} "
                            + $"actual={Convert.ToHexString(actual)}");
                    }
                }
                else if (!actual.AsSpan(0, PrefixMatchLength)
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

    private static async Task<string> BuildDaoDerivedSampleTextAsync(
        string context,
        char precedingBoundaryChar,
        char previousBoundaryChar,
        char boundaryChar)
    {
        char[] chars = context switch
        {
            "plain" => CreateFilledText('a'),
            "auxiliary" => CreateAuxiliaryText(),
            _ => (await ReadTemplateTextAsync(context)).ToCharArray(),
        };

        chars[252] = precedingBoundaryChar;
        chars[253] = previousBoundaryChar;
        chars[254] = boundaryChar;
        return new string(chars);
    }

    private static char[] CreateFilledText(char value) =>
        Enumerable.Repeat(value, 360).ToArray();

    private static char[] CreateAuxiliaryText()
    {
        char[] chars = CreateFilledText('a');
        chars[12] = '\u00C1';
        chars[25] = '\u00ED';
        chars[86] = '-';
        chars[102] = '-';
        return chars;
    }

    private static async Task<string> ReadTemplateTextAsync(string rowName)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var reader = await AccessReader.OpenAsync(
            TestDatabases.TestIndexCodesV2010,
            new AccessReaderOptions { UseLockFile = false },
            ct);
        var dataTable = await reader.ReadDataTableAsync("Table11", cancellationToken: ct);
        var row = dataTable.Rows
            .Cast<DataRow>()
            .Single(row => string.Equals((string)row["name"], rowName, StringComparison.OrdinalIgnoreCase));
        return (string)row["data"];
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

            var entries =
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

            var entries = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
            foreach (var e in entries)
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
    /// (the proprietary suffix at <c>[508..509]</c> would otherwise perturb the
    /// order). Ties on the prefix fall back to full-length unsigned compare,
    /// keeping the sort total — the on-disk keys break ties identically since
    /// Access's own suffix is a deterministic function of the entry body.
    /// </summary>
    /// <param name="a">The first value to compare.</param>
    /// <param name="b">The second value or byte buffer.</param>
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
