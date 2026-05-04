namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707
#pragma warning disable CA1303
#pragma warning disable CA1305
#pragma warning disable SA1600

/// <summary>
/// One-shot diagnostic: dump source-text bytes around the chunk-1/chunk-2
/// boundary and the V2010 510-byte truncation point so we can verify what
/// chars Access actually encodes vs what our chunk2 source range produces.
/// </summary>
public sealed class LongRowSourceProbe
{
    [Fact]
    public async Task DumpV2010LongRowSource()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var reader = await AccessReader.OpenAsync(TestDatabases.TestIndexCodesV2010, cancellationToken: ct);
        DataTable dt = await reader.ReadDataTableAsync("Table11", cancellationToken: ct);
        var layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        List<IndexEntry> keys = await CollectAllLeafKeysAsync(reader, layout, reader.PageSize, firstPage: 112, ct);
        var sb = new System.Text.StringBuilder();
        for (int rowIdx = 2; rowIdx <= 4; rowIdx++)
        {
            object o = dt.Rows[rowIdx]["data"];
            if (o is not string v)
            {
                continue;
            }

            sb.AppendLine($"row[{rowIdx}] len={v.Length}");
            sb.Append("  positions 0..15:");
            for (int i = 0; i <= 15 && i < v.Length; i++)
            {
                sb.Append($" [{i}]={(int)v[i]:X2}({Show(v[i])})");
            }

            sb.AppendLine();
            sb.Append("  positions 175..185:");
            for (int i = 175; i <= 185 && i < v.Length; i++)
            {
                sb.Append($" [{i}]={(int)v[i]:X2}({Show(v[i])})");
            }

            sb.AppendLine();
            sb.Append("  positions 250..260:");
            for (int i = 250; i <= 260 && i < v.Length; i++)
            {
                sb.Append($" [{i}]={(int)v[i]:X2}({Show(v[i])})");
            }

            sb.AppendLine();

            IndexEntry expectedEntry = keys[rowIdx];
            byte[] expected = expectedEntry.Key;
            byte[] actual = GeneralTextIndexEncoder.Encode(v, ascending: true);
            GeneralLegacyTextIndexEncoder.CharHandler[] codes = GetGeneralCodes();
            GeneralLegacyTextIndexEncoder.CharHandler[] extCodes = GetGeneralExtCodes();
            byte[] uncappedFull = GeneralLegacyTextIndexEncoder.EncodeWithTables(
                v,
                ascending: true,
                codes,
                extCodes,
                maxEntryLength: 0);
            int firstDiff = FirstDiff(expected, actual);
            sb.AppendLine($"  expected len={expected.Length} actual len={actual.Length} firstDiff={firstDiff}");
            sb.AppendLine($"  expected tail={Tail(expected, 16)}");
            sb.AppendLine($"  actual   tail={Tail(actual, 16)}");
            sb.AppendLine($"  row ptr   dp=0x{expectedEntry.DataPage:X6} dr=0x{expectedEntry.DataRow:X2} tail4={Tail(expected, 4)}");
            sb.AppendLine($"  uncapped len={uncappedFull.Length} truncTail={Tail(uncappedFull[..Math.Min(expected.Length, uncappedFull.Length)], 16)} omitted={Convert.ToHexString(uncappedFull.AsSpan(Math.Min(expected.Length, uncappedFull.Length)))}");

            if (rowIdx == 2 || rowIdx == 3 || rowIdx == 4)
            {
                // Test varying char prefixes to find which count matches expected up to last 2 bytes
                int bestPrefix = -1;
                int bestDiff = int.MaxValue;
                for (int prefixLen = 248; prefixLen <= 270; prefixLen++)
                {
                    byte[] prefixEnc = GeneralTextIndexEncoder.Encode(v[..Math.Min(prefixLen, v.Length)], ascending: true);
                    byte[] uncapped = GeneralLegacyTextIndexEncoder.EncodeWithTables(
                        v[..Math.Min(prefixLen, v.Length)],
                        ascending: true,
                        codes,
                        extCodes,
                        maxEntryLength: 0);
                    byte[] truncated = uncapped.Length > expected.Length ? uncapped[..expected.Length] : uncapped;
                    int truncatedDiff = FirstDiff(expected, truncated);
                    if (truncatedDiff >= 0 && truncatedDiff < bestDiff)
                    {
                        bestDiff = truncatedDiff;
                        bestPrefix = prefixLen;
                    }

                    sb.AppendLine(
                        $"  prefix {prefixLen,3}: cappedLen={prefixEnc.Length} cappedDiff={FirstDiff(expected, prefixEnc),3} cappedTail={Tail(prefixEnc, 8)} uncappedLen={uncapped.Length} truncDiff={truncatedDiff,3} uncappedTail={Tail(uncapped, 8)} chars={DescribeChars(v, prefixLen)}");
                }

                sb.AppendLine($"  best truncated-prefix diff: prefix={bestPrefix} diff={bestDiff}");
            }
        }

        Assert.Fail(sb.ToString());
    }

    private static async Task<List<IndexEntry>> CollectAllLeafKeysAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long firstPage,
        CancellationToken ct)
    {
        long current = firstPage;
        var result = new List<IndexEntry>();
        while (current != 0)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            List<IndexEntry> entries = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
            result.AddRange(entries);

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    private static int FirstDiff(byte[] expected, byte[] actual)
    {
        int common = Math.Min(expected.Length, actual.Length);
        for (int i = 0; i < common; i++)
        {
            if (expected[i] != actual[i])
            {
                return i;
            }
        }

        return expected.Length == actual.Length ? -1 : common;
    }

    private static string Tail(byte[] bytes, int count)
        => Convert.ToHexString(bytes.AsSpan(Math.Max(0, bytes.Length - count)));

    private static string DescribeChars(string value, int prefixLen)
    {
        int start = Math.Max(0, prefixLen - 4);
        int end = Math.Min(value.Length, prefixLen + 1);
        var sb = new System.Text.StringBuilder();
        for (int i = start; i < end; i++)
        {
            if (sb.Length > 0)
            {
                sb.Append(' ');
            }

            sb.Append('[');
            sb.Append(i);
            sb.Append("]=");
            sb.Append((int)value[i]);
            sb.Append('(');
            sb.Append(Show(value[i]));
            sb.Append(')');
        }

        return sb.ToString();
    }

    private static GeneralLegacyTextIndexEncoder.CharHandler[] GetGeneralCodes()
        => GetLazyField<GeneralLegacyTextIndexEncoder.CharHandler[]>(typeof(GeneralTextIndexEncoder), "Codes");

    private static GeneralLegacyTextIndexEncoder.CharHandler[] GetGeneralExtCodes()
        => GetLazyField<GeneralLegacyTextIndexEncoder.CharHandler[]>(typeof(GeneralTextIndexEncoder), "ExtCodes");

    private static T GetLazyField<T>(Type type, string name)
    {
        FieldInfo field = type.GetField(name, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Missing field {type.FullName}.{name}.");
        object boxed = field.GetValue(null)
            ?? throw new InvalidOperationException($"Null field {type.FullName}.{name}.");
        var lazy = Assert.IsType<Lazy<T>>(boxed);
        return lazy.Value;
    }

    private static string Show(char c)
        => c < 0x20 ? $"\\x{(int)c:X2}" : c.ToString();
}
