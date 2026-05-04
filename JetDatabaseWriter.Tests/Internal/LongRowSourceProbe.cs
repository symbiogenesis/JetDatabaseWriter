namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707
#pragma warning disable CA1303
#pragma warning disable CA1305
#pragma warning disable SA1600
#pragma warning disable SA1507
#pragma warning disable SA1515
#pragma warning disable SA1025

/// <summary>
/// One-shot diagnostic: dump source-text bytes around the chunk-1/chunk-2
/// boundary and the V2010 510-byte truncation point so we can verify what
/// chars Access actually encodes vs what our chunk2 source range produces.
/// </summary>
public sealed class LongRowSourceProbe
{
    [Fact(Skip = "Diagnostic probe — always Assert.Fail's to dump data; not a regression test.")]
    public async Task DumpV2010SuffixAnalysis()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var reader = await AccessReader.OpenAsync(TestDatabases.TestIndexCodesV2010, cancellationToken: ct);
        DataTable dt = await reader.ReadDataTableAsync("Table11", cancellationToken: ct);
        var ascLayout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        List<IndexEntry> ascKeys = await CollectAllLeafKeysAsync(reader, ascLayout, reader.PageSize, firstPage: 112, ct);

        GeneralLegacyTextIndexEncoder.CharHandler[] codes = GetGeneralCodes();
        GeneralLegacyTextIndexEncoder.CharHandler[] extCodes = GetGeneralExtCodes();

        var sb = new System.Text.StringBuilder();

        // Collect data for all 3 rows
        int[][] rowToLeaf = [[2, 2], [3, 4], [4, 3]];
        var rowData = new List<(int Ri, ushort Expected, byte[] Full, string Text)>();
        foreach (int[] pair in rowToLeaf)
        {
            int ri = pair[0];
            int li = pair[1];
            string v = (string)dt.Rows[ri]["data"];
            byte[] expected = ascKeys[li].Key;
            ushort expectedSuffix = (ushort)((expected[508] << 8) | expected[509]);
            byte[] full = GeneralLegacyTextIndexEncoder.EncodeWithTables(
                v[..Math.Min(255, v.Length)], true, codes, extCodes, maxEntryLength: 0);
            rowData.Add((ri, expectedSuffix, full, v));
            sb.AppendLine($"row[{ri}] expected=0x{expectedSuffix:X4} fullLen={full.Length} textLen={v.Length}");
        }

        // Prepare input candidates for each row
        var cp1252 = System.Text.Encoding.GetEncoding(1252);
        string[][] inputNames =
        [
            [
                "full[508..]", "full[510..]", "full[508..^1]",
                "text[255..] CP1252", "text[255..] UTF16LE", "text UTF16LE",
                "text[255..] upper CP1252", "text upper CP1252",
                "extras only", "unprint only", "extras+unprint",
            ],
        ];

        // Build inputs per row
        byte[][][] allInputs = new byte[rowData.Count][][];
        for (int r = 0; r < rowData.Count; r++)
        {
            var (_, _, full, text) = rowData[r];
            string remaining = text[255..];
            string upper = text.ToUpperInvariant();
            string remainUpper = upper[255..];


            // Extract extras and unprint from full entry.
            // After inline, full has: END_TEXT(01) + extras + section separators + unprint + END_EXTRA(00).
            // Find END_TEXT position (first 0x01 after inline).
            int endTextPos = -1;
            for (int i = 508; i < full.Length; i++)
            {
                if (full[i] == 0x01)
                {
                    endTextPos = i;
                    break;
                }
            }

            byte[] extras = endTextPos >= 0 && endTextPos + 1 < full.Length
                ? full[(endTextPos + 1)..^1] // from after END_TEXT to before final END_EXTRA
                : [];
            byte[] unprint = [];
            // Find unprint section: look for 01 01 01 pattern in extras area
            if (extras.Length > 3)
            {
                for (int i = 0; i < extras.Length - 2; i++)
                {
                    if (extras[i] == 0x01 && extras[i + 1] == 0x01)
                    {
                        unprint = extras[(i + 2)..];
                        extras = extras[..i];
                        break;
                    }
                }
            }

            allInputs[r] =
            [
                full[508..], full[510..], full[508..^1],
                cp1252.GetBytes(remaining), System.Text.Encoding.Unicode.GetBytes(remaining),
                System.Text.Encoding.Unicode.GetBytes(text),
                cp1252.GetBytes(remainUpper), cp1252.GetBytes(upper),
                extras, unprint, [.. extras, .. unprint],
            ];
        }


        // Approach 3: What if V2010 puts MORE inline bytes instead of END_TEXT + extras?
        // Check what inline bytes chars 255+ produce, and if they match the suffix.
        sb.AppendLine("\n--- Char-by-char inline analysis around position 508 ---");

        for (int r = 0; r < rowData.Count; r++)
        {
            var (ri, expSuffix, full, text) = rowData[r];
            sb.AppendLine($"\nrow[{ri}] expected=0x{expSuffix:X4}");

            int inlinePos = 1; // flag byte at position 0
            int lastCharBefore508 = -1;
            int firstCharAt508 = -1;

            for (int ci = 0; ci < Math.Min(text.Length, 300); ci++)
            {
                char c = text[ci];
                var ch = c <= (char)0x00FF ? codes[c] : extCodes[c - (char)0x0100];
                byte[]? inl = ch.GetInlineBytes(c);
                int inlLen = inl?.Length ?? 0;

                if (inlinePos + inlLen > 508 && firstCharAt508 < 0)
                {
                    firstCharAt508 = ci;
                }

                if (inlinePos <= 508)
                {
                    lastCharBefore508 = ci;
                }

                if (ci >= 250 && ci <= 260)
                {
                    sb.AppendLine($"  char[{ci}]='{c}' (0x{(int)c:X4}) inlinePos={inlinePos} inlLen={inlLen} inl={InlineHex(inl)}");
                }

                inlinePos += inlLen;
            }

            sb.AppendLine($"  lastCharBefore508={lastCharBefore508} firstCharAt508={firstCharAt508}");

            // Now rebuild: what if we continue inline past char 254 and truncate at 510?
            var buf = new System.Collections.Generic.List<byte>(512);
            buf.Add(0x7F);
            int charCount = 0;
            for (int ci = 0; ci < text.Length; ci++)
            {
                char c = text[ci];
                var ch = c <= (char)0x00FF ? codes[c] : extCodes[c - (char)0x0100];
                byte[]? inl = ch.GetInlineBytes(c);
                if (inl is not null)
                {
                    buf.AddRange(inl);
                }

                charCount++;
                if (buf.Count >= 510)
                {
                    break;
                }
            }

            sb.AppendLine($"  pure inline charsUsed={charCount} totalLen={buf.Count}");
            if (buf.Count >= 510)
            {
                // Truncate to 510
                ushort tail = (ushort)((buf[508] << 8) | buf[509]);
                sb.AppendLine($"  tail[508..509]=0x{tail:X4} match={tail == expSuffix}");
                sb.AppendLine($"  hex[506..509]={Convert.ToHexString(buf.GetRange(506, 4).ToArray())}");
            }
        }

        Assert.Fail(sb.ToString());
    }

    private static string ModeStr(int mode)
    {
        return mode switch
        {
            0 => "normal/init0",
            1 => "reflected/init0",
            2 => "normal/initFF",
            3 => "reflected/initFF",
            _ => "?",
        };
    }

    private static ushort ReflectU16(ushort v)
    {
        unchecked
        {
            ushort r = 0;
            for (int i = 0; i < 16; i++)
            {
                r = (ushort)((r << 1) | (v & 1));
                v >>= 1;
            }

            return r;
        }
    }

    private static ushort CrcGeneric(byte[] data, ushort poly, ushort reflectedPoly, ushort init, bool reflected)
    {
        unchecked
        {
            ushort crc = init;
            if (reflected)
            {
                foreach (byte b in data)
                {
                    crc ^= b;
                    for (int i = 0; i < 8; i++)
                    {
                        crc = (crc & 1) != 0
                            ? (ushort)((crc >> 1) ^ reflectedPoly)
                            : (ushort)(crc >> 1);
                    }
                }
            }
            else
            {
                foreach (byte b in data)
                {
                    crc ^= (ushort)(b << 8);
                    for (int i = 0; i < 8; i++)
                    {
                        crc = (crc & 0x8000) != 0
                            ? (ushort)((crc << 1) ^ poly)
                            : (ushort)(crc << 1);
                    }
                }
            }

            return crc;
        }
    }

    // Adler-16: mod 251 (largest prime < 256)
    private static ushort Adler16(byte[] data)
    {
        unchecked
        {
            uint a = 1, b = 0;
            foreach (byte d in data)
            {
                a = (a + d) % 251;
                b = (b + a) % 251;
            }

            return (ushort)((b << 8) | a);
        }
    }

    // MurmurHash3 32-bit → XOR-fold to 16 bits
    private static ushort Murmur3_16(byte[] data, uint seed)
    {
        unchecked
        {
            uint h = seed;
            int len = data.Length;
            int nblocks = len / 4;

            for (int i = 0; i < nblocks; i++)
            {
                uint k = BitConverter.ToUInt32(data, i * 4);
                k *= 0xcc9e2d51;
                k = (k << 15) | (k >> 17);
                k *= 0x1b873593;
                h ^= k;
                h = (h << 13) | (h >> 19);
                h = (h * 5) + 0xe6546b64;
            }

            int tail = nblocks * 4;
            uint k1 = 0;
            switch (len & 3)
            {
                case 3:
                    k1 ^= (uint)data[tail + 2] << 16;
                    goto case 2;
                case 2:
                    k1 ^= (uint)data[tail + 1] << 8;
                    goto case 1;
                case 1:
                    k1 ^= data[tail];
                    k1 *= 0xcc9e2d51;
                    k1 = (k1 << 15) | (k1 >> 17);
                    k1 *= 0x1b873593;
                    h ^= k1;
                    break;
            }

            h ^= (uint)len;
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;

            return (ushort)((h >> 16) ^ (h & 0xFFFF));
        }
    }

    private static string ExtractHex(byte[] data, int start, int end)
    {
        if (start >= data.Length)
        {
            return "(OOB)";
        }

        end = Math.Min(end, data.Length - 1);
        return Convert.ToHexString(data.AsSpan(start, end - start + 1));
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

    private static string InlineHex(byte[]? bytes)
        => bytes is null ? "(none)" : Convert.ToHexString(bytes);

    private static string ExtraHex(GeneralLegacyTextIndexEncoder.CharHandler ch)
    {
        byte[]? extra = ch.ExtraBytes;
        byte mod = ch.ExtraByteModifier;
        if (extra is not null)
        {
            return Convert.ToHexString(extra);
        }

        return mod != 0 ? $"mod={mod:X2}" : "(none)";
    }
}
