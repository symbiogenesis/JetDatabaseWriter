// Brute-force chunk boundary discovery for long-row index entries.
// Reads the testIndexCodes V2000/V2003/V2007/V2010 fixtures, finds the
// long-row leaf entries (Table11.DataIndex), splits on the format-specific
// 4-byte separator (08 07 08 04 for genleg, 07 09 07 06 for general), and
// for each chunk searches for the (K_start, K_end) source-character range
// whose inline-only encoding matches the chunk bytes exactly.
//
// Usage: dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-bisect
// Legacy: $env:DIAG_LONG_ROW_BISECT="1" ; dotnet run --project JetDatabaseWriter.FormatProbe

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;

internal static class LongRowBisect
{
    public static async Task<int> RunAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Long-row chunk-boundary bisection");
        sb.AppendLine();

        // Build inline-only encoders over the existing per-codepoint tables.
        InlineEncoder genlegEncoder = LoadEncoder("GeneralLegacyTextIndexEncoder");
        InlineEncoder genEncoder = LoadEncoder("GeneralTextIndexEncoder");

        var tasks = new (string Path, byte[] Sep, InlineEncoder Encoder)[]
        {
            (Path.Combine(fixturesDir, "Jackcess", "V2000", "testIndexCodesV2000.mdb"), [0x08, 0x07, 0x08, 0x04], genlegEncoder),
            (Path.Combine(fixturesDir, "Jackcess", "V2003", "testIndexCodesV2003.mdb"), [0x08, 0x07, 0x08, 0x04], genlegEncoder),
            (Path.Combine(fixturesDir, "Jackcess", "V2007", "testIndexCodesV2007.accdb"), [0x08, 0x07, 0x08, 0x04], genlegEncoder),
            (Path.Combine(fixturesDir, "Jackcess", "V2010", "testIndexCodesV2010.accdb"), [0x07, 0x09, 0x07, 0x06], genEncoder),
        };

        foreach (var t in tasks)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"## {Path.GetFileName(t.Path)} (separator: {Convert.ToHexString(t.Sep)})");
            sb.AppendLine();
            await BisectAsync(t.Path, t.Sep, t.Encoder, sb);
            sb.AppendLine();
        }

        Directory.CreateDirectory(Path.GetDirectoryName(outFile)!);
        await File.WriteAllTextAsync(outFile, sb.ToString());
        Console.WriteLine($"Wrote {outFile}");
        return 0;
    }

    private static readonly MethodInfo LoadCodesMethod = typeof(AccessReader).Assembly
        .GetType("JetDatabaseWriter.Indexes.Collation.GeneralLegacyTextIndexEncoder")!
        .GetMethod("LoadCodes", BindingFlags.NonPublic | BindingFlags.Static)!;

    private static InlineEncoder LoadEncoder(string typeName)
    {
        var suffix = typeName == "GeneralTextIndexEncoder" ? "gen" : "genleg";

        var codes = (object[])LoadCodesMethod.Invoke(null, [$"JetDatabaseWriter.IndexCodeTables.index_codes_{suffix}.txt.gz", (char)0x0000, (char)0x00FF])!;
        var ext = (object[])LoadCodesMethod.Invoke(null, [$"JetDatabaseWriter.IndexCodeTables.index_codes_ext_{suffix}.txt.gz", (char)0x0100, (char)0xFFFF])!;
        return new InlineEncoder(codes, ext);
    }

    private static async Task BisectAsync(string path, byte[] sep, InlineEncoder encoder, StringBuilder sb)
    {
        if (!File.Exists(path))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"_(missing)_");
            return;
        }

        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, CancellationToken.None);

        var layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        List<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Table11");
        int dataOrdinal = FindColumnOrdinal(columns, "data");
        var rowValues = new List<string?>();
        await foreach (string[] row in reader.RowsAsStrings("Table11", cancellationToken: CancellationToken.None))
        {
            rowValues.Add(dataOrdinal < row.Length ? row[dataOrdinal] : null);
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Table11");
        var idx = indexes.First(i => i.Columns.Count == 1 && i.Columns[0].Name == "data" && i.FirstDp > 0);
        bool asc = idx.Columns[0].IsAscending;

        // Collect leaves
        long current = idx.FirstDp;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, CancellationToken.None);
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            var iEntries = IndexLeafIncremental.DecodeIntermediateEntries(layout, page, reader.PageSize);
            current = iEntries[0].ChildPage;
        }

        var allKeys = new List<byte[]>();
        while (current != 0)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, CancellationToken.None);
            var entries = IndexLeafIncremental.DecodeEntries(layout, page, reader.PageSize);
            foreach (var e in entries)
            {
                allKeys.Add(e.Key);
            }

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        // For each row long enough (>127 chars), find its key by trying all leaves.
        var longRows = new List<(int Idx, string Val)>();
        for (int i = 0; i < rowValues.Count; i++)
        {
            if (rowValues[i] is { Length: > 127 } v)
            {
                longRows.Add((i, v));
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"Long rows: {longRows.Count}; long leaves: {allKeys.Count(k => k.Length > 50)}");
        sb.AppendLine();

        foreach (var (rowIdx, val) in longRows)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"### row[{rowIdx}] len={val.Length}");
            InlineEncodingCache inlineCache = encoder.Encode(val);

            // Show some specific source chars to diagnose chunk-boundary rule.
            for (int probe = 175; probe <= 185 && probe < val.Length; probe++)
            {
                char c = val[probe];
                sb.AppendLine(CultureInfo.InvariantCulture, $"  val[{probe}] = U+{(int)c:X4} ('{(char.IsControl(c) ? '?' : c)}')");
            }

            // Find which leaf this row belongs to: scan for one whose first chunk
            // matches one of the candidates.
            int matchedLeaf = -1;
            int matchedK = -1;
            byte[]? leafBytes = null;
            for (int leafIndex = 0; leafIndex < allKeys.Count; leafIndex++)
            {
                byte[] k = allKeys[leafIndex];
                if (k.Length < 200)
                {
                    continue;
                }

                // skip 1-byte flag
                int sepAt = IndexOf(k, sep, 1);
                if (sepAt < 0)
                {
                    continue;
                }

                int chunk1Length = sepAt - 1;
                for (int cand = 100; cand <= 200 && cand <= val.Length; cand++)
                {
                    if (inlineCache.SliceEqualsBytes(0, cand, k, 1, chunk1Length))
                    {
                        matchedLeaf = leafIndex;
                        matchedK = cand;
                        leafBytes = k;
                        break;
                    }
                }

                if (matchedLeaf >= 0)
                {
                    break;
                }
            }

            if (matchedLeaf < 0 || leafBytes is null)
            {
                // Maybe descending: also try complemented forms
                sb.AppendLine(CultureInfo.InvariantCulture, $"  no chunk-1 match found in {allKeys.Count} leaves (asc={asc}). Skipping.");
                sb.AppendLine();
                continue;
            }

            sb.AppendLine(CultureInfo.InvariantCulture, $"  chunk #1: K1={matchedK} (val[0..{matchedK}))  leafIdx={matchedLeaf} leafLen={leafBytes.Length}");

            // Now find chunk #2: bytes from after separator up to the END_TEXT (0x01).
            int sepIdx = IndexOf(leafBytes, sep, 1);
            int chunk2Start = sepIdx + sep.Length;

            // chunk #2 ends at next 0x01 (END_TEXT) — but need to be careful because
            // 0x01 can appear inside extras. The structure is:
            //   chunk1 | sep | chunk2 | END_TEXT (0x01) | extras | (END_TEXT END_TEXT) | (crazy/unprint) | END_EXTRA_TEXT
            // We can find chunk2 by trying all K2_start, K2_end on val and seeing
            // which inline encoding of val[K2_start..K2_end] is a prefix of leafBytes from chunk2Start.
            // The K2_start is hypothesised to be K1 + 1 (drop one char) or K1 + 2 (drop two chars).
            // K2_end is the largest k such that the inline encoding fits within the budget.
            sb.AppendLine(CultureInfo.InvariantCulture, $"  chunk #2 region starts at byte offset {chunk2Start}");

            // Try a range of (K2_start, K2_end) pairs.
            int? bestK2Start = null, bestK2End = null;
            int bestChunk2Length = 0;
            for (int k2s = matchedK; k2s <= matchedK + 5 && k2s <= val.Length; k2s++)
            {
                for (int k2e = k2s + 50; k2e <= val.Length && k2e <= matchedK + 600; k2e++)
                {
                    int inline2Length = inlineCache.ByteLength(k2s, k2e);
                    if (inline2Length == 0)
                    {
                        continue;
                    }

                    if (chunk2Start + inline2Length > leafBytes.Length)
                    {
                        continue;
                    }

                    if (inlineCache.SliceMatchesAt(k2s, k2e, leafBytes, chunk2Start))
                    {
                        // Track longest match
                        if (inline2Length > bestChunk2Length)
                        {
                            bestK2Start = k2s;
                            bestK2End = k2e;
                            bestChunk2Length = inline2Length;
                        }
                    }
                }
            }

            if (bestK2Start.HasValue)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"  chunk #2: K2_start={bestK2Start} K2_end={bestK2End}  (drops {bestK2Start - matchedK} chars between chunks)");
                sb.AppendLine(CultureInfo.InvariantCulture, $"  chunk #2 byte-length: {bestChunk2Length}");
            }
            else
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"  chunk #2: no match found");

                // Show first 60 bytes after the separator for diagnosis.
                int n = Math.Min(60, leafBytes.Length - chunk2Start);
                sb.AppendLine(CultureInfo.InvariantCulture, $"  bytes after separator (first {n}): {Convert.ToHexString(leafBytes.AsSpan(chunk2Start, n).ToArray())}");

                // Also show what inline-encoding val[181..255] would yield.
                if (val.Length > 255)
                {
                    int trialEnd = Math.Min(255, val.Length);
                    sb.AppendLine(CultureInfo.InvariantCulture, $"  inline-only(val[181..255]) ({inlineCache.ByteLength(181, trialEnd)} B): {inlineCache.ToHex(181, trialEnd)}");
                }
            }

            // Show the full structure
            sb.AppendLine(CultureInfo.InvariantCulture, $"  total leaf bytes: {leafBytes.Length}, header(1) + chunk1({sepIdx - 1}) + sep(4) + chunk2({bestChunk2Length}) + tail({leafBytes.Length - chunk2Start - bestChunk2Length})");
            if (bestK2Start.HasValue)
            {
                int tailStart = chunk2Start + bestChunk2Length;
                sb.AppendLine(CultureInfo.InvariantCulture, $"  tail hex: {Convert.ToHexString(leafBytes.AsSpan(tailStart).ToArray())}");
            }

            sb.AppendLine();
        }
    }

    private static int IndexOf(byte[] hay, byte[] needle, int from)
    {
        int pos = hay.AsSpan(from).IndexOf(needle);
        return pos >= 0 ? pos + from : -1;
    }

    private static int FindColumnOrdinal(IReadOnlyList<ColumnMetadata> columns, string name)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new InvalidOperationException($"Column '{name}' not found.");
    }

    private sealed class InlineEncoder
    {
        private readonly Func<char, byte[]?>[] codes;
        private readonly Func<char, byte[]?>[] extCodes;

        public InlineEncoder(object[] codes, object[] extCodes)
        {
            this.codes = BuildDelegates(codes);
            this.extCodes = BuildDelegates(extCodes);
        }

        public InlineEncodingCache Encode(string value) => new(value, this);

        public void AppendInline(char c, List<byte> output)
        {
            Func<char, byte[]?> getInlineBytes = c <= 0x00FF ? codes[c] : extCodes[c - 0x0100];
            byte[]? inline = getInlineBytes(c);
            if (inline is not null)
            {
                output.AddRange(inline);
            }
        }

        private static Func<char, byte[]?>[] BuildDelegates(object[] handlers)
        {
            var methodCache = new Dictionary<Type, MethodInfo>();
            var delegates = new Func<char, byte[]?>[handlers.Length];
            for (int i = 0; i < handlers.Length; i++)
            {
                object handler = handlers[i];
                Type handlerType = handler.GetType();
                if (!methodCache.TryGetValue(handlerType, out MethodInfo? method))
                {
                    method = handlerType.GetMethod("GetInlineBytes")
                        ?? throw new MissingMethodException(handlerType.FullName, "GetInlineBytes");
                    methodCache.Add(handlerType, method);
                }

                delegates[i] = (Func<char, byte[]?>)method.CreateDelegate(typeof(Func<char, byte[]?>), handler);
            }

            return delegates;
        }
    }

    private sealed class InlineEncodingCache
    {
        private readonly byte[] bytes;
        private readonly int[] offsets;

        public InlineEncodingCache(string value, InlineEncoder encoder)
        {
            offsets = new int[value.Length + 1];
            var output = new List<byte>(value.Length * 2);
            for (int i = 0; i < value.Length; i++)
            {
                offsets[i] = output.Count;
                encoder.AppendInline(value[i], output);
            }

            offsets[value.Length] = output.Count;
            bytes = [.. output];
        }

        public int ByteLength(int start, int end)
            => offsets[end] - offsets[start];

        public bool SliceEqualsBytes(int start, int end, byte[] other, int otherOffset, int otherLength)
        {
            int byteLength = ByteLength(start, end);
            return byteLength == otherLength
                   && bytes.AsSpan(offsets[start], byteLength).SequenceEqual(other.AsSpan(otherOffset, otherLength));
        }

        public bool SliceMatchesAt(int start, int end, byte[] haystack, int haystackOffset)
        {
            int byteLength = ByteLength(start, end);
            return haystackOffset + byteLength <= haystack.Length
                   && bytes.AsSpan(offsets[start], byteLength).SequenceEqual(haystack.AsSpan(haystackOffset, byteLength));
        }

        public string ToHex(int start, int end)
            => Convert.ToHexString(bytes.AsSpan(offsets[start], ByteLength(start, end)));
    }
}
