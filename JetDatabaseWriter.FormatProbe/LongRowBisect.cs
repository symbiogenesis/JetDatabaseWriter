// Brute-force chunk boundary discovery for long-row index entries.
// Reads the testIndexCodes V2000/V2003/V2007/V2010 fixtures, finds the
// long-row leaf entries (Table11.DataIndex), splits on the format-specific
// 4-byte separator (08 07 08 04 for genleg, 07 09 07 06 for general), and
// for each chunk searches for the (K_start, K_end) source-character range
// whose inline-only encoding matches the chunk bytes exactly.
//
// Usage: $env:DIAG_LONG_ROW_BISECT="1" ; dotnet run --project JetDatabaseWriter.FormatProbe

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

internal static class LongRowBisect
{
    public static async Task<int> RunAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Long-row chunk-boundary bisection");
        sb.AppendLine();

        // Build inline-only encoder via reflection on the existing per-codepoint tables.
        var (genlegCodes, genlegExt) = LoadHandlers("GeneralLegacyTextIndexEncoder");
        var (genCodes, genExt) = LoadHandlers("GeneralTextIndexEncoder");

        var tasks = new (string Path, byte[] Sep, dynamic Codes, dynamic ExtCodes)[]
        {
            (Path.Combine(fixturesDir, "Jackcess", "V2000", "testIndexCodesV2000.mdb"), [0x08, 0x07, 0x08, 0x04], genlegCodes, genlegExt),
            (Path.Combine(fixturesDir, "Jackcess", "V2003", "testIndexCodesV2003.mdb"), [0x08, 0x07, 0x08, 0x04], genlegCodes, genlegExt),
            (Path.Combine(fixturesDir, "Jackcess", "V2007", "testIndexCodesV2007.accdb"), [0x08, 0x07, 0x08, 0x04], genlegCodes, genlegExt),
            (Path.Combine(fixturesDir, "Jackcess", "V2010", "testIndexCodesV2010.accdb"), [0x07, 0x09, 0x07, 0x06], genCodes, genExt),
        };

        foreach (var t in tasks)
        {
            sb.AppendLine($"## {Path.GetFileName(t.Path)} (separator: {Convert.ToHexString(t.Sep)})");
            sb.AppendLine();
            await BisectAsync(t.Path, t.Sep, (object[])t.Codes, (object[])t.ExtCodes, sb);
            sb.AppendLine();
        }

        Directory.CreateDirectory(Path.GetDirectoryName(outFile)!);
        await File.WriteAllTextAsync(outFile, sb.ToString());
        Console.WriteLine($"Wrote {outFile}");
        return 0;
    }

    private static (object[] Codes, object[] Ext) LoadHandlers(string typeName)
    {
        // Force lazy init on the encoder class via Encode("a", true), then grab the static field arrays.
        var asm = typeof(AccessReader).Assembly;
        var t = asm.GetType($"JetDatabaseWriter.Internal.{typeName}")!;
        t.GetMethod("Encode", BindingFlags.Public | BindingFlags.Static)!
            .Invoke(null, new object?[] { "a", true });

        // The lazy holders are private static. Use the GeneralLegacy LoadCodes path directly.
        var glType = asm.GetType("JetDatabaseWriter.Internal.GeneralLegacyTextIndexEncoder")!;
        var loadCodes = glType.GetMethod("LoadCodes", BindingFlags.NonPublic | BindingFlags.Static)!;

        (string main, string ext) resources = typeName == "GeneralTextIndexEncoder"
            ? ("JetDatabaseWriter.IndexCodeTables.index_codes_gen.txt.gz",
               "JetDatabaseWriter.IndexCodeTables.index_codes_ext_gen.txt.gz")
            : ("JetDatabaseWriter.IndexCodeTables.index_codes_genleg.txt.gz",
               "JetDatabaseWriter.IndexCodeTables.index_codes_ext_genleg.txt.gz");

        var codes = (object[])loadCodes.Invoke(null, new object[] { resources.main, (char)0x0000, (char)0x00FF })!;
        var ext = (object[])loadCodes.Invoke(null, new object[] { resources.ext, (char)0x0100, (char)0xFFFF })!;
        return (codes, ext);
    }

    private static async Task BisectAsync(string path, byte[] sep, object[] codes, object[] extCodes, StringBuilder sb)
    {
        if (!File.Exists(path)) { sb.AppendLine($"_(missing)_"); return; }

        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, CancellationToken.None);

        var layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        DataTable dt = await reader.ReadDataTableAsync("Table11");
        var rowValues = new List<string?>();
        foreach (DataRow r in dt.Rows)
        {
            object v = r["data"];
            rowValues.Add(v is DBNull ? null : (string?)v);
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Table11");
        var idx = indexes.First(i => i.Columns.Count == 1 && i.Columns[0].Name == "data" && i.FirstDp > 0);
        bool asc = idx.Columns[0].IsAscending;

        // Collect leaves
        long current = idx.FirstDp;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, CancellationToken.None);
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf) break;
            var iEntries = IndexLeafIncremental.DecodeIntermediateEntries(layout, page, reader.PageSize);
            current = iEntries[0].ChildPage;
        }
        var allKeys = new List<byte[]>();
        while (current != 0)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, CancellationToken.None);
            var entries = IndexLeafIncremental.DecodeEntries(layout, page, reader.PageSize);
            foreach (var e in entries) allKeys.Add(e.Key);
            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        // For each row long enough (>127 chars), find its key by trying all leaves.
        var longRows = new List<(int Idx, string Val)>();
        for (int i = 0; i < rowValues.Count; i++)
        {
            if (rowValues[i] is { Length: > 127 } v) longRows.Add((i, v));
        }

        sb.AppendLine($"Long rows: {longRows.Count}; long leaves: {allKeys.Count(k => k.Length > 50)}");
        sb.AppendLine();

        foreach (var (rowIdx, val) in longRows)
        {
            sb.AppendLine($"### row[{rowIdx}] len={val.Length}");
            // Show some specific source chars to diagnose chunk-boundary rule.
            for (int probe = 175; probe <= 185 && probe < val.Length; probe++)
            {
                char c = val[probe];
                sb.AppendLine($"  val[{probe}] = U+{(int)c:X4} ('{(char.IsControl(c) ? '?' : c)}')");
            }
            // Encode various prefixes inline-only and search for a leaf containing the result
            // followed by the separator.
            var candidates = new List<(int K, byte[] Bytes)>();
            // Try all source positions K from 100 to 200 — covers expected K1.
            for (int k = 100; k <= 200 && k <= val.Length; k++)
            {
                byte[] inline = EncodeInlineOnly(val.AsSpan(0, k), codes, extCodes);
                candidates.Add((k, inline));
            }

            // Find which leaf this row belongs to: scan for one whose first chunk
            // matches one of the candidates.
            int matchedLeaf = -1;
            int matchedK = -1;
            byte[]? leafBytes = null;
            foreach (byte[] k in allKeys)
            {
                if (k.Length < 200) continue;
                // skip 1-byte flag
                int sepAt = IndexOf(k, sep, 1);
                if (sepAt < 0) continue;
                var chunk1 = k.AsSpan(1, sepAt - 1).ToArray();
                foreach (var (cand, candBytes) in candidates)
                {
                    if (chunk1.AsSpan().SequenceEqual(candBytes))
                    {
                        matchedLeaf = allKeys.IndexOf(k);
                        matchedK = cand;
                        leafBytes = k;
                        break;
                    }
                }
                if (matchedLeaf >= 0) break;
            }

            if (matchedLeaf < 0 || leafBytes is null)
            {
                // Maybe descending: also try complemented forms
                sb.AppendLine($"  no chunk-1 match found in {allKeys.Count} leaves (asc={asc}). Skipping.");
                sb.AppendLine();
                continue;
            }

            sb.AppendLine($"  chunk #1: K1={matchedK} (val[0..{matchedK}))  leafIdx={matchedLeaf} leafLen={leafBytes.Length}");

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
            sb.AppendLine($"  chunk #2 region starts at byte offset {chunk2Start}");

            // Try a range of (K2_start, K2_end) pairs.
            int? bestK2Start = null, bestK2End = null;
            byte[]? bestChunk2 = null;
            for (int k2s = matchedK; k2s <= matchedK + 5 && k2s <= val.Length; k2s++)
            {
                for (int k2e = k2s + 50; k2e <= val.Length && k2e <= matchedK + 600; k2e++)
                {
                    byte[] inline2 = EncodeInlineOnly(val.AsSpan(k2s, k2e - k2s), codes, extCodes);
                    if (inline2.Length == 0) continue;
                    if (chunk2Start + inline2.Length > leafBytes.Length) continue;
                    if (leafBytes.AsSpan(chunk2Start, inline2.Length).SequenceEqual(inline2))
                    {
                        // Track longest match
                        if (bestChunk2 is null || inline2.Length > bestChunk2.Length)
                        {
                            bestK2Start = k2s;
                            bestK2End = k2e;
                            bestChunk2 = inline2;
                        }
                    }
                }
            }

            if (bestK2Start.HasValue)
            {
                sb.AppendLine($"  chunk #2: K2_start={bestK2Start} K2_end={bestK2End}  (drops {bestK2Start - matchedK} chars between chunks)");
                sb.AppendLine($"  chunk #2 byte-length: {bestChunk2!.Length}");
            }
            else
            {
                sb.AppendLine($"  chunk #2: no match found");
                // Show first 60 bytes after the separator for diagnosis.
                int n = Math.Min(60, leafBytes.Length - chunk2Start);
                sb.AppendLine($"  bytes after separator (first {n}): {Convert.ToHexString(leafBytes.AsSpan(chunk2Start, n).ToArray())}");
                // Also show what inline-encoding val[181..255] would yield.
                if (val.Length > 255)
                {
                    var trial = EncodeInlineOnly(val.AsSpan(181, 74), codes, extCodes);
                    sb.AppendLine($"  inline-only(val[181..255]) ({trial.Length} B): {Convert.ToHexString(trial)}");
                }
            }

            // Show the full structure
            sb.AppendLine($"  total leaf bytes: {leafBytes.Length}, header(1) + chunk1({sepIdx - 1}) + sep(4) + chunk2({(bestChunk2?.Length ?? 0)}) + tail({leafBytes.Length - chunk2Start - (bestChunk2?.Length ?? 0)})");
            if (bestChunk2 is not null)
            {
                int tailStart = chunk2Start + bestChunk2.Length;
                sb.AppendLine($"  tail hex: {Convert.ToHexString(leafBytes.AsSpan(tailStart).ToArray())}");
            }
            sb.AppendLine();
        }
    }

    private static int IndexOf(byte[] hay, byte[] needle, int from)
    {
        for (int i = from; i + needle.Length <= hay.Length; i++)
        {
            bool ok = true;
            for (int j = 0; j < needle.Length; j++) { if (hay[i + j] != needle[j]) { ok = false; break; } }
            if (ok) return i;
        }
        return -1;
    }

    /// <summary>
    /// Run the per-codepoint state machine over <paramref name="chars"/> and return
    /// only the inline bytes (no flag, no END_TEXT, no extras/unprintable/crazy, no
    /// END_EXTRA_TEXT). Mirrors GeneralLegacyTextIndexEncoder.EncodeWithTables but
    /// stops after the inline-emission loop.
    /// </summary>
    private static byte[] EncodeInlineOnly(ReadOnlySpan<char> chars, object[] codes, object[] extCodes)
    {
        var bout = new List<byte>(chars.Length * 2);
        foreach (char c in chars)
        {
            object handler = c <= 0x00FF ? codes[c] : extCodes[c - 0x0100];
            var t = handler.GetType();
            // Call GetInlineBytes(c)
            var m = t.GetMethod("GetInlineBytes")!;
            byte[]? inline = (byte[]?)m.Invoke(handler, new object[] { c });
            if (inline is not null) bout.AddRange(inline);
        }
        return [.. bout];
    }
}
