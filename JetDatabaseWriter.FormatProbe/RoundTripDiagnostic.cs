// Round-trip diagnostic probe.
//
// Compares a "post-write" .accdb (the output of AccessWriter.CreateTableAsync
// against a copy of NorthwindTraders.accdb) byte-for-byte against the
// original NorthwindTraders.accdb fixture, and dumps every fact that bears on
// docs/design/round-trip-test-failures-2026-05-02.md hypothesis #1–#4:
//
//   1. MSysObjects row payload disagrees with the spliced index key.
//   2. MSysObjects TDEF index summary stale (used_pages / usage_map_page).
//   3. MSysObjects.Id autonumber counter not advanced.
//   4. MSysRelationships / MSysObjects "Type" mismatches.
//
// USAGE
// -----
//   $env:DIAG_RT_PROBE = "C:\path\to\source.accdb"
//   $env:DIAG_RT_BASELINE = "C:\repos\.\Tests\Databases\NorthwindTraders.accdb"  # optional; defaults to the test fixture
//   dotnet run --project JetDatabaseWriter.FormatProbe
//
// The probe writes a self-contained markdown report to
//   <source-dir>/round-trip-diagnostic.md
// alongside the source file so DIAG_RT_KEEP work directories can host them.

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;

internal static class RoundTripDiagnostic
{
    public static async Task<int> RunAsync(string sourcePath, string baselinePath, string outPath)
    {
        if (!File.Exists(sourcePath))
        {
            Console.Error.WriteLine($"[rt-diag] source not found: {sourcePath}");
            return 1;
        }

        if (!File.Exists(baselinePath))
        {
            Console.Error.WriteLine($"[rt-diag] baseline not found: {baselinePath}");
            return 1;
        }

        Console.WriteLine($"[rt-diag] source   = {sourcePath}");
        Console.WriteLine($"[rt-diag] baseline = {baselinePath}");

        var sb = new StringBuilder();
        sb.AppendLine("# Round-trip diagnostic dump");
        sb.AppendLine();
        sb.AppendLine($"- Source (post-write, DAO-rejected): `{sourcePath}`");
        sb.AppendLine($"- Baseline (pristine NorthwindTraders): `{baselinePath}`");
        sb.AppendLine($"- Generated: {DateTimeOffset.UtcNow:u}");
        sb.AppendLine();

        await using var src = await AccessReader.OpenAsync(sourcePath);
        await using var bas = await AccessReader.OpenAsync(baselinePath);

        await DumpFileLevelAsync(sb, src, bas);
        await DumpCatalogAsync(sb, src, bas);
        await DumpMSysObjectsTDefAsync(sb, src, bas);
        await DumpMSysObjectsIndexLeavesAsync(sb, src, bas);
        await DumpMSysObjectsNewRowsAsync(sb, src);
        await DumpKeyVsRowEncodingAsync(sb, src);

        Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);
        await File.WriteAllTextAsync(outPath, sb.ToString());
        Console.WriteLine($"[rt-diag] wrote {outPath}");
        return 0;
    }

    // ───────────────────────── §1 file-level diff ───────────────────────────

    private static async Task DumpFileLevelAsync(StringBuilder sb, AccessReader src, AccessReader bas)
    {
        sb.AppendLine("## §1 File-level page diff");
        sb.AppendLine();

        long srcLen = new FileInfo(src.HostDatabasePath).Length;
        long basLen = new FileInfo(bas.HostDatabasePath).Length;
        int pgSz = src.PageSize;
        long srcPages = srcLen / pgSz;
        long basPages = basLen / pgSz;

        sb.AppendLine($"- src size = {srcLen:N0} ({srcPages} pages × {pgSz} B)");
        sb.AppendLine($"- bas size = {basLen:N0} ({basPages} pages × {pgSz} B)");
        sb.AppendLine($"- delta    = {srcLen - basLen:+#,##0;-#,##0;0} ({srcPages - basPages:+#;-#;0} pages)");
        sb.AppendLine();

        long sharedPages = Math.Min(srcPages, basPages);
        var diffs = new List<long>();
        for (long p = 0; p < sharedPages; p++)
        {
            byte[] a = await src.GetRawPageBytesAsync(p, default);
            byte[] b = await bas.GetRawPageBytesAsync(p, default);
            if (!a.AsSpan().SequenceEqual(b)) diffs.Add(p);
        }

        sb.AppendLine($"- pages that differ in the shared range [0..{sharedPages - 1}]: **{diffs.Count}** → {string.Join(", ", diffs.Take(64).Select(p => p.ToString(CultureInfo.InvariantCulture)))}{(diffs.Count > 64 ? ", …" : string.Empty)}");
        sb.AppendLine();
    }

    // ───────────────────────── §2 catalog diff ──────────────────────────────

    private static async Task DumpCatalogAsync(StringBuilder sb, AccessReader src, AccessReader bas)
    {
        sb.AppendLine("## §2 MSysObjects catalog (post-write rows that are NEW vs. baseline)");
        sb.AppendLine();

        var srcCat = await ReadCatalogAsync(src);
        var basCat = await ReadCatalogAsync(bas);
        var basIds = new HashSet<long>(basCat.Select(c => c.Id));

        var added = srcCat.Where(c => !basIds.Contains(c.Id)).OrderBy(c => c.Id).ToList();
        sb.AppendLine($"- src catalog rows: {srcCat.Count}, baseline catalog rows: {basCat.Count}, new rows: {added.Count}");
        sb.AppendLine();
        sb.AppendLine("| Id | Name | Type | Flags | TDEF page | ParentId |");
        sb.AppendLine("|---:|---|---:|---|---:|---:|");
        foreach (var c in added)
            sb.AppendLine($"| {c.Id} | `{Md(c.Name)}` | {c.Type} | 0x{unchecked((uint)c.Flags):X8} | {c.TdefPage} | {c.ParentId} |");
        sb.AppendLine();
    }

    // ─────────────────── §3 MSysObjects TDEF descriptor diff ────────────────

    private static async Task DumpMSysObjectsTDefAsync(StringBuilder sb, AccessReader src, AccessReader bas)
    {
        sb.AppendLine("## §3 MSysObjects TDEF (page 2) — header + per-real-idx descriptors");
        sb.AppendLine();
        sb.AppendLine("If `row_count` did not advance, or any real-idx slot's `first_dp` / `usage_map_page` / `used_pages` triple changed when it should not have (or didn't change when it should), that's a hypothesis-#2 hit.");
        sb.AppendLine();

        byte[] srcTd = (await src.GetRawTDefBytesAsync(2, default)) ?? Array.Empty<byte>();
        byte[] basTd = (await bas.GetRawTDefBytesAsync(2, default)) ?? Array.Empty<byte>();

        // Jet4/ACE TDEF (per AccessBase.cs):
        //   row_count        @ offset 16  (uint32 LE) — Constants.TableDefinition.RowCountOffset
        //   ddl_or_autonum?  @ offset 20  (uint32 LE) — autonumber next value (per mdbtools HACKING.md)
        //   num_rows2?       @ offset 24
        //   num_cols         @ offset 45  (uint16 LE)
        //   num_idx          @ offset 47  (uint32 LE)
        //   num_real_idx     @ offset 51  (uint32 LE)
        //   block_end        @ offset 63
        //
        // Per-real-idx physical descriptor (Jet4, 12 bytes per AccessBase, but
        // mdbtools HACKING.md documents the FULL Jet4 phys descriptor as 52 B
        // including first_dp / usage_map_page / used_pages). The 12 B figure
        // is the SKIP entry only — the 52 B descriptor lives later in the
        // TDEF after column descs and column names.

        sb.AppendLine("| Field (offset) | Baseline | Source | delta |");
        sb.AppendLine("|---|---:|---:|---|");
        DiffU32(sb, "row_count @ 0x10", basTd, srcTd, 16);
        DiffU32(sb, "(0x14: autonum/ddl)", basTd, srcTd, 20);
        DiffU32(sb, "(0x18)", basTd, srcTd, 24);
        DiffU16(sb, "num_cols @ 0x2D", basTd, srcTd, 45);
        DiffU32(sb, "num_idx @ 0x2F", basTd, srcTd, 47);
        DiffU32(sb, "num_real_idx @ 0x33", basTd, srcTd, 51);
        sb.AppendLine();

        // Walk per-real-idx skip entries in the 12-byte block right after
        // _tdBlockEnd (offset 63). Per-mdbtools, this region is described as
        // {first_dp(4), usage_map_page(4), used_pages(4)} per real-idx.
        int numRealIdx = ReadI32(basTd, 51);
        sb.AppendLine($"### Per-real-idx 12-byte skip entries (offset 63 + 12*i, num_real_idx={numRealIdx})");
        sb.AppendLine();
        sb.AppendLine("| ri | bas first_dp | src first_dp | bas u4 | src u4 | bas u8 | src u8 | changed? |");
        sb.AppendLine("|---:|---:|---:|---:|---:|---:|---:|:---:|");
        for (int i = 0; i < numRealIdx && 63 + 12 * (i + 1) <= Math.Min(basTd.Length, srcTd.Length); i++)
        {
            int o = 63 + i * 12;
            uint a0 = ReadU32(basTd, o), b0 = ReadU32(srcTd, o);
            uint a4 = ReadU32(basTd, o + 4), b4 = ReadU32(srcTd, o + 4);
            uint a8 = ReadU32(basTd, o + 8), b8 = ReadU32(srcTd, o + 8);
            bool diff = a0 != b0 || a4 != b4 || a8 != b8;
            sb.AppendLine($"| {i} | {a0} | {b0} | 0x{a4:X8} | 0x{b4:X8} | 0x{a8:X8} | 0x{b8:X8} | {(diff ? "**yes**" : "no")} |");
        }
        sb.AppendLine();

        // Total-bytes diff summary on the entire TDEF chain — anything outside
        // the row_count field is suspicious.
        var byteDiffs = new List<int>();
        int common = Math.Min(basTd.Length, srcTd.Length);
        for (int i = 0; i < common; i++) if (basTd[i] != srcTd[i]) byteDiffs.Add(i);
        sb.AppendLine($"- Total byte-level diffs across the TDEF chain: **{byteDiffs.Count}** (lengths bas={basTd.Length}, src={srcTd.Length})");
        sb.AppendLine($"- Diff offsets (first 64): {string.Join(", ", byteDiffs.Take(64).Select(o => $"0x{o:X4}"))}{(byteDiffs.Count > 64 ? ", …" : string.Empty)}");
        sb.AppendLine();
    }

    // ─────────────── §4 MSysObjects spliced index leaf entries ──────────────

    private static async Task DumpMSysObjectsIndexLeavesAsync(StringBuilder sb, AccessReader src, AccessReader bas)
    {
        sb.AppendLine("## §4 MSysObjects index-leaf splice verification");
        sb.AppendLine();
        sb.AppendLine("Decodes pages 8 (Id PK) and 2790 (ParentIdName composite) from both src and baseline. New entries should be present in src and absent in baseline; key bytes should re-decode losslessly.");
        sb.AppendLine();

        long[] suspectLeaves = [8, 2790];
        var layout = IndexLeafPageBuilder.LeafPageLayout.Jet4;

        foreach (long p in suspectLeaves)
        {
            byte[] a = await bas.GetRawPageBytesAsync(p, default);
            byte[] b = await src.GetRawPageBytesAsync(p, default);
            sb.AppendLine($"### page {p}: type bas=0x{a[0]:X2}, src=0x{b[0]:X2}");
            sb.AppendLine();
            if (a[0] != Constants.IndexLeafPage.PageTypeLeaf || b[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                sb.AppendLine("> not a leaf in one of the two files — skipped.");
                sb.AppendLine();
                continue;
            }

            List<IndexEntry> aEntries, bEntries;
            try { aEntries = IndexLeafIncremental.DecodeEntries(layout, a, src.PageSize); }
            catch (Exception ex) { sb.AppendLine($"> bas decode failed: {ex.GetType().Name}: {ex.Message}"); sb.AppendLine(); continue; }
            try { bEntries = IndexLeafIncremental.DecodeEntries(layout, b, src.PageSize); }
            catch (Exception ex) { sb.AppendLine($"> src decode failed: {ex.GetType().Name}: {ex.Message}"); sb.AppendLine(); continue; }

            sb.AppendLine($"- bas entries: {aEntries.Count}, src entries: {bEntries.Count}");
            var aKeys = new HashSet<string>(aEntries.Select(e => Convert.ToHexString(e.Key)));
            var added = bEntries.Where(e => !aKeys.Contains(Convert.ToHexString(e.Key))).ToList();
            var removed = aEntries.Where(e => !new HashSet<string>(bEntries.Select(x => Convert.ToHexString(x.Key))).Contains(Convert.ToHexString(e.Key))).ToList();
            sb.AppendLine($"- entries added in src: {added.Count}");
            foreach (var e in added.Take(8))
                sb.AppendLine($"  - key=`{Convert.ToHexString(e.Key)}` → page {e.DataPage}, row {e.DataRow}");
            sb.AppendLine($"- entries removed in src: {removed.Count}");
            foreach (var e in removed.Take(8))
                sb.AppendLine($"  - key=`{Convert.ToHexString(e.Key)}` → page {e.DataPage}, row {e.DataRow}");

            // Sort verification.
            bool sorted = true;
            for (int i = 1; i < bEntries.Count; i++)
            {
                if (CompareKey(bEntries[i - 1].Key, bEntries[i].Key) > 0) { sorted = false; break; }
            }

            sb.AppendLine($"- src sort order intact: {(sorted ? "yes" : "**NO — splice broke ordering**")}");
            sb.AppendLine();
        }
    }

    // ───────────────── §5 dump the new MSysObjects rows on disk ─────────────

    private static async Task DumpMSysObjectsNewRowsAsync(StringBuilder sb, AccessReader src)
    {
        sb.AppendLine("## §5 New MSysObjects rows — decoded values");
        sb.AppendLine();

        var msys = await src.GetMSysObjectsTableDefAsync(default)
            ?? throw new InvalidOperationException("MSysObjects TDEF missing");
        int idxId = msys.Columns.FindIndex(c => c.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));
        int idxName = msys.Columns.FindIndex(c => c.Name.Equals("Name", StringComparison.OrdinalIgnoreCase));
        int idxParent = msys.Columns.FindIndex(c => c.Name.Equals("ParentId", StringComparison.OrdinalIgnoreCase));
        int idxType = msys.Columns.FindIndex(c => c.Name.Equals("Type", StringComparison.OrdinalIgnoreCase));
        int idxFlags = msys.Columns.FindIndex(c => c.Name.Equals("Flags", StringComparison.OrdinalIgnoreCase));

        sb.AppendLine($"- column slots: Id={idxId}, Name={idxName}, ParentId={idxParent}, Type={idxType}, Flags={idxFlags}");
        sb.AppendLine();
        sb.AppendLine("| Id | ParentId | Type | Flags | Name |");
        sb.AppendLine("|---:|---:|---:|---|---|");

        await foreach (var row in src.EnumerateMSysObjectsRowsAsync(msys, default))
        {
            string name = SafeGet(row, idxName);
            if (!name.StartsWith("RT_", StringComparison.Ordinal)) continue;
            sb.AppendLine($"| {SafeGet(row, idxId)} | {SafeGet(row, idxParent)} | {SafeGet(row, idxType)} | {SafeGet(row, idxFlags)} | `{Md(name)}` |");
        }

        sb.AppendLine();
    }

    // ─────────── §6 row-vs-key encoding round-trip (hypothesis #1) ──────────

    private static async Task DumpKeyVsRowEncodingAsync(StringBuilder sb, AccessReader src)
    {
        sb.AppendLine("## §6 Hypothesis #1: row → IndexKeyEncoder → splice key roundtrip");
        sb.AppendLine();
        sb.AppendLine("For every new MSysObjects row, recompute the (ParentId, Name) composite-key bytes via `IndexKeyEncoder` and the Id PK bytes, and check whether matching keys exist on the spliced leaves.");
        sb.AppendLine();

        var msys = await src.GetMSysObjectsTableDefAsync(default)!;
        int idxId = msys.Columns.FindIndex(c => c.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));
        int idxName = msys.Columns.FindIndex(c => c.Name.Equals("Name", StringComparison.OrdinalIgnoreCase));
        int idxParent = msys.Columns.FindIndex(c => c.Name.Equals("ParentId", StringComparison.OrdinalIgnoreCase));

        var idCol = msys.Columns[idxId];
        var nameCol = msys.Columns[idxName];
        var parentCol = msys.Columns[idxParent];

        var page8 = await src.GetRawPageBytesAsync(8, default);
        var page2790 = await src.GetRawPageBytesAsync(2790, default);
        var pkEntries = IndexLeafIncremental.DecodeEntries(IndexLeafPageBuilder.LeafPageLayout.Jet4, page8, src.PageSize);
        var compEntries = IndexLeafIncremental.DecodeEntries(IndexLeafPageBuilder.LeafPageLayout.Jet4, page2790, src.PageSize);
        var pkKeys = new HashSet<string>(pkEntries.Select(e => Convert.ToHexString(e.Key)));
        var compKeys = new HashSet<string>(compEntries.Select(e => Convert.ToHexString(e.Key)));

        sb.AppendLine("| Row | recomputed Id key | found on page 8? | recomputed (ParentId,Name) key | found on page 2790? |");
        sb.AppendLine("|---|---|:---:|---|:---:|");

        await foreach (var row in src.EnumerateMSysObjectsRowsAsync(msys, default))
        {
            string name = SafeGet(row, idxName);
            if (!name.StartsWith("RT_", StringComparison.Ordinal)) continue;

            int idVal = (int)long.Parse(SafeGet(row, idxId), CultureInfo.InvariantCulture);
            int parentVal = (int)long.Parse(SafeGet(row, idxParent), CultureInfo.InvariantCulture);

            byte[] pkKey;
            byte[] compKey;
            try
            {
                pkKey = IndexKeyEncoder.EncodeEntry(idCol.Type, idVal, ascending: true);
                byte[] pBytes = IndexKeyEncoder.EncodeEntry(parentCol.Type, parentVal, ascending: true);
                byte[] nBytes = IndexKeyEncoder.EncodeEntry(nameCol.Type, name, ascending: true);
                compKey = new byte[pBytes.Length + nBytes.Length];
                Buffer.BlockCopy(pBytes, 0, compKey, 0, pBytes.Length);
                Buffer.BlockCopy(nBytes, 0, compKey, pBytes.Length, nBytes.Length);
            }
            catch (Exception ex)
            {
                sb.AppendLine($"| `{Md(name)}` | encode err: {ex.GetType().Name}: {ex.Message} | — | — | — |");
                continue;
            }

            string pkHex = Convert.ToHexString(pkKey);
            string compHex = Convert.ToHexString(compKey);
            bool pkFound = pkKeys.Contains(pkHex);
            bool compFound = compKeys.Contains(compHex);

            sb.AppendLine($"| `{Md(name)}` | `{pkHex}` | {(pkFound ? "✅" : "**❌**")} | `{compHex}` | {(compFound ? "✅" : "**❌**")} |");
        }

        sb.AppendLine();
        sb.AppendLine("If any row fails (❌) — the splice's idea of the key disagrees with what `IndexKeyEncoder` produces for the on-disk row values, which is exactly hypothesis #1.");
        sb.AppendLine();
    }

    // ───────────────────────── helpers ──────────────────────────────────────

    private static int CompareKey(byte[] a, byte[] b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++) { int d = a[i].CompareTo(b[i]); if (d != 0) return d; }
        return a.Length - b.Length;
    }

    private static void DiffU32(StringBuilder sb, string label, byte[] a, byte[] b, int o)
    {
        if (o + 4 > a.Length || o + 4 > b.Length) { sb.AppendLine($"| {label} | (oob) | (oob) | — |"); return; }
        uint av = ReadU32(a, o), bv = ReadU32(b, o);
        long delta = (long)bv - av;
        sb.AppendLine($"| {label} | {av} | {bv} | {(delta == 0 ? "—" : delta.ToString("+#;-#;0", CultureInfo.InvariantCulture))} |");
    }

    private static void DiffU16(StringBuilder sb, string label, byte[] a, byte[] b, int o)
    {
        if (o + 2 > a.Length || o + 2 > b.Length) { sb.AppendLine($"| {label} | (oob) | (oob) | — |"); return; }
        ushort av = BinaryPrimitives.ReadUInt16LittleEndian(a.AsSpan(o, 2));
        ushort bv = BinaryPrimitives.ReadUInt16LittleEndian(b.AsSpan(o, 2));
        int delta = bv - av;
        sb.AppendLine($"| {label} | {av} | {bv} | {(delta == 0 ? "—" : delta.ToString("+#;-#;0", CultureInfo.InvariantCulture))} |");
    }

    private static uint ReadU32(byte[] b, int o) => BinaryPrimitives.ReadUInt32LittleEndian(b.AsSpan(o, 4));

    private static int ReadI32(byte[] b, int o) => BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(o, 4));

    private static string SafeGet(List<string> row, int i) => i >= 0 && i < row.Count ? row[i] ?? string.Empty : string.Empty;

    private static string Md(string s) => (s ?? string.Empty).Replace("|", "\\|", StringComparison.Ordinal).Replace("`", "\\`", StringComparison.Ordinal);

    private static async Task<List<CatalogEntry>> ReadCatalogAsync(AccessReader r)
    {
        var msys = await r.GetMSysObjectsTableDefAsync(default)!;
        int idxId = msys.Columns.FindIndex(c => c.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));
        int idxName = msys.Columns.FindIndex(c => c.Name.Equals("Name", StringComparison.OrdinalIgnoreCase));
        int idxType = msys.Columns.FindIndex(c => c.Name.Equals("Type", StringComparison.OrdinalIgnoreCase));
        int idxFlags = msys.Columns.FindIndex(c => c.Name.Equals("Flags", StringComparison.OrdinalIgnoreCase));
        int idxParent = msys.Columns.FindIndex(c => c.Name.Equals("ParentId", StringComparison.OrdinalIgnoreCase));
        var list = new List<CatalogEntry>();
        await foreach (var row in r.EnumerateMSysObjectsRowsAsync(msys, default))
        {
            long id = long.TryParse(SafeGet(row, idxId), NumberStyles.Integer, CultureInfo.InvariantCulture, out long v1) ? v1 : 0;
            long parent = long.TryParse(SafeGet(row, idxParent), NumberStyles.Integer, CultureInfo.InvariantCulture, out long v2) ? v2 : 0;
            int type = int.TryParse(SafeGet(row, idxType), NumberStyles.Integer, CultureInfo.InvariantCulture, out int v3) ? v3 : 0;
            long flags = long.TryParse(SafeGet(row, idxFlags), NumberStyles.Integer, CultureInfo.InvariantCulture, out long v4) ? v4 : 0;
            list.Add(new CatalogEntry(id, SafeGet(row, idxName), type, flags, id & 0x00FFFFFFL, parent));
        }

        return list;
    }

    private sealed record CatalogEntry(long Id, string Name, int Type, long Flags, long TdefPage, long ParentId);
}
