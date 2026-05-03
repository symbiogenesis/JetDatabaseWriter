# Round-Trip Test Failures — Investigation Notes (2026-05-02)

## Tests in question

Two tests in [JetDatabaseWriter.Tests/Core/AccessRoundTripTests.cs](JetDatabaseWriter.Tests/Core/AccessRoundTripTests.cs):

- `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
- `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`

Both:
1. Copy `NorthwindTraders.accdb` to a temp dir,
2. Use `AccessWriter` to add `RT_Customers` / `RT_Orders` (and `RT_OrderLines` for the composite case) plus a relationship,
3. Close the writer, then shell out to a bitness-matched `powershell.exe` and call `DAO.DBEngine.120.CompactDatabase(src, dst)`,
4. Re-open the compacted file and assert schema/rows survived.

DAO refuses to compact the post-write file, the script returns exit code 1, and `RoundTripSession.RunDaoCompact` throws `XunitException("DAO CompactDatabase failed (exit=1)")`.

## What DAO actually says

Current `main` (splice enabled):

```
DAO err: 3011 [DAO.DbEngine]
The Microsoft Access database engine could not find the object 'MSysDb'.
```

If the splice in `InsertCatalogEntryAsync` ([JetDatabaseWriter/Core/AccessWriter.cs#L6187](JetDatabaseWriter/Core/AccessWriter.cs#L6187)) is disabled, DAO fails with a different error:

```
Exception calling "CompactDatabase" ... "Object invalid or no longer set."
```

So the splice changes which integrity check DAO trips on, but never produces a file DAO is willing to compact.

`'MSysDb'` is **not** a real catalog object. It's the row at `Id=268435456 / ParentId=251658242 / Type=2 / Flags=0x80000000` in `MSysObjects` (a Databases-properties entry). DAO references that name when its own consistency walk over the catalog stops being self-consistent — it's a symptom, not a missing object.

## What we know about the write path

`InsertCatalogEntryAsync` ([JetDatabaseWriter/Core/AccessWriter.cs#L6155](JetDatabaseWriter/Core/AccessWriter.cs#L6155)):

1. `InsertRowDataLocAsync(2, …)` writes the new MSysObjects row (Id = TDEF page, ParentId = `0x0F000001` Tables parent, Name, Type=1, etc.) and bumps the table's row count.
2. `TrySpliceCatalogIndexEntryAsync` ([JetDatabaseWriter/Core/AccessWriter.cs#L11913](JetDatabaseWriter/Core/AccessWriter.cs#L11913)) walks every real-idx slot, descends to the rightmost leaf, encodes the new composite key for that index, splices it into the existing entry list, and rewrites the leaf via `IndexLeafPageBuilder.BuildLeafPage`.

In the failing run the splice **does succeed** (returns true, no `_lastIncrementalBail` set) and the leaf bytes look correct under inspection.

## Page-level diff (Northwind original vs writer output before compact)

```
Diff pages: 2, 5, 8, 2790, 2994, 2995
File size  : 12,320,768 → 12,386,304 (+64 KiB = +16 pages)
```

| Page | What's there | Expected change |
|------|--------------|-----------------|
| 2    | TDEF/DBA-related (1-byte change at offset 0x10) | Possibly autonumber counter / usage map back-reference |
| 5    | Usage-map / allocation page | 7 byte diffs concentrated near offsets 0x20B/0x23F/0x273 |
| 8    | MSysObjects PK index (Id) leaf | Splice: 239 → 241 entries, `pref_len` 0→1, new keys `7F 80 00 0B C0/C2` |
| 2790 | MSysObjects ParentIdName composite-index leaf | Splice: 114 → 116 entries, two new `RT_*` entries under `ParentId = 0x0F000001` |
| 2994, 2995 | New TDEF/data pages for `RT_Customers`/`RT_Orders` | New table allocation |

## What I verified about the splice bytes

Decoding the rewritten pages with `IndexLeafIncremental.DecodeEntries` (via a temp test runner) confirms:

- **Page 8 (`Id` PK)**: 241 entries, sorted, every original key still decodes losslessly, two new entries `7F 80 00 0B C0` and `7F 80 00 0B C2` for `Id = 3008`/`3010`. `pref_len` recomputed from 0 to 1 (every key now starts with `7F`).
- **Page 2790 (ParentIdName composite)**: 116 entries, two new keys with the form `7F 8F 00 00 01 7F <Name GeneralLegacy bytes> 01 00` slotted into the `ParentId = 0x0F000001` (Tables) range. `pref_len` 1→4 because every entry now shares `7F 8F 00 00`.

Per [/memories/repo/round-trip-tests.md](memories/repo/round-trip-tests.md), this matches the previously verified state — the leaf-level encoding is correct.

## Why DAO still rejects it — current best hypotheses

DAO's compact reads each system table via the index, *then* re-reads it via the row data, and complains when the two views disagree. That points at one of:

### 1. MSysObjects row payload disagrees with the spliced index key (highest probability)

The splice encodes `(ParentId, Name)` keys using `IndexKeyEncoder` (numeric BE for Int32, `GeneralLegacyTextIndexEncoder` for the Name text). DAO reads the row, re-encodes, and compares to the leaf entry. If anything in the row payload disagrees byte-for-byte, the row "doesn't exist" from DAO's index-driven viewpoint and the next catalog walk (which crosschecks `MSysObjects → MSysDb`) blows up with the 3011 message.

Concrete things to check:
- **Name encoding**: `MSysObjects.Name` is a Unicode column. The on-row bytes must produce the same `GeneralLegacy` sort-key bytes the splice used. NFC normalization, compressed-Unicode prefix byte, and trailing-space handling are all candidates.
- **ParentId encoding**: 32-bit signed. The splice writes BE-encoded for the index key (`7F 8F 00 00 01` for `0x0F000001`). The row's fixed-column area must store `0x0F000001` LE — verify the on-row bytes and the round-trip.
- **`Id` autonumber column**: row stores `0x0B C0` / `0x0B C2` (LE) for `Id = 3008` / `3010`. Splice key uses `7F 80 00 0B C0` (BE with sign-flip). Verify both halves of that decode to the same int.

### 2. TDEF index slot's usage-map / used_pages not bumped

Page 5's diffs look like an allocation/usage-map update. If the splice writes through an existing leaf page but `MSysObjects`'s TDEF still records the old `used_pages` count or stale `usage_map_page` for that index, DAO's compact may walk pages that are no longer in the official allocation chain and conclude the catalog is corrupt.

The splice in [JetDatabaseWriter/Core/AccessWriter.cs#L11913](JetDatabaseWriter/Core/AccessWriter.cs#L11913) only rewrites the existing leaf in place — it never touches the TDEF's per-real-idx `firstDp` / `usage_map_page` / `used_pages` triples. That's correct for in-place rewrites that don't change page count, but worth re-verifying against an Access-authored compact's write pattern.

### 3. MSysObjects.Id autonumber counter not advanced

The new TDEF pages got Ids `3008`, `3010`, etc. (= the page numbers we allocated). MSysObjects has an autonumber `Id` column whose next-value counter lives on the DBA page (page 2 — and we are diffing exactly one byte there). If the counter isn't moved past the highest used Id, DAO may consider the catalog inconsistent.

### 4. MSysRelationships / MSysObjects "Type" mismatches

The composite test also creates a relationship through `CreateRelationshipAsync`, which writes to MSysRelationships and a Type=8 MSysObjects entry. Worth re-checking that the splice covers MSysObjects entries created by the relationship path too (not just the table-creation path).

## Where the splice is byte-clean

These have been verified and **should not regress**:

- Jet4 leaf-page header layout (`prev`@12, `next`@16, `tail`@20, `pref_len`@24, bitmask@0x1B, first_entry@0x1E0) — see `Constants.IndexLeafPage.Jet4`.
- Big-endian intermediate child pointers in `IndexBTreeBuilder` and reads in `IndexBTreeSeeker.SelectChildPage` / `IndexLeafIncremental.DecodeIntermediateChildPointer`.
- `pref_len` recomputation on splice.
- Bitmask + `free_space` recomputation.
- Sort + tie-break ordering preserves Access-authored entry order.

## Recommended path forward

1. **Add a row-vs-key roundtrip assertion** before the splice writes the leaf: read back the just-written `MSysObjects` row from disk via `AccessReader`, re-encode its `(ParentId, Name)` and `Id` columns through the same `IndexKeyEncoder` paths, and assert byte equality with what the splice is about to insert. Most likely first failure mode: text encoding for `Name`.
2. If the keys agree, dump the on-row bytes for the new MSysObjects rows and bit-compare against an Access-authored row created interactively (e.g. add a table in the Access UI to a fresh copy of NorthwindTraders, then compare). Differences in compressed-Unicode prefix, fixed-area padding, or null-bitmap sizing will jump out.
3. If row bytes match, instrument or hex-diff the page 2 (DBA) and page 5 (allocation/usage-map) deltas. Compare against the pages produced by a UI-driven Access add-table on the same fixture. The 1-byte change at page 2 offset 0x10 is the most suspicious — it sits near the database-creation/version field cluster.
4. After every leaf splice, re-derive and write the affected real-idx slot's `usage_map_page` / `used_pages` in the TDEF page (page 2's MSysObjects TDEF) so the catalog statistics agree with the on-disk leaf chain.

A useful tactical knob: keep `TrySpliceCatalogIndexEntryAsync`'s `_lastIncrementalBail` plumbing and have failing tests dump that value plus the raw page-2/5 byte diffs into the existing `DIAG_RT_KEEP` work directory so post-mortem diffs don't require manual setup.

## FormatProbe diagnostic harness (added 2026-05-02)

`JetDatabaseWriter.FormatProbe` now carries two opt-in probes for triaging this regression. Both are off by default — they only fire when the matching environment variable is set, so the standard `dotnet run --project JetDatabaseWriter.FormatProbe` keeps producing the existing `format-probe-appendix-*.md` files unchanged.

### `DIAG_RT_PROBE` — single-file post-mortem ([RoundTripDiagnostic.cs](../../JetDatabaseWriter.FormatProbe/RoundTripDiagnostic.cs))

```pwsh
$env:DIAG_RT_PROBE   = "<DIAG_RT_KEEP work dir>\source.accdb"
$env:DIAG_RT_BASELINE = "<repo>\JetDatabaseWriter.Tests\Databases\NorthwindTraders.accdb"  # optional
dotnet run --project JetDatabaseWriter.FormatProbe
```

Writes `round-trip-diagnostic.md` next to `source.accdb`. Sections:
1. **§1 file-level page diff** — total size delta + every shared-range page that differs.
2. **§2 catalog diff** — every `MSysObjects` row in src that isn't in the baseline (Id/ParentId/Type/Flags/TDEF page).
3. **§3 MSysObjects TDEF (page 2)** — `row_count` / `num_idx` / `num_real_idx` deltas plus the per-real-idx 12-byte skip entries; surfaces hypothesis #2 (stale `usage_map_page` / `used_pages`).
4. **§4 index leaf splice verification** — re-decodes pages 8 + 2790 in both files, lists added / removed entries, asserts sort order.
5. **§5 new MSysObjects rows** — decoded `Id` / `ParentId` / `Type` / `Flags` / `Name` for every row whose name starts with `RT_`.
6. **§6 row → IndexKeyEncoder → splice key roundtrip** — re-encodes each new row's `Id` PK key and `(ParentId, Name)` composite key, then asserts presence on the spliced leaves. ❌ here = hypothesis #1.

First run against the failing `9ac4fd00…\source.accdb` showed every section green: keys match, sort order intact, only the `row_count` byte changed in the entire MSysObjects TDEF chain. So hypotheses #1 and #2 (as currently understood) are NOT the cause.

### `DIAG_RT_BISECT` — escalating-step regression bisector ([RoundTripBisect.cs](../../JetDatabaseWriter.FormatProbe/RoundTripBisect.cs))

```pwsh
$env:DIAG_RT_BISECT = "1"
dotnet run --project JetDatabaseWriter.FormatProbe
```

Copies `NorthwindTraders.accdb` once per step, runs the writer through an escalating action set (`N0` open/close → `N1` one table → `N2` two tables → `N3` add relationship → `N4` insert rows), and shells DAO compact for each via `C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe`. Output is one line per step:

```
[bisect] N0_OpenClose: ✅ OK
[bisect] N1_CreateOneTable: ❌ MSysDb
           stderr: Exception calling "CompactDatabase" ...
```

This gives the smallest writer surface that breaks DAO. Use it before adding new instrumentation — if `N1_CreateOneTable` already fails, the relationship/insert paths are off the critical path entirely.

### Hooking either probe into a fresh failure

The `DIAG_RT_KEEP` work dirs (`%TEMP%\JetDatabaseWriter.Tests.RoundTrip\<guid>\source.accdb`) survive failing test runs verbatim. Point `DIAG_RT_PROBE` at one of them to regenerate the markdown without re-running the writer; pair with `DIAG_RT_BISECT` to find the smallest reproducer to feed back into the probe.

## Background already captured

- [/memories/repo/round-trip-tests.md](memories/repo/round-trip-tests.md) — leaf-bytes verification + Jet4 layout invariants.
- [docs/design/catalog-index-maintenance-notes.md](docs/design/catalog-index-maintenance-notes.md) — design rationale for the splice approach.
- [docs/design/round-trip-diagnostics.md](docs/design/round-trip-diagnostics.md) — reusable bisection matrix (N1–N14) and page-dump probe recipe from the historical scratch tests; use when narrowing a new round-trip regression.
- README §"Round-trip through Microsoft Access Compact & Repair" — testing methodology and known limitation.
