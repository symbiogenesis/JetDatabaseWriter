# Round-Trip Test Failures — Investigation Status

**Last verified:** 2026-05-03 — both tests still fail with the same surface error described below.

## Tests in question

Two tests in [JetDatabaseWriter.Tests/Core/AccessRoundTripTests.cs](../../JetDatabaseWriter.Tests/Core/AccessRoundTripTests.cs):

- `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
- `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`

Both:

1. Copy `NorthwindTraders.accdb` to a temp dir.
2. Use `AccessWriter` to add `RT_Customers` / `RT_Orders` (and `RT_OrderLines` for the composite case) plus a relationship.
3. Close the writer, then shell out to a bitness-matched `powershell.exe` and call `DAO.DBEngine.120.CompactDatabase(src, dst)`.
4. Re-open the compacted file and assert schema and rows survived.

DAO refuses to compact the post-write file, the script returns exit code 1, and `RoundTripSession.RunDaoCompact` throws:

```
DAO err: 3011 [DAO.DbEngine]
The Microsoft Access database engine could not find the object 'MSysDb'.
```

`'MSysDb'` is **not** a real catalog object. It is the `MSysObjects` row at `Id=268435456 / ParentId=251658242 / Type=2 / Flags=0x80000000` (a Databases-properties entry). DAO names it when its catalog-walk consistency check fails — the name is a symptom, not a missing object.

## Confirmed root cause (leading suspect)

The new TDEF page emitted by `AccessWriter.CreateTableAsync` is missing its **per-table usage-map page pointer**.

A throwaway `NewTdefStructuralProbe` test (recipe in [round-trip-diagnostics.md](round-trip-diagnostics.md), deliberately not committed) dumped the writer-emitted RT_Customers TDEF alongside an Access-authored baseline (Northwind `Customers`) and surfaced these specific divergences in the TDEF header:

| Field (Jet4 offset) | Writer | Access baseline | Verdict |
|---|---|---|---|
| `ump_page` @ 0x36 (3-byte page ref) | **`00-00-00`** | `00-00-2E` | **Smoking gun.** Zero here makes the table look like it owns no data pages. |
| `free_ump_page` @ 0x3A (3-byte page ref) | **`00-00-00`** | `00-01-2E` | Same root cause as `ump_page`. |
| `autonum_flag` @ 0x18 | **`0x00`** | `0x01` | Secondary suspect — Access sets `0x01` whenever any column has the autonumber bit; RT_Customers has `IsAutoIncrement = true`. |
| `tdef_len` @ 0x08 | 251 | 371 | Expected — fewer columns / shorter index block. |
| `next_tdef_pg` @ 0x0C | 0 | 1625 | Expected — TDEF fits in one page so no continuation. |
| `row_count` @ 0x10 | 0 | 16 | Expected — empty new table. |
| All other header bytes | identical | identical | — |

Access's compact pass walks every catalog row, dereferences `ump_page` to enumerate the table's data pages, and aborts when that pointer is zero. The aborted walk surfaces as "could not find object `'MSysDb'`" because `MSysDb` is the *first* row the catalog walk expected to revisit on its second pass.

The `autonum_flag` divergence is a probable secondary fix (Access checks it before consulting the autonum-next counter at 0x14) but the `ump_page` zero is the bigger structural break.

## Recommended fix (next step)

In `AccessWriter.CreateTableAsync` (or wherever the TDEF page bytes are finalised before `InsertCatalogEntryAsync`):

1. Allocate a fresh page, format it as a **table-data usage-map page** (page_type = `0x05`), and either zero its bitmap (no data pages owned yet) or set the bit for the table's first allocated leaf data page if one exists.
2. Patch the new TDEF's bytes 0x35..0x37 (`ump_row` byte + `ump_page` 3-byte page ref) to point at the freshly-allocated usage-map page.
3. Patch `free_ump_row` / `free_ump_page` at 0x39..0x3B (probably the same usage-map page or a sibling — confirm by adding an empty table in the Access UI and re-probing the resulting TDEF).
4. Set `autonum_flag` @ 0x18 to `0x01` whenever any column has `IsAutoIncrement = true`.

After landing the `ump_page` fix, re-run `DIAG_RT_BISECT=1` and expect either `N1_CreateOneTable: ✅ OK` (root cause confirmed) or a different DAO error (one of the secondary hypotheses promoted to leading suspect).

## What the splice path is NOT (ruled-out hypotheses)

`InsertCatalogEntryAsync` ([JetDatabaseWriter/Core/AccessWriter.cs](../../JetDatabaseWriter/Core/AccessWriter.cs) `InsertCatalogEntryAsync`) calls `IndexMaintainer.TrySpliceCatalogIndexEntryAsync` ([JetDatabaseWriter/Internal/IndexMaintainer.cs](../../JetDatabaseWriter/Internal/IndexMaintainer.cs) `TrySpliceCatalogIndexEntryAsync`), which walks every real-idx slot of MSysObjects, descends to the rightmost leaf, encodes the new composite key, splices it into the existing entry list, and rewrites the leaf via `IndexLeafPageBuilder.BuildLeafPage`.

The splice is **byte-correct**. Decoding both rewritten leaves with `IndexLeafIncremental.DecodeEntries` (FormatProbe §4 + §6 of the diagnostic) confirms:

- **Page 8 (`Id` PK)**: 239 → 241 entries, sorted, every original key still decodes losslessly, two new entries `7F 80 00 0B C0` and `7F 80 00 0B C2` for `Id = 3008` / `3010`. `pref_len` recomputed from 0 to 1 (every key now starts with `7F`).
- **Page 2790 (`ParentIdName` composite)**: 114 → 116 entries, two new keys `7F 8F 00 00 01 7F <Name GeneralLegacy bytes> 01 00` slotted into the `ParentId = 0x0F000001` (Tables) range. `pref_len` 1 → 4 because every entry now shares `7F 8F 00 00`.

Disabling the splice surfaces a *different* DAO error (`Object invalid or no longer set`), confirming the splice is necessary for the row to be findable at all; enabling it is not what trips the `'MSysDb'` walk.

| # | Hypothesis | Status | Evidence |
|---|---|:---:|---|
| 1 | Splice key encoding wrong (text NFC, ParentId byte order, etc.) | ✅ ruled out | FormatProbe §6 re-encodes each new row's key via `IndexKeyEncoder` and finds both keys present on pages 8 + 2790. |
| 2 | Per-real-idx skip-block stale (`num_idx_rows` not bumped with `row_count`) | ✅ fixed | Was a real defect — `UpdateRowCountAsync` now mirrors row-count delta into `num_idx_rows` (Jet4 offset `+4` of each 12-byte slot). Guarded by [TdefRowCountSyncTests.cs](../../JetDatabaseWriter.Tests/Core/TdefRowCountSyncTests.cs). Did not on its own resolve `'MSysDb'`. |
| 3 | Real-idx `flags` byte stamped at wrong offset / missing `0x80` UNKNOWN bit | ✅ fixed | Writer was using offset 42 (the unknown gap) instead of 46, and emitting `0x00` / `0x01` instead of `0x80` / `0x81` / `0x89`. Now uses `IndexLayout.FlagsOffsetWithinPhys` and the `Constants.TableDefinition` flag constants. Guarded by [IndexFlagCombinationsTests.cs](../../JetDatabaseWriter.Tests/Core/IndexFlagCombinationsTests.cs). Did not on its own resolve `'MSysDb'`. |
| 4 | Relationship / row-insert paths break compact | ✅ ruled out | The N1 minimum reproducer is a single empty `CreateTableAsync` call, no relationship, no inserts, and DAO still throws `'MSysDb'`. |
| 5 | New TDEF page malformed | ❌ **confirmed** | `ump_page` and `free_ump_page` are zero (see "Confirmed root cause" above). Folds in #7 — Access's per-table `ump_page` IS the per-table allocation map. |
| 6 | New MSysObjects row variable-length area / null-bitmap / `Owner` / `LvProp` malformed | 🟡 deferred | Decode of the spliced leaves shows the new row's PK and composite keys round-trip cleanly, suggesting the column data we hand to the encoder agrees with what we wrote. Re-examine only if `'MSysDb'` persists after the §5 fix. |
| 7 | Global page-allocation map missing the new TDEF page | 🟡 folds into #5 | Access's per-table `ump_page` is the relevant per-table allocation map; the global DBA-rooted bitmap is separate but a zero `ump_page` breaks the catalog walk before the global map matters. |
| 8 | Test infra wrong | ✅ ruled out | The N1 reproducer is invoked from FormatProbe with a hand-rolled writer-only flow — no xUnit, no `RoundTripSession`. Same DAO error. |
| 9 | DAO requires `MSysAccessStorage` / `MSysComplexColumns` / `MSysNavPaneGroups` / `MSysNameMap` rows | 🟡 lowest priority | Access-UI-created tables don't immediately get NavPane / NameMap entries either, and DAO compacts those files fine. |
| 10 | New row's `ParentId = 0x0F000001` is the wrong group | 🟡 low priority | Existing Northwind Type=1 user-table rows already use `0x0F000001`. |
| 11 | TDEF back-pointer wrong | ✅ ruled out | The structural-diff probe in "Confirmed root cause" above shows all header bytes outside the four divergent fields are byte-identical to the baseline. |

## Page-level diff (N1 minimum reproducer)

A single empty `RT_Customers` table, no relationship, no inserts:

```
Diff pages: 2, 8, 2790, <new TDEF page>
File size  : 12,320,768 → 12,328,960 (+8 KiB = +2 pages)
```

| Page | What's there | Observed change |
|------|--------------|-----------------|
| 2    | MSysObjects TDEF | 3 byte diffs: `row_count` @ 0x10 +1, ri=0 entry-count u4 @ 0x43 +1, ri=1 entry-count u4 @ 0x4F +1. All three correct. |
| 8    | MSysObjects PK (`Id`) leaf | Splice: 239 → 240 entries. New key `7F 80 00 0B C0` for `Id=3008`. |
| 2790 | MSysObjects `ParentIdName` composite leaf | Splice: 114 → 115 entries. New key `7F 8F 00 00 01 7F <RT_Customers GeneralLegacy bytes> 01 00`. |
| New TDEF page | RT_Customers TDEF | **Untouched-by-Access territory** — written entirely by us. The `ump_page` / `free_ump_page` zeros described above live here. |

Notes:

- Page 5 (the page-allocation map) does **not** differ in N1.
- Page 0 (the actual DBA / file header) does **not** differ.
- The exact page number of the new TDEF varies with what other writes have happened in the bisect step (the first appended page in N1, two appended pages in cases that emit indexes / data). Don't pin tests on that page number.

## Where the splice is byte-clean (do not regress)

- Jet4 leaf-page header layout: `prev` @ 12, `next` @ 16, `tail` @ 20, `pref_len` @ 24, bitmask @ 0x1B, first-entry @ 0x1E0. Constants in `Constants.IndexLeafPage.Jet4`.
- Big-endian intermediate child pointers: `IndexBTreeBuilder` writes BE; `IndexBTreeSeeker.SelectChildPage` and `IndexLeafIncremental.DecodeIntermediateChildPointer` read BE.
- `pref_len` recomputed on splice; bitmask + `free_space` recomputed; sort + tie-break ordering preserves Access-authored entry order.
- Per-real-idx `num_idx_rows` mirrors `row_count` (fix #2 above).
- Real-idx `flags` byte at Jet4 phys+46 with the `0x80` UNKNOWN bit always set (fix #3 above).

## FormatProbe diagnostic harness

`JetDatabaseWriter.FormatProbe` carries two opt-in probes for triaging this regression. Both are off by default — they only fire when the matching environment variable is set, so the standard `dotnet run --project JetDatabaseWriter.FormatProbe` keeps producing the existing `format-probe-appendix-*.md` files unchanged.

### `DIAG_RT_PROBE` — single-file post-mortem

[RoundTripDiagnostic.cs](../../JetDatabaseWriter.FormatProbe/RoundTripDiagnostic.cs):

```pwsh
$env:DIAG_RT_PROBE    = "<work-dir>\source.accdb"
$env:DIAG_RT_BASELINE = "<repo>\JetDatabaseWriter.Tests\Databases\NorthwindTraders.accdb"  # optional
dotnet run --project JetDatabaseWriter.FormatProbe
```

Writes `round-trip-diagnostic.md` next to `source.accdb`. Sections:

1. **§1 file-level page diff** — total size delta + every shared-range page that differs.
2. **§2 catalog diff** — every `MSysObjects` row in src that isn't in the baseline.
3. **§3 MSysObjects TDEF (page 2)** — `row_count` / `num_idx` / `num_real_idx` deltas plus the per-real-idx skip entries.
4. **§4 index leaf splice verification** — re-decodes pages 8 + 2790 in both files, lists added / removed entries, asserts sort order.
5. **§5 new MSysObjects rows** — decoded `Id` / `ParentId` / `Type` / `Flags` / `Name` for every row whose name starts with `RT_`.
6. **§6 row → IndexKeyEncoder → splice key roundtrip** — re-encodes each new row's PK + composite keys and asserts presence on the spliced leaves.

### `DIAG_RT_BISECT` — escalating-step regression bisector

[RoundTripBisect.cs](../../JetDatabaseWriter.FormatProbe/RoundTripBisect.cs):

```pwsh
$env:DIAG_RT_BISECT = "1"
dotnet run --project JetDatabaseWriter.FormatProbe
```

Copies `NorthwindTraders.accdb` once per step, runs the writer through an escalating action set (`N0` open/close → `N1` one table → `N2` two tables → `N3` add relationship → `N4` insert rows), and shells DAO compact for each via `C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe`. Output is one line per step:

```
[bisect] N0_OpenClose: ✅ OK
[bisect] N1_CreateOneTable: ❌ MSysDb
```

`N1_CreateOneTable` is the smallest writer surface that breaks DAO. If it already fails, the relationship / insert paths are off the critical path.

### Hooking either probe into a fresh failure

The `DIAG_RT_KEEP=1` work dirs (`%TEMP%\JetDatabaseWriter.Tests.RoundTrip\<guid>\source.accdb`) survive failing test runs verbatim. Point `DIAG_RT_PROBE` at one of them to regenerate the markdown without re-running the writer; pair with `DIAG_RT_BISECT` to find the smallest reproducer to feed back into the probe.

## Background already captured

- [/memories/repo/round-trip-tests.md](../../memories/repo/round-trip-tests.md) — leaf-bytes verification + Jet4 layout invariants.
- [docs/design/catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md) — design rationale for the splice approach (now landed).
- [docs/design/round-trip-diagnostics.md](round-trip-diagnostics.md) — reusable bisection matrix (N1–N14) and page-dump probe recipe from the historical scratch tests.
- README §"Round-trip through Microsoft Access Compact & Repair" — testing methodology and known limitation.
