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

Current `main` (splice enabled, **plus** the per-real-idx `num_idx_rows` fix landed 2026-05-02 — see Update §A below):

```
DAO err: 3011 [DAO.DbEngine]
The Microsoft Access database engine could not find the object 'MSysDb'.
```

If the splice in `InsertCatalogEntryAsync` ([JetDatabaseWriter/Core/AccessWriter.cs#L6176](JetDatabaseWriter/Core/AccessWriter.cs#L6176)) is disabled, DAO fails with a different error:

```
Exception calling "CompactDatabase" ... "Object invalid or no longer set."
```

So the splice changes which integrity check DAO trips on, but never produces a file DAO is willing to compact.

`'MSysDb'` is **not** a real catalog object. It's the row at `Id=268435456 / ParentId=251658242 / Type=2 / Flags=0x80000000` in `MSysObjects` (a Databases-properties entry). DAO references that name when its own consistency walk over the catalog stops being self-consistent — it's a symptom, not a missing object.

## What we know about the write path

`InsertCatalogEntryAsync` ([JetDatabaseWriter/Core/AccessWriter.cs#L6144](JetDatabaseWriter/Core/AccessWriter.cs#L6144)):

1. `InsertRowDataLocAsync(2, …)` writes the new MSysObjects row (Id = TDEF page, ParentId = `0x0F000001` Tables parent, Name, Type=1, etc.) and bumps the table's row count via `UpdateRowCountAsync`. **As of 2026-05-02 that method also bumps every per-real-idx `num_idx_rows` counter in lock-step** — see Update §A.
2. `TrySpliceCatalogIndexEntryAsync` ([JetDatabaseWriter/Core/AccessWriter.cs#L11928](JetDatabaseWriter/Core/AccessWriter.cs#L11928)) walks every real-idx slot, descends to the rightmost leaf, encodes the new composite key for that index, splices it into the existing entry list, and rewrites the leaf via `IndexLeafPageBuilder.BuildLeafPage`.

In the failing run the splice **does succeed** (returns true, no `_lastIncrementalBail` set) and the leaf bytes look correct under inspection.

## Page-level diff

### Original (full failing test — two tables + relationship)

```
Diff pages: 2, 5, 8, 2790, 2994, 2995
File size  : 12,320,768 → 12,386,304 (+64 KiB = +16 pages)
```

### N1 minimal reproducer (single `RT_Customers` table, no relationship, no inserts)

DAO still rejects this minimum repro with the same `'MSysDb'` error, so the relationship and insert paths are off the critical path.

```
Diff pages: 2, 8, 2790, 2994
File size  : 12,320,768 → 12,328,960 (+8 KiB = +2 pages)
```

| Page | What's there | Observed change in N1 |
|------|--------------|-----------------------|
| 2    | MSysObjects TDEF | 3 byte diffs only — `row_count`@0x10 +1, ri=0 entry-count u4@0x43 +1, ri=1 entry-count u4@0x4F +1. All three are correct +1 bumps. |
| 8    | MSysObjects PK (Id) leaf | Splice: 239 → 240 entries, new key `7F 80 00 0B C0` for `Id=3008`. Decodes losslessly, sort intact. |
| 2790 | MSysObjects ParentIdName composite leaf | Splice: 114 → 115 entries, new key `7F8F0000017F<RT_Customers GeneralLegacy bytes>0100`. Decodes losslessly, sort intact. |
| 2994 | New TDEF page for `RT_Customers` | Brand-new page — written entirely by us. **Untouched-by-Access territory.** |

Notes:
- Page 5 (the usage / allocation map) does **not** differ in N1, so any hypothesis tied to it is a non-starter for the minimum repro.
- Page 0 (the actual DBA / file-header page) does **not** differ. The previous draft of this doc called the page-2 diff a "DBA" change; that was wrong — page 2 is the MSysObjects TDEF, not the DBA.

## What I verified about the splice bytes

Decoding the rewritten pages with `IndexLeafIncremental.DecodeEntries` (via a temp test runner) confirms:

- **Page 8 (`Id` PK)**: 241 entries, sorted, every original key still decodes losslessly, two new entries `7F 80 00 0B C0` and `7F 80 00 0B C2` for `Id = 3008`/`3010`. `pref_len` recomputed from 0 to 1 (every key now starts with `7F`).
- **Page 2790 (ParentIdName composite)**: 116 entries, two new keys with the form `7F 8F 00 00 01 7F <Name GeneralLegacy bytes> 01 00` slotted into the `ParentId = 0x0F000001` (Tables) range. `pref_len` 1→4 because every entry now shares `7F 8F 00 00`.

Per [/memories/repo/round-trip-tests.md](memories/repo/round-trip-tests.md), this matches the previously verified state — the leaf-level encoding is correct.

## Why DAO still rejects it — hypothesis elimination matrix

DAO's compact reads each system table via the index, then re-reads it via the row data, and complains when the two views disagree. The error string `'MSysDb'` is consistently the *first* row DAO mentions when its catalog walk goes off the rails — it is not a missing object, it is the row whose existence DAO can no longer confirm because some earlier crosscheck failed.

Status legend: ✅ ruled out · ❌ ruled in (confirmed cause) · 🟡 open · ⏳ in progress

| # | Hypothesis | Status | Evidence / how it was ruled out |
|---|---|:---:|---|
| 1 | MSysObjects row payload disagrees with the spliced (ParentId, Name) / Id index keys (text encoding, NFC, compressed-Unicode prefix, ParentId byte order) | ✅ | FormatProbe §6 re-encodes each new row's `Id` PK key and `(ParentId, Name)` composite key via `IndexKeyEncoder` and finds both keys present on pages 8 and 2790. See [round-trip-diagnostic.md §6](#) regen output. |
| 2 | TDEF index slot's per-real-idx skip-block fields stale after writing a row | ✅ (partial — see Update §A) | The `usage_map_page` / `used_pages` / `first_dp` parts are clean: the splice doesn't change page count, so there's nothing for them to drift from. **However, the per-real-idx `num_idx_rows` u4 (offset +4 of each 12/8-byte slot) WAS drifting** — `UpdateRowCountAsync` only bumped `row_count`@0x10 and left the per-real-idx counters at the baseline value. Fixed 2026-05-02; the fix is necessary for correctness and is guarded by [JetDatabaseWriter.Tests/Core/TdefRowCountSyncTests.cs](../../JetDatabaseWriter.Tests/Core/TdefRowCountSyncTests.cs), but **does not on its own resolve the DAO `'MSysDb'` failure** — the round-trip tests still fail post-fix. So this hypothesis was real but is not the root cause; some other crosscheck is also failing. |
| 3 | MSysObjects.Id autonumber "next value" counter not advanced past the new row's Id (3008) | 🟡 | The probe's TDEF dump has no autonumber-counter byte change at any offset on page 2. Either the autonumber lives elsewhere (worth checking page 0/DBA + the ddl/0x14 area on each TDEF), or DAO doesn't crosscheck it during compact. **Action: extend FormatProbe §3 to dump the full DBA page (page 0) byte-diff and to locate the per-table autonumber counter (Jet stores it 4 bytes after `row_count` in modern formats — at 0x14 — but baseline shows 0 there, suggesting either Northwind has never advanced its counter or the counter lives on a different page).** |
| 4 | MSysRelationships / Type=8 MSysObjects entries created by `CreateRelationshipAsync` aren't covered by splice | ✅ | N1 minimum repro creates **only one table** with no relationship and no row inserts — and DAO still throws `'MSysDb'`. So the relationship/insert paths are off the critical path entirely. |
| 5 | The new TDEF page (2994) we write for `RT_Customers` is itself malformed in some byte DAO crosschecks | 🟡 | Page 2994 is the **only page in the N1 diff that originates entirely from our writer** with no analogous baseline page to diff against. FormatProbe currently has no §7 that validates a writer-emitted TDEF against an Access-authored TDEF (e.g. one of the other Northwind TDEFs). **Action: add a §7 that picks any baseline TDEF page, parses it, then byte-compares its structural fields (header, column descriptors, real-idx entries, usage_map_page chain, free space, row-count area) against page 2994 and lists every structural-field divergence.** This is now the highest-priority unexamined hypothesis. |
| 6 | The new MSysObjects row's variable-length area / null-bitmap / LvProp / Owner column bytes are subtly malformed (so the row decodes "fine" with our reader but trips DAO's stricter walk) | 🟡 | FormatProbe §5 only decodes Id / ParentId / Type / Flags / Name. The MSysObjects row also carries `DateCreate` / `DateUpdate` / `Owner` / `LvProp` / `LvExtra` / `Database` / `Connect` / `ForeignName` / `Flags` / `RmtInfoLong` / `RmtInfoShort` columns. **Action: extend §5 to dump the full raw byte slice of the new row + null-bitmap + var-offset table, and to bit-compare its column-descriptor layout against an existing Type=1 row (e.g. the `Categories` table's MSysObjects row).** Especially relevant: Owner is a binary SID-like blob that Access populates; do we leave it null where Access would write a default owner? |
| 7 | MSysObjects's *own* table-allocation usage map doesn't include page 2994 (so DAO's "walk every catalog row, verify its TDEF page is owned by *some* table's allocation map" check fails) | 🟡 | Strictly speaking the new TDEF page belongs to the *new* table, not MSysObjects. But Access also tracks "all tables' TDEF pages" via the global page-allocation map (the DBA-rooted bitmap on page 1). If our `AllocateTablePagesAsync` doesn't mark 2994 in the global map, DAO won't see it as a legitimately-allocated page and any catalog row that points at it looks like a dangling reference. **Action: extend FormatProbe to read the page-allocation map and verify that page 2994 is marked allocated.** |
| 8 | Test infrastructure is itself wrong (false-positive failure) | ✅ | The N1 reproducer is invoked from FormatProbe with a hand-rolled writer-only flow — no xUnit, no `RoundTripSession`, no test-fixture copy logic. It produces the same DAO `'MSysDb'` error. So the failure is in the writer output, not in the test harness. |
| 9 | DAO is checking some legacy MSysAccessStorage / MSysComplexColumns / MSysNavPaneGroups / MSysNameMap consistency we don't write to | 🟡 | These tables exist in Northwind but we only modify MSysObjects. If DAO's compact verifies that *every* MSysObjects row has corresponding entries in (e.g.) MSysAccessStorage or MSysNameMap, our new row would be dangling. Lower priority because Access-UI-created tables don't *immediately* get NavPane / NameMap entries either, and DAO compacts those files fine. **Action: post-N1, add a UI-created-table baseline (add a single empty table in Access UI, save, close) and diff the resulting MSysAccessStorage / MSysNameMap row counts vs. ours.** |
| 10 | Our new MSysObjects row's `ParentId = 0x0F000001` (Tables group) is wrong — should be `0x0F000002` (Databases) or some other group for the kind of Type=1 entry DAO recognizes during compact | 🟡 | Existing Northwind Type=1 user-table rows do use `ParentId = 0x0F000001`, so this is most likely correct. But verify that the *exact* set of integer group IDs DAO recognizes for Type=1 is just `0x0F000001` and there isn't a second one (e.g. for hidden / system tables) we should be using for some catalog rows. **Action: dump every Type=1 row's ParentId in baseline Northwind and confirm a single group ID.** |
| 11 | The new TDEF page header lacks a back-pointer to the MSysObjects row that owns it (e.g. `parentId` field on the TDEF), or the back-pointer is wrong | 🟡 | Modern Jet/ACE TDEF headers do contain a `tdef_id` / parent reference. **Action: cover this under #5's §7 — include the TDEF header's parent / id fields in the structural diff.** |

### Provenance of the matrix

- All ✅ rulings come from a **single FormatProbe run against the N1 minimum reproducer** (`%TEMP%\JetDatabaseWriter.RtBisect\N1_CreateOneTable.accdb` produced by `DIAG_RT_BISECT=1`). They are reproducible by running the bisect, then `DIAG_RT_PROBE=<…>\N1_CreateOneTable.accdb` and reading [round-trip-diagnostic.md](../../JetDatabaseWriter.FormatProbe/RoundTripDiagnostic.cs).
- 🟡 entries are listed in priority order. **#5 (new-TDEF structural diff) is the next thing to land in FormatProbe.** Once §7 exists, expect either a smoking-gun field divergence on page 2994 (turns ❌) or another row of clean greens (turns ✅, escalating to #6/7).
- The original tests (`SinglePk_AndSingleColumnFk_…` and `CompositePk_AndMultiColumnFk_…`) are **not** assumed correct — but they *are* corroborated by the writer-only N1 reproducer, which is independent of the test harness. So the failure is in the writer's output, not in the test code.



## Where the splice is byte-clean

These have been verified and **should not regress**:

- Jet4 leaf-page header layout (`prev`@12, `next`@16, `tail`@20, `pref_len`@24, bitmask@0x1B, first_entry@0x1E0) — see `Constants.IndexLeafPage.Jet4`.
- Big-endian intermediate child pointers in `IndexBTreeBuilder` and reads in `IndexBTreeSeeker.SelectChildPage` / `IndexLeafIncremental.DecodeIntermediateChildPointer`.
- `pref_len` recomputation on splice.
- Bitmask + `free_space` recomputation.
- Sort + tie-break ordering preserves Access-authored entry order.

## Updates

### §A — per-real-idx `num_idx_rows` sync fix landed (2026-05-02)

`AccessWriter.UpdateRowCountAsync` now also mirrors the row-count delta into the per-real-idx `num_idx_rows` u32 field (offset +4 of each 12-byte/8-byte slot in the leading real-idx skip block at `[_tdef.BlockEnd, _tdef.BlockEnd + numRealIdx * _tdef.RealIdxEntrySz)`). Per mdbtools `HACKING.md` the slot is `unknown(4) + num_idx_rows(4) + unknown(4)`. This is necessary because every real-idx in MSysObjects covers every catalog row, so the per-index counter MUST equal `row_count` at all times.

Guarded by [JetDatabaseWriter.Tests/Core/TdefRowCountSyncTests.cs](../../JetDatabaseWriter.Tests/Core/TdefRowCountSyncTests.cs) (6 tests across Jet3 / Jet4 / ACE × {CreateTable, InsertRow}; verified inverse: reverting the fix fails 3/6 tests with messages like `TDEF page 14, real-idx 0: num_idx_rows=0 but row_count=7`).

**However, the round-trip tests still fail with the same `'MSysDb'` error after this fix.** So the hypothesis-#2 drift was real, but it was not the root (or only) cause of the DAO compact failure. Hypotheses #3 / #5 / #6 / #7 / #9 / #10 / #11 remain in scope; the rank-ordering in "Recommended path forward" below is unchanged.

## Recommended path forward (post-elimination)

The matrix above leaves five 🟡 hypotheses. Rank-ordered next steps:

1. **Land FormatProbe §7 — new-TDEF structural diff (hypothesis #5).** Pick a baseline Type=1 TDEF page (e.g. `Categories`'s TDEF) and structurally diff page 2994 against it: header block, column descriptor array, real-idx slot array, usage-map-page chain, free-space, row-count area, padding bytes. Report every divergent field. Highest priority because page 2994 is the only page in the N1 diff that has no Access-authored counterpart to diff against, and the splice/TDEF-summary hypotheses are now ruled out.
2. **Extend §5 — full row diff (hypothesis #6).** Dump the new MSysObjects row's raw byte slice plus null-bitmap + var-offset table, and bit-compare its column-descriptor layout against an Access-authored Type=1 row's. Look especially at the `Owner` blob (do we leave it null?), `LvProp` (long-value props), `DateCreate` / `DateUpdate` (do we write a timestamp?), and the trailing variable-column section.
3. **Add allocation-map probe (hypothesis #7).** Read the global page-allocation bitmap rooted at the DBA page and confirm page 2994 is marked allocated. If not, our `AllocateTablePagesAsync` is updating the per-table usage map but not the global one.
4. **Hunt the autonumber counter (hypothesis #3).** It's not at TDEF offset 0x14 in our diff, so dump every byte of page 0 (DBA) and any MSysObjects-related metadata page to find where the next-Id counter lives. If found and not advanced past 3008, that's the cause.
5. **ParentId / sibling-table sanity (hypothesis #10).** One-line probe: `SELECT DISTINCT ParentId FROM MSysObjects WHERE Type=1` against the baseline. If multiple distinct values, we may be putting our new row under the wrong group.

Hypothesis #11 (TDEF back-pointer) folds into #5's structural diff. Hypothesis #9 (MSysAccessStorage / MSysNameMap) is the lowest-priority follow-up because Access UI add-table doesn't touch them eagerly either.

A useful tactical knob already in place: `TrySpliceCatalogIndexEntryAsync`'s `_lastIncrementalBail` plumbing, plus `DIAG_RT_KEEP=1` work-dir preservation. Run `DIAG_RT_BISECT=1` to regenerate `N1_CreateOneTable.accdb`, then `DIAG_RT_PROBE=…\N1_CreateOneTable.accdb` to regenerate the diagnostic markdown after each FormatProbe extension.

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

First run against the failing `9ac4fd00…\source.accdb` showed only `row_count`@0x10 changed and §6 keys-on-leaf all green. That single-byte-changed observation was itself the smoking gun for the per-real-idx `num_idx_rows` drift (Update §A): the bumped `row_count` should have been mirrored to the two per-real-idx counters at 0x43 and 0x4F, and wasn't. After landing the fix the FormatProbe output reproduced in §3 above shows all three counters bumping in lock-step (`+1`/`+1`/`+1`).

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
