# Round-Trip Test Failures — Investigation Status

**Status:** Both gating tests still fail under DAO Compact & Repair with err 3011 `'MSysDb'`. Multiple structural fixes have landed (TDEF header, per-table usage-map page, catalog row var-length area, `MSysObjects.Owner`). Byte-level decode of the new MSysObjects catalog row body now confirms the row is **structurally well-formed and shape-compatible with Access-authored Type=1 rows that DAO already accepts in the same file** (see "Empirical findings 2026-05-03" below). The previous leading suspect — `LvProp` being null — has been **partly walked back**: a sweep of every Type=1 row in `NorthwindTraders.accdb` initially appeared to find ~14 Access-authored rows with `LvProp = NULL`, but this was a script bug (PowerShell `[int]((14)/8)` rounds 1.75 → 2, hitting the wrong null-mask byte). The sweep needs to be redone before LvProp is conclusively cleared. **What is solidly established**: the writer's row body decodes correctly per the table below, and the structural defect must originate in either (a) the new TDEF page body beyond the validated header bytes, (b) the per-table usage-map page contents (not just its shape), or (c) some referenced structure not yet identified. Without an Access-UI-authored RT_-prefix baseline in the same file, the remaining hypotheses cannot be empirically ranked.

## Tests in question

Two tests in [AccessRoundTripTests.cs](../../JetDatabaseWriter.Tests/Core/AccessRoundTripTests.cs):

- `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
- `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`

Both:

1. Copy `NorthwindTraders.accdb` to a temp dir.
2. Use `AccessWriter` to add `RT_Customers` / `RT_Orders` (and `RT_OrderLines` for the composite case) plus a relationship.
3. Close the writer, then shell to a bitness-matched `powershell.exe` and call `DAO.DBEngine.120.CompactDatabase(src, dst)`.
4. Re-open the compacted file and assert schema and rows survived.

DAO refuses to compact the post-write file, the script returns exit code 1, and `RoundTripSession.RunDaoCompact` throws:

```
DAO err: 3011 [DAO.DbEngine]
The Microsoft Access database engine could not find the object 'MSysDb'.
```

`'MSysDb'` is **not** a real catalog object. It is the `MSysObjects` row at `Id=268435456 / ParentId=251658242 / Type=2 / Flags=0x80000000` (a Databases-properties entry). DAO names it when its catalog walk fails — symptom, not a missing object.

## Current diff (N1 minimum reproducer)

A single empty `RT_Customers` table, no relationship, no inserts:

```
Diff pages (shared range): 2, 8, 2790, 2994
Pages added              : new TDEF, new PK leaf, new usage-map page (+3)
```

| Page | What's there | Status |
|---|---|---|
| 2 | MSysObjects TDEF | ✅ correct (`row_count` +1, per-real-idx entry counts +1) |
| 8 | MSysObjects PK (`Id`) leaf | ✅ splice byte-clean |
| 2790 | MSysObjects `ParentIdName` composite leaf | ✅ splice byte-clean |
| 2994 | MSysObjects data page hosting the new row | ✅ row body structurally well-formed (decodes correctly; entries / EOD / nullmask / varLen all match Access-authored Type=1 rows) — see "Empirical findings 2026-05-03" |
| New TDEF page (3008) | RT_Customers TDEF | 🔴 only superficially verified ("byte-shape-identical to baseline" header bytes); body / index descriptors / column descriptors not yet diffed against an Access-authored RT_-prefix table |
| New PK leaf (3009) | RT_Customers PK leaf (page_type=0x04, parent=3008) | 🟡 empty leaf, well-formed in isolation |
| New usage-map page (3010) | RT_Customers used/free pages (page_type=0x01) | 🟡 byte-identical to a baseline usage-map page in isolation — bitmap *contents* (which bits set) not yet diffed against an Access-UI baseline |

Page 5 of NorthwindTraders is `page_type=0x02` (a TDEF page), NOT a global page-allocation map. There is no evidence Jet/ACE has a centralised GPM separate from per-table usage maps; an earlier note in this document claiming page 5 was the GPM was **incorrect** and has been removed.

## Fixes already landed (do not regress)

All in `AccessWriter.cs` / `Constants.cs` unless noted; Jet4/ACE only where applicable.

### TDEF page

- `0x02..0x03` = `pgSz - tdef_len - 8` (free-space hint; verified 52/52 across NorthwindTraders TDEFs).
- `0x0C..0x0F` = `0x00000659` (format-wide magic; verified across NorthwindTraders + Jackcess V2003/V2007/V2010 fixtures).
- `0x18` = `0x01` whenever any column has the autonumber flag bit (`Flags & 0x04`) — `PatchAutoNumFlag`.
- `0x37..0x3A` = `(row=0x00, page LE3)` pointing at the table's usage-map page — `used_pages` — `PatchUsageMapPointers`.
- `0x3B..0x3E` = `(row=0x01, page LE3)` pointing at the same usage-map page — `free_pages` — `PatchUsageMapPointers`.

### Per-table usage-map page

`AppendUsageMapPageAsync` allocates a `page_type=0x01` data page hosting two 69-byte rows: row 0 = `used_pages` map, row 1 = `free_pages` map. Each row is `0x00` (inline type-0 marker) followed by 68 zero bitmap bytes, packed at page tail offsets `pgSz - 69` and `pgSz - 138`. Byte-identical to an Access-authored usage-map (`02 00 BB 0F 76 0F` row-offset table, `64 0F` free-space slot).

### MSysObjects index splice

`InsertCatalogEntryAsync` calls `IndexMaintainer.TrySpliceCatalogIndexEntryAsync`, which walks every real-idx slot of MSysObjects, descends to the rightmost leaf, encodes the new composite key, and splices it in via `IndexLeafPageBuilder.BuildLeafPage`. Verified byte-clean for both real-idx slots (PK `Id`, composite `ParentIdName`).

Supporting fixes (each was a real defect; each is regression-guarded):

- Per-real-idx `num_idx_rows` mirrors `row_count` delta (`UpdateRowCountAsync`). Guarded by [TdefRowCountSyncTests.cs](../../JetDatabaseWriter.Tests/Core/TdefRowCountSyncTests.cs).
- Real-idx `flags` byte at Jet4 phys+46 (was previously offset 42) with the `0x80` UNKNOWN bit always set. Guarded by [IndexFlagCombinationsTests.cs](../../JetDatabaseWriter.Tests/Core/IndexFlagCombinationsTests.cs).
- Jet4 leaf-page header offsets: `prev` @ 12, `next` @ 16, `tail` @ 20, `pref_len` @ 24, bitmask @ 0x1B, first-entry @ 0x1E0 — constants in `Constants.IndexLeafPage.{Jet3,Jet4}`. Hard-coded `AsSpan(8/12/16/20, ...)` reads in Jet4 paths are a bug; use `IndexLeafPageBuilder.LeafPageLayout`.
- Intermediate index pages use 4-byte **big-endian** child pointers (despite other 32-bit fields on the page being LE). `IndexBTreeBuilder` writes BE; `IndexBTreeSeeker.SelectChildPage` and `IndexLeafIncremental.DecodeIntermediateChildPointer` read BE.

### MSysObjects catalog row (`SerializeRow`, `InsertCatalogEntryAsync`)

- The variable-column offset table now declares the **schema's full var-column count** (`maxDefinedVarIdx + 1`), not just `maxNonNullVarIdx + 1`. Trailing null vars get zero-length entries pointing at EOD, matching the layout Access stamps on every catalog row.
- `MSysObjects.Owner` is populated with the constant 2-byte blob `0x71 0x10` (`Constants.SystemObjects.DefaultOwnerBlob`). Verified across all Type=1 user-table and Type=8 relationship rows in NorthwindTraders.

## Format facts established during the investigation

### Jet4 / ACE TDEF invariants (verified across 52 NorthwindTraders + Jackcess V2003/V2007/V2010 TDEFs)

- `0x00..0x01` = `02 01` (page-type marker)
- `0x02..0x03` = `pgSz - tdef_len - 8` (free-space hint)
- `0x04..0x07` = next-TDEF-page chain (0 when fits in one page)
- `0x08..0x0B` = `tdef_len`
- `0x0C..0x0F` = `0x00000659` (format-wide magic)
- `0x10..0x13` = live-row count (`Constants.TableDefinition.RowCountOffset`)
- `0x14..0x17` = autonumber next value (when applicable)
- `0x18` = autonum_flag (`0x01` if any column has the autonumber bit)
- `_tdef.NumCols - 5` = `0x4E` (mdbtools "magic" byte)
- `0x37..0x3A` = `used_pages` pointer `(row=0x00, page LE3)`
- `0x3B..0x3E` = `free_pages` pointer `(row=0x01, page LE3)`

The per-table usage-map *data* page is `page_type = 0x01` (NOT `0x05`; mdbtools spec mislabels this — `0x05` is LVAL). Access uses row 0 for used, row 1 for free, both pointing at the same usage-map data page.

### Jet3 vs Jet4/ACE differences

| Field | Jet3 (.mdb, Access 97) | Jet4 / ACE (.mdb 2000+/.accdb) |
|---|---|---|
| TDEF `0x0C..0x0F` | Per-table page number (semantics unclear; see `Jet3Test.mdb`) | Format-wide magic `0x00000659`. Stamp on Jet4/ACE only. |
| Page size | 2048 | 4096 |
| Format byte at file offset `0x14` | `0x00` | `0x01` (Jet4) / `0x02` (ACE) |
| Database header magic | `Standard Jet DB\0` | `Standard Jet DB\0` (Jet4) / `Standard ACE DB\0` (ACE) |
| Compressed text marker | N/A — ANSI codepage | `FF FE` prefix + 1 byte/char when all chars ≤ U+00FF and length ≥ 3 |
| Real-idx physical descriptor size | 39 bytes | 51 bytes |
| Real-idx entry block size in TDEF | 8 bytes/idx | 12 bytes/idx |
| TDEF `BlockEnd` (column-descriptor start) | 43 | 63 |
| Column descriptor size | 18 bytes | 25 bytes |
| Column-name length prefix | 1 byte | 2 bytes (LE) |
| Column-name encoding | ANSI codepage 1252 | UTF-16 LE (or compressed via `FF FE` marker) |

## Empirical findings 2026-05-03

Closed via byte-level decode of N1 reproducer (single empty `RT_Customers` table) against `NorthwindTraders.accdb`:

### Writer's RT_Customers row body decodes correctly

Page 2994, row 9 (the new row), file offset `0xB64`, length **77 bytes** (not 186 — earlier "186-byte" estimate confused a deleted row 8 at `0x8BB1` for live content):

| Field | Value |
|---|---|
| `numCols` | 17 |
| `Id` (fixed off 2..5) | 3008 (`0x00000BC0`) |
| `ParentId` (fixed off 6..9) | 251658241 (`0x0F000001`) |
| `Type` (fixed off 10..11) | 1 |
| `DateCreate` / `DateUpdate` (fixed off 12..27) | current UTC (8 bytes each) |
| `Flags` (fixed off 28..31) | 0 |
| Var entry[0] = `Name` | row[32..45] = 14 bytes `FF FE 52 54 5F 43 75 73 74 6F 6D 65 72 73` (compressed UCS-2 `"RT_Customers"`) |
| Var entry[1] = `Owner` | row[46..47] = 2 bytes `71 10` (`Constants.SystemObjects.DefaultOwnerBlob`) |
| Var entry[2..10] = `Database`/`Connect`/`ForeignName`/`RmtInfoShort`/`RmtInfoLong`/`Lv`/`LvProp`/`LvModule`/`LvExtra` | all NULL — zero-length entries pointing at EOD=48 |
| Trailer | EOD=48, var-table (11 entries reverse-packed), `varLen=11`, `nullMask=FF 00 00` |

EOD/var-table/null-mask/var-length all parse cleanly; structurally indistinguishable from an Access-authored Type=1 catalog row except that:

1. `Name` is written as compressed UCS-2 (`FF FE` marker + 1B/char) instead of raw UTF-16. Already ruled out as the blocker (hypothesis #12; re-confirmed today).
2. `LvProp` (varIdx 8) and `LvExtra` (varIdx 10) are NULL.

### `LvProp` NULL — verdict deferred (script bug)

A sweep across every Type=1 row in `NorthwindTraders.accdb` was attempted via `dump-type1.ps1` to determine whether DAO tolerates a NULL `LvProp` on user-table catalog rows. The sweep reported ~14 system rows (`MSysObjects`, `MSysACEs`, `MSysQueries`, `MSysRelationships`, `MSysComplexColumns`, all `MSysComplexType_*`, `MSysAccessStorage`, `MSysNameMap`, `MSysNavPane*`, `MSysResources`, plus user table `f_086A23…_Data`) with `LvProp = NULL`. **This was a script bug**: PowerShell's `[int]((14)/8)` rounds the float 1.75 → 2 (banker's rounding), so the script was reading `nullMask[2]` instead of `nullMask[1]` for column 14's null bit. The actual nullmask bit for `LvProp` was therefore not checked. The sweep needs to be redone with `[int][math]::Floor((14)/8)` (or `14 -shr 3`) before this hypothesis can be conclusively closed.

Independent observation — `Companies` (Access-authored Type=1 row at id=50) has `entry[8] = 52`, `entry[9] = 64`: a 12-byte `LvProp` payload that does NOT begin with the `MR2\0` property-block magic (bytes are `40 00 34 00 34 00 34 00 34 00 34 00`, which look like jump-table offsets, not a property block). So Access's `LvProp` semantics on user-table catalog rows are not necessarily "emit a property block" — they appear to be more opaque than `JetExpressionConverter.BuildLvPropBlob` assumes. This warrants its own investigation before any synthetic LvProp blob is added.

- All 49 rows use `ParentId = 0x0F000001` (re-confirms hypothesis #10).

### Where the defect must live

Given (a) the catalog row body is well-formed and (b) the MSysObjects TDEF and both spliced index leaves are byte-clean, the failure must originate in one of the three pages **added** by the writer (which are NOT visible in the shared-range diff), in the new row's variable-length payload (LvProp/LvExtra; not yet conclusively cleared), or in some structure that references those new pages without proper accounting:

1. **New TDEF page body (3008)** has only been verified "byte-shape-identical to baseline" at the header level. The column descriptors, real-idx descriptors, and trailing index/usage-map block beyond offset 0x3F have not been diffed against an Access-UI-created RT_-prefix table on the same file.
2. **New per-table usage-map page (3010)** is byte-identical to a baseline usage-map *in isolation* — but the bitmap contents (which bits are set) have not been diffed. An empty bitmap may be inconsistent with the TDEF claiming the table owns a PK leaf page (3009).
3. **New PK leaf (3009)** is well-formed in isolation, but its parent-page back-pointer chain (`prev`/`next` on the leaf, parent linkage in the TDEF) has not been independently verified end-to-end.
4. **`LvProp` on the new row** — re-verify with a corrected null-mask check; if Access actually requires a non-null payload here for user-table rows, this is still in play.

## Hypothesis matrix

| # | Hypothesis | Status | Evidence |
|---|---|:---:|---|
| 1 | Splice key encoding wrong (text NFC, ParentId byte order, etc.) | ✅ ruled out | FormatProbe §6 re-encodes each new row's key via `IndexKeyEncoder` and finds both keys present on the spliced leaves. |
| 2 | Per-real-idx skip-block stale (`num_idx_rows` not bumped with `row_count`) | ✅ fixed | `UpdateRowCountAsync` mirrors row-count delta into `num_idx_rows`. Guarded. |
| 3 | Real-idx `flags` byte at wrong offset / missing `0x80` UNKNOWN bit | ✅ fixed | Now uses `IndexLayout.FlagsOffsetWithinPhys` and `Constants.TableDefinition` flag constants. Guarded. |
| 4 | Relationship / row-insert paths break compact | ✅ ruled out | N1 reproducer is a single empty `CreateTableAsync` call and still fails. |
| 5 | New TDEF page malformed | ✅ fixed | All sub-faults landed; TDEF header bytes 0..0x3F now byte-shape-identical to baseline. |
| 6 | New MSysObjects row variable-length area | 🟡 partly walked back (2026-05-03) | Byte-level decode confirms the row is well-formed structurally, but the earlier "DAO accepts LvProp=NULL on 14 baseline rows" sweep was a script bug; LvProp NULL is not yet conclusively safe. Companies' real `LvProp` is 12 opaque bytes, NOT an `MR2\0` property block. |
| 7 | Global page-allocation map (page 5) missing the new TDEF / PK-leaf / usage-map pages | ❌ withdrawn (2026-05-03) | Page 5 of NorthwindTraders is `page_type=0x02` (a TDEF page), NOT a global page-allocation map. Jet/ACE does not appear to have a centralised GPM separate from per-table usage maps. |
| 8 | Test infra wrong | ✅ ruled out | FormatProbe N1 reproducer is hand-rolled writer-only; same DAO error. |
| 9 | DAO requires `MSysAccessStorage` / `MSysComplexColumns` / `MSysNavPaneGroups` / `MSysNameMap` rows | 🟡 lowest priority | Access-UI-created tables don't immediately get NavPane / NameMap entries either. |
| 10 | New row's `ParentId = 0x0F000001` is the wrong group | ✅ ruled out | Existing Northwind Type=1 user-table rows already use `0x0F000001`. |
| 11 | TDEF back-pointer wrong | ✅ ruled out | All TDEF header bytes outside the originally-divergent fields are byte-identical to baseline. |
| 12 | Catalog `Name` column written in compressed UCS-2 (`FF FE` marker + 1B/char) instead of raw UTF-16 LE | ✅ ruled out as the blocker | Disabling `EncodeJet4Text` compression entirely did not change bisect output. *Cosmetically* unlike Access-authored rows but not the trigger. |

## Recommended next steps (priority order)

1. **Create an Access-UI baseline.** Open `NorthwindTraders.accdb` in Microsoft Access, manually create one empty table named `RT_Customers` with columns matching the test's `(CustomerID INT PK AUTOINC, Name VARCHAR(100) NOT NULL)`, save, and close. Diff this byte-for-byte against the writer's N1 output (FormatProbe `DIAG_RT_BISECT` step `N1_CreateOneTable`). Without this baseline, every remaining hypothesis is speculative — there is no other available ground truth for what the new TDEF body, new PK leaf, new usage-map bitmap, and new MSysObjects row's `LvProp` payload should each contain for an empty user table.
2. **Redo the LvProp NULL sweep** with a corrected null-mask byte index (`[math]::Floor((14)/8)` or `14 -shr 3` instead of the buggy `[int]((14)/8)` which banker's-rounds 1.75 → 2). If any Access-authored Type=1 user-table row genuinely has `LvProp = NULL` (nullmask bit 14 clear), the writer's choice is safe; if every Access-authored user-table row has `LvProp` non-null, the writer must synthesize a payload.
3. **Inspect the writer's new TDEF body (page 3008)** beyond the header bytes — column descriptors, the two real-idx physical descriptors (51 bytes each on Jet4/ACE at offset 63 + n*51), and the trailing index/usage-map block.
4. **Inspect the writer's new usage-map page (3010)** bitmap contents, comparing which bits are set against an Access-authored single-table usage-map for an empty table.
5. **Aesthetic follow-up** (does not block these tests): switch `EncodeJet4Text` to emit uncompressed UTF-16 in catalog `Name` columns for byte-level consistency with Access-authored output. Re-confirmed on 2026-05-03 via the bisect: forcing `compressible = false` in `EncodeJet4Text` produced identical bisect output (still `MSysDb` at N1), so this is cosmetic, not a DAO trigger.

## FormatProbe diagnostic harness

`JetDatabaseWriter.FormatProbe` carries two opt-in probes for triaging this regression. Both are off by default — they only fire when the matching environment variable is set.

### `DIAG_RT_PROBE` — single-file post-mortem

[RoundTripDiagnostic.cs](../../JetDatabaseWriter.FormatProbe/RoundTripDiagnostic.cs):

```pwsh
$env:DIAG_RT_PROBE    = "<work-dir>\source.accdb"
$env:DIAG_RT_BASELINE = "<repo>\JetDatabaseWriter.Tests\Databases\NorthwindTraders.accdb"  # optional
dotnet run --project JetDatabaseWriter.FormatProbe
```

Writes `round-trip-diagnostic.md` next to `source.accdb`. Sections:

1. file-level page diff,
2. catalog diff (rows in src missing from baseline),
3. MSysObjects TDEF header + per-real-idx skip entries,
4. index leaf splice verification (decodes the spliced leaves in src + baseline, lists added/removed entries, asserts sort order),
5. decoded `Id` / `ParentId` / `Type` / `Flags` / `Name` for every new RT_* row,
6. row → `IndexKeyEncoder` → splice key roundtrip (asserts presence on spliced leaves).

### `DIAG_RT_BISECT` — escalating-step regression bisector

[RoundTripBisect.cs](../../JetDatabaseWriter.FormatProbe/RoundTripBisect.cs):

```pwsh
$env:DIAG_RT_BISECT = "1"
dotnet run --project JetDatabaseWriter.FormatProbe
```

Copies `NorthwindTraders.accdb` once per step, runs the writer through an escalating action set (`N0` open/close → `N1` one table → `N2` two tables → `N3` add relationship → `N4` insert rows), and shells DAO compact for each. One line per step:

```
[bisect] N0_OpenClose: ✅ OK
[bisect] N1_CreateOneTable: ❌ MSysDb
```

`N1_CreateOneTable` is the smallest writer surface that breaks DAO. If it already fails, the relationship / insert paths are off the critical path.

### Hooking either probe into a fresh failure

The `DIAG_RT_KEEP=1` work dirs (`%TEMP%\JetDatabaseWriter.Tests.RoundTrip\<guid>\source.accdb`) survive failing test runs verbatim. Point `DIAG_RT_PROBE` at one of them to regenerate the markdown without re-running the writer; pair with `DIAG_RT_BISECT` to find the smallest reproducer to feed back into the probe.

## Background

- `/memories/repo/round-trip-tests.md` (agent memory, not in repo) — leaf-bytes verification + Jet4 layout invariants.
- [catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md) — design rationale for the splice approach (now landed).
- [round-trip-diagnostics.md](round-trip-diagnostics.md) — reusable bisection matrix (N1–N14) and page-dump probe recipe from the historical scratch tests.
- README §"Round-trip through Microsoft Access Compact & Repair" — testing methodology and known limitation.
