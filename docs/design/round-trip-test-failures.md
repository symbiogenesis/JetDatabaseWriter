# Round-Trip Test Failures — Investigation Status

**Status (2026-05-04):** All catalog index issues are **RESOLVED**. Three fixes landed: prefix compression cap (2026-05-03), entry-start bitmask sentinel (2026-05-04), and split-path `maxPrefixLength` cap in `TryBuildSplitLeafPages` (2026-05-04). DAO Compact & Repair now **succeeds** for both N1 (single table) and N2+ (multiple tables + relationships) — the `MSysDb (3011)` and `Object invalid or no longer set` errors are gone. However, DAO **drops rows** from writer-created user tables during compact (row count = 0 post-compact). This is the same issue tracked by `DaoValidationTests` ("Unrecognized database format" when DAO opens writer-created TDEFs). The remaining blocker is a per-table TDEF page layout incompatibility — the catalog is correct.

**Fixes landed (all necessary, collectively sufficient for catalog correctness):**

- ✅ **LvProp**: `Constants.SystemObjects.DefaultLvPropPlaceholder` (12 zero bytes) stamped when `JetExpressionConverter.BuildLvPropBlob` returns null.
- ✅ **MSysACEs**: `InsertAceRowsForTableAsync` inserts 3 ACE rows (owner/admins/users) per new user table. Harvests the Admins-group SID dynamically. Gated on `catalogFlags == 0` (user tables only). Column name corrected from `"Inheritable"` to `"FInheritable"` (the TDEF column name for the boolean ACE field).
- ✅ **GPM (page 1) ruled out for append-only writes**: Page 1's bitmap uses convention "1 = free, 0 = in-use". Pages appended beyond original file size already have bits = 0 (in-use by default).
- ✅ **TDEF magic stamps (`0x00000659`)**: Stamped in column descriptors (bytes 1–4), real-idx physical descriptors (first 4 bytes), and logical-idx entry descriptors (first 4 bytes) across `BuildTDefPagesWithIndexOffsets`, `BuildMSysObjectsTDef`, and `RelationshipManager.EmitFkLogicalIdxAsync`.
- ✅ **Real-idx flags byte**: `0x80` bit set at `Constants.TableDefinition.Jet4.RealIdx.FlagsOffset` for FK backing indexes.
- ✅ **DB-header modify counter at `0x0E02`**: Manually patched from `0x00` to `0x04` — **RULED OUT** (still fails).
- ✅ **Prefix compression cap** (2026-05-03): `BuildLeafPage` now accepts optional `maxPrefixLength` parameter. `TrySpliceCatalogIndexEntryAsync` and `TryAppendToTailLeafAsync` read the existing page's `pref_len` before decoding and pass it to `BuildLeafPage`, preventing the writer from increasing prefix compression beyond what was on disk. Result: page 8 `pref_len` stays 0 (was being recomputed to 1), page 2790 `pref_len` stays 1 (was being recomputed to 4). Free-space values now match the DAO baseline.
- ✅ **Entry-start bitmask sentinel** (2026-05-04): `BuildLeafPage` now writes a sentinel bit at the position one past the last entry in the entry-start bitmask. Access/DAO always writes this sentinel (verified on every leaf page in NorthwindTraders.accdb) and validates it during Compact & Repair. The N1 reproducer (single `CreateTableAsync`) now **passes DAO compact** with this fix. Five test helper `CountLeafEntries` methods updated to subtract 1 from the bitmask popcount to account for the sentinel.
- ✅ **Split-path `maxPrefixLength` cap** (2026-05-04): `TryBuildSplitLeafPages` now accepts `int maxPrefixLength` and forwards it to every `BuildLeafPage` call. All three call sites (`TrySpliceCatalogIndexEntryAsync`, `TrySurgicalCrossLeafMaintainAsync`, and the incremental splice path) read the original leaf's `pref_len` and pass it through. This prevents split-product pages from getting unrestricted prefix compression (N2 reproducer: page 2790 pref was growing 1→4, page 3019 was getting pref=16). **Combined with #16 and #17, this resolved the N2+ reproducer** — DAO Compact now succeeds for multiple `CreateTableAsync` calls.

**Root cause isolated via binary page-level bisection (2026-05-03):**

A binary patch experiment on the N1 reproducer (single empty `RT_Customers` table) revealed:
- Reverting ALL shared-range modified pages (2, 3, 8, 2790, 2994, 2998) to original while keeping 6 appended pages (3008–3013) → DAO compact **PASSES**.
- Testing each changed page individually (keep ONE modified, revert rest to original):

| Page | What it is | Keep-one result (pre-pref_len fix) | Keep-one result (post-pref_len fix) | Post-sentinel fix |
|---|---|---|---|---|
| 2 | MSysObjects TDEF | ✅ PASS | ✅ PASS | ✅ PASS |
| 3 | MSysACEs TDEF | ✅ PASS | ✅ PASS | ✅ PASS |
| 8 | MSysObjects PK (`Id`) index leaf | 🔴 `MSysDb (3011)` | 🔴 `The search key was not found in any record` | ✅ PASS (N1) |
| 2790 | MSysObjects `ParentIdName` composite index leaf | 🔴 `MSysDb (3011)` | 🔴 `MSysDb (3011)` | ✅ PASS (N1) |
| 2994 | MSysObjects data page (new catalog row) | ✅ PASS | 🔴 `Object invalid or no longer set` (test creates 2 tables + rel) | ✅ PASS (N1) |
| 2998 | MSysACEs data page | ✅ PASS | ✅ PASS | ✅ PASS |

**Post-pref_len-fix observations:**
- Page 8's error changed from `MSysDb (3011)` to `The search key was not found in any record` — the bitmask/entry layout is closer to correct but still not right.
- Page 2790 still triggers `MSysDb (3011)` despite having only **1 byte** difference vs the DAO baseline (a bitmask bit at offset `0x01DD`: Writer=`0x00`, DAO=`0x40`). This single bitmask bit marks the entry-start position of the spliced entry and is expected to differ (the writer's entry has a different sort key because its table Id = 3008 vs DAO's 2671). Yet DAO rejects the page.
- Page 2994 now also fails when tested with the full round-trip test (which creates 2 tables + relationship, unlike N1 which only creates 1). The error `Object invalid or no longer set` suggests a cascading catalog-consistency issue when multiple catalog rows are present.

The DAO-authored baseline probe (see [DaoBaselineProbe.cs](../../JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs)) has produced empirical ground truth: a copy of `NorthwindTraders.accdb` to which the **same** `RT_Customers` table was added via `DAO.DBEngine.120` (the engine path Access UI uses) survives DAO compact ✅, while the writer's copy of the same fixture fails ❌.

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
Diff pages (shared range): 2, 3, 8, 2790, 2994, 2998
Pages added              : new TDEF, new PK leaf, new usage-map page (+3 to +6 depending on rels/ACEs)
```

| Page | What's there | Keep-one bisect | Status |
|---|---|:---:|---|
| 2 | MSysObjects TDEF | ✅ PASS | Correct (`row_count` +1, per-real-idx entry counts +1) |
| 3 | MSysACEs TDEF | ✅ PASS | Correct (`row_count` +3, ACE rows inserted with `FInheritable` column name fix) |
| 8 | MSysObjects PK (`Id`) leaf | 🔴 **FAIL** | Post-pref_len fix: error changed from `MSysDb (3011)` to `search key not found`. 18 byte diffs vs baseline (expected: new entry). `pref_len=0` now matches baseline. `free_space=1456` matches DAO baseline. **Post-sentinel fix: ✅ PASS (N1).** |
| 2790 | MSysObjects `ParentIdName` composite leaf | 🔴 **FAIL** | Post-pref_len fix: still `MSysDb (3011)`. Only **1 byte** diff vs DAO baseline (bitmask bit at `0x01DD`). `pref_len=1` matches baseline. `free_space=10` matches DAO baseline. ~1479 byte diffs vs baseline (expected: entries shift after sorted insertion). **Post-sentinel fix: ✅ PASS (N1).** |
| 2994 | MSysObjects data page hosting the new row | 🔴 **FAIL** | `Object invalid or no longer set` when tested with full 2-table+rel test. N1 (single table) bisection was ✅ PASS. |
| 2998 | MSysACEs data page | ✅ PASS | ACE rows correctly inserted |
| New TDEF page (3008) | RT_Customers TDEF | ✅ (appended pages all pass) | Header + magic stamps + usage-map pointers all correct |
| New PK leaf (3009) | RT_Customers PK leaf (page_type=0x04, parent=3008) | ✅ | Empty leaf, well-formed |
| New usage-map page (3010) | RT_Customers used/free pages (page_type=0x01) | ✅ | Byte-identical to baseline |

Page 5 of NorthwindTraders is `page_type=0x02` (a TDEF page), NOT a global page-allocation map. An earlier note claiming page 5 was the GPM was incorrect. **Page 1 is the actual global page-allocation map** (see §"DAO baseline differences §2" below), but it is irrelevant for append-only writes — appended pages are inherently "in-use" without a GPM update.

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

`InsertCatalogEntryAsync` calls `IndexMaintainer.TrySpliceCatalogIndexEntryAsync`, which walks every real-idx slot of MSysObjects, descends to the rightmost leaf, encodes the new composite key, and splices it in via `IndexLeafPageBuilder.BuildLeafPage`. **Binary page-level bisection identified the spliced leaf pages (8 and 2790) as the root cause of the DAO rejection.** A prefix compression cap fix plus a bitmask sentinel fix (see below) have resolved the N1 (single-table) case — DAO now accepts the writer's output for a single `CreateTableAsync` call. The N2 (two-table) case still fails, likely due to the second splice operating on pages already modified by the first.

Supporting fixes (each was a real defect; each is regression-guarded):

- Per-real-idx `num_idx_rows` mirrors `row_count` delta (`UpdateRowCountAsync`). Guarded by [TdefRowCountSyncTests.cs](../../JetDatabaseWriter.Tests/Core/TdefRowCountSyncTests.cs).
- Real-idx `flags` byte at Jet4 phys+46 (was previously offset 42) with the `0x80` UNKNOWN bit always set. Guarded by [IndexFlagCombinationsTests.cs](../../JetDatabaseWriter.Tests/Core/IndexFlagCombinationsTests.cs).
- Jet4 leaf-page header offsets: `prev` @ 12, `next` @ 16, `tail` @ 20, `pref_len` @ 24, bitmask @ 0x1B, first-entry @ 0x1E0 — constants in `Constants.IndexLeafPage.{Jet3,Jet4}`. Hard-coded `AsSpan(8/12/16/20, ...)` reads in Jet4 paths are a bug; use `IndexLeafPageBuilder.LeafPageLayout`.
- Intermediate index pages use 4-byte **big-endian** child pointers (despite other 32-bit fields on the page being LE). `IndexBTreeBuilder` writes BE; `IndexBTreeSeeker.SelectChildPage` and `IndexLeafIncremental.DecodeIntermediateChildPointer` read BE.
- **Prefix compression cap (2026-05-03):** `BuildLeafPage` now accepts optional `maxPrefixLength` parameter. When splicing into an existing leaf, the caller reads the page's original `pref_len` and passes it to cap the recomputed prefix. This prevents the writer from increasing prefix compression beyond what was on disk — DAO rejects pages whose `pref_len` grows (entries shift position, bitmask becomes inconsistent with what DAO expects). Applied in both `TrySpliceCatalogIndexEntryAsync` and `TryAppendToTailLeafAsync`.

### MSysObjects catalog row (`SerializeRow`, `InsertCatalogEntryAsync`)

- The variable-column offset table now declares the **schema's full var-column count** (`maxDefinedVarIdx + 1`), not just `maxNonNullVarIdx + 1`. Trailing null vars get zero-length entries pointing at EOD, matching the layout Access stamps on every catalog row.
- `MSysObjects.Owner` is populated with the constant 2-byte blob `0x71 0x10` (`Constants.SystemObjects.DefaultOwnerBlob`). Verified across all Type=1 user-table and Type=8 relationship rows in NorthwindTraders.
- `MSysObjects.LvProp` is populated with `Constants.SystemObjects.DefaultLvPropPlaceholder` (12 zero bytes) when `JetExpressionConverter.BuildLvPropBlob` returns null. Sets `nullMask` bit 14 to match DAO layout.

### MSysACEs rows (`InsertAceRowsForTableAsync`, added 2026-05-03)

- `InsertAceRowsForTableAsync(tdefPageNumber, ct)` inserts 3 ACE rows per new user table (owner / admins / users).
- Called from `CreateTableInternalAsync` after `InsertCatalogEntryAsync`, gated on `catalogFlags == 0` (user tables only, not system tables).
- Finds MSysACEs TDEF via `_relationships.FindSystemTableTdefPageAsync("MSysACEs", ct)`.
- Harvests the Admins-group SID dynamically from existing ACE rows (any SID > 2 bytes on an existing ACE data page).
- Uses `InsertSystemRowAndMaintainAsync` pattern (row insertion + index maintenance on MSysACEs).
- Constants in `Constants.Aces`: `DefaultAcm = 0x000FFEFF`, `OwnerSid = [0x71, 0x10]`, `UsersSid = [0x70, 0x10]`.
- Column values per row: `ObjectId = (int)tdefPageNumber`, `ACM = DefaultAcm`, `FInheritable = true`, `SID = <varies>`.
- **Bug fix (2026-05-03):** `SetValueByName("Inheritable", true)` was silently failing because the MSysACEs TDEF column is named `"FInheritable"`, not `"Inheritable"`. `TableDef.SetValueByName` returns without writing when the name doesn't match. Fixed to `SetValueByName("FInheritable", true)`.

### TDEF magic stamps (`0x00000659`, added 2026-05-03)

- Column descriptors: bytes 1–4 after the column-type byte stamped with `0x00000659` via `Wi32(page, o + 1, 0x00000659)`.
- Real-idx physical descriptors: first 4 bytes stamped with `0x00000659`.
- Logical-idx entry descriptors: first 4 bytes (at `logEntry - LogicalEntryFieldsOffset`) stamped with `0x00000659`.
- Applied in three code paths: `BuildTDefPagesWithIndexOffsets` (user tables), `BuildMSysObjectsTDef` (MSysObjects cols), `RelationshipManager.EmitFkLogicalIdxAsync` (FK backing indexes).
- **Binary page bisection confirmed**: TDEF pages (2, 3) pass individually — magic stamps are correct and not the trigger.

### DB-header modify counter — ruled out (2026-05-03)

Manually patching file offset `0x0E02` from `0x00` to `0x04` (matching DAO's bump) on a writer-produced file still fails DAO compact. The modify counter is not validated by DAO's compact path.

### GPM (page 1) — ruled out for append-only (2026-05-03)

Page 1 **is** the global page-allocation bitmap (confirmed by DAO baseline diff — see §"DAO baseline differences §2" below). Its convention is **1 = free, 0 = in-use**. Pages appended beyond the original file size have no corresponding bit (or their bit is already 0 = in-use). DAO only modifies page 1 when *reusing* free pages from the free list (clearing their "1 = free" bits to "0 = in-use"). The writer's append-only allocator never creates pages that need GPM updates — new pages are inherently "in-use" by virtue of not having a "free" bit set.

### Entry-start bitmask sentinel (2026-05-04)

`BuildLeafPage` now writes a sentinel bit at the position one past the last entry in the entry-start bitmask. Access/DAO always writes this sentinel (verified on every leaf page in NorthwindTraders.accdb) and validates it during Compact & Repair. Without the sentinel, DAO rejects the page even when all entry data is correct. Five test helper `CountLeafEntries` methods updated to subtract 1 from the bitmask popcount to account for the sentinel. **Combined with the prefix compression cap (#16), this resolved the N1 reproducer.**

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

### `LvProp` NULL — resolved (fix landed 2026-05-03)

> **Superseded:** The DAO baseline comparison (§"DAO baseline differences §4" below) conclusively showed DAO writes `LvProp` non-null on every user-table catalog row. Fix landed: `Constants.SystemObjects.DefaultLvPropPlaceholder` (12 zero bytes) is stamped when `JetExpressionConverter.BuildLvPropBlob` returns null. Necessary but not sufficient — DAO compact still rejected after this fix alone.

Note: An earlier `dump-type1.ps1` sweep had a script bug (PowerShell banker's rounding on the null-mask byte index) which produced incorrect results. The DAO baseline probe supersedes that sweep.

### ~~Where the defect must live~~ (superseded 2026-05-04)

> **Superseded:** The actual root cause was the index leaf pages (8, 2790) — specifically missing prefix compression cap and entry-start bitmask sentinel. All four candidates below were cleared by the N1 bisection + sentinel fix. This section is preserved for historical context only.

1. ~~**New TDEF page body (3008)**~~ — verified correct; appended pages all pass individually.
2. ~~**New per-table usage-map page (3010)**~~ — byte-identical to baseline.
3. ~~**New PK leaf (3009)**~~ — well-formed; passes individually.
4. ~~**`LvProp` on the new row**~~ — fix landed (12-byte placeholder); necessary but not the trigger.

## DAO baseline differences (2026-05-03 evening)

Empirical comparison of writer-authored vs DAO-authored `RT_Customers` against the same `NorthwindTraders.accdb` baseline. Source: `DIAG_RT_DAO_BASELINE` probe; full report at `%TEMP%\JetDatabaseWriter.RtDaoBaseline\dao-baseline-diff.md`.

**DAO compact verdict: ✅ accepts DAO-authored copy, ❌ rejects writer-authored copy with `MSysDb (3011)` from the same starting fixture.** This isolates the cause to the writer's output, not the test infra or the fixture.

### 1. Page reuse vs always-append (architectural)

DAO **adds 0 pages**, **reuses 5 pre-existing free pages** (2671 TDEF, 2997+3003 leaf-idx, 2998+3002 data, 2843 data); writer **always appends 3 new pages** (3008 TDEF, 3009 PK leaf, 3010 usage-map).

The reused pages in the baseline have first 8 bytes `09 01 F0 0F 4C 56 41 4C` — `page_type=0x09`, `freespace=0x0FF0`, ASCII tag `"LVAL"`. **This is Access's "freed page" sentinel** (NOT a usable LVAL — the type-9 marker means "this page is on the free list"). DAO's allocator finds these and overwrites them; the writer has no equivalent free-page-finder.

### 2. Page 1 is the global page-allocation map (revises 2026-05-03 morning withdrawal of hypothesis #7)

DAO modifies page 1 at offsets `0x0FD5` (`80 → 00`, clearing one bit) and `0x0FFF` (`7C → 70`). Page 1's first byte is `01 01` (type-0x01 data page). **Page 1 IS the global page-allocation map** — Jet/ACE does have one, just on page 1 rather than page 5 as the original (since-withdrawn) hypothesis assumed. Bit at `0x0FD5 bit 7` covers a specific page number; DAO clears it when claiming that page off the free list. The writer never updates page 1 — every newly-appended page is invisible to the global allocation map. **This is a concrete protocol gap.**

### 3. Page 3 is `MSysACEs` TDEF — DAO adds 3 ACE rows per new table

Page 3 first byte is `02 01` (TDEF). Its trailing payload contains the property-block strings `"ACM"`, `"Inheritable"`, `"ObjectId"`, `"SID"` — these are the column names of the `MSysACEs` (Access Control Entries) table. DAO bumps the row count at offset `0x10` from `0x02CE → 0x02D1` (Δ +3) and adds 3 ACE rows for `RT_Customers` (1 owner + 1 admins + 1 users, by Access convention). The new ACE rows land on data pages 2843 and 2998 (which DAO modifies) and corresponding leaf entries on page 2997. **The writer creates zero `MSysACEs` rows for new user tables.**

### 4. `LvProp` is non-null on the DAO-authored user-table row (closes hypothesis #6, fix landed 2026-05-03)

Writer row body (pre-fix, 77 bytes, page 2994): `nullMask = FF 00 00`. Bit 14 (`LvProp`, varIdx 8) clear → **NULL**.
DAO row body (99 bytes, page 2994): `nullMask = FF 40 00`. Bit 14 set → **NOT NULL**, with a 12-byte payload at varIdx 8.

DAO writes `LvProp` non-null on every user-table catalog row. The 12 bytes (`62 00 00 40 05 1B 0B 00 00 00 00 00`) do not begin with the `MR2\0` property-block magic and appear to vary across runs — likely uninitialized memory from DAO's authoring path. **Fix landed:** `Constants.SystemObjects.DefaultLvPropPlaceholder` (12 zero bytes) is stamped into `MSysObjects.LvProp` whenever `JetExpressionConverter.BuildLvPropBlob` returns null. Verified: writer-authored row now has `nullMask = FF 40 00` matching DAO. **DAO compact still rejects with `MSysDb (3011)` after this fix alone**, confirming LvProp is necessary but not sufficient.

Verified empirical column metadata for MSysObjects (via `AccessReader.ReadTableDefAsync(2)` on a writer-authored copy):

```
col  varIdx  fixedOff  type  flags  name
  0    0       0       0x04  0x13   Id
  1    0       4       0x04  0x13   ParentId
  2    0       0       0x0A  0x12   Name
  3    1       8       0x03  0x13   Type
  4    1      10       0x08  0x13   DateCreate
  5    1      18       0x08  0x13   DateUpdate
  6    1       0       0x09  0x32   Owner
  7    2      26       0x04  0x13   Flags
  8    2       0       0x0C  0x12   Database
  9    3       0       0x0C  0x12   Connect
 10    4       0       0x0A  0x12   ForeignName
 11    5       0       0x09  0x12   RmtInfoShort
 12    6       0       0x0B  0x12   RmtInfoLong
 13    7       0       0x0B  0x12   Lv
 14    8       0       0x0B  0x12   LvProp
 15    9       0       0x0B  0x12   LvModule
 16   10       0       0x0B  0x12   LvExtra
```

The earlier confusion about whether the 12-byte payload was at varIdx 8 (LvProp) or varIdx 9 (LvModule) is resolved: it is at varIdx 8 — **LvProp** has 12 bytes, **LvModule** has 0 bytes (NULL).

### 5. Format magic `0x00000659` stamped inside column descriptors — fix landed, ruled out as trigger

Inside DAO's TDEF column descriptors, the 4 bytes immediately after the column-type byte are `59 06 00 00` (`= 0x00000659`, the same format magic the writer already stamps in the TDEF header at offset `0x0C`). **Fix landed:** writer now stamps these in all three TDEF-building paths (`BuildTDefPagesWithIndexOffsets`, `BuildMSysObjectsTDef`, `RelationshipManager.EmitFkLogicalIdxAsync`). Also stamps real-idx physical and logical-idx entry descriptors. **Binary page bisection confirmed TDEF pages (2, 3) individually PASS** — magic stamps are correct and not the trigger.

### 6. Page 0 (DB header) — modify counter — RULED OUT

DAO bumps a single byte at file offset `0x0E02` from `0x00 → 0x04`. Manually patching this to match DAO's value still fails DAO compact. **Not validated by DAO's compact path.**

### 7. DAO uses raw UTF-16 in catalog `Name`; writer uses compressed UCS-2 (cosmetic, already known)

Re-confirmed — does not block compact (bisect proved this earlier).

### Likelihood ranking for the `MSysDb (3011)` trigger

**Updated 2026-05-04:** The sentinel bit fix resolved the N1 (single-table) reproducer — DAO now accepts the writer's output when creating a single table. The bisect probe confirms N0 and N1 pass, N2 (two tables) fails with `Object invalid or no longer set`. The remaining failure is in the N2+ code path where the second `CreateTableAsync` call's splice overflows the ParentIdName leaf and splits without the `maxPrefixLength` cap.

The root cause of the N1 failure was two missing pieces in `IndexLeafPageBuilder.BuildLeafPage`:

1. **Prefix compression growing beyond original** — fixed by the `maxPrefixLength` cap parameter.
2. **Missing entry-start bitmask sentinel** — Access/DAO writes a one-past-the-end bit in the bitmask; the writer was omitting it. Fixed by writing the sentinel after the last entry.

Root cause of N2+ failure (fixed 2026-05-04):

1. **Split path lacked `maxPrefixLength` cap** — `TryBuildSplitLeafPages` was calling `BuildLeafPage` without `maxPrefixLength`, so split-product pages got unrestricted prefix compression. Fixed by plumbing `int maxPrefixLength` through `TryBuildSplitLeafPages` and passing `originalPrefLen` from all three call sites.

## Hypothesis matrix

| # | Hypothesis | Status | Evidence |
|---|---|:---:|---|
| 1 | Splice key encoding wrong (text NFC, ParentId byte order, etc.) | ✅ ruled out (N1) | N1 passes DAO compact — key encoding is correct for single-table case. |
| 2 | Per-real-idx skip-block stale (`num_idx_rows` not bumped with `row_count`) | ✅ fixed | `UpdateRowCountAsync` mirrors row-count delta into `num_idx_rows`. Guarded. |
| 3 | Real-idx `flags` byte at wrong offset / missing `0x80` UNKNOWN bit | ✅ fixed | Now uses `IndexLayout.FlagsOffsetWithinPhys` and `Constants.TableDefinition` flag constants. Guarded. |
| 4 | Relationship / row-insert paths break compact | ✅ ruled out | N1 reproducer is a single empty `CreateTableAsync` call and still fails. |
| 5 | New TDEF page malformed | ✅ fixed | All sub-faults landed; TDEF pages individually pass binary bisection. |
| 6 | New MSysObjects row variable-length area | ✅ fixed | LvProp 12-byte placeholder landed. Data page (2994) individually passes binary bisection (N1 reproducer). |
| 7 | Global page-allocation map (page 1) | ✅ ruled out | Append-only pages already "in-use". |
| 8 | Test infra wrong | ✅ ruled out | FormatProbe N1 reproducer is hand-rolled writer-only; same DAO error. |
| 9 | MSysACEs rows missing or malformed | ✅ ruled out | MSysACEs pages (3, 2998) individually pass binary bisection. FInheritable column name bug fixed. |
| 10 | New row's `ParentId = 0x0F000001` is the wrong group | ✅ ruled out | Existing Northwind Type=1 user-table rows already use `0x0F000001`. |
| 11 | TDEF back-pointer wrong | ✅ ruled out | TDEF pages individually pass binary bisection. |
| 12 | Catalog `Name` compression (UCS-2 vs UTF-16) | ✅ ruled out | Cosmetic only; bisect proved. |
| 13 | Col-desc magic `0x00000659` inside TDEF column/index descriptors | ✅ fixed & ruled out | Stamps landed in all three TDEF paths. TDEF pages individually pass binary bisection. |
| 14 | DB-header modify counter at `0x0E02` | ✅ ruled out | Manual patch still fails. |
| **15** | **MSysObjects index leaf splice (pages 8 and 2790)** | ✅ **FIXED** | **Binary page-level bisection: pages 8 and 2790 each individually triggered DAO rejection.** Pref_len cap + sentinel fix resolved N1 (single table). Split-path `maxPrefixLength` cap fix resolved N2+ (multiple tables). |
| **16** | **Prefix compression (`pref_len`) growing beyond original** | ✅ **fixed** | `BuildLeafPage` was recomputing `pref_len` from scratch, increasing it beyond the original page's value (page 8: 0→1, page 2790: 1→4). This shifted entry positions and made the bitmask inconsistent. Fix: cap `pref_len` at the original page's value. Values now match baseline/DAO. |
| **17** | **Missing entry-start bitmask sentinel** | ✅ **fixed** | Access/DAO writes a one-past-the-end bit in the entry-start bitmask at the position immediately after the last entry. The writer was omitting this sentinel. Fix: `BuildLeafPage` now writes the sentinel after the entry loop. Verified on every leaf page in NorthwindTraders.accdb. |
| **18** | **Split-path `maxPrefixLength` cap missing** | ✅ **fixed** | `TryBuildSplitLeafPages` was calling `BuildLeafPage` without `maxPrefixLength`, allowing split-product pages to get unrestricted pref (N2: page 2790 pref 1→4, page 3019 pref=16). Fix: plumbed `int maxPrefixLength` through `TryBuildSplitLeafPages` and all three call sites now pass the original leaf's `pref_len`. |
| **19** | **User-table TDEF page layout incompatibility** | 🔴 **remaining blocker** | DAO Compact succeeds but drops rows from writer-created user tables (row count = 0 post-compact). Same root cause as `DaoValidationTests` skip ("Unrecognized database format"). Catalog index pages are now correct; the issue is in the per-table TDEF page itself. |
## Recommended next steps (priority order)

**All catalog index issues are resolved.** DAO Compact & Repair now succeeds for both N1 and N2+ scenarios. The remaining blocker is a per-table TDEF page layout incompatibility that causes DAO to drop rows from writer-created user tables during compact. Next steps focus on the TDEF issue:

1. ~~**Byte-level diff of pages 8 and 2790**~~ ✅ Done.
2. ~~**Inspect `pref_len` / prefix compression**~~ ✅ Done. Pref_len cap fix landed.
3. ~~**Fix the entry-start bitmask sentinel**~~ ✅ Done. Sentinel bit fix landed. N1 now passes.
4. ~~**Investigate N2 failure**~~ ✅ Done (2026-05-04). Root cause identified and fixed.
5. ~~**Fix `TryBuildSplitLeafPages` to pass `maxPrefixLength` cap**~~ ✅ Done (2026-05-04). Split-path cap fix landed. N2+ now passes DAO compact.
6. ~~**Investigate page 2994**~~ ✅ Resolved — was a cascading failure from corrupted split-product index pages; now passes with the split-path fix.
7. **Investigate user-table TDEF incompatibility** — DAO compact succeeds but drops rows (row count = 0 post-compact). Same issue as `DaoValidationTests` ("Unrecognized database format"). Needs byte-level comparison of a writer-created TDEF page vs a DAO-created one using the `DIAG_RT_DAO_BASELINE` probe.

## N2 failure analysis (2026-05-04) — RESOLVED

**Root cause (fixed 2026-05-04):** `TryBuildSplitLeafPages` (called from the overflow path of `TrySpliceCatalogIndexEntryAsync`) was not passing `maxPrefixLength` to `BuildLeafPage`. When the second `CreateTableAsync` overflowed page 2790, the resulting split pages got unrestricted prefix compression — `pref_len` grew from 1 (original/capped) to 4 on the rewritten page 2790 and 16 on the new page 3019. Fix: plumbed `int maxPrefixLength` through `TryBuildSplitLeafPages` and all three call sites now pass the original leaf's `pref_len`.

### Sequence of events

For the N2 scenario (`CreateTableAsync("RT_Customers")` then `CreateTableAsync("RT_Orders")`):

**First CreateTableAsync (RT_Customers):**
- ri=0 (ParentIdName, intermediate root p.7 → tail leaf chain): Descends to page 2790 (orig free=34, pref=1). Splices RT_Customers entry (~24 bytes). Rewritten with `maxPrefixLength: 1`. Result: free=10, pref=1. **No split.** Page 7 unmodified.
- ri=1 (Id PK, single leaf p.8): Splices entry (~9 bytes). Rewritten with `maxPrefixLength: 0`. Result: free=1456, pref=0. **No split.**

**Second CreateTableAsync (RT_Orders):**
- ri=0 (ParentIdName): Descends to page 2790 (now free=10, pref=1). Splices RT_Orders entry (~24 bytes) → **overflow** (free=10 < entry_size≈24). Triggers `TryGreedySplitLeafInN` → 2-page split:
  - Page 2790 (reused): free=493, **pref=4** ← BUG (should be capped at 1)
  - Page 3019 (new): free=3489, **pref=16** ← BUG (should be capped at 1)
  - Intermediate page 7 updated (free: 3506→3449) with new child pointer for page 3019. ✅ Correct.
  - Page 2996 (next leaf) prev pointer patched from 2790→3019. ✅ Correct.
  - Sibling chain: ...→ 2790 → 3019 → 2996 → (end). ✅ Correct.
- ri=1 (Id PK): Splices entry. Result: free=1447, pref=0. **No split.**

### Why DAO rejects the split pages

The prefix compression cap hypothesis (hypothesis #16) was confirmed for the non-split case in the N1 fix — DAO rejects pages whose `pref_len` exceeds the value it originally authored. The same principle applies after a split: DAO expects the split products to respect the tree's original prefix convention. The writer's split path in `TryBuildSplitLeafPages` was omitting the `maxPrefixLength` parameter, allowing `BuildLeafPage` to recompute prefix compression from scratch.

**Fix landed (2026-05-04):** `TryBuildSplitLeafPages` now accepts `int maxPrefixLength` and forwards it to every `BuildLeafPage` call:

```csharp
// FIXED: maxPrefixLength parameter caps pref_len at original
pageBytesAll[p] = IndexLeafPageBuilder.BuildLeafPage(
    layout,
    writer._pgSz,
    tdefPage,
    splitPages[p],
    prevPage: thisPrev,
    nextPage: thisNext,
    tailPage: 0,
    enablePrefixCompression: true,
    maxPrefixLength: maxPrefixLength);
```

All three call sites (`TrySpliceCatalogIndexEntryAsync`, `TrySurgicalCrossLeafMaintainAsync`, and the incremental splice path) read the original leaf's `pref_len` and pass it through.

### Observed page state (N2 bisect output, pre-fix)

| Page | Type | N1 state | N2 state (pre-fix) | Expected |
|---:|:---:|---|---|---|
| 7 | intermediate | Unmodified (free=3506) | Modified (free=3449) — new child for 3019 | ✅ |
| 8 | PK leaf | free=1456, pref=0 | free=1447, pref=0 | ✅ |
| 2790 | ParentIdName leaf | free=10, pref=1 | free=493, **pref=4** | Should be pref≤1 ✅ (fixed) |
| 2996 | ParentIdName tail | prev=2790 | prev=3019 | ✅ |
| 3019 | ParentIdName split | (doesn't exist) | free=3489, **pref=16** | Should be pref≤1 ✅ (fixed) |

### Fix — LANDED (2026-05-04)

`maxPrefixLength: originalPrefLen` is now passed to every `BuildLeafPage` call inside `TryBuildSplitLeafPages` (all three call sites: `TrySpliceCatalogIndexEntryAsync`, `TrySurgicalCrossLeafMaintainAsync`, and the incremental splice path). The `originalPrefLen` is read from the leaf page before the split decision point. DAO Compact & Repair now succeeds for N2+.

## FormatProbe diagnostic harness

`JetDatabaseWriter.FormatProbe` carries two opt-in probes for triaging this regression. Both are off by default — they only fire when the matching environment variable is set.

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

### `DIAG_RT_DAO_BASELINE` — DAO-authored ground-truth comparator

[DaoBaselineProbe.cs](../../JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs):

```pwsh
$env:DIAG_RT_DAO_BASELINE = "1"
dotnet run --project JetDatabaseWriter.FormatProbe
```

Makes two copies of `NorthwindTraders.accdb`. On copy A runs the writer's N1 step (`CreateTableAsync RT_Customers`); on copy B shells `DAO.DBEngine.120` from `SysWOW64\WindowsPowerShell` and creates the same table via `Database.CreateTableDef` / `TableDef.CreateField` / `TableDef.CreateIndex` / `TableDefs.Append` — the API path Microsoft Access UI uses internally. Then runs `DBEngine.CompactDatabase` on both, captures both verdicts, and emits a side-by-side report at `%TEMP%\JetDatabaseWriter.RtDaoBaseline\dao-baseline-diff.md` with:

1. Authoring + DAO-compact outcomes per copy (writer's must fail with `MSysDb`; DAO's must succeed — that's the validity check that the baseline is real).
2. File-level page-count and shared-range page-diff summaries vs the original NorthwindTraders.
3. RT_Customers catalog-row metadata in each (Id, ParentId, TDEF page).
4. A unified table of every changed/added page in either copy, labeled by page-type byte.
5. Side-by-side hex of the two `RT_Customers` TDEF pages (with byte-diff markers).
6. Located-and-extracted MSysObjects new-row body bytes from both copies (so `LvProp` / `LvExtra` payloads can be compared directly without going through any decoder).

Per-page raw bytes for every changed/added page are dumped to `pages\page<NNNNN>_{writer,dao}.bin` so a binary diff tool (e.g. `fc /b`, `vbindiff`, VS Code's hex editor) can be pointed at any specific page pair. This is how the byte-level diff work (step 1 of "Recommended next steps") was completed without manual Access-UI work.

### Hooking either probe into a fresh failure

The `DIAG_RT_KEEP=1` work dirs (`%TEMP%\JetDatabaseWriter.Tests.RoundTrip\<guid>\source.accdb`) survive failing test runs verbatim. Pair `DIAG_RT_BISECT` with `DIAG_RT_DAO_BASELINE` to find the smallest reproducer and then diff the writer's output against a DAO-authored ground truth for the same operation.

## Background

- [catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md) — design rationale for the splice approach (now landed).
- README §"Round-trip through Microsoft Access Compact & Repair" — testing methodology and known limitation.
