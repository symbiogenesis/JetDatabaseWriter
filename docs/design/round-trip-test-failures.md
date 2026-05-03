# Round-Trip Test Failures — Investigation Status

**Status:** Both gating tests still fail under DAO Compact & Repair with err 3011 `'MSysDb'`. The DAO-authored baseline probe (see [DaoBaselineProbe.cs](../../JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs)) has now produced empirical ground truth: a copy of `NorthwindTraders.accdb` to which the **same** `RT_Customers` table was added via `DAO.DBEngine.120` (the engine path Access UI uses) survives DAO compact ✅, while the writer's copy of the same fixture fails ❌. The probe's full diff (`%TEMP%\JetDatabaseWriter.RtDaoBaseline\dao-baseline-diff.md`) reveals **at least seven concrete structural differences** the writer is missing — see "DAO baseline differences (2026-05-03 evening)" below. Several of these are fundamental architectural gaps in the writer (page reuse vs always-append; missing global page-allocation map maintenance; missing `MSysACEs` rows; non-null `LvProp` on user-table catalog rows), not byte-level fixes.

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

### 5. Format magic `0x00000659` stamped inside column descriptors

Inside DAO's TDEF column descriptors, the 4 bytes immediately after the column-type byte are `59 06 00 00` (`= 0x00000659`, the same format magic the writer already stamps in the TDEF header at offset `0x0C`). The writer leaves these zero. Differences appear at TDEF offsets `0x4D-0x4F`, `0x66-0x68`, plus other interior magic stamps in the index-descriptor block.

### 6. Page 0 (DB header) — modify counter

DAO bumps a single byte at file offset `0x0E02` from `0x00 → 0x04`. Likely a "modification counter" or "schema version" field Access touches on every catalog write. The writer never updates the DB header.

### 7. DAO uses raw UTF-16 in catalog `Name`; writer uses compressed UCS-2 (cosmetic, already known)

Re-confirmed — does not block compact (bisect proved this earlier).

### Likelihood ranking for the `MSysDb (3011)` trigger

In rough order of likely contribution to DAO's compact rejection:

1. **#2 missing GPM update on page 1** — if pages 3008/3009/3010 aren't marked "in use" in the global page-allocation map, DAO's compact catalog walk will treat them as garbage referenced by a live TDEF, which exactly matches the symptom (DAO can't find an object whose pages "don't exist").
2. **#3 missing `MSysACEs` rows** — DAO compact may require every catalog entry to have at least one ACE; missing ACEs would cause an "object not found" during the security-descriptor pass.
3. **#4 NULL `LvProp`** — confirmed not the only issue (DAO succeeds on the DAO-authored copy which has #1+#2+#3 right *and* #4 right), but may still be a contributing factor on its own.

#5 (col-desc magic) and #6 (DB-header counter) are lower priority — bytes that Access stamps but DAO compact may tolerate when absent.

## Hypothesis matrix

| # | Hypothesis | Status | Evidence |
|---|---|:---:|---|
| 1 | Splice key encoding wrong (text NFC, ParentId byte order, etc.) | ✅ ruled out | FormatProbe §6 re-encodes each new row's key via `IndexKeyEncoder` and finds both keys present on the spliced leaves. |
| 2 | Per-real-idx skip-block stale (`num_idx_rows` not bumped with `row_count`) | ✅ fixed | `UpdateRowCountAsync` mirrors row-count delta into `num_idx_rows`. Guarded. |
| 3 | Real-idx `flags` byte at wrong offset / missing `0x80` UNKNOWN bit | ✅ fixed | Now uses `IndexLayout.FlagsOffsetWithinPhys` and `Constants.TableDefinition` flag constants. Guarded. |
| 4 | Relationship / row-insert paths break compact | ✅ ruled out | N1 reproducer is a single empty `CreateTableAsync` call and still fails. |
| 5 | New TDEF page malformed | ✅ fixed | All sub-faults landed; TDEF header bytes 0..0x3F now byte-shape-identical to baseline. |
| 6 | New MSysObjects row variable-length area | 🟡 partially fixed (2026-05-03 evening) | DAO writes 12-byte LvProp at varIdx 8; writer left it NULL. Fix landed: `Constants.SystemObjects.DefaultLvPropPlaceholder` stamps 12 zero bytes when no per-column properties are declared. Writer row now has `nullMask = FF 40 00` matching DAO. **DAO still rejects after this fix alone** — necessary but not sufficient. Remaining gaps in row body (raw UTF-16 vs compressed UCS-2 for Name, etc.) are cosmetic. |
| 7 | Global page-allocation map missing the new TDEF / PK-leaf / usage-map pages | 🔴 confirmed defect (2026-05-03 evening) | Page **1** (not page 5) IS the GPM. DAO modifies it at `0x0FD5` (clears one bit) and `0x0FFF` when claiming pages off the free list; writer never updates page 1. The earlier withdrawal of this hypothesis was based on the wrong page assumption. |
| 8 | Test infra wrong | ✅ ruled out | FormatProbe N1 reproducer is hand-rolled writer-only; same DAO error. |
| 9 | DAO requires `MSysACEs` / `MSysAccessStorage` / `MSysComplexColumns` / `MSysNavPaneGroups` / `MSysNameMap` rows | 🔴 confirmed for `MSysACEs` (2026-05-03 evening) | DAO baseline probe shows DAO bumps `MSysACEs` row count by 3 on page 3 TDEF (the per-table ACL entries: owner/admins/users), and writes new ACE rows on data pages 2843/2998 + leaf entries on 2997. Writer creates zero ACE rows. NavPane/NameMap remain low priority. |
| 10 | New row's `ParentId = 0x0F000001` is the wrong group | ✅ ruled out | Existing Northwind Type=1 user-table rows already use `0x0F000001`. |
| 11 | TDEF back-pointer wrong | ✅ ruled out | All TDEF header bytes outside the originally-divergent fields are byte-identical to baseline. |
| 12 | Catalog `Name` column written in compressed UCS-2 (`FF FE` marker + 1B/char) instead of raw UTF-16 LE | ✅ ruled out as the blocker | Disabling `EncodeJet4Text` compression entirely did not change bisect output. *Cosmetically* unlike Access-authored rows but not the trigger. |

## Recommended next steps (priority order)

The DAO baseline probe has now answered the previous "diff against ground truth" item; what remains is to land each of the protocol gaps it surfaced. **LvProp (item 4 below) is fixed but is not the silver bullet.** In rough order of likely contribution to the remaining `MSysDb (3011)` rejection:

1. **Implement global page-allocation map maintenance on page 1.** Every newly-allocated page (new TDEF, new PK leaf, new usage-map page) must clear its corresponding bit on page 1's bitmap. The exact bitmap layout (start offset, bit ordering, type-0 inline vs type-1 multi-page-pointer encoding) needs reverse-engineering from comparing pre-write vs post-write page-1 bytes across multiple Access-UI-authored single-table additions to the same fixture — the probe's `pages\page00001_dao.bin` snapshot is sufficient ground truth for `RT_Customers`. Note: page 1 is itself a data page hosting two ~69-byte usage-map rows; the bytes DAO modifies (`0x0FD5`, `0x0FFF`) lie inside the longer of those two rows, suggesting the GPM is a per-database (rather than format-wide) bitmap stored as a system-table usage map.
2. **Implement `MSysACEs` row creation per new user table.** DAO writes 3 ACE rows (owner / admins / users) on every `CreateTableDef`, hosted on existing data pages 2843/2998 with leaf entries on 2997. The probe's per-page bins (`pages\page02843_dao.bin`, `pages\page02998_dao.bin`, `pages\page02997_dao.bin`) contain the exact row bytes to model from. The writer's `MSysObjects` index-splice infrastructure can be reused for `MSysACEs`'s indexes.
3. **Investigate free-page reuse.** DAO finds and reuses pages bearing the `09 01 F0 0F 4C 56 41 4C` "freed page" sentinel (type 9, free-space `0x0FF0`, ASCII tag `"LVAL"`). The writer's append-only allocator never finds these. May or may not be required for compact correctness — DAO compact is what *creates* free pages, so a fresh-write file with no free pages may still be valid; defer until #1+#2 are landed and re-test.
4. **Synthesize a non-null `LvProp` for user-table catalog rows.** DAO writes ~12 bytes; `Companies` (existing) shows a similar pattern. Bytes do NOT begin with `MR2\0`, so this is NOT a property block — characterise the layout from comparing the probe's `pages\msysobjects_row_dao.bin` against several existing Access-authored Type=1 rows before implementing.
5. **Land #5 / #6 (col-desc magic stamps; DB-header counter)** as housekeeping after the test passes — these are cosmetic deltas DAO almost certainly tolerates, but byte-level parity would close the ground-truth diff entirely.
6. **Aesthetic: switch `EncodeJet4Text` to raw UTF-16 in catalog `Name`.** Re-confirmed cosmetic.

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

Per-page raw bytes for every changed/added page are dumped to `pages\page<NNNNN>_{writer,dao}.bin` so a binary diff tool (e.g. `fc /b`, `vbindiff`, VS Code's hex editor) can be pointed at any specific page pair. This is how step 1 of "Recommended next steps" above is now answered without manual Access-UI work.

### Hooking either probe into a fresh failure

The `DIAG_RT_KEEP=1` work dirs (`%TEMP%\JetDatabaseWriter.Tests.RoundTrip\<guid>\source.accdb`) survive failing test runs verbatim. Pair `DIAG_RT_BISECT` with `DIAG_RT_DAO_BASELINE` to find the smallest reproducer and then diff the writer's output against a DAO-authored ground truth for the same operation.

## Background

- `/memories/repo/round-trip-tests.md` (agent memory, not in repo) — leaf-bytes verification + Jet4 layout invariants.
- [catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md) — design rationale for the splice approach (now landed).
- [round-trip-diagnostics.md](round-trip-diagnostics.md) — reusable bisection matrix (N1–N14) and page-dump probe recipe from the historical scratch tests.
- README §"Round-trip through Microsoft Access Compact & Repair" — testing methodology and known limitation.
