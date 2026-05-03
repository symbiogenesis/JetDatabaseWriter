# Round-Trip Test Failures — Investigation Status

**Last verified:** 2026-05-03 (late) — both tests still fail. Three additional fixes have landed since the morning's update (TDEF `0x0C` magic constant, TDEF `0x02` free-space hint, 69-byte usage-map rows) and a fourth experiment was tried (disabling Jet4 text compression in `EncodeJet4Text`). **None** cleared the DAO `'MSysDb'` error in the bisect. The writer-emitted TDEF and usage-map data page are now byte-shape-identical to a comparable Access-authored baseline; the remaining defect is in **the catalog row variable-length area** (LvProp / Owner / null-bitmap / variable-length jump table), which is now the only structural difference left on the diff'd pages.

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

## Initial hypothesis (since superseded — see "What we got wrong about the format" below)

The new TDEF page emitted by `AccessWriter.CreateTableAsync` was first thought to be missing its **per-table usage-map page pointer**, with the suspect fields being `ump_page` @ 0x36 and `free_ump_page` @ 0x3A. That diagnosis was directionally right (the writer was emitting zeros where Access stamps a usage-map page reference) but the offsets, field widths, and usage-map page format were all wrong. The corrected layout is captured in "What we got wrong about the format" below; the corresponding fix landed and brought the writer-emitted TDEF byte-shape-identical to the baseline at those offsets.

The `autonum_flag` @ 0x18 divergence (Writer = `0x00`, Access = `0x01` whenever any column has the autonumber bit) was real and is also fixed.

Despite both fixes, DAO still throws `'MSysDb'` — see hypothesis #6 below for the current leading suspect (the new MSysObjects row's variable-length area).

## Attempted fix (2026-05-03) — reduced the divergence but did NOT clear `'MSysDb'`

Landed in `AccessWriter.CreateTableInternalAsync` (Jet4/ACE only):

1. **`AppendUsageMapPageAsync`** — allocates a fresh page formatted as a normal data page (`page_type = 0x01`, not `0x05` — see "What we got wrong about the format" below) carrying two 1-byte rows. Row 0 is the table's `used_pages` map, row 1 is `free_pages`; both consist of a single `0x00` byte (Access "inline" usage-map type-0 marker followed by a zero-length bitmap).
2. **`PatchUsageMapPointers`** — writes `(row=0x00, page_LE3)` at TDEF offset `0x37..0x3A` (`used_pages`) and `(row=0x01, page_LE3)` at `0x3B..0x3E` (`free_pages`). Both pointers reference the freshly-allocated usage-map data page.
3. **`PatchAutoNumFlag`** — sets TDEF byte `0x18` to `0x01` whenever any column has the autonumber flag bit (`Flags & 0x04`).

After the fix, the writer-emitted TDEF (page 3008 in N1) reads:

```
000: 02 01 00 00 00 00 00 00 FB 00 00 00 00 00 00 00
010: 00 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00      <- 0x18 = 0x01 ✅
020: 00 00 00 00 00 00 00 00 4E 02 00 01 00 02 00 01
030: 00 00 00 01 00 00 00 00 C2 0B 00 01 C2 0B 00 00      <- used_pages=00·BC2 / free_pages=01·BC2 ✅
```

Compare a small Access-authored single-column baseline TDEF (page 23 in `NorthwindTraders.accdb`):

```
000: 02 01 92 0F 00 00 00 00 66 00 00 00 59 06 00 00
010: 00 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00      <- 0x18 = 0x01
020: 00 00 00 00 00 00 00 00 4E 01 00 00 00 01 00 00
030: 00 00 00 00 00 00 00 00 18 00 00 01 18 00 00 02      <- used_pages=00·018 / free_pages=01·018
```

Header bytes 0x37..0x3E are now byte-shape-identical to the baseline (`row_index + 3-byte LE page ref`, used = row 0, free = row 1, both pointing at the same usage-map data page). Re-running `DIAG_RT_BISECT=1` after the fix:

```
[bisect] N0_OpenClose:        ✅ OK
[bisect] N1_CreateOneTable:   ❌ MSysDb  (unchanged)
[bisect] N2_CreateTwoTables:  ❌ MSysDb  (unchanged)
[bisect] N3_TwoTables_Rel:    ❌ MSysDb  (unchanged)
[bisect] N4_TwoTables_Rel_Rows: ❌ MSysDb  (unchanged)
```

So the zero `used_pages` pointer was a real defect, but it was **not** the (only) thing tripping `'MSysDb'`. The fix is left in place — DAO will need this anyway, and the writer-emitted bytes are now closer to the baseline shape — but the gating tests still fail.

### What we got wrong about the format

Three claims in the original hypothesis above were inaccurate; preserved here so future diff probes start from correct offsets:

| Original (incorrect) claim | Actual format (verified against Northwind page 23) |
|---|---|
| Field is `ump_page` at offset **0x36** (3-byte page ref alone) | Field is `used_pages` at offset **0x37..0x3A** = 1-byte row index + 3-byte LE page. mdbtools labels it `used_pages`; offsets 55..58 from page start match `_tdef.BlockEnd - 8`. |
| Field is `free_ump_page` at offset **0x3A** (3-byte page ref alone) | Field is `free_pages` at offset **0x3B..0x3E** = 1-byte row index + 3-byte LE page. mdbtools offsets 59..62. |
| `ump_row` byte at **0x35**, `free_ump_row` byte at **0x39** | The row-index byte is the *first* byte of each 4-byte slot (0x37 / 0x3B), not preceding it. Access uses **row 0 for used, row 1 for free**, both pointing at the same usage-map data page. |
| The usage-map page is `page_type = 0x05` | The usage-map *data* page is a normal `page_type = 0x01` data page hosting the two bitmap rows; `0x05` is the LVAL page type, not the usage map. (mdbtools spec mislabels this — Access-authored Northwind page 24, the usage-map for the page-23 TDEF, is `0x01 0x01`.) |

### What still differs from the baseline (latest, post-fix-pass-2)

After the second batch of fixes (TDEF `0x0C` magic, TDEF `0x02` free-space hint, 69-byte usage-map rows — see "Attempted fix pass 2" below), the writer-emitted N1 page diff narrows to:

| Page | Role | Status after pass-2 |
|---|---|---|
| **2** (MSysObjects TDEF) | Existing TDEF, row_count + per-real-idx counters bumped | ✅ correct (3-byte diff, all expected) |
| **8** (MSysObjects PK leaf) | Spliced PK key | ✅ byte-clean |
| **2790** (MSysObjects ParentIdName leaf) | Spliced composite key | ✅ byte-clean |
| **2994** (MSysObjects data page hosting the new row) | New catalog row | ❌ **only remaining structural diff** |
| **New TDEF page** (3008) | RT_Customers TDEF | ✅ byte-shape-identical to baseline (header bytes 0..0x3F now match column-by-column with the page-23 single-column baseline; only the genuine table-shape diffs remain) |
| **New index leaf** (3009) | RT_Customers PK leaf | ✅ empty leaf, well-formed |
| **New usage-map page** (3010) | RT_Customers used/free pages | ✅ **byte-identical** to Northwind page 24 |

This pinpoints the catalog row body itself. Its byte dump (writer, 55 bytes after pass-2) vs an existing Type=1 row on the same page (writer, 109 bytes), with the relevant fields aligned:

```
 New row:      11 00 C0 0B 00 00 01 00 00 0F 01 00 2A 1F 20 A3 33 88 E6 40 ...
 Existing row: 11 00 B6 00 00 80 03 00 00 0F 08 00 06 0F 98 7D AF 83 E6 40 ...
                ^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^...
                NumCols Id          ParentId    Type/Lv     DateCreate
```

Layout matches column-by-column up through the fixed-length area. The variable-length-jump-table tail (last few bytes) and the variable-length data segment (Name, Owner, LvProp) are where the writer and an Access-authored row could plausibly diverge. The writer's row has a bogus 0-length Owner blob and an LvProp that's either empty or doesn't carry the property block Access expects.

### Attempted fix pass 2 (2026-05-03 late) — landed but DAO still throws `'MSysDb'`

Three additional fixes landed in `AccessWriter.cs`:

1. **TDEF `0x0C..0x0F = 0x00000659`** on Jet4/ACE TDEFs — applied in both `BuildTDefPageWithIndexOffsets` (the user-table path) and `BuildMSysObjectsTDef` (the empty-database path). Justified by the corpus probe (see "What `0x0C` actually is" above); 100% of well-formed Jet4/ACE TDEFs across Northwind + Jackcess V2003/V2007/V2010 carry this exact value regardless of database.
2. **TDEF `0x02..0x03` free-space hint = `pgSz - tdef_len - 8`** on Jet4/ACE — corpus probe across all 52 TDEFs in `NorthwindTraders.accdb` shows this exact computation in **52/52** TDEFs. Writer was leaving it zero; it now matches the baseline computation.
3. **Usage-map rows = 69 bytes each** (1-byte type-0 marker + 68 zero bitmap bytes), packed at the page tail at offsets `pgSz - 69` (row 0 = used) and `pgSz - 138` (row 1 = free). Verified byte-identical to Northwind page 24 (the Access-authored usage-map for the page-23 single-column TDEF), including the row-offset table (`02 00 BB 0F 76 0F`) and the free-space slot (`64 0F`).

Bisect after pass-2:

```
[bisect] N0_OpenClose:               ✅ OK
[bisect] N1_CreateOneTable:          ❌ MSysDb  (unchanged)
[bisect] N2_CreateTwoTables:         ❌ MSysDb  (unchanged)
[bisect] N3_TwoTables_Relationship:  ❌ MSysDb  (unchanged)
[bisect] N4_TwoTables_Rel_Rows:      ❌ MSysDb  (unchanged)
```

The fixes reduced the structural divergence dramatically (the only remaining diff is on page 2994 — the new catalog row body) but did not clear DAO. They are **kept in place** because they bring writer output strictly closer to the Access-authored baseline shape on both ACE and Jet4.

### Experiment: disable Jet4 text compression (`EncodeJet4Text`) — also did not clear `'MSysDb'`

Hypothesis: when the writer encodes the catalog row's `Name` column, `EncodeJet4Text` chooses the compressed UCS-2 form (`FF FE` marker + 1 byte per char) for any ASCII string ≥ 3 chars, while the existing Access-authored Type=1 rows on the same page use the uncompressed form (raw UTF-16 LE, 2 bytes per char, no marker). DAO's catalog walk might require uncompressed UTF-16 in `MSysObjects.Name`.

Test: temporarily forced `compressible = false` in `AccessBase.EncodeJet4Text`, re-ran the bisect:

```
[bisect] N1_CreateOneTable: ❌ MSysDb  (unchanged)
```

Result: the new row's `Name` field now decodes as raw UTF-16 LE (`52 00 54 00 5F 00 43 00 75 00 73 00 74 00 6F 00 6D 00 65 00 72 00 73 00`) instead of compressed (`FF FE 52 54 5F 43 75 73 74 6F 6D 65 72 73`), but DAO still throws `'MSysDb'`. The compression toggle is **not** what trips the catalog walk. Reverted.

This is still a likely *cosmetic* divergence — every Access-authored Type=1 row in Northwind uses uncompressed UTF-16 in the catalog — but it's not the blocker. (TBD: should we change this behavior anyway for consistency? Out of scope for this fix.)

### Format / version differences observed during the investigation

While probing the corpus, several Jet3-vs-Jet4/ACE format differences surfaced that aren't yet documented elsewhere:

| Field | Jet3 (.mdb, Access 97) | Jet4 / ACE (.mdb 2000+/.accdb) |
|---|---|---|
| TDEF `0x0C..0x0F` | Per-table page number (e.g. `0x0A`, `0x14`, `0x00` in `Jet3Test.mdb`); semantics unclear, may be a continuation pointer or per-table allocation root. | Format-wide magic constant `0x00000659`. Writer must stamp this on Jet4/ACE only. |
| Page size | 2048 | 4096 |
| Format byte at file offset `0x14` | `0x00` | `0x01` (Jet4) / `0x02` (ACE) |
| Database header magic | `Standard Jet DB\0` | `Standard Jet DB\0` (Jet4) / `Standard ACE DB\0` (ACE) |
| Compressed text marker | N/A — Jet3 uses ANSI codepage encoding | `FF FE` prefix + 1 byte/char when all chars ≤ U+00FF and length ≥ 3 |
| Real-idx physical descriptor size | 39 bytes (`PhysSize` in `Constants.TableDefinition.Jet3.RealIdx`) | 51 bytes |
| Real-idx entry block size in TDEF | 8 bytes/idx | 12 bytes/idx |
| TDEF `BlockEnd` (column-descriptor start) | 43 | 63 |
| Column descriptor size | 18 bytes | 25 bytes |
| Column-name length prefix | 1 byte | 2 bytes (LE) |
| Column-name encoding | ANSI codepage 1252 | UTF-16 LE (or compressed via `FF FE` marker) |

### Other corpus-confirmed Jet4/ACE TDEF invariants the writer now satisfies

These are the ones we've explicitly verified across 52 NorthwindTraders TDEFs + Jackcess V2003/V2007/V2010 fixtures:

- `0x00..0x01` = `02 01` (page-type marker)
- `0x02..0x03` = `pgSz - tdef_len - 8` (free-space hint, **52/52 in Northwind**)
- `0x04..0x07` = next-TDEF-page chain (0 when fits in one page)
- `0x08..0x0B` = `tdef_len`
- `0x0C..0x0F` = `0x00000659` (format-wide magic, **all probed Jet4/ACE TDEFs**)
- `0x10..0x13` = live-row count (`Constants.TableDefinition.RowCountOffset`)
- `0x14..0x17` = autonumber next value (when applicable)
- `0x18` = autonum_flag (0x01 if any column has the autonumber bit)
- `_tdef.NumCols - 5` = `0x4E` (mdbtools "magic" byte; writer emits this)
- `0x37..0x3A` = used_pages pointer `(row=0x00, page LE3)`
- `0x3B..0x3E` = free_pages pointer `(row=0x01, page LE3)`

## Recommended fix (next steps, in priority order)

1. **Diff the new MSysObjects row's variable-length area** against an Access-UI-created RT_-prefixed table on the same Northwind file. The only remaining N1 diff is on page 2994 (the data page hosting the new row); the writer's 55-byte row is shorter than Access-authored Type=1 rows by an implausible margin. Specifically check:
   - The 16-byte `Owner` blob (Access stores a SID; the writer leaves it null/zero-length).
   - The `LvProp` BLOB (Access stamps a small property block even on tables with no per-column properties; the writer emits `null` and skips the column).
   - The variable-length jump table at the row tail (`var_count`, `var_offsets[]`, `null_bitmap`).
2. **If a hand-crafted row matching Access's layout clears `'MSysDb'`**, port the encoding into `InsertCatalogEntryAsync` / `RowDataEncoder`.
3. **If still failing after that**, examine page 5 (the global PAM / page-allocation map) — DAO might require the new TDEF / index-leaf / usage-map page numbers to be marked allocated there. Currently page 5 is unchanged in N1, which would explain why DAO can't reconcile its allocation walk.
4. **Aesthetic follow-up** (does not block these tests): consider switching `EncodeJet4Text` to emit uncompressed UTF-16 in catalog `Name` columns for consistency with Access-authored output, even though the experiment showed it's not the blocker.

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
| 5 | New TDEF page malformed | ✅ **fully fixed (per byte-diff)** | All four sub-faults landed: `used_pages`/`free_pages` (pass-1), `autonum_flag` (pass-1), `0x0C` magic constant (pass-2), `0x02` free-space hint (pass-2). Writer-emitted TDEF header bytes 0..0x3F are now byte-shape-identical to the page-23 single-column baseline. Did not on its own resolve `'MSysDb'`. |
| 6 | New MSysObjects row variable-length area / null-bitmap / `Owner` / `LvProp` malformed | 🔴 **promoted to leading suspect** | After pass-2 fixes the only remaining structural diff in N1 is on page 2994 (the MSysObjects data page hosting the new row). The writer's row is 55 bytes; comparable Access-authored Type=1 rows are 65–110 bytes. The fixed-length area aligns column-by-column, so the divergence is in the variable-length jump table, the `Owner` SID blob, and/or the `LvProp` property block. |
| 7 | Global page-allocation map missing the new TDEF page | ✅ ruled out (as a TDEF-header issue) | The `0x0C..0x0F` field that prompted this hypothesis turned out to be a format-wide magic constant `0x00000659`, not a per-database PAM pointer. Now stamped on every Jet4/ACE TDEF. The actual global PAM (page 5) is unchanged in N1 — re-promote only if hypothesis #6 also fails to clear `'MSysDb'`. |
| 8 | Test infra wrong | ✅ ruled out | The N1 reproducer is invoked from FormatProbe with a hand-rolled writer-only flow — no xUnit, no `RoundTripSession`. Same DAO error. |
| 9 | DAO requires `MSysAccessStorage` / `MSysComplexColumns` / `MSysNavPaneGroups` / `MSysNameMap` rows | 🟡 lowest priority | Access-UI-created tables don't immediately get NavPane / NameMap entries either, and DAO compacts those files fine. |
| 10 | New row's `ParentId = 0x0F000001` is the wrong group | 🟡 low priority | Existing Northwind Type=1 user-table rows already use `0x0F000001`. |
| 11 | TDEF back-pointer wrong | ✅ ruled out | The structural-diff probe in "Initial hypothesis" above shows all header bytes outside the four divergent fields are byte-identical to the baseline. |
| 12 | Catalog `Name` column written in compressed UCS-2 (`FF FE` marker + 1B/char) instead of raw UTF-16 LE | ✅ ruled out as the blocker | Disabled `EncodeJet4Text` compression entirely; bisect output unchanged. The compressed form is *cosmetically* unlike Access-authored rows (which always use raw UTF-16 in the catalog) but does not trip the DAO walk. |

## Page-level diff (N1 minimum reproducer, post-fix-pass-2)

A single empty `RT_Customers` table, no relationship, no inserts:

```
Diff pages (shared range): 2, 8, 2790, 2994
Pages added              : 3008 (TDEF), 3009 (PK leaf), 3010 (usage-map)
File size                : 12,320,768 → 12,333,056 (+12 KiB = +3 pages)
```

| Page | What's there | Observed change |
|------|--------------|-----------------|
| 2    | MSysObjects TDEF | 3 byte diffs: `row_count` @ 0x10 +1, ri=0 entry-count u4 @ 0x43 +1, ri=1 entry-count u4 @ 0x4F +1. All three correct. |
| 8    | MSysObjects PK (`Id`) leaf | Splice: 239 → 240 entries. New key `7F 80 00 0B C0` for `Id=3008`. ✅ byte-clean. |
| 2790 | MSysObjects `ParentIdName` composite leaf | Splice: 114 → 115 entries. New key `7F 8F 00 00 01 7F <RT_Customers GeneralLegacy bytes> 01 00`. ✅ byte-clean. |
| 2994 | MSysObjects data page hosting the new row | 🔴 **Only remaining structural diff.** New 55-byte row inserted at offset `0x0B7A`; row 8's offset entry gains the `0x8000` deleted/overflow bit (`0x8BB1`) — a side-effect of inserting before it. Row body's variable-length area diverges from comparable Access-authored rows. |
| 3008 | RT_Customers TDEF | Written entirely by us. Post pass-2: `0x02` free-space hint, `0x0C` magic, `0x18` autonum_flag, `0x37..0x3E` usage-map pointers all match baseline shape. |
| 3009 | RT_Customers PK leaf | Empty (`page_type=0x04`, `parent_page=<TDEF>`, no entries). |
| 3010 | RT_Customers `used_pages` / `free_pages` host | `page_type=0x01` data page with two 69-byte rows. ✅ **byte-identical** to Northwind page 24 (the comparable Access-authored usage-map). |

Notes:

- Page 5 (the page-allocation map) does **not** differ in N1 — re-examine if hypothesis #6 fix doesn't clear DAO.
- Page 0 (the actual DBA / file header) does **not** differ.
- The exact page number of the new TDEF varies with what other writes have happened in the bisect step. Don't pin tests on that page number.

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

- `/memories/repo/round-trip-tests.md` (agent memory, not in repo) — leaf-bytes verification + Jet4 layout invariants.
- [catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md) — design rationale for the splice approach (now landed).
- [round-trip-diagnostics.md](round-trip-diagnostics.md) — reusable bisection matrix (N1–N14) and page-dump probe recipe from the historical scratch tests.
- README §"Round-trip through Microsoft Access Compact & Repair" — testing methodology and known limitation.
