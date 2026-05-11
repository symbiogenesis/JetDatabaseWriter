# Round-Trip `OpenRecordset` Failure — Hypothesis Log

> **Status: RESOLVED 2026-05-10.**
> Companion to [round-trip-test-failures.md](round-trip-test-failures.md).
> This log tracked why DAO `OpenRecordset` rejected writer-created user
> tables with `"Unrecognized database format ''."` (`OpenDatabase`
> succeeded; only recordset materialization failed). The root cause and
> fix are recorded in §1; the full hypothesis ledger is preserved in §4
> for posterity. New work belongs in the adjacent-issues backlog (§5).

---

## 1. Resolution

| Field | Value |
|---|---|
| **Root cause** | **H48** — writer's TDEF `tdef_len` field was 8 bytes short of DAO's, for any Jet4/ACE table with at least one index. |
| **Mechanism** | DAO-authored TDEFs reserve 8 trailing zero bytes after the `0xFFFF` "no usage-map" sentinel and count them inside `tdef_len`. The writer wrote the same zero bytes (the page is zero-initialised) but stopped `tdef_len` 8 bytes early, so DAO's cursor parser tripped at the apparent end of the index-name section. |
| **Fix** | [`TDefPageBuilder.BuildTDefPagesWithIndexOffsets`](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs): for `jet4 && numIdx > 0`, advance `namePos += 8` immediately after writing the FFFF sentinel. Only widens `tdef_len` (offset 8) and the companion `freeSpace` (offset 2). |
| **Adjacent fix** | [`RelationshipManager.AddFkLogicalIdxEntry`](../../JetDatabaseWriter/Relationships/RelationshipManager.cs) was stamping the format-wide magic `0x00000659` on the appended real-idx physical descriptor. Use `Constants.TableDefinition.Jet4.RealIdx.LeadingMagic` (`0x00000783`), matching `BuildTDefPagesWithIndexOffsets`. Latent — surfaced by H48 unblocking the FK round-trip tests, which then tripped on `AssertTdefMagicStampsAsync`. |
| **Verification** | `DIAG_RT_DAO_BASELINE` probe (extended with an `OpenRecordset` smoke step in §0): both writer and DAO copies report `OpenRecordset('RT_Customers')` exit=0. 2026-05-11 follow-up: the previously opt-in `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` and `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` tests now pass under the normal Microsoft Access runtime guard. |
| **Tests now passing** | `DaoOpenRecordset_RowCount_MatchesWriterOutput`, `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey`, `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly`, `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert`. |
| **Tests still opt-in skipped** | 4 adjacent issues remain — see §5. |

---

## 2. Smoking gun

Empirically isolated on 2026-05-10 by disconfirming H46 (LvProp blob).
With LvProp ruled out, the §4 TDEF hex diff in `dao-baseline-diff.md`
showed two byte-level divergences in the writer's `RT_Customers` TDEF
that the §7 hypothesis battery had not covered.

### 2.1 TDEF byte-level diffs — writer vs DAO

| Offset | Writer | DAO | Significance |
|---:|---|---|---|
| `0x02..0x03` | `FB 0E` (free=`0x0EFB`) | `F3 0E` (free=`0x0EF3`) | `freeSpace = pgSz − tdef_len − 8`; differs by 8 — derived from H48. |
| `0x08..0x0B` | `FD 00 00 00` (`tdef_len=253`) | `05 01 00 00` (`tdef_len=261`) | **H48: TDEF data length differs by 8 bytes.** |
| `0x38`, `0x3C` | `…BC30…` | `…BBA0…` | owned/free-space-pages pointers — page-number-dependent, benign. |
| `0x74` | `01` | `00` | `Name` (TEXT) `ExtraFlags` byte 16 = COMPRESSED_UNICODE — H47, benign. |
| `0xC0`, `0xC3` | `…BC2/BC1…` | `…BBA/BBB…` | first-data-page pointers — page-number-dependent, benign. |

### 2.2 Bisect verdict

| Variant | `OpenRecordset` outcome |
|---|---|
| Writer baseline (no fix) | ❌ `"Unrecognized database format ''."` |
| H46 alone — `BuildLvPropBlob` suppressed | ❌ same error |
| H47 alone — `IsCompressedUnicode=false` on TEXT | ❌ same error |
| **H48 alone — `tdef_len += 8` after FFFF sentinel** | ✅ **OK** |
| H47 + H48 | ✅ OK |

H48 is necessary and sufficient. H46 and H47 are benign divergences;
DAO accepts both states.

---

## 3. Probe harness — `dao-baseline-diff.md`

A single execution authoritatively answers TDEF-byte-level questions.

**Run:**

```pwsh
$env:DIAG_RT_DAO_BASELINE = "1"
dotnet run --project JetDatabaseWriter.FormatProbe
```

Output lands under `%TEMP%\JetDatabaseWriter.RtDaoBaseline\`.

**Report sections:**

| § | Purpose |
|---|---|
| §0 | Outcomes — DAO authoring, `CompactDatabase`, and `OpenRecordset` PASS/FAIL for both copies. |
| §1 | File-level summary — page counts, page deltas. |
| §2 | `RT_Customers` catalog row — Id / ParentId / Type / Flags / TDEF page. |
| §3 | Changed/added pages by type — per-page byte-type table for the divergence set. |
| §4 | `RT_Customers` TDEF (raw bytes) — side-by-side hex of writer vs DAO TDEFs. |
| §5 | New `MSysObjects` row bytes — row-body hex dump (used to disconfirm H46). |
| §6 | Pages DAO modifies that the writer never touches. |
| §7 | Accumulating hypothesis diff — one PASS/FAIL row per H26-H45 hypothesis. |

---

## 4. Hypothesis ledger (historical)

> Preserved verbatim so that future investigations into adjacent or
> similar TDEF-layout questions can short-circuit known dead-ends. **Do
> not re-test entries below.**

### 4.1 Confirmed and fixed

| ID | Bug | Fix | Test / verification |
|---|---|---|---|
| **H22** | TEXT/MEMO branch overwrote redundant `col_num` at descriptor byte 9 with `0x0001`. | `TDefPageBuilder` writes `col.ColNum` at byte 9 unconditionally for Jet4/ACE. | [`WriterColumnDescriptorRedundantColNumTests`](../../JetDatabaseWriter.Tests/Schema/WriterColumnDescriptorRedundantColNumTests.cs) |
| **H25** | `DataPageInserter.PatchAutoNumFlag` wrote TDEF[`0x18`] = `0x00` for tables with no autonumber column. | Stamp `0x01` unconditionally. | [`WriterTDefAutoNumFlagTests`](../../JetDatabaseWriter.Tests/Schema/WriterTDefAutoNumFlagTests.cs) |
| **H48** ⭐ | `BuildTDefPagesWithIndexOffsets` set `tdef_len = (namePos − 8)` immediately after writing the trailing `0xFFFF` sentinel; DAO reserves 8 zero bytes after it inside `tdef_len`. **The sole `OpenRecordset` blocker.** | Advance `namePos += 8` after the FFFF sentinel for `jet4 && numIdx > 0`. | `DIAG_RT_DAO_BASELINE` probe `OpenRecordset` step exit=0 for both copies; `DaoOpenRecordset_RowCount_MatchesWriterOutput` and `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey` pass. |
| **H48-adj** | `RelationshipManager.AddFkLogicalIdxEntry` stamped `0x00000659` on the appended real-idx physical descriptor instead of `0x00000783`. Latent until H48 unblocked the FK round-trip tests. | Use `Constants.TableDefinition.Jet4.RealIdx.LeadingMagic`. | `AssertTdefMagicStampsAsync` passes for FK-bearing tables. |
| **H49** | Newly appended user-table data pages were not marked in the per-table owned/free usage maps, and the first attempted fix used the wrong inline-row bitmap offset. DAO sequential/snapshot recordsets walk these maps, so writer rows existed on disk but could be invisible to `MoveFirst()`. | [`DataPageInserter.MarkPageInOwnedMapAsync`](../../JetDatabaseWriter/Pages/DataPageInserter.cs) now marks both owned and free rows for user tables. INLINE rows are `type byte + int32 start_page + 64 bitmap bytes`, so the bitmap begins at row offset +5. | `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` passes under the normal Microsoft Access runtime guard. |
| **AutoNumber-adj** | DAO continued AutoNumber values from TDEF offset `0x14`, not from the PK leaf or row scan. Writer-created tables left that high-water value stale, so DAO reused an existing ID and hit a duplicate-index error. | [`AccessWriter.UpdateTDefAutoNumberHighWaterAsync`](../../JetDatabaseWriter/AccessWriter.cs) persists the maximum inserted AutoNumber value to [`Constants.TableDefinition.AutoNumberOffset`](../../JetDatabaseWriter/Constants.cs) after successful inserts. | `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` passes under the normal Microsoft Access runtime guard. |

### 4.2 Disconfirmed — writer matches DAO byte-for-byte

| ID | Region / claim | How tested | Result |
|---|---|---|---|
| **H21** | real-idx `first_dp` (phys+38) is zero / wrong | [`WriterRealIdxFirstDpStampingTests`](../../JetDatabaseWriter.Tests/Indexes/WriterRealIdxFirstDpStampingTests.cs) | All non-FK slots have valid `first_dp` pointing at a `0x04` leaf. |
| **H23** | real-idx `flags` byte at offset 42 (mdbtools) instead of 46 | [`WriterRealIdxFlagsOffsetTests`](../../JetDatabaseWriter.Tests/Indexes/WriterRealIdxFlagsOffsetTests.cs) | Offset 46 holds `0x80` on every NorthwindTraders slot; offset 42 does not. mdbtools doc is stale. |
| **H24** | TEXT/MEMO `misc_ext` (sort-order version) byte = 0 vs 1 | [`WriterColumnDescriptorTextSortOrderTests`](../../JetDatabaseWriter.Tests/Schema/WriterColumnDescriptorTextSortOrderTests.cs) | DAO uses sort-order version 0 ("General Legacy") on every TEXT/MEMO descriptor; writer matches. |
| **H26** | per-table usage-map row 0 type byte should be `0x01` (REFERENCE) | §7 of `dao-baseline-diff.md` | Both writer and DAO stamp `0x00` (INLINE) for small/empty tables. |
| **H27** | non-FK logical-idx `RelIdxNum` [13..16] must be `0xFFFFFFFF` | §7 | Writer already populates the sentinel. |
| **H28** | logical-idx `putInt(0)` at bytes [24..27] | §7 | Trailer int is intact. |
| **H32** | non-text/numeric/complex col-desc bytes [11..14] zero | §7 | LONG `CustomerID` zero across [11..14]. |
| **H33** | col-desc bytes [17..20] (always-0 `putInt`) zero | §7 | Both columns zero across [17..20]. |
| **H34** | col-desc `ExtraFlags` byte [16] zero for non-TEXT/MEMO | §7 | LONG column has byte[16] == 0. |
| **H37** | PK real-idx flags @46 has `UNIQUE` \| `REQUIRED` \| `UNKNOWN` bits | §7 | Both writer and DAO emit `0x89` (`0x80 \| 0x08 \| 0x01`). |
| **H38** | real-idx [42..45] (unknown `putInt`) zero | §7 | Gap is zero in both. |
| **H39** | empty PK leaf header (bytes 0..28) | Direct hex-diff (writer page 3009 vs DAO page 3003) | Only 2 bytes differ — the legitimate parent-TDEF back-pointer. |
| **H40** | index-leaf entry-mask trailing byte | Same hex-diff as H39 | Bytes 6..4095 byte-identical. |
| **H41** | TDEF[`0x1C..0x1F`] `next_complex_auto_number` zero | §7 | Both 0. |
| **H42** | logical-idx name length-prefix is byte count (even) | §7 | Writer correctly writes `Encoding.Unicode.GetBytes(name).Length`. |
| **H43** | `unknown_jet4` 4-byte field after `next_pg` | Code review against `JetFormat.Jet4Format` | No such field for Jet4. |
| **H44** | TDEF `owned_pages[0x37]` / `free_space_pages[0x3B]` non-zero | §7 | Writer populates both 4-byte slots with valid usage-map row pointers. |
| **H45** | per-table usage-map row `tdef_back_pointer` (REFERENCE rows only) | §7 | N/A — both rows are INLINE; depends on H26. |
| **H46** | Writer-emitted `Required=true` LvProp entries reject `OpenRecordset` | Bypassed `BuildLvPropBlob` via `DIAG_RT_NO_LVPROP=1` env-var hook; re-ran probe with new `OpenRecordset` step | DAO still rejected the table after the writer's LvProp matched DAO's 12-byte placeholder. The catalog row's `LvProp` column is **not** consulted by `OpenRecordset`. |
| **H47** | TEXT/MEMO `ExtraFlags` byte 16 must be `0x00` (no `COMPRESSED_UNICODE`) | Set `IsCompressedUnicode=false` on the probe's `Name` column; re-ran | DAO accepts both `0x00` and `0x01` here. Benign. |

### 4.3 Disconfirmed in `round-trip-test-failures.md` — do not re-test

- LvProp 12-byte payload as dangling chained-LVAL (H20).
- Writer-private `0x08` NOT-NULL flag (already removed).
- Unconditional `ExtraFlags = 0x01` on TEXT/MEMO (already removed).
- TDEF magic stamps `0x00000659` / `0x00000783` (already correct in `BuildTDefPagesWithIndexOffsets` and `BuildMSysObjectsTDef`; H48-adj was the only stale call site).
- DB-header modify counter at `0x0E02` (manually patched, still failed).
- Page 1 GPM updates for appended pages (append-only pages already "in-use").
- `MSysACEs` rows (already inserted with correct `FInheritable` column).
- Catalog row Name UCS-2 vs UTF-16 encoding (cosmetic only).
- All index-leaf issues #15–18 (compact passes; only `OpenRecordset` failed).

### 4.4 Untested / not applicable to current fixture

| ID | Region | Why deferred |
|---|---|---|
| **H29** | TDEF column-usage-map terminator for tables with zero indexes | `RT_Customers` always has a PK; can't manifest. Verify before dismissing if a no-index path is added. |
| **H30** | TDEF tail / pad bytes after `namePos` on continuation pages | Buffer is `new byte[…]` (zero-init); revisit if a buffer pool is introduced. |
| **H31** | NUMERIC col-desc bytes [13..14] zero | Probe N/A — `RT_Customers` has no NUMERIC column. |
| **H35** | `AUTO_NUMBER_GUID_FLAG_MASK = 0x40` for GUID auto-number | Latent issue; current fixtures use LONG auto-number. |
| **H36** | real-idx `unique_entry_count` slot drift after inserts | For empty PK, `unique_entry_count == row_count == 0`; OK at create time. |

---

## 5. Adjacent issues exposed by the H48 fix

H48 unblocking `OpenRecordset` exposed pre-existing latent bugs in code
paths that DAO never previously reached. These are **not** regressions
caused by H48. As of 2026-05-11, two of the six initially re-skipped
adjacent tests are resolved and now run under the normal Microsoft
Access runtime guard; four remain opt-in compatibility-gap tests.

**Resolved adjacent issues**

| Test | Original symptom | Resolution |
|---|---|---|
| `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` | DAO `$rs.MoveFirst()` raised `"No current record"` even though the row existed on disk and the PK index reported it. | **H49**: mark newly appended user-table data pages in the per-table owned/free usage maps, using DAO's actual INLINE row layout (`type + int32 start_page + 64-byte bitmap`). |
| `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` | DAO INSERT after the writer's last autonumber row raised `"duplicate values in the index"`. | Persist the AutoNumber high-water value at TDEF offset `0x14` after successful writer inserts. |

**Open opt-in gaps**

| Test (opt-in skipped) | Symptom | First place to look |
|---|---|---|
| `DaoRelationshipEnforcement_FkViolation_RaisesError` | DAO INSERT into Child table with non-existent ParentId succeeds; no FK violation raised. | Writer's `MSysRelationships` row layout / FK-flag bits and per-TDEF FK logical-entry fields. DAO accepts the row at catalog-load time but does not enforce it at runtime. |
| `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` | DAO Compact succeeds but post-compact RowCount=0 on FK-bearing tables (writer-inserted rows dropped). | FK metadata plus data-page / usage-map interaction during Compact & Repair. |
| `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` | Same as above with composite-PK fixture. | Same as above, with composite key ordering and relationship-column ordinals in scope. |
| `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` | DAO `CompactDatabase` on an Agile-encrypted writer ACCDB raises `"Unrecognized database format"`. | Encryption-header / per-page encryption — distinct from the plaintext TDEF and usage-map defects. |

**Recommended order of attack** (smallest-blast-radius first):

1. `DaoRelationshipEnforcement` — pure metadata investigation; create a DAO-authored FK baseline and compare `MSysRelationships` rows plus both endpoint TDEF FK logical entries.
2. `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`, then composite — same compact path with increasing key complexity.
3. `DaoCompactDatabase_OnEncryptedOutput` — orthogonal encrypted-container path; defer until plaintext FK compact behavior is understood.

---

## 5a. 2026-05-11 session — H49 and AutoNumber follow-up

### H49 final shape — usage-map row layout

`DataPageInserter.FindInsertTargetAsync` appends brand-new data pages
for writer-created user tables. Before H49, those page numbers were
never recorded in the per-table owned-pages usage map referenced by TDEF
bytes `0x37..0x3A`; DAO sequential / snapshot recordsets walk that map
instead of scanning the file for `0x01 / parent_tdef == this` data
pages. The row bytes were on disk and visible to this library's reader,
but DAO saw an empty recordset.

The first H49 attempt identified the right map but used an incomplete
INLINE-row layout. DAO-authored baselines show the row body is 69 bytes:

| Byte range | Meaning |
|---|---|
| `+0` | row type (`0x00` = INLINE) |
| `+1..+4` | little-endian `int32 start_page` |
| `+5..+68` | 64-byte bitmap covering 512 pages from `start_page` |

The corrected implementation writes the start page at `rowOff + 1` and
sets bits from `rowOff + 5`. Low-page maps keep `start_page = 0`; high
page maps lazily stamp `(pageNumber / 8) * 8` so the new page lands in
the 512-bit window. Both the owned-pages row (TDEF `0x37`) and the
free-space-pages row (TDEF `0x3B`) are marked when a user-table data page
is appended. Pre-existing system-table TDEFs remain excluded (`tdefPage
<= 1024`) because mutating their usage maps caused DAO `OpenDatabase` to
raise `"Invalid argument"` during the investigation.

Result: `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` now passes
and is no longer behind `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS`.

### AutoNumber high-water

DAO-authored AutoNumber fixtures showed that TDEF bytes `0x14..0x17`
store the AutoNumber **high-water value** (the last issued value), not
the next value. A 10-row DAO fixture stamped `0A 00 00 00` there. The
writer already generated unique values for its own inserts, but it did
not persist that high-water value, so a subsequent DAO insert reused an
existing ID and failed the PK unique index.

The fix adds `Constants.TableDefinition.AutoNumberOffset = 20` and calls
`AccessWriter.UpdateTDefAutoNumberHighWaterAsync` after successful
single-row, object-array bulk, and generic bulk inserts. The helper scans
inserted rows for AutoNumber columns and writes the maximum inserted
value if it is greater than the current TDEF high-water value.

Result: `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` now
passes and is no longer behind `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS`.

### Probe artifacts

The `DIAG_MEMO_READBACK` branch in
[`JetDatabaseWriter.FormatProbe/Program.cs`](../../JetDatabaseWriter.FormatProbe/Program.cs)
remains useful for ad-hoc inspection of `%TEMP%\JetDatabaseWriter.MemoDiag\writer_memo.accdb`.
For the remaining open gaps in §5, capture a fresh DAO-authored baseline
with the same table shape before forming byte-layout hypotheses.

---

## 6. Reference sources

Use these only when an empirical baseline diff is insufficient (rare).

### 6.1 Primary ground truth

- **Jackcess** — [`jahlborn/jackcess`](https://github.com/jahlborn/jackcess):
  - [`ColumnImpl`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/ColumnImpl.java) (`writeDefinition`)
  - [`IndexData`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/IndexData.java) (`writeDefinition`)
  - [`IndexImpl`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/IndexImpl.java) (`writeDefinition`)
  - [`TableImpl`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/TableImpl.java) (`writeTableDefinitionHeader`, `createUsageMapDefinitionBuffer`)
  - [`JetFormat`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/JetFormat.java) (Jet4 / Jet12 / Jet14 / Jet16 / Jet17 offsets)
  - [`PropertyMaps`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/PropertyMaps.java) (LvProp encoding)
- **mdbtools** — [`HACKING.md`](https://github.com/mdbtools/mdbtools/blob/dev/HACKING.md) and `src/libmdb/table.c` (`mdb_read_table`). Note: occasionally stale (see H23).

### 6.2 Specifications

**MS-CFB** and **MS-OFCRYPTO** are already implemented in the codebase.
**No `[MS-MDB]` spec exists** — the ACE on-disk format is undocumented
by Microsoft, which is why the empirical `DIAG_RT_DAO_BASELINE` probe
is the authoritative reference for this codebase.

### 6.3 Community archives

UtterAccess (Albert D. Kallal, Hans Vogelaar), Stephen Lebans (Wayback
Machine), Allen Browne, Tony Toews' Access FAQ, MSDN Archive / DevBlogs
(Lucas Sanders, Clint Covington, Andy Baron, Mary Chipman), CodeProject
"Jet database engine internals" articles, Jackcess GitHub Discussions /
SourceForge tracker, mdbtools mailing list archives.
