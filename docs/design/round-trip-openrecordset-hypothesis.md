# Round-Trip `OpenRecordset` Failure — Hypothesis Log

Companion to [round-trip-test-failures.md](round-trip-test-failures.md).
Tracks why DAO `OpenRecordset` rejects writer-created user tables with
`"Unrecognized database format ''."` (`OpenDatabase` succeeds; only
recordset materialization fails).

> **RESOLVED 2026-05-10.** Root cause confirmed: **H48 — writer's
> `tdef_len` field undercounts the TDEF data area by 8 bytes** for any
> Jet4/ACE table that has at least one index. DAO walks the TDEF using
> `tdef_len` as a hard upper bound and rejects the table when its
> internal cursor parser stops 8 bytes short of the expected end of the
> index-name section. Fix landed in
> [`TDefPageBuilder.BuildTDefPagesWithIndexOffsets`](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs):
> after the trailing `0xFFFF` "no usage-map" sentinel, advance `namePos`
> by 8 bytes (the page is zero-initialised so no payload is needed) so
> the `tdef_len` and `freeSpace` calculations include them. Verified
> empirically via the `DIAG_RT_DAO_BASELINE` probe: writer-authored
> `RT_Customers` now matches DAO byte-for-byte in this region and DAO
> `OpenRecordset` returns the table successfully.
>
> **Adjacent fix:** `RelationshipManager.AddFkLogicalIdxEntry` was
> stamping the format-wide magic `0x00000659` on the new real-idx
> physical descriptor instead of the real-idx-specific `0x00000783`
> (`Constants.TableDefinition.Jet4.RealIdx.LeadingMagic`). Corrected at
> the same time so `AssertTdefMagicStampsAsync` round-trip checks pass
> on FK-bearing tables.

---

## 1. Status (2026-05-10)

| | |
|---|---|
| **Root cause** | **H48 — writer-emitted `tdef_len` was 8 bytes short of DAO baseline** |
| **Fix** | [`TDefPageBuilder.BuildTDefPagesWithIndexOffsets`](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs): advance `namePos += 8` after the FFFF sentinel for `jet4 && numIdx > 0` |
| **Verification** | `DIAG_RT_DAO_BASELINE` probe (extended with `OpenRecordset` smoke step) — both writer copy and DAO copy now report `OpenRecordset('RT_Customers')` exit=0 |
| **Tests unblocked** | `DaoOpenRecordset_RowCount_MatchesWriterOutput`, `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey` (previously skipped on H46/H25 reasons; now pass) |
| **Adjacent issues newly exposed** | FK enforcement (separate hypothesis), MEMO row visibility under DAO, autonumber continuation seed, encrypted Compact, FK-table Compact row preservation — see §4.5 |
| **Probe harness** | `DIAG_RT_DAO_BASELINE=1 dotnet run --project JetDatabaseWriter.FormatProbe` → `dao-baseline-diff.md` (now also reports `OpenRecordset` outcome in §0) |
| **Hypotheses tested** | H21 – H48 (28 total) — see §4 |

---

## 2. Smoking gun — H48 (TDEF `tdef_len` undercounts trailing region)

> Empirically isolated 2026-05-10 by running the H46 disconfirm test:
> bypassing `BuildLvPropBlob` showed the catalog row's `LvProp` payload
> is **irrelevant** to `OpenRecordset` (DAO still rejected the table
> when our `LvProp` matched DAO's). With LvProp ruled out, the §4 TDEF
> hex diff still showed two byte-level divergences in the writer's
> `RT_Customers` TDEF page that §7's accumulated hypothesis battery had
> not covered:

### 2.1 TDEF byte-level diffs (writer vs DAO, after H46 disconfirm)

| Offset | Writer | DAO | Meaning |
|---:|---:|---:|---|
| 0x02–0x03 | `FB 0E` (free=0x0EFB) | `F3 0E` (free=0x0EF3) | `freeSpace` = `pgSz − tdef_len − 8`; differs by 8 |
| 0x08–0x0B | `FD 00 00 00` (`tdef_len=253`) | `05 01 00 00` (`tdef_len=261`) | TDEF data length; **differs by 8** |
| 0x38, 0x3C | `…BC30…` | `…BBA0…` | owned/free-space-pages pointers (legitimately page-number-dependent) |
| 0x74 | `01` | `00` | `Name` (TEXT) column descriptor `ExtraFlags` byte 16 = COMPRESSED_UNICODE (writer default true) |
| 0xC0, 0xC3 | `…BC2/BC1…` | `…BBA/BBB…` | first-data-page pointers (page-number-dependent) |

The 8-byte tdef_len delta corresponds to **8 trailing zero bytes
immediately after the `0xFFFF` sentinel** that the writer was already
emitting at the end of the index-name section. Both files have
identical zero bytes there (the page is zero-initialised), but DAO's
`tdef_len` field counts those 8 bytes as part of the TDEF data area
while the writer's did not.

### 2.2 Bisect verdict

| Variant | `OpenRecordset` outcome |
|---|---|
| Writer baseline (no fix) | ❌ "Unrecognized database format" |
| H46 alone (LvProp suppressed) | ❌ "Unrecognized database format" |
| H47 alone (`IsCompressedUnicode=false` on TEXT) | ❌ "Unrecognized database format" |
| **H48 alone (`tdef_len += 8` after FFFF sentinel)** | ✅ **OK** |
| H47 + H48 | ✅ OK |

H48 is **necessary and sufficient**. H47 (TEXT `ExtraFlags=0x00`) is
benign — DAO accepts the COMPRESSED_UNICODE bit on writer-authored
TEXT columns, and the writer's reader also handles both forms. H46 is
benign — DAO accepts the writer's `Required=true` LvProp blob.



---

## 4. Hypothesis ledger

### 4.1 Disconfirmed (writer matches DAO)

| ID | Region / claim | How tested | Result |
|---|---|---|---|
| **H21** | real-idx `first_dp` (phys+38) is zero / wrong | [WriterRealIdxFirstDpStampingTests](../../JetDatabaseWriter.Tests/Indexes/WriterRealIdxFirstDpStampingTests.cs) | All non-FK slots have valid `first_dp` pointing at a `0x04` leaf. |
| **H23** | real-idx `flags` byte at offset 42 (mdbtools) instead of 46 | [WriterRealIdxFlagsOffsetTests](../../JetDatabaseWriter.Tests/Indexes/WriterRealIdxFlagsOffsetTests.cs) | Offset 46 holds `0x80` on every NorthwindTraders slot; offset 42 does not. mdbtools is stale. |
| **H24** | TEXT/MEMO `misc_ext` (sort-order version) byte = 0 vs 1 | [WriterColumnDescriptorTextSortOrderTests](../../JetDatabaseWriter.Tests/Schema/WriterColumnDescriptorTextSortOrderTests.cs) | DAO uses sort-order version 0 ("General Legacy") on every TEXT/MEMO descriptor; writer matches. |
| **H26** | per-table usage-map row 0 type byte should be `0x01` (REFERENCE) | §7 of `dao-baseline-diff.md` | Both writer and DAO stamp `0x00` (INLINE). Jackcess-derived hypothesis is empirically false for small/empty tables. |
| **H27** | non-FK logical-idx `RelIdxNum` [13..16] must be `0xFFFFFFFF` | §7 | Writer already populates the sentinel. |
| **H28** | logical-idx `putInt(0)` at bytes [24..27] | §7 | Trailer int is intact. |
| **H32** | non-text/numeric/complex col-desc bytes [11..14] zero | §7 | LONG `CustomerID` zero across [11..14]. |
| **H33** | col-desc bytes [17..20] (always-0 putInt) zero | §7 | Both columns zero across [17..20]. |
| **H34** | col-desc ExtraFlags byte [16] zero for non-TEXT/MEMO | §7 | LONG column has byte[16] == 0. |
| **H37** | PK real-idx flags @46 has UNIQUE \| REQUIRED \| UNKNOWN bits | §7 | Both writer and DAO emit `0x89` (`0x80 \| 0x08 \| 0x01`). |
| **H38** | real-idx [42..45] (unknown putInt) zero | §7 | Gap is zero in both. |
| **H39** | empty PK leaf header (bytes 0..28) | Direct hex-diff (writer page 3009 vs DAO page 3003) | Only 2 byte difference, both legitimate parent-TDEF back-pointer. |
| **H40** | index-leaf entry-mask trailing byte | Same hex-diff as H39 | Bytes 6..4095 byte-identical. |
| **H41** | TDEF[0x1C..0x1F] `next_complex_auto_number` zero | §7 | Both 0. |
| **H42** | logical-idx name length-prefix is byte count (even) | §7 | Writer correctly writes `Encoding.Unicode.GetBytes(name).Length`. |
| **H43** | `unknown_jet4` 4-byte field after `next_pg` | Code review against `JetFormat.Jet4Format` | No such field for Jet4. |
| **H44** | TDEF `owned_pages[0x37]` / `free_space_pages[0x3B]` non-zero | §7 | Writer populates both 4-byte slots with valid usage-map row pointers. |
| **H45** | per-table usage-map row `tdef_back_pointer` (REFERENCE rows only) | §7 | N/A — both rows are INLINE; depends on H26. |
| **H46** | Writer-emitted `Required=true` LvProp entries reject `OpenRecordset` | Bypassed `BuildLvPropBlob` via `DIAG_RT_NO_LVPROP=1` env-var hook; re-ran probe with new `OpenRecordset` smoke step | DAO still rejected the table after the writer's LvProp matched DAO's 12-byte placeholder shape. The catalog row's `LvProp` column is **not** consulted by `OpenRecordset`. |
| **H47** | TEXT/MEMO `ExtraFlags` byte 16 must be `0x00` (no COMPRESSED_UNICODE) | Set `IsCompressedUnicode=false` on the probe's `Name` column; re-ran | DAO accepts both `0x00` and `0x01` here; writer matched DAO byte-for-byte at offset 0x74 but `OpenRecordset` still failed. Benign divergence. |

### 4.2 Confirmed-and-fixed (regression-tested) but **not** the sole blocker

> Each fix is correct and matches DAO ground truth, but DAO
> `OpenRecordset` continued to fail with `''` after each landing.

| ID | Bug | Fix | Test |
|---|---|---|---|
| **H22** | TEXT/MEMO branch overwrote redundant `col_num` at descriptor byte 9 with `0x0001` | `TDefPageBuilder` writes `col.ColNum` at byte 9 unconditionally for Jet4/ACE | [WriterColumnDescriptorRedundantColNumTests](../../JetDatabaseWriter.Tests/Schema/WriterColumnDescriptorRedundantColNumTests.cs) |
| **H25** | `DataPageInserter.PatchAutoNumFlag` wrote TDEF[0x18] = `0x00` for tables with no autonumber column | Stamp `0x01` unconditionally | [WriterTDefAutoNumFlagTests](../../JetDatabaseWriter.Tests/Schema/WriterTDefAutoNumFlagTests.cs) |

### 4.2.1 Confirmed-and-fixed — **the** sole `OpenRecordset` blocker

| ID | Bug | Fix | Verification |
|---|---|---|---|
| **H48** | `BuildTDefPagesWithIndexOffsets` set `tdef_len = (namePos − 8)` immediately after writing the trailing `0xFFFF` "no usage-map" sentinel. DAO-authored TDEFs reserve **8 trailing zero bytes after that sentinel** and count them as part of `tdef_len`. The writer's body was therefore 8 bytes shorter than DAO's, causing DAO's `OpenRecordset` cursor builder to abort with `"Unrecognized database format ''."`. | Advance `namePos += 8` after the FFFF sentinel for `jet4 && numIdx > 0`. The page is zero-initialised so no payload write is needed; the change only widens `tdef_len` and the companion `freeSpace` field at offset 2. | `DIAG_RT_DAO_BASELINE` probe now reports `OpenRecordset('RT_Customers')` exit=0 for both writer and DAO copies. `DaoOpenRecordset_RowCount_MatchesWriterOutput` and `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey` (previously skipped) now pass. |
| **H48-adj** | `RelationshipManager.AddFkLogicalIdxEntry` stamped the format-wide magic `0x00000659` on the appended real-idx physical descriptor, instead of the real-idx-specific `0x00000783` (`Constants.TableDefinition.Jet4.RealIdx.LeadingMagic`). Latent — surfaced by H48 unblocking the FK round-trip tests, which then tripped on `AssertTdefMagicStampsAsync`. | Use `RealIdx.LeadingMagic` at the descriptor's first 4 bytes, matching `BuildTDefPagesWithIndexOffsets`. | Round-trip TDEF magic assertion now passes for FK-bearing tables. |

### 4.3 Untested / not applicable to current fixture

| ID | Region | Why deferred |
|---|---|---|
| **H29** | TDEF column-usage-map terminator for tables with zero indexes | RT_Customers always has a PK; can't manifest. Verify before dismissing if a no-index path is added. |
| **H30** | TDEF tail / pad bytes after `namePos` on continuation pages | Buffer is `new byte[…]` (zero-init); revisit if a buffer pool is introduced. |
| **H31** | NUMERIC col-desc bytes [13..14] zero | Probe N/A — RT_Customers has no NUMERIC column. |
| **H35** | `AUTO_NUMBER_GUID_FLAG_MASK = 0x40` for GUID auto-number | Latent issue; current fixtures use LONG auto-number. |
| **H36** | real-idx `unique_entry_count` slot drift after inserts | For empty PK, `unique_entry_count == row_count == 0`; OK at create time. |

### 4.4 Disconfirmed earlier (per `round-trip-test-failures.md` — do **not** re-test)

- LvProp 12-byte payload as dangling chained-LVAL (H20)
- Writer-private `0x08` NOT-NULL flag (already removed)
- Unconditional `ExtraFlags = 0x01` on TEXT/MEMO (already removed)
- TDEF magic stamps `0x00000659` / `0x00000783` (already correct)
- DB-header modify counter at `0x0E02` (manually patched, still fails)
- Page 1 GPM updates for appended pages (append-only pages already "in-use")
- MSysACEs rows (already inserted with correct `FInheritable` column)
- Catalog row Name UCS-2 vs UTF-16 encoding (cosmetic only)
- All index-leaf issues #15–18 (compact passes; only `OpenRecordset` fails)

### 4.5 Adjacent issues newly exposed by the H48 fix

H48 unblocking `OpenRecordset` exposed pre-existing latent bugs in code
paths that DAO never previously reached. These are **not** regressions
caused by H48; the relevant tests have been re-skipped with precise new
reasons referencing this section.

| Test | Symptom | Suspected next-investigation focus |
|---|---|---|
| `DaoRelationshipEnforcement_FkViolation_RaisesError` | DAO INSERT into Child table with non-existent ParentId succeeds (no FK violation raised) | Writer's MSysRelationships entry not DAO-recognised for runtime FK enforcement |
| `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` | DAO `$rs.MoveFirst()` raises `"No current record"` | MEMO long-value page or row-data-page linkage not DAO-readable; rows invisible to DAO |
| `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` | DAO INSERT after writer's last autonumber row raises `"duplicate values in the index"` | Autonumber-continuation seed and/or PK index leaf does not surface writer rows to DAO's uniqueness check |
| `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` | DAO Compact on Agile-encrypted writer ACCDB raises `"Unrecognized database format"` | Encryption-header / page-encryption issue, distinct from TDEF tdef_len defect |
| `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` | DAO Compact succeeds but post-compact RowCount=0 (writer-inserted rows dropped) on FK-bearing tables | Adjacent FK / data-page-pointer issue surfacing once TDEF is DAO-readable |
| `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` | Same as above with composite-PK fixture | Same as above |

---

## 5. Probe harness — `dao-baseline-diff.md`

The probe authoritatively answers TDEF-byte-level questions in a single
execution. Layout of the report:

| § | Section | Purpose |
|---|---|---|
| §0 | Outcomes | DAO authoring + `CompactDatabase` PASS/FAIL for both copies |
| §1 | File-level summary | page counts, page deltas |
| §2 | RT_Customers catalog row | Id / ParentId / Type / Flags / TDEF page |
| §3 | Changed/added pages by type | per-page byte-type table for the divergence set |
| §4 | RT_Customers TDEF (raw bytes) | side-by-side hex of writer vs DAO TDEFs |
| §5 | New MSysObjects row bytes | the row-body hex dump that exposed H46 |
| §6 | Pages DAO modifies that the writer never touches | DAO-only diffs (system metadata pages) |
| §7 | H26-H45 accumulating hypothesis diff | one-row-per-hypothesis PASS/FAIL table |

**Run:**

```pwsh
$env:DIAG_RT_DAO_BASELINE = "1"
dotnet run --project JetDatabaseWriter.FormatProbe
```

Output lands under `%TEMP%\JetDatabaseWriter.RtDaoBaseline\`.

---

## 6. Sources to consult next (only if H46 is disconfirmed)

### 6.1 Primary ground truth

1. **Jackcess** — [`jahlborn/jackcess`](https://github.com/jahlborn/jackcess)
   on GitHub:
   - [`ColumnImpl.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/ColumnImpl.java)
   - [`IndexData.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/IndexData.java)
   - [`IndexImpl.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/IndexImpl.java)
   - [`TableImpl`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/TableImpl.java)
     (`writeTableDefinitionHeader`, `createUsageMapDefinitionBuffer`)
   - [`JetFormat`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/JetFormat.java)
     (Jet4 / Jet12 / Jet14 / Jet16 / Jet17 offsets)
   - [`PropertyMaps`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/PropertyMaps.java)
     (LvProp encoding — directly relevant to H46)
2. **mdbtools** —
   [`HACKING.md`](https://github.com/mdbtools/mdbtools/blob/dev/HACKING.md)
   plus `src/libmdb/table.c` (`mdb_read_table`).

### 6.2 Third-party deep-dive (off-the-beaten-path)

- UtterAccess.com forum threads (Albert D. Kallal, Hans Vogelaar)
- Stephen Lebans' archived site (Wayback Machine)
- Allen Browne's allenbrowne.com — Access compaction & corruption FAQs
- Tony Toews' Microsoft Access FAQ (granite.ab.ca/access)
- MSDN Archive / DevBlogs — Lucas Sanders, Clint Covington, Andy Baron, Mary Chipman
- CodeProject "Jet database engine internals" articles (early 2000s)
- Jackcess GitHub Discussions + SourceForge tracker
- mdbtools mailing list archives (Brian Bruns' CVS commits)
- Aspose.Cells / Spire.XLS / DBeaver / SQL Workbench/J ACE-driver source + changelogs

### 6.3 Specifications

- **MS-CFB** + **MS-OFCRYPTO** — already in the codebase. **No `[MS-MDB]`
  spec exists**; ACE is undocumented by Microsoft.
