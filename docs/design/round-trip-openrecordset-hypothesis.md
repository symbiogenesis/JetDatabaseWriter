# Round-Trip `OpenRecordset` Failure — Hypothesis Log

Companion to [round-trip-test-failures.md](round-trip-test-failures.md).
Tracks why DAO `OpenRecordset` rejects writer-created user tables with
`"Unrecognized database format ''."` (`OpenDatabase` succeeds; only
recordset materialization fails).

> **Working assumption** (established by H21):
> The empty `''` in DAO's error means DAO read a length-prefixed name or
> page-type tag and got zero bytes. The failure surfaces the moment DAO
> builds an `IndexData` cursor for the user table.

---

## 1. Status (2026-05-10)

| | |
|---|---|
| **Active root-cause candidate** | **H46 — writer-emitted `Required=true` LvProp entries** |
| **Next experiment** | Bypass `BuildLvPropBlob` and re-test DAO `OpenRecordset` (see §3) |
| **Probe harness** | `DIAG_RT_DAO_BASELINE=1 dotnet run --project JetDatabaseWriter.FormatProbe` → `dao-baseline-diff.md` |
| **Hypotheses tested** | H21 – H45 (25 total) — see §4 |
| **TDEF byte regions ruled out** | All 13 testable regions match DAO byte-for-byte (§4 §7 results) |

---

## 2. Smoking gun — H46 (LvProp `Required` blob)

> Empirically isolated 2026-05-10 via §5 of `dao-baseline-diff.md`
> (after the probe was extended to scan writer-added MSysObjects pages).

### 2.1 What §5 showed

| | Writer row | DAO row |
|---|---:|---:|
| Total `MSysObjects.RT_Customers` row length | **197 bytes** | **99 bytes** |
| Fixed prefix (numCols + Id + ParentId + Type + flags + dates + UCS-2 `RT_Customers`) | bytes 0..0x37 (identical) | bytes 0..0x37 (identical) |
| Trailer (`0B 00 FF 40 00`) | last 5 bytes | last 5 bytes |
| **Variable-length payload** | **~140 bytes** | **~42 bytes** |

The writer's variable-length payload begins with the magic
`4D 52 32 00` ("**MR2\0**" — Jackcess `PropertyMaps` v2 property-bag
magic) and contains the literal UCS-2 strings **`Required`**,
**`CustomerID`**, **`Name`** — i.e., a per-column property map with
`Required=true` for both NOT-NULL columns.

DAO's payload contains **no MR2 magic, no column names, no `Required`
entry**. Its 42 bytes begin
`00 40 05 1B 0B 00 00 00 00 00 46 00 46 00 46 00 3A 00 …`
("FFF::::::::8 …") — a much smaller, opaque blob.

### 2.2 Source of the divergence

[`JetExpressionConverter.BuildLvPropBlob`](../../JetDatabaseWriter/Schema/JetExpressionConverter.cs)
emits a `Required=true` entry for every NOT-NULL column. Its in-source
comment claims:

> "The TDEF column-flag byte has no DAO-recognised bit for nullability,
> so LvProp is the only round-trip-safe place to record it."

**The DAO baseline empirically contradicts this comment.** DAO-authored
`RT_Customers` has two NOT-NULL columns (`CustomerID`, `Name`) yet
ships **no** `Required` LvProp entry. DAO must encode nullability
elsewhere — most likely:

- a TDEF column-flag bit not yet reverse-engineered, or
- implicitly via the PK index's `REQUIRED_INDEX_FLAG` (0x08) for PK
  columns plus a separate mechanism for non-PK NOT-NULL columns.

---

## 3. Active hypothesis: H46

**H46 — Writer-emitted `Required=true` LvProp entries cause DAO
`OpenRecordset` to reject the table.**

### 3.1 Test plan

1. Bypass `BuildLvPropBlob` (force return `null`) at the call site in
   [`AccessWriter.CreateTableInternalAsync`](../../JetDatabaseWriter/AccessWriter.cs#L626).
2. Add an `OpenRecordset("RT_Customers")` smoke step to
   [`DaoBaselineProbe`](../../JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs)
   so the probe yields a definitive PASS/FAIL signal.
3. Re-run `DIAG_RT_DAO_BASELINE=1` and inspect the new step output.

### 3.2 Decision tree

| OpenRecordset outcome | Conclusion | Next step |
|---|---|---|
| ✅ Opens | **H46 confirmed.** LvProp is the sole blocker. | Reverse-engineer DAO's TDEF NOT-NULL encoding (likely a column-flag bit or implied by `REQUIRED_INDEX_FLAG`). Replace `BuildLvPropBlob`'s Required-emission path with the discovered TDEF mechanism. |
| ❌ Still fails with `''` | LvProp is *one* blocker but not the only one. | Bisect the remaining ~42-byte DAO payload against the writer's smaller residual; identify the next divergence and formulate H47+. |

### 3.3 Why this is the highest-payoff experiment

Of the four post-§7 candidate regions originally listed (PK leaf page,
empty data page, global usage-map page 1, LvProp), the first three were
empirically disconfirmed:

- **PK leaf page** — writer page 3009 vs DAO page 3003 differ in only
  2 bytes, both at offset 4-5 (the parent-TDEF-page back-pointer,
  legitimately different because the two files used different TDEF
  pages). Bytes 0..3 and 6..4095 are byte-identical. (Disconfirms H39
  and H40 in one stroke.)
- **Empty user-table data page / global usage-map** — no candidate
  divergence remains after §7 verified all owned-pages /
  free-space-pages slots and per-table usage-map row layouts match.
- **LvProp** — the only region with a multi-byte writer-vs-DAO
  divergence (~98 bytes, see §2).

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

### 4.2 Confirmed-and-fixed (regression-tested) but **not** the sole blocker

> Each fix is correct and matches DAO ground truth, but DAO
> `OpenRecordset` continued to fail with `''` after each landing.

| ID | Bug | Fix | Test |
|---|---|---|---|
| **H22** | TEXT/MEMO branch overwrote redundant `col_num` at descriptor byte 9 with `0x0001` | `TDefPageBuilder` writes `col.ColNum` at byte 9 unconditionally for Jet4/ACE | [WriterColumnDescriptorRedundantColNumTests](../../JetDatabaseWriter.Tests/Schema/WriterColumnDescriptorRedundantColNumTests.cs) |
| **H25** | `DataPageInserter.PatchAutoNumFlag` wrote TDEF[0x18] = `0x00` for tables with no autonumber column | Stamp `0x01` unconditionally | [WriterTDefAutoNumFlagTests](../../JetDatabaseWriter.Tests/Schema/WriterTDefAutoNumFlagTests.cs) |

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
