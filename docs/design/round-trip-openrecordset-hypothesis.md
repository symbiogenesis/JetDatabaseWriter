# Round-Trip `OpenRecordset` Failure — Hypothesis & Research Plan (2026-05-10)

Companion to [round-trip-test-failures.md](round-trip-test-failures.md). Captures
the current best hypothesis for why DAO `OpenRecordset` rejects writer-created
user tables with `"Unrecognized database format ''."`, plus a ranked list of
sources to consult next.

## Top hypothesis

**H21 — The user-table real-idx physical descriptor's `first_dp` (root index
page pointer) is zero or wrong.**

> **Disconfirmed (2026-05-10).** Tested via
> [WriterRealIdxFirstDpStampingTests.cs](../../JetDatabaseWriter.Tests/Indexes/WriterRealIdxFirstDpStampingTests.cs).
> Both single-table-PK and parent+child fresh-database scenarios pass: every
> non-FK real-idx slot's `first_dp` is > 1, in-range, and points at a page
> with tag `0x04` (leaf). Verified at both the reader-decoded
> (`IndexMetadata.FirstDp`) and raw on-disk-byte (`TDEF[phys + 38]`) layers.
> The writer's `CreateTableInternalAsync` correctly patches `first_dp` after
> appending the empty leaf page (see `AccessWriter.cs` lines 549–574). The
> empty `''` in DAO's error message must originate from a different
> length-prefixed read; investigate H22 / H23 / H24 next.

Reasoning chain:

1. **The empty `''` in `"Unrecognized database format ''"` is the strongest
   clue.** DAO normally fills those quotes with the offending object/format
   identifier. An empty string means DAO read a length-prefixed name or a
   page-type tag and got **zero bytes / `0x00`**.
2. **`OpenDatabase` succeeds, `OpenRecordset` fails — the differential is
   index materialization.** `OpenDatabase` parses MSysObjects + the database
   header. `OpenRecordset` is the first call that builds an `IndexData` cursor
   for the user table, which (per Jackcess `IndexData.create`) reads the
   index's **root page** (`first_dp`) off disk and validates its page-type tag
   (must be `0x03` / `0x04` / `0x01`).
3. **If `first_dp = 0`, DAO reads page 0** — the database header. Page 0's
   first byte is `0x00` (DB-def page) and bytes 4..19 are the literal ASCII
   string `Standard Jet DB ` / `Standard ACE DB `. DAO's index-page validator
   sees tag `0x00`, rejects the page, and reports the page-type identifier as
   the empty string → `''`.
4. The writer's published TDEF invariants list bytes `0x37..0x3E` (table
   usage-map pointers) but the round-trip notes never explicitly call out
   *per-real-idx* `first_dp` stamping in the 52-byte physical descriptor. The
   notes mention `flags` at phys+46 and `LeadingMagic` at phys+0 — but **no
   mention of `first_dp` at phys+38**.
5. mdbtools `HACKING.md` definitively documents the Jet4 real-idx physical
   descriptor layout: 4 unknown + 30 (10×3 col slots) + 4 `used_pages` +
   **4 `first_dp`** + 1 flags + 9 unknown = 52 bytes. Flags at offset 42,
   `first_dp` at offset 38.

## Strong secondary hypotheses

**H22 — Redundant `col_num` at column-descriptor offset 9 is missing.**
mdbtools' Jet4 column descriptor (25 bytes) shows `col_num` appearing
**twice**: bytes 5–6 (after the 4 unknown) AND bytes 9–10 (after `offset_V`).
The user's "Recommended next steps" list this candidate. `OpenDatabase` may
skip the cross-check; `OpenRecordset` reads both fields.

> **Confirmed (2026-05-10), but insufficient on its own.** Tested via
> [WriterColumnDescriptorRedundantColNumTests.cs](../../JetDatabaseWriter.Tests/Schema/WriterColumnDescriptorRedundantColNumTests.cs).
> Ground-truth side: every column descriptor in DAO-authored
> `NorthwindTraders.accdb` has bytes 9–10 == bytes 5–6 (== `col_num`).
> Writer side: a fresh `Customers` table (CustomerID/FirstName/LastName/
> BirthDate/Balance/Notes) had 4 of 6 column descriptors with mismatched
> redundant `col_num` (mismatches: `col[2] primary=2 redundant=1;
> col[3] primary=3 redundant=0; col[4] primary=4 redundant=0;
> col[5] primary=5 redundant=1`) — the `0x0001` values came from the
> writer's TEXT/MEMO branch which mis-stamped the byte intending it as
> `misc_flags` (compressed-unicode actually lives at byte 16, not 9).
> **Fix landed**: `TDefPageBuilder.BuildTDefPagesWithIndexOffsets` and
> `BuildMSysObjectsTDef` now write `col.ColNum` at descriptor byte 9
> unconditionally for the Jet4/ACE branch; the bogus
> `Wu16(page, o + 9, 0x0001)` in the TEXT/MEMO branch was removed.
> **DAO `OpenRecordset` still fails** — re-ran all 7 gated DAO/round-trip
> tests with the H22 fix in place; all 7 still throw `"Unrecognized
> database format ''."`. Tests re-skipped. The fix is correct (matches
> ground truth) but is not the sole blocker. Investigate H23 / H24 next.

**H23 — Real-idx flags byte placed at offset 46 instead of 42.** Writer notes
say a previous bug was at offset 42 and was "fixed" by moving to 46. mdbtools
clearly documents flags at offset 42 (4+30+4+4=42 bytes preceding it within
the 52-byte block). Worth re-verifying against a hex dump of an Access-authored
real-idx descriptor. If the writer is currently at 46 but ground truth is 42,
the writer's `flags=0x80` lands in the "9 bytes unknown" trailer, and offset
42 reads as `0x00` → DAO sees no UNKNOWN flag → rejects.

> **Disconfirmed (2026-05-10).** Tested via
> [WriterRealIdxFlagsOffsetTests.cs](../../JetDatabaseWriter.Tests/Indexes/WriterRealIdxFlagsOffsetTests.cs).
> Strategy: per Jackcess `IndexData.UNKNOWN_INDEX_FLAG = 0x80` is set
> unconditionally on every real-idx slot, providing a byte-pattern
> fingerprint. Surveyed every real-idx slot across every user table in
> DAO-authored `NorthwindTraders.accdb`; offset 46 had the `0x80` bit set
> on **all** slots, offset 42 did **not**. Writer-authored Customers
> table (PK + name) shows the same pattern. The writer's choice (and
> Jackcess's choice) is correct; mdbtools `HACKING.md`'s "flags at
> offset 42" is stale or miscounted. Investigate H22's residual / H24
> next.

**H24 — `misc_ext` (text sort-order version) byte = 0.** mdbtools: "text sort
order version num is 2nd byte". ACE 2010+ uses version 1 (General) sort.
Writer may emit 0; DAO post-2010 may reject unknown sort version with a
name-lookup error → `''`.

> **Disconfirmed (2026-05-10).** Tested via
> [WriterColumnDescriptorTextSortOrderTests.cs](../../JetDatabaseWriter.Tests/Schema/WriterColumnDescriptorTextSortOrderTests.cs).
> Surveyed every TEXT (`0x0A`) and MEMO (`0x0C`) column descriptor across
> every user table in DAO-authored `NorthwindTraders.accdb` (an ACE 2007+
> fixture): bytes 11-12 (LCID) == `0x0409` and bytes 13-14
> (`misc_ext` / sort-order version) == `0` on **all 25+ descriptors
> sampled** — DAO uses "General Legacy" (version 0) here, **not**
> "General" (version 1). The writer's `TDefPageBuilder.cs` TEXT/MEMO
> branch already stamps `Wi32(page, o + MiscOff, 0x00000409)`, which
> places `0x09 0x04 0x00 0x00` at bytes 11-14 — byte-exact match with
> DAO. Writer-vs-DAO comparison test (writer-authored Customers TDEF vs
> NorthwindTraders mode) passes. Hypothesis that "ACE 2010+ uses
> version 1" is empirically false for this fixture; the DAO `''` error
> source is elsewhere. Investigate H25 (TDEF byte `0x18` autonum_flag)
> or fall back to a side-by-side hex diff via Tier-3 step #16.

**H25 — TDEF byte `0x18` (autonum_flag) is conditional in writer; Jackcess
sets it unconditionally to `0x01`** with the comment "this makes autonumbering
work in access". mdbtools agrees. Lower probability (writer's tables without
autonumber columns shouldn't need it), but cheap to test.

> **Confirmed (2026-05-10), but insufficient on its own.** Tested via
> [WriterTDefAutoNumFlagTests.cs](../../JetDatabaseWriter.Tests/Schema/WriterTDefAutoNumFlagTests.cs).
> Ground-truth survey across all 23 user tables in DAO-authored
> `NorthwindTraders.accdb`: every table has TDEF byte `0x18 == 0x01`,
> including the **4 tables without any autonumber column**
> (`Catalog_TableOfContents`, `States`, `TaxStatus`, `Titles`). The
> writer's `DataPageInserter.PatchAutoNumFlag` was conditional — wrote
> `0x01` only when some column carried the `0x04` autonum flag, else
> `0x00` — and disagreed with DAO ground truth on no-autonum tables.
> **Fix landed**: `PatchAutoNumFlag` now stamps `tdefPage[0x18] = 0x01`
> unconditionally; the corresponding writer-side test asserts both
> autonum and no-autonum tables get `0x01`. **DAO `OpenRecordset` still
> fails** — re-ran all 7 gated DAO/round-trip tests with the H25 fix in
> place; all 7 still throw `"Unrecognized database format ''."`. Tests
> re-skipped. The fix is correct (matches DAO ground truth on every
> NorthwindTraders user table) but is not the sole blocker. Fall back
> to a side-by-side hex diff via Tier-3 step #16, or investigate the
> 12-byte logical-idx descriptor / TDEF-tail / LvProp blob layouts next.

## H26+ candidate hypotheses (drafted 2026-05-10 from Jackcess + mdbtools deep-dive)

The hypotheses below were synthesized after reading the canonical writers
in [`ColumnImpl.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/ColumnImpl.java),
[`IndexImpl.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/IndexImpl.java),
[`IndexData.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/IndexData.java),
[`TableImpl.writeTableDefinitionHeader` / `createUsageMapDefinitionBuffer`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/TableImpl.java),
and [`JetFormat.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/JetFormat.java)
(authoritative offsets for Jet4 / Jet12 / Jet14 / Jet16 / Jet17), and after
re-reading [TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs).
Ranked by **estimated payoff** = probability(true) × ease-of-test.

> All offsets below assume Jet4 / ACE (4096-byte page). Bytes are
> 0-indexed within the page.

### Tier-A — single-byte / single-int divergences, easy to test

- **H26 — Per-table usage-map row 0 type byte should be `0x01` (`MAP_TYPE_REFERENCE`)**.
  Jackcess `TableImpl.createUsageMapDefinitionBuffer` writes
  `MAP_TYPE_REFERENCE = 0x01` for the *owned-pages* map (row 0) and
  `MAP_TYPE_INLINE = 0x00` for the *free-space* map (row 1). The writer
  per `round-trip-test-failures.md §"Per-table usage-map page"` stamps
  `0x00` (inline) for **both** rows. DAO `OpenRecordset` may need an
  external pointer here to materialize the cursor's page-iteration plan.
  *Test:* hex-diff usage-map page bytes 0..1 against a DAO baseline.
- **H27 — Logical-idx `relIndexNumber` for non-FK indexes must be `INVALID_INDEX_NUMBER` (`0xFFFFFFFF`), not `0`**.
  `IndexImpl.writeDefinition` writes `putInt(INVALID_INDEX_NUMBER)` (= -1)
  at logical-idx bytes [13..16] when the index has no related index.
  Verify writer's `Constants.LogicalIdx.Jet4.RelIdxNumOffset` slot is
  populated with `0xFFFFFFFF` (mdbtools confirms). If writer leaves it
  as zero, DAO may treat the PK as a malformed FK and reject.
- **H28 — Logical-idx `unknown` `putInt(0)` at bytes [24..27] must be present**.
  `IndexImpl.writeDefinition` finishes with `buffer.putInt(0)` after
  `idxType` (offset 23). Writer's `IndexLayout` exposes
  `IndexTypeFieldOffset = 23` but no field beyond it — verify the writer
  does not truncate the descriptor at byte 24, or DAO's logical-idx
  iterator may misalign for any subsequent index.
- **H29 — TDEF column-usage-map block missing for tables with no long-value columns**.
  Jackcess writes 10 bytes per MEMO/OLE column then the `0xFFFF` end
  marker. The writer emits only `0xFFFF` when no long-value column
  exists. That matches Jackcess only when the descriptor block is
  syntactically present (`numIdx > 0`); the writer's
  `if (jet4 && numIdx > 0)` guard means a table with **zero** indexes
  would have no terminator at all. Round-trip schema always has a PK so
  this likely doesn't manifest, but verify before dismissing.
- **H30 — TDEF tail / pad bytes between `namePos` and end of last continuation page**.
  Jackcess pads remaining TDEF bytes with `0x00`; writer relies on
  `new byte[logicalCapacity]` zero-init. Verify the *physical* page tail
  isn't overwritten by carry-over from a previous page write into the
  same buffer.

### Tier-B — column descriptor zero-init reliance (subtle)

- **H31 — NUMERIC column-descriptor bytes [13..14] not explicitly zeroed**.
  Jackcess writes `putShort((short) 0)` after precision/scale to clear
  bytes 13-14 ("Unknown, but always 0"). Writer only stamps `[11]=prec`
  `[12]=scale` and relies on the buffer's zero-init. If the same logical
  buffer is ever reused (e.g., the `splitLogicalTDefIntoPages` path or a
  buffer pool), bytes 13-14 could carry stale data interpreted as a
  phantom sort-order version.
- **H32 — Non-text/non-numeric/non-complex column-descriptor bytes [11..14] not stamped**.
  For LONG/INT/BYTE/MONEY/FLOAT/DOUBLE/DATETIME/BOOLEAN/GUID/BINARY,
  TDefPageBuilder.cs (lines 187–211) skips the `MiscOff` write entirely
  and relies on zero-init. Jackcess explicitly writes
  `put((byte)0); put((byte)0); putShort((short)0)` for these. Same
  buffer-reuse risk as H31.
- **H33 — Column descriptor bytes [17..20] (Jackcess "always 0" `putInt`) not stamped**.
  Jackcess unconditionally writes `putInt(0)` at offset 17 immediately
  after the ExtraFlags byte. Writer skips this. Zero-init covers it for
  fresh buffers; verify nothing writes to this region during the column
  loop.
- **H34 — ExtraFlags byte at offset 16 not stamped to `0x00` for non-TEXT/MEMO columns**.
  Writer only sets `page[o + FlagsOff + 1] = col.ExtraFlags` inside the
  `T_TEXT || T_MEMO` branch. For other column types the byte is left at
  the buffer's zero. Same zero-init concern.
- **H35 — `AUTO_NUMBER_GUID_FLAG_MASK` (`0x40`) for GUID auto-number columns**.
  Jackcess `getColumnBitFlags` uses `0x40` (not `0x04`) for GUID
  auto-number. Writer's `BuildTableDefinition` always sets `0x04`. Not
  triggered by current gating tests (CustomerID/OrderID are LONG), but a
  latent issue.

### Tier-C — index physical descriptor & index leaf

- **H36 — Real-idx `unique_entry_count` slot in TDEF index-def block out of sync**.
  `IndexData.create` reads `uniqueEntryCount = tableBuffer.getInt(OFFSET_INDEX_DEF_BLOCK + idx*SIZE_INDEX_DEFINITION + 4)`.
  Writer's `UpdateRowCountAsync` mirrors `row_count` into this slot
  (`countOff = _tdef.BlockEnd + (i * _tdef.RealIdxEntrySz) + 4`), but
  for **unique** indexes (PK) `unique_entry_count == row_count` only
  when no duplicates exist. For an *empty* table with a PK, both are 0
  and that's fine. After auto-number inserts, both should be N. Verify
  the slot survives the post-insert `UpdateRowCountAsync` write.
- **H37 — Real-idx `flags` byte at offset 46 missing `IGNORE_NULLS_INDEX_FLAG` (`0x02`) or `REQUIRED_INDEX_FLAG` (`0x08`) bits for the PK**.
  Jackcess's `getIndexFlags()` returns `UNIQUE(0x01) | REQUIRED(0x08) | IGNORE_NULLS(0x02)?`
  for a PK. Writer's `Constants.TableDefinition.Jet4.RealIdx.FlagsOffset = 46`
  is correct (matches Jackcess), but the *value* may be missing the
  required bit. If writer emits only `0x01` (UNIQUE) but DAO expects
  `0x09` or `0x0B` for a PK, OpenRecordset may reject.
- **H38 — Real-idx `unknown` 4-byte putInt at offset 42..45 not zeroed**.
  Jackcess writes `putInt(0)` at offset 42. Writer's real-idx layout has
  `FlagsOffset=46` so offsets 42..45 are unstamped — relies on zero-init.
- **H39 — PK index root leaf page: header layout drift**.
  Jackcess `IndexData.NEW_ROOT_DATA_PAGE` initializes `prev/next/tail = 0`,
  `pref_len = 0`, `compressed_byte_count = 0` (Jet4 byte at offset 24),
  and the `entry_mask` region. Verify writer's empty-PK leaf page header
  bytes 0..28 byte-for-byte match a DAO-authored empty PK leaf.
- **H40 — Index-leaf entry-mask trailing byte at the end of the page**.
  Jackcess sets the entry-mask last byte to a sentinel; mdbtools also
  notes a trailing byte after the entry mask. Verify writer's leaf
  layout matches for an empty leaf.

### Tier-D — header-level / file-level

- **H41 — Jet12 `OFFSET_NEXT_COMPLEX_AUTO_NUMBER` (TDEF byte 28) not zeroed for ACCDB**.
  Per `JetFormat.Jet12Format`, ACCDB tables carry a 4-byte
  `next_complex_auto_number` at TDEF offset **28** (Jet4 `.mdb` has
  this at -1 = absent). Writer's H25 fix stamps `0x01` at byte 24
  (autonum_flag) and presumably leaves bytes 25..39 as zero — verify
  bytes 28..31 are 0 (likely OK via zero-init, but worth a one-byte
  assert).
- **H42 — Index-name UTF-16 length-prefix byte count vs character count**.
  Writer writes `Wu16(page, namePos, nameBytes.Length)` where
  `nameBytes = Encoding.Unicode.GetBytes(idx.Name)` — that's a *byte*
  count, matching Jackcess `putShort((short)(name.length()*2))`. ✓ but
  worth re-asserting that the writer never accidentally writes a
  *char* count.
- **H43 — `unknown_jet4` 4-byte field after `next_pg`**.
  Writer's `BuildTDefPagesWithIndexOffsets` writes `tdef_len` at byte 8
  and `MAGIC_TABLE_NUMBER` at byte 12. `JetFormat.Jet4Format` confirms
  this is correct (no field between bytes 8 and 12 for Jet4). ✓
- **H44 — Owned/free-space usage-map page numbers at TDEF bytes 55..62**.
  Jackcess populates `OFFSET_OWNED_PAGES = 55` and
  `OFFSET_FREE_SPACE_PAGES = 59` with the per-table usage-map page
  number (3-byte page + 1-byte row). If the writer leaves either as
  zero or points to a malformed map, DAO's cursor cannot iterate data
  pages and `OpenRecordset` would fail at materialization. **Highest
  systemic risk** — verify both 4-byte slots point at valid usage-map
  rows whose page-data block (bytes after the type byte) reflects the
  newly-allocated data pages for the table.
- **H45 — Per-table usage-map "row" inside the global-usage-map page lacks the `tdef_back_pointer` (offset 5..8 in row data)**.
  Jackcess `TableImpl.createUsageMapDefinitionBuffer` for
  `MAP_TYPE_REFERENCE` writes the TDEF page number at bytes [1..4] of
  the row payload. Verify writer's usage-map row layout.

### Recommended attack order

1. **H44** (top) + **H26** (paired): both touch the per-table usage-map
   row layout, where DAO must read the data-page list to materialize
   the cursor. A single hex-diff of the per-table usage-map row vs. a
   DAO baseline closes both at once.
2. **H27**, **H28**, **H37**: logical-idx + real-idx flag/sentinel
   verification. Add focused tests in
   `JetDatabaseWriter.Tests/Schema/` modeled on
   `WriterColumnDescriptorRedundantColNumTests`.
3. **H31**, **H32**: assert the writer explicitly writes zero (not
   relies on zero-init) for column-descriptor MiscOff bytes 11..14 on
   non-text/non-numeric columns. Cheap defensive write.
4. **H39**, **H40**: empty-leaf hex diff against a DAO-authored empty
   PK leaf.

### Quick experiment harness suggestion

Extend `JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs` with an
`accumulating diff` mode that, for the same TDEF page, side-by-side
prints the writer-emitted bytes vs. DAO-authored bytes for the
specific regions covered by H26-H45 (usage-map page, TDEF index-def
slots, logical-idx descriptor, column descriptor MiscOff). Each
hypothesis becomes a one-line PASS/FAIL row, letting the next probe
run rule out 5-10 hypotheses per execution.

> **Implemented (2026-05-10).** §7 of `dao-baseline-diff.md`. Verdicts
> below.

### §7 accumulating-diff results (probe run 2026-05-10)

Per single execution of `DIAG_RT_DAO_BASELINE=1`:

| ID | Hypothesis | Verdict | Notes |
|---|---|:---:|---|
| H22 | col-desc byte 9 redundant col_num == byte 5 col_num | ✅ PASS | (re-confirmation of H22 fix) |
| H25 | TDEF[0x18] autonum_flag == 0x01 | ✅ PASS | (re-confirmation of H25 fix) |
| H26 | per-table usage-map row 0 type byte (writer == DAO) | ✅ PASS | both writer **and** DAO stamp INLINE (`0x00`); the Jackcess-derived "DAO uses REFERENCE" claim is empirically false for an empty/small table |
| H27 | non-FK logical-idx `RelIdxNum` [13..16] == `0xFFFFFFFF` | ✅ PASS | writer already populates the sentinel correctly |
| H28 | logical-idx bytes [24..27] putInt(0) preserved | ✅ PASS | trailer int is intact |
| H31 | NUMERIC col-desc bytes [13..14] == 0 | ⚠️ N/A | RT_Customers has no NUMERIC column; cannot probe with current fixture |
| H32 | non-text/numeric/complex col-desc bytes [11..14] == 0 | ✅ PASS | LONG `CustomerID` zero across [11..14] |
| H33 | col-desc bytes [17..20] (always-0 putInt) == 0 | ✅ PASS | both columns zero across [17..20] |
| H34 | ExtraFlags byte [16] == 0 for non-TEXT/MEMO | ✅ PASS | LONG column has byte[16] == 0 |
| H37 | PK real-idx flags @46 has UNIQUE\|REQUIRED\|UNKNOWN | ✅ PASS | both writer and DAO emit `0x89` (`0x80 \| 0x08 \| 0x01`) |
| H38 | real-idx [42..45] (unknown putInt) == 0 | ✅ PASS | gap is zero in both |
| H41 | TDEF[0x1C..0x1F] `next_complex_auto_number` == 0 | ✅ PASS | both 0 |
| H42 | logical-idx name length-prefix is byte count (even) | ✅ PASS | writer correctly writes `Encoding.Unicode.GetBytes(name).Length` |
| H44 | TDEF `owned_pages` / `free_space_pages` non-zero | ✅ PASS | writer populates both 4-byte slots with valid usage-map row pointers |
| H45 | usage-map row `tdef_back_pointer` (REFERENCE rows only) | ⚠️ N/A | rows are INLINE so no back-pointer slot exists (depends on H26) |

**Summary: 13 ✅ PASS · 0 ❌ FAIL · 2 ⚠️ N/A.** The writer's TDEF page,
real-idx physical descriptor, logical-idx entry, column descriptors, and
per-table usage-map *row* match DAO byte-for-byte in every region the §7
harness can directly compare. **The DAO `OpenRecordset '' ` failure is
*not* in any of H22-H45.**

### Where to look next (post-§7)

With the entire TDEF surface ruled out, the residual failure must live
in one of these unexamined regions:

1. **PK index root leaf page (H39 + H40, Tier-C)** — the writer-emitted
   empty leaf page that `first_dp` points at. DAO `OpenRecordset` opens
   this page first via `IndexData.create`; if the leaf header (bytes
   0..28) or the entry-mask trailing byte differs from a DAO-authored
   empty leaf, the cursor builder rejects with a length-prefixed
   page-type read returning the empty string → `''`. **Now the
   highest-payoff next step.**
2. **The empty user-table data page itself.** §3 of
   `dao-baseline-diff.md` lists every page the writer added beyond the
   baseline; one is the new data page and one is the new leaf page.
   Hex-diff each against the DAO copy's equivalent page.
3. **Global usage-map (page 1).** Writer may not stamp newly-allocated
   data/leaf pages as "in-use" in the GPM bitmap for pages > 0x7FFF.
4. **MSysObjects row `LvProp` payload.** §5 of `dao-baseline-diff.md`
   already dumps the row body for both copies; compare the varIdx 8/10
   payloads byte-for-byte.

> **2026-05-10 update — Steps 1 + 4 executed, smoking gun in step 4.**
>
> **Step 1 (H39 + H40) — DISCONFIRMED.** Hex-diff of writer's PK leaf
> (page 3009) vs DAO's PK leaf (page 3003) shows only **2 bytes
> differ**, both at offset 4-5 (parent TDEF page pointer:
> `0x0BC0=3008` writer vs `0x0A6F=2671` DAO — legitimately different
> because the two files used different TDEF pages). Bytes 0..3 (page
> type, free space, header magic) and bytes 6..4095 (all zeros) are
> byte-identical. The empty PK leaf is byte-perfect.
>
> **Step 4 (H6 / LvProp) — CONFIRMED smoking gun.** §5 of
> `dao-baseline-diff.md` (after the 2026-05-10 fix to scan
> writer-added pages, not just shared-range diffs) reveals:
>
> | | Writer row | DAO row |
> |---|---:|---:|
> | Total length | **197 bytes** | **99 bytes** |
> | Fixed prefix (numCols + Id + ParentId + Type + flags + DateCreate + DateUpdate + Owner placeholder + UCS-2 "RT_Customers") | bytes 0..0x37 | bytes 0..0x37 |
> | Trailer (`0B 00 FF 40 00`) | last 5 bytes | last 5 bytes |
> | Variable-length payload | **~140 bytes** | **~42 bytes** |
>
> The writer's variable-length payload contains the magic
> `4D 52 32 00` ("**MR2\0**" = Jackcess `PropertyMaps` v2 property-bag
> magic) followed by the literal UCS-2 strings **"Required",
> "CustomerID", "Name"** — i.e., a full per-column property map
> encoding `Required=true` for both `CustomerID` and `Name` (both
> declared `IsNullable = false` in the probe authoring call).
>
> DAO's payload has none of this. The 42-byte DAO payload begins
> `00 40 05 1B 0B 00 00 00 00 00 46 00 46 00 46 00 3A 00 ...`
> ("FFF::::::::8 …") — a much smaller blob with no MR2 magic, no
> column names, no `Required` entry.
>
> **Source:**
> [JetExpressionConverter.BuildLvPropBlob](../../JetDatabaseWriter/Schema/JetExpressionConverter.cs)
> emits a `Required=true` property map entry for every NOT-NULL
> column. The accompanying code comment claims "The TDEF column-flag
> byte has no DAO-recognised bit for nullability, so LvProp is the
> only round-trip-safe place to record it." **The DAO baseline
> empirically contradicts this** — DAO has NOT-NULL columns without
> any `Required` LvProp entry, so DAO must encode nullability
> elsewhere (likely a TDEF column-flag bit not yet reverse-engineered,
> or implicitly via the index `REQUIRED` bit on the PK).
>
> ### Recommended H46 experiment
>
> **H46 — Writer-emitted `Required=true` LvProp entries cause DAO
> `OpenRecordset` to reject the table.**
>
> Test plan:
>
> 1. Temporarily change the probe authoring step in
>    [`DaoBaselineProbe.RunDaoCreateRtCustomers`](../../JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs)
>    /
>    [`AccessWriter.CreateTableInternalAsync`](../../JetDatabaseWriter/AccessWriter.cs#L626)
>    so that `BuildLvPropBlob` returns `null` (i.e., bypass the
>    Required emission for this experiment).
> 2. Re-run the probe and additionally invoke DAO
>    `Database.OpenRecordset("RT_Customers")` against the writer
>    output. If the recordset opens, **H46 is confirmed and the path
>    forward is to find the DAO-recognised TDEF column-flag bit for
>    NOT NULL** (likely `0x01` per Jackcess's `MUST_HAVE_NULL_FLAG`
>    inverse, or carried implicitly by the PK's `REQUIRED` index flag
>    for PK columns).
> 3. If the recordset still fails, the LvProp blob is *not* the sole
>    blocker; bisect the remaining ~42-byte DAO payload against the
>    writer's smaller residual to identify the next divergence.
>
> Add an `OpenRecordset` smoke step to `DaoBaselineProbe`'s DAO
> interop section so future hypothesis runs can include the
> definitive PASS/FAIL signal.

## Disconfirmed (per `round-trip-test-failures.md` — do NOT re-test)

- LvProp 12-byte payload as dangling chained-LVAL (H20, tested 2026-05-10,
  no effect)
- The writer-private `0x08` NOT-NULL flag (already removed, baseline matches DAO)
- Unconditional `ExtraFlags = 0x01` on TEXT/MEMO (already removed)
- TDEF magic stamps `0x00000659` / `0x00000783` (already correct)
- DB-header modify counter at `0x0E02` (manually patched, still fails)
- Page 1 GPM updates for appended pages (append-only pages already "in-use")
- MSysACEs rows (already inserted with correct `FInheritable` column name)
- Catalog row Name UCS-2 vs UTF-16 encoding (cosmetic only)
- All index-leaf issues #15–18 (compact passes; only `OpenRecordset` fails)

## Sources to consult next (ordered by likely payoff)

### Tier 1 — primary ground truth

1. **Jackcess** — [`jahlborn/jackcess`](https://github.com/jahlborn/jackcess)
   on GitHub. Already loaded `TableImpl.java`. Next must-reads:
   - [`ColumnImpl.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/ColumnImpl.java)
     — the byte-by-byte column descriptor writer (will resolve H22 and H24).
   - [`IndexData.writeDefinition`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/IndexData.java)
     — the real-idx 52-byte physical descriptor writer (will resolve H21 and
     H23).
   - `IndexImpl.writeDefinition` — the 28-byte logical-idx writer.
   - `JetFormat.SIZE_INDEX_DEFINITION` constant (12) and
     `SIZE_INDEX_COLUMN_BLOCK` (52 expected).
2. **mdbtools `HACKING.md`** —
   [`mdbtools/mdbtools`](https://github.com/mdbtools/mdbtools/blob/dev/HACKING.md)
   on GitHub `dev` branch. Definitive ASCII layout reference. Pair with
   mdbtools source `src/libmdb/table.c` `mdb_read_table()` for the exact
   decoder implementation.

### Tier 2 — third-party deep-dive blogs and forums (off-the-beaten-path)

3. **UtterAccess.com** — the deepest forum for Access internals, especially
   threads by Albert D. Kallal and Hans Vogelaar. Search
   `"Unrecognized database format"` + `OpenRecordset`.
4. **Stephen Lebans' archived site** (lebans.com — defunct but Wayback
   Machine has DAO/OLE-DB internals work).
5. **Allen Browne's allenbrowne.com** — Access compaction & corruption
   recovery FAQs.
6. **Tony Toews' Microsoft Access FAQ** (granite.ab.ca/access).
7. **MSDN Archive blogs** — Lucas Sanders ("Lucas's Access Blog"), Clint
   Covington (former Access PM), Andy Baron, Mary Chipman. Hosted on the
   [Microsoft DevBlogs archive](https://devblogs.microsoft.com/) and
   `web.archive.org`.
8. **CodeProject Jet/ACE deep-dive articles** — search for "Jet database
   engine internals"; the early 2000s had several reverse-engineering
   write-ups.
9. **Jackcess GitHub Discussions** — `jahlborn/jackcess/discussions` and the
   older SourceForge tracker for Jet4 quirks discovered during writer
   development. Search for "Access opens but recordset fails".
10. **mdbtools mailing list archives** on SourceForge — Brian Bruns' original
    CVS commits and discussion threads.
11. **Aspose.Cells / Spire.XLS internal docs** — these commercial libraries
    reverse-engineered ACE; their changelogs sometimes mention specific TDEF
    byte fixes.
12. **DBeaver / SQL Workbench/J Access driver source** — third-party Access
    JDBC drivers occasionally document TDEF surprises.
13. **MS-OFCRYPTO** + **MS-CFB** specs (Microsoft Open Specifications) —
    already in the codebase, but the ACE format itself is undocumented by
    Microsoft. There is **no published `[MS-MDB]` spec**.
14. **Vincent Rijmen's Jet RC4 analysis** and academic crypto papers — for
    Jet4 encryption, not relevant here.
15. **Jet Database Engine "Cardinal Sins" articles** by Joe Fallon and others
    on EggHeadCafe (archived).

### Tier 3 — targeted experimentation

16. Use the existing `DIAG_RT_DAO_BASELINE` probe to **dump the full
    writer-vs-DAO TDEF as a side-by-side hex diff** and walk every
    differing region. With H21–H25 disconfirmed or fixed, the residual
    `''` failure must be in an unexamined region — most likely the
    12-byte logical-idx descriptor, the TDEF tail (post real-idx block),
    or the LvProp blob. Bisect by region until the first non-cosmetic
    divergence is identified, then formulate H26+.
