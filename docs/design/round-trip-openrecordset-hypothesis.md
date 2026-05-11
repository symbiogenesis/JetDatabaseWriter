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

**H23 — Real-idx flags byte placed at offset 46 instead of 42.** Writer notes
say a previous bug was at offset 42 and was "fixed" by moving to 46. mdbtools
clearly documents flags at offset 42 (4+30+4+4=42 bytes preceding it within
the 52-byte block). Worth re-verifying against a hex dump of an Access-authored
real-idx descriptor. If the writer is currently at 46 but ground truth is 42,
the writer's `flags=0x80` lands in the "9 bytes unknown" trailer, and offset
42 reads as `0x00` → DAO sees no UNKNOWN flag → rejects.

**H24 — `misc_ext` (text sort-order version) byte = 0.** mdbtools: "text sort
order version num is 2nd byte". ACE 2010+ uses version 1 (General) sort.
Writer may emit 0; DAO post-2010 may reject unknown sort version with a
name-lookup error → `''`.

**H25 — TDEF byte `0x18` (autonum_flag) is conditional in writer; Jackcess
sets it unconditionally to `0x01`** with the comment "this makes autonumbering
work in access". mdbtools agrees. Lower probability (writer's tables without
autonumber columns shouldn't need it), but cheap to test.

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

16. Use the existing `DIAG_RT_DAO_BASELINE` probe to **dump the writer-vs-DAO
    TDEF as a side-by-side hex diff** with focus on the 52-byte real-idx
    physical descriptor block (post the column descriptors and column names;
    offset depends on column count). This will resolve H21 and H23 in one
    shot.
17. Confirm H22 by hex-dumping bytes 9–10 of every column descriptor in a
    DAO-authored TDEF.

The DAO baseline probe already exists; the immediate next action is to re-run
it post the most recent fixes and grep the diff specifically for `first_dp`
mismatches and the redundant `col_num` field.
