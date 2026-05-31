# Design notes: MSysObjects catalog index maintenance for round-trip-safe writes

**Status:** Phase C0 + C1 shipped, and the later DAO round-trip blockers are closed. All three catalog index fixes landed: **prefix compression cap** (2026-05-03), **entry-start bitmask sentinel** (2026-05-04), and **split-path `maxPrefixLength` cap** (2026-05-04). Later work fixed the separate user-table/FK compact failures by preserving system-table rows on mapped system pages, emitting relationship catalog/ACE metadata, sharing table/index usage-map rows, and reusing single-leaf index pages in place. DAO Compact & Repair now passes for the FK round-trip tests and encrypted compact test on Access-equipped hosts. Updated 2026-05-24: generic index maintenance has grown beyond this note's original C3 wording, C3's catalog split/ancestor surface is closed by rebuilding the affected catalog index from existing index entries when in-place ancestor propagation is not safe, C4's zero-slot success case is regression-guarded, C5's linked-table object-id/index/metadata-routing path is shipped, catalog callers now throw when the MSysObjects splice path reports `false`, linked-table catalog splice failures remove the just-inserted Type 4/6 row before rethrowing, low-level catalog helpers reject duplicate `(ParentId, Name)` rows before splicing, `MSysRelationships` / `MSysACEs` / `MSysComplexColumns` inserts and `MSysACEs` deletes now use incremental-or-skip maintenance instead of bulk rebuild fallback, Jet4/ACE `MSysObjects` catalog deletes/renames fail fast when incremental maintenance cannot update indexes, Jet3 `MSysObjects` catalog splicing is regression-guarded on an Access-authored Jet3 fixture, Access-file and text linked-table DAO compact/OpenRecordset validation is closed, cached-schema ODBC CompactDatabase validation is covered, and public ODBC link creation now writes generated `LvProp` property blocks instead of the legacy placeholder. See [`round-trip-openrecordset-hypothesis.md`](round-trip-openrecordset-hypothesis.md) for the closed-out compatibility record and [`writer-disk-format-validation-matrix.md`](writer-disk-format-validation-matrix.md) for the current validation coverage matrix.

## Closed Catalog-Index State

- **C3 is closed for supported writer-created catalog mutations.** `TrySpliceCatalogIndexEntryAsync` supports Jet4/ACE and Jet3 catalog indexes. Leaf-split cases with a clean ancestor path still use in-place propagation; root-leaf splits, overshoot/no-clean-path splits, and overflowing ancestor-summary rewrites now fall back to rebuilding only the affected index tree from existing index entries plus the inserted row pointer. Remaining `false` returns are fail-fast guards for malformed index pages, impossible catalog-key encoding, single entries too large to pack, or unexpected append-position mismatches; they are not open C3 behavior. Catalog callers fail fast on that return instead of leaving unmaintained catalog indexes; linked-table catalog insertion also removes the just-inserted Type 4/6 row when the splice fails before ACE insertion.

**Driver:** Two pinned round-trip tests in [JetDatabaseWriter.Tests/RoundTrip/AccessRoundTripTests.cs](../../JetDatabaseWriter.Tests/RoundTrip/AccessRoundTripTests.cs):

- `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
- `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`

Both now run under the normal Microsoft Access guard and pass. They remain the primary Access/DAO acceptance signal for catalog, relationship, usage-map, and B-tree compatibility.

**Validation requirement:** any PR touching system-table B-tree writes MUST round-trip through Microsoft Access on Windows (open, compact-and-repair, re-open) or the DAO CompactDatabase tests named below — see §7. The two FK compact tests above are the primary automated gating signal.

> ⚠️ Reverse-engineered. Cross-reference [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §3–§5 for TDEF / leaf / sort-key formats. The MSysObjects-specific facts in §3 below are observed from `NorthwindTraders.accdb` and ought to be re-verified with `JetDatabaseWriter.FormatProbe` against any new fixture before relying on byte offsets.

---

## 1. Background

`AccessWriter.CreateTableAsync` performs three operations against the live database:

1. **Allocate and write the new TDEF page(s)** for the user table (and a leaf/usage page if any indexes are emitted).
2. **Append a row to MSysObjects** describing the new table (Id, ParentId, Name, Type, ObjectId, Lv, LvProp, …).
3. **Append rows to MSysComplexColumns / MSysRelationships** when applicable.

The original failure mode (pre-Phase C0/C1, fixed) was that step 2 wrote a new MSysObjects row but never touched the existing index leaves rooted at MSysObjects's own indexes. DAO Compact & Repair would then walk data pages, read the new TDEF's `tdef_id`, fail to look that id up in MSysObjects's PK leaf, emit JET error `-1601` to `MSysCompactError`, and either silently drop the table (single-table case) or abort with COMException `0x800A0D5C` "Object invalid or no longer set" (multi-table case).

Phase C0 + C1 (below) closed that path: every `InsertCatalogEntryAsync` call now splices the new row's keys into every real-idx leaf of MSysObjects. Later compact work closed the remaining user-table/FK allocation defects; see [`round-trip-openrecordset-hypothesis.md`](round-trip-openrecordset-hypothesis.md).

## 2. Why MSysObjects still has a specialized splice

The historical full-rebuild paths explain why MSysObjects still cannot be treated like an ordinary table even though the general index maintainer has gained stronger incremental and surgical paths.

### 2.1 `InsertSystemRowAndMaintainAsync` (`AccessWriter.cs`) — current state

Used today by `MSysRelationships`, `MSysComplexColumns`, and `MSysACEs` writes. It inserts the row, verifies the system table's real-index roots look maintainable, then:

- Skips index maintenance when the system table has no maintainable real-index roots.
- Requires `TryMaintainIndexesIncrementalAsync` for maintainable system tables, including `MSysRelationships`, `MSysComplexColumns`, and `MSysACEs`.
- Throws when incremental maintenance bails, instead of falling back to `MaintainIndexesAsync`.

`MSysACEs` delete cleanup follows the same rule: deleted ACE-row hints are routed through incremental maintenance when the table has maintainable indexes and fail fast on a bail.

Jet4/ACE `MSysObjects` catalog deletes/renames are stricter: they fail fast when the incremental path cannot update indexes instead of falling back to `MaintainIndexesAsync`, preserving the invariant that `MSysObjects` is never bulk-rebuilt by re-encoding rows the writer cannot losslessly emit.

The full rebuild path is still unsafe for MSysObjects itself because it:

- Tears down every index leaf for the target system table.
- Re-encodes every row using the writer's encoder.
- Writes out fresh leaves.

This **drops the special MSysObjects rows the writer cannot re-encode** — most visibly the "Databases" properties row (`ParentId=0xF000_0000`, holds workspace-level LvProp blobs that include connection / VBA / nav-pane state). When `MaintainIndexesAsync` re-encodes MSysObjects, that row's `LvProp` content is lost, and Access reports "could not find the object 'Databases'" on next open.

Empirically: routing MSysObjects through this path caused **every** AccessRoundTripTests case to fail, not just the two historical FK compact failures. That rejection still holds; MSysObjects must preserve existing catalog row bytes it cannot losslessly re-encode.

### 2.2 `TryMaintainIndexesIncrementalAsync` / `TrySpliceCatalogIndexEntryAsync`

The targeted Phase C1 splice path (`IndexMaintainer.TrySpliceCatalogIndexEntryAsync`) descends MSysObjects's real-idx tree, decodes the target leaf, splices the new entry, and writes it back. It now uses key-based descent, rightward sibling-chain walking, prefix-length capping, and a leaf-split path with ancestor-summary rewrites when a clean descent path is available. Both Northwind real-idx slots (ri=0 `ParentIdName`, ri=1 `Id` PK) report success for the covered table/FK compact flows.

A raw-byte decode of the spliced `Id` PK leaf (page 8, orig 239 entries pref=0 → spliced 241 entries pref=0 post-fix) and the spliced `ParentIdName` composite leaf (page 2790, orig 114 entries pref=1 → spliced 116 entries pref=1 post-fix) against the original `NorthwindTraders.accdb` confirms:

- All original entries on both pages decode losslessly after the splice (canonical-key reconstruction with the new shared prefix matches the orig canonical keys byte-for-byte).
- The two new entries on each page sort correctly relative to their neighbours under big-endian byte comparison.
- The page-shared prefix is recomputed to the longest common prefix of the new entry set; the entry-start bitmask matches the actual variable-length entry stride; the parent intermediate page (p.7) is byte-identical to the original.

**Binary page-level bisection (2026-05-03) proved that pages 8 and 2790 each individually trigger DAO rejection.** A prefix compression cap fix (same day) brought the pages much closer, and a bitmask sentinel fix (2026-05-04) resolved the N1 case entirely:

- **Page 8:** `pref_len=0` matches baseline, `free_space=1456` matches DAO. Post-sentinel fix: ✅ **PASS (N1)**.
- **Page 2790:** `pref_len=1` matches baseline, `free_space=10` matches DAO. Post-sentinel fix: ✅ **PASS (N1)**.
- **N2 (two tables):** ✅ **DAO Compact succeeds** (split-path `maxPrefixLength` cap fix landed 2026-05-04). Later FK/user-table compact failures were outside the catalog splice itself and were resolved by system-table page placement, relationship catalog/ACE metadata, shared table/index usage-map rows, and in-place single-leaf reuse.

The sentinel fix: Access/DAO writes a one-past-the-end bit in the entry-start bitmask at the position immediately after the last entry. Verified on every leaf page in NorthwindTraders.accdb. The writer was omitting this sentinel, causing DAO to reject the page during Compact & Repair.

The full-rebuild `InsertSystemRowAndMaintainAsync` path was rejected for a separate reason — it re-encodes every existing row, dropping content the writer cannot losslessly emit (the special "Databases" properties row's LvProp blob). That rejection still holds, which is why MSysObjects must use the splice path and other writer-maintained system tables now require incremental maintenance or skip only when no maintainable index roots exist.

## 3. MSysObjects index layout (NorthwindTraders.accdb, Jet4)

> **Note.** Earlier drafts claimed four indexes (`Id`, `ParentIdName`, `ParentIdType`, `Name`). Empirical inspection of `NorthwindTraders.accdb`'s MSysObjects TDEF (page 2) via `JetDatabaseWriter.FormatProbe` shows **only two real-idx slots are present** in this fixture (ri=0 keyCols=[1,2] = `ParentIdName`, ri=1 keyCols=[0] = `Id` PK); see [`format-probe-appendix-index.md`](../format-probe/format-probe-appendix-index.md) §"`MSysObjects` — TDEF page 2". The four-index shape may apply to other Access versions / fixtures; re-probe before relying on it.

| # | Index name (logical) | Real-idx slot | Columns (col_num order) | Root page (this fixture) |
|---|---|---|---|---|
| 0 | `ParentIdName` (composite) | 0 | `ParentId` (col 1, Int32 asc), `Name` (col 2, Text asc, GeneralLegacy) | leaf chain rooted at p.7 → tail leaf p.2790 (114 entries) |
| 1 | `Id` (PK) | 1 | `Id` (col 0, Int32 asc) | leaf chain rooted at p.8 (single leaf, 239 entries pre-insert) |

Per-leaf entry format follows the standard rules in [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §4: `entry_start` bitmask + sort-key bytes + 4-byte row pointer (`page << 8 | row_index_within_page`). Page-shared prefix compression (§4.4.1, `pref_len` header field) is the only compression scheme; the previously-suspected per-entry incremental scheme does not exist (§4.4.2).

The Text column `Name` uses the **General Legacy** text encoder (`JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs`). That encoder is already shipped and exercised by user-table indexes; it is reusable here.

## 4. Design as shipped

### 4.1 Scope

A **system-table-specialized leaf-splice path** invoked by `InsertCatalogEntryAsync`. Not generalised to user tables — `TryMaintainIndexesIncrementalAsync` already handles those, and user tables don't carry the LvProp / Owner edge cases that break re-encoding.

### 4.2 Entry point

No public API changes. The splicer lives on `IndexMaintainer`:

- `IndexMaintainer.TrySpliceCatalogIndexEntryAsync` — per-real-idx splice with an index-entry rebuild fallback for catalog split/ancestor cases that cannot be safely updated in place. It returns `false` only when malformed pages, impossible key encoding, an oversized single entry, or append-position mismatch prevents a safe splice/tree update. Catalog callers require success and throw on `false` for every writer format.
- `AccessWriter.TrySpliceCatalogIndexEntryAsync` — thin forwarding wrapper, called from `InsertCatalogEntryAsync` immediately after `InsertRowDataLocAsync`.

Append-only tail-leaf maintenance for ordinary index paths is handled by `IndexBTreeEditor.TryAppendToTailLeafAsync`; the catalog splice path has its own key-based leaf selection and split handling, with the page-level mutation work delegated to the same editor.

### 4.3 Algorithm (per real-idx slot)

1. **Resolve root page**: read the catalog TDEF, walk to the real-idx slot for `I.RealIndexNumber`, and read the format-specific `first_dp` offset from the per-slot physical descriptor — see [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §3.1.
2. **Encode the sort key** for the new row's index columns via `IndexKeyEncoder` (`Int32` / `Int16` ascending: big-endian, high-bit flipped; `Text` ascending: `GeneralLegacyTextIndexEncoder`; concatenated for composites).
3. **Build the entry payload**: `entry_start_bitmask + sort_key_bytes + 4-byte row pointer`. The bitmask carries one bit per leading column; for a single-row insert it is uniformly `0xFF...` (per [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §4.2).
4. **Descend the B-tree** to the target leaf via `IndexCursor` descent logic. Big-endian intermediate child pointers are preserved as part of the historical root-cause rollup in [`round-trip-openrecordset-hypothesis.md`](round-trip-openrecordset-hypothesis.md#7-historical-root-cause-rollup).
5. **Splice into the leaf**:
   - Binary-search for the sorted insertion point.
   - Account for **page-shared prefix compression** (`pref_len` header field): the new entry's prefix-stripped form depends on the entry immediately before it; recompute the page's `pref_len` to the longest common prefix of the new entry set and re-emit every entry's stripped form.
   - Update the entry-start bitmask and `free_space`.
   - Persist the page.
   - On leaf overflow, greedily split the leaf, append new right-hand pages, patch sibling pointers, and rewrite ancestor summaries when a clean descent path was captured. If there is no clean ancestor path or the ancestor-summary rewrite would overflow, rebuild only the affected catalog index tree from existing index entries plus the new row pointer and patch that real-index `first_dp`; this avoids the unsafe row re-encode performed by the old bulk system-table rebuild path.
6. **Repeat for every real-idx slot.** Partial updates leave the catalog inconsistent.

### 4.4 Why this is safe where the rebuild path isn't

We never re-encode rows we did not insert. The "Databases" row (and any other rows the writer's encoder would mangle) keeps its existing row bytes; the splice only adds one new index entry per affected real index for each catalog-row insert.

### 4.5 Transactional behaviour

When `UseTransactionalWrites` is enabled, or when the caller opened an explicit `JetTransaction`, the leaf-splice writes participate in that batch and a thrown splice failure rolls back the catalog-row insert too. With the default flush-per-page mode, linked-table catalog insertion has a local cleanup guard: if the splice path returns `false` before ACE insertion, it marks the just-inserted Type 4/6 row deleted and restores the `MSysObjects` row count before rethrowing.

## 5. Phasing

| Phase | Scope | Status |
|---|---|---|
| **C0** | Per-format leaf-page header offsets across `Constants.IndexLeafPage`, `IndexLeafPageBuilder.LeafPageLayout`, `IndexBTreeBuilder`, `IndexCursor`, `IndexPageCodec`, `IndexBTreeEditor`, and `AccessWriter.MaintainIndexesAsync`. | **Shipped 2026-05-02.** |
| **C1** | `IndexMaintainer.TrySpliceCatalogIndexEntryAsync` wired into `AccessWriter.InsertCatalogEntryAsync`; tail-leaf append for monotonic Id inserts. | **Shipped.** Splice verified byte-correct against both MSysObjects real-idx slots. Prefix compression cap fix (2026-05-03) + bitmask sentinel fix (2026-05-04) + split-path `maxPrefixLength` cap (2026-05-04) all landed. DAO Compact & Repair succeeds for N1, N2+, FK, and encrypted compact acceptance cases. |
| **C2** | Re-route `InsertSystemRowAndMaintainAsync` (used by MSysRelationships / MSysComplexColumns / MSysACEs) away from unsafe full system-table rebuilds wherever possible. | **Shipped 2026-05-23.** `MSysRelationships`, `MSysComplexColumns`, and `MSysACEs` inserts now require incremental maintenance when their index roots are maintainable, skip only when no maintainable roots exist, and throw instead of bulk-rebuilding on a bail. `MSysACEs` delete cleanup follows the same rule. |
| **C3** | General mid-tree leaf split + intermediate-page rebalancing for system tables. | **Shipped 2026-05-24.** Generic incremental maintenance now includes single-leaf, multi-level, cross-leaf, leaf-split, merge, and recursive intermediate split paths. The MSysObjects catalog splicer keeps the narrower byte-preserving in-place path for direct leaf/ancestor updates, and every supported catalog insert split/ancestor case that cannot use that path falls back to rebuilding the affected index tree from existing index entries instead of re-encoding catalog rows. Remaining `false` returns are fail-fast corruption/input/staging guards, not an open C3 gap. |
| **C4** | Harden `TryMaintainIndexesIncrementalAsync`'s slot decoder so it no longer silently returns `true` on `slots.Count == 0` for tables known to have indexes. | **Shipped 2026-05-23.** The incremental path now returns `false` with a `C1d no usable real-idx slots` bail reason, so system-table callers can fail fast instead of silently reporting success. Covered by `FastPath_Bails_WhenIndexedTdefDecodesNoRealIndexKeyColumns`. |
| **C5** | Route linked-table Type 4/6 `MSysObjects` rows through catalog-only object-id allocation and catalog-index maintenance. | **Shipped 2026-05-23; expanded 2026-05-24.** `CreateLinkedTableAsync`, `CreateLinkedOdbcTableAsync`, and `CreateLinkedTextTableAsync` allocate catalog-only negative object ids, avoid low-24 catalog-id collisions, stamp fixture-aligned flags and linked-object ACE rows, and use the MSysObjects splice path for Jet4/ACE and Jet3. Access-file and text Type 6 links additionally match DAO-authored null cache columns and DAO-shaped `Database` LVAL storage, with CompactDatabase plus compacted OpenRecordset coverage. ODBC Type 4 links have CompactDatabase coverage when created with a caller-supplied cached-schema `LvProp`; public generated ODBC links now write parseable table-level `LvProp`, and the source-column overload adds generated column targets. |

**C0 + C1 have shipped.** C2, C3, C4, and C5 are now regression-guarded for the covered writer-created table, relationship, complex-column, encrypted-output, Access-file linked-table, text linked-table, cached-schema ODBC, and generated ODBC catalog flows. DAO Compact & Repair succeeds for the DAO-compatible covered flows.

## 6. Verification

The shipped splice is exercised by:

- `IndexMaintainer.TrySpliceCatalogIndexEntryAsync` is hit on every `AccessWriter.CreateTableAsync` call against a real-idx-bearing catalog table; any user-table create going through the existing test suite covers it.
- `LinkedTableCatalogWriterTests` verifies that writer-created linked Access/ODBC/text rows allocate catalog-only negative object ids, avoid low-24 catalog-id collisions, stamp linked row flags and linked-object ACE rows, and that inserting a linked table grows the Jet4/ACE `MSysObjects` index leaves instead of leaving the catalog row unindexed. It also asserts Access-file and text linked rows leave `Lv`/`LvProp`/`LvModule`/`LvExtra` null, survive DAO CompactDatabase, and can be opened through DAO OpenRecordset after compaction. Cached-schema ODBC rows store a non-placeholder Access/DAO `LvProp` property block and survive DAO CompactDatabase; generated ODBC rows store parseable table-level `LvProp`, and the source-column overload stores generated column targets. The suite also asserts splice failures do not leave the just-inserted linked-table catalog row live, copies an Access-authored Jet3 fixture, creates a table, asserts Jet3 `MSysObjects` index leaves gain entries through the format-specific splice path, and stress-inserts catalog rows until fresh `MSysObjects` indexes promote to intermediate roots.
- The `rt-dao-baseline` FormatProbe mode (legacy `DIAG_RT_DAO_BASELINE`, implemented by `DaoBaselineProbe`) re-decodes the spliced leaves and compares them against a DAO-authored copy of the same operation, asserting every original key still decodes losslessly and the new key sorts correctly.

The two gating round-trip tests (§1) pass. The historical N2 failure was fixed by passing the original leaf `pref_len` cap into split-product pages; later FK compact work fixed relationship/system-table allocation metadata outside this catalog-splice subsystem.

## 7. Validation protocol

Per the policy in `index-and-relationship-format-notes.md` §8, any code that writes to system-table B-trees must:

1. Pass the two gating tests in §1 above (which exercise DAO Compact & Repair via `AccessRoundTripEnvironment.RunDaoCompact`).
2. Be manually verified by opening the post-write `.accdb` in **Microsoft Access on Windows**, running **Database Tools → Compact & Repair Database**, and re-opening to confirm:
   - All user tables still appear in the navigation pane.
   - Relationships are intact (Database Tools → Relationships).
   - No rows are silently dropped from any user table.
3. The post-compact byte size and `MSysObjects.Id` sequence should be stable across re-runs of the same input.

## 8. Open questions

1. **Tail-only Id monotonicity is no longer the main constraint.** User-table catalog ids are physical TDEF page numbers and relationship ids are allocated from a negative id range; the current splicer descends by key instead of assuming a strict tail append. Keep testing non-tail catalog inserts when adding new Type values.
2. **Case-insensitive catalog names remain fixture-sensitive.** The observed Northwind MSysObjects shape has `ParentIdName`, not a standalone `Name` index. GeneralLegacy is case-insensitive and public create paths reject duplicate object names, but a case-only duplicate fixture would still be useful.
3. **MSysObjects `ParentIdName` uniqueness is guarded before catalog writes.** Fresh ACCDB bootstrap emits `ParentIdName` as unique, public table, relationship, and linked-table create paths perform duplicate-name checks, and low-level catalog helpers now reject duplicate `(ParentId, Name)` before splicing/direct insert. Covered by `InsertCatalogObjectAsync_DuplicateParentIdName_ThrowsBeforeSplice`.
