# Design notes: MSysObjects catalog index maintenance for round-trip-safe writes

**Status:** Phase C0 + C1 shipped. All three index fixes have landed: **prefix compression cap** (2026-05-03), **entry-start bitmask sentinel** (2026-05-04), and **split-path `maxPrefixLength` cap** (2026-05-04). DAO Compact & Repair now **succeeds** for both N1 (single table) and N2+ (multiple tables + relationships) — the `MSysDb (3011)` and `Object invalid or no longer set` catalog errors are resolved. However, DAO drops rows from writer-created user tables during compact (row count = 0 post-compact), indicating a separate TDEF page layout incompatibility. This is the same issue tracked by the `DaoValidationTests` skip ("Unrecognized database format"). See [`round-trip-test-failures.md`](round-trip-test-failures.md) for the full investigation.

**Driver:** Two pinned round-trip tests in [JetDatabaseWriter.Tests/RoundTrip/AccessRoundTripTests.cs](../../JetDatabaseWriter.Tests/RoundTrip/AccessRoundTripTests.cs):

- `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
- `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`

Both currently skip with a TDEF-compatibility reason. DAO Compact & Repair now succeeds (no catalog errors), but DAO drops rows from writer-created user tables during compact (row count = 0 post-compact). The remaining blocker is the per-table TDEF page layout — same issue tracked by `DaoValidationTests` ("Unrecognized database format").

**Validation requirement:** any PR landing this work MUST round-trip through Microsoft Access on Windows (open, compact-and-repair, re-open) — see §7. The two failing tests above are the gating signal.

> ⚠️ Reverse-engineered. Cross-reference [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §3–§5 for TDEF / leaf / sort-key formats. The MSysObjects-specific facts in §3 below are observed from `NorthwindTraders.accdb` and ought to be re-verified with `JetDatabaseWriter.FormatProbe` against any new fixture before relying on byte offsets.

---

## 1. Background

`AccessWriter.CreateTableAsync` performs three operations against the live database:

1. **Allocate and write the new TDEF page(s)** for the user table (and a leaf/usage page if any indexes are emitted).
2. **Append a row to MSysObjects** describing the new table (Id, ParentId, Name, Type, ObjectId, Lv, LvProp, …).
3. **Append rows to MSysComplexColumns / MSysRelationships** when applicable.

The original failure mode (pre-Phase C0/C1, fixed) was that step 2 wrote a new MSysObjects row but never touched the existing index leaves rooted at MSysObjects's own indexes. DAO Compact & Repair would then walk data pages, read the new TDEF's `tdef_id`, fail to look that id up in MSysObjects's PK leaf, emit JET error `-1601` to `MSysCompactError`, and either silently drop the table (single-table case) or abort with COMException `0x800A0D5C` "Object invalid or no longer set" (multi-table case).

Phase C0 + C1 (below) closed that path: every `InsertCatalogEntryAsync` call now splices the new row's keys into every real-idx leaf of MSysObjects. The current gating-test failure is a separate writer defect on the new TDEF page itself — see [`round-trip-test-failures.md`](round-trip-test-failures.md).

## 2. Why existing index-maintenance code did not solve this on its own

The two pre-existing system-table index entry points each fail to address this end-to-end:

### 2.1 `InsertSystemRowAndMaintainAsync` (`AccessWriter.cs`)

Used today by `MSysRelationships` / `MSysComplexColumns` writes. It calls the full `MaintainIndexesAsync` rebuild path, which:

- Tears down every index leaf for the target system table.
- Re-encodes every row using the writer's encoder.
- Writes out fresh leaves.

This **drops the special MSysObjects rows the writer cannot re-encode** — most visibly the "Databases" properties row (`ParentId=0xF000_0000`, holds workspace-level LvProp blobs that include connection / VBA / nav-pane state). When `MaintainIndexesAsync` re-encodes MSysObjects, that row's `LvProp` content is lost, and Access reports "could not find the object 'Databases'" on next open.

Empirically: routing MSysObjects through this path causes **every** AccessRoundTripTests case to fail, not just the two we are trying to fix.

### 2.2 `TryMaintainIndexesIncrementalAsync` / `TrySpliceCatalogIndexEntryAsync`

The targeted Phase C1 splice path (`IndexMaintainer.TrySpliceCatalogIndexEntryAsync`) descends MSysObjects's real-idx tree, decodes the tail leaf, splices the new entry, and writes it back. Both real-idx slots (ri=0 `ParentIdName`, ri=1 `Id` PK) report success with no bail-out.

A raw-byte decode of the spliced `Id` PK leaf (page 8, orig 239 entries pref=0 → spliced 241 entries pref=0 post-fix) and the spliced `ParentIdName` composite leaf (page 2790, orig 114 entries pref=1 → spliced 116 entries pref=1 post-fix) against the original `NorthwindTraders.accdb` confirms:

- All original entries on both pages decode losslessly after the splice (canonical-key reconstruction with the new shared prefix matches the orig canonical keys byte-for-byte).
- The two new entries on each page sort correctly relative to their neighbours under big-endian byte comparison.
- The page-shared prefix is recomputed to the longest common prefix of the new entry set; the entry-start bitmask matches the actual variable-length entry stride; the parent intermediate page (p.7) is byte-identical to the original.

**Binary page-level bisection (2026-05-03) proved that pages 8 and 2790 each individually trigger DAO rejection.** A prefix compression cap fix (same day) brought the pages much closer, and a bitmask sentinel fix (2026-05-04) resolved the N1 case entirely:

- **Page 8:** `pref_len=0` matches baseline, `free_space=1456` matches DAO. Post-sentinel fix: ✅ **PASS (N1)**.
- **Page 2790:** `pref_len=1` matches baseline, `free_space=10` matches DAO. Post-sentinel fix: ✅ **PASS (N1)**.
- **N2 (two tables):** ✅ **DAO Compact succeeds** (split-path `maxPrefixLength` cap fix landed 2026-05-04). However, DAO drops rows from writer-created user tables during compact (row count = 0 post-compact). The catalog index pages are now correct; the remaining issue is user-table TDEF page layout incompatibility (same as `DaoValidationTests` skip: "Unrecognized database format").

The sentinel fix: Access/DAO writes a one-past-the-end bit in the entry-start bitmask at the position immediately after the last entry. Verified on every leaf page in NorthwindTraders.accdb. The writer was omitting this sentinel, causing DAO to reject the page during Compact & Repair.

The full-rebuild `InsertSystemRowAndMaintainAsync` path was rejected for a separate reason — it re-encodes every existing row, dropping content the writer cannot losslessly emit (the special "Databases" properties row's LvProp blob). That rejection still holds, which is why MSysObjects must use the splice path even though MSysRelationships / MSysComplexColumns can use the rebuild.

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

- `IndexMaintainer.TrySpliceCatalogIndexEntryAsync` — per-real-idx splice, returns `false` only when the splice cannot be done in place (in which case the caller falls back to the rebuild path; in practice this never fires for current MSysObjects writes).
- `AccessWriter.TrySpliceCatalogIndexEntryAsync` — thin forwarding wrapper, called from `InsertCatalogEntryAsync` immediately after `InsertRowDataLocAsync`.

Tail-leaf append for monotonic Id inserts is handled by `IndexMaintainer.TryAppendToTailLeafAsync`.

### 4.3 Algorithm (per real-idx slot)

1. **Resolve root page**: read the catalog TDEF, walk to the real-idx slot for `I.RealIndexNumber`, read `first_dp` (offset 38 in the per-slot 52-byte Jet4 descriptor — see [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §3.1).
2. **Encode the sort key** for the new row's index columns via `IndexKeyEncoder` (`Int32` / `Int16` ascending: big-endian, high-bit flipped; `Text` ascending: `GeneralLegacyTextIndexEncoder`; concatenated for composites).
3. **Build the entry payload**: `entry_start_bitmask + sort_key_bytes + 4-byte row pointer`. The bitmask carries one bit per leading column; for a single-row insert it is uniformly `0xFF...` (per [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §4.2).
4. **Descend the B-tree** to the target leaf via `IndexBTreeSeeker` descent logic. Big-endian intermediate child pointers (see top-of-doc "do not regress" list in [`round-trip-test-failures.md`](round-trip-test-failures.md)).
5. **Splice into the leaf**:
   - Binary-search for the sorted insertion point.
   - Account for **page-shared prefix compression** (`pref_len` header field): the new entry's prefix-stripped form depends on the entry immediately before it; recompute the page's `pref_len` to the longest common prefix of the new entry set and re-emit every entry's stripped form.
   - Update the entry-start bitmask and `free_space`.
   - Persist the page.
   - For overflow today: `TryAppendToTailLeafAsync` walks `next_pg` to the rightmost leaf and splices there. General mid-tree splits / parent rebalancing / underflow merges are deferred (Phase C3).
6. **Repeat for every real-idx slot.** Partial updates leave the catalog inconsistent.

### 4.4 Why this is safe where the rebuild path isn't

We never touch entries we did not insert. The "Databases" row (and any other rows the writer's encoder would mangle) keeps its existing leaf bytes verbatim. We only **add** one new entry per index per `CreateTableAsync` call.

### 4.5 Transactional behaviour

`AccessWriter.CreateTableAsync` runs under the writer's outer page-write batching; the leaf-splice writes participate in that batch. If any per-index splice throws, the surrounding `JetTransaction` rolls back the catalog-row insert too — either all index updates plus the row commit, or none of them.

## 5. Phasing

| Phase | Scope | Status |
|---|---|---|
| **C0** | Per-format leaf-page header offsets across `Constants.IndexLeafPage`, `IndexLeafPageBuilder.LeafPageLayout`, `IndexBTreeBuilder`, `IndexLeafIncremental`, `IndexBTreeSeeker`, `AccessWriter.MaintainIndexesAsync`. | **Shipped 2026-05-02.** |
| **C1** | `IndexMaintainer.TrySpliceCatalogIndexEntryAsync` wired into `AccessWriter.InsertCatalogEntryAsync`; tail-leaf append for monotonic Id inserts. | **Shipped.** Splice verified byte-correct against both MSysObjects real-idx slots. Prefix compression cap fix (2026-05-03) + bitmask sentinel fix (2026-05-04) + split-path `maxPrefixLength` cap (2026-05-04) all landed. **DAO Compact & Repair now succeeds for N1 and N2+.** Remaining blocker: user-table TDEF incompatibility causes DAO to drop rows during compact (see `DaoValidationTests`). |
| **C2** | Re-route `InsertSystemRowAndMaintainAsync` (used by MSysRelationships / MSysComplexColumns) through the same splicer; remove dependency on `MaintainIndexesAsync`'s full-rebuild path for system tables. | Open — non-gating. |
| **C3** | General mid-tree leaf split + intermediate-page rebalancing for system tables. | Open — non-gating. |
| **C4** | Harden `TryMaintainIndexesIncrementalAsync`'s slot decoder so it no longer silently returns `true` on `slots.Count == 0` for tables known to have indexes. | Open — internal-only. |

**C0 + C1 have shipped.** C2/C3/C4 reduce technical debt but are not gating. DAO Compact & Repair now succeeds — all catalog index issues are resolved. The remaining round-trip blocker is a per-table TDEF page layout incompatibility that causes DAO to drop rows from writer-created tables during compact (same issue as `DaoValidationTests`).

## 6. Verification

The shipped splice is exercised by:

- `IndexMaintainer.TrySpliceCatalogIndexEntryAsync` is hit on every `AccessWriter.CreateTableAsync` call against a real-idx-bearing catalog table; any user-table create going through the existing test suite covers it.
- The `rt-dao-baseline` FormatProbe mode (legacy `DIAG_RT_DAO_BASELINE`, implemented by `DaoBaselineProbe`) re-decodes the spliced leaves and compares them against a DAO-authored copy of the same operation, asserting every original key still decodes losslessly and the new key sorts correctly.

The two gating round-trip tests (§1) **do not yet pass** — they fail because the split path in `TrySpliceCatalogIndexEntryAsync` omits the `maxPrefixLength` parameter when building split-product pages, allowing `pref_len` to grow beyond the original value (see [`round-trip-test-failures.md`](round-trip-test-failures.md) §"N2 failure analysis"). The single-table N1 case (which never triggers a split on NorthwindTraders) passes DAO compact. The byte-level divergence is exclusively in the split-product pages' prefix length field.

## 7. Validation protocol

Per the policy in `index-and-relationship-format-notes.md` §8, any code that writes to system-table B-trees must:

1. Pass the two gating tests in §1 above (which exercise DAO Compact & Repair via `AccessRoundTripEnvironment.RunDaoCompact`).
2. Be manually verified by opening the post-write `.accdb` in **Microsoft Access on Windows**, running **Database Tools → Compact & Repair Database**, and re-opening to confirm:
   - All user tables still appear in the navigation pane.
   - Relationships are intact (Database Tools → Relationships).
   - No rows are silently dropped from any user table.
3. The post-compact byte size and `MSysObjects.Id` sequence should be stable across re-runs of the same input.

## 8. Open questions

1. **Are MSysObjects PK Id values truly monotonic in our writes?** `AccessWriter` allocates fresh Ids by walking the existing max + 1. Confirm there is no path (e.g. recycle-after-drop) that would let us emit a non-tail Id; if so, the tail-leaf-only restriction is sound.
2. **Does the `Name` index need case-insensitive collation?** GeneralLegacy is case-insensitive by default in Jet; the existing encoder handles this. Verify against a fixture where two table names differ only in case.
3. **MSysObjects `ParentIdName` uniqueness**: is this index unique? If yes, we should reject duplicate `(ParentId, Name)` *before* splicing, with a friendly exception rather than letting DAO discover the corruption later.
4. **What about Jet3 (.mdb)?** MSysObjects in Jet3 has a different real-idx descriptor size (8 bytes vs Jet4's 12) and different sort-key encoding for Text. The writer currently does not target Jet3 catalog writes; if it ever does, the splice must branch on `DatabaseFormat`.
