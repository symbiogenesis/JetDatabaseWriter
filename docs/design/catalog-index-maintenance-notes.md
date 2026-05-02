# Design notes: MSysObjects catalog index maintenance for round-trip-safe writes

**Status:** Phase C1 partially implemented (`AccessWriter.TrySpliceCatalogIndexEntryAsync`, ~line 12309) and wired into `InsertCatalogEntryAsync` (~line 6310). The splice succeeds for single-column uniform-length indexes (e.g. MSysObjects's `Id` PK) but **still corrupts composite/text leaves** (e.g. MSysObjects's `ParentIdName`). The previous root-cause hypothesis (per-entry incremental prefix compression) was disproven on 2026-05-02 — see §2.2 below and [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §4.4.2 for the corrected analysis. Two gating tests below remain failing.

**Driver:** Two pinned round-trip tests in [JetDatabaseWriter.Tests/Core/AccessRoundTripTests.cs](../../JetDatabaseWriter.Tests/Core/AccessRoundTripTests.cs):

- `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`
- `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`

Both currently fail with DAO error `Could not find the object 'MSysDb'` (originally diagnosed as "PK index not updated" — the Phase C1 PK-splice landed but the composite-index splice corrupts the leaf, producing the same surface error from a different root cause).

**Validation requirement:** any PR landing this work MUST round-trip through Microsoft Access on Windows (open, compact-and-repair, re-open) — see §7. The two failing tests above are the gating signal.

> ⚠️ Reverse-engineered. Cross-reference [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §3–§5 for TDEF / leaf / sort-key formats. The MSysObjects-specific facts in §3 below are observed from `NorthwindTraders.accdb` and ought to be re-verified with `JetDatabaseWriter.FormatProbe` against any new fixture before relying on byte offsets.

---

## 1. Background

`AccessWriter.CreateTableAsync` performs three operations against the live database:

1. **Allocate and write the new TDEF page(s)** for the user table (and a leaf/usage page if any indexes are emitted).
2. **Append a row to MSysObjects** describing the new table (Id, ParentId, Name, Type, ObjectId, Lv, LvProp, …).
3. **Append rows to MSysComplexColumns / MSysRelationships** when applicable.

Step 2 is the failure point. The current code path (`InsertCatalogEntryAsync` → `InsertRowDataAsync`) writes a new row onto MSysObjects's last data page (or appends a new data page) and updates the TDEF's `num_rows` counter, but it never touches the existing index leaves rooted at the indexes recorded in MSysObjects's own TDEF.

DAO Compact & Repair then:

1. Walks data pages → finds our new TDEF pages → reads their `tdef_id`.
2. Looks up that `tdef_id` (which equals the MSysObjects `Id` value we just wrote) in MSysObjects's PK index leaf via B-tree descent.
3. Fails to find a match (because the leaf was never updated) and emits JET error `-1601` ("The search key was not found in any record.") to MSysCompactError.
4. Either silently drops the table from the compacted output (single-table case) or aborts with COMException `0x800A0D5C` "Object invalid or no longer set" (multi-table case).

Verified by reading `MSysCompactError` after the compact run. See `/memories/repo/round-trip-tests.md` for raw findings.

## 2. Why existing index-maintenance code does not solve this

There are two existing entry points that look applicable; both fail in their current form:

### 2.1 `InsertSystemRowAndMaintainAsync` (`AccessWriter.cs` ~line 8573)

Used today by `MSysRelationships` / `MSysComplexColumns` writes. It calls the full `MaintainIndexesAsync` rebuild path, which:

- Tears down every index leaf for the target system table.
- Re-encodes every row using the writer's encoder.
- Writes out fresh leaves.

This **drops the special MSysObjects rows the writer cannot re-encode** — most visibly the "Databases" properties row (`ParentId=0xF000_0000`, holds workspace-level LvProp blobs that include connection / VBA / nav-pane state). When `MaintainIndexesAsync` re-encodes MSysObjects, that row's `LvProp` content is lost, and Access reports "could not find the object 'Databases'" on next open.

Empirically: routing MSysObjects through this path causes **every** AccessRoundTripTests case to fail, not just the two we are trying to fix.

### 2.2 `TryMaintainIndexesIncrementalAsync` / `TrySpliceCatalogIndexEntryAsync`

The targeted Phase C1 splice path (`AccessWriter.TrySpliceCatalogIndexEntryAsync`, ~line 12309) does correctly descend MSysObjects's real-idx tree, decode the tail leaf, splice the new entry, and write it back — instrumentation via the `DIAG_CATALOG_SPLICE` env var (2026-05-01) confirms both real-idx slots (ri=0 ParentIdName, ri=1 Id PK) report success with no bail-out, and the `Id` PK rewrite is in fact lossless (DAO finds the new row at the expected `Id` after the splice).

The remaining failure is in the **`ParentIdName` composite leaf rewrite**. Bisecting via `DIAG_CATALOG_SPLICE_SKIP_RI=0` (skip the composite splice, only update the `Id` PK) makes DAO open the post-write file cleanly and find all 240 MSysObjects rows including `MSysDb`. Re-enabling the composite splice corrupts the leaf and DAO aborts with `Could not find the object 'MSysDb'`.

Root cause (corrected 2026-05-02): the original 2026-05-01 hypothesis blamed an unmodelled per-entry incremental prefix compression scheme. That hypothesis was wrong — the symptom was actually a 4-byte header offset error in our writer/reader (we were reading the Jet4/ACE leaf header at Jet3 offsets, so `pref_len` came back as `0` from the wrong byte). That offset bug has been fixed: `Constants.IndexLeafPage` now exposes per-format offsets and `IndexLeafPageBuilder.LeafPageLayout` carries them through every writer + reader path. See [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §4.1 (per-format offset table) and §4.4.2 (withdrawn hypothesis).

The header-offset fix did not by itself green the two gating tests — the splice still produces a leaf DAO Compact rejects with `Could not find the object 'MSysDb'`.

**Byte-level diff (2026-05-02).** A raw-byte decode of the spliced `Id` PK leaf (page 8, orig 239 entries pref=0 → spliced 241 entries pref=1) and the spliced `ParentIdName` middle composite leaf (page 2790, orig 114 entries pref=1 → spliced 116 entries pref=4) against the original `NorthwindTraders.accdb` confirms:
- All original entries on both pages decode losslessly after the splice (canonical-key reconstruction with the new shared prefix matches the orig canonical keys byte-for-byte).
- The two new entries on each page sort correctly relative to their neighbours under big-endian byte comparison.
- The page-shared prefix is recomputed to the longest common prefix of the new entry set; the entry-start bitmask matches the actual variable-length entry stride; the parent intermediate page (p.7) is byte-identical to the original; pages 2676 / 2677 / 2996 are unchanged.

The leaf-page on-disk layout is therefore not the bug. The remaining suspects have shifted off the splice writer itself onto:
- (a) the new `MSysObjects` row's `ParentId` / `Name` field encoding in the data row (the composite key we splice must agree byte-for-byte with what a re-scan of the row would produce — any mismatch in `ParentId` BE-encoding, `Name` `GeneralLegacy` text encoding, or trailing `01 00` terminator would manifest as "the index points at row X but row X has a different sort key");
- (b) the index `used_pages` / `usage_map` summary on the index definition not being marked dirty, so DAO's Compact pass reads stale free-page accounting;
- (c) the `MSysObjects.Id` autonumber counter (DBA `next-id`) not being advanced past the spliced IDs (we allocated IDs 3008 / 3010 but the on-page counter may still report the pre-splice max).

Earlier, the previous (full-rebuild) `InsertSystemRowAndMaintainAsync` path was rejected for a different reason — it re-encodes every existing row, dropping content the writer cannot losslessly emit (the special "Databases" properties row's LvProp blob). That rejection still holds. Phase C1's incremental design is the correct shape; only the leaf decoder/encoder is incomplete.

## 3. MSysObjects index layout (NorthwindTraders.accdb, Jet4)

> **Corrected 2026-05-01.** This section originally claimed four indexes (`Id`, `ParentIdName`, `ParentIdType`, `Name`). Empirical inspection of `NorthwindTraders.accdb`'s MSysObjects TDEF (page 2) via `JetDatabaseWriter.FormatProbe` shows **only two real-idx slots are present** in this fixture; see [`format-probe-appendix-index.md`](format-probe-appendix-index.md) §"`MSysObjects` — TDEF page 2" and the diagnostic logs in `/memories/repo/round-trip-tests.md` (`DIAG_CATALOG_SPLICE` output: ri=0 keyCols=[1,2], ri=1 keyCols=[0]). The four-index shape may apply to other Access versions / fixtures; re-probe before relying on it.

| # | Index name (logical) | Real-idx slot | Columns (col_num order) | Root page (this fixture) |
|---|---|---|---|---|
| 0 | `ParentIdName` (composite) | 0 | `ParentId` (col 1, Int32 asc), `Name` (col 2, Text asc, GeneralLegacy) | leaf chain rooted at p.7 → tail leaf p.2790 (114 entries) |
| 1 | `Id` (PK) | 1 | `Id` (col 0, Int32 asc) | leaf chain rooted at p.8 (single leaf, 239 entries pre-insert) |

Per-leaf entry format follows the standard rules in [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) §4: `entry_start` bitmask + sort-key bytes + 4-byte row pointer (`page << 8 | row_index_within_page`). Page-shared prefix compression (§4.4.1, `pref_len` header field) is the only compression scheme; the previously-suspected per-entry incremental scheme does not exist (§4.4.2).

The Text column `Name` uses the **General Legacy** text encoder (`JetDatabaseWriter/Internal/GeneralLegacyTextIndexEncoder.cs`). That encoder is already shipped and exercised by user-table indexes; it is reusable here.

## 4. Proposed design

### 4.1 Scope

Add a **system-table-specialized leaf-splice path** invoked only by `InsertCatalogEntryAsync` (and, optionally, by the existing MSysRelationships / MSysComplexColumns inserts to remove their dependency on the brittle full-rebuild path). Do **not** generalize this to user tables — `TryMaintainIndexesIncrementalAsync` already handles those, and user tables don't carry the LvProp/Owner edge cases that break re-encoding.

### 4.2 Public surface

No public API changes. All work is internal to `JetDatabaseWriter`. The new helper class lives at:

`JetDatabaseWriter/Internal/CatalogIndexLeafSplicer.cs`

with the entry point:

```csharp
internal static ValueTask SpliceCatalogEntryAsync(
    JetDatabaseContext ctx,
    int catalogTdefPage,             // 2 for MSysObjects, etc.
    IReadOnlyList<IndexMetadata> idx, // from AccessReader.ListIndexesAsync
    RowLocation newRowLoc,            // page/row of the just-inserted catalog row
    object[] newRowValues,            // raw column values (already typed)
    CancellationToken ct);
```

Called from `InsertCatalogEntryAsync` immediately after `InsertRowDataLocAsync`, replacing the current plain `InsertRowDataAsync` call.

### 4.3 Algorithm

For each index `I` in `idx`:

1. **Resolve root page**: read MSysObjects's TDEF, walk to the real-idx slot for `I.RealIndexNumber`, read `first_dp` (offset 38 in the per-slot 52-byte Jet4 descriptor — see `index-and-relationship-format-notes.md` §3.1).

2. **Encode the sort key** for the new row's index columns:
   - `Int32` ascending: 4-byte big-endian, with the high bit flipped (standard Jet sort-key encoding).
   - `Int16` ascending: 2-byte big-endian, high-bit flipped.
   - `Text` ascending (`Name`): delegate to `GeneralLegacyTextIndexEncoder.Encode(string, ascending: true)`.
   - Concatenate per-column sort keys for composite indexes.

3. **Build the entry payload**: `entry_start_bitmask + sort_key_bytes + 4-byte row pointer`. The `entry_start` bitmask carries one bit per leading column indicating "start of a new key for this column" — for a single-row insert it is uniformly `0xFF...` per §4.2 of the index notes.

4. **Descend the B-tree** to the target leaf: starting at `first_dp`, while page type is `0x03` (intermediate), binary-search the intermediate's child entries for the largest key ≤ our new key, follow that child pointer; stop when page type becomes `0x04` (leaf). Reuse `IndexBTreeSeeker`'s descent logic — extract `IndexBTreeSeeker.DescendToLeafAsync` as an internal helper if not already factored out.

5. **Splice into the leaf**:
   - Binary-search the leaf for the sorted insertion point.
   - **Critical:** account for **prefix compression** (§4.4 of index notes). The new entry's prefix-stripped form depends on the entry immediately before it; the entry immediately after it may need its prefix-stripped form recomputed if the new entry becomes its new predecessor.
   - Compute the byte delta from inserting the new entry plus any recomputed-suffix delta on the following entry.
   - **If the leaf has space** (used + delta ≤ 4096 − leaf header):
     - Shift trailing bytes right by `delta`.
     - Write the new entry's bytes at the insertion point.
     - Recompute & overwrite the next entry's prefix-stripped form if its predecessor changed.
     - Update the leaf's row-pointer table and `num_rows` field.
     - Persist the page.
   - **If the leaf overflows**: this is the multi-page case. For the round-trip tests we ship today, the new MSysObjects rows always sort *after* the maximum existing key (Id values are monotonically allocated and we hand out fresh ones). Implement only the **append-to-tail-leaf** sub-case in v1:
     - If the new key ≥ the max existing key in this leaf chain, walk to the rightmost leaf via `next_pg`, attempt the splice there.
     - If even the rightmost leaf is full, allocate a fresh leaf page, write the new entry as its sole content, link `prev_leaf.next_pg = new_leaf`, and propagate a new separator key up the parent intermediate page (single-level upward propagation only).
   - Defer general mid-tree splits, parent rebalancing, and underflow merges to a follow-up phase. Throw `NotSupportedException("MSysObjects index leaf split required for non-monotonic key insertion")` if the new key would land non-tail; that path is unreachable in current tests but we want a loud failure if it ever fires.

6. **Repeat for every index** in `idx`. All four MSysObjects indexes must be updated; partial updates leave the catalog inconsistent and re-trigger -1601 from a different index.

### 4.4 Why this is safe where the rebuild path isn't

We never touch entries we did not insert. The "Databases" row (and any other rows the writer's encoder would mangle) keeps its existing leaf bytes verbatim. We only **add** one new entry per index per `CreateTableAsync` call.

### 4.5 Transactional behaviour

`AccessWriter.CreateTableAsync` already runs under the writer's outer page-write batching; the leaf-splice writes participate in that batch. If any per-index splice throws, the surrounding `JetTransaction` rolls back the catalog-row insert too, so we either commit all index updates plus the row, or none of them. Callers see the same atomicity they had pre-fix.

## 5. Phasing

| Phase | Scope | Gating tests |
|---|---|---|
| **C0 (LANDED 2026-05-02)** | ~~Implement Access's per-entry incremental-prefix compression.~~ Hypothesis withdrawn — see §4.4.2 of index notes. The actual fix shipped was a 4-byte header-offset correction (per-format `LeafPageLayout` offsets across `Constants.IndexLeafPage`, `IndexLeafPageBuilder`, `IndexBTreeBuilder`, `IndexLeafIncremental`, `IndexBTreeSeeker`, `AccessWriter.MaintainIndexesAsync`). All 2501 non-round-trip tests pass. | n/a |
| **C1** | (Already partially landed as `TrySpliceCatalogIndexEntryAsync`.) The composite-index splice still corrupts the leaf even after C0; remaining bug is in the splice's entry encoding / prefix recomputation, not in the underlying decoder. | `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`, `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`. Plus all of `AccessRoundTripTests` must stay green. |
| **C2** | Re-route `InsertSystemRowAndMaintainAsync` (used by MSysRelationships/MSysComplexColumns) through the same splicer; remove dependency on `MaintainIndexesAsync`'s full-rebuild path for system tables. | Existing relationship/complex-column round-trip tests (already passing) stay green; no new test required if behaviour is byte-equivalent. |
| **C3** | Implement general mid-tree leaf split + intermediate-page rebalancing for system tables. | New unit tests in `JetDatabaseWriter.Tests/Internal/CatalogIndexLeafSplicerTests.cs` constructing synthetic many-table scenarios that force splits; round-trip through DAO. |
| **C4** | Harden `TryMaintainIndexesIncrementalAsync`'s slot decoder so it no longer silently returns `true` on `slots.Count == 0` for tables known to have indexes. Treat that case as a hard error and emit a diagnostic. | Internal-only; covered by existing user-table round-trip tests. |

**C0 has landed; C1 still requires further investigation.** C2/C3/C4 are follow-ups that reduce technical debt but are not gating.

## 6. Implementation checklist (C1)

1. Create `JetDatabaseWriter/Internal/CatalogIndexLeafSplicer.cs` with `SpliceCatalogEntryAsync` per §4.2.
2. Extract `IndexBTreeSeeker.DescendToLeafAsync` into an internal helper if it isn't already callable independently of the seek-row materialization.
3. Implement sort-key encoders for the column types MSysObjects's indexes use: `Int32`, `Int16`, `Text` (delegate to `GeneralLegacyTextIndexEncoder`). Reuse any existing encoders in `JetDatabaseWriter/Internal/IndexKeyEncoder.cs`.
4. Implement `TryAppendToTailLeafAsync`:
   - Walk `next_pg` chain to rightmost leaf.
   - Compute new entry bytes including prefix-compression against the current last entry.
   - If fits, splice; else allocate a new leaf and propagate one separator up.
5. Wire into `AccessWriter.InsertCatalogEntryAsync`, replacing the plain `InsertRowDataAsync` call. Use `AccessReader.ListIndexesAsync(2 → "MSysObjects", ct)` to get the `IndexMetadata` list.
6. Drop the workaround comment that reverts to `InsertRowDataAsync`; update XML doc comments on `InsertCatalogEntryAsync` to describe the new contract.
7. Run the gating tests:
   ```pwsh
   dotnet test --project JetDatabaseWriter.Tests --filter-class "JetDatabaseWriter.Tests.Core.AccessRoundTripTests"
   ```
   Both `SinglePk_…` and `CompositePk_…` must pass. No previously-passing test may regress.
8. Run the full suite. Only the unrelated `LimitationsTests.SpecializedColumns_CreateTableAsync_RejectsIsCalculated` text-matching failure is expected to remain.
9. Append an entry to `index-and-relationship-format-notes.md` §7 (Phase index) noting catalog index maintenance shipped.

## 7. Validation protocol

Per the policy in `index-and-relationship-format-notes.md` §8, any code that writes to system-table B-trees must:

1. Pass the two gating tests in §1 above (which exercise DAO Compact & Repair via `AccessRoundTripEnvironment.RunDaoCompact`).
2. Be manually verified by opening the post-write `.accdb` in **Microsoft Access on Windows**, running **Database Tools → Compact & Repair Database**, and re-opening to confirm:
   - All user tables still appear in the navigation pane.
   - Relationships are intact (Database Tools → Relationships).
   - No rows are silently dropped from any user table.
3. The post-compact byte size and `MSysObjects.Id` sequence should be stable across re-runs of the same input.

## 8. Open questions

1. **Are MSysObjects PK Id values truly monotonic in our writes?** `AccessWriter` allocates fresh Ids by walking the existing max + 1. Confirm there is no path (e.g. recycle-after-drop) that would let us emit a non-tail Id; if so, C1's tail-leaf-only restriction is sound.
2. **Does the `Name` index need case-insensitive collation?** GeneralLegacy is case-insensitive by default in Jet; the existing encoder handles this. Verify against a fixture where two table names differ only in case.
3. **MSysObjects `ParentIdName` uniqueness**: is this index unique? If yes, we should reject duplicate `(ParentId, Name)` *before* splicing, with a friendly exception rather than letting DAO discover the corruption later.
4. **What about Jet3 (.mdb)?** MSysObjects in Jet3 has a different real-idx descriptor size (8 bytes vs Jet4's 12) and different sort-key encoding for Text. The writer currently does not target Jet3; if it ever does, C1 must branch on `DatabaseFormat`.
