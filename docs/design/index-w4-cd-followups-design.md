# W4-C follow-up sub-phases — design notes

**Status:** Phase-1 design document. No code changes accompany this file.
**Scope:** Eliminate the three remaining triggers that fall the surgical
multi-level path (`AccessWriter.TrySurgicalMultiLevelMaintainAsync`) back to
the W4-D bulk descend-walk-rebuild path.
**Companion:** [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md)
§7.12 (W4-C-3 / W4-C-4 — what shipped) and §9 (open W4-C follow-ups).

---

## 1. What we are closing

The W4-C-3 / W4-C-4 path that shipped 2026-04-27 covers the dominant case —
every change in the batch lands on the same leaf, no recursive intermediate
overflow, no leaf-becomes-empty underflow. It bails to W4-D in three
documented situations (§7.12 caveats):

| Bail | Trigger | Phase below |
|---|---|---|
| **B-1 cross-leaf change-set** | Two keys in the batch descend to different leaves. Hit by every update that moves a row across a leaf boundary, and by INSERT batches whose new keys span a leaf boundary. | W4-C-5 |
| **B-2 leaf underflow** | Splice empties a leaf entirely. Currently bails because the surgical path has no merge / sibling-redistribute logic. | W4-C-6 |
| **B-3 intermediate overflow** | A 2-way leaf split tries to insert a new summary into a parent intermediate that is already at capacity. Currently bails because there is no recursive "split the intermediate, propagate one new summary up to the grandparent, repeat" loop. | W4-C-7 |

Each B-row is independent — fixing any one of them shrinks the W4-D fall-back
rate without forcing the others to ship. A reasonable order is **W4-C-5 →
W4-C-6 → W4-C-7**, biggest workload reduction first, simplest commit-order
story first. None of them changes the on-disk byte format produced by
`IndexLeafPageBuilder.BuildLeafPage` / `IndexBTreeBuilder.TryBuildIntermediatePage`;
they are pure orchestration changes inside `AccessWriter`.

---

## 2. Shared invariants every sub-phase MUST preserve

The W4-C-3 / W4-C-4 commit order pattern (append → patch sibling →
rewrite-in-place ancestors) leans on the page cache + `WritePageAsync`
preserving page-level atomicity at the OS level. New sub-phases must keep
the same guarantees:

1. **No page is written twice in a single batch unless the second write
   strictly succeeds the first.** A reader concurrently scanning the index
   (e.g. the W15 unique probe inside the same `InsertRowsAsync` call) must
   never see a torn intermediate-summary state.
2. **`first_dp` in the TDEF is the LAST byte to change** when the operation
   allocated a new root. Any failure before that point leaves a fully-formed
   tree at the OLD `first_dp` plus orphaned pages — same disposal model as
   W4-D.
3. **Sibling-pointer invariant:** every leaf in the chain except the
   leftmost has `prev_page > 0`; every leaf except the rightmost has
   `next_page > 0`; leaf `prev_page` / `next_page` always point at leaf
   pages. Any sub-phase that touches sibling pointers MUST patch BOTH sides
   in the same batch.
4. **`tail_page` invariant (W18, §4.5):** every intermediate page's
   `tail_page` field equals the absolute page number of the rightmost LEAF
   reachable through that intermediate's subtree. Any sub-phase that
   changes which leaf is rightmost (i.e. touches the rightmost leaf or
   inserts a new leaf to the right of the current rightmost) MUST recompute
   `tail_page` on every ancestor up to the root. The W18 append fast path
   already does this trivially because it never inserts new leaves — the
   new sub-phases must do it explicitly.
5. **Bail must be detectable BEFORE any disk write.** Existing surgical
   helpers (`TryRebuildLeafWithSiblings`, `TryBuildIntermediatePage`)
   already return `null` on overflow without touching disk. The new
   helpers must follow the same contract: build candidate page bytes in
   memory, return `null` on any overflow, commit only after every page in
   the plan validates.

A useful refactor that benefits all three sub-phases: hoist the "build a
list of `(pageNumber, pageBytes)` writes, validate every page fits, then
commit" pattern out of `TrySurgicalMultiLevelMaintainAsync` into a small
`PageWriteBatch` builder. The current code already implicitly does this
via the `PrepareAncestorReplaceWrites` / `PrepareAncestorSplitWrites`
helpers; promoting it to a first-class type keeps the new sub-phases honest.

---

## 3. W4-C-5 — cross-leaf change-set support

### 3.1 Problem

`ConfirmKeyTargetsSamePath` rejects the batch as soon as any two keys pick
different children at any intermediate level. In practice the workloads
that hit this hardest are:

- **Bulk-insert** of N rows whose keys span a leaf boundary. `InsertRowsAsync`
  passes the whole batch as one change-set; even if each individual leaf
  insert would fit, today the whole batch falls to W4-D.
- **Cascade-update** of a parent PK column that fans out to many child rows
  whose old/new keys are on different leaves.
- **Bulk-delete** by predicate that matches rows across multiple leaves.

### 3.2 Approach — per-leaf grouping

Replace the current "verify all keys hit one leaf, then rewrite that leaf"
flow with:

1. **Path-capturing descent for the FIRST change-set key** stays unchanged
   (gives us a reference path).
2. **Per-key path classification.** For every other change-set key, repeat
   the descent (cheap — pure read of cached pages), recording the
   `(targetLeafPage)` at the bottom. This is O(K · depth · log fanout); K
   is the change-set size.
3. **Group by target leaf.** Build a `Dictionary<long, (List<addEntries>,
   List<removeEntries>)>` keyed by leaf page number. Bail if the number of
   distinct leaves exceeds a sanity cap (say 64) — beyond that the W4-D
   bulk rebuild is genuinely cheaper because it reads the leaf chain
   linearly.
4. **Per-leaf surgical mutate** — for each (leaf, sub-batch), run the
   existing W4-C-3 leaf-rebuild / W4-C-4 split logic. Track whether the
   leaf's max key changed and stage one parent-summary update per leaf.
5. **Commit phase.** Apply all leaf rewrites first, then all sibling-pointer
   patches, then collapse the per-leaf parent-summary updates into a
   single rebuilt parent intermediate. If two leaves share a parent
   intermediate AND both summaries changed, the parent gets rewritten
   once with both updates — falling out of the existing
   `PrepareAncestorReplaceWrites` plan once it is generalised to accept
   multiple in-page summary updates.

### 3.3 New / changed helpers

- **`AccessWriter.GroupChangesByTargetLeaf(layout, firstDp, addEntries,
   removeEntries, ct) → Dictionary<long, ChangeBucket>?`** — descent loop
  over every change-set key. Returns `null` (bail) on overshoot, malformed
  page chain, or `> MaxLeafGroupCount` distinct leaves.
- **`PrepareAncestorReplaceWrites` generalisation** — accept a
  `Dictionary<long, byte[]> leafMaxKeyChanges` keyed by leaf page number
  (the value is the NEW max key) instead of the current single
  `(leafPage, newMax)`. Walk every captured intermediate level and rewrite
  any intermediate that has at least one summary entry to update. Bail if
  any rewritten intermediate would overflow (overflow handling lives in
  W4-C-7, not here).
- **`PrepareAncestorSplitWrites` generalisation** — same shape, accepting a
  list of `(leafPage, rightLeafPage, rightMaxKey)` split events. Each
  parent intermediate gets all its inserts applied at once; bail on
  overflow.

### 3.4 Bail triggers W4-C-5 keeps

- Any single per-leaf sub-batch hits W4-C-3 / W4-C-4's existing bail
  triggers (3+ page split, leaf becomes empty, parent overflow on a
  single-leaf split that would otherwise have succeeded).
- > 64 distinct target leaves (heuristic; see §3.5 below).
- Encoder rejects any key (caught in the existing pre-pass, unchanged).
- Path-capturing descent overshoots `tail_page` for any key (same rationale
  as W4-C-3 — surgical needs deterministic `(page, child-index)` per
  level).

### 3.5 Heuristic bound rationale

A change-set spanning every leaf is exactly the workload W4-D was designed
for: linear walk of the leaf chain + linear rebuild. The crossover
depends on key encode cost vs. page IO cost. 64 is a placeholder; W4-C-5's
bench harness should sweep `MaxLeafGroupCount ∈ {8, 16, 32, 64, 128, 256}`
on a representative ACE table (50 K rows, INT key, BMP TEXT key, NUMERIC
key) and pick the value that breaks even with W4-D wall-clock. **Do not
ship a hard-coded constant without that sweep.**

### 3.6 Tests (W4-C-5)

In `JetDatabaseWriter.Tests/Core/IndexSurgicalCrossLeafMutationTests.cs` (new file):

- **`BulkInsert_AcrossTwoLeaves_RewritesBothInPlace`** — 800-row table, insert
  3 rows where keys 25, 425, 825 land on three different leaves. Assert
  ≤ 1 page appended (no fresh tree).
- **`BulkInsert_AcrossManyLeavesAtCap_StillSurgical`** — change-set hits
  exactly `MaxLeafGroupCount` leaves; assert surgical engaged (page count
  unchanged).
- **`BulkInsert_AboveCap_BailsToW4D`** — change-set hits
  `MaxLeafGroupCount + 1` leaves; assert page count strictly increased
  (W4-D behaviour preserved).
- **`CascadeUpdate_ParentPkChange_FanOutAcrossLeaves_RewritesInPlace`** —
  parent table with 5 children spanning 3 leaves; cascade-update one
  parent key; assert child index mutated in place.
- **`SharedParentIntermediate_TwoLeavesUpdated_RewritesParentOnce`** — two
  changed leaves whose summaries are both children of the same parent
  intermediate; assert that parent appears in the write batch exactly
  once (instrument via a recording `IPageWriter` fake or count overall
  page writes).

---

## 4. W4-C-6 — leaf merge / redistribute on delete underflow

### 4.1 Problem

When a delete batch empties a leaf entirely, today's surgical path
returns `null` and W4-D rebuilds the whole tree. Even a non-empty but
nearly-empty leaf is a future correctness risk: the BTree shape diverges
from what Access produces if we rebuild via W4-D versus what the surgical
path leaves behind. Access does NOT enforce a strict B+ tree fill factor,
but it also never leaves a leaf with zero entries reachable from a
non-empty tree.

### 4.2 Approach — three states post-splice

After splicing the delete batch into the affected leaf:

1. **Leaf still has ≥ 1 entry** → existing W4-C-3 in-place rewrite. No
   change needed (this is already the shipped path).
2. **Leaf becomes empty AND has at least one sibling** → **redistribute or
   merge.** Read the immediate left sibling (`prev_page`) if non-zero,
   else the immediate right sibling (`next_page`). Decode its entries.
   - **Merge case (preferred):** entries fit on one page → emit one merged
     leaf at the surviving sibling's page number, free the now-empty leaf
     by patching the surviving sibling's `prev_page`/`next_page` to skip
     it, remove the dead leaf's summary entry from the parent intermediate.
   - **Redistribute case:** sibling alone won't fit (rare on delete — the
     sibling didn't grow), the W4-C-6 path bails. (Redistribution only
     matters when overflow becomes possible, which means the sibling was
     already full. On delete that doesn't happen unless we are also
     servicing inserts in the same batch.)
3. **Leaf becomes empty AND has no sibling (it is the only leaf)** →
   the index is now empty. Emit a zero-entry single-root leaf at the
   original page number, patch `tail_page = 0` on the leaf itself.

### 4.3 Parent-intermediate handling

The parent intermediate loses one summary entry. Three cases:

- **Parent still has ≥ 2 entries** → in-place rewrite via
  `PrepareAncestorRemoveWrites` (new helper, mirrors `Replace` /
  `Split`).
- **Parent becomes single-entry** → leave it alone. Single-entry
  intermediates are valid (the seeker descends through their lone child
  pointer); collapsing them is a future optimisation (W4-C-8?).
- **Parent becomes empty** → the level above must lose one summary too.
  Recurse upward; bail to W4-D if the recursion reaches the root and the
  root would become empty (the index has zero leaves and zero
  intermediates — emit a fresh empty single-root leaf and update
  `first_dp`).

### 4.4 Commit order

1. Read the surviving sibling.
2. Build merged leaf bytes at the surviving sibling's page number; bail
   on overflow.
3. Build the parent-intermediate rewrite without the dead leaf's summary
   AND with the merged leaf's summary key updated to the merged max;
   bail on overflow (cannot happen on delete-only — the entry count
   only drops).
4. Build any further ancestor rewrites (if the parent's max changed
   because the dead leaf was the rightmost in the parent's group, or
   because a recursive empty-parent collapse is needed).
5. **Commit (in this order):**
   1. Rewrite the surviving sibling's far-side neighbour to skip the
      dead leaf (e.g. if we kept the LEFT sibling, patch the dead leaf's
      `next_page`'s `prev_page`).
   2. Rewrite the surviving sibling with merged content + adjusted
      sibling pointer.
   3. Rewrite the parent intermediate (and ancestors).
   4. Patch `first_dp` last if a new empty-tree root was emitted.

The dead leaf page is left orphaned (not reclaimed). Same disposal model
as the W4-D path — Compact & Repair sweeps it.

### 4.5 Tests (W4-C-6)

In `JetDatabaseWriter.Tests/Core/IndexSurgicalLeafMergeTests.cs` (new):

- **`Delete_EmptiesMiddleLeaf_MergesIntoLeftSibling`** — assert two leaves
  collapse to one, parent intermediate has one fewer summary, file
  page-count unchanged (only orphans).
- **`Delete_EmptiesLeftmostLeaf_MergesIntoRightSibling`** — covers the
  no-`prev_page` case.
- **`Delete_EmptiesRightmostLeaf_MergesIntoLeftSibling_TailPageUpdated`** —
  asserts `tail_page` on every ancestor up to root now points at the
  surviving (former left sibling) leaf.
- **`Delete_EmptiesOnlyLeaf_LeavesEmptySingleRoot`** — index goes back to
  the single-empty-leaf state; subsequent INSERT engages the W4-C-1
  single-leaf path.
- **`Delete_RecursivelyEmptiesIntermediate_BailsToW4D`** — set up a tree
  whose entire intermediate-level page would empty out; assert W4-D
  ran (page count strictly increased). Recursive intermediate collapse
  is out of scope for W4-C-6.
- **`Delete_LeafBecomesEmpty_OverlapWithInsertInSameBatch_BailsToW4D`** —
  current scope is delete-only underflow; mixed batches that empty AND
  refill a leaf bail (covered by W4-C-7 / future work).

---

## 5. W4-C-7 — recursive intermediate split

### 5.1 Problem

W4-C-4 splits one leaf 2-way and inserts ONE new summary into the parent
intermediate. If the parent is at capacity, the surgical path bails. The
fix is to recursively split the parent: emit a new intermediate page for
the right half of the summaries, insert one new summary into the
GRANDPARENT, repeat until either some ancestor accepts the new summary
without overflow, OR the root itself splits and a new root must be
allocated + `first_dp` patched.

### 5.2 Approach — bottom-up split with full plan validation

Phase the work into two passes:

**Pass A — plan.** Walk the captured descent path bottom-up. At each
level, ask `TryBuildIntermediatePage` to emit the post-mutation page;
if it returns `null`, greedy-split the intermediate's entry list 2-way
(same heuristic as W4-C-4's leaf split: greedy-fill the left half until
overflow, put the remainder on the right; bail on 3+ pages), allocate a
new page number for the right half, and stage `(leftPageNumber,
rightPageNumber, rightSummaryKey, rightChildPointer)` for the level
above. Continue. Bail on 3+ pages at any level OR if the loop reaches
the root AND the root itself splits — in that case allocate one MORE
new page for a fresh root intermediate with two summary entries (left
old root, right new sibling), then patch `first_dp`.

**Pass B — commit.** Append all newly-allocated pages first (right
halves at every split level + possibly a new root). Then rewrite all
left halves in place (preserving their existing page numbers). Then
patch `first_dp` last if the root was reallocated. Sibling-pointer
patches happen as part of each rewrite.

### 5.3 New helpers

- **`AccessWriter.PlanIntermediateSplitChain(layout, capturedPath,
   leafSplitEvent, ct) → IntermediateSplitPlan?`** — returns the full
  list of `(level, leftPageNumber, rightPageNumber, leftEntries,
  rightEntries, parentSummary)` tuples + a boolean `RootReallocated`
  flag + the new root page number when applicable. Bails on 3+ pages
  at any level.
- **`AccessWriter.CommitIntermediateSplitPlan(plan, ct)`** — pure
  commit; no validation. Order: append all new pages → rewrite all
  left halves in place → patch `first_dp` last.
- **`tail_page` post-condition** — when the right-half of any
  intermediate inherits the rightmost subtree (the split was at the
  far-right end of the entry list), `tail_page` on the right-half and
  on every newly-allocated ancestor must point at the rightmost leaf
  of the new subtree. The plan builder MUST compute this once per
  level.

### 5.4 Bail triggers W4-C-7 keeps

- 3+ pages needed at any level (heuristic — `TryGreedySplitInTwo`
  fails). This is the same bail W4-C-4 already has at the leaf level.
- Captured descent overshoot at any level — same rationale as the
  shipped path.
- Root has > MaxRootSplitDepth (say 8) levels above it. Real Access
  trees rarely exceed 4 levels for ≤ 10 M rows; deeper trees are
  almost certainly malformed input and W4-D's read-everything pass
  has a better chance of recovering.

### 5.5 Tests (W4-C-7)

In `JetDatabaseWriter.Tests/Core/IndexSurgicalIntermediateSplitTests.cs` (new):

- **`SplitLeaf_ParentIntermediateOverflow_SplitsParentInPlace`** — set up
  a parent intermediate at capacity, split a child leaf, assert one new
  intermediate page appended, parent rewritten in place, `first_dp`
  unchanged.
- **`SplitLeaf_RecursiveSplitTwoLevels_AppendsTwoIntermediates`** —
  parent AND grandparent at capacity; assert exactly two new
  intermediate pages appended.
- **`SplitLeaf_SplitsRoot_AllocatesNewRoot_PatchesFirstDp`** — root is at
  capacity; assert one new root intermediate appended, `first_dp` in
  TDEF now points at the new root, the old root is rewritten as the
  left half (kept at its existing page number).
- **`SplitLeaf_TailPageUpdatedAfterRootSplit`** — when the split is at
  the far right, assert `tail_page` on every page in the new right
  spine equals the rightmost leaf page (so W18 append fast paths still
  work post-split).
- **`SplitLeaf_ThreePageSplitNeeded_BailsToW4D`** — engineered case
  where greedy split at the leaf level overflows even into three
  pages; assert W4-D ran.

---

## 6. Open questions for the implementer

1. **Should W4-C-5 grouping reuse the descent path captured for the first
   key, or re-descend independently per key?** Re-descending is simpler
   (no shared mutable path state); reusing is cheaper (cached page bytes
   in the LRU). Suggest re-descending in W4-C-5 v1, optimise only if
   benchmarks show descent dominates.
2. **W4-C-6 + W4-C-7 interaction.** A mixed batch (some keys deleted,
   some inserted) could simultaneously empty a leaf AND overflow a
   different leaf. Current shipping plan: keep these orthogonal — if
   both occur in one batch, bail to W4-D. Future W4-C-8 could combine
   them by running W4-C-6 first then W4-C-7.
3. **Page reclamation.** Today, every surgical path leaves orphaned
   pages on success (the LEFT half of a split is the only page reused;
   right halves are always freshly appended). W4-C-6 also orphans the
   merged-away leaf. Reclaiming these into a free-page list is a
   meaningful future phase but is OUT OF SCOPE here — Compact & Repair
   handles reclamation today.
4. **Atomicity under crash.** None of these phases survive a process
   crash mid-commit cleanly today. The page cache buffers writes;
   `WritePageAsync` does not fsync per page. Adding a real write-ahead
   log is its own multi-month phase (cf. README "Concurrency"
   limitation) — these sub-phases inherit the existing best-effort
   model.

---

## 7. Implementation order + acceptance bar

| # | Sub-phase | Work | Tests | New W4-D bails removed |
|---|-----------|------|-------|------------------------|
| 1 | W4-C-5 cross-leaf grouping | Generalise `Confirm…SamePath`, `PrepareAncestor*Writes`; new `GroupChangesByTargetLeaf` | 5 | "multi-leaf change-sets always bail" |
| 2 | W4-C-6 leaf merge | New `PrepareAncestorRemoveWrites`; sibling-merge logic | 6 | "empty-leaf underflow always bails" |
| 3 | W4-C-7 intermediate split (v1: parent-of-leaf only, with root realloc) | New `TryGreedySplitIntermediateInTwo`; in-place split + grandparent absorb / root reallocation + `first_dp` patch | 3 stress / round-trip | "parent-of-leaf intermediate overflow always bails" (v1; recursive split deferred to v2) |

Acceptance bar (all three sub-phases):

1. Full test suite green (~2318 tests today; expect ~2334 after the 16
   new tests above).
2. **Manual Access compact-and-repair pass on Windows for at least one
   file from each sub-phase's test set.** The §8 validation gap stays
   open in general, but each sub-phase's PR must include a manual-pass
   note in the PR description (file produced + Access version + result).
3. README "Limitations → Indexes" bullet 4 ("Recursive intermediate
   split, leaf merge / redistribute on delete underflow, and changes
   spanning multiple leaves still fall back to the W4-D rebuild …")
   shrinks one phrase at a time as each sub-phase ships.
4. `index-and-relationship-format-notes.md` §9 "open phases" entry for
   W4 sub-phase C is updated when each sub-phase ships, mirroring the
   §7.12 structure used for W4-C-3 / W4-C-4.

---

## 8. References

- [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md)
  §4.1, §4.4, §4.5 (page header + prefix compression + `tail_page`),
  §7.9 (W4-C-1 / W4-C-2), §7.12 (W4-C-3 / W4-C-4), §9 (open phases).
- [`AccessWriter.TryMaintainIndexesIncrementalAsync`](../../JetDatabaseWriter/Core/AccessWriter.cs)
  — orchestration entry point all new sub-phases plug into.
- [`IndexLeafIncremental`](../../JetDatabaseWriter/Internal/IndexLeafIncremental.cs)
  and [`IndexBTreeBuilder`](../../JetDatabaseWriter/Internal/IndexBTreeBuilder.cs)
  — page-level helpers (`DecodeIntermediateEntries`,
  `TryRebuildLeafWithSiblings`, `TryBuildIntermediatePage`) the new
  helpers compose with.
- Jackcess reference: `IndexData.addEntry` / `removeEntry` /
  `splitIndexPages` (Apache 2.0; see `THIRD-PARTY-NOTICES.md`). The
  recursive-split walker in `splitIndexPages` is the model for
  `PlanIntermediateSplitChain`.
