# Code-Quality Audit — Open Findings

**Reviewed:** 2026-10-07 against `5f967ea706c83a34c5eb5bb85c176281a3d53f9b`.
**Scope:** the `JetDatabaseWriter` production library; tests and design notes were inspected as
supporting evidence. This is a source review, not a new build, test, benchmark or interoperability verdict.
**Status:** open findings only. Finding identifiers remain stable.

The repository enables strict Release analysis, nullable references, checked arithmetic and
warnings-as-errors in [Directory.Build.props](../Directory.Build.props). The findings below concern
responsibility boundaries, mutation invariants and inconsistent failure policy that those checks do
not establish. **High** denotes substantial maintenance risk in storage-changing workflows;
**Medium** denotes a concrete policy inconsistency or ambiguous internal contract. Size alone is not
evidence of a defect, and the structural findings do not establish data corruption.

File sizes count physical lines, including comments and blanks. Method spans run from the declaration
through its closing brace, excluding preceding XML documentation. Counts and source anchors refer to
the reviewed revision.

## Severity Summary

| # | Finding | Category | Severity |
|---|---------|----------|----------|
| 1 | Mutation services retain mixed responsibilities | Structure / ownership | **High** |
| 2 | Long mutation workflows couple planning and page updates | Complexity / testability | **High** |
| 12 | Index capacity decisions use broad range exceptions | Failure contracts | **Medium** |
| 13 | Duplicated row decoding has divergent malformed-value policies | Consistency / correctness | **Medium** |

## 1. Mutation Services Retain Mixed Responsibilities — **High**

Several services combine policy, catalog orchestration and physical storage edits. The useful
extraction boundaries are the responsibilities below, rather than arbitrary file-size limits.

| Type | Lines | Current concentration and useful boundary |
|------|------:|-------------------------------------------|
| [RelationshipManager](../JetDatabaseWriter/Relationships/RelationshipManager.cs) | 2,679 | Relationship validation and catalog lifecycle, physical FK index/TDEF edits, and partner-link repair during schema rewrites. Separate physical FK metadata editing from relationship orchestration. |
| [IndexMaintainer](../JetDatabaseWriter/Indexes/IndexMaintainer.cs) | 2,150 | Index discovery, key preparation, strategy selection, tree rebuilding, usage-map updates and catalog-specific tree surgery. Give tree mutation planning one owner shared by ordinary and catalog indexes. |
| [ComplexColumnManager](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs) | 1,828 | Fresh database system-table/permission scaffolding alongside complex-column allocation, attachment/multivalue writes and child deletion. Extract general system-catalog bootstrap from complex-value storage. |
| [TableSchemaEditor](../JetDatabaseWriter/Tables/TableSchemaEditor.cs) | 1,775 | Declaration validation, expression/property projection, table copy/transplant, relationship/complex-column repair and storage reclamation. Separate rewrite planning and storage ownership from declaration policy. |

Concrete seams:

- [`RelationshipManager.EmitFkLogicalIdxAsync`](../JetDatabaseWriter/Relationships/RelationshipManager.cs#L625)
  reconstructs column, physical-index and logical-index sections of a TDEF and adjusts counts. That
  byte-layout responsibility is distinct from `CreateRelationshipAsync` and `DropRelationshipAsync`.
- [`IndexMaintainer.TrySpliceCatalogIndexEntryAsync`](../JetDatabaseWriter/Indexes/IndexMaintainer.cs#L1814)
  still descends a tree, walks leaves, splices entries, reserves split pages and patches links despite
  the separate `IndexBTreeEditor` service. Changes to tree mutation span both owners.
- [`ComplexColumnManager.CreateCoreSystemTablesAsync`](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs#L104)
  creates `MSysACEs`, `MSysQueries` and `MSysRelationships`; nearby code initializes permissions and
  patches system-table header pointers. These are database bootstrap responsibilities.
- [`TableSchemaEditor.RewriteTableAsync`](../JetDatabaseWriter/Tables/TableSchemaEditor.cs#L929)
  carries constraints, properties, row projections, indexes, counters and complex/FK identities through
  temporary-table creation and either transplant or copy/swap.

**Why it matters:** an ownership or format change requires understanding several independently
changing concerns in the same service. Extracted collaborators should make those contracts explicit,
including who reserves, links and releases pages and who invalidates schema state.

**Remediation:** extract cohesive collaborators at these seams while preserving the existing injected
dependencies and transaction boundary. Keep declaration-specific validation at its appropriate service
entry point. Moving methods into partial files or moving all guards to a transaction wrapper does not
separate the responsibilities. The existing
[ServiceGraphTests](../JetDatabaseWriter.Tests/Architecture/ServiceGraphTests.cs) already check dependency
boundaries and cycles; retain those checks and extend them for new collaborators.

## 2. Long Mutation Workflows Couple Planning and Page Updates — **High**

These methods carry state across several mutation phases:

| Method | Lines | Coupled phases |
|--------|------:|----------------|
| [`IndexMaintainer.TryMaintainIndexesIncrementalCoreAsync`](../JetDatabaseWriter/Indexes/IndexMaintainer.cs#L1290) | 355 | Metadata/key preparation, mutation strategy and rebuild fallback, TDEF refresh, and usage-map updates. |
| [`IndexBTreeEditor.TryStageIntermediateRewritesAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L1679) | 346 | Pending parent operations, collapse, tail propagation, intermediate splitting and root replacement. |
| [`IndexBTreeEditor.TrySurgicalCrossLeafMaintainAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L1167) | 339 | Leaf classification/splice, empty/split handling, boundary stitching, ancestor staging and ordered writes. |
| [`IndexMaintainer.TrySpliceCatalogIndexEntryAsync`](../JetDatabaseWriter/Indexes/IndexMaintainer.cs#L1814) | 281 | Descent, leaf-chain search, splice/split selection, reservations and sibling/ancestor updates. |
| [`TableSchemaEditor.RewriteTableAsync`](../JetDatabaseWriter/Tables/TableSchemaEditor.cs#L929) | 271 | Schema/row projection, identity preservation, copy/transplant and dependent-metadata repair. |
| [`IndexBTreeEditor.TrySurgicalMultiLevelMaintainAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L690) | 159 | Leaf rewrite/split selection, sibling links and ancestor updates. |

The risk is the shared mutation state and ordering, not merely the lengths. For example,
`TrySurgicalCrossLeafMaintainAsync` maintains staged leaf writes, dropped pages, split information and
parent operations before committing them in order. `TryMaintainIndexesIncrementalCoreAsync` must
refresh its TDEF image after tree operations while preserving pending metadata changes. A helper
extraction that changes these boundaries can invalidate the workflow even if each helper looks correct.

There are already useful seams and substantial regression coverage, including
[cross-leaf mutation](../JetDatabaseWriter.Tests/Indexes/IndexSurgicalCrossLeafMutationTests.cs),
[recursive intermediate splits](../JetDatabaseWriter.Tests/Indexes/IndexSurgicalRecursiveIntermediateSplitTests.cs)
and [page reservations](../JetDatabaseWriter.Tests/Indexes/IndexPageReservationTests.cs). The remaining
problem is that many planning phases and their state are embedded in large private workflows, making
phase-specific reasoning and fault injection harder.

**Remediation:** introduce explicit mutation-plan/state types and named planning, validation and commit
steps where they clarify ownership. Preserve staging-before-write, reservation release, link/write
ordering and rollback behavior. Test refusal, cancellation and failure at phase boundaries alongside the
existing end-to-end cases. Do not impose a line-count target or replace a readable linear workflow with
indirection solely to shorten it.

## 12. Index Capacity Decisions Use Broad Range Exceptions — **Medium**

[`IndexPageCodec.TryBuildLeafPage`](../JetDatabaseWriter/Indexes/IndexPageCodec.cs#L224) and
[`TryBuildIntermediatePage`](../JetDatabaseWriter/Indexes/IndexPageCodec.cs#L368) catch every
`ArgumentOutOfRangeException` from their builders and return `null`. Their documented contract describes
entries that do not fit, but the builders also use that exception for invalid page sizes and page-number
fields. For example, [`BuildLeafPage`](../JetDatabaseWriter/Indexes/IndexPageCodec.cs#L57) throws it both
for page overflow and for a data-page pointer outside the 24-bit range.

The same ambiguity reaches callers:
[`IndexMaintainer.TrySpliceCatalogIndexEntryAsync`](../JetDatabaseWriter/Indexes/IndexMaintainer.cs#L1979)
catches a range exception and enters its leaf-overflow split path. Other tree placement paths also use
range exceptions as a normal refusal signal.

**Why it matters:** expected capacity exhaustion, invalid input and a range bug in an encoder can take
the same fallback path. This obscures the cause and makes it harder to verify which failures are safe
to retry as a split or rebuild. This review does not establish a resulting corrupt write or a measured
performance regression.

**Remediation:** calculate encoded size and format limits before writing, or return an explicit build
result distinguishing insufficient capacity from invalid metadata. Share the sizing rules with the
encoder, including prefix compression and bitmask limits. Preserve deliberate failures for invalid
pointers and layouts. Test that an overfull valid page selects splitting while invalid metadata does
not masquerade as page fullness. Coordinate this work with **I2** and **S11** in [todo.md](todo.md).

## 13. Duplicated Row Decoding Has Divergent Malformed-Value Policies — **Medium**

`RowDecodePlan` separately dispatches ordinary and calculated values for string and typed output.
The branches contain observable policy differences at the helper level:

- [`DecodeStringVariableValueAsync`](../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs#L577)
  returns `string.Empty` for a non-empty variable slot shorter than its fixed payload requires. Its
  `ArgumentException` and `IndexOutOfRangeException` handlers also return empty text without consulting
  `strictParsing`.
- [`DecodeTypedVariableValue`](../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs#L739)
  sends those cases through strict-aware failure policy.
  [`TypedRowFallbackPolicy`](../JetDatabaseWriter/ValueDecoding/TypedRowFallbackPolicy.cs#L28) throws
  `InvalidDataException` in strict mode and returns `DBNull.Value` in lenient mode.
- The calculated [string](../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs#L653) and
  [typed](../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs#L819) branches repeat the differing
  argument/range exception handling.

**Why it matters:** output representation can determine whether malformed content is reported or
looks like a legitimate empty value. A format-validation fix in one branch does not automatically
reach the others. The existing
[TypedRowFallbackPolicyTests](../JetDatabaseWriter.Tests/ValueDecoding/TypedRowFallbackPolicyTests.cs)
pin the typed helper contract; they do not establish parity across public read surfaces.

**Remediation:** centralize validation and failure policy while retaining intentional differences in
output representation and optimized decoding. Add matching malformed-input cases through string,
typed, POCO and calculated reads in both parsing modes. Preserve unreadable-value handling for
write-back snapshots and propagation of actual I/O failures. This is a concrete structural contributor
to the existing **S3** decode-policy work in [todo.md](todo.md); public-surface reproduction and regression
coverage belong to that work, rather than being claimed by this source review.

## Recommended Order

1. Pin and reconcile decode-policy parity (#13), because it affects error visibility at read boundaries.
2. Make index capacity/refusal results explicit (#12), reducing ambiguity before further tree refactoring.
3. Decompose staged index and schema mutation workflows (#2), preserving their ownership and failure invariants.
4. Extract the remaining responsibility boundaries (#1), coordinated with the relevant format and correctness work in [todo.md](todo.md).
