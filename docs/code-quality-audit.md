# Code-Quality Audit — Open Backlog

**Scope:** the `JetDatabaseWriter` library project (production code only — tests, benchmarks,
and FormatProbe excluded except where noted).
**Date:** 2026-06-16; trimmed to open items 2026-06-30 (line numbers re-verified 2026-06-30); #1, #2, #9, and #10 updated 2026-10-02 after the writer and reader facade splits.
**Status:** this list tracks **open findings only**. #1 (god classes) and #2 (monster methods) are
**High**; #9–#11 are **Low**. Resolved findings #3–#8 were removed on 2026-06-30 — their durable
essence is preserved in repo memory and the linked design docs (for example the former #7 lives in
[concurrency-and-lock-ordering.md](design/concurrency-and-lock-ordering.md)). Finding numbers are kept
stable, so the gap between #2 and #9 is intentional.

> **Context first.** This is a disciplined, well-tested codebase: strict build settings
> (`WarningLevel 9999`, `AnalysisLevel latest-all`, warnings-as-errors, StyleCop + Roslynator +
> BannedApi), checked arithmetic, reproducible builds, ~1,492 tests across ~48k test LOC, and
> almost no `TODO`/`HACK`/dead-code debt. The items below are therefore mostly **structural and
> design-level** rather than sloppiness — but they are the ones most likely to slow future change,
> hide defects, and raise the cost of onboarding.

---

## Severity Summary

| # | Finding | Category | Severity |
|---|---------|----------|----------|
| 1 | God classes / monolithic types | Structure / cohesion | **High** |
| 2 | Monster methods (200–450 lines) | Complexity | **High** |
| 9 | Member-ordering suppressions hiding organic accretion | Maintainability | **Low** |
| 10 | `Public`/`Core` method-pair duplication | Boilerplate | **Low** |
| 11 | Tests held to a lower analyzer bar than production | Consistency | **Low** |

---

## 1. God Classes / Monolithic Types — **High**

A handful of types have accreted far too many responsibilities. None are split with `partial`,
so each is a single, monolithic, hard-to-navigate file.

| Type | Lines | Role |
|------|------:|------|
| [JetDatabaseWriter/Indexes/IndexBTreeEditor.cs](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) | 1,937 | B-tree mutation |
| [JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs) | 1,691 | attachments/multivalue |
| [JetDatabaseWriter/Indexes/IndexMaintainer.cs](../JetDatabaseWriter/Indexes/IndexMaintainer.cs) | 1,673 | index orchestration |
| [JetDatabaseWriter/Relationships/RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs) | 1,531 | FK lifecycle |
| [JetDatabaseWriter/DatabaseFile.cs](../JetDatabaseWriter/DatabaseFile.cs) | 1,420 | shared page I/O and format core |
| [JetDatabaseWriter/Tables/TableReader.cs](../JetDatabaseWriter/Tables/TableReader.cs) | 1,138 | table scans and reads |

`AccessWriter` (3,148 lines), `AccessReader` (2,885), and `AccessBase` (1,470) were the clearest
offenders and are now resolved. The root cause was the same in each: the facade doubled as the shared
context. Writer collaborators took `AccessWriter` and found each other through its internal
`Relationships`, `ComplexColumns`, and `Constraints` properties; reader helpers took `AccessReader`
and some called its public methods back; and page I/O lived in the `AccessBase` base class, so any
service that read pages held the facade object. `ReaderServices` and `WriterServices` now build each
facade's collaborators and inject their dependencies; the collaborators depend on `DatabaseFile` (the
page I/O extracted from `AccessBase`), not on a facade; and the read and write workflows live in
services (`TableReader`, `IndexRowReader`, `SchemaReader`, `TableDataWriter`, `TableSchemaEditor`,
`CatalogArtifactWriter`). The facades are now [AccessReader.cs](../JetDatabaseWriter/AccessReader.cs)
(522 lines, mostly XML docs and one-line forwarders), [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs)
(804), and [AccessBase.cs](../JetDatabaseWriter/AccessBase.cs) (35). `ServiceGraphTests` guards against
regressions, including a check that neither facade is reachable from its services at runtime.

`DatabaseFile` and `TableReader` remain large but are each cohesive around one concern (the bytes of
one open file; reading a table's rows). They are listed so they are watched, not because they mix roles.

**Why it matters:** these files exceed what a reviewer can hold in working memory, force wide-ranging
merge conflicts, and make it impossible to unit-test slices in isolation.

**Remediation:** split the remaining large types along their internal seams (B-tree descent vs. splice
vs. page rewrite in `IndexBTreeEditor`; scaffolding vs. row-level writes vs. cascades in
`ComplexColumnManager`). Moving inline logic into partial files would only hide the coupling.

---

## 2. Monster Methods (over 100 lines) — **High**

[IndexBTreeEditor.cs](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) contains several methods that are
each longer than many entire classes:

- [`TrySurgicalCrossLeafMaintainAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L1077) — **~341 lines**.
- [`TryStageIntermediateRewritesAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L1591) — **~346 lines**.
- [`TrySurgicalMultiLevelMaintainAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L600) — **~159 lines**.

Moved unchanged out of `AccessWriter` in the facade split:

- [`TableDataWriter.UpdateRowsAsync`](../JetDatabaseWriter/Tables/TableDataWriter.cs#L197) — ~146 lines.
- [`CatalogArtifactWriter.CreateCatalogTableArtifactAsync`](../JetDatabaseWriter/Catalog/CatalogArtifactWriter.cs#L230) — ~139 lines.

These methods interleave several distinct phases (descent, validation, splice, page rewrite,
parent/ancestor patching) with deep nesting and many local mutable variables. They are the riskiest
code in the repository to modify: high cyclomatic complexity, many early-return branches, and no
seams for targeted testing.

**Remediation:** extract each phase into a named, individually testable method (or a small
state-object with explicit steps). Even without changing behavior, decomposing a 340-line method into
6–8 named steps dramatically improves reviewability and lets the B-tree split/merge phases be unit
tested directly.

---

## 9. Member-Ordering Suppressions Hiding Organic Accretion — **Low**

The largest types suppress StyleCop ordering rules to tolerate mixed member layout:

- `#pragma warning disable SA1204` in
  [ComplexColumnManager.cs](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs#L25) and
  [RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs#L21).

[IndexBTreeEditor.cs](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) and
[IndexMaintainer.cs](../JetDatabaseWriter/Indexes/IndexMaintainer.cs) previously carried the same
suppression; both were removed on 2026-06-30 once their members were reordered to satisfy
SA1202/SA1204 without it — the remediation below, applied in miniature.
[AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs) dropped its copy on 2026-10-02 when the facade
split left it with nothing out of order.

These appear only in the god classes and are a tell-tale of accretion: the files grew until enforcing
member ordering became inconvenient, so the rule was switched off. They are harmless in isolation but
correlate exactly with findings #1 and #2.

**Remediation:** resolving the god-class/monster-method findings removes the need for these suppressions.

---

## 10. `Public`/`Core` Method-Pair Duplication — **Low**

Every mutating public API on `AccessWriter` is a one-line forwarder that wraps a service method in
[`RunAutoCommitAsync`](../JetDatabaseWriter/AccessWriter.cs#L732) — e.g. `InsertRowAsync` →
`TableDataWriter.InsertRowAsync`, `DropTableAsync` → `TableSchemaEditor.DropTableAsync`. Since the facade
split the pairs no longer share a class, so they no longer inflate the facade, but each service method
still repeats the same `Guard.*` + `ThrowIfDisposedOrCancelled` preamble.

**Remediation:** keep the wrapper indirection, but hoist the repeated guard/disposal preamble into the
`RunAutoCommitAsync` wrapper so the core methods start at their actual logic.

---

## 11. Tests Held to a Lower Analyzer Bar Than Production — **Low**

[JetDatabaseWriter.Tests.csproj](JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj#L9) disables
`RunAnalyzersDuringBuild`, `EnforceCodeStyleInBuild`, and `GenerateDocumentationFile` for non-Release
configurations. This is a deliberate build-speed trade-off (and documented in repo conventions), but it
means the large test corpus (~48k LOC) is only style/analyzer-checked in Release. Latent analyzer
issues in tests can accumulate unseen during day-to-day Debug work.

**Remediation:** acceptable as-is for iteration speed; just ensure CI builds Tests in Release (or with
analyzers on) so the bar is enforced before merge.

---

## Recommended Order of Attack

1. Decompose the large `IndexBTreeEditor` methods (#2) — highest defect risk per line.
2. Split the remaining large types along their internal seams (#1).
3. Hoist the repeated guard/disposal preamble into `RunAutoCommitAsync` (#10).
4. Build the Tests project in Release / with analyzers on in CI so its bar matches production (#11).
