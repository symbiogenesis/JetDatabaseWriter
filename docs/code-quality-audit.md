# Code-Quality Audit — Open Backlog

**Scope:** the `JetDatabaseWriter` library project (production code only — tests, benchmarks,
and FormatProbe excluded except where noted).
**Date:** 2026-06-16; trimmed to open items 2026-06-30 (line numbers re-verified 2026-06-30).
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
| [JetDatabaseWriter/AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs) | 3,148 | public writer facade |
| [JetDatabaseWriter/AccessReader.cs](../JetDatabaseWriter/AccessReader.cs) | 2,885 | public reader facade |
| [JetDatabaseWriter/Indexes/IndexBTreeEditor.cs](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) | 1,937 | B-tree mutation |
| [JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs) | 1,676 | attachments/multivalue |
| [JetDatabaseWriter/Indexes/IndexMaintainer.cs](../JetDatabaseWriter/Indexes/IndexMaintainer.cs) | 1,608 | index orchestration |
| [JetDatabaseWriter/Relationships/RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs) | 1,583 | FK lifecycle |
| [JetDatabaseWriter/AccessBase.cs](../JetDatabaseWriter/AccessBase.cs) | 1,412 | shared base |

`AccessWriter` is the clearest offender. It is declared at
[AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs#L41) as a single `sealed` class that:

- implements two public interfaces (`IAccessWriter`, `IAccessSchema`) plus the `AccessBase` contract;
- owns **~15 collaborator fields** (lock coordinator, index maintainer, TDEF builder, long-value
  encoder, unique-index checker, transaction lifecycle, catalog writer, row encoder, data-page
  inserter, page allocator, relationship manager, complex-column manager, constraint registry, …);
- and still hosts table DDL, row DML, schema evolution, catalog
  bootstrap, AutoNumber bookkeeping, and disposal logic directly in its own body.

It is already a *facade over* well-factored managers — but it never finished delegating, so it is
simultaneously the facade **and** a large implementation. The composition is good; the residue left
in the facade is the smell.

**Why it matters:** these files exceed what a reviewer can hold in working memory, force wide-ranging
merge conflicts, and make it impossible to unit-test slices in isolation.

**Remediation:** push the remaining inline logic into the collaborators `AccessWriter`/`AccessReader`
already own, then split the facades into `partial` files grouped by concern (DDL / DML / catalog /
disposal) as an interim step.

---

## 2. Monster Methods (over 100 lines) — **High**

[IndexBTreeEditor.cs](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) contains several methods that are
each longer than many entire classes:

- [`TrySurgicalCrossLeafMaintainAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L1077) — **~341 lines**.
- [`TryStageIntermediateRewritesAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L1591) — **~346 lines**.
- [`TrySurgicalMultiLevelMaintainAsync`](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L600) — **~159 lines**.

In [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs):

- [`UpdateRowsCoreAsync`](../JetDatabaseWriter/AccessWriter.cs#L1151) — ~146 lines.
- [`CreateCatalogTableArtifactAsync`](../JetDatabaseWriter/AccessWriter.cs#L632) — ~139 lines.

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

- [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs#L34) — `#pragma warning disable SA1204`
  [ComplexColumnManager.cs](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs#L23) and
  [RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs#L20).

[IndexBTreeEditor.cs](../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) and
[IndexMaintainer.cs](../JetDatabaseWriter/Indexes/IndexMaintainer.cs) previously carried the same
suppression; both were removed on 2026-06-30 once their members were reordered to satisfy
SA1202/SA1204 without it — the remediation below, applied in miniature.

These appear only in the god classes and are a tell-tale of accretion: the files grew until enforcing
member ordering became inconvenient, so the rule was switched off. They are harmless in isolation but
correlate exactly with findings #1 and #2.

**Remediation:** resolving the god-class/monster-method findings removes the need for these suppressions.

---

## 10. `Public`/`Core` Method-Pair Duplication — **Low**

Nearly every mutating public API is a thin forwarder to a private `…CoreAsync`/`…EntryAsync` twin via
[`RunAutoCommitAsync`](JetDatabaseWriter/AccessWriter.cs#L1693) — e.g. `InsertRowAsync` →
`InsertRowEntryAsync`, `UpdateRowsAsync` → `UpdateRowsCoreAsync`, `DropTableAsync` → `DropTableEntryAsync`.
The pattern is *intentional* (it centralizes auto-commit/transaction wrapping) and is therefore not a
defect, but it roughly **doubles** the method count of an already oversized class and repeats the same
`Guard.*` + `ThrowIfDisposedOrCancelled` preamble in every core method.

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
2. Finish delegating `AccessWriter`/`AccessReader` into their existing collaborators and split into
   `partial` files (#1), which also clears #9.
3. Hoist the repeated guard/disposal preamble into `RunAutoCommitAsync` (#10).
4. Build the Tests project in Release / with analyzers on in CI so its bar matches production (#11).
