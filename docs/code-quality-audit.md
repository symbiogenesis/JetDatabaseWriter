# Code-Quality Audit — Most Serious Deficiencies, Anti-Patterns, and Code Smells

**Scope:** the `JetDatabaseWriter` library project (production code only — tests, benchmarks,
and FormatProbe excluded except where noted).
**Date:** 2026-06-16.
**Method:** static review of file/method size metrics, exception-handling patterns, public API
surface, concurrency primitives, and analyzer-suppression density.

> **Context first.** This is a disciplined, well-tested codebase: strict build settings
> (`WarningLevel 9999`, `AnalysisLevel latest-all`, warnings-as-errors, StyleCop + Roslynator +
> BannedApi), checked arithmetic, reproducible builds, ~1,492 tests across ~48k test LOC, and
> almost no `TODO`/`HACK`/dead-code debt. Analyzer suppressions are sparse and individually
> justified. The issues below are therefore mostly **structural and design-level** rather than
> sloppiness — but they are the ones most likely to slow future change, hide defects, and raise
> the cost of onboarding.

---

## Severity Summary

| # | Finding | Category | Severity |
|---|---------|----------|----------|
| 1 | God classes / monolithic types | Structure / cohesion | **High** |
| 2 | Monster methods (200–450 lines) | Complexity | **High** |
| 3 | Weak, primitive-obsessed public API | API design | **Resolved** |
| 4 | Reader doubles as MIME sniffer + base64 data-URI generator | SRP / performance | **Resolved** |
| 5 | Silent exception swallowing / exceptions-as-control-flow | Error handling | **Resolved** |
| 6 | Sync-over-async busy-poll lock + duplicated sync/async loops | Concurrency | **Resolved** |
| 7 | Sprawling, overlapping concurrency model | Concurrency | **Resolved** |
| 8 | Large parameter lists & boolean-flag parameters | API ergonomics | **Low-Medium** |
| 9 | Member-ordering suppressions hiding organic accretion | Maintainability | **Low** |
| 10 | `Public`/`Core` method-pair duplication | Boilerplate | **Low** |
| 11 | Tests held to a lower analyzer bar than production | Consistency | **Low** |

---

## 1. God Classes / Monolithic Types — **High**

A handful of types have accreted far too many responsibilities. None are split with `partial`,
so each is a single, monolithic, hard-to-navigate file.

| Type | Lines | Role |
|------|------:|------|
| [JetDatabaseWriter/AccessWriter.cs](JetDatabaseWriter/AccessWriter.cs) | 3,442 | public writer facade |
| [JetDatabaseWriter/AccessReader.cs](JetDatabaseWriter/AccessReader.cs) | 2,925 | public reader facade |
| [JetDatabaseWriter/Indexes/IndexBTreeEditor.cs](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) | 1,802 | B-tree mutation |
| [JetDatabaseWriter/AccessBase.cs](JetDatabaseWriter/AccessBase.cs) | 1,620 | shared base |
| [JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs) | 1,501 | attachments/multivalue |
| [JetDatabaseWriter/Relationships/RelationshipManager.cs](JetDatabaseWriter/Relationships/RelationshipManager.cs) | 1,410 | FK lifecycle |
| [JetDatabaseWriter/Indexes/IndexMaintainer.cs](JetDatabaseWriter/Indexes/IndexMaintainer.cs) | 1,307 | index orchestration |

`AccessWriter` is the clearest offender. It is declared at
[AccessWriter.cs](JetDatabaseWriter/AccessWriter.cs#L44) as a single `sealed` class that:

- implements two public interfaces (`IAccessWriter`, `IAccessSchema`) plus the `AccessBase` contract;
- owns **~15 collaborator fields** (lock coordinator, index maintainer, TDEF builder, long-value
  encoder, unique-index checker, transaction lifecycle, catalog writer, row encoder, data-page
  inserter, page allocator, relationship manager, complex-column manager, constraint registry, …);
- and still hosts table DDL, row DML, linked-table creation, schema evolution, catalog
  bootstrap, AutoNumber bookkeeping, and disposal logic directly in its own body.

It is already a *facade over* well-factored managers — but it never finished delegating, so it is
simultaneously the facade **and** a large implementation. The composition is good; the residue left
in the facade is the smell.

**Why it matters:** these files exceed what a reviewer can hold in working memory, force wide-ranging
merge conflicts, and make it impossible to unit-test slices in isolation.

**Remediation:** push the remaining inline logic into the collaborators it already owns (e.g. move
AutoNumber high-water maintenance into a small `AutoNumberMaintainer`, linked-table creation entirely
into `LinkedTableManager`), and split the facades into `partial` files grouped by concern (DDL / DML /
catalog / disposal) as an interim step.

---

## 2. Monster Methods (200–450 lines) — **High**

[IndexBTreeEditor.cs](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) contains several methods that are
each longer than many entire classes:

- [`TrySurgicalCrossLeafMaintainAsync`](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L911) — **~438 lines** (911–1349).
- [`TryStageIntermediateRewritesAsync`](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L1528) — **~412 lines** (1528–1940).
- [`TrySurgicalMultiLevelMaintainAsync`](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L398) — **~218 lines** (398–615).

In [AccessWriter.cs](JetDatabaseWriter/AccessWriter.cs):

- [`UpdateRowsCoreAsync`](JetDatabaseWriter/AccessWriter.cs#L1102) — ~164 lines (1102–1265).
- [`CreateCatalogTableArtifactAsync`](JetDatabaseWriter/AccessWriter.cs#L633) — ~140 lines (633–772).
- [`DeleteRowsCoreAsync`](JetDatabaseWriter/AccessWriter.cs#L1266) — ~114 lines (1266–1380).

These methods interleave several distinct phases (descent, validation, splice, page rewrite,
parent/ancestor patching) with deep nesting and many local mutable variables. They are the riskiest
code in the repository to modify: high cyclomatic complexity, many early-return branches, and no
seams for targeted testing.

**Remediation:** extract each phase into a named, individually testable method (or a small
state-object with explicit steps). Even without changing behavior, decomposing a 438-line method into
6–8 named steps dramatically improves reviewability and lets the B-tree split/merge phases be unit
tested directly.

---

## 3. Weak, Primitive-Obsessed Public API — **Resolved**

> **Resolved.** A named-column row type
> ([RowValues](JetDatabaseWriter/Models/RowValues.cs)) and a multi-column
> predicate type ([RowCriteria](JetDatabaseWriter/Models/RowCriteria.cs) +
> [ColumnPredicate](JetDatabaseWriter/Models/ColumnPredicate.cs)) were added.
> `InsertRowAsync`/`InsertRowsAsync` now accept `RowValues` so column identity is
> by name rather than position (omitted columns default to null/AutoNumber);
> `UpdateRowsAsync`/`DeleteRowsAsync` accept a `RowCriteria` that expresses
> `WHERE a = 1 AND b > 2`, `IN` sets, ranges, and null checks. Predicate
> resolution/evaluation lives in
> [RowCriteriaEvaluator](JetDatabaseWriter/ValueDecoding/RowCriteriaEvaluator.cs),
> which compiles column names to indices once per call. The single-column
> `UpdateRowsAsync`/`DeleteRowsAsync` overloads remain as thin convenience wrappers
> over the same criteria core, and positional `object?[]` inserts remain as the
> low-level primitive. The original finding follows for context.

The row and query surface leans heavily on untyped primitives:

- Rows are `object?[]` positional arrays — e.g.
  [`InsertRowAsync(string tableName, object?[] values, …)`](JetDatabaseWriter/AccessWriter.cs#L1023).
  Column identity is *positional*, so a caller silently corrupts data if the array order drifts from
  the schema. There is no compile-time protection.
- Table and column names are bare `string`s everywhere (stringly-typed), with correctness deferred to
  runtime guard checks.
- Update/delete predicates are limited to **a single column equality**:
  [`UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, …)`](JetDatabaseWriter/AccessWriter.cs#L1099)
  and the matching delete overload. There is no way to express `WHERE a = 1 AND b > 2`, an `IN` set,
  or a range without reading rows yourself. This is a genuine functional limitation, not just a style
  concern.
- Updated values arrive as `IReadOnlyDictionary<string, object?>` — again stringly-typed and unchecked
  until execution.

A further subtlety: the public entry points accept `object?[]` but immediately normalize to
non-nullable `object[]` for the private core (e.g.
[`InsertRowEntryAsync(string tableName, object[] values, …)`](JetDatabaseWriter/AccessWriter.cs#L1026)),
so nullability is enforced by convention rather than by the type system.

**Remediation:** offer a typed/named-column row abstraction (the generic `InsertRowAsync<T>` overload
is a good model — lean into it) and a small predicate/criteria type so multi-column and range filters
are first-class instead of requiring manual read-filter-write loops.

**Resolution:** implemented as described above — `RowValues` for named-column inserts and
`RowCriteria`/`ColumnPredicate` for first-class multi-column, range, set, and null filters on
update/delete.

---

## 4. Reader Doubles as a MIME Sniffer and Base64 Data-URI Generator — **Resolved**

> **Resolved.** The OLE payload-detection, MIME-magic matching, raw-byte
> extraction, and `data:`-URI synthesis were extracted out of `AccessReader`
> into a dedicated [OleObjectDecoder](JetDatabaseWriter/ValueDecoding/OleObjectDecoder.cs).
> The typed/`Rows()` hot path projects OLE columns as raw `byte[]` via
> `OleObjectDecoder.DecodeOleValueBytes` and never base64-encodes; the
> `data:`-URI convenience (`OleObjectDecoder.TryDecodeOleObject`) is only invoked
> by the explicit string projection. The original finding follows for context.

[AccessReader.cs](JetDatabaseWriter/AccessReader.cs) is a low-level JET/ACE *database* reader, yet it
also embeds content-type detection for OLE blobs and synthesizes `data:` URIs:

- [`TryCreateOleDataUriFromKnownMagic`](JetDatabaseWriter/AccessReader.cs#L2248) returns
  `"data:" + mimeType + ";base64," + Convert.ToBase64String(buffer, payloadStart, payloadLength)`.
- [`TryMatchOlePayloadMagic`](JetDatabaseWriter/AccessReader.cs#L2342) hard-codes recognition of JPEG,
  PNG, GIF, BMP, TIFF, PDF, ZIP/OOXML, RTF, etc.

Two problems:

1. **Single-responsibility violation.** Image/document format recognition and web-oriented data-URI
   formatting are presentation concerns that do not belong in a storage-format reader. They expand the
   reader's surface and test burden for a feature most consumers don't want in this form.
2. **Performance/memory.** Forcing every recognized OLE payload through `Convert.ToBase64String`
   allocates a fresh string ~1.37× the blob size (potentially multi-megabyte) every time the value is
   projected — even when the caller only wants the raw bytes. Base64 inflation plus string allocation
   is a poor default for binary columns.

**Remediation:** expose raw payload bytes (and at most a detected MIME *string*) and move the
data-URI/format-detection convenience into a separate opt-in helper or extension, so the hot read path
never base64-encodes unless explicitly asked.

---

## 5. Silent Exception Swallowing & Exceptions-as-Control-Flow — **Resolved**

> **Resolved.** The three empty `catch { }` blocks in the AutoNumber high-water
> scan were replaced with a non-throwing `TryGetAutoNumberCandidate` type switch in
> [AccessWriter.cs](JetDatabaseWriter/AccessWriter.cs), so an unexpected boxed
> value is skipped deterministically instead of being silently swallowed from a
> throwing `Convert.ToInt64`. The fixed-column decoders
> [JetTypeInfo.ReadFixedString / ReadFixedTyped](JetDatabaseWriter/Schema/JetTypeInfo.cs)
> now validate the read up front with a shared `FixedReadInBounds` guard (Numeric
> still self-validates with strict-mode `JetLimitationException` semantics) and
> collapse their three stacked `catch` clauses into a single `when`-filtered catch
> that no longer catches `IndexOutOfRangeException` — an out-of-range index now
> surfaces as the slicing bug it indicates rather than collapsing to an empty
> value. [RowDecodePlan.cs](JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs)
> likewise collapses its four stacked catches into one `when` filter and stops
> swallowing `IndexOutOfRangeException`. The original finding follows for context.

Most catches in the codebase are disciplined (typed filters such as
`catch (Exception ex) when (ex is A or B or C)`). A few, however, swallow exceptions into empty bodies
or sentinel returns, which can mask real corruption:

- **Empty catches** in the AutoNumber high-water scan
  ([AccessWriter.cs](JetDatabaseWriter/AccessWriter.cs#L2108) — `FormatException`, then
  [L2111](JetDatabaseWriter/AccessWriter.cs#L2111) `InvalidCastException`, then
  [L2114](JetDatabaseWriter/AccessWriter.cs#L2114) `OverflowException`), all with `{ }` bodies. A bad
  value is silently ignored rather than surfaced or logged.
- **Decode paths that catch and return a sentinel**, e.g.
  [JetTypeInfo.cs](JetDatabaseWriter/Schema/JetTypeInfo.cs#L232) and
  [JetTypeInfo.cs](JetDatabaseWriter/Schema/JetTypeInfo.cs#L320) catch `ArgumentException` /
  `OverflowException` and return `string.Empty`; [RowDecodePlan.cs](JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs#L159)
  catches and returns `false`. Catching `IndexOutOfRangeException`
  ([JetTypeInfo.cs](JetDatabaseWriter/Schema/JetTypeInfo.cs#L236) region) to return an empty value is
  especially risky: an out-of-range index almost always indicates a real slicing/offset bug, and
  turning it into "" hides the defect behind plausible-looking empty output.

There is also a **stylistic inconsistency**: the same intent is sometimes written as one `when`-filtered
catch and sometimes as three or four stacked single-type catches with identical bodies.

**Remediation:** collapse stacked catches into single `when` filters for readability; for the decode
paths, prefer non-throwing `Try*`/`BinaryPrimitives` reads with explicit bounds checks rather than
catching range/overflow exceptions; and avoid catching `IndexOutOfRangeException` as an expected
condition — treat it as a bug signal.

---

## 6. Sync-over-Async Busy-Poll Lock + Duplicated Sync/Async Loops — **Resolved**

> **Resolved.** The duplicated sync poll loop was removed. Acquisition now runs a
> single async primitive
> ([`AcquireBlockingAsync`](JetDatabaseWriter/Transactions/JetByteRangeLock.cs)),
> and the (test-only) synchronous entry point
> ([`AcquirePageLock`](JetDatabaseWriter/Transactions/JetByteRangeLock.cs)) bridges
> onto it exactly once at the call boundary
> (`AcquireBlockingAsync(...).AsTask().GetAwaiter().GetResult()`) instead of
> running its own per-iteration `Task.Delay(...).GetAwaiter().GetResult()`. The
> fixed 20 ms interval was replaced with a deadline-clamped exponential backoff
> (2 ms doubling to a 64 ms cap), encapsulated in a
> [`PollBackoff`](JetDatabaseWriter/Transactions/JetByteRangeLock.cs) value type so
> the schedule and timeout accounting live in one place. Because this repo bans
> `Thread.Sleep`/`Task.Wait`/`Task.Result` (see
> [BannedSymbols.txt](BannedSymbols.txt)), a single boundary bridge is the
> sanctioned synchronous escape hatch rather than an OS wait handle. The original
> finding follows for context.

[JetByteRangeLock.cs](JetDatabaseWriter/Transactions/JetByteRangeLock.cs#L190) acquires a contended lock
by **blocking on an async delay**:

```csharp
Task.Delay(PollIntervalMilliseconds).ConfigureAwait(false).GetAwaiter().GetResult();
```

inside [`AcquireBlocking`](JetDatabaseWriter/Transactions/JetByteRangeLock.cs#L180). This is the classic
sync-over-async pattern (thread-pool starvation risk under load) wrapped around a **busy-poll**
(retry every N ms until a timeout) rather than an OS-level blocking wait. Worse, the logic is duplicated
almost verbatim in the async sibling
[`AcquireBlockingAsync`](JetDatabaseWriter/Transactions/JetByteRangeLock.cs#L201) — the same poll loop
with `await Task.Delay(...)` instead of the blocking form, so any fix must be made in two places.

**Remediation:** keep a single async acquisition primitive and, where a synchronous entry point is truly
required, bridge it once at the boundary. Replace the fixed-interval poll with exponential backoff (or a
proper wait handle) to reduce wasted wakeups and latency.

---

## 7. Sprawling, Overlapping Concurrency Model — **Resolved**

> **Resolved.** The intended lock hierarchy and acquisition order are now
> documented in one place:
> [docs/design/concurrency-and-lock-ordering.md](design/concurrency-and-lock-ordering.md).
> That note enumerates the coordination primitives, gives the single
> outermost→innermost acquisition order, traces the write / commit / read /
> dispose call paths, lists the reentrancy rules, and records a deliberate
> decision **not** to merge the in-process gates. Verifying the code against the
> audit's "conceptual overlap" showed the overlap does not hold: `IoGate`
> serializes backing-stream I/O, `operationGate` is a reader-disposal drain that
> intentionally allows concurrent reentrant reads, and the writer's `stateLock`
> guards only a two-field insert-page hint cache — not transaction state.
> Consolidating them would conflate three distinct responsibilities and add
> contention; the only genuine (optional) simplification — downgrading the
> insert-page-cache `ReaderWriterLockSlim` to a plain lock — is noted in the doc,
> not done. The original finding follows for context.

Mutual exclusion in the writer is spread across an unusually large set of mechanisms, each with its own
ordering rules:

- `ReaderWriterLockSlim stateLock` on [AccessWriter.cs](JetDatabaseWriter/AccessWriter.cs#L55);
- `SemaphoreSlim IoGate` on [AccessBase.cs](JetDatabaseWriter/AccessBase.cs#L120);
- a plain `lock` over `ownedDataPagesCacheLock` ([AccessBase.cs](JetDatabaseWriter/AccessBase.cs#L1224));
- a `Lock`/`object` pair selected by target framework ([AccessBase.cs](JetDatabaseWriter/AccessBase.cs#L111));
- [`AsyncReentrantOperationGate`](JetDatabaseWriter/Infrastructure/AsyncReentrantOperationGate.cs#L58) with its own internal `lock`;
- the cooperative [`JetByteRangeLock`](JetDatabaseWriter/Transactions/JetByteRangeLock.cs);
- and the [`LockFileCoordinator`](JetDatabaseWriter/Transactions/LockFileCoordinator.cs) (`.ldb`/`.laccdb`).

Each may be individually justified, but together they create a high-cognitive-load locking surface with
no single documented lock-ordering hierarchy. That is fertile ground for deadlocks and lock-ordering
regressions as the code evolves, and it makes the concurrency invariants hard to verify.

**Remediation:** document the intended lock hierarchy and acquisition order in one place
(`docs/design/`), and consider consolidating the in-process gates (the `ReaderWriterLockSlim`,
`SemaphoreSlim`, and the operation gate overlap conceptually) behind one async coordination primitive.

---

## 8. Large Parameter Lists & Boolean-Flag Parameters — **Low-Medium**

Several APIs have parameter lists long enough to invite call-site mistakes (positional `bool`/`long`
arguments are easy to transpose):

- [`IndexPageCodec.BuildLeafPage`](JetDatabaseWriter/Indexes/IndexPageCodec.cs#L48) — **9 parameters**,
  including three adjacent `long` page pointers (`prevPage`, `nextPage`, `tailPage`) and a trailing
  `bool enablePrefixCompression`.
- [`IndexPageCodec.BuildIntermediatePage`](JetDatabaseWriter/Indexes/IndexPageCodec.cs#L170) — **8 parameters**.
- [`InsertPreparedBatchAsync`](JetDatabaseWriter/AccessWriter.cs#L2006) — 6 parameters.
- The [`MarkRowDeletedAsync`](JetDatabaseWriter/AccessWriter.cs#L3771) overload family threads a
  `bool clearRowData` flag through multiple overloads — a boolean-flag-parameter smell where two named
  methods (or an enum) would read better at the call site.

**Remediation:** group the cohesive page-pointer/compression parameters into a small options/record
struct, and replace boolean flags with named methods or an explicit enum.

---

## 9. Member-Ordering Suppressions Hiding Organic Accretion — **Low**

The largest types suppress StyleCop ordering rules to tolerate mixed member layout:

- [AccessWriter.cs](JetDatabaseWriter/AccessWriter.cs#L38) — `#pragma warning disable SA1202` /
  [L39](JetDatabaseWriter/AccessWriter.cs#L39) `SA1204`, with the comment *"Keep member order stable
  while synchronous APIs remain private compatibility helpers."*
- Same suppressions in
  [ComplexColumnManager.cs](JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs#L23),
  [IndexBTreeEditor.cs](JetDatabaseWriter/Indexes/IndexBTreeEditor.cs#L13), and
  [IndexMaintainer.cs](JetDatabaseWriter/Indexes/IndexMaintainer.cs#L18).

These appear only in the god classes and are a tell-tale of accretion: the files grew until enforcing
member ordering became inconvenient, so the rule was switched off. They are harmless in isolation but
correlate exactly with findings #1 and #2.

**Remediation:** resolving the god-class/monster-method findings removes the need for these suppressions.

---

## 10. `Public`/`Core` Method-Pair Duplication — **Low**

Nearly every mutating public API is a thin forwarder to a private `…CoreAsync`/`…EntryAsync` twin via
[`RunAutoCommitAsync`](JetDatabaseWriter/AccessWriter.cs#L1773) — e.g. `InsertRowAsync` →
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

## What Is Already Done Well (for balance)

- Clear domain decomposition into `Catalog/`, `Schema/`, `Pages/`, `Indexes/`, `Encryption/`, etc., with
  per-module model folders.
- Excellent format documentation and glossary; constants are centralized and well-named in
  [Constants.cs](JetDatabaseWriter/Constants.cs) rather than scattered as magic numbers in logic.
- Disciplined, type-filtered exception handling in the majority of call sites.
- Strong multi-targeting hygiene (`netstandard2.1` shims gated behind `#if`).
- Very large, categorized test suite and an evidence-based design-doc/oracle culture.

## Recommended Order of Attack

1. Decompose the two ~400-line `IndexBTreeEditor` methods (#2) — highest defect risk per line.
2. Finish delegating `AccessWriter`/`AccessReader` into their existing collaborators and split into
   `partial` files (#1), which also clears #9.
3. ~~Add a typed row + multi-column predicate API (#3) — biggest external/usability win.~~ **Done** — `RowValues` + `RowCriteria`/`ColumnPredicate`.
4. ~~Move OLE MIME/data-URI synthesis out of the reader hot path (#4).~~ **Done** — extracted into `OleObjectDecoder`.
5. ~~Tidy the swallow-to-sentinel decode paths (#5)~~ **Done** — non-throwing AutoNumber/decode guards + consolidated `when` filters. ~~Tidy the sync-over-async lock (#6)~~ **Done** — single async primitive + one boundary bridge + exponential backoff.
6. ~~Document the lock-ordering hierarchy (#7)~~ **Done** — [docs/design/concurrency-and-lock-ordering.md](design/concurrency-and-lock-ordering.md); in-process gates kept separate by design.
