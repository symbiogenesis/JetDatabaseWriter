# Read-path performance plan, v2

Successor to [typed-row-read-perf-plan.md](typed-row-read-perf-plan.md).
That plan delivered the per-row decode wins it set out to deliver
(Phase 5 alone dropped `Rows<T>()` allocations by ~6 KB/row), but the
gains were largely invisible in the Phase 7 numbers because every
benchmark op pays the full `OpenAsync` cost (~50 ms / ~3.3 MB) and the
test table only has 130 rows. This plan attacks both the measurement
problem **and** the next tier of decode/open-time wins.

Workload assumption (per user): **all of the above** — streaming
`Rows<T>()` over large tables, `ReadDataTableAsync` into memory, **and**
many short-lived `OpenAsync` + small read. This plan therefore covers
both the steady-state decode hot path *and* the open-time floor.

## Guiding principles

1. **Measure first.** No optimization lands without a benchmark that
   isolates the change from the `OpenAsync` floor.
2. **Don't regress correctness.** The two pre-existing DAO Compact
   failures documented in `/memories/repo/round-trip-tests.md` are the
   only acceptable failing tests. Every phase ends with `dotnet test`.
3. **Don't widen the public API surface.** Pooling, projection, and
   span plumbing are internal. Public callers see the same
   `IAsyncEnumerable<object?[]>` / `IAsyncEnumerable<T>` they see today.
4. **Stop when the data says stop.** If a phase's benchmark delta is
   <5 % and allocations don't move, document it and skip the next
   sub-phase rather than chasing diminishing returns.

---

## Phase 1 — Fix the benchmarks (mandatory, blocks everything else)

The current
[AccessReaderBenchmarks.cs](../../JetDatabaseWriter.Benchmarks/AccessReaderBenchmarks.cs)
opens a fresh reader inside every `[Benchmark]` method. With the test
DBs in this repo (largest table ≈130 rows) this means decode work is
≈2 % of every measurement and per-row deltas are buried under
margin-of-error. Without fixing this, **none of the later phases can
be evaluated**.

### Tasks

- [x] Added [`AccessReaderRowDecodeBenchmarks`](../../JetDatabaseWriter.Benchmarks/AccessReaderRowDecodeBenchmarks.cs)
  that pre-opens three readers in `[GlobalSetup]` (one per synthetic DB)
  and disposes them in `[GlobalCleanup]`. Each table shape × code-path
  cell is its own `[Benchmark]` method.
- [x] Added [`SyntheticDatabases`](../../JetDatabaseWriter.Benchmarks/SyntheticDatabases.cs)
  which generates three `.accdb` files into `%TEMP%\JetBench\` on first
  run (25 k numeric, 25 k text-heavy, 10 k wide-40-col). Files are
  cached by path — delete them to force a rebuild. Built via this
  repo's own `AccessWriter`.
- [x] Kept the existing `AccessReaderBenchmarks` class untouched so v1
  Phase 0/7 numbers remain comparable.
- [x] Added [`AccessReaderOpenBenchmarks`](../../JetDatabaseWriter.Benchmarks/AccessReaderOpenBenchmarks.cs)
  measuring `OpenAsync` + `Dispose` in isolation across NW, synthetic
  numeric, synthetic wide.
- [x] Captured baselines below.

### Baseline (2026-05-02, .NET 10.0.7, Intel Core Ultra 7 268V, Release)

Run: `dotnet run --project JetDatabaseWriter.Benchmarks -c Release -- --filter "*AccessReaderRowDecodeBenchmarks*" --warmupCount 2 --iterationCount 5 --invocationCount 1 --unrollFactor 1`

Synthetic DBs (built once into `%TEMP%\JetBench\` from
[SyntheticDatabases.cs](../../JetDatabaseWriter.Benchmarks/SyntheticDatabases.cs)):
- `Numeric` — 25 000 rows, 9 columns (5×int, short, currency, single, 2×datetime).
- `TextHeavy` — 25 000 rows, 6 columns (int + 5×short text).
- `Wide` — 10 000 rows, 40 columns (1×int + 20×int + 19×short text).
  `WideRowNarrowProjection` DTO binds only `Id`, `N0`, `N1`, `S0`.

Per-op numbers in the table; per-row figures in the bullets are derived
from these (op ÷ row count).

| Method                                | Mean / op | Allocated / op |
| ------------------------------------- | --------: | -------------: |
| `Decode_Numeric_Untyped` (25 k rows)  |  35.94 ms |       8.00 MB  |
| `Decode_Numeric_Typed`                |  35.03 ms |      10.90 MB  |
| `Decode_Numeric_AsStrings`            |  57.84 ms |      11.81 MB  |
| `Decode_Numeric_DataTable`            |  44.37 ms |      15.22 MB  |
| `Decode_Text_Untyped` (25 k rows)     |  67.49 ms |      20.72 MB  |
| `Decode_Text_Typed`                   |  62.99 ms |      22.27 MB  |
| `Decode_Text_AsStrings`               |  78.71 ms |      23.61 MB  |
| `Decode_Text_DataTable`               |  73.98 ms |      27.26 MB  |
| `Decode_Wide_Untyped` (10 k rows)     |  56.08 ms |      31.71 MB  |
| `Decode_Wide_Typed_NarrowProjection`  |  63.85 ms |      32.19 MB  |

Open benchmarks (`AccessReaderOpenBenchmarks`):

| Method                   | Mean / op | Allocated / op |
| ------------------------ | --------: | -------------: |
| `Open_Northwind`         |  1.74 ms  |       26.6 KB  |
| `Open_Synthetic_Numeric` |  1.32 ms  |       27.3 KB  |
| `Open_Synthetic_Wide`    |  1.10 ms  |       27.5 KB  |

#### Per-row figures (key derived numbers)

- `Decode_Numeric_Untyped`: **1.44 µs/row, 320 B/row**.
- `Decode_Numeric_Typed`:   **1.40 µs/row, 436 B/row**.
- `Decode_Numeric_AsStrings`: **2.31 µs/row, 472 B/row**.
- `Decode_Text_Untyped`: **2.70 µs/row, 829 B/row**.
- `Decode_Text_Typed`:   **2.52 µs/row, 891 B/row**.
- `Decode_Wide_Untyped`: **5.61 µs/row, 3.17 KB/row** (40 cols).
- `Decode_Wide_Typed_NarrowProjection`: **6.39 µs/row, 3.22 KB/row** —
  same alloc as untyped because every column is still decoded and
  boxed; this is exactly the Phase 2 target.

#### Headline findings (and plan re-prioritization)

1. **`OpenAsync` is not a real cost.** Measured in isolation it is
   ~1–1.7 ms / ~27 KB — three orders of magnitude smaller than the
   v1 plan's "~50 ms / ~3.3 MB" figure (which conflated `[GlobalSetup]`
   work, file I/O warm-up, and JIT into the per-op number).
   **Phase 5 (OpenAsync floor) is dropped from this plan** — it cannot
   move workloads any meaningful amount.
2. **Typed is currently slower-allocating than Untyped on Numeric**
   (436 vs 320 B/row, +36 %). The `RowMapper<T>` delegate from v1
   Phase 5 didn't eliminate per-row boxing — it just avoided
   `Convert.ChangeType`. Phase 3 (direct page→T decoder) is the only
   thing that closes this gap.
3. **Wide-table narrow projection is dead weight.** Same
   3.2 KB / row whether the DTO binds 4 columns or all 40. Phase 2
   (cracker column projection) directly attacks this.
4. **Text decode is the slowest path per row** (~2.5 µs/row, ~870 B/row
   typed). Each `string` is a fresh heap allocation; little we can do
   without an interning cache (out of scope) or `ReadOnlyMemory<char>`
   slices (would change the public contract).
5. **`AsStrings` and `DataTable` paths are still the worst** on
   allocation. They aren't the typed-row hot path the v1 plan
   prioritized, but Phase 7 (`ArrayPool` row buffers) would directly
   help `DataTable`.

### Exit criteria — done

- [x] New benchmark classes added and run.
- [x] Baselines recorded above.
- [x] `dotnet test` clean (modulo the two known DAO Compact failures
  documented in `/memories/repo/round-trip-tests.md`) — verified after
  Phase 1 changes (benchmark-only edits, no production code touched).

---

## Phase 2 — Cracker-level column projection for `Rows<T>()`

**Hypothesis:** `Rows<T>()` decodes and boxes every column on the page,
even when the bound `T` only consumes a subset. For wide tables and
narrow DTOs (the common LINQ-projection shape) this is pure waste.

### Tasks

- [ ] Add `RowMapper<T>.GetBoundColumnIndices(string[] headers)` that
  returns the sorted set of header indices the compiled mapper actually
  reads.
- [ ] Add an internal overload
  `TryCrackRowSync(byte[] page, int rowStart, int rowSize, TableDef td,
   ReadOnlySpan<int> wantedColumns, out object?[]? row, out bool needsLongValue)`
  that:
  - Always parses the row layout (cheap and required for var-area
    offsets even on unread columns — verify via test).
  - Calls `ResolveColumnSlice` only for columns in `wantedColumns`.
  - Fills unread slots with `null` (mapper already skips `null` /
    `DBNull` per Phase 5).
- [ ] Wire it into the `Rows<T>()` path in
  [AccessReader.cs](../../JetDatabaseWriter/Core/AccessReader.cs) via a
  new `EnumerateTypedRowsAsync(..., ReadOnlySpan<int> wantedColumns)`
  overload. `Rows()` keeps calling the existing all-columns overload.
- [ ] Skip `ResolveComplexColumns` / `WrapHyperlinkColumns` for columns
  not in `wantedColumns`.

### Risk

- Var-area offsets are cumulative — must verify that skipping decode
  doesn't break offset tracking for *later* var columns the mapper
  *does* want. `ResolveColumnSlice` is independent of decode, so this
  should be fine, but pin it with a test.

### Exit criteria

- New benchmark `Decode_Wide_Typed` (40 cols, mapper binds 4) shows
  measurable speedup and allocation drop.
- All existing tests still pass.

---

## Phase 3 — Compile a direct page → `T` decoder for `Rows<T>()`

**Hypothesis:** Even after Phase 2, `Rows<T>()` still allocates a
per-row `object?[]`, boxes every primitive, then unboxes inside the
mapper. A compiled `delegate void DecodeAndAssign(byte[] page,
int rowStart, in RowLayout layout, T target)` reads bytes from the
page and writes properties directly — no row buffer, no boxing.

### Tasks

- [ ] In [RowMapper.cs](../../JetDatabaseWriter/Internal/RowMapper.cs)
  add `BuildPageDecoder<T>(ColumnInfo[] columns, string[] headers, Type[] sourceTypes)`
  that emits an expression tree per bound column:
  ```
  ColumnSlice slice = ResolveColumnSlice(page, rowStart, rowSize, layout, columns[i]);
  switch (slice.Kind) {
      case Fixed: target.Foo = ReadInt32LE(page, rowStart + slice.DataStart); break;
      case Var:   target.Bar = DecodeText(page, rowStart + slice.DataStart, slice.DataLen); break;
      ...
  }
  ```
  using the type-specific primitive readers from
  [JetTypeInfo.cs](../../JetDatabaseWriter/Internal/JetTypeInfo.cs) and
  emitting only the branch needed for the column's known type — no
  runtime `switch` on `col.Type`.
- [ ] Add `Rows<T>()` fast-path: when no column needs LVAL chain
  resolution, skip `EnumerateTypedRowsAsync` entirely and call the
  compiled decoder directly inside the page loop.
- [ ] Keep the Phase 2 `object?[]` path as a fallback for:
  - Tables with `T_MEMO`/`T_OLE` columns the mapper binds (LVAL chain
    is async).
  - Tables with complex / hyperlink columns the mapper binds (post-
    processing wants the row array).
- [ ] Pin parity with the existing path via a property test that maps a
  random NW table both ways and compares.

### Risk

- Largest design change in this plan. Defer if Phase 2 already moved
  the needle enough for your workload.
- Expression trees + `Span<byte>` historically don't mix; may need to
  emit IL via `DynamicMethod` or pass `byte[] + int offset` instead of
  `Span<byte>`. Decide after Phase 4 (the span plumbing phase).

### Exit criteria

- `Decode_Numeric_Typed` ≥2× faster than Phase 2.
- Per-row allocation for `Rows<T>()` ≤ size of `T` itself.

---

## Phase 4 — `ReadOnlySpan<byte>` plumbing through the cracker

**Hypothesis:** `ReadFixedTyped`, `ReadVarTypedSync`, and
`ResolveColumnSlice` all take `(byte[] page, int offset, int len)`.
Switching to `ReadOnlySpan<byte>` removes redundant bounds checks,
unlocks the non-allocating `Encoding.UTF8.GetString(ReadOnlySpan<byte>)`
overload (already used in some places, not all), and makes Phase 3's
direct-decode emission cleaner.

### Tasks

- [ ] Add span-based overloads on
  [JetTypeInfo.cs](../../JetDatabaseWriter/Internal/JetTypeInfo.cs)
  alongside the existing `(byte[], int, ...)` ones; keep the legacy
  signatures delegating to the span versions so external callers (the
  `RowsAsStrings` path, diagnostics) don't break.
- [ ] Convert `TryCrackRowSync` and `ReadVarTypedSync` in
  [AccessReader.cs](../../JetDatabaseWriter/Core/AccessReader.cs) to
  take and pass `ReadOnlySpan<byte>` (sync paths only — `async` methods
  still need `byte[]` because spans can't cross `await`).
- [ ] Convert `ResolveColumnSlice` and `TryParseRowLayout` to span.
- [ ] Confirm `Encoding.GetString(ReadOnlySpan<byte>)` is used
  everywhere instead of `(byte[], int, int)`.

### Exit criteria

- No measurable regression on `Decode_*_Untyped`.
- Code is structurally ready for Phase 3.

---

## Phase 5 — ~~Attack the `OpenAsync` allocation floor~~ DROPPED

**Status: dropped after Phase 1 baselines.** Measured in isolation
`OpenAsync` is **1.1–1.7 ms / ~27 KB** across all tested DBs (see
`AccessReaderOpenBenchmarks` table above), not the 50 ms / 3.3 MB the
v1 plan estimated. The earlier figure conflated `[GlobalSetup]` work,
file warm-up, and JIT into the per-op number. There is no meaningful
win available here.

Original hypothesis preserved below for historical context.

<details>
<summary>Original Phase 5 plan (do not implement)</summary>

**Hypothesis:** ~3.3 MB / ~50 ms on every `OpenAsync` is dominated by
catalog and system-table parsing that allocates `List<>`-backed
collections eagerly, copies pages, and parses tables that most
short-lived callers never touch (linked-table info, statistics, etc.).
This phase is what makes the "many short-lived `OpenAsync` + small
read" workload tolerable.

### Tasks

- [ ] **Profile first** — `dotnet-trace` or `dotMemory` against
  `Open_Small` benchmark. Identify the top 3 allocation sites. Record
  in this doc.
- [ ] Make `LinkedTableManager`-related state lazy (only built on first
  linked-table lookup).
- [ ] Make complex-column metadata lazy per table (today
  `BuildComplexColumnDataAsync` runs on first `Rows()` per table — keep
  that; verify nothing else eagerly walks the complex catalog at open).
- [ ] Replace `byte[]` page copies in catalog parsing with span reads
  off the cached page where possible.
- [ ] Pre-size `List<>` capacities where the count is knowable from a
  header.
- [ ] Consider a `OpenAsync(path, AccessReaderOptions { LazyCatalog =
  true })` mode that defers everything not needed to answer
  `ListTablesAsync` until first table read. Default off for back-compat.

### Exit criteria

- `Open_Small` allocation drops by ≥30 % or the phase is documented as
  "not worth it" with profiling data.

</details>

---

## Phase 6 — Cache parsed row directory in the LRU page cache

**Hypothesis:** `EnumerateLiveRowBounds(page)` re-parses the page
trailer on every call. When the same page is read multiple times
(`ReadDataTableAsync` then `Rows<T>()`, or two passes of `Rows()`),
that work is repeated.

### Tasks

- [ ] Change the LRU cache value from `byte[] page` to a small struct
  `CachedPage(byte[] Bytes, RowBound[]? LiveRows)` where `LiveRows` is
  populated on first `EnumerateLiveRowBounds` call and reused on
  subsequent ones. Use `ArrayPool<RowBound>` if cache eviction is hot.
- [ ] Skip parsing for non-data pages.

### Risk

- Increases per-cached-page memory by ~16 bytes + (rows × 8 bytes).
  Mitigation: only populate for data pages, cap rows-per-page (~250).

### Exit criteria

- `Decode_*` benchmarks that re-scan the same table show measurable
  improvement.

---

## Phase 7 — `ArrayPool<object?>` row buffers for non-yielding consumers

**Hypothesis:** Phase 4 of v1 punted on pooling because `Rows()` yields
the buffer to user code. But two key consumers immediately discard it:
- `ReadDataTableAsync` does `dt.Rows.Add(row)` (which copies values
  into a `DataRow`) and drops the array.
- `Rows<T>()` invokes the mapper and drops the array.

### Tasks

- [ ] Add an internal `EnumerateTypedRowsPooledAsync` that returns
  `(object?[] buffer, int validLength)` from `ArrayPool<object?>.Shared`.
- [ ] Route `ReadDataTableAsync` and `Rows<T>()` (the v1 path; remove
  if Phase 3 lands and supersedes it) through the pooled enumerator.
- [ ] **Do not** change public `Rows()`.

### Risk

- Easy to leak pool buffers on exception. Use `try`/`finally` with
  `ArrayPool<object?>.Shared.Return(buffer, clearArray: true)`.

### Exit criteria

- `ReadDataTableAsync` row-buffer allocations drop to zero.

---

## Phase 8 — Verify, document, decide

- [ ] Re-run all Phase 1 benchmarks; record final numbers.
- [ ] Run full `dotnet test`.
- [ ] Update [README.md](../../README.md) perf bullet with concrete
  numbers if any phase delivered ≥2×.
- [ ] If Phase 3 (direct decoder) materially regressed maintainability,
  consider keeping it behind an opt-in `AccessReaderOptions` flag.

---

## Out of scope (intentional)

- **Page I/O / decryption.** Already cached. Not the bottleneck for
  steady-state reads. Could revisit if Phase 5 profiling implicates it.
- **Index/seek paths.** Separate work item; this plan is about full-
  table scans and `OpenAsync`.
- **Writer perf.** Out of scope; mention only if a span/pool change in
  the cracker accidentally also benefits the writer.
- **Long-value (LVAL) decode optimization.** The async chain walker is
  fine for the rare path it handles; not the hot path.

---

## Phase ordering rationale (revised after Phase 1 baselines)

```
Phase 1 (benchmarks)                ✓ done
   ├─ Phase 2 (column projection)   ← biggest win for Wide_NarrowProjection
   │    └─ Phase 3 (direct decoder) ← only path that beats Untyped on Numeric
   │         └─ Phase 4 (span plumbing) ← prerequisite cleanup for 3
   ├─ Phase 5 (OpenAsync floor)     ✗ DROPPED — see Phase 1 finding #1
   ├─ Phase 6 (cached row directory) ← low priority; helps repeated scans only
   └─ Phase 7 (pooled row buffers)  ← high priority for DataTable path
Phase 8 (verify + document)
```

Recommended order based on Phase 1 data:
1. **Phase 2** first — fastest to implement, directly attacks the
   3.2 KB / row Wide-narrow-projection waste.
2. **Phase 7** in parallel with Phase 2 (independent code paths) —
   biggest alloc win for `DataTable` consumers.
3. **Phase 4** then **Phase 3** — Phase 3 is the only optimization
   that flips the Typed-vs-Untyped allocation gap; defer until Phase 2
   ships so its numbers form a clean baseline for Phase 3 to beat.
4. **Phase 6** last (or skip) — its win only materializes for
   workloads that read the same pages twice, which is rare in
   streaming scans.
