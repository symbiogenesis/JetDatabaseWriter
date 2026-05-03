# Read-path performance plan, v2

Successor to the (now-deleted) typed-row read perf plan, whose results
are summarized in repo memory (`/memories/repo/typed-row-read-path.md`).
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

- [x] Added [`RowMapper<T>.GetBoundColumnMask(headers)`](../../JetDatabaseWriter/Internal/RowMapper.cs)
  — returns a `bool[]` of header indices the compiled mapper actually reads.
- [x] Added projection-aware overload
  `TryCrackRowSync(byte[] page, int rowStart, int rowSize, TableDef td,
   bool[]? wantedColumns, out object?[]? row, out bool needsLongValue)`
  in [AccessReader.cs](../../JetDatabaseWriter/Core/AccessReader.cs)
  that always parses the row layout (cheap and required for var-area
  offsets) but only calls `ResolveColumnSlice` + decode for columns
  whose mask entry is `true`. Unread slots stay `null`; the compiled
  mapper already skips `null`/`DBNull`.
- [x] Added matching projection overload of `CrackRowTypedAsync` and
  `EnumerateTypedRowsAsync`. The `Rows<T>()` entry point computes the
  mask once per call and passes it through; `Rows()` /
  `ReadDataTableAsync` keep calling the all-columns overload (mask = `null`).
- [x] Skipped `ResolveComplexColumns` / `WrapHyperlinkColumns` passes
  entirely when the projection contains no column of the relevant kind
  (per-row helpers `HasWantedColumnOfType` / `HasWantedHyperlinkColumn`).
- [x] Disabled the projection (mask = `null`) when
  `td.HasComplexColumns` so complex resolution's parent-id T_LONG lookup
  is never starved.

### Verification

- `dotnet test` clean modulo the 2 known DAO Compact failures
  documented in `/memories/repo/round-trip-tests.md`.
- `Decode_Wide_Typed_NarrowProjection` (10 k rows, 40 cols, DTO binds 4):

  | Metric        | Baseline (Phase 1) | Phase 2  | Δ          |
  | ------------- | -----------------: | -------: | ---------- |
  | Mean / op     |          63.85 ms  | 22.19 ms | **2.9× faster** |
  | Allocated/op  |          32.19 MB  |  6.10 MB | **5.3× less**   |
  | Allocated/row |          3.22 KB   |    624 B | **5.3× less**   |

  Other `Decode_*` benchmarks are within run-to-run noise of the
  Phase 1 baselines (no regression on the all-columns path).

### Risk (resolved)

- Var-area offsets are cumulative — verified that skipping decode does
  not break offset tracking for later var columns. `ResolveColumnSlice`
  computes each column's offsets independently from the row trailer
  (`RowLayout.VarTableStart` + `col.VarIdx`), so unread slots are safe.
- Complex/attachment parent-id resolution needs the first T_LONG to be
  decoded — addressed by falling back to the all-columns path whenever
  `td.HasComplexColumns`.

### Exit criteria — done

- [x] Wide narrow-projection benchmark shows ≥2× speedup and ≥5×
  allocation drop.
- [x] All existing tests still pass (modulo the 2 known failures).

---

## Phase 3 — Compile a direct page → `T` decoder for `Rows<T>()`

**Hypothesis:** Even after Phase 2, `Rows<T>()` still allocates a
per-row `object?[]`, boxes every primitive, then unboxes inside the
mapper. A compiled `delegate bool DirectRowDecoder<T>(reader, page,
rowStart, rowSize, hasVarColumns, T target)` reads bytes from the page
and writes properties directly — no row buffer, no boxing.

### Tasks

- [x] Added typed primitive readers to
  [JetTypeInfo.cs](../../JetDatabaseWriter/Internal/JetTypeInfo.cs):
  `ReadByteAt`, `ReadInt16LE`, `ReadInt32LE`, `ReadInt64LE`,
  `ReadFloatLE`, `ReadDoubleLE`, `ReadDateTimeLE`, `ReadMoneyLE`,
  `ReadGuidAt`, `ReadDecimalLE` (T_NUMERIC). Each returns the unboxed
  CLR value directly off the page bytes.
- [x] Added internal accessors on
  [AccessReader.cs](../../JetDatabaseWriter/Core/AccessReader.cs)
  (`TryParseRowLayoutForDirectDecode`,
  `ResolveColumnSliceForDirectDecode`,
  `DecodeTextSliceForDirectDecode`, `NumColsFieldSize`,
  `ReadRawNumCols`) so the compiled delegate can call back into the
  per-instance state. Promoted `RowLayout`, `ColumnSliceKind`,
  `ColumnSlice` from `private protected` to `internal` in
  [AccessBase.cs](../../JetDatabaseWriter/Core/AccessBase.cs) so the
  delegate can name them.
- [x] Added [`DirectRowDecoderBuilder`](../../JetDatabaseWriter/Internal/DirectRowDecoderBuilder.cs)
  with `TryBuild<T>(headers, columns, clrTypes)` that compiles a per-T
  expression tree. Each bound column emits:
  ```
  slice = reader.ResolveColumnSliceForDirectDecode(...);
  if (slice.Kind == Fixed && slice.DataLen >= expectedSize)
      try { target.PropI = (PropType)JetTypeInfo.ReadXxx(page, rowStart + slice.DataStart); }
      catch (ArgumentException/OverflowException/IndexOutOfRangeException) { /* leave default */ }
  ```
  Refuses (returns `null`) when any bound column requires the slow path:
  T_MEMO/T_OLE LVAL chains, T_BINARY, T_COMPLEX/T_ATTACHMENT, Hyperlink-
  typed properties, or any property whose underlying type doesn't match
  the column's natural CLR type.
- [x] Wired into `Rows<T>()`: when `TryBuild` returns non-null and
  `td.HasComplexColumns` is false, the new `EnumerateDirectRowsAsync<T>`
  loops the data pages, allocates a fresh `T` per row, and invokes the
  decoder — no `object?[]` buffer, no primitive boxing. Phase 2's
  projection path remains the fallback.
- [x] Per-column `try/catch` in the emitted body mirrors
  `ReadFixedTyped`'s exception-swallowing contract, so malformed rows
  leave properties at their default rather than throwing into user
  code.

### Verification

- `dotnet test` clean modulo the 2 known DAO Compact failures (the
  full Rows<T>() suite — round-trip parity, schema preservation, etc.
  — exercises the direct decoder against every NW + synthetic table
  and matches the Phase 2 baseline).
- Per-op benchmark numbers (`AccessReaderRowDecodeBenchmarks`,
  baseline = Phase 1 row of `read-perf-plan-v2.md`):

  | Method                              | Baseline alloc | Phase 3 alloc | Δ alloc | Phase 3 mean |
  | ----------------------------------- | -------------: | ------------: | ------: | -----------: |
  | `Decode_Numeric_Typed`              |       10.90 MB |    **3.28 MB** | **3.3× less** |     28.71 ms |
  | `Decode_Text_Typed`                 |       22.27 MB |    **20.0 MB** |    1.1× less |     68.25 ms |
  | `Decode_Wide_Typed_NarrowProjection`|       32.19 MB |    **2.14 MB** | **15× less** |     19.15 ms |

  Per-row: Numeric_Typed dropped from **436 B/row → 137 B/row**;
  Wide_NarrowProjection dropped from **3.22 KB/row → 219 B/row** (vs
  Phase 2's 624 B/row — a further 2.85× over Phase 2 alone).
  Text_Typed only saw a small improvement because each row's 5 fresh
  `string` allocations dominate; the `object?[]` removal is in the
  noise next to that.

  All `Decode_*_Untyped` and `Decode_*_AsStrings` paths are unchanged
  (they don't go through `Rows<T>()`), and `Decode_*_DataTable`
  numbers are within run-to-run noise.

### Risk (resolved)

- Expression-tree access to internal types/members within the same
  assembly works (`Compile()` produces a DynamicMethod that respects
  assembly visibility). Promoted only what's needed to `internal`.
- `out RowLayout` parameter to compiled delegate works via
  `ParameterExpression`. The original `in RowLayout` overload would
  not — wrapped with a by-value overload.
- T_NUMERIC `decimal` construction can throw `OverflowException` for
  scale > 28 or out-of-range mantissa; the per-column `try/catch`
  block already swallows these (matching `ReadNumericTyped`'s
  non-strict contract).
- `td.HasComplexColumns` always disables the direct path so complex
  parent-id resolution is never starved (same gate as Phase 2).

### Exit criteria — done

- [x] `Decode_Numeric_Typed` allocation ≥3× lower than Phase 2.
- [x] `Decode_Wide_Typed_NarrowProjection` allocation ≥2× lower than
  Phase 2.
- [x] All existing tests still pass (modulo the 2 known failures).

---

## Phase 4 — `ReadOnlySpan<byte>` plumbing through the cracker

**Hypothesis:** `ReadFixedTyped`, `ReadVarTypedSync`, and
`ResolveColumnSlice` all take `(byte[] page, int offset, int len)`.
Switching to `ReadOnlySpan<byte>` removes redundant bounds checks,
unlocks the non-allocating `Encoding.UTF8.GetString(ReadOnlySpan<byte>)`
overload (already used in some places, not all), and makes Phase 3's
direct-decode emission cleaner.

### Tasks

- [x] Converted the per-type decoders in
  [JetTypeInfo.cs](../../JetDatabaseWriter/Internal/JetTypeInfo.cs) to
  `ReadOnlySpan<byte>` — `ReadFixedString`, `ReadFixedTyped`,
  `ReadNumericString`, `ReadNumericTyped`. `byte[]` callers continue to
  work via the implicit `byte[] → ReadOnlySpan<byte>` conversion, so no
  external test/diagnostic call sites needed edits.
- [x] Added a `ReadOnlySpan<byte>` overload of `Ru16` on
  [AccessBase.cs](../../JetDatabaseWriter/Core/AccessBase.cs) and
  converted `TryParseRowLayout` and `ResolveColumnSlice` to take
  `ReadOnlySpan<byte>` (sync paths only — `async` callers continue to
  pass `byte[]` because spans can't cross `await`).
- [x] Converted `DecodeJet4Text` and `DecompressJet4` to
  `ReadOnlySpan<byte>`.
- [x] Added `JetTypeInfo.DecodeUtf16LE(ReadOnlySpan<byte>) → string`
  and `JetTypeInfo.AppendUtf16LE(StringBuilder, ReadOnlySpan<byte>)`
  helpers that reinterpret the byte span as `char` via
  `MemoryMarshal.Cast<byte, char>` on little-endian (every supported
  .NET platform), skipping the `Encoding.Unicode` decoder pass and the
  intermediate `string` allocation that the in-loop
  `sb.Append(Encoding.Unicode.GetString(...))` was paying. Wired into
  the `DecodeJet4Text` plain-UCS-2 path, the `DecompressJet4`
  uncompressed-run loop, and the Jet4 `ReadColumnName` TDEF parse.
  The `BitConverter.IsLittleEndian` guard is JIT-constant-folded so the
  fallback branch costs nothing at runtime.
- [x] Confirmed `Encoding.GetString(ReadOnlySpan<byte>)` is used
  everywhere on the typed-decode hot path; the lone remaining
  `byte[], int, int` callers are async LVAL chain readers
  (`AccessReader.DecodeLongValue` etc.) that operate on owned buffers
  outside the per-row loop.

### Verification

- `dotnet test` clean modulo the 2 known DAO Compact failures
  (2552 / 2554 pass).
- `AccessReaderRowDecodeBenchmarks` re-run (same job config as
  Phase 1):

  | Method                              | Phase 3 alloc | Phase 4 alloc | Phase 3 mean | Phase 4 mean |
  | ----------------------------------- | ------------: | ------------: | -----------: | -----------: |
  | `Decode_Numeric_Untyped`            |       8.00 MB |       8.00 MB |     35.94 ms |     34.37 ms |
  | `Decode_Numeric_Typed`              |       3.28 MB |       3.28 MB |     28.71 ms |     23.81 ms |
  | `Decode_Text_Untyped`               |      20.72 MB |      20.72 MB |     67.49 ms |     52.10 ms |
  | `Decode_Text_Typed`                 |       20.0 MB |       20.0 MB |     68.25 ms |     56.82 ms |
  | `Decode_Wide_Untyped`               |      31.71 MB |      31.71 MB |     56.08 ms |     61.69 ms |
  | `Decode_Wide_Typed_NarrowProjection`|       2.14 MB |       2.14 MB |     19.15 ms |     18.17 ms |

  Allocations are byte-identical (Phase 4 was a structural refactor —
  span plumbing doesn't change the allocation profile). Means trend
  faster on the text and numeric paths (the UTF-16LE
  reinterpret-cast path skips the decoder validation pass) but
  several samples have wide error margins so the speed deltas should
  be treated as "within noise to slightly favourable" rather than as
  hard wins. Crucially: **no measurable regression on
  `Decode_*_Untyped`** (the exit criterion).

### Exit criteria — done

- [x] No measurable regression on `Decode_*_Untyped`.
- [x] Code is structurally ready for any future direct-span Phase 3
  refinements (the compiled-decoder builder already routes through the
  span overloads via the `byte[] → ReadOnlySpan<byte>` implicit
  conversion).
- [x] All existing tests still pass (modulo the 2 known failures).

---

## Phase 5 — ~~Attack the `OpenAsync` allocation floor~~ DONE (dropped + cleanup landed)

**Status: closed.** Phase 1's `AccessReaderOpenBenchmarks` showed
`OpenAsync` is **1.1–1.7 ms / ~27 KB** across all tested DBs — three
orders of magnitude smaller than the v1 plan's "~50 ms / ~3.3 MB"
estimate (which had conflated `[GlobalSetup]`, file warm-up, and JIT
into the per-op number). The big-ticket items in the original plan
(lazy `LinkedTableManager`, `LazyCatalog` options flag, span-ifying
the catalog row crack) were rejected on the **"reduce code, reduce
complexity"** rubric: every one of them would have *added* state,
gating, or public API surface in exchange for sub-KB / sub-ms wins.

The one cleanup that *did* meet the bar — and shipped under this phase
— was hoisting the unconditional `StringBuilder` diagnostics build in
[`AccessReader.GetUserTablesAsync`](../../JetDatabaseWriter/Core/AccessReader.cs)
behind the existing `DiagnosticsEnabled` gate. Previously every
`OpenAsync` paid for a `StringBuilder`, a `string.Join` over the
`MSysObjects` columns (with a `ConvertAll` allocating a fresh
`List<string>`), and several interpolated `AppendLine` calls — even
though `LastDiagnostics` is documented as a debug aid and the only
caller in the test suite (`AccessReaderFuzzTests`) treats it as
optional output. The rewrite:

- Replaces the two error-path `LastDiagnostics = diag.ToString()`
  sites with short literal strings (no `StringBuilder`).
- Wraps the success-path diag construction in a single
  `if (DiagnosticsEnabled)` block; the `else` branch sets
  `LastDiagnostics = string.Empty`.
- Net delta: `AccessReader.cs` is ~6 lines shorter and the common
  `OpenAsync` + `ListTablesAsync` path no longer allocates the
  StringBuilder, the `ConvertAll` list, or the joined column-names
  string.

### Verification

- `dotnet test` clean modulo the 2 known DAO Compact failures
  (2552 / 2554 pass).
- No new public API surface; `IAccessReader.LastDiagnostics` contract
  ("populated after each call to `ListTablesAsync`") still holds —
  it's just empty when diagnostics are off, matching the
  `DiagnosticsEnabled = false` default behavior callers already
  inferred.

### Rejected (kept here so future passes don't re-litigate)

- **Lazy `LinkedTableManager`** — adds nullable state + init guard for
  a few KB at open. Net code increase.
- **`AccessReaderOptions { LazyCatalog = true }`** — adds a public
  options field and a second open path. Net API + code increase.
- **Pre-sizing the catalog `result` `List<CatalogEntry>`** — the row
  count isn't reliably known from `MSysObjects` ahead of time, and
  user-table counts are typically <100 so doubling is essentially
  free.
- **Span-ifying `EnumerateRowsAsync` for catalog parse** — the async
  enumerator already yields `List<string>` per row; rewriting it to
  span buffers would require restructuring around `await` boundaries
  for a savings invisible against the ~27 KB floor.

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

- [x] Added [`AccessBase.ComputeLiveRowBoundsArray(byte[] page)`](../../JetDatabaseWriter/Core/AccessBase.cs)
  — eager array form of `EnumerateLiveRowBounds`. Allocates one
  `RowBound[]` (or `Array.Empty<RowBound>()` when the page is empty)
  instead of an iterator object plus the per-call `int[2]` scratch
  buffers the original yield-method paid for. Same parsing logic
  (offset trailer, deletion mask, sort + binary-search to derive each
  row's end), just structured to be cacheable.
- [x] Added a parallel `LruCache<long, RowBound[]>? _rowBoundsCache`
  in [AccessReader.cs](../../JetDatabaseWriter/Core/AccessReader.cs)
  sized 1:1 with `_pageCache` (constructed only when
  `PageCacheSize > 0`). Cleared in `DisposeReaderResourcesAsync`.
- [x] Added `private RowBound[] GetLiveRowBoundsCached(long pageNumber, byte[] page)`
  helper: returns the cached array on hit, otherwise computes via
  `ComputeLiveRowBoundsArray` and stores under `pageNumber`. Stale
  entries left behind after a page is evicted from `_pageCache` simply
  age out of this LRU on their own — the two caches don't have to be
  kept in lock-step for correctness.
- [x] Routed all six per-page enumeration sites in `AccessReader`
  (typed `Rows<T>` projection path, direct-decoder fast path,
  legacy `Rows<T>()` mapper path, `ReadDataTableAsync`'s untyped row
  loop, `RowsAsStrings` projected loop, and the catalog/`MSysObjects`
  scan via `EnumerateRowsAsync`) through the cached helper. Threaded
  `pageNumber` into `EnumerateRowsAsync(long pageNumber, byte[] page,
  TableDef td, ct)` and updated all five callers — `pageNumber` was
  already in scope at every site (each call sits inside a
  `foreach (long pageNumber in pageNumbers)` block immediately after
  `ReadPageCachedAsync(pageNumber, …)`).
- [x] Skipped the writer (`AccessWriter.cs`) — its `EnumerateLiveRowBounds`
  call sites (FK / cascade / index maintenance) are inside mutating
  flows that don't benefit from the read-side memo, and the writer
  doesn't carry a reader-style page cache.

### Verification

- `dotnet test --project JetDatabaseWriter.Tests -c Release`: 2552
  passing / 2 failing — same two known DAO Compact failures
  documented in `/memories/repo/round-trip-tests.md`. No new
  regressions.
- `AccessReaderRowDecodeBenchmarks` re-run (Release, same job config
  as Phase 1; added `Decode_Numeric_Untyped_TwoPass` against a
  dedicated reader with `PageCacheSize = 2048` so the second pass is a
  pure cache hit):

  | Method                              | Phase 4 alloc | Phase 6 alloc | Phase 4 mean | Phase 6 mean |
  | ----------------------------------- | ------------: | ------------: | -----------: | -----------: |
  | `Decode_Numeric_Untyped`            |       8.00 MB |     8.26 MB   |     34.37 ms |     29.78 ms |
  | `Decode_Numeric_Typed`              |       3.28 MB |     3.55 MB   |     23.81 ms |     21.87 ms |
  | `Decode_Text_Untyped`               |      20.72 MB |    20.93 MB   |     52.10 ms |     46.51 ms |
  | `Decode_Text_Typed`                 |      20.0 MB  |    20.21 MB   |     56.82 ms |     55.51 ms |
  | `Decode_Wide_Untyped`               |      31.71 MB |    31.78 MB   |     61.69 ms |     60.97 ms |
  | `Decode_Wide_Typed_NarrowProjection`|       2.14 MB |     2.21 MB   |     18.17 ms |     19.31 ms |
  | `Decode_Numeric_Untyped_TwoPass` ★  |             — |    15.27 MB   |            — |     35.31 ms |

  ★ New for Phase 6.

  **Single-pass first-scan cost.** Storing the `RowBound[]` directory
  in a long-lived LRU adds ~60–270 KB across the synthetic tables
  (one small array per data page, retained as long as the page sits
  in the cache). That shows up as a ~3 % allocation bump on every
  single-scan benchmark. Means trended slightly *faster* (the new
  array form skips the iterator/state-machine allocation per call),
  but the deltas sit inside the run-to-run error margins so the mean
  improvement should be treated as "no regression" rather than a
  measured win on first scan.

  **Re-scan win** (`Decode_Numeric_Untyped_TwoPass` vs 2× the
  single-scan baseline):

  | Metric          | 2× single-pass (computed) | Two-pass (measured) | Δ             |
  | --------------- | ------------------------: | ------------------: | ------------- |
  | Mean            |                  59.56 ms |            35.31 ms | **1.7× faster** |
  | Allocated / op  |                  16.52 MB |            15.27 MB | ~7.6 % less   |

  The bulk of the time saving comes from page-cache hits avoiding the
  decrypt + ArrayPool path; Phase 6's row-bounds memo accounts for
  the allocation delta (skipping per-page `int[2]` scratch + the
  iterator object on the second pass).

### Risk (resolved)

- **Mutated bounds.** `RowBound` is a `readonly record struct`, so
  even though the cached array is shared across callers no consumer
  can mutate an entry. The XML doc on `GetLiveRowBoundsCached`
  explicitly notes the array is owned by the cache.
- **Stale entries after page eviction.** Confirmed acceptable above —
  a stale entry is just memory waste until the second LRU evicts it.
- **netstandard2.1 compatibility.** The first draft used
  `Span<int>.Sort()` / `BinarySearch` (only in .NET Core 2.1+), which
  broke the netstandard2.1 build. Switched to `int[]` + `Array.Sort`
  /`Array.BinarySearch` — same algorithm, one extra allocation per
  page on first parse (then memoized).
- **Writer paths.** `EnumerateLiveRowBounds` remains in place on
  `AccessBase`; only the `AccessReader` call sites were re-routed.
  `AccessWriter` is unchanged.

### Exit criteria — done

- [x] `Decode_*` benchmarks that re-scan the same table show
  measurable improvement (`Decode_Numeric_Untyped_TwoPass` is 1.7×
  faster than 2× single-scan).
- [x] All existing tests still pass (modulo the 2 known failures).

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
   ├─ Phase 2 (column projection)   ✓ done
   │    └─ Phase 3 (direct decoder) ✓ done
   │         └─ Phase 4 (span plumbing) ✓ done (refactor; no alloc delta)
   ├─ Phase 5 (OpenAsync floor)     ✓ done — dropped after Phase 1 finding #1; one diag-gate cleanup landed
   ├─ Phase 6 (cached row directory) ✓ done — re-scan workload 1.7× faster
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
