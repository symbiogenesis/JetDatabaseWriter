# Recent .NET feature opportunities

This document lists places where .NET and C# features from roughly .NET 6 through .NET 10 could plausibly improve this project. It is intentionally repo-specific: the library already uses nullable annotations, latest analyzers, collection expressions, `ValueTask`, `IAsyncEnumerable<T>`, spans, `BinaryPrimitives`, `SearchValues`, pooled buffers, central package management, lock files, and Microsoft Testing Platform. The useful remaining work is therefore targeted rather than a broad modernization pass.

The main project targets `netstandard2.1;net10.0`. Any BCL feature that is unavailable to `netstandard2.1` should either be isolated behind `#if NET6_0_OR_GREATER` / `#if NET8_0_OR_GREATER` / `#if NET10_0_OR_GREATER`, implemented through a small compatibility helper, or deliberately left out until the older asset is dropped.

## Medium-value candidates

### 3. Use frozen/read-only collection types for immutable lookup data

**Feature family:** .NET 8+ `FrozenDictionary` / `FrozenSet`; newer read-only collection helpers.

**Where:**

- `JetDatabaseWriter/ComplexColumns/Models/AttachmentWrapper.cs`
- `JetDatabaseWriter/Models/DatabaseStatistics.cs`
- any static lookup tables added under `Constants`, schema, or value-decoding code

**Why it matters:**

The scaffold tool already uses `FrozenDictionary<Type, string>` for friendly type names. The library has a few immutable lookup-like structures that are still normal mutable collections wrapped or held privately.

**Concrete work:**

- Change `AttachmentWrapper.CompressedFormats` from a mutable `HashSet<string>` to a `FrozenSet<string>` for the `net8.0+`/`net10.0` asset, or leave it alone if adding `System.Collections.Frozen` to the `netstandard2.1` asset is not worth the dependency.
- Consider replacing `DatabaseStatistics.EmptyTableRowCounts = new ReadOnlyDictionary<string, long>(new Dictionary<string, long>())` with an empty frozen dictionary or `ReadOnlyDictionary<TKey,TValue>.Empty` if available to the target.
- Prefer frozen collections only for truly immutable, repeated lookup data. Do not apply it to per-table/per-operation dictionaries that are built once and mutated during parsing.

**Expected benefit:** code quality and small lookup/read-only correctness gains.

### 4. Use `CollectionsMarshal.GetValueRefOrAddDefault` only in measured dictionary-building hot paths

**Feature family:** .NET 6+ `CollectionsMarshal` dictionary ref APIs.

**Where:**

- `JetDatabaseWriter/AccessReader.cs` in `BuildOwnedDataPageIndexAsync`
- `JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs` where dictionaries of `HashSet<int>` or `List<long>` are built in loops
- `JetDatabaseWriter/Indexes/IndexMaintainer.cs` only if grouping shows up in benchmarks
- `JetDatabaseWriter/Schema/ColumnPropertyBlockBuilder.cs` name-pool construction, though `TryAdd` is probably sufficient there

**Why it matters:**

Some hot loops perform a lookup and then an add/update. `CollectionsMarshal.GetValueRefOrAddDefault` can collapse duplicate lookups, but it is a sharp tool: the returned reference must not survive dictionary mutations that resize the dictionary. Use it only where benchmarks justify the complexity.

**Concrete work:**

- Prefer simple `TryAdd` for low-volume builders such as the column-property name pool.
- Consider `CollectionsMarshal.GetValueRefOrAddDefault` for page-owner grouping and complex-column cascade maps if profiling shows dictionary overhead.
- Wrap any usage in small helper methods with tests, because misuse can create subtle correctness bugs.

**Expected benefit:** performance in selected loops, with a readability tradeoff.

### 7. Introduce `TimeProvider` only where it buys deterministic behavior

**Feature family:** .NET 8+ `TimeProvider`.

**Where:**

- `JetDatabaseWriter/AccessWriter.cs`
- `JetDatabaseWriter/Catalog/CatalogWriter.cs`
- `JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs`
- `JetDatabaseWriter/Transactions/JetByteRangeLock.cs`
- fuzz tests that stamp crash filenames or log timestamps

**Why it matters:**

The writer stamps `DateCreate`, `DateUpdate`, and default attachment timestamps using `DateTime.UtcNow`. Lock acquisition uses `Stopwatch` and polling delays. This is fine for production behavior, but tests and round-trip fixture generation can become more deterministic with an injectable clock.

**Concrete work:**

- Consider an optional internal/public clock on `AccessWriterOptions` only if deterministic emitted timestamps become important for test fixtures or reproducible database bytes.
- Consider `TimeProvider` in `JetByteRangeLock` only if lock-timeout tests need virtual time; otherwise `Stopwatch` is appropriate and low-risk.
- Do not introduce a broad clock abstraction just for style.

**Expected benefit:** test determinism and reproducibility when needed; otherwise low priority.

## Low-value or situational candidates

### 9. Use `System.Threading.Lock` for simple synchronous locks after target constraints are settled

**Feature family:** .NET 9+ `System.Threading.Lock`.

**Where:**

- `JetDatabaseWriter.Tests/RoundTrip/DaoValidationFixture.cs`
- possibly `JetDatabaseWriter/Infrastructure/AsyncReentrantOperationGate.cs`, but only after careful review

**Why it matters:**

`System.Threading.Lock` is a clearer, allocation-friendly replacement for private `object` locks in synchronous code. The main current candidate is test-fixture synchronization. The async operation gate uses `AsyncLocal`, `Interlocked`, and task completion state; changing it is not obviously beneficial and could reduce clarity.

**Concrete work:**

- Convert simple test-only `_sync` locks to `System.Threading.Lock` if the test project remains `net10.0`.
- Leave `SemaphoreSlim`-based async gates alone; `System.Threading.Lock` is not an async lock.
- Leave `ReaderWriterLockSlim` in `LruCache` unless benchmarks show a simpler exclusive lock performs better. The current cache intentionally allows read-lock access for count/stat properties and write-lock access for LRU mutation.

**Expected benefit:** small readability/allocation polish in tests.

### 10. Use package validation and API compatibility checks before publishing

**Feature family:** SDK package validation, API compatibility checks, analyzer properties.

**Where:**

- `JetDatabaseWriter/JetDatabaseWriter.csproj`
- `Directory.Build.props`
- CI/build scripts, if present later

**Why it matters:**

The library is packaged with symbols, documentation, strict analyzers, and multi-targeting. Package validation can catch accidental public API breaks, incompatible assets, and packaging issues before release.

**Concrete work:**

- Evaluate `<EnablePackageValidation>true</EnablePackageValidation>` for pack/release builds.
- Add a baseline after the next intentional public API release if the current surface has known compatibility changes.
- Keep nullable/analyzer suppressions in `JetDatabaseWriter.csproj` visible as debt; package validation should not mask them.
- Treat `IsAotCompatible` / `IsTrimmable` as future investigation only. The current library uses `System.Data`, expression-tree mapping, encodings, and dynamic schema behavior, so AOT/trimming compatibility needs a separate audit rather than a flag flip.

**Expected benefit:** release robustness and code quality.

### 11. Add more BenchmarkDotNet diagnostics for modernization work

**Feature family:** modern BenchmarkDotNet diagnosers and memory/disassembly exporters.

**Where:**

- `JetDatabaseWriter.Benchmarks/AccessReaderBenchmarks.cs`
- `JetDatabaseWriter.Benchmarks/AccessReaderRowDecodeBenchmarks.cs`
- `JetDatabaseWriter.Benchmarks/AccessWriterBenchmarks.cs`
- `JetDatabaseWriter.Benchmarks/LruCacheBenchmarks.cs`
- `JetDatabaseWriter.Benchmarks/TypedValueParserBenchmarks.cs`

**Why it matters:**

Many candidate changes are micro-optimizations. BenchmarkDotNet is already present; use it to prevent tidy-looking changes from making row decoding, index encoding, cache behavior, or crypto slower.

**Concrete work:**

- Add `MemoryDiagnoser` consistently where allocation changes are being evaluated.
- Use disassembly or hardware-counter diagnosers only for focused investigations, not default benchmark runs.
- Add targeted benchmarks before changing `CollectionsMarshal`, hash loops, or index key encoding.

**Expected benefit:** performance correctness and better regression visibility.

## Areas already using recent features well

These are worth preserving but do not need modernization churn right now.

- `ValueTask` and `IAsyncEnumerable<T>` are already central to the reader/writer API and hot paths.
- `AccessReader` already uses `[EnumeratorCancellation]`, pooled row buffers, and sync-completing `ValueTask` paths for typed row decoding.
- `Span<T>`, `ReadOnlySpan<T>`, `Memory<T>`, `MemoryMarshal`, and `BinaryPrimitives` are already widely used in page parsing, row encoding/decoding, index encoding, and text handling.
- `ColumnPropertyBlockBuilder` now uses exact-size byte arrays plus `BinaryPrimitives` for chunk, name-pool, and property-block serialization while preserving stable name-pool ordering and unknown-chunk re-emission.
- `SearchValues` is already used for OLE payload signature scanning, zlib header suffix detection, and collation line-break scanning.
- Collection expressions and target-typed `new` are already common.
- Primary constructors and `required` members are already used where they improve local code clarity, especially in FormatProbe and scaffold code.
- The scaffold tool already uses `FrozenDictionary<Type, string>` for static type-name lookup.
- Office Crypto Standard/Agile paths use one-shot AES-CBC APIs and shared span-based hash/HMAC helpers on modern targets, with `netstandard2.1` fallbacks where required.
- xUnit v3 and Microsoft Testing Platform are already wired through `global.json`, the test project, and VS Code tasks.
- Central package management, package lock files, nullable, latest analyzers, warnings-as-errors, NuGet audit, reproducible builds, XML docs, symbols, and strict arithmetic are already enabled.

## Features that are probably not worth adopting now

- `GeneratedRegex`: there are no real regex hot paths in the library today.
- Source generators or interceptors: no obvious generator target is worth the maintenance cost yet. Row mapping already has expression-tree and direct-decoder paths.
- Broad `System.Threading.Lock` conversion: async gates, `SemaphoreSlim`, and `ReaderWriterLockSlim` have domain-specific reasons to exist.
- `FrozenDictionary` for per-operation dictionaries: frozen collections pay a construction cost and are best for static immutable lookups.
- AOT/trimming flags: valuable to investigate, but risky to declare without a dedicated compatibility pass.
- SIMD/vector intrinsics: binary parsing is format-bound and branchy; use only after benchmarks identify a tight CPU-bound loop.

## Suggested implementation order

1. Package validation/API compatibility in pack/release builds.
2. Targeted lower-priority polish: frozen static lookups, span Base64/hex parsing, `System.Threading.Lock` in tests, and FormatProbe LINQ cleanups.
