# Recent .NET feature opportunities

This document lists places where .NET and C# features from roughly .NET 6 through .NET 10 could plausibly improve this project. It is intentionally repo-specific: the library already uses nullable annotations, latest analyzers, collection expressions, `ValueTask`, `IAsyncEnumerable<T>`, spans, `BinaryPrimitives`, `SearchValues`, pooled buffers, central package management, lock files, and Microsoft Testing Platform. The useful remaining work is therefore targeted rather than a broad modernization pass.

The main project targets `netstandard2.1;net10.0`. Any BCL feature that is unavailable to `netstandard2.1` should either be isolated behind `#if NET6_0_OR_GREATER` / `#if NET8_0_OR_GREATER` / `#if NET10_0_OR_GREATER`, implemented through a small compatibility helper, or deliberately left out until the older asset is dropped.

## Highest-value candidates

### 1. Use one-shot and span-based crypto APIs in Office Crypto paths

**Feature family:** .NET 6+ one-shot hash and HMAC helpers such as `SHA1.HashData`, `SHA1.TryHashData`, `SHA512.HashData`, `SHA512.TryHashData`, `SHA256.HashData`, `HMACSHA512.HashData`, and `HMACSHA512.TryHashData`; `CryptographicOperations.ZeroMemory`; span-based text encoding.

**Where:**

- `JetDatabaseWriter/Encryption/OfficeCryptoStandard.cs`
- `JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs`
- `JetDatabaseWriter/Encryption/EncryptionManager.cs`
- `JetDatabaseWriter/Encryption/EncryptionConverter.cs`

**Why it matters:**

Encryption is both correctness-sensitive and allocation-sensitive. Standard encryption performs 50,000 SHA-1 iterations; Agile performs 100,000 SHA-512 iterations. Current code often creates hash/HMAC instances and allocates intermediate byte arrays for inputs and outputs. That is correct, but the newer APIs express the single-shot intent better and can reduce GC pressure in password verification, encryption, re-encryption, and encrypted page open paths.

**Concrete work:**

- Replace `SHA1.Create().ComputeHash(...)` in Standard verifier hashing, password verification, key finalization, and PBKDF loops with static span-based helpers where possible.
- Replace `SHA512.Create().ComputeHash(...)` in Agile verifier hashing, `ResolvePassword`, `SegmentIv`, `FlatPageIv`, `HmacIv`, `DeriveKey`, `DeriveAllPasswordKeys`, and `FinalizeKey` with span-based static helpers.
- Replace `new HMACSHA512(key).ComputeHash(data)` in Agile data-integrity creation and verification with `HMACSHA512.HashData` / `TryHashData`.
- Replace disposable `SHA256` instances used for AES page-key derivation with `SHA256.TryHashData` or a helper that writes into a stack span.
- Avoid `password.ToArray()` in Standard key derivation. Encode `ReadOnlySpan<char>` directly into a stack or pooled UTF-16 buffer, then scrub it with `CryptographicOperations.ZeroMemory`.
- Prefer `CryptographicOperations.FixedTimeEquals` for verifier/HMAC comparison helpers after validating comparable lengths. The existing XOR loops are constant-time-ish, but the BCL API makes the security intent clearer.

**Expected benefit:** performance, robustness, code clarity, lower allocation pressure, and clearer security intent.

**Compatibility note:** keep SHA-1, MD5, AES-CBC, and AES-ECB where the JET/Office formats require them. The recommendation is about safer/faster API shape, not changing algorithms.

### 2. Use `FileStreamOptions` and preallocation in file creation/replacement

**Feature family:** .NET 6+ `FileStreamOptions`, `PreallocationSize`, clearer async/sequential/random access options.

**Where:**

- `JetDatabaseWriter/AccessBase.cs`
- `JetDatabaseWriter/AccessWriter.cs`
- `JetDatabaseWriter/Encryption/EncryptionManager.cs`
- `JetDatabaseWriter/Transactions/LockFileSlotWriter.cs`
- transaction and lock-file tests that open `FileStream` directly

**Why it matters:**

The project has a few central file-open paths plus several direct `new FileStream(...)` calls. `FileStreamOptions` would make access/share/options choices harder to misread, and `PreallocationSize` can reduce fragmentation and partial-allocation surprises when writing known-size database images or temporary encrypted replacements.

**Concrete work:**

- Update `AccessBase.OpenDatabaseFileStream` to construct `FileStreamOptions` from the existing mode/access/share/options/buffer-size inputs.
- Use `FileStreamOptions` in `AccessWriter.CreateDatabaseAsync(string, ...)` and set `PreallocationSize = dbBytes.Length` when creating the initial database image.
- Use `FileStreamOptions` in `EncryptionManager.ReplaceFileAtomicAsync` and set `PreallocationSize = contents.Length` for the replacement file.
- Consider using `FileStreamOptions` in `LockFileSlotWriter.Open` for the JET lock file path, while preserving `FileShare.ReadWrite | FileShare.Delete` semantics.
- Clean up direct `FileStream` construction in transaction/byte-range-lock tests for consistency.

**Expected benefit:** readability, maintainability, and modest I/O robustness/performance during create/re-encrypt workflows.

### 3. Consider `RandomAccess` for page reads on path-backed streams

**Feature family:** .NET 6+ `System.IO.RandomAccess` positional reads/writes.

**Where:**

- `JetDatabaseWriter/AccessBase.cs`
- `JetDatabaseWriter/AccessReader.cs`
- `JetDatabaseWriter/AccessReaderOptions.cs`

**Why it matters:**

`ReadPageAsync` currently serializes stream positioning through `_ioGate`, seeks, then reads. That is correct for arbitrary `Stream` inputs, encrypted streams, and journal-aware writer paths, but it prevents true positional reads when the backing source is a `FileStream`. This is especially relevant because `AccessReaderOptions` already has `ParallelPageReadsEnabled`; positional I/O would make that option easier to exploit without shared stream-position races.

**Concrete work:**

- Add an internal path/file-handle fast path for `FileStream`-backed readers that uses `RandomAccess.ReadAsync(handle, buffer, fileOffset, cancellationToken)`.
- Keep the current `_ioGate` + seek/read path for caller-supplied streams, active journals, in-memory decrypted Office Crypto streams, and any stream without a safe file handle.
- Benchmark table streaming and catalog scans before enabling by default; the current page cache and DAO-format constraints may make the win workload-dependent.
- If positional writes are considered later, account carefully for byte-range locks, transaction journals, and page encryption.

**Expected benefit:** performance and correctness headroom for parallel page reads; less reliance on mutable stream position.

### 4. Use `Stream.ReadExactlyAsync` / `ReadAtLeastAsync` where exact bytes are required (DONE)

**Feature family:** .NET 7+ exact-read APIs.

**Where:**

- `JetDatabaseWriter/AccessBase.cs`
- `JetDatabaseWriter/CompoundFile/CompoundFileReader.cs`
- `JetDatabaseWriter/Encryption/EncryptionManager.cs`

**Why it matters:**

Several methods manually loop until a header, page, CFB sector, or sniff buffer is filled. Manual loops are fine, but exact-read APIs document the invariant directly and give more uniform truncated-file behavior.

**Concrete work:**

- Use `ReadExactlyAsync` in `ReadHeaderAsync` for the 128-byte database header.
- Use exact reads in CFB sector/header parsing where partial data is always corrupt.
- Use exact reads in encryption detection and sniffing only where truncation should be an error; retain partial-read tolerance where the code is intentionally probing short streams.
- Keep page reads conservative: a missing page in a corrupt/truncated database should produce a clear format error, but the writer's transaction/journal paths may need their existing behavior.

**Expected benefit:** correctness, robustness, and readability.

### 5. Use `ZLibStream` for zlib-wrapped attachment data (DONE)

**Feature family:** .NET 6+ `System.IO.Compression.ZLibStream`.

**Where:**

- `JetDatabaseWriter/AccessReader.cs`
- possibly `JetDatabaseWriter/ComplexColumns/Models/AttachmentWrapper.cs`, after confirming the exact wire format

**Why it matters:**

`AccessReader.DecompressAttachmentData` scans for a zlib header, skips the 2-byte header, then feeds the remainder to `DeflateStream`. That works for many payloads, but it hand-rolls part of the zlib wrapper handling. `ZLibStream` understands the wrapper directly, which should make attachment/OLE decompression less brittle.

**Concrete work:**

- In the heuristic reader path, once `FindZlibHeader` locates a candidate, feed the slice beginning at the zlib header to `ZLibStream` instead of skipping into `DeflateStream`.
- Keep `AttachmentWrapper.RawDeflate` on `DeflateStream` unless DAO/Jackcess fixture evidence shows that wrapper payload is zlib-wrapped rather than raw deflate.
- Add tests with known zlib-wrapped and raw-deflate attachment samples before changing writer output.

**Expected benefit:** correctness and maintainability for complex attachment/OLE payload handling.

## Medium-value candidates

### 6. Replace `BinaryWriter` + `MemoryStream` with direct span writers in small binary serializers

**Feature family:** span APIs, `BinaryPrimitives`, `IBufferWriter<byte>` / `ArrayBufferWriter<byte>` where useful.

**Where:**

- `JetDatabaseWriter/Schema/ColumnPropertyBlockBuilder.cs`
- optionally similar small schema/CFB emitters after profiling

**Why it matters:**

The column-property block serializer builds known little-endian binary layouts using `MemoryStream` and `BinaryWriter`. The code is correct, but this area is format-sensitive and already documented by exact byte layout. Direct span writes can make the on-disk structure more explicit and avoid small transient allocations.

**Concrete work:**

- Replace `WriteChunk`, `BuildNamePoolPayload`, and `BuildPropertyBlockPayload` with helpers that reserve exact sizes and write with `BinaryPrimitives.WriteUInt16LittleEndian` / `WriteUInt32LittleEndian`.
- Use `Encoding.GetByteCount` to compute exact string payload sizes before allocating.
- Preserve the current stable name-pool ordering and unknown-chunk re-emission behavior.

**Expected benefit:** readability for binary layout, small allocation reduction, and easier auditing.

### 7. Use frozen/read-only collection types for immutable lookup data

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

### 8. Use `CollectionsMarshal.GetValueRefOrAddDefault` only in measured dictionary-building hot paths

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

### 9. Use span-based Base64 and hex parsing to avoid substring/split allocations

**Feature family:** span overloads for `Convert`, `Convert.FromHexString`, `Convert.TryFromBase64String`, `Convert.TryFromBase64Chars`, `Convert.ToHexString`.

**Where:**

- `JetDatabaseWriter/AccessReader.cs`
- `JetDatabaseWriter/ValueDecoding/TypedValueParser.cs`
- `JetDatabaseWriter/Models/Hyperlink.cs`
- `JetDatabaseWriter/Indexes/Helpers/IndexHelpers.cs`, if binary debug-key formatting appears in hot diagnostics

**Why it matters:**

Several string-to-binary paths split strings or allocate substrings before parsing Base64 or dash-separated hex. These paths are not the main table-read hot path, but they affect OLE, binary round-tripping, diagnostics, and strict parsing behavior.

**Concrete work:**

- Replace Base64 `Substring` + `Convert.FromBase64String` in data-URI parsing with span-based `TryFromBase64Chars` / `TryFromBase64String` and exact destination sizing.
- Keep `Convert.FromHexString` for plain hex, but add a small dash-separated hex parser that walks the string span rather than `Split('-')` + per-token conversion.
- Consider `Convert.ToHexString` for any future binary string output if the dash-separated `BitConverter.ToString` format is not part of public behavior.

**Expected benefit:** performance, fewer exception-driven parse paths, and cleaner malformed-input handling.

### 10. Use modern AES one-shot APIs where they match the required cipher mode

**Feature family:** newer `Aes` span/one-shot helpers such as CBC/ECB encrypt/decrypt methods where available on the target.

**Where:**

- `JetDatabaseWriter/Encryption/OfficeCryptoStandard.cs`
- `JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs`
- `JetDatabaseWriter/Encryption/EncryptionManager.cs`

**Why it matters:**

The code currently uses `ICryptoTransform.TransformFinalBlock` or cached transforms. That is a normal implementation, but one-shot AES helpers can remove transform lifetime boilerplate in Standard/Agile package encryption and decryption. The page-level ECB path in `EncryptionManager` is already in-place and allocation-aware, so it should be changed only if the newer API can preserve the same in-place behavior and format-required no-padding semantics.

**Concrete work:**

- Evaluate one-shot AES helpers for `AesCbcZeroIv` and `AesCbcRaw`.
- Keep spec-required `PaddingMode.None`, zero IVs, per-segment IV derivation, and ECB page encryption exactly as-is.
- Benchmark and fuzz before replacing the in-place page transform path.

**Expected benefit:** readability and potential allocation reduction in package encryption/decryption.

### 11. Introduce `TimeProvider` only where it buys deterministic behavior

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

### 12. Use .NET 8 LINQ helpers in probes and diagnostics

**Feature family:** `CountBy`, `AggregateBy`, `Index`, and newer LINQ aggregation helpers.

**Where:**

- `JetDatabaseWriter.FormatProbe/FormatProbeApplication.cs`
- `JetDatabaseWriter.FormatProbe/DaoBaselineProbe.cs`
- `JetDatabaseWriter.FormatProbe/LongRowBisect.cs`

**Why it matters:**

FormatProbe is an analysis tool, not a production hot path. Some summaries scan the same collection multiple times with `Count(...)` / `Sum(...)`. Newer LINQ helpers can reduce scans and clarify intent, but the performance impact is tiny compared with DAO and file I/O.

**Concrete work:**

- Replace repeated verdict counting with a single local aggregation or `CountBy` when it makes the code clearer.
- Avoid clever LINQ if a simple `foreach` is more readable.

**Expected benefit:** readability and minor analysis-tool performance.

### 13. Use `System.Threading.Lock` for simple synchronous locks after target constraints are settled

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

### 14. Use package validation and API compatibility checks before publishing

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

### 15. Add more BenchmarkDotNet diagnostics for modernization work

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
- Add targeted benchmarks before changing `RandomAccess`, `CollectionsMarshal`, hash loops, zlib handling, or index key encoding.

**Expected benefit:** performance correctness and better regression visibility.

## Areas already using recent features well

These are worth preserving but do not need modernization churn right now.

- `ValueTask` and `IAsyncEnumerable<T>` are already central to the reader/writer API and hot paths.
- `AccessReader` already uses `[EnumeratorCancellation]`, pooled row buffers, and sync-completing `ValueTask` paths for typed row decoding.
- `Span<T>`, `ReadOnlySpan<T>`, `Memory<T>`, `MemoryMarshal`, and `BinaryPrimitives` are already widely used in page parsing, row encoding/decoding, index encoding, and text handling.
- `SearchValues` is already used for OLE payload signature scanning, zlib header suffix detection, and collation line-break scanning.
- Collection expressions and target-typed `new` are already common.
- Primary constructors and `required` members are already used where they improve local code clarity, especially in FormatProbe and scaffold code.
- The scaffold tool already uses `FrozenDictionary<Type, string>` for static type-name lookup.
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

1. Crypto hash/HMAC modernization in `OfficeCryptoStandard`, `OfficeCryptoAgile`, `EncryptionManager`, and `EncryptionConverter`, backed by existing encryption tests plus allocation benchmarks.
2. `FileStreamOptions` and preallocation in database create/re-encrypt paths.
3. Exact-read helpers for headers, CFB sectors, and encryption sniffing. (DONE)
4. `ZLibStream` experiments for Access attachment/OLE decompression with fixture tests.
5. Package validation/API compatibility in pack/release builds.
6. Targeted lower-priority polish: frozen static lookups, span Base64/hex parsing, `System.Threading.Lock` in tests, and FormatProbe LINQ cleanups.
