# Bounded external long-value streaming

The selected hostile-file surface is `LongValueStore.CopyPayloadAsync` for an external chained descriptor. `verification/LongValueTraversal.dfy` proves termination and resource bounds for its row schedule. Production calls the generated `LongValueKernel.ChainVisitAdmission`, `ChainPayloadCount`, `NextVisitedCapacity`, `VisitedSlot` and `NextVisitedSlot` decisions. This is scoped implementation correspondence with focused conformance tests, not verification of the complete C# parser, native row ownership or arbitrary locator/destination implementations.

## Admission and progress

Descriptor admission first bounds the declared stored length `L` and configured row budget `B`. A null pointer, exhausted row budget or repeated row identity refuses before another acquisition. Successful distinct visits consume one row-budget unit. The generated payload selector admits only a nonempty bounded row payload and returns at most the remaining declared bytes. Each completed write therefore decreases the remaining payload `L - copied` by at least one. `Traverse` uses `B - acquisitions` as its loop rank, so a zero-payload row, absent row or invalid slice refuses after its single acquisition instead of permitting another iteration. The mathematical visited set contains exactly the distinct nonzero acquired identities.

Only the descriptor's declared prefix is streamed. Pointers or padding after completion are not interpreted. This behavior cannot establish complete storage ownership or authorize reclamation. Single-page and inline paths retain their existing format rules; the arbitrary chained-graph proof concerns the external chained path.

## Explicit resource bounds

For `n` attempted row acquisitions, including a final malformed row, the theorem establishes:

- `n <= min(L, B)` and exactly `n` retained logical row identities;
- emitted bytes at most `L`, with each successful chunk bounded by the effective page length;
- logical bounded-page acquisition work at most `pageSize * n`;
- current visited-table capacity at most `16 + 4*n` unsigned-integer slots;
- cumulative visited-table allocations at most `32 + 8*n` slots, counting every superseded array, hence at most `128 + 32*n` bytes of array payload.

`LongValueVisitedSet` allocates lazily with an initial 16-slot table and doubles at half occupancy. Every successful growth adds exactly the new capacity to cumulative allocated slots; their sum is strictly less than twice the current capacity. During rehash, only the old/new arrays are referenced by the set, and their combined size is bounded by that cumulative sum. Duplicate lookup occurs before growth, so retrying an identity does not allocate another table. Growth refuses before producing an array length beyond the bounded native selector. An allocation failure propagates; successful allocation or sufficient machine memory is not assumed for termination by refusal.

The formulas count traversal-owned array payload, not CLR object/array headers, async frames, error strings, allocator bookkeeping or GC retention policies. The number of arrays is also bounded by their cumulative slots because every array has at least 16 slots. The parser does not allocate a buffer of size `L`; it awaits each borrowed row's write and releases that row before the next acquisition. The locator may use pooled arrays larger than the effective page length; `pageSize*n` is acquisition work over admitted page prefixes, not an allocator-byte bound on locator internals. Locator/cache allocations and destination storage are independent contracts.

## Probe and production correspondence

`ProbeRankDecreases`, `ProbeResiduesAreDistinct`, `CircularProbeCoversEverySlot`, `NonfullProbeFindsEmpty` and `NativeProbeMatchesCircularStep` establish bounded circular probing and coverage. Every production lookup/insertion has an explicit decreasing remaining-slot counter. Half occupancy provides empty slots, and rehash preserves the existing logical identities by inserting every nonzero old slot into the new array.

The proof's visited set is mathematical. Exact C# insertion, early-empty lookup and rehash membership correspondence remain a manual refinement obligation. `LongValueTraversalTests` independently compares membership through collisions, growth and duplicate reinsertion, tests repeated identities across growth boundaries, refuses every no-progress chained row size, and checks the generated slice selector against a wide-arithmetic boundary oracle. Existing `LongValueStreamingTests` cover exhaustion before another lookup, cycles, truncated chains, partial writes, cancellation, source/destination errors and delayed buffer release. These controls supplement the proof; they do not mechanically verify the handwritten table implementation.

## Effects and remaining assumptions

The pure model assumes that each described row acquisition and successful write completes. Actual I/O may fail, and cancellation is checked before each acquisition/chunk. Failures can leave a partial destination; no rollback or destination flush is promised. A successful locator transfers its scoped page lease with the located row. The traversal disposes that location in `finally`, including invalid locations and failed writes, after the awaited output has completed. Both reader-cache and compact-discovery locators use this ownership path; there is no separate raw-buffer release callback. If the locator itself throws, it retains ownership. Actual buffer return and successful stream effects remain implementation contracts; the [frame ownership ledger](page-frame-ownership.md) is a separate abstract specification with manual C# correspondence.

The row directory, page type, pointer/token interpretation, native format compatibility, cache ownership, rented-buffer sizing, destination persistence and concurrent mutation of borrowed bytes remain separate obligations. Native fixtures, malformed-file tests and fuzzing remain necessary. No whole-library hostile-file or crash-durability guarantee follows from these bounds.
