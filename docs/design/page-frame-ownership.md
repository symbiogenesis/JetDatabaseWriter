# Page-frame ownership

`AccessOptions.MaxActivePageLeases` (on both `AccessReaderOptions` and `AccessWriterOptions`) bounds pending acquisitions plus live page leases across `ReaderPageCache`, direct `PageFile` reads, and `Pager` reads, with immediate refusal (`JetErrorCode.PageLeaseBudgetExceeded`) rather than waiting for another held lease. `AccessOptions.MaxLivePageFrames` (0 for automatic `Math.Max(0, PageCacheSize) + MaxActivePageLeases`, or an explicit positive ceiling) bounds total live `PageFrame` instances (`RetainedFrames + EvictedPinnedFrames`), evicting unpinned cached frames (`IsExclusivelyOwned`, `ReferenceCount == 1`) in LRU/CLOCK order when headroom is needed and refusing immediately when all allowed live frames are pinned by active leases. `AccessOptions.MaxCachedRowDirectoryEntries` bounds the total lazily parsed `RowBound[]` slot entries retained across live frames, releasing reserved counts when a frame's final reference drops. Direct `PageFile.ReadPageAsync` and `Pager` store loads also bound concurrent in-flight physical reads (`InFlightReads` / `InFlightStoreReads`) under the active lease budget. Concurrent non-Pager misses for the same page share one load, including when retention is disabled; a cancelled producer does not poison an uncancelled waiter. `PageFrameOwnership.AggregateFramesFitAdmission` proves the C + L cardinality bound for the union of retained frames and ticket-covered borrowed frames; actual C# coverage of that ownership relation remains a contract. The generated `DataPageKernel.TryReserveCount` proves reservation arithmetic; locks, CAS publication, frame identity and actual lifetime correspondence remain reviewed and tested C# effects.

Read leases keep immutable page bytes alive independently of cache membership.
`PageFrame` owns one pooled buffer and its lazily parsed row directory;
`PageLease` exposes the effective page prefix as read-only memory. The internal
array bridge used by existing byte-array decoders is a borrowed read-only
reference: its caller must keep the lease alive through every synchronous or
asynchronous use and must not let the array escape that scope.

## Ownership and mutation

A frame's reference count includes each live lease and its cache owner, when
retained. Cache lookup and lease acquisition occur under the same cache lock,
so eviction cannot remove the last owner between those operations. Eviction,
replacement, invalidation and cache disposal release cache ownership; existing
leases retain their bytes as `EvictedPinnedFrames` within the `MaxLivePageFrames`
budget until disposed. Disposing a lease releases its reference once. Only
the final release returns the buffer to the pool and decrements `LiveFrames`
and `CachedRowDirectoryEntries` on the frame's `IPageFrameOwner` (`PageFile`,
`Pager`, or standalone `ReaderPageCache`).

Acquisition refuses a nonlive frame and checks headroom before incrementing the
signed reference count. Allocation failure while creating a lease must undo an
already acquired reference. Temporary producer ownership transfers to a cache
or lease without losing the buffer on an exception. Buffer allocation, object
construction and these transfers remain explicit implementation obligations.

Writer updates, including readable zero reservations, replace immutable frame
images. They do not clear or overwrite the bytes of an existing leased frame.
Mutable update copies belong to their callers and are separate from read
leases. Rollback and invalidation detach old retained frames; an in-flight
load may enter the cache only if its captured generation still matches the
current generation. This retention rule does not claim linearizable reads
through concurrent database mutation.

Lease lifetime holds no I/O semaphore. Scan, seek, overflow and long-value
callers keep their own scoped leases, including asynchronous prefetch cleanup,
and release them on completion, early disposal, cancellation and failure.
Long-value streaming retains a located row until its awaited output completes
and releases it before acquiring the next row. `PageCacheStatistics` and
`PagerStatistics` report `RetainedFrames` (`CachedFrames`), `EvictedPinnedFrames`,
`LiveFrames`, `OutstandingLeases`, `ActiveLoadGates`, `InFlightStoreReads`,
`DirtyBufferedPages` and `CachedRowDirectoryEntries`. These named per-instance
budgets do not constitute a process-wide heap or aggregate concurrent-reader
memory limit.

## Checked model and production boundary

[PageFrameOwnership.dfy](../../verification/Pages/Paging/PageFrameOwnership.dfy) is a
verification-only ledger for one frame. `Valid` equates the bounded reference
count with its cache or temporary producer owner plus the set of live lease
tickets. `Acquire`, `Release`, `Detach` and `DropProducer` describe atomic
successful reference-count transitions. `Retain` transfers producer ownership
to the cache without incrementing the count; its generation check belongs to
the cache controller. Lease tickets are unique
within that frame, and frame bytes and their acquisition generation never
change.

`StepPreserves` and `TracePreserves` establish the invariant for arbitrary finite
admitted traces. `PinnedFrameCannotReturn`, `LastOwnerReturnsExactlyOnce`,
the repeated-disposal lemmas, `ReturnedOrFullFrameRefusesAcquisition` and
`OldGenerationCannotBeRetained` cover their named safety and refusal cases.
`LeaseLifetimeCompletes` supplies a reachable two-reader trace through eviction,
repeated disposal and one final pool return; `EarlyReturnWithLiveLeaseIsInvalid`
rejects premature recycling. `RetainedProductionReadCompletes` and
`UnretainedProductionReadCompletes` exercise successful cache admission and
producer release, including acquisition before the producer-to-cache handoff.
`ProducerHandoffPreservesReferenceCount` matches that handoff's count behavior.
These witnesses keep success and refusal distinct.

This model does not execute in the library. Actual CAS linearization, cache
locks, frame/array identity, producer handoff, pool behavior, decoder discipline
and cancellation/disposal paths require source review and focused C# tests.
Immutability is part of the modeled transition relation, not a proof that an
arbitrary C# array user cannot mutate bytes. Complete C# refinement and the
containing lifecycle composition remain S1/V10 work.
