# Page-frame ownership

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
leases retain their bytes. Disposing a lease releases its reference once. Only
the final release returns the buffer to the pool.

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
and releases it before acquiring the next row. A cache capacity limits retained
frames; live borrowers can keep additional evicted frames alive. It is not a
process-wide heap or aggregate concurrent-reader memory limit.

## Checked model and production boundary

[PageFrameOwnership.dfy](../../verification/PageFrameOwnership.dfy) is a
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
arbitrary C# array user cannot mutate bytes. Aggregate memory accounting,
complete C# refinement and the containing lifecycle composition remain I7/S1/V10
work. Paired hosted measurements are required for performance acceptance; no
allocation or throughput improvement follows from these theorems.
