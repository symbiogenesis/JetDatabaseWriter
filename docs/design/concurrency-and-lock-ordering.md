# Concurrency and lock ordering

Status: active reference
Date: 2026-06-16
Last updated: 2026-10-02

This note is the single, canonical description of the synchronization model used
by [AccessReader](../../JetDatabaseWriter/AccessReader.cs),
[AccessWriter](../../JetDatabaseWriter/AccessWriter.cs), their services, and the
[DatabaseFile](../../JetDatabaseWriter/DatabaseFile.cs) each facade owns. It exists to answer one
question that the code alone makes hard to verify: **when more than one
synchronization primitive is involved, which one is outer and which is inner?**

Read this before adding a new lock, taking an existing lock from a new call
site, or moving an `await` inside a critical section. The
[code-quality audit](../code-quality-audit.md) finding #7 ("sprawling,
overlapping concurrency model") is resolved by this document plus the decision
recorded in [Why these are not consolidated](#why-these-are-not-consolidated).

## The primitives

The library has **seven** distinct coordination mechanisms. Five are in-process;
two are cross-process / cross-opener (advisory). Each has a single, distinct
responsibility — the apparent overlap noted in the audit is superficial (see the
closing section).

| # | Primitive | Type | Declared in | Scope | Protects |
|---|-----------|------|-------------|-------|----------|
| 1 | `operationGate` (`ReaderServices.Operations`) | `AsyncReentrantOperationGate` | [ReaderServices.cs#L35](../../JetDatabaseWriter/ReaderServices.cs#L35) | Reader instance; entered by each reader service's public operations | Drains in-flight reader operations against async disposal |
| 2 | `IoGate` | `SemaphoreSlim(1,1)` | [DatabaseFile.cs#L190](../../JetDatabaseWriter/DatabaseFile.cs#L190) | One open database file (reader or writer) | Serializes seek-based stream I/O and journal attach/detach |
| 3 | `ByteRangeLock` | `JetByteRangeLock` | [DatabaseFile.cs#L177](../../JetDatabaseWriter/DatabaseFile.cs#L177) | One open database file | Cooperative JET byte-range page / commit-lock sentinels (advisory) |
| 4 | `insertPageHintLock` | `Lock` / `object` | [DataPageInserter.cs#L28](../../JetDatabaseWriter/Pages/DataPageInserter.cs#L28) | Writer instance (`DataPageInserter`) | The two-field insert-page hint cache only |
| 5 | `ownedDataPagesCacheLock` | `Lock` / `object` | [DatabaseFile.cs#L42](../../JetDatabaseWriter/DatabaseFile.cs#L42) | One open database file | The `ownedDataPagesByTdef` dictionary only |
| 6 | `lockFile` / `lockFileCoordinator` | `LockFileCoordinator` | [LockFileCoordinator.cs](../../JetDatabaseWriter/Transactions/LockFileCoordinator.cs) | Reader + writer instances | `.ldb` / `.laccdb` slot (cross-process) |
| 7 | `AsyncReentrantOperationGate.stateLock` | `Lock` / `object` | [AsyncReentrantOperationGate.cs#L21](../../JetDatabaseWriter/Infrastructure/AsyncReentrantOperationGate.cs#L21) | Internal to #1 | The gate's own drain bookkeeping |

> Note: #4 and #7 are both plain `lock` objects guarding unrelated in-memory
> state (the writer's insert-page hint and the reader gate's drain bookkeeping).
> They never interact.

## Acquisition-order hierarchy (outermost → innermost)

When two or more primitives are held at once, they are always acquired in this
order. Never acquire one earlier in this list while holding one later in it.

```
1. operationGate lease            (reader only; wraps a whole public operation; reentrant)
2. IoGate                         (one logical page I/O, or a journal attach/detach)
3. ByteRangeLock per-page     (one durable page write; INSIDE IoGate)

— leaf locks (never held across an await, never nested under each other) —
   insertPageHintLock             (insert-page cache; pure memory)
   ownedDataPagesCacheLock        (owned-page dictionary; pure memory)

— cross-process, lifetime-scoped (not part of per-operation nesting) —
   ByteRangeLock commit-lock  (spans one transaction's replay window)
   LockFileCoordinator slot       (held for the whole reader/writer lifetime)
```

The only pair that genuinely nests on a hot path is **`IoGate` (outer) →
`ByteRangeLock` per-page (inner)**, inside
[`WritePageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L688) and
[`AppendPageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L720). Everything else
is either strictly outer (the operation gate), a leaf, or lifetime-scoped.

### Key invariant: do not hold `IoGate` across a page-write call

[`WritePageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L688) and
[`AppendPageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L720) always acquire
`IoGate` themselves; [`ReadPageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L359)
acquires it on its seek-and-read path (the positionless `RandomAccess` fast path
— uncached `FileStream` reads outside a transaction — bypasses the gate because
it never touches the shared stream position). Callers must **not** already hold
`IoGate` when calling any of them: the gated path would self-deadlock. The
transaction commit path depends on this: it takes `IoGate` only to detach the
journal, then **releases it before** the replay loop so each replayed
`WritePageAsync` can re-acquire it. See
[`CommitTransactionAsync`](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs#L156).

## Annotated call paths

### Writer auto-commit (`UseTransactionalWrites = true`)

`InsertRowsAsync` → [`RunAutoCommitAsync`](../../JetDatabaseWriter/AccessWriter.cs#L732)
→ [`TransactionLifecycle.RunAutoCommitAsync`](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs#L68)
→ `BeginTransactionAsync` → *work* → `tx.CommitAsync`.

```
BeginTransactionAsync
  └─ IoGate ──▶ set ActiveJournal / ActiveTransaction ──▶ release IoGate

work phase (row encode, index maintenance, page allocation)
  └─ no durable locks held: every WritePageAsync/AppendPageAsync sees
     ActiveJournal and buffers into the in-memory journal while holding only
     IoGate for the buffer swap.
  └─ insertPageHintLock and ownedDataPagesCacheLock may be taken briefly (leaf, memory only).

CommitTransactionAsync
  ├─ IoGate ──▶ detach journal (ActiveJournal = ActiveTransaction = null) ──▶ release IoGate
  ├─ ByteRangeLock commit-lock sentinel  ◀── held across the entire replay
  │     foreach buffered page (ascending page order):
  │         WritePageAsync
  │           └─ IoGate ──▶ ByteRangeLock per-page ──▶ seek/write/flush ──▶ release both
  │     FlushDurableAsync
  └─ release commit-lock (finally)
```

The commit-lock sentinel is "outer" only in the sense that it spans the replay
window; it is acquired **after** `IoGate` has been released, so it never nests
outside an already-held `IoGate`.

### Writer non-transactional (default `UseTransactionalWrites = false`)

No journal, no commit-lock. Each page mutation flushes immediately:

```
WritePageAsync
  └─ IoGate ──▶ ByteRangeLock per-page ──▶ seek/write/flush ──▶ release both
```

### Reader operation

Every public reader operation opens with
`using AsyncReentrantOperationGate.Lease operation = operations.Enter();` in the
reader service that implements it (for example
[TableReader.cs#L127](../../JetDatabaseWriter/Tables/TableReader.cs#L127)). The
LINQ provider and the index-query handles call those services directly, so they
enter the same gate.

```
operationGate lease  (reentrant: nested reader calls on the same async flow join the root)
  └─ per page read: IoGate ──▶ seek/read ──▶ release IoGate
       (uncached FileStream reads outside a transaction instead use a
        positionless RandomAccess read that bypasses IoGate)
  └─ owned-page cache build: ownedDataPagesCacheLock (leaf, memory only)
```

### Disposal

Reader — [`DisposeAsync`](../../JetDatabaseWriter/AccessReader.cs#L384):

```
operationGate.TryBeginDispose(out waitForOperations)
  └─ lockFile.DisposeAfterAsync(waitForOperations, DisposeReaderResourcesAsync)
        ├─ await waitForOperations   (in-flight reader operations drain)
        ├─ DisposeReaderResourcesAsync → services.Dispose (page and catalog caches)
        │                                 → DatabaseFile.DisposeAsync
        └─ release .ldb / .laccdb slot  (always last)
  └─ operationGate.CompleteDispose()
```

Writer — [`DisposeAsync`](../../JetDatabaseWriter/AccessWriter.cs#L681) (no
operation gate; the writer is single-writer by construction):

```
lockFileCoordinator.DisposeAfterAsync(
    TransactionLifecycle.DisposeActiveTransactionAsync,  (implicit rollback of any open tx)
    RewrapAndCloseOuterEncryptedStreamAsync,             (Agile re-encrypt on close)
    DatabaseFile.DisposeAsync)
  └─ release .ldb / .laccdb slot  (always last)
```

## Reentrancy

| Primitive | Reentrant? | Mechanism / consequence |
|-----------|-----------|--------------------------|
| `operationGate` | Yes | `AsyncLocal<int> operationDepth`; nested calls on one async flow join the active root operation |
| `IoGate` | No | Binary `SemaphoreSlim(1,1)` — re-entering on the same flow self-deadlocks; never hold it across a `*PageAsync` call |
| `ByteRangeLock` per-page | No | OS advisory byte-range lock; re-locking the same range blocks |
| `insertPageHintLock` | No | Plain `lock`; leaf only |
| `ownedDataPagesCacheLock` | No | Plain `lock`; leaf only |

## Rules for new code

1. Acquire primitives in the documented order. If you need two, the one higher
   in the hierarchy is taken first and released last.
2. Never hold `IoGate` when calling `ReadPageAsync` / `WritePageAsync` /
   `AppendPageAsync` — they take it themselves on their gated paths.
3. Keep `insertPageHintLock` and `ownedDataPagesCacheLock` as leaf locks: pure in-memory
   work, no `await` and no other lock acquired while held.
4. Per-page byte-range locks go **inside** `IoGate`, never the reverse.
5. The cross-process locks (`LockFileCoordinator` slot, `ByteRangeLock`
   commit-lock) are lifetime- or transaction-scoped, not per-page; do not fold
   them into the per-operation nesting.

## Why these are not consolidated

The audit suggested collapsing the insert-page cache lock (then a
`ReaderWriterLockSlim`), the `SemaphoreSlim`, and the operation gate behind one
async primitive. We deliberately keep them
separate because their responsibilities do not actually overlap:

- **`IoGate`** serializes *backing-stream I/O*. It must be an async-aware mutex
  because it is held across real `await`ed reads/writes/flushes.
- **`operationGate`** is a *reader-disposal drain*, not a mutex: it permits
  unbounded concurrent reentrant operations and only blocks *new* top-level
  operations once disposal starts. Merging it into `IoGate` would serialize
  reads that are intentionally allowed to overlap.
- **`insertPageHintLock`** guards a two-field insert-page hint cache
  ([`TryGetCachedInsertPageNumber`](../../JetDatabaseWriter/Pages/DataPageInserter.cs#L120) /
  [`SetCachedInsertPageNumber`](../../JetDatabaseWriter/Pages/DataPageInserter.cs#L135)).
  It is a pure-memory leaf lock with no I/O; routing it through the I/O mutex
  would add contention for no benefit.

A single "do everything" primitive would conflate I/O serialization, a
read-concurrency drain, and a memory-cache guard — increasing contention and
coupling. The genuine simplification went the other way: the insert-page cache
moved from the writer facade into `DataPageInserter`, its only consumer, and its
`ReaderWriterLockSlim` became a plain `lock`, which also removed a disposal step.
