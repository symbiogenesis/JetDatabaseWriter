# Concurrency and lock ordering

Status: active reference
Date: 2026-06-16
Last updated: 2026-10-03

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

The library has **eight** distinct coordination mechanisms. Six are in-process;
two are cross-process / cross-opener (advisory). Each has a single, distinct
responsibility — the apparent overlap noted in the audit is superficial (see the
closing section).

| # | Primitive | Type | Declared in | Scope | Protects |
|---|-----------|------|-------------|-------|----------|
| 1 | `operationGate` (`ReaderServices.Operations`) | `AsyncReentrantOperationGate` | [ReaderServices.cs#L34](../../JetDatabaseWriter/ReaderServices.cs#L34) | Reader instance; entered by each reader service's public operations | Drains in-flight reader operations against async disposal |
| 2 | `IoGate` | `SemaphoreSlim(1,1)` | [DatabaseFile.cs#L190](../../JetDatabaseWriter/DatabaseFile.cs#L190) | One open database file (reader or writer) | Serializes seek-based stream I/O and journal attach/detach |
| 3 | `ByteRangeLock` | `JetByteRangeLock` | [DatabaseFile.cs#L177](../../JetDatabaseWriter/DatabaseFile.cs#L177) | One open database file | Cooperative JET byte-range page / commit-lock sentinels (advisory) |
| 4 | `insertPageHintLock` | `Lock` / `object` | [DataPageInserter.cs#L28](../../JetDatabaseWriter/Pages/DataPageInserter.cs#L28) | Writer instance (`DataPageInserter`) | The two-field insert-page hint cache only |
| 5 | `ownedDataPagesCacheLock` | `Lock` / `object` | [DatabaseFile.cs#L42](../../JetDatabaseWriter/DatabaseFile.cs#L42) | One open database file | The `ownedDataPagesByTdef` dictionary only |
| 6 | `lockFile` / `lockFileCoordinator` | `LockFileCoordinator` | [LockFileCoordinator.cs](../../JetDatabaseWriter/Transactions/LockFileCoordinator.cs) | Reader + writer instances | `.ldb` / `.laccdb` slot (cross-process) |
| 7 | `AsyncReentrantOperationGate.stateLock` | `Lock` / `object` | [AsyncReentrantOperationGate.cs#L21](../../JetDatabaseWriter/Infrastructure/AsyncReentrantOperationGate.cs#L21) | Internal to #1 | The gate's own drain bookkeeping |
| 8 | `aesGate` | `Lock` / `object` | [PageDecryptionKeys.cs#L23](../../JetDatabaseWriter/Encryption/Models/PageDecryptionKeys.cs#L23) | One open database file | The cached AES-ECB page transforms: their lazy build and every page encrypt or decrypt |

> Note: #4 and #7 are both plain `lock` objects guarding unrelated in-memory
> state (the writer's insert-page hint and the reader gate's drain bookkeeping).
> They never interact. #8 exists because pages are decrypted after `IoGate` is
> released (and the `RandomAccess` read path never takes it), so table-scan
> read-ahead or two operations on one reader can decrypt pages on two threads
> at once, and an `ICryptoTransform` is not safe to share between threads.
> Writes encrypt inside `IoGate`. The Jet3 XOR and Jet4 RC4 paths keep no state
> between pages and take no lock.

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
   aesGate                        (AES page transform; CPU only; reads take it after IoGate,
                                   writes inside it)

— cross-process, lifetime-scoped (not part of per-operation nesting) —
   ByteRangeLock commit-lock  (spans one transaction's replay window)
   LockFileCoordinator slot       (held for the whole reader/writer lifetime)
```

The only pair that genuinely nests on a hot path is **`IoGate` (outer) →
`ByteRangeLock` per-page (inner)**, inside
[`WritePageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L750) and
[`AppendPageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L782). Everything else
is either strictly outer (the operation gate), a leaf, or lifetime-scoped.

### Key invariant: do not hold `IoGate` across a page-write call

[`WritePageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L750) and
[`AppendPageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L782) always acquire
`IoGate` themselves; [`ReadPageAsync`](../../JetDatabaseWriter/DatabaseFile.cs#L377)
acquires it on its seek-and-read path (the positionless `RandomAccess` fast path
— uncached `FileStream` reads outside a transaction — bypasses the gate because
it never touches the shared stream position). Callers must **not** already hold
`IoGate` when calling any of them: the gated path would self-deadlock. The
transaction commit path depends on this: it takes `IoGate` only to detach the
journal, then **releases it before** the replay loop so each replayed
`WritePageAsync` can re-acquire it. See
[`CommitTransactionAsync`](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs#L199).

## Annotated call paths

### Writer auto-commit (`UseTransactionalWrites = true`)

`InsertRowsAsync` → [`RunAutoCommitAsync`](../../JetDatabaseWriter/AccessWriter.cs#L771)
→ [`TransactionLifecycle.RunAutoCommitAsync`](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs#L95)
→ `BeginTransactionAsync` → *work* → `tx.CommitAsync`.

```
BeginTransactionAsync
  └─ IoGate ──▶ capture writer state (insertPageHintLock briefly)
              ──▶ set ActiveJournal / ActiveTransaction ──▶ release IoGate

work phase (row encode, index maintenance, page allocation)
  └─ no durable locks held: every WritePageAsync/AppendPageAsync sees
     ActiveJournal and buffers into the in-memory journal while holding only
     IoGate for the buffer swap.
  └─ insertPageHintLock and ownedDataPagesCacheLock may be taken briefly (leaf, memory only).

CommitTransactionAsync
  ├─ IoGate ──▶ detach journal (ActiveJournal = ActiveTransaction = null) ──▶ release IoGate
  ├─ ByteRangeLock commit-lock sentinel  ◀── held across the entire replay
  │     last cancellation check (nothing written yet)
  │     foreach buffered page (ascending page order), with CancellationToken.None:
  │         WritePageAsync
  │           └─ IoGate ──▶ ByteRangeLock per-page ──▶ seek/write/flush ──▶ release both
  │     FlushDurableAsync (CancellationToken.None)
  └─ release commit-lock (finally)

RollbackTransactionAsync  (auto-commit calls it when the work throws)
  └─ IoGate ──▶ detach journal ──▶ restore writer state (catalog invalidated,
                insertPageHintLock briefly, constraint registry) ──▶ release IoGate
```

The commit-lock sentinel is "outer" only in the sense that it spans the replay
window; it is acquired **after** `IoGate` has been released, so it never nests
outside an already-held `IoGate`. Capturing and restoring the writer state is
memory-only, so the leaf `insertPageHintLock` is the only lock taken inside
`IoGate` there. A commit that fails before its first page write restores the
same state from its `catch`, after `IoGate` has been released, and marks the
transaction rolled back. One that fails after replay has started only
invalidates the catalog and forgets the insert hint, and leaves the transaction
neither committed nor rolled back. The replay is not crash-atomic: there is no
before-image or redo log, so the pages written before a failure stay written.

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
[TableReader.cs#L126](../../JetDatabaseWriter/Tables/TableReader.cs#L126)). The
LINQ provider and the index-query handles call those services directly, so they
enter the same gate.

```
operationGate lease  (reentrant: nested reader calls on the same async flow join the root)
  └─ per page read: IoGate ──▶ seek/read ──▶ release IoGate ──▶ aesGate decrypt (AES files only)
       (uncached FileStream reads outside a transaction instead use a
        positionless RandomAccess read that bypasses IoGate)
  └─ table-scan read-ahead: the next data page's read runs alongside the
     caller's decode of the current page, so two page reads can be in flight
  └─ owned-page cache build: ownedDataPagesCacheLock (leaf, memory only)
```

A path-opened reader's file handle is synchronous (`FileOptions.None`; see
"Page I/O handle" in `read-performance-bottlenecks.md`), so each page read runs
as a blocking read on a thread-pool thread. On Windows the I/O manager
serializes I/O on a synchronous file object: the read-ahead pair and
concurrent scans on one reader queue their reads in the kernel, below every
lock listed here, and still overlap them with decode. That queue adds no lock
to this hierarchy and cannot deadlock with one, because no reader lock is held
while the OS reads, except `IoGate` on the seek-and-read path, which already
serializes that path's reads. The writer opens an overlapped handle.

### Disposal

Reader — [`DisposeAsync`](../../JetDatabaseWriter/AccessReader.cs#L402):

```
operationGate.TryBeginDispose(out waitForOperations)
  └─ lockFile.DisposeAfterAsync(waitForOperations, DisposeReaderResourcesAsync)
        ├─ await waitForOperations   (in-flight reader operations drain)
        ├─ DisposeReaderResourcesAsync → services.Dispose (page and catalog caches)
        │                                 → DatabaseFile.DisposeAsync
        └─ release .ldb / .laccdb slot  (always last)
  └─ operationGate.CompleteDispose()
```

Writer — [`DisposeAsync`](../../JetDatabaseWriter/AccessWriter.cs#L690) (no
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
| `aesGate` | No | Plain `lock`; leaf only |

## Rules for new code

1. Acquire primitives in the documented order. If you need two, the one higher
   in the hierarchy is taken first and released last.
2. Never hold `IoGate` when calling `ReadPageAsync` / `WritePageAsync` /
   `AppendPageAsync` — they take it themselves on their gated paths.
3. Keep `insertPageHintLock`, `ownedDataPagesCacheLock` and `aesGate` as leaf locks: pure
   in-memory work, no `await` and no other lock acquired while held.
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
