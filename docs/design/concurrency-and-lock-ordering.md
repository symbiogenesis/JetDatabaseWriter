# Concurrency and lock ordering

Status: active reference
Date: 2026-06-16
Last updated: 2026-10-04

This note is the single, canonical description of the synchronization model used
by [AccessReader](../../JetDatabaseWriter/AccessReader.cs),
[AccessWriter](../../JetDatabaseWriter/AccessWriter.cs), their services, and the
[DatabaseFile](../../JetDatabaseWriter/DatabaseFile.cs) each facade owns, with its page file: a
read-only [PageFile](../../JetDatabaseWriter/Pages/Paging/PageFile.cs) for the reader, the
writer's [Pager](../../JetDatabaseWriter/Pages/Paging/Pager.cs). It exists to answer one
question that the code alone makes hard to verify: **when more than one
synchronization primitive is involved, which one is outer and which is inner?**

Read this before adding a new lock, taking an existing lock from a new call
site, or moving an `await` inside a critical section. The
[code-quality audit](../code-quality-audit.md) finding #7 ("sprawling,
overlapping concurrency model") is resolved by this document plus the decision
recorded in [Why these are not consolidated](#why-these-are-not-consolidated).

## The primitives

The library has **ten** distinct coordination mechanisms. Eight are in-process;
two are cross-process / cross-opener (advisory). Each has a single, distinct
responsibility — the apparent overlap noted in the audit is superficial (see the
closing section).

| # | Primitive | Type | Declared in | Scope | Protects |
|---|-----------|------|-------------|-------|----------|
| 1 | `operationGate` (`ReaderServices.Operations`) | `AsyncReentrantOperationGate` | [ReaderServices.cs](../../JetDatabaseWriter/ReaderServices.cs) | Reader instance; entered by each reader service's public operations | Drains in-flight reader operations against async disposal |
| 2 | `IoGate` | `SemaphoreSlim(1,1)` | [PageFile.cs](../../JetDatabaseWriter/Pages/Paging/PageFile.cs) (`PageFile.IoGate`) | One open database file (reader or writer) | Serializes seek-based stream I/O; in the writer, also journal attach/detach through a `Pager.JournalGate` lease |
| 3 | `ByteRangeLock` | `JetByteRangeLock` | [Pager.cs](../../JetDatabaseWriter/Pages/Paging/Pager.cs) (`Pager.ByteRangeLock`) | The writer's open database file | Cooperative JET byte-range page / commit-lock sentinels (advisory) |
| 4 | `insertPageHintLock` | `Lock` / `object` | [DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs) | Writer instance (`DataPageInserter`) | The two-field insert-page hint cache only |
| 5 | `ownedDataPagesCacheLock` | `Lock` / `object` | [OwnedDataPages.cs](../../JetDatabaseWriter/Pages/OwnedDataPages.cs) | One open database file (reader only) | The `ownedDataPagesByTdef` dictionary only |
| 6 | `lockFile` / `lockFileCoordinator` | `LockFileCoordinator` | [LockFileCoordinator.cs](../../JetDatabaseWriter/Transactions/LockFileCoordinator.cs) | Reader + writer instances | `.ldb` / `.laccdb` slot (cross-process) |
| 7 | `AsyncReentrantOperationGate.stateLock` | `Lock` / `object` | [AsyncReentrantOperationGate.cs](../../JetDatabaseWriter/Infrastructure/AsyncReentrantOperationGate.cs) | Internal to #1 | The gate's own drain bookkeeping |
| 8 | `aesGate` | `Lock` / `object` | [PageDecryptionKeys.cs](../../JetDatabaseWriter/Encryption/Models/PageDecryptionKeys.cs) | One open database file | The cached AES-ECB page transforms: their lazy build and every page encrypt or decrypt |
| 9 | `ownedDataPageIndex` gate | `SemaphoreSlim(1,1)` inside `AsyncLazyInitializer` | [AsyncLazyInitializer.cs](../../JetDatabaseWriter/Infrastructure/AsyncLazyInitializer.cs), built in [OwnedDataPages.cs](../../JetDatabaseWriter/Pages/OwnedDataPages.cs) (`BuildOwnedDataPageIndexAsync`) | One open database file (reader only) | The one-time whole-file pass that maps every data page to its table, run for the first table whose owned-pages usage map fails validation |
| 10 | `tdefBytesCacheLock` | `Lock` / `object` | [TableDefReader.cs](../../JetDatabaseWriter/Schema/TableDefReader.cs) | One open database file (reader only) | The `tdefBytesByPage` dictionary only |

> Note: #4 and #7 are both plain `lock` objects guarding unrelated in-memory
> state (the writer's insert-page hint and the reader gate's drain bookkeeping).
> They never interact, and neither do #5 and #10, the reader's two memo locks.
> #8 exists because pages are decrypted after `IoGate` is
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
2. ownedDataPageIndex gate        (reader only; held across the whole-file owned-page pass,
                                   whose page reads take IoGate on the seek-and-read path)
3. IoGate                         (one logical page I/O, or one Pager.JournalGate lease:
                                   a journal attach/detach with the writer-state capture/restore)
4. ByteRangeLock per-page     (one durable page write; INSIDE IoGate)

— leaf locks (never held across an await, never nested under each other) —
   insertPageHintLock             (insert-page cache; pure memory)
   ownedDataPagesCacheLock        (owned-page dictionary; pure memory)
   tdefBytesCacheLock             (TDEF-bytes dictionary; pure memory)
   aesGate                        (AES page transform; CPU only; reads take it after IoGate,
                                   writes inside it)

— cross-process, lifetime-scoped (not part of per-operation nesting) —
   ByteRangeLock commit-lock  (spans one transaction's replay window)
   LockFileCoordinator slot       (held for the whole reader/writer lifetime)
```

The only pair that genuinely nests on a hot path is **`IoGate` (outer) →
`ByteRangeLock` per-page (inner)**, inside `Pager.WritePageAsync` and
`Pager.AppendPageAsync` ([Pager.cs](../../JetDatabaseWriter/Pages/Paging/Pager.cs)). The
`ownedDataPageIndex` gate also nests outside `IoGate`, but at most once per
reader, for the whole-file pass. Everything else is either strictly outer (the
operation gate), a leaf, or lifetime-scoped.

### Key invariant: do not hold `IoGate` across a page-write call

`Pager.WritePageAsync` and `Pager.AppendPageAsync` always acquire `IoGate`
themselves; `PageFile.ReadPageAsync` acquires it on its seek-and-read path (the
positionless `RandomAccess` fast path — a path-opened reader's uncached
`FileStream` reads; the writer never takes it, and `Pager.CanReadPositionally`
turns it off while a journal is attached — bypasses the gate because it never
touches the shared stream position). On that path a pending page of the
writer's journal is copied under the same gate acquisition
(`Pager.TryCopyPendingPage`) and is not decrypted. Callers must **not** already
hold `IoGate` when calling any of them, and so must not hold a
`Pager.JournalGate` lease, which holds `IoGate`: the gated path would
self-deadlock. `IoGate` itself is private to the page file; only a
`JournalGate` lease (`Pager.EnterJournalGateAsync`) holds it outside a page
call. The transaction commit path depends on this: it takes a lease only to
detach the journal, then **disposes it before** the replay loop so each
replayed `WritePageAsync` can re-acquire the gate. See `CommitTransactionAsync`
in [TransactionLifecycle.cs](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs).

## Annotated call paths

### Writer auto-commit (`UseTransactionalWrites = true`)

`InsertRowsAsync` → `RunAutoCommitAsync` ([AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs))
→ `TransactionLifecycle.RunAutoCommitAsync` ([TransactionLifecycle.cs](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs))
→ `BeginTransactionAsync` → *work* → `tx.CommitAsync`.

```
BeginTransactionAsync
  └─ JournalGate lease (IoGate) ──▶ capture writer state (insertPageHintLock briefly)
              ──▶ gate.Attach(journal); set ActiveTransaction ──▶ dispose the lease

work phase (row encode, index maintenance, page allocation)
  └─ no durable locks held: every WritePageAsync/AppendPageAsync sees the
     attached journal and buffers into it while holding only IoGate for the
     buffer swap.
  └─ insertPageHintLock may be taken briefly (leaf, memory only). The writer's file never
     caches owned pages or TDEF bytes, so the writer never takes ownedDataPagesCacheLock
     or tdefBytesCacheLock.

CommitTransactionAsync
  ├─ JournalGate lease (IoGate) ──▶ gate.Detach(); ActiveTransaction = null ──▶ dispose the lease
  ├─ ByteRangeLock commit-lock sentinel  ◀── held across the entire replay
  │     last cancellation check (nothing written yet)
  │     foreach buffered page (ascending page order), with CancellationToken.None:
  │         WritePageAsync
  │           └─ IoGate ──▶ ByteRangeLock per-page ──▶ seek/write/flush ──▶ release both
  │     FlushDurableAsync (CancellationToken.None)
  └─ release commit-lock (finally)

RollbackTransactionAsync  (auto-commit calls it when the work throws)
  └─ JournalGate lease (IoGate) ──▶ gate.Detach() ──▶ restore writer state (catalog invalidated,
                insertPageHintLock briefly, constraint registry) ──▶ dispose the lease
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
reader service that implements it (for example `TableReader.Rows` in
[TableReader.cs](../../JetDatabaseWriter/Tables/TableReader.cs)). The
LINQ provider and the index-query handles call those services directly, so they
enter the same gate.

```
operationGate lease  (reentrant: nested reader calls on the same async flow join the root)
  └─ per page read: IoGate ──▶ seek/read ──▶ release IoGate ──▶ aesGate decrypt (AES files only)
       (a path-opened reader's uncached FileStream reads instead use a
        positionless RandomAccess read that bypasses IoGate)
  └─ table-scan read-ahead: the next data page's read runs alongside the
     caller's decode of the current page, so two page reads can be in flight
  └─ owned-page cache build: ownedDataPagesCacheLock (leaf, memory only)
  └─ TDEF-bytes memo: tdefBytesCacheLock (leaf, memory only; the chain is read
     before the lock is taken)
  └─ owned-page index build (the first table whose usage map fails validation):
       ownedDataPageIndex gate ──▶ ReadPageAsync for every page from 3 to the end of file
       (each takes IoGate on the seek-and-read path) ──▶ release the gate
       (other operations that need the index await the gate for the whole pass)
```

A path-opened reader's file handle is synchronous (`FileOptions.None`; see
"Page I/O handle" in `read-performance-bottlenecks.md`), so each page read runs
as a blocking read on a thread-pool thread. On Windows the I/O manager
serializes I/O on a synchronous file object: the read-ahead pair and
concurrent scans on one reader queue their reads in the kernel, below every
lock listed here, and still overlap them with decode. That queue adds no lock
to this hierarchy and cannot close a deadlock cycle, because it never waits on
a library lock: a queued read waits only for the reads ahead of it, and each
of those completes without taking one. Library gates are held while the OS
reads: the operation lease across every read, the `ownedDataPageIndex` gate
across the whole-file pass, and `IoGate` across each read on the seek-and-read
path. The library only ever awaits them (`WaitAsync`, or the disposal drain)
and never blocks a thread on one, so a read queued in the kernel can delay the
operations waiting on those gates but cannot deadlock them. The writer opens
an overlapped handle.

When the read starts on a thread-pool thread with no synchronization context
and the default task scheduler, a path-opened reader runs it on that thread
instead of another pool thread (`PageFile.ReadsInlineOnThreadPool`). On
the seek-and-read path that thread holds `IoGate` across the blocking read,
just as an offloaded read held it across the awaited one, so the hierarchy is
unchanged. An inline read cannot be cancelled once started; the token is
checked before it. Threads with a synchronization context, such as UI threads,
never block on a page read.

### Disposal

Reader — `DisposeAsync` ([AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)):

```
operationGate.TryBeginDispose(out waitForOperations)
  └─ lockFile.DisposeAfterAsync(waitForOperations, DisposeReaderResourcesAsync)
        ├─ await waitForOperations   (in-flight reader operations drain)
        ├─ DisposeReaderResourcesAsync → services.Dispose (page and catalog caches)
        │                                 → DatabaseFile.DisposeAsync (PageFile: stream, IoGate,
        │                                   page cipher; then the TableDefReader and
        │                                   OwnedDataPages caches)
        └─ release .ldb / .laccdb slot  (always last)
  └─ operationGate.CompleteDispose()
```

Writer — `DisposeAsync` ([AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)) (no
operation gate; the writer is single-writer by construction):

```
lockFileCoordinator.DisposeAfterAsync(
    TransactionLifecycle.DisposeActiveTransactionAsync,  (implicit rollback of any open tx; then
                                                          Pager.ForceDetachJournal without the gate)
    RewrapAndCloseOuterEncryptedStreamAsync,             (Agile re-encrypt on close)
    DatabaseFile.DisposeAsync)
  └─ release .ldb / .laccdb slot  (always last)
```

## Reentrancy

| Primitive | Reentrant? | Mechanism / consequence |
|-----------|-----------|--------------------------|
| `operationGate` | Yes | `AsyncLocal<int> operationDepth`; nested calls on one async flow join the active root operation |
| `ownedDataPageIndex` gate | No | Binary `SemaphoreSlim(1,1)` — the index build only reads pages; it must never ask for the owned-page index itself |
| `IoGate` | No | Binary `SemaphoreSlim(1,1)` — re-entering on the same flow self-deadlocks; never hold it, or a `Pager.JournalGate` lease, across a `*PageAsync` call |
| `ByteRangeLock` per-page | No | OS advisory byte-range lock; re-locking the same range blocks |
| `insertPageHintLock` | No | Plain `lock`; leaf only |
| `ownedDataPagesCacheLock` | No | Plain `lock`; leaf only |
| `tdefBytesCacheLock` | No | Plain `lock`; leaf only |
| `aesGate` | No | Plain `lock`; leaf only |

## Writer-owned caches and the exclusive-writer assumption

The writer keeps some of what it reads from the file in memory for the rest of
the session, on the assumption that nothing else changes the file while it is
open:

| Cache | Owner | Dropped when |
|-------|-------|--------------|
| User-table list and calculated result types | `TableCatalog` | `Invalidate`: every catalog write (create, drop or rewrite a table, add a catalog object), a rollback, a failed `UseTransactionalWrites` call and a failed commit. Each call also moves `TableCatalog.Generation` on. |
| Insert-page hint and the set of owned maps it may extend | `DataPageInserter` | Restored to its state at `BeginTransactionAsync` on a rollback; the hint is forgotten after a commit that fails during replay. |
| Constraint lists, AutoNumber `NextAutoValue` and complex-reference counters | `ConstraintRegistry` | Re-registered by schema changes; restored on a rollback. A commit that fails during replay keeps them, so counters never move back over values that may be on disk. |
| Enforced relationships, which every insert, update and delete checks | `RelationshipCatalogStore` | Every `MSysRelationships` write through the store (create, drop or rename a relationship, rename a key column), and whenever `TableCatalog.Generation` has moved on since the set was loaded, so a rollback or failed commit reloads it. |

All of these are memory-only, so restoring or dropping them takes no lock
beyond the leaf `insertPageHintLock`; the generation counters use `Interlocked`.

A writer opened by path holds the file with `FileShare.Read`, so no other
process can open it for writing while the writer is open. A writer opened on a
caller-supplied stream leaves that to the caller: if another process writes the
file underneath it, the writer keeps using the tables, counters and
relationships it already read and does not see the change. Every
`MSysRelationships` write in this library must go through
`RelationshipCatalogStore`, which drops the relationship cache; a new path that
writes those rows some other way must call `RelationshipCatalogStore.Invalidate`.

## Reader-owned caches and the unchanging-file assumption

The reader keeps what it reads from the file in memory until it is disposed,
on the assumption that the file does not change while it is open:

| Cache | Owner | Dropped when |
|-------|-------|--------------|
| Pages and their row directories, up to `PageCacheSize` pages | `ReaderPageCache` | The least recently used page first, when the cache is full; every page at dispose. |
| User-table list | `TableCatalog` | Dispose. |
| Linked-table list | `LinkedTableReader` | Dispose. |
| Each table's TDEF bytes | `TableDefReader` | Dispose. |
| Each table's owned data pages, and the whole-file owner index | `OwnedDataPages` | Dispose. |

Nothing writes through the reader, so its own work never makes them stale.
`AccessReaderOptions.FileShare` defaults to `ReadWrite`, so Access or another
process may have the file open for writing at the same time, but the reader
cannot tell when that process changes the file: for a table it has read, it
keeps the columns, row count, indexes and owned pages it read first, and a read
may combine pages cached before a change with pages read after it. The reader
has to be reopened to see such changes. A new memo in the reader keeps to the
same assumption.

## Rules for new code

1. Acquire primitives in the documented order. If you need two, the one higher
   in the hierarchy is taken first and released last.
2. Never hold `IoGate`, or a `Pager.JournalGate` lease, when calling
   `ReadPageAsync` / `WritePageAsync` / `AppendPageAsync` — they take the gate
   themselves on their gated paths.
3. Keep `insertPageHintLock`, `ownedDataPagesCacheLock`, `tdefBytesCacheLock` and `aesGate`
   as leaf locks: pure in-memory work, no `await` and no other lock acquired while held.
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
  (`TryGetCachedInsertPageNumber` / `SetCachedInsertPageNumber` in
  [DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs)).
  It is a pure-memory leaf lock with no I/O; routing it through the I/O mutex
  would add contention for no benefit.

A single "do everything" primitive would conflate I/O serialization, a
read-concurrency drain, and a memory-cache guard — increasing contention and
coupling. The genuine simplification went the other way: the insert-page cache
moved from the writer facade into `DataPageInserter`, its only consumer, and its
`ReaderWriterLockSlim` became a plain `lock`, which also removed a disposal step.
