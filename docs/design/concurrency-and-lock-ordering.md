# Concurrency and lock ordering

Status: active reference
Date: 2026-06-16
Last updated: 2026-10-05

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

The reader operation gate drains active operations before disposal. The page store
serializes physical stream access, while the writer's pager separately coordinates
its transaction journal and plaintext frame cache.

| Primitive | Owner | Responsibility |
|---|---|---|
| `operationGate` | `ReaderServices.Operations` | Reentrant reader-operation leases and disposal drain |
| `IoGate` | `PageFile`, used by `Pager.JournalGate` | Journal reads, attach and detach; never held across a store operation |
| `frameGate` | `Pager` | Serializes cache fills, writes, append allocation, truncation and journal transitions |
| `frameSync` | `Pager` | Frame dictionary, CLOCK bookkeeping, counters and invalidation generation; memory only |
| `ioGate` | `StreamPageStore` | Seek-based reads, writes, length changes and flushes; positional reads bypass it |
| Per-page and commit locks | `JetByteRangeLock` | Advisory cross-process page writes and transaction replay |
| `aesGate` | `AesEcbPageCodec` | Lazy transform creation, transforms and key disposal |
| `insertPageHintLock` | `DataPageInserter` | Insert-page hint; memory only |
| `ownedMapSetsLock` | `CatalogOwnedMapPolicy` | Writable and refused TDEF sets; memory only |
| `ownedDataPagesCacheLock` | `OwnedDataPages` | Reader's per-table owned-page cache; memory only |
| `tdefBytesCacheLock` | `TableDefReader` | Reader's TDEF-byte cache; memory only |
| Owned-page index initializer | `AsyncLazyInitializer` in `OwnedDataPages` | One reader whole-file discovery pass |
| `AsyncReentrantOperationGate.stateLock` | Reader operation gate | Drain bookkeeping; memory only |
| Lock-file slot | `LockFileCoordinator` | `.ldb` / `.laccdb` slot held until disposal finishes |

## Acquisition order

A writer cache fill or write holds `frameGate`. It may briefly take `IoGate` to
consult or modify the journal, then releases `IoGate` before calling the store.
A physical write encodes its scratch buffer first, takes its per-page byte-range
lock, then takes the store's `ioGate` for seek/write/flush. The page lock is
released after the store gate. AES transforms never run under the store gate.
A physical read releases the store gate before decoding.

`frameSync`, `aesGate` and the other memory locks are leaves: no awaits or store
calls while holding them. Invalidation takes only `frameSync`, increments a
generation and drops every frame. A load that began under an older generation
cannot retain its bytes after invalidation.

A `Pager.JournalGate` lease holds `frameGate` and then `IoGate`. It waits for
physical writes to finish before capturing transaction state and must never
span a page read, write, append, cache fill or store call. Transaction attach
and detach invalidate frames while under this lease; disposal releases
`IoGate` and then `frameGate`. The commit path disposes its lease before
replaying pages through normal page writes.

The reader operation lease and owned-page index initializer can span reads, but
neither can be acquired while holding a store or pager gate. Positional reader
I/O bypasses the store gate and leaves the shared stream position alone. The
cross-process commit sentinel spans the transaction replay; the lock-file slot
spans the open database lifetime.
## Annotated call paths

### Writer auto-commit (`UseTransactionalWrites = true`)

`InsertRowsAsync` → `RunAutoCommitAsync` ([AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs))
→ `TransactionLifecycle.RunAutoCommitAsync` ([TransactionLifecycle.cs](../../JetDatabaseWriter/Transactions/TransactionLifecycle.cs))
→ `BeginTransactionAsync` → *work* → `tx.CommitAsync`.

```
BeginTransactionAsync
  └─ JournalGate lease (frameGate, then IoGate) ──▶ capture writer state (insertPageHintLock, ownedMapSetsLock briefly)
              ──▶ gate.Attach(journal); set ActiveTransaction ──▶ dispose the lease

work phase (row encode, index maintenance, page allocation)
  └─ no durable locks held: every WritePageAsync/AppendPageAsync sees the
     attached journal and buffers into it while holding frameGate and briefly IoGate for the
     buffer swap.
  └─ insertPageHintLock and ownedMapSetsLock may be taken briefly (leaf, memory only;
     the owned-map policy's MSysObjects scan runs outside its lock). The writer's file never
     caches owned pages or TDEF bytes, so the writer never takes ownedDataPagesCacheLock
     or tdefBytesCacheLock.

CommitTransactionAsync
  ├─ JournalGate lease (frameGate, then IoGate) ──▶ gate.Detach(); ActiveTransaction = null ──▶ dispose the lease
  ├─ ByteRangeLock commit-lock sentinel  ◀── held across the entire replay
  │     last cancellation check (nothing written yet)
  │     foreach buffered page (ascending page order), with CancellationToken.None:
  │         WritePageAsync
  │           └─ frameGate ──▶ encode ──▶ ByteRangeLock per-page ──▶ store ioGate ──▶ seek/write/flush
  │     FlushDurableAsync (CancellationToken.None)
  └─ release commit-lock (finally)

RollbackTransactionAsync  (auto-commit calls it when the work throws)
  └─ JournalGate lease (frameGate, then IoGate) ──▶ gate.Detach() ──▶ restore writer state (catalog invalidated,
                insertPageHintLock and ownedMapSetsLock briefly, constraint registry) ──▶ dispose the lease
```

The commit-lock sentinel is "outer" only in the sense that it spans the replay
window; it is acquired **after** `IoGate` has been released, so it never nests
outside an already-held `IoGate`. Capturing and restoring the writer state is
memory-only, so the leaf `insertPageHintLock` and `ownedMapSetsLock`, one after
the other, protect those caches; frame invalidation briefly takes `frameSync` there. A commit that fails
before its first page write restores the same state from its `catch`, after
`IoGate` has been released, and marks the transaction rolled back. One that
fails after replay has started only invalidates the catalog, forgets the insert
hint and the owned-map policy's refusals, and puts the policy's writable set
back, and leaves the transaction neither committed nor rolled back. The replay is not crash-atomic: there is no
before-image or redo log, so the pages written before a failure stay written.

### Writer non-transactional (default `UseTransactionalWrites = false`)

No journal, no commit-lock. Each page mutation flushes immediately:

```
WritePageAsync
  └─ frameGate ──▶ encode ──▶ ByteRangeLock per-page ──▶ store ioGate ──▶ seek/write/flush
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
  └─ per page read: store ioGate ──▶ seek/read ──▶ release store ioGate ──▶ aesGate decrypt (AES files only)
       (a path-opened reader's uncached FileStream reads instead use a
        positionless RandomAccess read that bypasses store ioGate)
  └─ table-scan read-ahead: the next data page's read runs alongside the
     caller's decode of the current page, so two page reads can be in flight
  └─ owned-page cache build: ownedDataPagesCacheLock (leaf, memory only)
  └─ TDEF-bytes memo: tdefBytesCacheLock (leaf, memory only; the chain is read
     before the lock is taken)
  └─ owned-page index build (the first table whose usage map fails validation):
       ownedDataPageIndex gate ──▶ ReadPageAsync for every page from 3 to the end of file
       (each takes store ioGate on the seek-and-read path) ──▶ release the gate
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
across the whole-file pass, and `store ioGate` across each read on the seek-and-read
path. The library only ever awaits them (`WaitAsync`, or the disposal drain)
and never blocks a thread on one, so a read queued in the kernel can delay the
operations waiting on those gates but cannot deadlock them. The writer opens
an overlapped handle.

When the read starts on a thread-pool thread with no synchronization context
and the default task scheduler, a path-opened reader runs it on that thread
instead of another pool thread (`PageFile.ReadsInlineOnThreadPool`). On
the seek-and-read path that thread holds `store ioGate` across the blocking read,
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
        │                                 → DatabaseFile.DisposeAsync (PageFile: store and its gate, journal gate,
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
| `frameGate` / store `ioGate` | No | Binary semaphores; never re-enter on the same flow |
| `IoGate` | No | Binary `SemaphoreSlim(1,1)` — re-entering on the same flow self-deadlocks; never hold it, or a `Pager.JournalGate` lease, across a `*PageAsync` call |
| `ByteRangeLock` per-page | No | OS advisory byte-range lock; re-locking the same range blocks |
| `insertPageHintLock` | No | Plain `lock`; leaf only |
| `ownedMapSetsLock` | No | Plain `lock`; leaf only |
| `ownedDataPagesCacheLock` | No | Plain `lock`; leaf only |
| `tdefBytesCacheLock` | No | Plain `lock`; leaf only |
| `aesGate` | No | Plain `lock`; leaf only |

## Writer-owned caches and the exclusive-writer assumption

The writer keeps some of what it reads from the file in memory for the rest of
the session, on the assumption that nothing else changes the file while it is
open:

| Cache | Owner | Dropped when |
|-------|-------|--------------|
| Plaintext page frames, bounded by `PageCacheSize` | `Pager` | CLOCK eviction; transaction attach/detach, failed writes and truncation invalidate all frames. Each write refreshes its retained frame. |
| User-table list and calculated result types | `TableCatalog` | `Invalidate`: every catalog write (create, drop or rewrite a table, add a catalog object), a rollback, a failed `UseTransactionalWrites` call and a failed commit. Each call also moves `TableCatalog.Generation` on. |
| Insert-page hint | `DataPageInserter` | Restored to its state at `BeginTransactionAsync` on a rollback; forgotten after a commit that fails during replay. |
| The TDEF pages whose owned-page maps the writer may extend, and those it found it may not | `CatalogOwnedMapPolicy` | Restored to their state at `BeginTransactionAsync` on a rollback. After a commit that fails during replay the writable set is restored and the refusals are forgotten. A table the writer creates drops a refusal for its TDEF page. |
| Constraint lists, AutoNumber `NextAutoValue` and complex-reference counters | `ConstraintRegistry` | Re-registered by schema changes; restored on a rollback. A commit that fails during replay keeps them, so counters never move back over values that may be on disk. |
| Enforced relationships, which every insert, update and delete checks | `RelationshipCatalogStore` | Every `MSysRelationships` write through the store (create, drop or rename a relationship, rename a key column), and whenever `TableCatalog.Generation` has moved on since the set was loaded, so a rollback or failed commit reloads it. |

All of these are memory-only, so restoring or dropping them takes no lock
beyond the leaf `insertPageHintLock`, `ownedMapSetsLock` and `frameSync`; the generation
counters use `Interlocked`.

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
3. Keep `insertPageHintLock`, `ownedMapSetsLock`, `ownedDataPagesCacheLock`, `tdefBytesCacheLock`,
   `frameSync` and `aesGate` as leaf locks: pure in-memory work, no `await` and no other lock acquired while held.
4. Physical writes take the per-page byte-range lock before the store gate; never hold the journal `IoGate` across store I/O.
5. The cross-process locks (`LockFileCoordinator` slot, `ByteRangeLock`
   commit-lock) are lifetime- or transaction-scoped, not per-page; do not fold
   them into the per-operation nesting.

## Why these are not consolidated

The audit suggested collapsing the insert-page cache lock (then a
`ReaderWriterLockSlim`), the `SemaphoreSlim`, and the operation gate behind one
async primitive. We deliberately keep them
separate because their responsibilities do not actually overlap:

- **The store `ioGate`** serializes *backing-stream I/O*. It must be an async-aware mutex
  because it is held across real `await`ed reads/writes/flushes.
- **`operationGate`** is a *reader-disposal drain*, not a mutex: it permits
  unbounded concurrent reentrant operations and only blocks *new* top-level
  operations once disposal starts. Merging it into the store gate would serialize
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
