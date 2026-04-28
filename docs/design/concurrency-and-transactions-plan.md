# Concurrency, Lockfile Slots, and Transactions — Design Plan

> Status: **Phases 1 & 2 shipped; Phase 3 proposed.**
> Scope: address the three "Concurrency" bullets in the README's Limitations
> section by matching the **default behavior of Microsoft Access** as closely
> as a managed-only, OS-portable library reasonably can.

---

## 1. Goals & non-goals

### Goals

1. **Coexist safely with Microsoft Access**, the JET/ACE OLE DB provider, and
   other instances of `AccessReader` / `AccessWriter` against the same
   `.mdb` / `.accdb` file:
   - When Access has the file open, `AccessWriter.OpenAsync` should fail fast
     (or, for read-only, succeed silently as Access does).
   - When `AccessWriter` has the file open, Access should refuse to open it
     for write and should report it as in-use, exactly as it does for two
     concurrent Access processes.
2. **Populate `.ldb` / `.laccdb` slots** with a 64-byte machine-name / user
   record so Access's "Who has this database open?" surface (and `LDBView`)
   shows our process. This is the single most-visible interop gap.
3. **Add page-level byte-range locking** on Windows via
   `LockFileEx`/`UnlockFileEx`, mirroring the JET locking protocol used by
   `msaccess.exe` (commit-time locks for writers; share locks for readers).
4. **Add ACID-ish transactions**: an in-memory journal of dirty pages with
   atomic commit and crash-safe rollback. Match Access's implicit
   auto-commit-per-statement default while also exposing an explicit
   `BeginTransactionAsync()` API.

### Non-goals

- A full multi-version concurrency control engine. Access itself is
  page-locking, not MVCC; we follow suit.
- Cross-platform byte-range locking parity. The JET locking protocol is
  Windows-specific (it relies on `LockFileEx` semantics and on Access's own
  conventions). On non-Windows we fall back to the lockfile + advisory
  `FileShare` only and document the gap.
- Replacing the `.ldb` lockfile design. JET's lockfile is intentionally
  unsynchronized (every opener appends a slot non-atomically); we replicate
  that, warts and all, instead of inventing a stricter protocol that Access
  would not honour.

---

## 2. Where we stand today

| Concern | Current state | Files |
|---|---|---|
| `.ldb` / `.laccdb` creation | **Phase 1 shipped.** Lock-file is opened with `FileShare.ReadWrite \| FileShare.Delete`, the owning `FileStream` is held for the lifetime of the connection, and the file is removed only by the last opener. `RespectExistingLockFile = true` rejects opens when one exists. | [JetDatabaseWriter/Internal/LockFileSlotWriter.cs](../../JetDatabaseWriter/Internal/LockFileSlotWriter.cs), [JetDatabaseWriter/Internal/LockFileManager.cs](../../JetDatabaseWriter/Internal/LockFileManager.cs) |
| `.ldb` slot entries | **Phase 1 shipped.** Each opener claims the first empty 64-byte slot and writes a `<machine>(32)<user>(32)` ASCII record (overridable via `LockFileMachineName` / `LockFileUserName` on both option types). 255-slot cap throws `IOException`. | [JetDatabaseWriter/Internal/LockFileSlotWriter.cs](../../JetDatabaseWriter/Internal/LockFileSlotWriter.cs) |
| Byte-range page locks | **Phase 2 shipped (Windows).** Per-page exclusive ranges acquired via Win32 `LockFile` immediately before each `WritePage[Async]` / `AppendPage[Async]` and released after flush. Default `UseByteRangeLocks = true` on Windows for the writer (false on the reader; opt-in there). Backed by [JetDatabaseWriter/Internal/JetByteRangeLock.cs](../../JetDatabaseWriter/Internal/JetByteRangeLock.cs). No-op on non-Windows or when the underlying stream is not a `FileStream`. | [JetByteRangeLock](../../JetDatabaseWriter/Internal/JetByteRangeLock.cs), [AccessBase.WritePage / AppendPage](../../JetDatabaseWriter/Core/AccessBase.cs) |
| Page write path | Each `WritePage[Async]` seeks, writes one page, flushes immediately. No batching, no journal. | [AccessBase.WritePage](JetDatabaseWriter/Core/AccessBase.cs#L819) |
| Transactions / rollback | None. Operations apply page-by-page; a crash mid-`InsertRowsAsync` leaves a partially-mutated database. | — |

The existing `_ioGate` `SemaphoreSlim` in `AccessBase` already serializes
intra-process page I/O, which gives us a clean place to hang transaction
boundaries and lock acquisition.

---

## 3. Background — what Access actually does

Distilled from [mdbtools `HACKING.md`](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md),
the published Office OLE DB / JET docs, MS-ACCDB §3.5 ("Database File Header
and Locking"), and the `LDBView` reference utility.

### 3.1 The `.ldb` / `.laccdb` lockfile

- File extension: `.ldb` for `.mdb`/`.mde`, `.laccdb` for `.accdb`/`.accde`.
- Created with `FileShare.ReadWrite` (sometimes `FileShare.Delete`) so every
  opener can append a slot and so the last closer can delete it.
- Format: a **flat array of 64-byte slots**, no header, no count. Each slot:
  - Bytes `0x00..0x1F`: 32 bytes — **computer name** (NetBIOS / hostname),
    null-padded ASCII.
  - Bytes `0x20..0x3F`: 32 bytes — **user / security name** (Access uses the
    Win32 `GetUserNameExA(NameSamCompatible, …)` value, truncated; for the
    JET workgroup model it is the JET user name).
- An opener finds the first slot whose computer-name byte is `\0`, fills both
  fields, and writes the 64 bytes back. No deletion on close — slots are
  reused opportunistically by the next opener.
- Hard cap: **255 slots** (~16 KiB). Hitting the cap is what surfaces
  Access's "Cannot open the database. The database has been opened by
  another user, or you have reached your limit of concurrent users" error.
- Two well-known bytes in the **database header itself** mirror the locking
  state:
  - Page 0 byte `0x14` ("commit lock byte") — incremented on every
    schema-changing commit so other openers can detect they need to refresh
    the catalog.
  - Page 0 byte `0x15` ("commit lock byte 2") — paired counter.

### 3.2 Byte-range page locks (`LockFileEx`)

JET overlays a logical lock map onto the database file using
`LockFileEx`/`UnlockFileEx`:

- **Page lock region.** A page-sized lock at offset `pageNumber * pageSize`
  gates writes to that page. Writers acquire an exclusive page lock for the
  duration of the page mutation; readers acquire a shared lock for the
  duration of the read of pages the writer might be touching.
- **Commit lock region.** A 1-byte exclusive lock at a fixed sentinel offset
  (historically `0xFFFFFFFE` for Jet3/Jet4; `0xFFFFFFFC` for ACE) gates
  schema commits and the increment of the commit-lock byte at `0x14`.
- **Open / "you can't have it" sentinel.** A 1-byte exclusive lock at a
  separate sentinel that an exclusive-mode opener acquires for the lifetime
  of the connection. This is what makes "Open Exclusive" in Access reject
  other openers without relying on `FileShare.None`.
- All of the above are cooperative — they only matter against other openers
  that follow the same protocol. Access does, the OLE DB JET provider does,
  and the ACE engine does. Adding the protocol here is what closes the
  Access ↔ this-library coexistence gap.

### 3.3 Transactions

JET uses **page-level shadowing**. Mutations are buffered in memory; on
`CommitTrans` the engine writes new pages, flushes, then bumps the commit
lock byte. A crash before commit leaves the on-disk pages untouched (because
nothing was written) — there is no separate write-ahead log file.

---

## 4. Proposed design

The work splits cleanly into three independent, opt-in phases that can each
ship on its own. Phase 1 unlocks the most-asked-for interop (Access shows
this library as an opener); Phase 3 is the largest change and is gated
behind an explicit option until it has bake time.

### Phase 1 — Populated `.ldb` / `.laccdb` slots — **Shipped**

- New internal type: `LockFileSlotWriter` in
  `JetDatabaseWriter/Internal/LockFileSlotWriter.cs`.
- API surface (instance, owned by `AccessBase`):
  - `Open(databasePath, ownerType, respectExisting)` — opens
    (or creates) the lockfile with `FileShare.ReadWrite | FileShare.Delete`,
    finds the first empty slot, writes our 64-byte record, and **keeps the
    `FileStream` open for the lifetime of the connection** so other openers
    see the file as in-use even if delete-on-close races.
  - `Dispose()` — best-effort: zero our slot in place, then close the
    stream. The last opener also attempts to delete the file (matching
    Access's behavior) — if the delete fails because another opener still
    has a handle, swallow.
- Slot record:
  - Computer name: `Environment.MachineName`, ASCII, truncated to 31 chars
    (leaving the trailing null).
  - User name: on Windows, `Environment.UserName` (the Win32
    `NameSamCompatible` form is overkill for non-workgroup files and would
    pull `Microsoft.Win32` interop into `netstandard2.1`); on other OSes,
    `Environment.UserName` likewise. Truncate to 31 chars.
- Encoding: ASCII (`Encoding.ASCII.GetBytes`), zero-pad. Non-ASCII chars
  are replaced with `?` to match Access's behaviour rather than throwing.
- "Slot full" handling: if all 255 slots are populated, behave like Access
  and throw `IOException("The database has been placed in a state by user
  '%s' on machine '%s' that prevents it from being opened…")`. The message
  text is informational; the type is what matters for callers.
- `AccessReaderOptions` / `AccessWriterOptions` gain:
  - `LockFileUserName` (`string?`, default `null` → `Environment.UserName`).
  - `LockFileMachineName` (`string?`, default `null` → `Environment.MachineName`).
  These let server-side hosts attribute slots to the logical end-user
  rather than the service account.

#### Tests
- Add `LockFileTests.Reader_PopulatesSlot_WithMachineAndUserName`.
- Add `LockFileTests.MultipleReaders_AppendDistinctSlots`.
- Add `LockFileTests.Slot_IsZeroedOnDispose`.
- Add `LockFileTests.SlotCap_255Openers_Throws`.
- Override-name test: custom `LockFileUserName` round-trips into bytes 0x20..0x3F.

#### Risks / mitigations
- **Encoding mismatch.** Access's exact code page for slot names is the
  active OEM code page on the writing machine. ASCII is a safe subset that
  every viewer (including `LDBView`) renders identically; document that
  non-ASCII names are mangled and require an explicit override.
- **Dispose race.** Two openers each see `slotCount == 0` and try to delete
  the file. Tolerate `FileNotFoundException` and `IOException` on delete.

---

### Phase 2 — Page-level byte-range locks (Windows-only) — **Shipped**

- New internal type: `JetByteRangeLock` in
  [JetDatabaseWriter/Internal/JetByteRangeLock.cs](../../JetDatabaseWriter/Internal/JetByteRangeLock.cs).
  Wraps Win32 `LockFile` / `UnlockFile` (chosen over `LockFileEx` to avoid
  the overlapped-IO completion path, which corrupts memory when the underlying
  `FileStream` was opened with `FileOptions.Asynchronous` because the kernel
  posts a completion against a stack-allocated `NativeOverlapped`).
- Public methods are no-ops on non-Windows hosts and when the underlying
  `Stream` is not a `FileStream` (e.g. the in-memory `MemoryStream` used by
  the Agile-encrypted ACCDB rewrap path).
- `AccessWriterOptions.UseByteRangeLocks` defaults to `true` on Windows,
  `false` elsewhere. `AccessReaderOptions.UseByteRangeLocks` defaults to
  `false` (readers don't need to participate in JET page locking unless they
  want fully-consistent reads against a concurrent writer that obeys the
  protocol).
- Both option types expose `LockTimeoutMilliseconds` (default `5_000`,
  matches JET's "Object is currently in use" timeout).
- Acquisition uses a poll loop: `LockFile` (which fails immediately on
  contention) followed by `Task.Delay(20)` / `Thread.Sleep(20)` until the
  configured timeout elapses, then `IOException` with the page number in
  the message.
- Wired into `AccessBase.WritePage[Async]` / `AppendPage[Async]` immediately
  after entering `_ioGate`. The lock is released before the gate is
  released. `ReadPageAsync` deliberately does **not** take a lock —
  matching Access reader behavior.
- The commit-lock and open-exclusive sentinels described in the original
  plan are **deferred to Phase 3 / a follow-on `OpenExclusive` option**.
  Page-level locks alone are what closes the per-write coexistence gap.

### Phase 2 — Page-level byte-range locks (Windows-only) — original plan

- New internal type: `JetByteRangeLock` in
  `JetDatabaseWriter/Internal/JetByteRangeLock.cs`. Wraps `LockFileEx`
  /`UnlockFileEx` via `LibraryImport` against `kernel32.dll`. Returns `null`
  / no-op on non-Windows (`OperatingSystem.IsWindows() == false`).
- New `AccessWriterOptions.UseByteRangeLocks` (default **`true` on
  Windows`**, `false` elsewhere). Same toggle on `AccessReaderOptions` for
  symmetry; default `false` (readers don't need it as long as the writer
  obeys the protocol — Access readers don't take page locks either).
- Lock regions used by JET (constants encoded in
  `JetByteRangeLock.Sentinels`):
  - **Page lock**: offset `pageNumber * pageSize`, length `pageSize`,
    exclusive — held only across `WritePage[Async]` / `AppendPage[Async]`.
  - **Commit lock**: offset `0xFFFFFFFE` (Jet3/Jet4) or `0xFFFFFFFC` (ACE),
    length 1, exclusive — held while bumping the commit-lock byte at
    page 0 offset `0x14` and while flushing schema-changing transactions.
  - **Open-exclusive sentinel**: offset `0xFFFFFFFD`, length 1, exclusive —
    optional, taken when a future `AccessWriterOptions.OpenExclusive = true`
    is set (out of scope for the first cut).
- Wire into `AccessBase.WritePage[Async]` / `AppendPage[Async]`:
  - Acquire the page lock immediately after entering `_ioGate`.
  - Release before exiting the `try`/`finally`.
  - Failure to acquire after the configured `LockTimeoutMilliseconds`
    (default 5,000, matches JET's "Object is currently in use" path) throws
    `IOException` with the page number embedded in the message.
- Wire into the future commit path (Phase 3): commit lock is acquired once
  per `Commit`, released after the page-0 byte is flushed.

#### Tests
- Marked `[Trait("Category", "RequiresWindows")]` and skipped on
  non-Windows. Use `xunit.v3` `[Fact(Skip="…")]` with `SkipWhen` predicate.
- `Two_Writers_Cannot_Take_Same_Page_Lock_Concurrently` — opens two
  `AccessWriter` instances against the same path, has the first start a
  long write, asserts the second's `WritePageAsync` throws within
  `LockTimeoutMilliseconds + ε`.
- `Reader_With_BoundsLocking_Off_Can_Read_While_Writer_Holds_PageLock`
  (default config — readers don't take locks, so this should always pass
  with stale data tolerated).

#### Risks / mitigations
- **P/Invoke shape.** `LockFileEx` is async-completion via `OVERLAPPED`.
  Use the synchronous-blocking variant (no `LOCKFILE_FAIL_IMMEDIATELY`)
  with a manual timeout via a separate `ThreadPool` task and
  `UnlockFileEx`-from-cancellation approach, **or** use
  `LOCKFILE_FAIL_IMMEDIATELY` in a poll loop with `Task.Delay(20)` between
  attempts. Prefer the latter for portability and simpler error semantics.
- **`netstandard2.1` P/Invoke.** `LibraryImport` is a `net7+` source
  generator; multi-target with classic `[DllImport]` for `netstandard2.1`.
- **Stream from caller.** When the writer was opened from a non-`FileStream`
  (e.g. `MemoryStream`), there is no Win32 handle and locks are skipped.
  Document.

---

### Phase 3 — Transactions and rollback

- New types in `JetDatabaseWriter/Internal/Transactions/`:
  - `PageJournal` — keyed by page number, stores
    `(originalPageBytes, currentPageBytes, isAppended, isFreed)`.
  - `Transaction` — implements `IAsyncDisposable`. Buffers all
    `WritePage`/`AppendPage` calls into the journal instead of touching the
    file. Exposes `CommitAsync()` and `RollbackAsync()`.
- New `AccessWriter` API:
  - `ValueTask<Transaction> BeginTransactionAsync(CancellationToken)`.
  - All existing mutation methods participate in either an explicit
    transaction (if one is active) or an implicit one that auto-commits at
    method exit. Implicit-mode behavior matches today's flush-per-page
    semantics from the caller's perspective.
- Commit algorithm (mirrors JET's page-shadow approach):
  1. Acquire commit lock (Phase 2).
  2. For each appended page: write to its allocated slot at end-of-file.
  3. For each modified page: write in-place.
  4. `_stream.Flush(flushToDisk: true)` (managed `FileStream.Flush(true)`).
  5. Increment page-0 byte `0x14` (and `0x15` for schema commits) to signal
     other openers; flush.
  6. Release commit lock.
- Rollback: discard the journal; nothing on disk has changed because step 2
  hasn't run yet. (Because we never write before commit, there is no need
  for a separate rollback journal file. Trade-off: the journal lives in
  process memory — bound it with
  `AccessWriterOptions.MaxTransactionPageBudget`, default 16,384 pages
  (~64 MiB at `pgSz=4096`). Exceeding the budget throws
  `JetLimitationException` and the transaction is rolled back.)
- Crash safety: if the process dies mid-commit, the on-disk file is in one
  of two states:
  - Pre-step-5 — pages may be written but page-0 commit byte is unchanged;
    Access still sees the previous catalog version. This is the same
    "torn write" window Access itself has and is acceptable.
  - Post-step-5 — fully committed.

#### Auto-commit semantics

Default behaviour (no explicit transaction) **must remain backward
compatible**: callers that do not opt into transactions see identical
crash-window behavior to today. Implementation: implicit auto-commit wraps
each public mutation call in a `using var tx = …; await tx.CommitAsync();`
internally, but the implicit mode flushes per page (today's behavior) when
`AccessWriterOptions.UseTransactionalWrites = false` (default in the first
release; flip to `true` in a later major).

#### Tests
- `Transactions/CommitWritesAllPagesAtomically`.
- `Transactions/RollbackLeavesFileUnchanged` — write, dispose without
  commit, reopen, assert original bytes.
- `Transactions/CrashBeforeCommitLeavesOriginal` — simulate by aborting
  the in-flight commit between step 4 and step 5 with a hook.
- `Transactions/JournalBudgetExceeded_Throws`.
- `Transactions/CompactAndRepairAfterCommit_RoundTripsThroughAccess`
  (manual / `[Trait("Category", "RequiresMsAccess")]`).

#### Risks / mitigations
- **Memory pressure.** Large `InsertRowsAsync` jobs could blow the budget.
  Document the budget; recommend chunking (and the chunked path can
  internally commit between chunks).
- **Interaction with the page cache (`AccessReaderOptions.PageCacheSize`).**
  Writes within an open transaction must update the page cache so reads
  inside the same transaction see uncommitted writes; on rollback we must
  invalidate any cached pages we touched. Add a transaction-scoped
  shadow-cache layered over the existing LRU cache.
- **Encryption.** `PrepareEncryptedPageForWrite` already happens just
  before each `Write`, so commit-time encryption is unchanged — encrypt at
  step 2/3, not when buffering.

---

## 5. Public API additions

```csharp
// JetDatabaseWriter/Core/AccessWriterOptions.cs
public sealed class AccessWriterOptions
{
    // … existing members …

    // Phase 1
    public string? LockFileUserName { get; init; }
    public string? LockFileMachineName { get; init; }

    // Phase 2
    public bool UseByteRangeLocks { get; init; } = OperatingSystem.IsWindows();
    public int LockTimeoutMilliseconds { get; init; } = 5_000;

    // Phase 3
    public bool UseTransactionalWrites { get; init; } // default false in first release
    public int MaxTransactionPageBudget { get; init; } = 16_384;
}

// JetDatabaseWriter/Core/AccessReaderOptions.cs — Phase 1 only
public sealed class AccessReaderOptions
{
    public string? LockFileUserName { get; init; }
    public string? LockFileMachineName { get; init; }
}

// JetDatabaseWriter/Core/AccessWriter.cs — Phase 3
public partial class AccessWriter
{
    public ValueTask<JetTransaction> BeginTransactionAsync(CancellationToken ct = default);
}

// JetDatabaseWriter/Core/JetTransaction.cs — Phase 3
public sealed class JetTransaction : IAsyncDisposable
{
    public ValueTask CommitAsync(CancellationToken ct = default);
    public ValueTask RollbackAsync(CancellationToken ct = default);
    public ValueTask DisposeAsync(); // rollback if not committed
}
```

No existing API breaks. All defaults preserve current observable behaviour
except Phase 1 (lockfile slot is now populated — observable via file size,
not via behaviour).

---

## 6. README updates

After each phase ships, edit
[README.md → Limitations → Concurrency](README.md#L660-L662):

- Phase 1 done → strike "no populated `.ldb` slots".
- Phase 2 done → strike "No byte-range locking" (qualify: "Windows only").
- Phase 3 done → strike "No transactions or rollback" and add the
  `BeginTransactionAsync` example to the "Writing Data" section.

---

## 7. Phasing & ordering

| Phase | Status | Effort (rel.) | Risk | User-visible win |
|---|---|---|---|---|
| 1 — Populated lockfile slots | **Shipped** | S | Low | Access ↔ library opener visibility |
| 2 — Byte-range page locks | **Shipped** | M | Med (Windows P/Invoke, edge cases) | Safe coexistence with Access at the page level |
| 3 — Transactions / rollback | Proposed | L | Med-High (journal, cache invalidation, crash matrix) | Atomic commit, crash safety |

Recommend implementing strictly in this order; Phase 2's commit-lock plumbing
becomes the foundation for Phase 3's commit step, and Phase 1's slot record
gives us a way to test that Access actually sees us as an opener before we
take on the harder phases.

---

## 8. Open questions

1. **OEM vs ASCII vs UTF-8 in `.ldb` slots.** Worth probing a real Access
   `.laccdb` written by an account whose name contains non-ASCII characters
   to confirm Access still uses OEM. Until then, ASCII subset.
2. **Should readers also populate slots by default?** Access's reader does.
   Recommendation: yes — symmetric Phase 1 behaviour for `AccessReader`.
3. **`OpenExclusive` mode.** Access has it; we don't. Worth exposing in
   Phase 2 as `AccessWriterOptions.OpenExclusive = true` (takes the
   sentinel lock, refuses any further opener).
4. **Cross-process `.ldb` cleanup on the last opener.** Access's behavior
   here is famously inconsistent; tolerate either outcome rather than
   forcing a delete.
