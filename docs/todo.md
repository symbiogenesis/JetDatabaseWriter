None of the bugs from my earlier report is fixed at `HEAD` (6eab703). The five commits fixed the code's structure, not its behaviour. I re-ran my four repros against a fresh build of `HEAD` and all four still fail. Re-checking the code also turned up nine more data-loss or corruption bugs, which I ran myself. The test suite passes (3,832 passed, 29 DAO tests skipped, as usual), but no test covers any of these bugs.

## What the five commits fixed
- **The service-locator problem is gone.** Nothing outside the facades takes `AccessWriter`, `AccessReader` or `AccessBase` any more. `WriterServices` and `ReaderServices` now wire everything explicitly, the two dependency cycles are removed, and `ServiceGraphTests` guards this.
- **Duplicate code was merged:**
  - one `TableCatalog` replaces the two `GetUserTablesAsync` copies;
  - the shared index-descriptor helpers live on `IndexCatalogReader`;
  - the row primitives exist only in `TableRowStore`;
  - `AccessWriter` no longer has its own snapshot copies.
- **The row decoders no longer depend on the reader facade.** `RowDecoder` and `LongValueDecoder` now take `DatabaseFile`, and `ReaderPageCache` reads straight through to pending transaction writes when one is active. That makes the main fix below cheap.
- **The concurrency doc now gives the correct `UseTransactionalWrites` default.**
- **No new defects, apart from one small one.** A before/after comparison over all 92 test databases found identical results. The only change is that the "Total rows scanned" diagnostic now also counts catalog rows it can't decode (25 became 28 on one fixture). The cause is [TableCatalog.cs:119](JetDatabaseWriter/Catalog/TableCatalog.cs#L119), and it affects only the diagnostic text.

## Fixed since 6eab703
- **Bug 1: a large MEMO dropped the rows after it on the same data page.** One ~1.4 MB MEMO row plus 29 small rows made `Rows()` return 1 of 30 rows. `ReaderPageCache` gave evicted pages back to the shared pool while a scan still read them, so the next page read overwrote the scan's data page. It now leaves evicted pages to the GC, and `LruCache` no longer has an eviction callback. `AccessReaderCacheTests.Rows_WhenLongValueChainEvictsCurrentDataPage_ReturnsEveryRow` covers it.
  - Follow-up: the read-ahead guards in `TableReader.ShouldReadAheadTablePages` (a cache of at least 3 pages, and no MEMO, OLE or complex columns) worked around this bug. They can probably be relaxed now, after benchmarking.

## Open: data loss and corruption, reproduced by me at `HEAD`
| # | Scenario | Result | Where |
|---|---|---|---|
| 2 | In a transaction: delete Id=1, then update Id=2 | Row 3 lost, row 2 duplicated | [TableDataWriter.cs:226-229](JetDatabaseWriter/Tables/TableDataWriter.cs#L226-L229) |
| 3 | Any commit on a Jet4 `.mdb` | The file reopens as ACCDB | [TransactionLifecycle.cs:200](JetDatabaseWriter/Transactions/TransactionLifecycle.cs#L200) → [:278-290](JetDatabaseWriter/Transactions/TransactionLifecycle.cs#L278-L290) increments the format byte |
| 4 | Update a NOT NULL column to null | Accepted | [TableDataWriter.cs:250](JetDatabaseWriter/Tables/TableDataWriter.cs#L250) runs only the calculated-column checks |
| 5 | **New:** in a transaction, CreateTable + 3 inserts + AddColumn | **0 rows** after commit | [TableSchemaEditor.cs:401](JetDatabaseWriter/Tables/TableSchemaEditor.cs#L401) copies the table from a reader that can't see the transaction |
| 6 | **New:** in a transaction, Insert(4) + AddColumn on an existing table | Row 4 lost | same |
| 7 | **New:** create a relationship in a transaction, then insert a violating row | Accepted and committed | [RelationshipCatalogStore.cs:181](JetDatabaseWriter/Relationships/RelationshipCatalogStore.cs#L181) |
| 8 | **New:** `UseTransactionalWrites=true` with a cascade delete | The child table's primary-key and foreign-key indexes point to the wrong rows (seek 1 → `(2,8)`) | [IndexMaintainer.cs:432-434](JetDatabaseWriter/Indexes/IndexMaintainer.cs#L432-L434) |
| 9 | **New:** `InsertRowsAsync` cancelled partway | Rows stay on disk and the index doesn't contain them | [TableDataWriter.cs:578](JetDatabaseWriter/Tables/TableDataWriter.cs#L578) runs the cleanup with the cancelled token |
| 10 | **New:** insert in a transaction, roll back, insert again | `EndOfStreamException` | Rollback only drops the pending writes ([TransactionLifecycle.cs:241-243](JetDatabaseWriter/Transactions/TransactionLifecycle.cs#L241-L243)) and leaves the writer's other cached state stale |
| 11 | **New:** create and use a table in a transaction, then roll back | Inserting into it throws `EndOfStream`, and re-creating it says "already exists" | same: the table catalog isn't invalidated |
| 12 | `ReadTableAsync<T>` where `T` has an Attachment property | 0 rows, while `Rows<T>` returns 2 | [TableReader.cs:419-446](JetDatabaseWriter/Tables/TableReader.cs#L419-L446) |

Bugs 2, 5, 6, 7 and 8 have the same cause. The writer still reads its own file through a separate `AccessReader` that can't see the transaction's pending writes ([TableSnapshotReader.cs:100-118](JetDatabaseWriter/Tables/TableSnapshotReader.cs#L100-L118)). It then pairs those rows with disk locations by position. The new commits make that worse to fix later: [ServiceGraphTests.cs:49](JetDatabaseWriter.Tests/Architecture/ServiceGraphTests.cs#L49) and `library-structure.md:379` now record it as a "deliberate exception".

## Open: reproduced by the re-check agents (I didn't run these myself)
- **Wide tables skip unique checks.** On a table whose definition spans more than one page (200 columns, 30 indexes), a duplicate insert into a UNIQUE index is accepted. [UniqueIndexChecker.cs:37-46](JetDatabaseWriter/Indexes/UniqueIndexChecker.cs#L37-L46) reads only the first page.
- **RenameColumn changes types and loses foreign-key indexes.**
  - NUMERIC(10,2) becomes (18,0), so 12.34 is stored as 12.
  - On a child table, the foreign-key index is dropped and the parent's link still points to the freed page.
  - The code is at [TableSchemaEditor.cs:236-257](JetDatabaseWriter/Tables/TableSchemaEditor.cs#L236-L257).
- **Jet3 table headers are corrupted.**
  - The row count is written into the AutoNumber slot ([TDefPageBuilder.cs:486-488](JetDatabaseWriter/Schema/TDefPageBuilder.cs#L486-L488)).
  - The AutoNumber writer overwrites the table-type byte, which went from 0x4E to 0xBC after 700 inserts ([AutoNumberMaintainer.cs:71-78](JetDatabaseWriter/Schema/AutoNumberMaintainer.cs#L71-L78)).
- **Constraints:**
  - Rolling back an AddColumn or DropColumn switches off every check on that table, so NULL gets into AutoNumber and Required columns.
  - AutoNumber IDs are reused across sessions ([ConstraintRegistry.cs:382-424](JetDatabaseWriter/Schema/ConstraintRegistry.cs#L382-L424)).
  - Validation rules and default values stored in the file are never enforced.
  - Rules declared in code are lost when the writer is reopened.
  - Calculated expressions follow Excel precedence: `-2^2` = 4, and `5%` is accepted as 0.05.
- **Values:**
  - Updating an unrelated column strips a leading prefix from an OLE value.
  - `Rows()` merges two attachments into one blob that is still compressed.
  - Multi-value cells read as DBNull.
- **LINQ mapping:** `[Column]` is ignored. Inserting an entity with a renamed property writes DBNull, and `Where(p => p.LastName == "Smith")` matches nothing.
- **Encryption:**
  - Every open reads the whole file: 3.17 MB for a 3.17 MB file, and one `UpdateRowsAsync` reads about 5× the file size ([EncryptionManager.cs:555](JetDatabaseWriter/Encryption/EncryptionManager.cs#L555)).
  - On a flat-Agile file, the writer opens without error, appends 2 unencrypted pages, then fails with misleading errors.

## Design flaw status
| Flaw | Status | What changed |
|---|---|---|
| Facade is the engine / service locator | **Partly resolved** | Locator and cycles are gone. `DatabaseFile` (1,420 lines, used by about 33 types) is the new catch-all: pager, transaction journal, encryption and table-definition parsing in one class. |
| Reader and writer are separate engines | Open, critical | Moved into `TableSnapshotReader`. The reusable decoder now exists but the writer doesn't use it. |
| No pager or buffer manager | Open, critical | The cache moved into `ReaderPageCache`, and its eviction bug (bug 1) is now fixed. There are still several end-of-file definitions, and the writer still has no cache. |
| Transactions not atomic; format byte | Open, critical | Changes were mechanical, and the docs still claim before-image journaling. Rollback is now shown to break the writer (bugs 10–11). |
| Schema isn't a model | Open | Three duplicate parsers merged, the rest unchanged |
| No write pipeline or real B-tree | Open | Moved into `TableDataWriter` with the same algorithms |
| Constraints live in the process, not the file | Open | No change |
| Lossy value model | Open | No change |
| Format knowledge scattered | Open | No change. Files importing `JetTypeInfo` went from 35 to 40. |
| Read API doesn't compose | Open | Moved into `TableReader`. All the drift between the read loops is still there. |
| Encryption isn't a page layer | Open | No change |
| Failures aren't modeled | Open | `LastDiagnostics` is now built from a typed record internally |
| No CI; untested netstandard2.1 build | Open | No change. A Release build also fails on 155 analyzer errors in the test project. |
| LINQ layer has no mapping model | Open | It now depends on reader services instead of the facade, which makes a package split easier |

## Suggested next steps
1. **Make the writer's reads see the transaction.** This is now small: build `TableSnapshotReader` from a `ReaderServices` over the writer's own `DatabaseFile`, with a capacity-0 `ReaderPageCache`. That should fix bugs 5–8, and likely bug 2 as well. Pairing rows to locations by position stays fragile until a cursor returns each row with its location. Remove the allow-list entry and the doc's "deliberate exception" at the same time.
2. **Rollback should reset writer state.** Inject `TableCatalog`, `DataPageInserter` and `ConstraintRegistry` into `TransactionLifecycle`, and invalidate or restore them on rollback (fixes bugs 10–11 and the disabled-constraints problem).
3. **One-line and small fixes:**
   - delete `BumpCommitLockByteAsync` and the test that asserts it (bug 3);
   - run the full constraint pass on update (bug 4);
   - run insert cleanup with `CancellationToken.None` (bug 9);
   - read only page 0 to detect encryption;
   - refuse to open flat-Agile files in the writer;
   - keep every column property in RenameColumn.
4. **Add CI** that runs the tests on every PR. Each repro above should become a failing test first; the programs are in `scratchpad\recheck\*` and `scratchpad\repro2`/`repro3`.

The per-claim results, with current file:line evidence and fix notes for all 14 flaws, are in `scratchpad\recheck.json` and `scratchpad\recheck-digest.md`.
