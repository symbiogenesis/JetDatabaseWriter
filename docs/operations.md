# Opening, configuring and operating databases

[Back to the README](../README.md) · [Reading guide](reading.md) · [Writing guide](writing.md)

This guide covers stream ownership, configuration, transactions and the limits that matter when operating on real files. Examples use types from `JetDatabaseWriter`, `JetDatabaseWriter.Models` and `JetDatabaseWriter.Enums`. For supported encrypted formats, see [Encryption Support](../README.md#encryption-support).

## Contents

- [Runtime compatibility](#runtime-compatibility)
- [Opening files and streams](#opening-a-reader-or-writer)
- [Transactions](#transactions)
- [Trimming and NativeAOT](#trimming-and-nativeaot)
- [Configuration](#configuration)
- [Error handling](#error-handling)
- [Encryption options](#encryption-options)
- [Detailed limitations](#limitations)

## Runtime compatibility

The package ships two builds. Apps on .NET 10 or later get the `net10.0` build; apps on .NET Core 3.x and .NET 5 through 9 get the `netstandard2.1` build, which brings in `System.Linq.Async`, `System.ComponentModel.Annotations` and `System.Text.Encoding.CodePages` for the APIs .NET 10 has built in. The test suite runs the native build on .NET 10 and .NET 11, plus a separate .NET 10 lane that loads the `netstandard2.1` build. Hosted ordinary tests exercise these three runtime/library combinations; compatibility with the older supported runtimes is not equivalent to a completed test run on each. The `net10.0` build can read eligible file pages with `RandomAccess`; the `netstandard2.1` build uses stream I/O. Trimming/NativeAOT evidence covers the .NET 10 consumer paths below. Unix maintenance-file creation permissions also differ between the builds; see [encryption options](#encryption-options).

## Opening a Reader or Writer

### From a file path

Choose a reader for reads or a writer for changes. These are separate sessions: dispose the reader before opening a writer on the same file.

```csharp
await using var reader = await AccessReader.OpenAsync("database.mdb", cancellationToken: cts.Token);
```

```csharp
await using var writer = await AccessWriter.OpenAsync("database.mdb");
```

A reader opened from a path reads each page on the calling thread when that thread is a thread-pool thread with no synchronization context, which is where most `await` continuations run in console, ASP.NET Core and worker apps; in our measurements that made each page read about twice as fast as handing it to another thread. Elsewhere, such as on a UI thread, page reads stay off the calling thread. Cancellation is checked before every page read, so a cancelled scan stops at its next page, but a page read that has started runs to completion.

### From a Stream

Both `AccessReader` and `AccessWriter` accept any seekable `Stream` — useful for byte arrays, Azure Blob Storage, embedded resources, or HTTP downloads.

```csharp
byte[] bytes = await File.ReadAllBytesAsync("database.mdb");
var ms = new MemoryStream(bytes);
await using var reader = await AccessReader.OpenAsync(ms);
```

By default the stream is disposed with the reader/writer. Pass `leaveOpen: true` to retain ownership:

```csharp
using var ms = new MemoryStream();
await ms.WriteAsync(await File.ReadAllBytesAsync("template.mdb"));
ms.Position = 0;
await using (var writer = await AccessWriter.OpenAsync(ms, leaveOpen: true))
{
    await writer.InsertRowAsync("Orders", new object[] { 1, "Widget", 9.99m });
}

byte[] modified = ms.ToArray();
```

> The stream must be readable and seekable. For `AccessWriter`, it must also be writable.

Give a writer exclusive access to the stream and underlying bytes until disposal; keep a reader's source unchanged. A non-`FileStream`, including a wrapper around one, has live rollback only. `leaveOpen` controls disposal ownership and does not enable persistent recovery. See the [caller-stream contract](design/caller-stream-recovery.md) for resize/flush requirements, failure ownership and reopen limits.

If you open a `FileStream` yourself for `AccessReader`, open it without `FileOptions.Asynchronous`. The reader reads every page through the stream, and on Windows, in our measurements, a page read on an overlapped handle took about twice as long as one on a synchronous handle, even when the page was already in the OS cache. `AccessReader.OpenAsync(path)` opens its file that way.

## Transactions

Row, table-schema, relationship and complex-item mutations are statement-atomic by default. Each call buffers a bounded batch of changed pages and spills it when it reaches `max(64, PageCacheSize / 2)` pages. A work-phase failure, cancellation or write-back failure restores the call. File-backed stores use a persistent hash manifest and selective raw before-images as their single source of original stored bytes and file length for both live rollback and crash recovery. Live rollback validates the manifest and original images and restores only captured pages and any removed tail; encrypted bytes remain encrypted. The same rollback owner retains raw before-images in memory for other stores. File-backed statement commits always flush the journal and database to the device. For other stores, `UseTransactionalWrites = true` additionally requests a durable flush when supported. Explicit transactions group several calls into one durable commit and remain subject to `MaxTransactionPageBudget`. Database creation, physical tail shrinking and encryption replacement have separate lifecycles. Physical shrinking restores write, erase, truncate and flush failures from the same selective persistent journal for files, or from bounded, spillable raw tail before-images for other streams; failed undo faults the writer. File-backed shrinking records flushed truncation intents for recovery after process termination. A successful shrink records its final hash before journal cleanup.

`AccessWriter` supports explicit page-buffered transactions for multi-row/page operations. All page mutations are buffered in memory until committed or rolled back.

```csharp
await using var tx = await writer.BeginTransactionAsync();
await writer.InsertRowAsync("Contacts", new object[] { 7, "Grace", "grace@example.com", 90.0m });
await writer.UpdateRowsAsync("Contacts", "ContactID", 2, new Dictionary<string, object?> { ["Score"] = 93.5m });
await tx.CommitAsync(); // Replays all buffered pages and flushes the stream
```

Disposing an uncommitted transaction rolls back its scope, including cached table metadata, constraints and AutoNumber counters. Repeated `BeginTransactionAsync` calls nest up to seven explicit levels, matching DAO. Resolve the innermost level first: its commit merges changes into its parent, while its rollback or disposal restores only that level. An outer rollback also undoes committed inner work. Only the outermost commit writes and flushes the database. Disposing the writer rolls back every open level.

Each mutation inside an explicit transaction has an internal savepoint. If the call fails, including cancellation or exceeding `MaxTransactionPageBudget`, its page changes, allocated pages, table metadata and AutoNumber counters are restored to their state before the call. Earlier successful calls stay in the transaction, which remains usable. As in DAO, mutations are permitted with at most five explicit levels open. At level six or seven they fail before changing state with `TransactionNestingLimit`; resolve inner levels before writing. All levels share the outer transaction's distinct-page budget; savepoint before-images and cached-state snapshots consume additional memory at each level. Mutations, commit, rollback and disposal serialize on each writer. A callback that tries to mutate the same writer throws with `ReentrantWriterCall`.

The outermost `CommitAsync` captures the original bytes of pages it will overwrite, then writes the new pages in page-number order and flushes the stream. That transaction ends whether or not its physical commit succeeds:

- If it fails before the first page write starts, the file is unchanged and `IsRolledBack` is `true`, unless preparing the persistent journal faults the writer.
- Once the first page write starts, cancellation is ignored through replay and recovery.
- If a stream write or flush fails before recording the commit decision, commit restores the original page bytes and file length. Successful recovery sets `IsRolledBack` to `true`, restores the writer's cached state and AutoNumber counters, and surfaces the original failure. The writer remains usable.
- If recovery also fails, both `IsCommitted` and `IsRolledBack` remain `false`. Further mutations, transaction starts and maintenance throw with `WriterFaulted`; disposal releases resources without flushing or rewriting the file. For a file-backed transaction, retain its recovery journal and reopen with `AccessWriter`; if validation refuses recovery, retain both files and restore from a known-good copy. Custom streams need caller-managed restoration.

File-backed transactions also create an adjacent `<database>.jdw-journal` before their first physical write. It contains a checksum-protected SHA-256 manifest of every original page, selective original images and checksummed write intents, including ciphertext for encrypted pages. Each intent is flushed before its database write. This covers bounded statement spills, explicit commits and physical shrinking; shrink saves every removed original page before recording its intended retained length and truncating. Successful commits flush the database before recording the commit decision. Initial manifests are prepared in `.jdw-journal.preparing` and promoted only after flushing; an abandoned preparation file never authorizes new writes. Readers refuse surviving preparation evidence; writer open validates it before recovery or cleanup and retains malformed or conflicting evidence.

After a process crash, open the database with `AccessWriter.OpenAsync` at its original path. Recovery runs before header parsing or password validation, checks the journal and all current database bytes, and either restores the exact original bytes and length or retains a verified committed image. Readers refuse a pending journal. Invalid or conflicting journal data is refused without replay; retain both files for diagnosis. A failed commit-decision flush leaves an uncertain outcome and faults the writer instead of overwriting a possibly committed result. Reopening resolves a valid journal's decision. Abandoned Access lockfiles are removed after successful recovery only if they can be opened exclusively. If lockfile cleanup fails, the journal retains the completed recovery decision so writer open can be retried after the obstruction is removed.

Preparation reads the entire database with bounded buffers but writes only 32 bytes per original page plus a 128-byte header. Each overwritten or removed original page adds one saved raw image; repeated writes add only new intents. Commit still hashes the resulting file. Allow disk space for the manifest, saved pages and write records; grouping changes in an explicit transaction amortizes preparation and commit hashing. Databases are bounded to 2 GiB and journals to 16 GiB. Recovery may scan the intent log repeatedly for changed pages.

Keep the journal beside the database at its original path until recovery completes. Microsoft Access does not understand this sidecar: recover with the library before opening an interrupted database in Access. Do not rename, replace, edit or copy the database independently of a pending journal. Identity validation uses the full path and raw contents; it is not a persistent operating-system file identifier or authentication against a maliciously replaced journal.

These guarantees cover process termination under exclusive file access. File and journal flushes do not establish journal directory-entry persistence across power loss. A non-`FileStream` caller stream has only in-process rollback; no persistent journal-stream API is available. Creation, physical shrinking and container replacement retain their separate guarantees. See the [recovery protocol](design/transaction-crash-recovery.md) for ordering and limits.

## Trimming and NativeAOT

On .NET 10, typed inserts, `Rows<T>` (including predicates),
`ReadTableAsync<T>` and typed index reads preserve the mapped public properties
and parameterless constructor needed by POCO and scaffold-generated models.
Generic helper methods wrapping these APIs must propagate their
`DynamicallyAccessedMembers` requirements; follow linker diagnostics rather than
suppressing them. `Query<T>` and its `Include` pipeline depend on runtime type
discovery and generic construction and require an untrimmed JIT application.
They carry `RequiresUnreferencedCode` and `RequiresDynamicCode` annotations.

CI runs `scripts/test-aot.ps1` to generate classes and records, then publish and
execute each as a trimmed and NativeAOT Windows x64 consumer. The checks cover
column-name mapping, nullable values, typed insert/read, streaming and predicate
reads, and compilation of generated relationship navigations. This evidence
covers the .NET 10 typed API paths exercised by that consumer.

---

## Configuration

Reader and writer examples are alternatives; dispose the reader before opening a writer on the same file.

```csharp
var options = new AccessReaderOptions("secretPassword")
{
    PageCacheSize            = 512,    // pages in LRU cache (default: 256)
    PageReadOptimizationMode = PageReadOptimizationMode.Auto, // random-access/read-ahead policy (default)
    DiagnosticsEnabled       = false,  // verbose logging (default: false)
    MaxComplexDiscoveryEntries = 65_536, // aggregate metadata work per complex-column discovery
    MaxAttachmentContentBytes = 64 * 1024 * 1024, // uncompressed content per attachment
    MaxAttachmentContentBytesPerOperation = 256L * 1024 * 1024, // shared across one attachment read/table scan
    ValidateOnOpen           = true,   // format check on open (default: true)
    FileAccess               = FileAccess.Read,        // default
    FileShare                = FileShare.Read,         // default; path readers retain read-only sharing
    UseLockFile              = true,   // create .ldb/.laccdb lockfile (default: true)
    // LinkedSourceResolver must supply an authorized stream to enable linked reads.
};
await using var reader = await AccessReader.OpenAsync("database.mdb", options);

var writerOptions = new AccessWriterOptions("secretPassword")
{
    UseLockFile = true,              // create .ldb/.laccdb lockfile (default: true)
    RespectExistingLockFile = true,  // refuse an existing lockfile with DatabaseInUse (default: true)
    PageCacheSize = 256,             // writer page frames; 0 disables caching
    MaxTransactionPageBudget = 16_384, // pages one transaction may hold in memory (default: 16384); OpenAsync and
                                       // CreateDatabaseAsync throw ArgumentOutOfRangeException for 0 or less
};
await using var writer = await AccessWriter.OpenAsync("database.mdb", writerOptions);
```

Both readers and writers limit one logical table-definition chain to
`MaxTableDefinitionBytes` (16 MiB by default). `MaxSchemaTraversalBytes`
(64 MiB by default) additionally bounds the combined logical definition bytes
in one catalog validation, including `MSysObjects`, and derives a catalog-entry
count ceiling from that limit. These are positive resource limits; callers may
increase them for trusted large schemas. A schema traversal limit reports
`SchemaTraversalBudgetExceeded` in strict and lenient modes. It does not imply
corruption or impose a process-wide memory ceiling: parsed objects, cache
lifetime and collection overhead have separate costs.

Linked Access/text data reads require `LinkedSourceResolver`; Access-source index and
complex-column metadata reads require it too. Listing link definitions, empty text-link
index/complex metadata and cached ODBC metadata does not. Register trusted, committed snapshot bytes independently
of the host database, then select them in the resolver. For example, after the application
has obtained `approvedBackendSnapshot` from its trusted source:

```csharp
var linkedOptions = new AccessReaderOptions
{
    LinkedSourceResolver = (link, pathHint, cancellationToken) =>
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (link.Kind != LinkedTableKind.Access
            || pathHint != @"C:\TrustedLinkedDatabases\Backend.accdb")
        {
            throw new UnauthorizedAccessException("The linked source is not registered.");
        }

        return new ValueTask<Stream>(new MemoryStream(approvedBackendSnapshot, writable: false));
    },
};
```

The hint selects an already-authorized resource; never use unchecked catalog metadata to
open files inside the callback. An application returning a FileStream must verify its actual
handle using its own platform policy or establish it in a trusted filesystem namespace.
Keep the source isolated and committed until disposal. The library neither reopens the
filename nor inspects its lock/journal sidecars; the application must perform any required
recovery before registration. The reader owns each returned stream, including cancellation
and parse failures, so return a fresh stream per request. The same resolver applies to nested
links, and the depth limit still bounds recursion through physical aliases. Source passwords
come only from `LinkedSourcePasswordResolver`, after stream authorization.

Access-linked index seeks, typed `FromIndex` reads, attachments and multi-value items
delegate to the approved source table, including transitive links. The source reader
and its authorized stream close when the operation or enumeration ends, including
early disposal and failures. Text links expose no indexes or complex columns; ODBC
links retain cached metadata and refuse data execution.

`MaxAttachmentContentBytesPerOperation` includes extension headers and is shared across attachment columns during one table scan. Each table in a multi-table read starts a fresh budget. Set a positive larger limit for trusted reads that exceed the 256 MiB default; `ValueTooLarge` refusals propagate in strict and lenient modes. Inflation checks cancellation between output chunks and before its final excess-byte check. A single decompressor read, checksum or copy remains a synchronous interval.

---

## Error Handling

Library-detected corruption, encryption, unsupported-format, size, constraint, lookup and state failures implement `IJetException` in `JetDatabaseWriter.Exceptions`. Use `ErrorCode` for programmatic handling and `ErrorInfo` for available table, column, index, relationship, object and page context. Messages describe failures for people; their wording is not a programmatic contract.

Actual storage exceptions propagate unchanged, including exceptions from caller streams. Ordinary argument validation, cancellation, disposal, system cryptographic-provider failures and caller callback failures retain their .NET contracts. A malformed in-memory compressed payload is classified as corruption; a stream failure is not treated as malformed data or a lenient missing value.

Ordinary Jet4/ACE chained Memo text requires complete UTF-16 code units. An odd stored payload raises a contextual `UnreadableLongValue` corruption error in strict reads; lenient reads return a missing value and emit a diagnostic. Unrelated updates refuse to write back that unreadable assigned value. Raw stored-byte reads retain the exact payload for diagnosis and preservation.

Trace diagnostics report operation, schema or physical-page context and exception types. They omit cell values, passwords, lock-file paths and machine/user identities. Delivery occurs outside page, pager and physical-I/O locks. Listener failures are ignored and recursive diagnostic delivery is suppressed. Database operations and disposal entered from a diagnostic callback are refused with `ReentrantDiagnosticCall` before taking a gate or beginning disposal.

Call `JetDiagnostics.EnableCounters()` before opening databases to register the optional `JetDatabaseWriter` EventSource. An EventListener or diagnostic collector can enable its EventCounters: `failures`, `corruptions`, `diagnostics` and `listener-failures`. These process-wide totals count constructed structured exceptions, corruption exceptions, diagnostic delivery attempts and suppressed listener failures, respectively; wrapping an exception can increment the failure total again. They contain no database names, paths or values. Recording a metric only updates an atomic integer and never initializes the provider or invokes a callback under a database lock. The library owns the returned provider.

| Exception | Typical cause |
|---|---|
| `JetObjectNotFoundException` | A named table or other database object is missing; inspect `ErrorCode`. |
| `JetCorruptDataException` | A malformed header, catalog, table, index, encryption descriptor, journal or value; inspect `ErrorCode` and available physical page/column context. |
| `JetLockException` | A lockfile or page-lock refusal, such as `DatabaseInUse` or `LockTimeout`. |
| `JetLimitationException` | A row or schema exceeds a supported limit, or a table's indexes cannot be maintained. |
| `JetAccessDeniedException` | Missing/incorrect password or a linked source that was not authorized. |
| `JetNotSupportedException` | An unsupported format, encryption provider, type or operation. |
| `JetOperationException` | An invalid operation or transaction state, including diagnostic reentry. |
| `ArgumentException` | Invalid names, definitions or values. |
| `IOException` | File, sharing or storage failure; structured corruption and lock exceptions also derive from this type. |
| `ObjectDisposedException` | The reader or writer has been disposed. |

These examples are not an exhaustive exception list. Catch a specific structured type or inspect `IJetException` before treating an `IOException` as a physical storage failure.

## Encryption options

`AccessReaderOptions.Password` and `AccessWriterOptions.Password` open an existing protected file. A nonempty writer password also requests native encryption during `CreateDatabaseAsync`; the initial database image is encrypted in memory before its first destination write.

Close readers and writers before calling `AccessDatabaseEncryption.EncryptAsync`, `DecryptAsync` or `ChangePasswordAsync`. Use a containing directory protected from untrusted access: staging and original backups can contain plaintext, and Unix file sharing does not provide access control. Maintenance processes pages through a reusable buffer, stages the final format in a unique adjacent file, updates native security metadata, flushes, then replaces the original. All operations use managed .NET APIs.

Windows maintenance files receive a protected owner-only ACL at creation. The .NET 10 build requests Unix mode `0600`, but inherited macOS ACLs can grant additional access. The .NET Standard build uses the host's default Unix creation permissions; it has no Unix permission API. Directory permissions and inherited ACLs must therefore provide the required protection. Windows replacement preserves the destination ACL; Unix replacement uses the staged file's permissions.

Cancellation and conversion failures before preparation preserve the original and clean up only owned incomplete files. A deterministic `<database>.jdw-replacement` intent binds the canonical path, generation and exact old/new content lengths and SHA-256 hashes. Complete old/new copies and a publication copy can require up to three extra database-sized files. Prepared without a complete valid commit decision selects the original; a complete valid committed decision selects the replacement. Invalid or conflicting evidence refuses without choosing a copy.

Recover an interrupted or cleanup-pending replacement with `AccessWriter.OpenAsync(path)`, including when the destination name is missing. Readers, encryption detection, caller FileStream opens and new database creation refuse pending replacement evidence. Keep every sidecar and generation file together in the protected directory. Legacy random candidates without a valid intent are retained rather than guessed. Commit errors may name complete retained copies in `IOException.Data["JetDatabaseWriter.ReplacementFile"]` and `["JetDatabaseWriter.OriginalFile"]`.

Contents are flushed with `FileStream.Flush(true)`; containing-directory persistence is not established. The protocol covers validated process recovery, not physical power-loss durability. Interrupted intent/ownership acquisition can retain incomplete evidence that requires refusal. Restoring a missing destination preserves validated database bytes and uses private maintenance-file creation permissions; filesystem ACLs, alternate streams and timestamps have no separate recovery record. See the [recovery contract](design/transaction-crash-recovery.md).

JET password-only files remain password-only after a password change. New JET encryption uses native RC4; new ACE encryption and ACE password changes use fresh AES-256-CBC/SHA-512. Existing encrypted files retain their provider during ordinary updates. JET3 passwords must fit 20 bytes in the database code page; JET4 passwords fit 20 UTF-16 characters. Unrepresentable passwords are refused before publishing output.

Linked databases receive passwords only through `LinkedSourcePasswordResolver`; the host database's password is not forwarded.

Encrypted descriptors are bounded before allocation or expensive key derivation:

| Option | Default | Purpose |
|---|---|---|
| `MaxEncryptionSpinCount` | 1,000,000 | Caps password-derivation iterations. |
| `MaxEncryptionInfoBytes` | 1 MiB | Caps the encryption descriptor size. |
| `AccessReaderOptions.MaxTotalEncryptionSpinCount` | 10,000,000 | Caps cumulative password-hash iterations across a reader and every linked source it opens during its lifetime. Reusing options for a new root reader starts a fresh budget. |

A resource-limit refusal does not mean the file is corrupt. Password hashing checks cancellation during its iteration loop. Malformed or contradictory descriptors fail before key derivation. Native Agile accepts AES-128/192/256, CBC/CFB8 and SHA-1/256/384/512; native fixtures include Microsoft-compacted AES-192, CFB8, SHA-256 and SHA-384 combinations with different password and page algorithms. RC4 CryptoAPI, compatibility AES and 50,000-iteration Standard have native DAO read/write/compact evidence. Standard and mixed Agile fixture provenance records their synthetic seeds separately from Microsoft compact output. Third-party extensible providers, non-AES Agile ciphers and Office compound packages remain unsupported database inputs.

For native Jet4, password verification uses the creation-date-masked header independently of the encoding key. RC4 page encryption uses the unmasked four-byte key XOR the little-endian page number. Password maintenance remasks catalog owners and access-control identities using the old and new header keys. Owner/SID indexes are validated before mutation and rebuilt from the remasked rows. Custom workgroup identities are preserved, but workgroup-user authentication is outside the library's session model. See [native encryption evidence](design/native-encryption-evidence.md).

For local DAO tests on hosts whose installed Office architecture differs from the default PowerShell architecture, `JETDATABASEWRITER_DAO_POWERSHELL` selects a compatible PowerShell executable. The test harness probes COM activation before using it.

## Limitations

### Thread safety and concurrent access

A shared `AccessReader` supports concurrent scans while the database remains unchanged. Eligible path-based reads on the .NET 10 build use positional I/O; stream reads and the .NET Standard build serialize access to the stream position.

Readers cache the table list, table definitions, owned data pages and up to `PageCacheSize` recently read pages. They do not refresh those caches. Path-based readers restrict sharing to read access for their lifetime, and refuse a pending recovery journal. A caller-supplied `FileStream` reader acquires an additional read-only lease; a writable or exclusively shared supplied handle can conflict with that lease and is refused. Prefer the path overload for file databases. Custom streams require caller-managed isolation.

Use one writer per file and keep external writers away for its entire lifetime. `AccessWriter` serializes its own mutation and lifecycle calls, and supports up to seven nested explicit transaction levels. It caches table metadata, AutoNumber counters and enforced relationships; competing writers can corrupt the database.

Path-based writers request exclusive file sharing, which Windows enforces. Unix sharing and locks depend on cooperating processes. With a caller-supplied stream, excluding other readers and writers is the caller's responsibility. Path aliases and non-cooperating processes must not bypass that coordination. Do not change or replace the database bytes outside the writer while it is open, even between calls. Dispose the writer before an external schema edit and reopen it afterwards; a new writer hydrates constraints and counters from the current database. `leaveOpen: true` retains stream ownership, but does not permit concurrent or between-call edits to that stream.

`UseLockFile` and `RespectExistingLockFile` default to `true`; keep them enabled to refuse an existing lockfile. Page byte-range locks are used on Windows, Linux and Android; they are a no-op on unsupported platforms such as macOS, iOS and tvOS. Lockfiles and page locks do not replace exclusive writer coordination. See the [concurrency design](design/concurrency-and-lock-ordering.md).

### Transaction durability

- File transactions and physical shrinking recover after process termination using the adjacent `.jdw-journal`. Preserve it and reopen with `AccessWriter` before using Access or other readers. Recovery refuses an invalid or conflicting journal. Custom streams and power loss are outside this guarantee.
- Statement page buffers are bounded. File-backed stores that support in-place writes spill large undo logs to temporary files; other stores retain undo in memory. `MaxTransactionPageBudget` limits explicit transactions only.
- Cancellation is checked between spill batches and before final write-back. Started physical batches and recovery finish without cancellation. A cancelled mutation restores earlier spills; inside an explicit transaction, it rolls back to an internal savepoint and leaves earlier successful calls intact.
- Path-based creation initializes and flushes an adjacent private staging file before publishing the complete database without overwriting an existing destination. If reopening the published file fails, the completed database remains available. A process crash before publication can leave a staging file, but cannot expose a partially initialized destination.
- File replacement flushes temporary contents before renaming, but does not flush the containing directory. The renamed directory entry is not guaranteed to survive power loss.

See [Transactions](#transactions) for commit, recovery and faulted-writer behavior.

### Column defaults and validation rules

Persisted `DefaultValue` and `ValidationRule` expressions run on the library's expression engine. Unsupported syntax or functions, such as `DLookUp`, refuse the write before mutation.

`GenGUID()` generates a GUID; `CurrentUser()` returns `Admin` for the supported session without workgroup authentication. Table-level rules check complete post-update rows. A CLR `ValidationRule` delegate applies only to the writer that created it. See [column constraints](writing.md#column-constraints).

### Table and row size

Tables hold at most 255 columns. Creating or adding a 256th column throws `JetLimitationException` before writing.

The library permits row layouts up to 2,036 bytes on Jet3 and 4,080 bytes on Jet4/ACE. These are tested page-capacity limits, not verified Microsoft Access record-size limits; native boundary checks remain open.

MEMO and binary OLE values over 64 encoded bytes use separate long-value pages and occupy 12 bytes in the row. Shorter values stay inline when they fit. To fit an oversized row, the writer moves the largest MEMO and byte-array OLE values out first. An OLE value supplied as a string stays inline. Access also moves long values out of oversized rows, but its exact selection order has not been verified.

If a row still cannot fit after those values move, the writer throws `JetLimitationException` before writing, including long-value pages. Updates and cascades also refuse rows that would exceed the limit. Jet3 text and names must fit the database's code page; see [writing data](writing.md#writing-data).

### Feature and format coverage

- Complex-column APIs expose native item IDs and atomically add, replace and remove attachments and multi-value entries. Existing append-only MEMO columns record native version history during inserts and assigned updates; history entries are read-only. See [complex-item mutation](writing.md#complex-item-mutation).
- Newly created Jet3 databases lack the system-table bootstrap needed to create relationships.
- Writes are refused when the writer cannot maintain a table's indexes, including unsupported collations or BIGBINARY keys.
- Access-file linked tables support read-through. ODBC links expose cached catalog metadata but do not execute queries; see [linked tables](writing.md#linked-tables).
- Supported encryption formats and maintenance restrictions are listed in [Encryption Support](../README.md#encryption-support).

See the [validation matrix](design/writer-disk-format-validation-matrix.md) for evidence and the [open requirements](todo.md) for remaining work.

### Compact & Repair

`ShrinkDatabaseAsync` truncates free pages at the physical end of the file. It does not move live pages, renumber references, rebuild tables into a new file or scrub every unused gap in live pages. See [storage maintenance and secure erase](writing.md#storage-maintenance-and-secure-erase).

### Forms, reports, macros, queries, VBA

The library does not execute forms, reports, macros, modules, saved queries or VBA. Unrelated storage edits aim to retain these objects, but complete preservation of application objects and their dependencies is not established. See the [schema-preservation requirements](todo.md#s--hostile-file-parsing-and-schema-preservation).

### SQL and ODBC

The library does not parse or execute SQL and does not provide an ODBC driver. Use [LINQ queries and streaming](reading.md) to filter, project and join data.

## How It Works

The library parses JET pages directly, based on the [mdbtools format specification](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md):

1. **Page 0** — header: Jet3/Jet4 detection, code page, encryption flag
2. **Page 2** — `MSysObjects` catalog: table names → TDEF page numbers
3. **TDEF pages** — table definition chains: column descriptors + names
4. **Data pages** — row slot arrays (an overflow row's slot points to the slot Access moved the row to) → null mask + fixed/variable fields
5. **LVAL pages** — long-value chains for MEMO, OLE, and attachment payloads
