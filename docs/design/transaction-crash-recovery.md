# Transaction crash recovery

File-backed statement and explicit transactions use `PersistentRollbackJournal` in addition to the pager's in-process undo log. The persistent journal stores raw bytes, so encrypted database pages remain encrypted. Non-file streams retain in-process rollback only.

## Write ordering

1. Before the first physical statement spill or final commit write, create `<database>.jdw-journal.preparing` under exclusive database coordination. Copy the complete original database with bounded buffers, record its length and SHA-256, and flush the completed header and snapshot to disk. Close it, promote it by a non-overwriting adjacent rename to `.jdw-journal`, then reopen the published journal before permitting writes. An existing final journal is never overwritten. An abandoned preparation file authorizes no writes: recovery and readers ignore it, and the next preparation replaces it.
2. Before each physical page write, append its aligned offset and encoded after-image with a SHA-256 checksum, then flush the journal to disk. A page can have several recorded versions across statement spills.
3. After every database page is written, flush the database to disk. Append a commit record containing the SHA-256 of the complete committed file and flush that decision before removing the sidecar.
4. For in-process rollback, detach intent recording, restore the original bytes and length, flush the database, then remove the journal. Original bytes are already authorized by the snapshot, so undo needs no additional intent records.

A failure while recording the commit decision has an uncertain outcome. The writer faults and leaves the database and journal available for recovery rather than starting rollback behind a potentially durable commit record. Cleanup failure after a completed durable decision does not turn a successful commit into a rollback. Journal creation failure faults the writer; live preparation failures attempt to remove their unused sidecar without masking the original error.

The existing pager and store gates serialize journal creation, raw intent recording and physical writes. Existing cooperative commit/page locks remain in place. File statement commits request durable flushes even when `UseTransactionalWrites` is false.

## Validation and recovery

Writer open recovers before reading database headers or checking passwords. A journal has a fixed 128-byte header, the original raw snapshot and fixed-size intent records. Header, snapshot and records have independent checksums. Only 2 KiB and 4 KiB pages are accepted; database length is capped at 2 GiB and journal length at 16 GiB. Buffers are bounded independently of lengths supplied by the journal.

Recovery validates the header, original snapshot, every complete record, full-path identity and current database before writing any recovery bytes. For an undecided transaction, the current length must be between the original length and the largest authorized write end. Each current byte must equal the original byte or a recorded version for the same page. This permits torn writes and a recovery that was interrupted after restoring only some original bytes. Bytes on untouched pages must match the snapshot. An incomplete final intent is ignored because it could not authorize a database write; a complete record with an invalid checksum is refused.

A valid final commit record must match the complete current database hash; recovery then keeps those bytes and removes the stale journal. Otherwise a fully validated undecided journal restores the original bytes and length, flushes the database and removes the journal. Repeating recovery after an interruption is safe because restored bytes still match the original snapshot. Invalid final journals and conflicting content are retained and refused without replay. A complete snapshot promoted before a crash can be recovered even if no database write has started; a crash during staging leaves the original database untouched. A truncated final snapshot is corruption and is refused.

After successful journal recovery, an abandoned `.ldb` or `.laccdb` is removed only if it can be opened exclusively. Ordinary opens still respect existing lockfiles. Recovery and lockfile removal are distinct from successful password validation.

## Access and identity

Path writers request `FileShare.None`. Path readers restrict sharing to `FileShare.Read` (or the caller's stricter choice) and reject pending journals before parsing. Caller-supplied file readers acquire a second read-only lease; conflicting supplied handles are refused. These lifetime sharing rules prevent a pre-existing reader from mixing cached pages with interrupted writes by a cooperating writer. Windows enforces sharing; Unix and arbitrary caller streams require cooperating access. A caller-supplied writer stream must exclude all other readers and writers throughout its lifetime.

Identity combines the full database path (case-normalized on Windows) with validation of all raw contents. It is not an operating-system file ID: byte-identical replacement at the same path is indistinguishable, and path aliases are not a recovery discovery mechanism. Preserve the original path, the database and its journal together; do not move, replace or independently edit them while recovery is pending. Checksums detect damage, not deliberate replacement by an attacker who can rewrite both journal data and checksums. Recovery assumes exclusive access to trusted local journal files.

Microsoft Access has no knowledge of this sidecar. After a crash, recover through `AccessWriter` before letting Access or another program open the file.

## Costs and boundaries

A transaction copies the entire original database and hashes the committed file. Journal storage adds a raw after-image record for each physical page write, and each record requires a flush. Grouping mutations into one explicit transaction amortizes the initial snapshot. Recovery uses bounded memory but can rescan all intents for each changed page. Selective before-images and faster validated recovery remain performance work.

The process-crash protocol does not establish power-loss atomicity: creating or deleting the journal changes directory entries, and this implementation does not flush the containing directory. On Linux, a file `fsync` alone does not guarantee its directory entry; a separate directory flush is required ([Linux fsync documentation](https://www.man7.org/linux/man-pages/man2/fsync.2.html)). Windows file flushing pushes buffered data toward the device, but filesystem and storage ordering still require separate verification ([Microsoft file flushing documentation](https://learn.microsoft.com/en-us/windows/win32/fileio/flushing-system-buffered-i-o-data-to-disk)).

Initial database creation, physical shrinking, replacement and non-file streams have separate lifecycles. Physical shrinking has bounded spillable raw-tail undo for in-process write, truncate and flush failures; that temporary undo is not a persistent crash-recovery journal. No explicit persistent journal-stream API is provided for custom streams.

## Regression evidence

`PersistentRollbackJournalTests` exercises torn writes, partial final records, multiple page versions mixed with partial undo, corrupt headers/snapshots/records, checksum-valid wrong paths and offsets, invalid lengths, stale commit decisions, incomplete final snapshots, abandoned staging and Windows path casing. `TransactionCrashRecoveryTests` kills a child process without disposal during statement spills and final commit writes, then compares recovered files byte for byte. It also kills after successful commit and compares against an independently committed image. These checks cover process termination, not power interruption or Access-native recovery.
