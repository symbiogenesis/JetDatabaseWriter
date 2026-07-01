# Data Remanence TODOs

**Status:** Proposed backlog, created 2026-05-31.

This document tracks follow-up work for data remanence, storage cleanup, and Access-compatible compaction behavior. The main distinction to preserve is:

- **Access-compatible default behavior:** match normal JET/ACE delete, update, free-list, and Compact & Repair behavior as closely as practical.
- **Secure erase behavior:** opt-in overwrite of old payload bytes before storage is made reusable. This can be stronger than normal Microsoft Access behavior and should stay behind `AccessWriterOptions.SecureEraseMode`.

Existing public documentation lives in [README.md](../../README.md#storage-maintenance-and-secure-erase). Current coverage and residual scope are summarized in [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

## Action Items

### 1. Fix or verify complex attachment secure erase

- [ ] Add a regression test for deleting a parent row with an attachment payload large enough to force external `FileData` LVAL pages while `SecureEraseMode.DeletedRowsAndFreedPages` is enabled.
- [ ] Add equivalent coverage for dropping an attachment column and dropping a table that owns attachment complex columns.
- [ ] Ensure hidden flat child rows are deleted with enough `TableDef` context to collect external `FileData` LVAL roots before the row body is cleared.
- [ ] Scrub and deallocate those external attachment LVAL pages when secure erase is enabled.
- [ ] Run the Access/DAO CompactDatabase signal after the byte-level marker checks.

Current concern: ordinary MEMO/OLE row deletion passes the owning `TableDef` into the row-delete path so external LVAL roots can be collected. Some complex-column paths delete or clear hidden flat rows without that context, which may leave attachment `FileData` LVAL pages orphaned until Compact & Repair.

### 2. Probe ordinary Access LVAL reclamation

- [ ] Create a DAO baseline probe for ordinary MEMO/OLE delete and update on Jet4/ACE databases.
- [ ] Determine whether Microsoft Access returns old external LVAL pages to the global free map before Compact & Repair, or leaves them orphaned until compaction.
- [ ] Compare single-page and chained LVAL payloads, including update-to-new-LVAL and delete-row cases.
- [ ] Promote any observed invariant into focused regression tests.

Decision point: if Access frees old LVAL pages without scrubbing them, add a normal reclamation path independent of `SecureEraseMode`. If Access leaves them until Compact & Repair, keep the current default behavior and document the probe result.

### 3. Design a managed Compact & Repair style rebuild API

- [ ] Sketch an API such as `CompactDatabaseAsync` that writes live database content into a new stream or file.
- [ ] Decide whether the first version supports Jet3, Jet4, ACE, encrypted files, complex columns, linked-table catalog entries, and relationships.
- [ ] Preserve catalog metadata that the writer can currently round-trip, including indexes, relationships, complex columns, column properties, linked-table metadata, and encryption settings where supported.
- [ ] Rebuild live table rows and indexes instead of moving pages in place.
- [ ] Validate with reader round-trip and DAO CompactDatabase on the rebuilt output.

Non-goal: forensic erasure of the original source file. A rebuild can omit deleted rows, orphaned pages, and interior free pages from the new file, but it cannot guarantee removal from storage hardware, filesystem journals, snapshots, backups, or prior copies.

### 4. Keep unused live-page gap cleanup rebuild-first

- [ ] Treat full live-page compaction, arbitrary unused-gap scrubbing, and broader orphan-page reclamation as compact/rebuild design work, not as an extension of current secure erase.
- [ ] Identify which unused byte ranges inside live data, index, TDEF, and usage-map pages can be safely overwritten.
- [ ] Avoid scrubbing bytes that Access treats as reserved, copied-forward, page checksum/version space, row-offset slots, index prefix state, or table-definition metadata.
- [ ] Prefer implementing broad gap cleanup through the managed compact/rebuild path before adding any in-place scrubber.
- [ ] If an in-place scrubber is later proposed, require a separate design that proves exact byte ranges are disposable and validates with marker tests and DAO CompactDatabase across Jet3, Jet4, and ACE.

This should not be mixed into `ScrubFreePagesAsync`, whose current contract is limited to pages already on the free list.

### 5. Evaluate an in-place update fast path

- [ ] Investigate whether Access updates row payloads in place when the replacement row fits the existing slot.
- [ ] If Access does, design a conservative writer fast path for same-size or fits-in-slot updates.
- [ ] Keep index maintenance, FK cascade behavior, calculated values, and long-value replacement semantics explicit.
- [ ] Add marker tests proving the old row body is not left behind when the in-place path is used.

This is a remanence and file-growth improvement, but it is more invasive than LVAL reclamation because row layout, indexes, and relationship enforcement all meet in `UpdateRowsAsync`.

### 6. Refresh public docs alongside implementation choices

- [ ] Update the README data-remanence bullets after each implemented behavior change.
- [ ] Keep the default-vs-secure distinction clear: normal Access parity and secure erase are separate goals.
- [ ] Update [complex-columns-format-notes.md](complex-columns-format-notes.md) if attachment flat-table cleanup changes.
- [ ] Update [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md) with the strongest new automated signal.

## Guardrails

- Do not make secure erase the default unless the public compatibility contract changes deliberately.
- Do not reclaim or scrub `MSys*` or generated complex flat-table storage aggressively without DAO CompactDatabase validation.
- Do not treat writer-created databases as Access-authored oracles; use Access-authored fixtures, DAO-authored scratch databases, or DAO probe output for parity decisions.
- Keep byte-level marker tests paired with semantic reader/DAO tests so cleanup does not silently corrupt live data.
