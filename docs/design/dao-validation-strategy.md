# DAO Validation Strategy

**Status:** Split out of the former writer disk-format validation table on 2026-05-23.

This note holds durable validation rules for writer-emitted Access disk-format bytes. The active cross-feature backlog lives in [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

## Validation Ladder

Use the strongest feasible automated signal for the mutation under test:

1. **Reader round-trip:** writer output is reopened through JetDatabaseWriter's reader and assertions cover schema, data, index metadata, or decoded payloads.
2. **DAO OpenRecordset:** a bitness-matched Windows PowerShell host drives `DAO.DBEngine.120` against writer output and materializes recordsets or DAO index seeks.
3. **DAO CompactDatabase:** DAO compacts writer output, then JetDatabaseWriter reopens the compacted file and verifies schema, data, relationships, passwords, or payloads as applicable.

Manual Microsoft Access UI checks are supplemental evidence. They are useful for workflows that DAO does not expose, such as saving an attachment back to disk through the UI, but they are not stronger than DAO automation when the same scenario can be automated.

DAO-authored byte comparisons and FormatProbe output are diagnostic oracles for undocumented format details. When a probe identifies a root-cause invariant, promote that invariant into a focused regression test instead of leaving the probe report as the only evidence.

## Fixture Choice

Prefer Access-authored fixtures such as [NorthwindTraders.accdb](../../JetDatabaseWriter.Tests/Databases/NorthwindTraders.accdb) for DAO compact tests that validate writer mutations. That keeps the trusted base in Microsoft Access bytes and isolates the writer-owned changes.

Writer-created fresh databases are still valuable for bootstrap coverage, but they make catalog scaffolding part of the trusted base. Use them when the feature being validated is the bootstrap itself, or when no reliable Access-authored host exists.

## Compact Rules

`CompactDatabase` exit code 0 is not enough. Always reopen the compacted output and assert the feature that motivated the test: rows, indexes, relationships, complex payloads, encryption/password handling, or page-reuse invariants.

Encrypted source compaction must use DAO's five-argument form, with the source password supplied separately from the destination locale/password. The historical details live in [round-trip-openrecordset-hypothesis.md](round-trip-openrecordset-hypothesis.md).

If a design note says Microsoft Access Compact and Repair validation is pending, replace the blanket warning with a link to the exact open checklist item in [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md) or to the feature doc that owns the residual byte-level gap.

## Current Coverage Shape

Access-equipped hosts currently cover DAO OpenRecordset row counts, DAO primary-key seeks, AutoNumber continuation, MEMO/OLE/LVAL fidelity, FK enforcement, FK CompactDatabase, encrypted CompactDatabase, fresh ACCDB bootstrap compact, complex-column compact, storage-maintenance compact, advanced ACE/Jet4 index compact, and conditional Jet3 index compact when the installed DAO engine can open Access 97 `.mdb` files.

Reader/regression coverage also guards writer-created linked Access/ODBC/text catalog rows: catalog-only negative object-id allocation, low-24 collision avoidance, Type 4/6 `MSysObjects` catalog-index splicing, fixture-aligned flags, non-null placeholder `LvProp`, and linked-object ACE rows. DAO validation for writer-created links remains open until the writer emits Access's cached linked-table schema payload in `MSysObjects.LvProp`.

Use [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md) as the active todo list for remaining validation promotions and residual byte-comparison gaps.
