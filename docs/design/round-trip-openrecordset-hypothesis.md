# DAO OpenRecordset Compatibility Log

Companion: [round-trip-test-failures.md](round-trip-test-failures.md)

## 1. Current Status

| Field | Value |
|---|---|
| Original issue | DAO `OpenRecordset` rejected writer-created user tables with `"Unrecognized database format ''."` even though `OpenDatabase` succeeded. |
| Original issue status | Resolved on 2026-05-10. |
| Current follow-up status | OpenRecordset, MEMO fidelity, AutoNumber continuation, and DAO FK enforcement are resolved or verified in the current branch. Plaintext FK Compact & Repair and encrypted compact remain opt-in compatibility gaps. |
| Primary next task | Diagnose why DAO Compact & Repair still drops or empties writer-created FK-bearing tables even after FK enforcement metadata is DAO-shaped. |
| Current date of this log | 2026-05-11. |

## 2. Test Matrix

| Bucket | Test | Guard | Current result |
|---|---|---|---|
| Original issue | `DaoOpenRecordset_RowCount_MatchesWriterOutput` | Requires Microsoft Access | Passing |
| Original issue | `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoRelationshipEnforcement_FkViolation_RaisesError` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` until unskipped | Passing after DAO-compatible FK cross-references and index `used_pages` fixes |
| Open gap | `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` | Still failing: DAO Compact succeeds, but the compacted database does not preserve the writer-created FK table rows |
| Open gap | `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` | Not revalidated after the latest FK metadata work; expected to share the single-FK compact failure mode |
| Open gap | `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` | Expected to fail until encrypted compact compatibility is fixed |

## 3. Confirmed Fixes

| ID | Problem | Fix | Verification |
|---|---|---|---|
| H48 | Jet4/ACE TDEF `tdef_len` was 8 bytes shorter than DAO's for tables with at least one index. DAO-authored TDEFs count 8 trailing zero bytes after the `0xFFFF` no-usage-map sentinel; the writer did not. | [JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs): after writing the `0xFFFF` sentinel, advance `namePos += 8` when `jet4 && numIdx > 0`. | `rt-dao-baseline` FormatProbe mode (legacy `DIAG_RT_DAO_BASELINE`) reported `OpenRecordset('RT_Customers')` exit=0 for both writer and DAO copies. `DaoOpenRecordset_RowCount_MatchesWriterOutput` passes. |
| H48-adj | `RelationshipManager.AddFkLogicalIdxEntry` stamped `0x00000659` on an appended real-idx physical descriptor. DAO expects the Jet4 real-idx descriptor magic `0x00000783`. | [JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs): use `Constants.TableDefinition.Jet4.RealIdx.LeadingMagic`. | FK-bearing table TDEF magic assertions pass. |
| H49 | Newly appended user-table data pages were not recorded in the per-table owned/free usage maps. DAO sequential and snapshot recordsets walk those maps, so rows existed on disk but could appear invisible to DAO. | [JetDatabaseWriter/Pages/DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs): mark newly appended user-table data pages in both owned and free usage-map rows. The INLINE row body is `type byte + int32 start_page + 64 bitmap bytes`; bitmap bits start at row offset `+5`. | `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` passes. |
| AutoNumber-adj | DAO chooses the next AutoNumber from the TDEF high-water value at offset `0x14`. The writer generated unique IDs for its own rows but did not persist that high-water value, so DAO reused an existing ID. | [JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs): after successful inserts, write the maximum inserted AutoNumber value to `Constants.TableDefinition.AutoNumberOffset` in [JetDatabaseWriter/Constants.cs](../../JetDatabaseWriter/Constants.cs). | `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` passes. |
| FK-enforcement-1 | DAO was not reliably reporting orphan-insert failures in the PowerShell validation harness because `Database.Execute(sql)` without options can suppress engine errors. | Use `$db.Execute(sql, 128)` (`dbFailOnError`) in DAO scripts that are meant to assert constraint failures. | The orphan-insert probe now returns DAO error `-2146825087` for both DAO-authored and writer-authored FK databases when metadata is correct. |
| FK-enforcement-2 | Writer FK logical-index entries did not match DAO cross-reference semantics. | Parent-side FK logical entry uses `rel_tbl_type = 0x01`; child-side uses `rel_tbl_type = 0x02`; `rel_idx_num` points to the partner logical-index number, not the partner real-index slot. Self-referential relationships must reserve distinct logical numbers from one original TDEF snapshot. | `CreateRelationshipAsync_SingleColumn_EmitsDaoCompatibleFkCrossReferences` passes; the DAO orphan-insert probe rejects invalid child rows. |
| FK-enforcement-3 | Rebuilt real-index descriptors for FK-bearing tables had `used_pages = 0`. DAO FK enforcement needs a valid index page usage-map pointer even when `first_dp` points at a valid B-tree. | Bulk `MaintainIndexesAsync` records every rebuilt B-tree page per real index, writes a DAO-shaped index usage-map page, and patches each rebuilt real-index `used_pages` slot. | `DaoRelationshipEnforcement_FkViolation_RaisesError` passes with the opt-in switch; child row counts remain stable after a rejected orphan insert. |

## 4. Layout Facts To Preserve

| Area | Fact | Source / verification |
|---|---|---|
| TDEF length | Jet4/ACE indexed tables include 8 zero bytes after the `0xFFFF` index-name sentinel inside `tdef_len`. | H48 DAO baseline diff. |
| TDEF free space | `freeSpace` changes with `tdef_len`; the H48 fix widens both consistently. | H48 DAO baseline diff. |
| Usage-map inline row | INLINE usage-map rows are 69 bytes: `+0` type, `+1..+4` little-endian `start_page`, `+5..+68` bitmap. | DAO-authored low-page and high-page usage-map baselines. |
| Usage-map scope | Writer-created user-table TDEFs can be marked in place. Pre-existing system-table TDEF usage maps must not be mutated by this path. | Marking system-table maps caused DAO `OpenDatabase` to raise `"Invalid argument"`. |
| AutoNumber high-water | TDEF bytes `0x14..0x17` store the last issued AutoNumber value, not the next value. | DAO-authored 10-row AutoNumber fixture stamped `0A 00 00 00`. |
| Real-idx descriptor magic | Jet4 real-idx physical descriptors use leading magic `0x00000783`, not the format-wide TDEF magic `0x00000659`. | H48-adj fix and TDEF magic assertions. |
| FK logical cross-reference | On DAO-authored FK pairs, `rel_tbl_type` is side-specific (`0x01` parent side, `0x02` child side) and `rel_idx_num` cross-references the partner logical-index number. | `fk-dao-baseline` FormatProbe mode (legacy `DIAG_FK_DAO_BASELINE`) writer-vs-DAO dump. |
| FK logical ordering/name | DAO inserts FK logical entries before the existing `PrimaryKey` logical entry. The parent-side logical name is hidden (`.rB` in the simple baseline); the child side uses the relationship name. | `fk-dao-baseline` FormatProbe mode writer-vs-DAO dump. This is confirmed format ground truth, but matching it has not yet been proven sufficient for compact survival. |
| Real-index `used_pages` | Jet4/ACE real-index descriptors used by FK enforcement need a non-zero `used_pages` row/page pointer to an INLINE usage-map row for the index pages. | FK enforcement failed before this pointer was patched and passed afterward. |
| `MSysACEs.FInheritable` | DAO-created user-table ACE rows use `FInheritable = False`, with `ACM = 0x000FFEFF` and owner/admins/users SIDs. | `fk-dao-baseline` dump. Changing this alone did not fix FK compact. |
| DAO text-column LvProp shape | DAO programmatic table creation emits `AllowZeroLength = False` for text columns, skips `Required` for AutoNumber columns, emits `Required = True/False` for non-AutoNumber text columns, writes Boolean property entries with `ddlFlag = 0x01`, and uses property-block chunk subtype `0x0001`. | `fk-dao-baseline` raw `MSysObjects.LvProp` hex diff. Matching this alone did not fix FK compact. |
| Compact can silently omit objects | DAO `CompactDatabase` can return success while omitting writer-created FK tables and `MSysRelationships` rows from the compacted output, or while preserving table metadata but dropping rows, depending on fixture shape. | `fk-dao-baseline` and the focused single-FK compact test. Treat exit code 0 as insufficient; always reopen and verify schema plus row counts. |

## 5. Disconfirmed Causes

Do not re-test these for the original `OpenRecordset` failure unless a new fixture changes the table shape in a relevant way.

| ID / group | Claim | Result |
|---|---|---|
| H46 | Writer-emitted `LvProp` content caused DAO `OpenRecordset` failure. | Disconfirmed. Suppressing `BuildLvPropBlob` did not fix DAO. The catalog row `LvProp` column is not the cursor-materialization blocker. |
| H47 | TEXT/MEMO `ExtraFlags` byte 16 must be `0x00` instead of compressed Unicode `0x01`. | Disconfirmed. DAO accepts both states. |
| H21, H23, H24, H26-H28, H32-H45 | Real-idx pointers/flags, logical-index sentinels, selected column descriptor bytes, empty PK leaf header, and usage-map pointer presence. | Disconfirmed for the `RT_Customers` indexed-table fixture; writer matched DAO or the difference was benign. |
| Earlier compact/OpenRecordset hypotheses | LvProp chained-LVAL shape, writer-private NOT NULL flag, TDEF magic in established create paths, DB-header modify counter, page 1 GPM updates, `MSysACEs`, catalog-name encoding, and index-leaf entry mechanics. | Disconfirmed in [round-trip-test-failures.md](round-trip-test-failures.md). |
| FK compact sufficiency checks | DAO-shaped `MSysRelationships` rows, FK logical cross-references, non-zero real-index `used_pages`, DAO-shaped ACE inheritance flags, and DAO-shaped text-column LvProp blobs. | Each is either necessary or DAO-observed, but together they have not yet made writer-created FK tables survive DAO compact. The remaining compact defect is narrower than the original FK enforcement defect. |

## 6. Open Gaps

| Priority | Test | Current symptom | Most likely area |
|---:|---|---|---|
| 1 | `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` | DAO Compact succeeds, but post-compact verification still fails. In the Northwind test the table metadata remains but rows are gone; in the simple FK baseline DAO omits the writer-created FK tables entirely. | Relationship/index allocation metadata used by Compact & Repair. The strongest current lead is the shape/location of real-index `used_pages` usage-map rows after bulk index rebuilds. |
| 2 | `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` | Not re-run after the latest single-column FK diagnostics; likely same compact preservation defect plus composite key ordering/relationship-column ordinal sensitivity. | Same as priority 1, then composite FK col-map and `MSysRelationships.icolumn` ordering. |
| 3 | `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` | DAO `CompactDatabase` rejects Agile-encrypted writer ACCDB with `"Unrecognized database format"`. | Encryption header and per-page encryption compatibility. Defer until plaintext FK compact is understood. |

## 7. Current Next Steps

### Step 1: Finish FK Compact Diagnosis

| Action | Detail |
|---|---|
| 1. Complete index usage-map diff | Extend or clean up `fk-dao-baseline` to dump each real-index `used_pages` row body. Compare DAO's row placement with the writer's post-rebuild map. In current dumps DAO points PK/FK index `used_pages` rows into the same usage-map page as table row 0/1 in the simple baseline, while the writer may allocate a separate index usage-map page after rebuild. |
| 2. Compare compact inputs vs outputs | For both the simple FK baseline and the Northwind round-trip test, record whether compact omits catalog rows, omits relationship rows, drops data rows, or rewrites TDEF/index usage-map pointers. These are distinct failure modes. |
| 3. Keep FK enforcement guarded but documented | The enforcement test now passes with the opt-in switch. Do not unskip it until the surrounding FK compact work is either fixed or explicitly split into a separate follow-up. |
| 4. Patch narrowly | Once the compact-specific byte delta is isolated, patch only that metadata path. Avoid further changes to `MSysRelationships`/LvProp/ACE fields unless a new DAO diff proves a mismatch. |
| 5. Verify | Re-run `fk-dao-baseline`, the single-FK compact test, and the composite-FK compact test with `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1`. |

Useful verification commands:

```pwsh
dotnet run --project JetDatabaseWriter.FormatProbe -- fk-dao-baseline
```

```pwsh
$env:JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS = "1"
dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoRelationshipEnforcement_FkViolation_RaisesError"
```

### Step 2: Re-test FK Compact Cases

Run the compact tests with the opt-in switch:

```pwsh
$env:JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS = "1"
dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.SinglePk_AndSingleColumnFk_SurviveCompactAndRepair"
dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.CompositePk_AndMultiColumnFk_SurviveCompactAndRepair"
```

If either still fails, compare pre-compact and post-compact files. Start with:

| Check | Reason |
|---|---|
| Writer-created table TDEF row counts | Confirms whether compact dropped rows or only metadata. |
| Per-table owned/free usage maps | DAO compact relies on these maps when enumerating data pages. |
| Parent and child FK logical-index entries | Compact may discard rows when relationship metadata is inconsistent. |
| Backing index root/leaf pages | Confirms whether DAO can traverse the PK/FK indexes before compact. |

When each compact test passes, switch it from the known-gap opt-in guard to the normal Microsoft Access guard.

### Step 3: Fix Encrypted Compact

Defer this until plaintext FK enforcement and FK compact behavior are understood. Then:

| Action | Detail |
|---|---|
| 1. Build baseline | Produce a DAO/Access-encrypted ACCDB and a writer-encrypted ACCDB with the same simple table. |
| 2. Compare container metadata | Start with encryption header, CFB streams, salt/hash metadata, and page-encryption boundaries. |
| 3. Verify compact | Run `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` with the opt-in switch. |
| 4. Unskip | When passing, move it to the normal Microsoft Access guard. |

## 8. Probe Commands

| Goal | Command |
|---|---|
| Reproduce original H48 baseline diff | `dotnet run --project JetDatabaseWriter.FormatProbe -- rt-dao-baseline` |
| Inspect preserved memo fixture | `dotnet run --project JetDatabaseWriter.FormatProbe -- memo-readback` |
| Run the normal DAO validation class | `dotnet test --project JetDatabaseWriter.Tests --filter-class "JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests"` |
| Run known-gap tests locally | Set `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1`, then run the focused method with `--filter-method`. |

## 9. Reference Sources

Use empirical DAO-authored baselines first. Use these sources only when a baseline diff is insufficient.

| Source | Use |
|---|---|
| [Jackcess](https://github.com/jahlborn/jackcess) | Cross-check table, column, index, usage-map, and property-map writer behavior. |
| [mdbtools](https://github.com/mdbtools/mdbtools) | Historical Jet/MDB internals. Treat offsets as advisory; some are stale. |
| MS-CFB / MS-OFFCRYPTO specs | CFB and encryption-container behavior. No `[MS-MDB]` on-disk table-format spec exists. |
