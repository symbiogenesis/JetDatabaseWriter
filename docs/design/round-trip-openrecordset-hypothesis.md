# DAO OpenRecordset Compatibility Log

Companion: [round-trip-test-failures.md](round-trip-test-failures.md)

## 1. Current Status

| Field | Value |
|---|---|
| Original issue | DAO `OpenRecordset` rejected writer-created user tables with `"Unrecognized database format ''."` even though `OpenDatabase` succeeded. |
| Original issue status | Resolved on 2026-05-10. |
| Current follow-up status | Two adjacent DAO gaps are resolved; four remain opt-in compatibility gaps. |
| Primary next task | Fix DAO enforcement of writer-created foreign-key relationships. |
| Current date of this log | 2026-05-11. |

## 2. Test Matrix

| Bucket | Test | Guard | Current result |
|---|---|---|---|
| Original issue | `DaoOpenRecordset_RowCount_MatchesWriterOutput` | Requires Microsoft Access | Passing |
| Original issue | `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` | Requires Microsoft Access | Passing |
| Open gap | `DaoRelationshipEnforcement_FkViolation_RaisesError` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` | Expected to fail until FK metadata is fixed |
| Open gap | `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` | Expected to fail until FK compact behavior is fixed |
| Open gap | `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` | Expected to fail until FK compact behavior is fixed |
| Open gap | `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` | Requires `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` | Expected to fail until encrypted compact compatibility is fixed |

## 3. Confirmed Fixes

| ID | Problem | Fix | Verification |
|---|---|---|---|
| H48 | Jet4/ACE TDEF `tdef_len` was 8 bytes shorter than DAO's for tables with at least one index. DAO-authored TDEFs count 8 trailing zero bytes after the `0xFFFF` no-usage-map sentinel; the writer did not. | [JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs): after writing the `0xFFFF` sentinel, advance `namePos += 8` when `jet4 && numIdx > 0`. | `DIAG_RT_DAO_BASELINE` reported `OpenRecordset('RT_Customers')` exit=0 for both writer and DAO copies. `DaoOpenRecordset_RowCount_MatchesWriterOutput` passes. |
| H48-adj | `RelationshipManager.AddFkLogicalIdxEntry` stamped `0x00000659` on an appended real-idx physical descriptor. DAO expects the Jet4 real-idx descriptor magic `0x00000783`. | [JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs): use `Constants.TableDefinition.Jet4.RealIdx.LeadingMagic`. | FK-bearing table TDEF magic assertions pass. |
| H49 | Newly appended user-table data pages were not recorded in the per-table owned/free usage maps. DAO sequential and snapshot recordsets walk those maps, so rows existed on disk but could appear invisible to DAO. | [JetDatabaseWriter/Pages/DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs): mark newly appended user-table data pages in both owned and free usage-map rows. The INLINE row body is `type byte + int32 start_page + 64 bitmap bytes`; bitmap bits start at row offset `+5`. | `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` passes. |
| AutoNumber-adj | DAO chooses the next AutoNumber from the TDEF high-water value at offset `0x14`. The writer generated unique IDs for its own rows but did not persist that high-water value, so DAO reused an existing ID. | [JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs): after successful inserts, write the maximum inserted AutoNumber value to `Constants.TableDefinition.AutoNumberOffset` in [JetDatabaseWriter/Constants.cs](../../JetDatabaseWriter/Constants.cs). | `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` passes. |

## 4. Layout Facts To Preserve

| Area | Fact | Source / verification |
|---|---|---|
| TDEF length | Jet4/ACE indexed tables include 8 zero bytes after the `0xFFFF` index-name sentinel inside `tdef_len`. | H48 DAO baseline diff. |
| TDEF free space | `freeSpace` changes with `tdef_len`; the H48 fix widens both consistently. | H48 DAO baseline diff. |
| Usage-map inline row | INLINE usage-map rows are 69 bytes: `+0` type, `+1..+4` little-endian `start_page`, `+5..+68` bitmap. | DAO-authored low-page and high-page usage-map baselines. |
| Usage-map scope | Writer-created user-table TDEFs can be marked in place. Pre-existing system-table TDEF usage maps must not be mutated by this path. | Marking system-table maps caused DAO `OpenDatabase` to raise `"Invalid argument"`. |
| AutoNumber high-water | TDEF bytes `0x14..0x17` store the last issued AutoNumber value, not the next value. | DAO-authored 10-row AutoNumber fixture stamped `0A 00 00 00`. |
| Real-idx descriptor magic | Jet4 real-idx physical descriptors use leading magic `0x00000783`, not the format-wide TDEF magic `0x00000659`. | H48-adj fix and TDEF magic assertions. |

## 5. Disconfirmed Causes

Do not re-test these for the original `OpenRecordset` failure unless a new fixture changes the table shape in a relevant way.

| ID / group | Claim | Result |
|---|---|---|
| H46 | Writer-emitted `LvProp` content caused DAO `OpenRecordset` failure. | Disconfirmed. Suppressing `BuildLvPropBlob` did not fix DAO. The catalog row `LvProp` column is not the cursor-materialization blocker. |
| H47 | TEXT/MEMO `ExtraFlags` byte 16 must be `0x00` instead of compressed Unicode `0x01`. | Disconfirmed. DAO accepts both states. |
| H21, H23, H24, H26-H28, H32-H45 | Real-idx pointers/flags, logical-index sentinels, selected column descriptor bytes, empty PK leaf header, and usage-map pointer presence. | Disconfirmed for the `RT_Customers` indexed-table fixture; writer matched DAO or the difference was benign. |
| Earlier compact/OpenRecordset hypotheses | LvProp chained-LVAL shape, writer-private NOT NULL flag, TDEF magic in established create paths, DB-header modify counter, page 1 GPM updates, `MSysACEs`, catalog-name encoding, and index-leaf entry mechanics. | Disconfirmed in [round-trip-test-failures.md](round-trip-test-failures.md). |

## 6. Open Gaps

| Priority | Test | Current symptom | Most likely area |
|---:|---|---|---|
| 1 | `DaoRelationshipEnforcement_FkViolation_RaisesError` | DAO accepts an orphan child insert instead of raising an FK violation. | `MSysRelationships.grbit`, `MSysRelationships` row values, and per-TDEF FK logical-entry fields. |
| 2 | `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` | DAO Compact succeeds, but post-compact row count is 0 for FK-bearing writer-created tables. | FK metadata interaction with Compact & Repair, data-page usage maps, and relationship backing indexes. |
| 3 | `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` | Same post-compact row loss as the single-column FK case, with composite keys. | Same as priority 2, plus composite key ordering and relationship-column ordinals. |
| 4 | `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` | DAO `CompactDatabase` rejects Agile-encrypted writer ACCDB with `"Unrecognized database format"`. | Encryption header and per-page encryption compatibility. |

## 7. Current Next Steps

### Step 1: Fix DAO FK Enforcement

| Action | Detail |
|---|---|
| 1. Create DAO baseline | Use DAO to create `Parent(ParentId PK)` and `Child(ChildId PK, ParentId FK)` with enforced referential integrity. Keep the table/column names aligned with `DaoRelationshipEnforcement_FkViolation_RaisesError`. |
| 2. Create writer comparison | Use the writer path from `DaoRelationshipEnforcement_FkViolation_RaisesError` to create the same schema and relationship. |
| 3. Compare catalog rows | Dump `MSysRelationships` for the DAO and writer files. Compare `ccolumn`, `grbit`, `icolumn`, `szColumn`, `szObject`, `szReferencedColumn`, `szReferencedObject`, and `szRelationship`. |
| 4. Compare TDEF FK entries | Diff both endpoint TDEFs. Focus on FK logical-index entries: `index_type`, `rel_idx_num`, `rel_tbl_page`, cascade bytes, backing real-idx slot, and logical-name records. |
| 5. Patch the writer | Change only the empirically mismatched FK metadata fields. Avoid changing relationship runtime enforcement code unless the disk metadata is proven correct. |
| 6. Verify | Run the FK enforcement test with the opt-in environment switch. When it passes, switch it from the known-gap opt-in guard to the normal Microsoft Access guard. |

Suggested verification command:

```pwsh
$env:JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS = "1"
dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoRelationshipEnforcement_FkViolation_RaisesError"
```

### Step 2: Re-test FK Compact Cases

After Step 1 passes, run the two compact tests with the opt-in switch:

```pwsh
$env:JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS = "1"
dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.SinglePk_AndSingleColumnFk_SurviveCompactAndRepair" "JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.CompositePk_AndMultiColumnFk_SurviveCompactAndRepair"
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
| Reproduce original H48 baseline diff | `$env:DIAG_RT_DAO_BASELINE = "1"; dotnet run --project JetDatabaseWriter.FormatProbe` |
| Inspect preserved memo fixture | `$env:DIAG_MEMO_READBACK = "1"; dotnet run --project JetDatabaseWriter.FormatProbe` |
| Run the normal DAO validation class | `dotnet test --project JetDatabaseWriter.Tests --filter-class "JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests"` |
| Run known-gap tests locally | Set `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1`, then run the focused method with `--filter-method`. |

## 9. Reference Sources

Use empirical DAO-authored baselines first. Use these sources only when a baseline diff is insufficient.

| Source | Use |
|---|---|
| [Jackcess](https://github.com/jahlborn/jackcess) | Cross-check table, column, index, usage-map, and property-map writer behavior. |
| [mdbtools](https://github.com/mdbtools/mdbtools) | Historical Jet/MDB internals. Treat offsets as advisory; some are stale. |
| MS-CFB / MS-OFFCRYPTO specs | CFB and encryption-container behavior. No `[MS-MDB]` on-disk table-format spec exists. |
