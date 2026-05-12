# DAO OpenRecordset / Compact Compatibility Log

Companion: [round-trip-test-failures.md](round-trip-test-failures.md)

## 1. Current Status

| Field | Value |
|---|---|
| Original issue | DAO `OpenRecordset` rejected writer-created user tables with `"Unrecognized database format ''."` even though `OpenDatabase` succeeded. |
| Original issue status | Resolved on 2026-05-10. |
| Follow-up status | Resolved on 2026-05-12. OpenRecordset, MEMO fidelity, AutoNumber continuation, DAO FK enforcement, FK Compact & Repair, and encrypted compact now pass on Access-equipped hosts. |
| Remaining known compatibility gaps from this investigation | None. The only skipped tests in the full suite are diagnostic long-row probes that are intentionally not regression tests. |
| Current date of this log | 2026-05-12. |

## 2. Test Matrix

| Bucket | Test | Guard | Current result |
|---|---|---|---|
| Original issue | `DaoOpenRecordset_RowCount_MatchesWriterOutput` | Requires Microsoft Access | Passing |
| Original issue | `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` | Requires Microsoft Access | Passing |
| Adjacent issue | `DaoRelationshipEnforcement_FkViolation_RaisesError` | Requires Microsoft Access | Passing |
| Former gap | `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` | Requires Microsoft Access | Passing |
| Former gap | `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` | Requires Microsoft Access | Passing |
| Former gap | `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` | Requires Microsoft Access | Passing |

Full-suite verification after these fixes: `dotnet test --project JetDatabaseWriter.Tests` passed with 3234 succeeded, 0 failed, and 2 intentionally skipped diagnostic probes.

## 3. Confirmed Fixes

| ID | Problem | Fix | Verification |
|---|---|---|---|
| H48 | Jet4/ACE TDEF `tdef_len` was 8 bytes shorter than DAO's for tables with at least one index. DAO-authored TDEFs count 8 trailing zero bytes after the `0xFFFF` no-usage-map sentinel; the writer did not. | [JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs): after writing the `0xFFFF` sentinel, advance `namePos += 8` when `jet4 && numIdx > 0`. | `rt-dao-baseline` FormatProbe mode reported `OpenRecordset('RT_Customers')` exit=0 for both writer and DAO copies. `DaoOpenRecordset_RowCount_MatchesWriterOutput` passes. |
| H48-adj | FK real-idx physical descriptors used the format-wide TDEF magic rather than DAO's Jet4 real-idx magic. | [JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs): use `Constants.TableDefinition.Jet4.RealIdx.LeadingMagic` (`0x00000783`). | FK-bearing table TDEF magic assertions pass. |
| H49 | Newly appended user-table data pages were not recorded in the per-table owned/free usage maps. DAO sequential and snapshot recordsets walk those maps, so rows existed on disk but could appear invisible to DAO. | [JetDatabaseWriter/Pages/DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs): mark newly appended user-table data pages in both owned and free usage-map rows. The INLINE row body is `type byte + int32 start_page + 64 bitmap bytes`; bitmap bits start at row offset `+5`. | `DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly` passes. |
| AutoNumber-adj | DAO chooses the next AutoNumber from the TDEF high-water value at offset `0x14`. The writer generated unique IDs for its own rows but did not persist that high-water value, so DAO reused an existing ID. | [JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs): after successful inserts, write the maximum inserted AutoNumber value to `Constants.TableDefinition.AutoNumberOffset` in [JetDatabaseWriter/Constants.cs](../../JetDatabaseWriter/Constants.cs). | `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert` passes. |
| FK-enforcement-1 | DAO was not reliably reporting orphan-insert failures in the PowerShell validation harness because `Database.Execute(sql)` without options can suppress engine errors. | Use `$db.Execute(sql, 128)` (`dbFailOnError`) in DAO scripts that are meant to assert constraint failures. | The orphan-insert probe returns DAO error `-2146825087` and leaves row counts unchanged. |
| FK-enforcement-2 | Writer FK logical-index entries did not match DAO cross-reference semantics. | Parent-side FK logical entry uses `rel_tbl_type = 0x01`; child-side uses `rel_tbl_type = 0x02`; `rel_idx_num` points to the partner logical-index number, not the partner real-index slot. Self-referential relationships reserve distinct logical numbers from one original TDEF snapshot. | `CreateRelationshipAsync_SingleColumn_EmitsDaoCompatibleFkCrossReferences` passes; `DaoRelationshipEnforcement_FkViolation_RaisesError` passes. |
| FK-enforcement-3 | Rebuilt real-index descriptors for FK-bearing tables had `used_pages = 0`. DAO FK enforcement needs a valid index page usage-map pointer even when `first_dp` points at a valid B-tree. | Bulk `MaintainIndexesAsync` records every rebuilt B-tree page per real index and patches each rebuilt real-index `used_pages` slot. | DAO rejects invalid child rows after writer-created FK metadata. |
| FK-compact-1 | System-table rows for `MSysObjects`, `MSysACEs`, and `MSysRelationships` could be appended onto pages outside the system table's owned usage map. DAO Compact & Repair trusted those usage maps and pruned otherwise valid rows. | [JetDatabaseWriter/Pages/DataPageInserter.cs](../../JetDatabaseWriter/Pages/DataPageInserter.cs): for system-table TDEF pages, scan existing mapped data pages and insert there when space is available instead of appending an unmapped page. | FK Compact & Repair no longer drops writer-created relationship/catalog rows. |
| FK-compact-2 | Writer-created relationships lacked the DAO-shaped Type=8 `MSysObjects` relationship object and relationship ACE rows. | [JetDatabaseWriter/Catalog/CatalogWriter.cs](../../JetDatabaseWriter/Catalog/CatalogWriter.cs) and [JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs): insert the relationship catalog object under the relationships pseudo-object and add DAO-shaped owner/group ACE rows. | Single-column and composite FK Compact & Repair tests pass. |
| FK-compact-3 | Rebuilt index usage-map rows were allocated on a separate page from the table owned/free rows, diverging from DAO's compact-safe shape. Single-leaf rebuilds also orphaned leaves unnecessarily. | [JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs) and [JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs): store table used/free rows and real-index rows in one usage-map page, update rows in place, and reuse an existing single leaf when the rebuilt tree still fits on one page. | FK Compact & Repair tests pass and incremental single-leaf tests now assert stable leaf counts. |
| Agile-compact-1 | Writer `AccdbAgile` output used a CFB Office Crypto wrapper, which DAO rejects as an ACCDB database format for modern password-protected Access files. | [JetDatabaseWriter/Encryption/EncryptionConverter.cs](../../JetDatabaseWriter/Encryption/EncryptionConverter.cs) now emits Access-native flat Agile ACCDBs for `AccessEncryptionFormat.AccdbAgile`: `EncryptionInfo` is embedded in page 0 and data pages are AES-CBC encrypted in place. The Office Crypto wrapper remains explicit via `AccessEncryptionFormat.AccdbAgileCfb`. | DAO can open writer-created Agile ACCDBs. |
| Agile-compact-2 | The flat Agile encoding key at page-0 offset `0x3E` was read/written as raw on-disk bytes even though it lives inside Access's fixed masked header region. | [JetDatabaseWriter/Encryption/EncryptionManager.cs](../../JetDatabaseWriter/Encryption/EncryptionManager.cs) exposes the symmetric page-0 header mask transform; [JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs](../../JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs) reads/writes the flat Agile encoding key through that transform and derives per-page IVs from the unmasked key. | The reader can open DAO-created encrypted ACCDBs, and DAO can compact writer-created encrypted ACCDBs. |
| Agile-compact-3 | The cheap encryption-format detector only read `0x80` bytes, so it missed flat Agile metadata at page-0 offset `0x299`. | [JetDatabaseWriter/Encryption/EncryptionManager.cs](../../JetDatabaseWriter/Encryption/EncryptionManager.cs): read the full Jet4/ACE header page for detection. | `EncryptAsync`, `ChangePasswordAsync`, stream overloads, and detection tests pass for `AccessEncryptionFormat.AccdbAgile`. |
| DAO compact call shape | DAO `CompactDatabase(src, dst, dstLocaleWithPwd)` does not supply the source password for encrypted sources; it can report `"Not a valid password"` even when `OpenDatabase` works. | Use the five-argument form: `CompactDatabase(src, dst, dstLocaleWithPwd, 0, srcPwd)`. Destination `PWD` keeps the compacted file encrypted; source `PWD` lets DAO read the encrypted source. | `DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds` passes. |

## 4. Layout Facts To Preserve

| Area | Fact | Source / verification |
|---|---|---|
| TDEF length | Jet4/ACE indexed tables include 8 zero bytes after the `0xFFFF` index-name sentinel inside `tdef_len`. | H48 DAO baseline diff. |
| Usage-map inline row | INLINE usage-map rows are 69 bytes: `+0` type, `+1..+4` little-endian `start_page`, `+5..+68` bitmap. | DAO-authored low-page and high-page usage-map baselines. |
| User-table usage maps | Writer-created user-table data pages are marked in the per-table owned/free maps. | DAO `OpenRecordset` and Compact & Repair validations. |
| System-table inserts | Do not patch pre-existing system-table TDEF maps from the generic user-table page-allocation path. Instead, insert new system rows onto existing mapped system data pages whenever possible. | Marking system-table maps caused DAO `OpenDatabase` to raise `"Invalid argument"`; reusing existing system pages made compact preserve relationship/catalog rows. |
| Real-index usage maps | DAO-compatible table usage-map pages can hold table used/free rows at rows 0/1 and one real-index usage row per real index at rows 2+. Rebuilt descriptors should point to those rows. | FK compact root cause and fix. |
| Single-leaf index maintenance | When a rebuilt index still fits on the existing single leaf, rewrite that leaf in place and keep `first_dp` stable. | Full suite and updated `IndexIncrementalMaintenanceTests`. |
| AutoNumber high-water | TDEF bytes `0x14..0x17` store the last issued AutoNumber value, not the next value. | DAO-authored 10-row AutoNumber fixture stamped `0A 00 00 00`. |
| Real-idx descriptor magic | Jet4 real-idx physical descriptors use leading magic `0x00000783`, not the format-wide TDEF magic `0x00000659`. | H48-adj fix and TDEF magic assertions. |
| FK logical cross-reference | On DAO-authored FK pairs, `rel_tbl_type` is side-specific (`0x01` parent side, `0x02` child side) and `rel_idx_num` cross-references the partner logical-index number. | `fk-dao-baseline` FormatProbe mode writer-vs-DAO dump. |
| Relationship catalog object | DAO writes a Type=8 `MSysObjects` row for a relationship under the relationships pseudo-object, plus relationship ACE rows. | FK compact fix. |
| DAO text-column LvProp shape | DAO programmatic table creation emits `AllowZeroLength = False` for text columns, skips `Required` for AutoNumber columns, emits `Required = True/False` for non-AutoNumber text columns, writes Boolean property entries with `ddlFlag = 0x01`, and uses property-block chunk subtype `0x0001`. | `fk-dao-baseline` raw `MSysObjects.LvProp` hex diff. This is a compatibility fact, not by itself the compact root cause. |
| Flat Agile metadata | Access-native Agile ACCDB stores the encoding key at unmasked page-0 offset `0x3E`, the `EncryptionInfo` length at `0x299`, and the Agile `EncryptionInfo` blob at `0x29B`. The key at `0x3E` is masked on disk with the fixed page-0 header mask. | DAO-created encrypted ACCDB baseline and Jackcess `PageChannel.readRootPage` behavior. |
| Flat Agile page IV | Data-page IV block key is `uint32_le(pageNumber) XOR encodingKey`, then `SHA512(keyDataSalt || blockKey)[:blockSize]`. Page numbers are one-based for data pages. | Verified by reading DAO-created encrypted ACCDBs and DAO-opening/compacting writer-created encrypted ACCDBs. |
| DAO compact verification | `CompactDatabase` exit code 0 is insufficient as a compatibility signal. Always reopen the compacted file and verify schema, relationships, row counts, and password handling when applicable. | Historical compact failures returned success while dropping rows/objects. Current tests perform post-compact reopen checks. |

## 5. Disconfirmed Or Insufficient Causes

Do not re-test these for the original `OpenRecordset` failure unless a new fixture changes the table shape in a relevant way.

| ID / group | Claim | Result |
|---|---|---|
| H46 | Writer-emitted `LvProp` content caused DAO `OpenRecordset` failure. | Disconfirmed. Suppressing `BuildLvPropBlob` did not fix DAO. The catalog row `LvProp` column is not the cursor-materialization blocker. |
| H47 | TEXT/MEMO `ExtraFlags` byte 16 must be `0x00` instead of compressed Unicode `0x01`. | Disconfirmed. DAO accepts both states. |
| H21, H23, H24, H26-H28, H32-H45 | Real-idx pointers/flags, logical-index sentinels, selected column descriptor bytes, empty PK leaf header, and usage-map pointer presence. | Disconfirmed for the `RT_Customers` indexed-table fixture; writer matched DAO or the difference was benign. |
| Earlier compact/OpenRecordset hypotheses | LvProp chained-LVAL shape, writer-private NOT NULL flag, TDEF magic in established create paths, DB-header modify counter, page 1 GPM updates, `MSysACEs`, catalog-name encoding, and index-leaf entry mechanics. | Disconfirmed in [round-trip-test-failures.md](round-trip-test-failures.md). |
| FK compact partial fixes | DAO-shaped `MSysRelationships` rows, FK logical cross-references, non-zero real-index `used_pages`, DAO-shaped ACE inheritance flags, and DAO-shaped text-column LvProp blobs. | Each is either necessary or DAO-observed, but they were insufficient until system-table page placement, Type=8 relationship catalog rows, relationship ACE rows, and table/index usage-map row placement were fixed. |
| CFB Agile wrapper for Access-native password databases | Hypothesis: modern Access/DAO encrypted `.accdb` output is a CFB Office Crypto package. | Disconfirmed for DAO compact compatibility. DAO-created password-protected ACCDBs use flat Access-native Agile encryption, not a CFB wrapper. The library still understands and can emit Office Crypto CFB containers via `AccessEncryptionFormat.AccdbAgileCfb`, while `AccessEncryptionFormat.AccdbAgile` writes the flat Access-native layout. |

## 6. Closed Gaps

| Former gap | Resolution |
|---|---|
| Plain OpenRecordset rejection | Resolved by TDEF length/trailer, real-idx magic, column/property, and data-page usage-map fixes. |
| DAO FK enforcement | Resolved by DAO-shaped FK logical cross-references, non-zero real-index usage-map pointers, and `dbFailOnError` DAO harness usage. |
| Plaintext FK Compact & Repair | Resolved by system-table page reuse, Type=8 relationship catalog objects, relationship ACEs, shared table/index usage-map rows, and in-place single-leaf reuse. |
| Encrypted DAO CompactDatabase | Resolved by flat Access-native Agile encryption, masked encoding-key handling, one-based flat-page IV derivation, full-header detection, and the five-argument DAO `CompactDatabase` source-password form. |

## 7. Verification Commands

```pwsh
dotnet test --project JetDatabaseWriter.Tests --filter-class "JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests"
```

```pwsh
dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.SinglePk_AndSingleColumnFk_SurviveCompactAndRepair"
dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.CompositePk_AndMultiColumnFk_SurviveCompactAndRepair"
```

```pwsh
dotnet test --project JetDatabaseWriter.Tests
```

Useful probes remain available for future regressions:

| Goal | Command |
|---|---|
| Reproduce original H48 baseline diff | `dotnet run --project JetDatabaseWriter.FormatProbe -- rt-dao-baseline` |
| Inspect preserved memo fixture | `dotnet run --project JetDatabaseWriter.FormatProbe -- memo-readback` |
| Compare writer-vs-DAO FK metadata | `dotnet run --project JetDatabaseWriter.FormatProbe -- fk-dao-baseline` |

## 8. Reference Sources

Use empirical DAO-authored baselines first. Use these sources only when a baseline diff is insufficient.

| Source | Use |
|---|---|
| [Jackcess](https://github.com/jahlborn/jackcess) | Cross-check table, column, index, usage-map, property-map, and flat Agile header-mask behavior. |
| [mdbtools](https://github.com/mdbtools/mdbtools) | Historical Jet/MDB internals. Treat offsets as advisory; some are stale. |
| MS-CFB / MS-OFFCRYPTO specs | CFB and Office Crypto container behavior. Access-native flat Agile embeds the Agile descriptor in the ACCDB header page rather than wrapping the database in CFB. No `[MS-MDB]` on-disk table-format spec exists. |
