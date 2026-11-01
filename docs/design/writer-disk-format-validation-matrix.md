# Writer Disk-Format Validation Matrix

**Status:** Incomplete interoperability evidence. Test presence and library round trips do not establish Microsoft compatibility. The open requirements are in [the project plan](../../PLAN.md).

This document is the compact map of writer-emitted Access disk-format surfaces, their base fixture or oracle, their strongest automated validation signal, and the residual trigger that would justify more coverage. General validation rules live in [dao-validation-strategy.md](dao-validation-strategy.md). Detailed compatibility history lives in [round-trip-openrecordset-hypothesis.md](round-trip-openrecordset-hypothesis.md) and [catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md).

`Jet3NativeBootstrapTests` compares fresh Jet3 core table schemas, catalog owners
and permission rows against both the frozen DAO3.6 fixture and an independently
DAO-created database. Targeted x86 DAO3.6 checks cover native open/read/insert/
update/delete, first relationship creation after writer reopen, orphan refusal,
cascades and CompactDatabase with managed compacted readback. Fresh system map
maintenance also covers REFERENCE rows and isolated shared rows and bitmaps;
`Jet3SystemOwnedMapTests` covers native empty relationship storage and
byte-preserving refusal when ownership cannot be established. These
checks establish the standard unencrypted CP1252/General 97 bootstrap; they do
not establish every Jet3 feature or map variant. See
[Jet3 definition metadata](jet3-definition-metadata.md#fresh-database-bootstrap).

Page-kernel conformance retains complete independent byte-image controls in
[IndexPageEmissionKernelTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexPageEmissionKernelTests.cs)
for Jet3/Jet4/ACE leaf and intermediate pages, including prefix compression,
full-width pointers, bitmap boundaries and sentinel/free space.
[DataPageReclaimKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/DataPageReclaimKernelTests.cs)
compares complete compacted images, flags, aliases, secure clear and untouched
pooled suffixes across those formats. These checks qualify implementation
correspondence for the retained layouts; the existing Microsoft-authored/native
controls below remain the independent format evidence. Generated execution does
not broaden interoperability or page-reclamation authority.

## Individual complex items and append-only Memo history

`ComplexItemIdentityTests` compares native AutoNumber identities with direct reads,
row cells, reopen and DAO compact. `ComplexItemMutationTests` verifies scoped
remove/replace, sibling preservation, every child index/count, rollback and complete
byte restoration after partial-write and flush failures. `ComplexItemMutationDaoTests`
starts with DAO-authored attachments and multi-values, accepts writer replacements
and deletions through native child recordsets, extracts a chained payload, performs
another native edit and preserves surviving identities through Compact & Repair.

`ComplexParentKeyDaoTests` independently compares DateTime, GUID, Decimal,
Currency, integral, Single, fixed/variable binary, long Memo, Null/empty and Text
predicates under native General Legacy and General. SQL literals provide the oracle
where DAO parameters would coerce empty text or numeric precision. Short fixed
binary and nonrepresentable numeric predicates cannot manufacture matches.
Attachment extraction succeeds before and after compact.

`AppendOnlyHistoryDaoTests`, `ComplexItemIdentityTests` and
`AppendOnlyHistorySchemaTests` use Microsoft-authored fixtures and fresh DAO controls
to distinguish omitted, explicit Null/empty and repeated same-value assignments,
shared sparse histories, Memo rename/drop, native reads/updates and compact.
`AppendOnlyHistoryMutationTests` supplies statement and transaction rollback evidence.
These controls establish the covered ACE history layouts; the broader format,
collation and property corpora remain governed by the open validation matrix items.

## Schema projection and native metadata

`TDefRewriteBytePreservationTests` marks opaque header, sentinel/trailer,
continuation-header and padding bytes independently of native compatibility,
including definition growth and shrinkage. `NativeTDefChainPreservationTests`
starts with DAO3.6 Jet3 and DAO120 Jet4/ACE wide tables, then checks Add/Drop/Rename,
retained chain bytes, sort metadata, values, indexed reads, relationships and the
absence of temporary catalog objects. Native reads, valid insert/update/delete,
uniqueness and FK enforcement, compact and reopened reads validate the edited files.

`PropertyTargetIdentityTests` and `NativePropertyTargetIdentityTests` keep same-name
column and index property maps separate by subtype. Only maps of explicitly
dropped logical indexes are removed; unrelated and orphan index maps survive.
`IndexDescriptorPreservationTests` checks source physical-index sharing, distinct
metadata identities and descending FK keys across schema edits. Synthetic marker
and native-accepted layout variants remain separate from Microsoft-authored fixtures.
`NativeSchemaPreservationTests` and `NativeSchemaDaoTests` additionally retain
Microsoft-authored Table4 column identities and multi-page attachment table metadata.

`NativeLookupPropertyTests` covers direct and chained lookup sources plus
parameterized SELECT, UNION, crosstab and stored append/update/delete/make-table
QueryDefs. DAO3.6 Jet3/Jet4 controls compare native type and SQL through native
and managed column rename/drop, referenced-table deletion, reopen and compact.
Pinned DAO3.6-authored query specimens retain all query rows, query catalog
values, query permission rows and lookup properties through every writer mode
on hosted CI. Saved definitions are never executed. Fresh ACE DAO120 checks,
remaining native query/application forms and Access UI Name AutoCorrect remain
separate S9/E2 evidence gaps.

`NativeSqlSpecificQueryPreservationTests` adds pinned DAO3.6-authored Jet3/Jet4
stored data-definition (Type 96), row-returning pass-through (112) and action
pass-through (144) specimens. All write modes retain complete query/catalog
values and every query's permission rows through column rename, case-only
rename, column drop and referenced-table deletion. Native edits and both native
and writer-output compact/reopen retain exact SQL, Connect, ReturnsRecords,
ODBCTimeout, MaxRecords and long Description properties. A deliberate DSN-byte
change proves that the snapshot detects changed connection metadata. Targeted
DAO3.6 controls ran successfully with `dao360.dll` 10.0.26100.5074. The inert
connections are never opened and the stored definitions are never executed;
these checks do not establish pass-through execution, ACE variants or UI
behavior. [Fixture provenance and hashes](../../tests/JetDatabaseWriter.Tests/Databases/SqlSpecificQueries/README.md)
retain the exact native specimen scope.

## Microsoft interoperability evidence

`NativeFixedNullScalarTests` independently compares DAO3.6-authored Jet3/Jet4
Byte/Integer/Long/Currency/Single/Double/DateTime/GUID Null-slot geometry with
writer insert/update and native update/compact. `FixedNullScalarTests` covers
all declared scalar widths and no-allocation oversized-row refusal. The writer
reserves complete fixed storage before variable data even for Null values.
`NativeDroppedNullSlotTests` adds four pinned DAO3.6-authored Jet3/Jet4 source
and compacted controls after dropping leading/middle/trailing fixed fields and a
variable field beside surviving Null Double/GUID values. Public typed/string/POCO
reads, insert/update, native reopen/update/compact and independent old/new row
geometry qualify those shapes. Producer details and hashes are in
`Databases/DroppedNullSlots/README.md`; native-only source and compacted fixtures
are separate from writer-mutated subjects. Fresh DAO120 Jet4/ACE confirmation,
native Numeric/Large Number/DateTime Extended, remaining dropped-slot variants
and broader record-limit acceptance remain F8/E2 gaps. Existing approved golden
specimens are unchanged.

Validation on 2026-10-06 used revision
`4b347f95886abdd72aadcfa59a997652e46badea`:

- [Windows CI 37571001909](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/37571001909)
  passed all six jobs: Release analyzers/package checks and both ordinary test
  legs. Each leg had zero failures; its 50 skips were the 47 guarded DAO cases
  and three explicit-only fuzz harnesses.
- [Portability 37571002529](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/37571002529)
  passed Release builds and ordinary tests on Linux and macOS, on net10.0 and
  net8.0. The net8.0 leg loads the netstandard2.1 library.
- [Bounded fuzz 37571005439](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/37571005439)
  passed 64 cases per harness and framework, 384 total, seed `20261008`.
  Each process had a 30-second timeout and 512 MiB memory limit.
- Matching Windows CI-built runners executed all 43 guarded DAO methods locally:
  **45 cases passed, 0 failed, 2 skipped on each framework**. The installed
  DAO120 engine activated through x64 hosts on Windows Arm64. Its Access97
  refusal caused both Jet3 skips; DAO3.6 registration alone is not activation
  evidence. No ordinary tests, builds or fuzzers ran locally.

Passing DAO cases include native JET4 encrypted DML/DDL/rollback and write/compact,
native ACE Agile mutation and compact, unenforced relationship deletion,
calculated/complex schema rewrites, table-rule rename/set, text comparisons,
BINARY values and owned-map growth. Fresh Jet4 creation includes native FK
rejection and exact compacted relationship/row readback. The three native
qualified calculated-reference forms survive writer mutation, column rename
and DAO compact/readback. Targeted x64 DAO120 checks on both frameworks also
verify read/write/compact after scrub and physical shrink of the native JET4 RC4
and ACE Agile fixtures. `TableRenamedQualifiedExpressionTests` separately
records DAO120 table-rename behavior: bare qualifiers bind to the current row,
including a name shared with another existing table. Writer insert, update
and column rename preserve that behavior through native update and compact,
without changing the other table. `QualifiedReservedFieldTests` verifies that
qualified `vbTrue`, `Yes`, `Null`, `And`, `Mod` and `Date` names bind actual
fields and keep rename/drop dependencies through native recalculation/compact.
Stale bracketed qualifiers yield native Null
and compact error 3790; writer recalculation of those variants remains refused
without byte changes. Nine whitespace variants of qualified calculated
references are refused without adding a field; their adjacent controls succeed
in the same DAO databases. See [Jet4 bootstrap security](jet4-bootstrap-security.md)
and [calculated expression notes](calculated-columns-format-notes.md).

Logs, XML results, matching runners, fuzz manifests and native oracle databases
are retained locally under `artifacts/retained/jet-ace-20261006/`. The source
regressions reproduce the checks on a compatible DAO host. These results prove
the tested cases, not complete format coverage or resistance to every hostile
input; subsequent code changes require matching-revision validation.

| Generation | Available evidence | Required evidence |
|---|---|---|
| JET before3 / Access1.x–2.0 | No supported parser or verified fixtures in the current matrix | Microsoft-authored corpus, format identification and read/write implementation |
| JET3 / Access97 | Microsoft-authored fixtures, x86 DAO36 fresh creation, mutation/relationship enforcement/compact and native scalar/wide indexed schema rewrites | Broader feature, collation, security and storage variants |
| JET4 / Access2000–2003 | Microsoft-authored fixtures and successful guarded DAO tests, including fresh creation with an enforced relationship | Complete feature, code-page, collation, security and malformed-file coverage |
| ACE / Access2007 and later | Microsoft-authored fixtures and successful guarded DAO cases | Later descriptor, complex, collation and encryption variants |
| Encryption variants | DAO-produced JET3/JET4 password and RC4 fixtures, custom workgroup security, Microsoft-compacted Standard and mixed Agile, plus pinned RC4 CryptoAPI and compatibility AES fixtures verified by DAO; [hashes and producer details](native-encryption-evidence.md) | Third-party extensible/non-AES providers and power-loss durability |

A conditional DAO test is evidence only when it actually ran successfully on
the revision and engine being evaluated.

`NativeJet3SchemaRewriteTests` exercises x86 DAO3.6 on native CP1252/CP932
fixtures and independently DAO-created scalar/wide tables. Add/Drop/Rename
retain the original object ID, owner, permission rows and unchanged catalog
keys. Native read/write/delete and compact cover a 100-column table with 16
indexes, continued writes after reopening, a multilevel catalog and
INLINE-to-REFERENCE owned-map growth. Synthetic duplicate-key refusal retains
all bytes; frozen-fixture key/security comparisons also run without DAO. Catalog malformed-tail, invalid-prefix and missing/past-end bitmap-sentinel regressions require contextual refusal and identical full-file bytes across all three write modes. Valid native empty and compressed catalog leaves remain accepted.
Fresh Jet3 creation is covered separately by `Jet3NativeBootstrapTests`; broader collation variants remain open.

`DaoNestedTransactionTests` compares native Jet3/Jet4/ACE nesting and the seven-level
begin limit and five-level mutation boundary with writer output, followed by
native read and compact. Ordinary nested transaction tests cover Jet3/Jet4/ACE
state restoration, child release, parent
rollback, cancellation, page-budget failure and custom-stream commit faults.
Only the outermost commit performs durable replay; custom streams still have
no persistent recovery contract. See [transaction recovery](transaction-crash-recovery.md).

## Focused correctness regressions

The library regressions below distinguish their round trips from explicit DAO checks:

- `Jet3DescriptorPreservationTests`, `Jet3IndexDefinitionTests` and `DaoJet3CurrencyDefinitionTests` compare DAO3.6-authored metadata with writer output: native physical index flags, stale optimizer statistics, column bytes 7–12, page-zero tail preservation, Currency descriptors and ascending/descending keys. New tables on native hosts and original native tables survive writer DML and Add/Drop/Rename followed by native mutation and compact; a hash-pinned DAO3.6 fixture retains ordinary CI coverage. `NativeJet3CatalogSecurityTests` verifies exact native permission inheritance and malformed-security refusal; `Jet3CatalogPropertyStorageTests` pins external LvProp storage and atomic rollback. The exact scope and uninterpreted bytes are recorded in [Jet3 definition metadata](jet3-definition-metadata.md). Fresh Jet3 creation and native core metadata are covered separately by `Jet3NativeBootstrapTests`.

- `ReplicationIdDescriptorTests` pins Microsoft-authored Table4/Table13 GUID descriptors and type-specific flag interpretation. `DaoReplicationIdTests` separately checks native fixture mutation, nullable versus primary-key Null behavior, DAO-created system/plain GUID variants, and writer-created GUID metadata, omitted inserts, explicit key updates, schema edits and compact/readback. DAO-created `GenGUID()` defaults use Memo type and DDL flag 1; writer output carries that metadata and preserves existing Microsoft Text/DDL-1 variants. GUID omission/default generation is distinct from explicit Null, and GUID keys remain mutable subject to indexes and relationships. `Guid.Empty` POCO omission is a library mapping convention; it is not native Null or ordinary GUID default behavior.

- `NativeAceCatalogSecurityTests` compares created and rewritten ACE owners/permissions with native MSysDb/container metadata and checks coherent fresh bootstrap. `CatalogOwnedUsageMapTests` checks literal imported system-map membership independently of the reader's ownership fallback. `DaoReplicationIdTests.MicrosoftGuidFixture_SchemaOperation_PreservesNativeCompact` checks native compact after Add/Rename/Drop on the Microsoft ACE GUID fixture; catalog map maintenance preserves the relocated catalog row. This does not establish complete native schema, security or column-owned LVAL map coverage.
- `ForeignKeyCascadeUpdateTests` covers contradictory-path and diamond refusal without byte changes, simple stable cycles and independently allocated equal binary keys across formats and write modes. `ForeignKeyUnequalPathTests` verifies declaration-order independence, accepted unequal-depth convergence and refusal of equal-depth arrivals even after an earlier direct path. `ForeignKeyCompositeCascadeTests` checks physical spelling propagation with an otherwise changing composite key. `DaoCompositeCascadeGraphTests` independently compares update/delete chains, cycles and diamonds on DAO-authored Jet3/Jet4/ACE graphs, including mixed-case keys and unaffected composite neighbors. Header and column descriptors pin LCID `0x0409` with General97, General Legacy version 0 and General version 1; General version 1 uses an Access-authored 2010 host. Native and writer rollback, native reopen and compact/readback verify the rows; refused writer updates preserve bytes. These graph checks do not establish the broader collation inventory required by F9.
- `DaoRelationshipMetadataTests` compares native Jet3/Jet4/ACE logical names/numbers, reciprocal links, backing-index choice, role/cascade bytes and flags across cascade policies, then verifies renamed relationships through native insert/delete, refusal, cascade and compact. Existing statistics slots are preserved; new zero writer slots are accepted before compact and become byte-equal to independently native-compacted statistics. Those Jet3 relationship fixtures retain a native unenforced seed relationship. `Jet3SystemUsageMapDaoTests` independently checks first relationship allocation on empty native core tables and shared/REFERENCE maps. This distinguishes native-authored source metadata from native acceptance of writer mutations.
- `DaoRelationshipCatalogIdentityTests` verifies drop/recreate, caller rollback, rename/drop/name reuse, unrelated object/index preservation, native open and compact on independently DAO-authored Jet3 (DAO3.6), Jet4 and ACE (DAO120) databases. `RelationshipCatalogIdentityTests` adds all write modes, byte-identical rollback and refusal, exclusive identity checks, and complete key deletion for synthetic additional catalog/security indexes.
- `DaoSelfRelationshipUpdateTests` compares simultaneous scalar and non-Null composite self-key assignments on DAO-authored Jet3 (DAO3.6), Jet4 and ACE (DAO120) files. Explicit caller FK assignments survive planning; retaining a removed old parent is refused. `DaoCascadePathTests` compares unequal-depth paths, merged self-key and unrelated caller assignments with downstream cascades, explicit-old-key refusal and caller rollback. It also characterizes native acceptance of contradictory parent assignments that leave one relationship unmatched; the writer deliberately refuses those assignments without changing bytes. Native reopen and compact/readback check both accepted output and refused sources. `DaoJet3CascadeCycleTests` compares two-/three-table update/delete cycles, caller rollback and native compact. Ordinary self-assignment, cascade and update-scope regressions preserve bytes, counts, indexes and transaction state across formats/write modes.
- `DaoCascadeDepthTests` distinguishes native execution limits from the writer's resource bound. DAO3.6 Jet3 and DAO120 Jet4/ACE simple chains accept 50 update edges and refuse 51 with error 3000 / reserved `-1623`; delete succeeds at 65 edges. On DAO-authored ACE chains, writer updates/deletes accept 64 edges, including rollback and native reopen/compact/readback, and refuse 65 without changing bytes in direct, automatic and caller transactions. These are characterized chain shapes, not a universal native graph-size guarantee.

- `DaoPartialNullRelationshipTests` compares exact partial-Null matching against nullable unique parent keys on DAO3.6 Jet3 and DAO120 Jet4/ACE files, including insert/update refusal, cascades, restrictions, relationship creation, the composite self-update reproduction and entirely Null parent dependencies even with duplicate Null parents. Native compact/readback verifies writer output. `ForeignKeyPartialNullTests` covers all formats and write modes, byte-identical refusal, caller transaction recovery and unrelated already-orphaned keys.
- `DaoTextCollationTests` table-rule cases cover getters/setters, rename/drop dependencies, unrelated updates, Null-result acceptance, cascades and transaction rollback, plus interpretation of a native `Partition(...)` table rule and selected DAO formatting boundaries. `PartitionTableValidationRuleTests` separately establish reopened-writer evaluation and byte-identical rejected inserts/updates on library fixtures across Jet3/Jet4/ACE. DAO120 reopens, enforces, writes and compacts the resulting databases. A synthetically Text-typed version of a DAO source rule separately verifies edited-output acceptance while preserving validation text; it is not a native fixture for that encoding. These four guarded cases passed on both target frameworks through x64 DAO120 on 2026-10-08. Broader engine-generation evidence remains in E2.
- `ColumnPropertyBlockTests`, `PersistedPropertyProjectorTests`, `SchemaRewritePropertyPreservationTests` and `TableValidationRuleTests` cover opaque target headers, native name lengths and preservation of entry padding, original indices/order and unused/duplicate names through property edits and schema rewrites. Repeated equivalent rule/text entries and table targets update or clear consistently, while contradictory original repeats refuse mutation. `DuplicateSchemaPropertyTests` covers consistent duplicate/split modeled targets, rename/remove projection, cold-reopen Required/default/rule/calculation binding and metadata, plus byte-preserving refusal of contradictory calculated Expression/ResultType entries. The ordinary repeated-entry/target regressions are synthetic; the separate guarded controls below establish their covered native acceptance. `LvPropReadTests` refuses malformed external property chains before Add/Drop/Rename and table-rule replacement/removal across Jet3/Jet4/ACE and write modes, preserving bytes and caller transaction state. Long-value page pointers are checked against the readable page count before reading. `DaoPropertyMutationTests` verifies DAO120 acceptance, metadata, rule enforcement, data and Compact & Repair for edited single-pool Jet4/ACE sources carrying synthetic padding and duplicate/unused names. `NativePropertyVariantsTests` adds DAO3.6 Jet3 property edits and native acceptance of equivalent repeated rule/text entries and same-subtype table targets across Jet3/Jet4/ACE, including valid inserts, enforcement and compact after schema/property edits. Split table targets expose only the first table target metadata; managed reads match that binding. Empty extra name-pool chunks preserve the active dictionary; adding properties to an empty-only blob installs a populated dictionary before those inert chunks. Tested populated repeats and splits still fail natively before writer edits; a native-accepted multiple-populated-pool layout remains unverified.
- `NativeSchemaPreservationTests` pins the distinct native descriptor identities in Microsoft-authored TestV2010 Table4 through Add/Drop/Rename, noncolliding added identities, and root/continuation/value/index preservation on 202-column attachment tables in every write mode. `ColumnDescriptorPreservationTests` includes identity bytes in surviving-descriptor comparisons. `NativeSchemaDaoTests` verifies native read and compact for the Microsoft fixture and extraction, insert/update/delete and compact for rewritten DAO-authored wide attachment tables. Both framework legs execute these guarded cases through x64 DAO120. Unmodeled TDEF headers/trailers and wider native metadata remain separate preservation requirements.
- `OwnedDataPagesTests` checks full row-directory removal from INLINE/REFERENCE free-space maps and transaction rollback on Jet4/ACE, while retaining ownership. Native free-space thresholds, Jet3 variants and LVAL maps remain open.
- `ReplacementCrashRecoveryTests` terminates processes after backup preparation, replacement and final file flush, then compares complete old/new destination and backup images. `ShrinkCrashRecoveryTests` terminates secure tail writes, truncation and successful shrink, then validates exact plain/encrypted images and retained rows. `CreateDatabaseTests` exercises staged publication, cancellation, destination races and retention after reopen failure. These checks establish process-interruption behavior, not power-loss ordering.
- `AtomicReplacementTests`, `JetByteRangeLockTests`, `CrossProcessLockTests` and `ReplacementCancellationTests` cover pre-cancellation, open-reader replacement, temporary-file cleanup, page/commit lock contention with a separate process on Windows/Linux, and cancellation after the replacement temporary file exists. macOS asserts the intentionally disabled BCL byte-range-lock contract. The hosted Portability workflow exercises Windows, Linux and macOS on both frameworks. Power-loss directory durability and native macOS process exclusion remain unverified.
- `CatalogCorruptionTests`, `CatalogTraversalCorruptionTests`, `CatalogDiagnosticsTests` and `SystemTableLookupTests` refuse malformed live `MSysObjects` layouts/required scalar fields, invalid system/user TDEF roots, duplicate table names, shared roots and ambiguous fallback matches. Strict and lenient scans both refuse corruption on repeated reads without caching partial results. Complete live-row traversal refuses out-of-file, cyclic and truncated overflow headers, impossible row counts and slots overlapping the page header, with `CorruptCatalog` and table/page context before publishing diagnostics or lookup results. Native ODBC definitions still resolve; negative catalog-only ODBC IDs remain metadata identities. `LinkedCatalogValidationTests` extends this to Access/text alias collisions, required linked metadata columns, and retry after storage faults. Catalog-only ODBC identities remain readable without fabricated physical TDEF references or mandatory connection strings.
- `ComplexColumnCatalogValidationTests` verifies required catalog/mapping structure, positive unique parent descriptor IDs, parent/column/flat identities, real template object kinds and compatible payload schemas in strict and lenient reads. Attachment checks cover named fields and types, including zero-template fallback with real payload rows; shared flat tables cannot return through fallback. `MaxComplexDiscoveryEntries` has aggregate refusal and caller-override regressions. `ComplexColumnsInfoTests` and the real Fields/Data fixtures retain valid layouts, while `ComplexColumnsFallbackOwnershipTests` retains descriptor-only fallback. Repeated reads and one-shot I/O faults guard against poisoned lookup state. These corruption mutations are synthetic evidence, not native acceptance of malformed catalogs.
- `ConstraintRegistryTests` checks changed storage types and generated flags, including Currency/Numeric columns with the same CLR type and AutoNumber counter rewind. `ColumnConstraintTests` exercises unsupported persisted insert defaults, unknown-function rules and other runtime-unevaluable rules through every public insert/update overload across Jet3/Jet4/ACE and direct, automatic and explicit transactions. After reopen and rollback, refusals preserve backing bytes and caller transaction state; valid follow-up writes retain AutoNumber continuity. Cached constraints also bind the TDEF root and physical column descriptors: same-name/type/flags replacements at different locations use their own defaults, rules and AutoNumber seed, and snapshot rollback restores the original binding. `ConstraintReplacementTests` verifies same-root and identical column-descriptor reuse through public drop/recreate operations across Jet3/Jet4/ACE, while allowing independently maintained usage-map pointers to differ. Replacement uses its own defaults, CLR and persisted rules, table rule and AutoNumber seed; refusals preserve bytes and caller work, rollback restores the original constraints/counter, and reopen hydrates current properties. `ConstraintReplacementIdentityTests` covers exact same-location descriptors at registry lifecycle boundaries, including unconstrained replacements and snapshot restore. Identity comes from serialized create/drop/rewrite operations and exclusive writer ownership, not an inferred native generation token; external edits require disposal and reopen (see [constraint identity](concurrency-and-lock-ordering.md#constraint-identity-during-schema-changes)). Same-writer parser-unsupported-default tests preserve bytes, caller transaction state and AutoNumber continuity across insert overloads and write modes without reopening or changing the schema. `ParserRefusalConstraintTests` independently verifies reuse of the same seeded counter and cached parser-refusal default plan without reseeding. These cases use invalid expression syntax; an unknown function can compile and fail only at evaluation. Parser-unsupported stored-rule regressions independently seed a hydrated registry counter fixture and pin reuse of the exact counter, physical constraint identity and Unsupported plan through refusal, Id-only updates and snapshot restore without reseeding. Public same-writer tests observe the same tentative generated ID across repeated refusals before, during and after a caller transaction across insert overloads and write modes, preserving backing bytes and pending caller work. Every probe insert is refused, so the seeded-counter fixture and public tentative-ID observations are distinct evidence. These invalid-syntax regressions establish library cache/refusal behavior, not native expression compatibility. Runtime-evaluation cases separately cover cached counter reuse.
- `NativeAceEncryptionTests` requires unchanged page 0 and retained fixture rows after scrub and actual physical shrink. Injected ordinary-write, torn-write and flush failures restore the original ciphertext, after which the same writer can scrub, shrink and insert. These fault checks establish library rollback behavior. Separate subprocess crash tests establish library sidecar recovery of the exact native ciphertext; Access-native crash recovery, power-loss durability remain unverified; separate mixed-Agile shrink fault sweeps verify in-process ciphertext rollback.
- `VariableComplexDecodePolicyTests` covers assigned variable Complex widths, invalid references and missing/reversed offsets through typed/string rows, POCOs, DataTables, queries and index reads. Strict failures carry `MalformedValue` and column context; lenient reads emit a payload-free diagnostic and their missing-value representation. Valid item joins, empty positive references and null/absent slots remain readable. Unrelated writer updates refuse malformed assigned slots with unchanged bytes. Variable descriptors are synthetic writer-created layouts; native fixtures use fixed references.
- `PartitionCalculatedColumnTests` records DAO120 error 3859 for direct, case-insensitive and nested `Partition` calculated-field calls without changing the field collection, followed by a supported calculation in the same database. Library create/add and persisted-expression recalculation refuse the same function with unchanged bytes; field names/string literals remain valid. Native stored-rule support and its write/compact evidence remain separate.
- `IndexPredicateTranslatorTests` and `InferredIndexReadTests` retain narrowing and rounding column conversions as residual predicates. Public typed rows and LINQ queries include wrapped byte-cast matches and preserve checked-cast overflow before a later indexed equality on Jet3/Jet4/ACE, including nested ANDs and consecutive `Where` calls. Extraction stops at the first residual or custom operator; understood leading comparisons still preserve short-circuit behavior. Lossless widening, enum and nullable-lifting controls remain inferable. These are query-correctness checks on library fixtures.
- `ComplexPocoValidationTests` checks that invalid complex POCO payloads and contradictory references raise a named argument error before mutation. Bulk refusal leaves bytes and ordinary/complex counters unchanged across write modes. This validates the reference-only typed insert contract; it does not establish native complex-item mutation or version-history compatibility.

- `SystemLinkedExecutionTests` verifies system/hidden flags cannot bypass linked execution policy while cached ODBC metadata remains readable. `LinkedSourceCapabilityTests` exercises explicit source-stream authorization, refusal across execution APIs, filename swaps, inert alias hints, nested links and ownership on failure/cancellation. Authorization belongs to the exact supplied stream, never a catalog path or a reopened filename; the host supplies a committed, isolated resource and handles physical identity at registration. `LinkedTextTableTests` checks resolver refusal for final text sources across count, metadata and materialized reads. `LinkedTextSourceStreamTests` checks consumed-byte limits when a source grows, exact-limit EOF and empty reads.
- `CveMitigationTests` locates MEMO descriptors using the catalog, physical row location, parsed row trailer and column slice, with valid readback before mutation, across Jet3/Jet4/ACE. Inline overstatement fails against its field bounds; a forged 30-bit chained length raises `ValueTooLarge` in strict and lenient modes before reading its first LVAL page. Read counts are bounded and source bytes remain unchanged. These synthetic corruptions establish library refusal behavior, not native interoperability.

- `IndexTreeReclamationTests` forces multi-page trees on the Access-authored Jet3 index fixture and checks repeated public insert batches do not accumulate writer-created orphans beyond retained native-map pages. Jet3/Jet4/ACE shrink cases check reachable entries and exact rollback bytes. Synthetic shared-map, invalid-map and partial-intermediate cases retain potentially owned pages. Native Jet3 map reclamation and matching-revision DAO compact evidence remain separate requirements.

- `FixedSlotBoundsDecodePolicyTests` mutates fixed descriptor offsets across Jet3/Jet4/ACE and checks contextual strict corruption, lenient missing values, direct/hybrid POCO parity and byte-preserving refusal of unrelated updates. Fixed payloads cannot consume variable data or row trailers; null fields retain their normal meaning. These are synthetic corruption regressions, not native interoperability evidence.
- [The format profile and type table](format-facts.md) own verified header, text, null-mask, row and descriptor facts. `JetTypeInfoMetadataTests` characterizes metadata and physical storage flags for all byte-valued column discriminators and refuses undefined codes before a schema write, including unchanged-byte refusal of unknown native descriptor rewrites. Known mappings and opaque native complex metadata remain readable. `UnknownColumnTypeTests` retains unknown raw descriptors/payloads and diagnostic hexadecimal/string metadata, checks byte-preserving refusal of unrelated populated-row updates, and refuses unknown physical/calculated serialization even for null values. Jet3/Jet4/ACE fixed/variable insert checks cover direct, automatic and explicit write modes, with usable caller transactions after rejection. These synthetic discriminators establish refusal and raw-byte preservation, not codecs or Microsoft provenance for unknown types.

- `InlineLongValueLengthDecodePolicyTests` mutates the high length bits of inline MEMO/OLE descriptors across Jet3/Jet4/ACE. Typed, string, materialized and fallback/hybrid POCO reads enforce all 30 native length bits: budget excess raises `ValueTooLarge` in both modes, and in-budget inline overstatement uses the strict/lenient unreadable-value policy. Refused unrelated-field updates preserve all bytes. These synthetic mutations do not establish native large-value interoperability.
- `RowDecodeParityTests`, `RowValueDecodePolicyTests` and `VariableSlotBoundsDecodePolicyTests` cover malformed variable-area Int32/Decimal and calculated Int32 payloads, truncated assigned slots, reversed Text offsets and calculated envelopes. Direct/hybrid Text controls verify that malformed assigned slots fail while null-mask and absent-column rows remain missing. Strict typed, string, direct/hybrid POCO and materialized reads report `MalformedValue` with column context; lenient reads use missing values and trace diagnostics that omit payload/exception text and tolerate listener failures. Unrelated-column updates refuse unreadable scalar content without changing bytes; valid nulls and native empty calculated caches retain their meaning. I/O, cancellation and resource-limit failures propagate. These are synthetic corruption checks with native null-cache controls.
- `CalculatedNumericDecodePolicyTests` checks incomplete and overstated calculated Decimal payload-length mutations through contextual strict failures and lenient missing-value diagnostics across typed, string, POCO and materialized reads. Unrelated updates preserve bytes on refusal; zero/null controls retain their meaning. The normal sixteen-byte layout has a reserved two-byte word, scale/sign bytes and fixed-offset Hi32/Lo32/Mid32 coefficient words. `CalculatedNumericNativeLayoutTests` independently compares reserved-word mutations and every payload width from four through sixteen bytes with DAO120 reads and compaction. Omitted trailing bytes zero-extend at their fixed offsets; the reserved word is not a mantissa length, a four-byte header represents Decimal zero, and an empty envelope represents ordinary Null. Malformed-payload mutations remain synthetic corruption evidence, distinct from those native layout controls.
- `FixedDateDecodePolicyTests` and `VariableDateDecodePolicyTests` cover malformed classic and Extended dates across supported fixed/variable storage. Strict typed, string, direct/hybrid POCO and materialized reads report `MalformedValue` with column context; lenient reads return their documented missing values and emit a trace warning. An unrelated-column update refuses unreadable dates without changing bytes. Extended cases include non-digits, an invalid trailer, out-of-range time and arithmetic overflow; valid boundaries retain all seven fractional digits. These are synthetic corruption and library round-trip checks.
- `CompoundFileOwnershipTests` checks that the internal CFB parser refuses regular/mini-sector aliases, shared tails, duplicate FAT declarations and metadata/data overlap before allocating an extracted chain buffer. `CompoundFileMetadataTests` additionally checks physically impossible metadata counts, inconsistent starts and chains, extra/missing FAT declarations, I/O propagation and cancellation. `CompoundFileHeaderTests` checks little-endian byte order, zero CLSID/reserved fields and version-4 header padding, version-3 directory count zero, version-4 directory counts against the actual chain, and the standard 4096-byte cutoff. The internal version-3 diagnostic writer's exact zero-cutoff dialect remains supported. True version-4 controls are synthetic writer output; these checks add no native Access evidence. The aggregate payload-buffer budget applies before FAT, mini-FAT, directory, root mini-stream and extracted-stream allocations, including overlapping temporary FAT-ID and mini-FAT conversion buffers. Fixed header/rented scratch storage and collection/object overhead remain outside this byte budget. The Office/OpenMcdf corpus and writer round trips remain the valid-input checks; these synthetic regressions do not establish a process-wide peak-memory limit or complete directory audit.

- CFB output stores sibling streams in a median-balanced, all-black binary search tree, as permitted by [MS-CFB section 2.6.4](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cfb/d30e462c-5f8a-435b-9c4c-cc0b9ea89956). Names sort by UTF-16 code-unit length before per-unit case comparison; supplementary characters retain their surrogate units. The comparer follows the Windows Vista-and-later casing exceptions in [Appendix A](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cfb/c46d0ecf-3836-4a3b-b379-339ab6a11589), and refuses invalid or equivalent duplicate names before writing. `CompoundFileDirectoryTreeTests` independently checks ordering, reachability, depth and payload identity across both writer versions. Native Windows structured-storage probes establish the exceptional casing representatives, distinct dotless-I and supplementary case pairs, and named-stream lookup with exact payloads in emitted version-4 output; these checks do not establish native Access compatibility for Office Crypto wrappers.

- `AttachmentResourceLimitTests` bounds per-value declared content, aggregate content and stored-input/processing work in strict and lenient reads. Multi-table materialization, streamed queries, Include and linked sources share a root budget; concurrent independent operations and reused options have independent scopes. Costly valid deflate streams with many empty blocks check cancellation during the first inflater read, and checksum/extension/copy work uses bounded chunks. Explicit larger caller limits permit reads that default budgets refuse. Cancellation and early disposal reset scopes; reader disposal drains an active query enumerator. `AttachmentCorruptionTests` retains malformed-wrapper refusal. These are synthetic resource/cancellation checks with ordinary attachment fixture controls; they add no native encryption-provider or authentication evidence.

- `LongValueSlotBoundsDecodePolicyTests` mutates assigned MEMO/OLE offsets and removes variable slots on Jet3/Jet4/ACE, including ACE calculated MEMO. Strict string, typed, POCO, OLE hybrid and materialized reads report contextual long-value corruption; lenient reads return their documented missing values with payload-free diagnostics and tolerate listener failures. Writer updates refuse before changing bytes. Null-mask and absent-column controls remain missing without diagnostics; variable Complex assignment checks use synthetic decode plans, not native variable Complex fixtures.
- `IndexCursorTests` checks self-cycles and two-leaf cycles before any leaf is reread in Contains, exact/list-range and streaming traversal. Cycles raise `CorruptIndex` with page context instead of hanging or silently completing with partial matches. Matching valid chains and cancellation controls preserve normal reads. These synthetic checks establish bounded leaf traversal, not complete index corruption auditing or Microsoft interoperability.

`AttachmentWrapperDaoTests` creates extensionless, empty, raw, compressed,
uppercase and longer-extension attachments through DAO120 LoadFromFile.
It verifies native on-disk headers, preserved FileType case, raw wrapper parity
and compressed content/header/checksum parity with writer copies. DAO extracts
both native and writer payloads before and after compact; captured native
empty/extensionless wrappers also run as ordinary decoder regressions.
`AttachmentResourceLimitTests` separately verifies per-value, aggregate-content
and decode-work budgets; these synthetic refusal checks do not establish
native acceptance of malformed wrappers. `ComplexDescriptorVariantTests` pins
native primitive multi-value, Decimal, calculated and append-only descriptors,
late-added shared references and a counter above live references. DAO reads,
mutates and compacts default/custom Decimal output and refuses duplicate items
with error 3820. Ordinary duplicate-item regressions cover every native primitive
type and writer mode, including exact-byte refusal and unconsumed counters.

`tests/JetDatabaseWriter.Tests/Databases/ComplexDescriptorVariants.accdb` was produced
by Microsoft's DAO/ACE 16.0 engine (Office file version 16.0.20430.20146), through
an x64 PowerShell host on Windows Arm64. Its SHA-256 is
`A4ABCEF7D615FCBF532E8615BF2405F36D4E44E24C992665927293058106052F`
(462,848 bytes). It contains eight primitive multi-value kinds, append-only Memo
history, a late-added Text multi-value column and a deleted highest parent.

`NativeCompressedTextDecodeTests` pins six tiny Text field vectors and their
hashes. The provenance distinguishes DAO-authored mixed/low-zero text from
separately specified native-read suffix and malformed-tail controls, with native
database hashes, field offsets and exact UTF-16 results. The managed decoder
uses the native single-zero mode transition while retaining field bounds for an
incomplete final Unicode unit; it does not copy a native corrupt-slot overread.

`NativeMemoCompressionTests` pins Jet4/ACE databases whose row payloads were
authored and reread ordinally by DAO120 16.0 (ACEDAO 16.0.20430.20182) in
writer-created empty schemas. Portable tests cover inline/single-row compressed
values, plain chains, escaped small literal U+FEFF and plain chained U+FEFF
through typed, string, POCO and DataTable reads. Guarded controls compare writer
Memo boundaries and large literal U+FEFF before and after native compact.
Write-back, schema rewrite and calculated-envelope regressions separately check
plain payload preservation. Unicode-compressed ordinary Memo accepts leading
U+FEFF at 6 through 2048 characters with a nonzero Latin-1 tail, matching frozen
native bytes and DAO3.6 Jet4 read/compact checks. Other short marker forms refuse
without changing bytes; their native encoding and fresh ACE checks remain open
under F8/E2. These
checks do not establish every native text variant or a complete managed compact.

## Validation Levels

Use the canonical ladder in [dao-validation-strategy.md](dao-validation-strategy.md). In this matrix, the levels are named as follows:

- **Reader round-trip:** writer output is reopened through JetDatabaseWriter's reader and assertions cover schema, data, index metadata, or decoded payloads.
- **DAO OpenRecordset:** a bitness-matched Windows PowerShell host drives `DAO.DBEngine.120` against writer output and materializes recordsets or DAO index seeks.
- **DAO CompactDatabase:** DAO compacts writer output, then JetDatabaseWriter reopens the compacted file and verifies schema, data, relationships, passwords, payloads, or page-reuse invariants.

Manual Microsoft Access UI verification is supplemental evidence. It remains useful for workflows DAO does not expose, such as saving an attachment back to disk through the UI, but it is not stronger than DAO automation when the same scenario can be automated.

## Update Rules

- When a writer disk-format feature changes, add or update the strongest feasible validation signal and refresh the relevant row below.
- For every DAO CompactDatabase test, reopen the compacted output and assert the feature that motivated the test.
- Prefer Access-authored fixtures such as [NorthwindTraders.accdb](../../tests/JetDatabaseWriter.Tests/Databases/NorthwindTraders.accdb), DAO-authored scratch databases, or Access/DAO-authored payload bytes as the oracle side of DAO/Access compatibility tests. Writer-created fresh databases are subjects under test, not fixture oracles; use them only when validating bootstrap or generated writer output itself.
- Promote any new DAO baseline probe invariant into a focused regression test.
- If a residual gap is accepted, name the trigger that would make it release-relevant instead of leaving a standing backlog item.

When the matrix says "writer-created", read that as the output under validation. Do not infer that a writer-created database is an engine-authored source of truth.

`CatalogSchemaBudgetTests` bounds the sum of assembled TDEF chain bytes in one
catalog validation, including cached images and the catalog root, plus a derived
catalog-entry ceiling. Exact-boundary reads, repeated refused scans, pre-read
refusal, cancellation, invalid options and linked-source option forwarding have
focused coverage. These logical-byte/count limits do not measure total heap
usage, collection overhead or cache lifetime.

## Schema rewrite identity

Add, drop and rename publish isolated replacement storage on the original TDEF root across Jet3, Jet4 and ACE. `TableRewriteIdentityTests` checks unchanged table IDs, catalog fields and permissions, continuation chains, complex parent/flat IDs and relationships. `TableRewriteStorageIsolationTests` guards unrelated data-page reads during Jet4/ACE publication; `TableRewriteFailureTests` sweeps complex rename/drop write failures and cancellation with exact rollback and same-writer retries.

`IndexDescriptorPreservationTests` retains opaque physical/logical index bytes
through Add/Drop/Rename while retargeting rebuilt columns, roots, maps and FK
references. It checks shared self-FK indexes, uniqueness, seek reads, exact
rollback and byte-preserving refusal of incompatible descriptor merges. Separate
DAO-authored Jet3/Jet4/ACE sources check native descriptors, index reads,
constraints, mutation and compact/reopen. Synthetic opaque marker checks are
separate from native acceptance of unmodified native metadata.

Native coverage uses `NativeSchemaDaoTests`, `NativeJet3SchemaRewriteTests`, `ComplexSchemaDaoTests` and the single/composite PK/FK compact cases. Northwind's original `ProductCategories` data macro depends on `GetAuditFieldsUserName` and already refuses standalone DAO parent updates with error 3892; `ComplexSchemaMacroDaoTests` verifies the refusal before any writer mutation and successful native compact. Its rewrite coverage retains catalog/application blobs and verifies native reads, extraction and compact. The other three complex fixtures retain persisted native attachment/multi-value writes. This does not establish arbitrary data-macro dependency rewriting or application execution semantics.

## Coverage Matrix

| Area | Base fixture / oracle | Strongest automated signal | Current coverage and tests / probes | Residual trigger |
|---|---|---|---|---|
| Fresh Jet4/ACE bootstrap | Writer-created Jet4/ACCDB bootstrap output under validation, checked against DAO and native header/security fixtures. | DAO CompactDatabase | Fresh Jet4 output passes DAO open, insert, FK enforcement and exact compact/readback in `Jet4NativeBootstrapTests.FreshJet4_DaoOpensWritesAndCompacts`; see [native bootstrap notes](jet4-bootstrap-security.md). Fresh writer-created ACCDB streams scaffold hidden core system tables, `MSysRelationships`, `MSysComplexColumns`, complex-type attachment template metadata, a simple user table, and inserted rows; the compacted copy reopens through this reader. Current signal: `FreshWriterCreatedDatabase_DaoCompactSmokeTest` in [DaoStorageMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs), plus [CreateDatabaseTests.cs](../../tests/JetDatabaseWriter.Tests/Writer/CreateDatabaseTests.cs) and [ComplexColumnsWriterTests.cs](../../tests/JetDatabaseWriter.Tests/ComplexColumns/ComplexColumnsWriterTests.cs). | Add byte-for-byte comparison with a DAO-created empty database only if empty-database parity becomes a release requirement. |
| Scalar user tables and LVAL values | DAO-authored ACCDB host mutated by the writer, plus DAO-authored memo payloads as reader oracles. | DAO OpenRecordset | DAO can count writer-mutated rows, seek primary keys, continue AutoNumber values, and round-trip MEMO embedded NUL/CJK text plus deterministic OLE binary payloads large enough to force LVAL storage. Current signal: `DaoOpenRecordset_RowCount_MatchesWriterOutput`, `DaoIndexTraversal_Seek_LocatesRowByPrimaryKey`, `DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert`, and `DaoMemoAndOleLvalFidelity_EmbeddedNulsCjkAndBinary_RoundTripExactly` in [DaoValidationTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoValidationTests.cs). | Automate Access UI save/export workflows only if those UI paths become release blockers. |
| Jet3 LVAL pages | Access 97-authored LVAL pages in `test2V1997.mdb` (Jackcess), plus the Access-authored Jet3 read fixtures. | Byte-level comparison plus focused DAO3.6 read/compact | The writer's Jet3 LVAL pages use the Jet3 data-page header (row count at 8, row offset at 10), pack single-page rows and last chunks at the page end, and store no token. A 348-byte single-page value written by the writer equals Access 97's page 49 in all 2048 bytes, and a 22,970-byte chain equals pages 37-48 apart from the next-row pointers. Current signals: `LvalPages_MatchAccess97PagesByteForByte`, `RoundTrip_EveryStorageForm_ReadsBackThroughEveryReadApi`, `AccessAuthoredJet3LongValues_ReadExactly`, `CreateTable_LvPropOver256Bytes_PropertiesRoundTrip` and `DropTable_ChainedLongValue_FreesOnlyItsLvalPages` in [Jet3LongValueTests.cs](../../tests/JetDatabaseWriter.Tests/ValueEncoding/Jet3LongValueTests.cs), Jet3 cases in `LongValueWriteBackTests`, `DataRemanenceTests` and `EmittedPageInvariantTests.SingleAndChainedLval_Jet3_EmitWellFormedPages`, and `LinkedTableCatalogWriterTests.CreateLinkedTableAsync_Jet3_DatabasePathRoundTrips`. | DAO3.6 verifies fresh mixed inline/chained MEMO/OLE tables through exact native reads and compact in `CompactRawTableBuilderTests`; DAO120 itself still refuses Access 97. Extend native coverage to remaining token, shared-page and code-page variants. |
| Jet3 rows over 255 bytes (jump table) and the row-size limit | The Access 97 row in `test2V1997.mdb` (MSP_PROJECTS, the only Access-authored jump table in the fixtures, with one `0xFF` dummy), the mdbtools/Jackcess Jet3 row readers, and writer-created Jet3 streams. | Byte-level comparison with the Access-authored trailer and a port of Jackcess's reader; conditional DAO CompactDatabase | The writer sizes the Jet3 jump table as `(rowLength - 1) / 256`, stores the low bytes of the EOD and offsets, and writes each entry as the index of the first offset at or past its 256-byte boundary, `0xFF` for a dummy; the reader applies the same rule. Rows at every boundary (exactly 256 bytes, a dummy entry, an offset at 256, 512-byte rows, EOD 1025), 200-Long fixed-only rows and 20,000 random rows decode identically through the reference and the library, and the MSP_PROJECTS trailer rebuilds byte for byte. A row larger than one data page (2,036 / 4,080 bytes) moves its largest inline MEMO and OLE values to LVAL pages until it fits, and throws `JetLimitationException` on every format when none is left to move. Current signals: [Jet3LongRowTests.cs](../../tests/JetDatabaseWriter.Tests/Writer/Jet3LongRowTests.cs), [Jet3JumpTableDecodeTests.cs](../../tests/JetDatabaseWriter.Tests/ValueDecoding/Jet3JumpTableDecodeTests.cs), [RowSizeLimitTests.cs](../../tests/JetDatabaseWriter.Tests/Writer/RowSizeLimitTests.cs), `ColumnConstraintTests.Jet3ClrDefaults_ExternalProperties_AreAppliedByALaterWriter`, and the conditional `Jet3LongRows_SurviveCompactAndRepair` in [DaoStorageMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs). | No fixture has a real (non-dummy) jump entry, so real entries rest on the mdbtools/Jackcess rule until the DAO test runs on a host that opens Access 97 files. Whether Access accepts rows between its documented 2,000-character record limit and the page capacity is unchecked. |
| PK/FK catalog and TDEF metadata | Access-authored Northwind and DAO-authored Jet3/Jet4/ACE files mutated by the writer. | DAO open, mutation and CompactDatabase | Single-column and composite PK/FK metadata survives DAO compact; FK enforcement rejects invalid child rows; wide endpoint rename preserves both ends; relationship drop reclaims shortened TDEF chains; Type 8 relationship `MSysObjects` rows are created, renamed without changing their security identity, and deleted with matching `MSysACEs` rows on drop; `DropTableAsync` refuses externally enforced relationships; unenforced and self relationships permit table deletion with metadata cleanup. The guarded unenforced-deletion case passed at the revision above. A completed drop must leave no partner FK entry naming a freed TDEF page. Current signals: `SinglePk_AndSingleColumnFk_SurviveCompactAndRepair` and `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair` in [AccessRoundTripTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/AccessRoundTripTests.cs); `DaoRelationshipEnforcement_FkViolation_RaisesError` in [DaoValidationTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoValidationTests.cs); `IndexRebuildAndShortenedTDefChain_SurviveCompactAndRepair` and `RelationshipRenameOnMultiPageTDef_SurvivesCompactAndRepair` in [DaoStorageMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs); the `TableDefs.Delete` oracle on a related Northwind table, a non-enforced relationship and a self-referencing one in [DaoRelationshipDropTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoRelationshipDropTests.cs), with the reader-side refusal and unlink checks in [DropTableRelationshipTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/DropTableRelationshipTests.cs). | Add broader rename/drop permutations or Access-authored byte comparisons only when a relationship mutation changes TDEF logical-index or `MSysRelationships` bytes. |
| Multi-table index/FK/allocation stress | Access-authored Northwind copy mutated by the writer. | DAO CompactDatabase | Multi-table writer output survives DAO compact with table survival, all stress child row counts, generated FK index names, and matching `MSysRelationships` rows intact. Current signal: `DaoCompact_MultiTableStress_SurvivesCompactAndRepair` in [DaoValidationTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoValidationTests.cs). | Add byte-for-byte Access index rebuild equivalence only if allocator or index rebuild shape changes enough to make parity release-relevant. |
| Jet3 FK logical-index entries | Access-authored `indexTestV1997.mdb` plus independently DAO3.6-authored Jet3 tables with natively initialized relationship storage. | Native DAO3.6 open, enforcement, mutation and compact | Jet3 FK entries use 20-byte logical entries, 39-byte real indexes with flags `0x00`, ANSI names ordered with General97 Access comparison keys, unique/PK covering parent indexes and cascade bytes on both sides. `DaoRelationshipMetadataTests` verifies create/rename metadata, initialization of new index-map pointers while preserving unrelated native rows, orphan refusal, cascades and compact. `DaoSelfRelationshipUpdateTests` and `DaoJet3CascadeCycleTests` verify self-updates, cycles and rollback; ordinary layout/schema-rewrite regressions retain byte/link coverage. | Fresh Jet3 creation passes DAO3.6 open, enforcement, mutation and compact in `Jet3NativeBootstrapTests`; `Jet3SystemUsageMapDaoTests` and `Jet3IndexUsageMapTests` add core shared/REFERENCE maps, live tree membership, full-page FK spill allocation, native constraints and compact. Native composite graph, path and depth checks are described above. |
| Compact discovery and raw index staging | DAO-authored FixedText Jet3/Jet4 fixtures, Access-authored Northwind/ComplexFields, the archived mdbtools Jet3 sample, and generated numeric-key trees as output under validation. | Guarded DAO36/DAO120 indexed reads, uniqueness and native compact; stored-key corpus comparisons | `CompactNativeIndexCorpusTests` audits every discovered physical index in the named native specimens. `CompactIndexAuditTests` separates entry-edge and complete observed-edge key fences, acyclicity and observed sibling-chain order; synthetic damage cases and budget/cancellation checks are separate evidence. `CompactRawIndexBuilderTests` compares preserved native stored keys and exercises disk-backed level fences. `CompactRawIndexNativeTests` installs staged bytes into a controlled unchanged tree allocation, then checks native indexed identities/payloads and duplicate enforcement before and after compact. | Whole-database relocation, maps, shared ownership, encryption and metadata preservation, managed destination validation, damaged-file recovery and durable publication remain I17 requirements. Controlled index-byte acceptance is not managed Compact & Repair acceptance. |
| Table, index and column usage maps | DAO3.6-authored Jet3 and DAO120-authored Jet4/ACE databases; preserved native trailers and separate synthetic alias/fault controls. | Native read, DML, constraints and CompactDatabase | `NativeDataPageFreeSpaceTests`, `NativeFreeMapGrowthTests`, `Jet3SystemUsageMapDaoTests`, `Jet3IndexUsageMapTests` and `LongValueUsageMapTests` cover live maps, INLINE-to-REFERENCE growth, full map-page spill, shared-row/bitmap isolation, native deletion/reuse, long-value packing/chained free-map behavior and compact/readback. Synthetic regressions separately cover rollback, global allocation aliases, overflow storage and surviving map ownership during drops. | Extend the corpus for new native descriptor/map variants. These checks do not establish full live-page compaction or repair; see [usage-map maintenance](usage-map-maintenance.md). |
| Storage maintenance and remanence | Access-authored Northwind copy mutated by the writer, plus byte-level marker checks on generated files. | DAO CompactDatabase plus byte-level regression checks | Page free-list reuse, tail-only shrink behavior, secure erase of deleted row bodies and old LVAL chains, ordinary user-table replaced index pages, and shortened TDEF-chain reclamation are pinned without promising full Access-style live-page compaction, live-page slack scrubbing, or broader orphan-page reclamation beyond those pinned cases. Current signals: [DaoStorageMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs), [PageAllocatorTests.cs](../../tests/JetDatabaseWriter.Tests/Pages/PageAllocatorTests.cs), and [DataRemanenceTests.cs](../../tests/JetDatabaseWriter.Tests/Writer/DataRemanenceTests.cs). | Design separately before enabling full Access-style live-page compaction/renumbering, arbitrary unused-gap scrubbing, broader orphan-page reclamation, or replacement-index-page reclamation for `MSys*` and generated complex flat tables. |
| Native JET/ACE encryption and maintenance | DAO-produced JET3/JET4/ACE fixtures, pinned historical ACE files and directly encrypted writer output. | DAO120 and compatible DAO36 read/write/compact | Native page codecs preserve original keys during ordinary updates. Public file maintenance rekeys/decrypts pages and remasks JET4 security identities; direct creation encrypts before its first destination write. Coverage: `DaoNativeEncryptionTests`, `DaoNativeJetSecurityTests`, `DaoNativeAceProviderTests`, `DaoEncryptionMaintenanceTests`, `NativeJet3SecurityTests` and `EncryptionMutationTests`. DAO36 verifies JET3 mutation/compact and a synthetic custom-owner JET4 workgroup. `NativeStandardDatabaseTests` and the mixed Agile fixture tests pin Microsoft-compacted Standard (50,000 iterations), AES-192, CFB8, SHA-256 and SHA-384 output; their bootstrap inputs are recorded as synthetic. Indexed security identity rebuilds, physical shrink fault rollback and replacement boundary failures have focused regressions. | Third-party extensible/non-AES provider contracts and power-loss durability remain outside this evidence; managed replacement, recovery copies and supported permission behavior run in focused Linux/macOS CI; directory entries are not flushed. |
| Complex schema edits and generated child tables | Microsoft-authored `NorthwindTraders.ProductCategories`, `ComplexFields.Documents`, both `complexDataTest.Table1` variants, plus a DAO-authored wide attachment table. | DAO child mutation, compact and readback | `ComplexSchemaDaoTests` covers complete scalar and complex Add/Drop/Rename sequences, preserved scalar/attachment/multi-value/history values, native unique Required parent indexes, newly generated reference indexes, persisted complex reference seeds, AppendOnly metadata and no temporary catalog objects. DAO writes attachments and tags through the generated child recordsets, then compacts and extracts surviving attachments. `ComplexColumnsFlatIndexesTests.ComplexFlatTables_UseNativeUserTableType` compares generated TDEF type `0x4E` with the native fixtures; `0x53` would make the children read-only. `NativeSchemaDaoTests.DaoAuthoredWideAttachment_RewriteSurvivesNativeMutationAndCompact` covers multi-page TDEF Add/Drop/Rename. | Individual mutation and native history evidence are described above. Native descriptor/reference variants and unique per-parent multi-value indexes are pinned by `ComplexDescriptorVariantTests`. |
| Writer-authored complex rows and complex catalog (`ComplexFields` host) | Access-authored `ComplexFields` complex fixture mutated by the writer for the DAO-compact signal; fresh writer-created complex streams as subjects under test for the reader-side suite. | DAO CompactDatabase | The skip-guarded DAO test compacts writer-authored attachment rows on the `ComplexFields` host — raw .jpg, compressed .txt and compressed extensionless payloads across multiple parents — and checks their exact bytes and parent references through this reader. It also checks that the compacted complex catalog (hidden `MSys*` tables, `MSysComplexColumns`, attachment-template columns) reopens. These checks require a host with Microsoft Access. Fresh writer-created complex coverage — grouped multi-value rows, decoded attachment bytes, row APIs, and flat-table index metadata — is validated reader-side. Current signals: `ComplexFixture_WriterAttachmentRowsSurviveCompactAndRepair` and `ComplexFixtureCatalog_SurvivesCompactAndRepair` in [DaoStorageMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs), plus [ComplexColumnsRowApiTests.cs](../../tests/JetDatabaseWriter.Tests/ComplexColumns/ComplexColumnsRowApiTests.cs), [ComplexColumnsFlatIndexesTests.cs](../../tests/JetDatabaseWriter.Tests/ComplexColumns/ComplexColumnsFlatIndexesTests.cs), [ComplexColumnsSchemaEvolutionTests.cs](../../tests/JetDatabaseWriter.Tests/ComplexColumns/ComplexColumnsSchemaEvolutionTests.cs), and the broader `ComplexColumns*Tests` suite. | Extend when bootstrap, complex-template scaffolding, complex row APIs, schema evolution, or complex flat-table index metadata changes. The Northwind-hosted row remains the Access-authored-host signal for schema evolution and chained-LVAL payloads. |
| Fixed-length Text | DAO36-authored Jet3 CP1252/CP932 and DAO120-authored Jet4/ACE fixtures, with producer details and hashes in `Databases/FixedText/README.md`. | DAO36/DAO120 native row and schema mutation with compact | `FixedTextFixtureTests` checks padding, embedded NUL, CP932 byte-width behavior, leading U+FEFF and typed/string/direct/hybrid routes. Fixed slots use uncompressed encoding rather than interpreting `FF FE` as a compression header. `NativeFixedTextPreservationTests` compares descriptors and raw bytes across unrelated updates and schema edits, including synthetic malformed UTF-16/CP932 and null-slot payloads. Ordinary/self cascades preserve unassigned slots; explicit equal-value assignments encode normally, and unstorable assignments leave bytes unchanged. DAO36 accepts writer Jet3 updates/inserts and Add/Drop/Rename with code-page-preserving compact; DAO120 accepts Jet4/ACE Add/Drop/Rename and updates/inserts through compact/readback. | Broader native fixed Text feature/collation variants and native acceptance of deliberately malformed content remain outside this evidence. |
| Calculated conversion contexts | DAO120-authored calculated fields and rejected field definitions; writer-created defaults/rules and refusal fixtures are separate subjects. | DAO create/refuse, read, writer update and compact/readback | `CalculatedConversionDaoTests` verifies eleven native-refused conversion functions and accepted `CDbl`, date literals, `DateSerial` and `TimeSerial`. `CalculatedConversionContextTests` preserves cached reads and default/rule conversions while refusing calculated create/add/recalculation before changing bytes. Byte Boolean True and out-of-range numbers yield native calculation errors; ordinary Null is distinct. Typed rows, object POCO properties and calculated DataTable cells retain `CalculatedError`; string rows render `#Error(6)` for the measured Byte overflow, and incompatible scalar POCO properties refuse conversion. | `CalculatedByteErrorReadTests` checks both parsing modes, update repair and ordinary Null/Byte controls. `NativeCalculatedErrorTests` adds independently native-authored error fixtures and writer/native read, repair and compact controls. Malformed framing retains its corruption policy. Additional expression/locale contexts remain R4/E2 work; the interpreter function list is not a blanket native-admissibility claim. |
| Linked Access-file Type 6 tables | Writer-created host database linking an Access source, compared against DAO-authored linked-row fixtures. | DAO CompactDatabase plus compacted OpenRecordset | Writer-created Access-file links allocate catalog-only negative ids, avoid low-24 collisions, use fixture-aligned flags, stamp linked-object ACE rows, leave `Lv` / `LvProp` / `LvModule` / `LvExtra` null like DAO-authored Access links, preserve `Database` through DAO compact with DAO-compatible uncompressed UCS-2 LVAL storage, and pass DAO OpenRecordset on the compacted linked table. Current signals: `CreateLinkedTableAsync_AccessSourceLeavesDaoCacheColumnsNull` and `CreateLinkedTableAsync_AccessSource_DaoCompactAndOpenRecordsetSucceed` in [LinkedTableCatalogWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/LinkedTableCatalogWriterTests.cs). | Extend when linked metadata routing, cache-column semantics, `Database` LVAL storage, or linked-source read-through rules change. |
| Linked text/CSV Type 6 tables | Writer-created host database linking a text source, compared against DAO-authored text-link shape. | DAO CompactDatabase plus compacted OpenRecordset | Writer-created text links use text-link flags, encode the text source filename with Access's `#` separator in raw `MSysObjects.ForeignName`, decode that name back to normal filenames for `ListLinkedTablesAsync`, leave `Lv` / `LvProp` / `LvModule` / `LvExtra` null, preserve `Database` through DAO compact via the DAO-shaped uncompressed UCS-2 LVAL path, and pass compacted OpenRecordset. Current signals: `CreateLinkedTextTableAsync_AllocatesCatalogId` and `CreateLinkedTextTableAsync_TextSource_DaoCompactAndOpenRecordsetSucceed` in [LinkedTableCatalogWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/LinkedTableCatalogWriterTests.cs). | Extend when text-link flags, raw filename encoding, `Database` LVAL storage, or linked-source read-through behavior changes. |
| Linked ODBC Type 4 cached-schema tables | Writer-created ODBC link rows using caller-supplied Access/DAO cached-schema `LvProp` payloads. | DAO CompactDatabase | The cached-schema overload validates a non-placeholder `MR2\0` / `KKD\0` property block, stores the caller-supplied Access/DAO cached linked-schema payload verbatim, and survives DAO CompactDatabase. DAO OpenRecordset remains DSN/source-dependent. Current signals: `CreateLinkedOdbcTableAsync_CachedSchemaLvPropStoresRealPayload`, `CreateLinkedOdbcTableAsync_CachedSchemaLvPropRejectsPlaceholder`, and `CreateLinkedOdbcTableAsync_CachedSchemaLvProp_DaoCompactSucceeds` in [LinkedTableCatalogWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/LinkedTableCatalogWriterTests.cs). | Add source-backed OpenRecordset coverage only when a reliable DSN/source fixture exists, or when the cached-schema contract changes. |
| Linked-table fixture oracles and ODBC `LvProp` probe | Access-authored linked Access/ODBC/text fixture rows, Jackcess, and mdbtools. | FormatProbe / fixture regression | Access-authored linked Access/ODBC fixture rows carry non-placeholder `MSysObjects.LvProp` payloads, pinning the cached-schema signal used by the ODBC cached-schema overload. DAO-authored Access-file and text Type 6 links are pinned as allowing null `Lv` / `LvProp` / `LvModule` / `LvExtra`. The `linked-odbc-lvprop` probe dumps the Access-authored ODBC fixture cache, the Type 6 comparison fixture, `NameMap` string runs, target/property frequencies, and parser/builder structural parity. Current signals: [LinkedOdbcLvPropProbe.cs](../../tools/JetDatabaseWriter.FormatProbe/LinkedTables/LinkedOdbcLvPropProbe.cs) and [format-probe-linked-odbc-lvprop.md](../format-probe/format-probe-linked-odbc-lvprop.md). | Promote new probe findings into focused regression tests if they identify a root-cause invariant. Public ODBC link generation is covered in the next row. |
| Generated linked ODBC Type 4 `LvProp` tables | Writer-created ODBC link rows with generated metadata. | Reader/regression coverage plus DAO CompactDatabase where source-independent | Public ODBC link creation writes parseable table-level `MR2\0` / `KKD\0` property blocks with generated `NameMap` data instead of the legacy 12-byte placeholder. The source-column overload emits per-column targets, linked-cache `GUID` binary properties, and a `NameMap` containing remote table/column identity strings. Current signals: `CreateLinkedOdbcTableAsync_MetadataOnly_GeneratesRealTableLvProp`, `CreateLinkedOdbcTableAsync_SourceColumnsGeneratesRealSchemaLvProp`, and `CreateLinkedOdbcTableAsync_SourceColumnsRejectsEmptySchema` in [LinkedTableCatalogWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/LinkedTableCatalogWriterTests.cs). | Extend when a new public ODBC link mode needs richer schema-cache synthesis or source-backed OpenRecordset validation. |
| Jet3 index emission and maintenance | Access-authored Jet3 `.mdb` fixture mutated by the writer, writer-created Jet3 streams, and Access-authored read fixtures. | Conditional DAO CompactDatabase | Jet3 reader/writer coverage includes single-column text/int indexes, unique mixed-direction composite indexes, update/delete/insert maintenance, and conditional DAO compact on hosts that can open Access 97 `.mdb` files. The DAO test mutates an Access-authored Jet3 host with a writer-created indexed table, primary and normal single-column indexes, post-insert update/delete/insert B-tree maintenance, DAO pre-compact OpenRecordset, and reader verification of the compacted copy. Current signals: `Jet3IndexEmissionAndMaintenance_SurviveCompactAndRepair` in [DaoStorageMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs), `MixedDirectionUniqueCompositeIndex_UpdateDeleteInsert_RoundTrips` in [IndexMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/Indexes/IndexMaintenanceTests.cs), Jet3 parameterizations in [IndexWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Indexes/IndexWriterTests.cs), [IndexMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/Indexes/IndexMaintenanceTests.cs), [IndexIncrementalMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/Indexes/IndexIncrementalMaintenanceTests.cs), [IndexBulkInsertStressTests.cs](../../tests/JetDatabaseWriter.Tests/Indexes/IndexBulkInsertStressTests.cs), and [format-probe-appendix-jet3-index.md](../format-probe/format-probe-appendix-jet3-index.md). | Modern Access/DAO 120 can reject Jet3 with "Cannot open a database created with a previous version of your application"; on those hosts the test skips and reader round-trip remains the strongest local signal. Add Access-authored byte comparison only when a reliable Jet3 DAO host exists and exact byte parity becomes release-relevant. |
| Advanced ACE/Jet4 index encodings and B-tree maintenance | DAO-authored ACCDB host mutated by the writer, writer-created outputs as subjects under test, and Access-authored read fixtures. | DAO CompactDatabase | DAO seeks writer-created mixed-direction composite and GUID indexes before compaction, then compacts and reopens to assert GUID, descriptor-scale NUMERIC, BINARY, mixed-direction metadata, descending col-map flag preservation, and post-update/delete/insert row state. Current signals: `AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair` in [DaoStorageMaintenanceTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/DaoStorageMaintenanceTests.cs), index tests under [tests/JetDatabaseWriter.Tests/Indexes](../../tests/JetDatabaseWriter.Tests/Indexes), and appendices under [docs/format-probe](../format-probe). | Add Access-authored byte comparisons for remaining key encodings or individual surgical split/merge sub-phases only when those bytes change. |
| System-table and catalog-index maintenance | Access-authored Northwind / Jet3 fixtures, writer-created catalog growth fixtures, and catalog-specific regression tests. | Reader/regression coverage plus DAO CompactDatabase where applicable | Indexed TDEF zero-slot decode bails instead of silently reporting success; `MSysRelationships` / `MSysACEs` / `MSysComplexColumns` inserts and `MSysACEs` deletes use incremental-or-skip maintenance and throw on incremental bail; Jet4/ACE `MSysObjects` deletes and renames fail fast on incremental failure; Jet3 `MSysObjects` catalog splicing is regression-guarded; Type 4/6 linked rows route through catalog-index splicing; catalog splice failures roll back linked-table rows before ACE insertion; low-level catalog helpers reject duplicate `(ParentId, Name)` rows; supported `MSysObjects` catalog insert split/ancestor cases update in place or rebuild only the affected index tree from existing index entries plus the inserted row pointer. Current signals: [LinkedTableCatalogWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/LinkedTableCatalogWriterTests.cs), [AccessRoundTripTests.cs](../../tests/JetDatabaseWriter.Tests/RoundTrip/AccessRoundTripTests.cs), [RelationshipWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/RelationshipWriterTests.cs), and [catalog-index-maintenance-notes.md](catalog-index-maintenance-notes.md). | Extend when a new system-table writer touches indexed catalog rows, or when a splice/rebuild fallback adds a new failure mode. Remaining splice `false` returns should stay fail-fast malformed-input or staging guards. |
| DAO-authored baseline probes | DAO-authored copies retained as diagnostic oracles beside writer-mutated Access-authored fixtures. | DAO OpenRecordset / DAO CompactDatabase plus focused byte-level regression guards | `rt-dao-baseline`, `fk-dao-baseline`, and related probes remain diagnostic oracles, but root-cause findings are promoted into ordinary regression tests. Historical findings for OpenRecordset, FK enforcement, FK CompactDatabase, Jet4/ACE TDEF magic, real-index descriptor magic, user-table `Owner` / `LvProp`, `MSysACEs.FInheritable`, text-column `AllowZeroLength` / `Required`, and FK logical cross-references are already promoted. Current signals: [DaoBaselineProbe.cs](../../tools/JetDatabaseWriter.FormatProbe/Dao/DaoBaselineProbe.cs), [FkDaoBaselineProbe.cs](../../tools/JetDatabaseWriter.FormatProbe/Dao/FkDaoBaselineProbe.cs), [Jet4FormatCookieTests.cs](../../tests/JetDatabaseWriter.Tests/Pages/Jet4FormatCookieTests.cs), [PersistedColumnPropertiesTests.cs](../../tests/JetDatabaseWriter.Tests/Schema/PersistedColumnPropertiesTests.cs), and [RelationshipWriterTests.cs](../../tests/JetDatabaseWriter.Tests/Relationships/RelationshipWriterTests.cs). | A new probe finding should become a focused regression test instead of remaining only in probe output. |

## Current Scope Decisions

- The TODO contains the required compatibility and hostile-input validation backlog.
- The former ESE-inspired coverage review is closed. ESE remains useful only as
	a reference checklist for analogous storage-engine risk categories: page
	mutation invariants, structural validation, delete/replace scrubbing,
	transaction durability, cache behavior, and long-value cleanup. Local
	coverage for those themes now lives in the matrix rows above, especially
	storage maintenance/remanence, complex columns/LVAL, public index seek,
	CompactDatabase automation, transaction tests, emitted-page invariant tests,
	and cache eviction tests. Future ESE-derived concerns should become matrix
	rows or feature-specific DAO/regression tests, not a separate backlog.
- Managed live-page compaction and repair follow the explicit [implementation plan](compact-and-repair.md); component evidence does not establish acceptance of a complete maintenance API.
- Byte-for-byte DAO/Access comparisons stay diagnostic unless exact byte parity becomes a release requirement for the specific area.
- ODBC OpenRecordset remains source- and DSN-dependent; current automated coverage validates metadata generation, cached-schema preservation, and CompactDatabase compatibility.
- Manual Access UI workflows remain supplemental for cases DAO cannot expose, such as saving an attachment back to disk through the UI.

`OwnedDataCompletenessTests` verifies strict typed/string/count refusal for a
mapped or physically discovered unknown page with matching ownership, lenient
faithful survivors with an incomplete-table trace, strict write-back with unchanged
bytes, warm writer-cache invalidation, restoration and cancellation. Unknown-page
evidence survives unreadable-map fallback and repeated reads. Stale row counts
remain advisory. The native [damage controls](compact-and-repair-evidence.md)
independently reproduce the mapped DATA-page fault and distinguish it from
primary/secondary index damage; the Microsoft-authored inputs retain their hashes.
This does not prove traversal completeness for missing map bits, recognized-type
substitutions or damaged owner bytes.

`CompactInventoryTests` checks bounded physical/catalog/TDEF/map observations,
complete per-page hashes, pinned opaque content, cancellation, real I/O propagation
and structured refusal. It also checks total read/work/record limits, storage
admission, short-page refusal, budget attribution through definition readers
and isolation from caller definition caches.
Inventory combines physical owner-header scans with independent native Id-index
bookmarks and exact stored-Id/key matching. Synthetic Jet3/Jet4/ACE direct-row
owner damage retains recovered pinned identities despite absent or malformed map
hints and wrong advisory row counts. Stale keys, invalid row directories and
conflicting identities do not authorize recovery. Missing independent index
evidence remains a gap. Damaged original and moved-payload owner headers can
retain identities only with complete independent Id-index ordering/routing
evidence; late index corruption discards those candidates. Intact original
headers with damaged moved-payload owners defer to independent index recovery.
Damaged copies of the existing native Jet3/Jet4/ACE specimens additionally check
unique ParentIdName fallback when the Id root is missing or corrupt, with exact
stored keys under a supported declared collation and a completely audited graph,
leaf chain, bookmark order and routing. Key mismatches, aliases, incomplete or
lossy Name text and unsupported collations withhold that recovery. Both key paths
unavailable still leaves identities unresolved. `storage-audit` consumes the stream into a bounded partial or completed
NDJSON inventory with bounded record serialization. Every region remains pinned;
neither this phase nor its completion marker proves full row/LVAL/index reachability or permits
relocation, reclamation, repair or replacement.

`LongValueStreamingTests` verifies exact opaque stored payloads, including a
generated value beyond 16 MiB copied through a reused page into a non-buffering
verifier. Explicit byte/row limits, corruption, cycles, cancellation, I/O exception
identity, deferred allocation on first-row refusal and awaited buffer release
are separate checks. This is synthetic
streaming evidence, not a Microsoft-authored large-value or native error-envelope
oracle.

`LogicalTDefChainTests` checks retained physical prefixes and original padding,
growth/shrink behavior, clearing of removed metadata and refusal of contradictory
declared lengths before mutation. `NativeTDefChainPreservationTests` compares
retained pages of DAO-authored wide tables and checks native reopen, insertion,
compact and readback. These chain write-back checks do not establish complete
generated-schema preservation or permission to relocate unknown metadata.

`CompactRawRowReaderTests` checks exact opaque row payloads and original/moved
bookmarks, bounded same-owner overflow traversal, live-target refusal, directory
geometry, cycles, limits before I/O, cancellation, source changes and real I/O
failures. Shared intermediate/terminal payload starts across live headers and
deleted slots aliasing live storage require refusal. Lazy tracking admission and
target-count limits include distinct deleted slots naming one physical start.
Existing Jet3/Jet4/ACE overflow specimens exercise stored-byte agreement;
this is a per-table physical owner-header scan, not a missing-owner or field-value
completeness proof.

`CompactIndexAuditTests` checks bounded raw keys/bookmarks and references,
page/type/owner validation, prefix reconstruction, in-page key order, ambiguous
child reachability, tail traversal, mixed tail/entry cycle refusal, acyclic alias
acceptance, graph/storage limits, cancellation and I/O failure.
Microsoft-authored catalog specimens and a compressed duplicate-key Jet3 leaf
check agreement with the existing codec. The terminal record explicitly leaves
whole-tree completeness unproven. These read-only observations neither regenerate
keys nor authorize tree relocation/reclamation or settle separator, global-order,
shared-root and cross-TDEF requirements.

The separate `AllObservedEdgeFencesValidated` flag checks complete observed
leaf-key envelopes across entry-child and tail edges, including aliases. Native
corpus regressions require the proof on the selected fixtures; synthetic
out-of-fence aliases withhold it without rejecting otherwise readable layouts.
This does not establish exact separator/leaf correspondence or bookmark ties.

`CompactIndexBookmarkAuditTests` adds explicit verification of same-owner live
row headers and bounded deleted moved-row chains for leaf and intermediate
bookmarks. Malformed targets/directories, aliases to live storage, cycles,
truncation, changing geometry and exhausted read/slot/storage/hop allowances
refuse without a terminal proof. `CompactNativeIndexCorpusTests` applies the
composed verifier to every discovered physical index root in the existing native
FixedText Jet3/Jet4, Northwind ACE, ComplexFields ACE and archived mdbtools Jet3
fixtures. This establishes observed target readability for those files, not
key/value correspondence, missing-entry detection, global ownership or native
acceptance of a managed compact copy.

`CompactIndexOwnershipAuditTests` and database discovery regressions also retain
each physical descriptor's exact ownership-map row. Exact-row aliases remain
explicit across tables; different rows may share one map data page. Index
roots/pages/targets colliding with declared map data pages refuse in either
declaration order. Missing roots/maps remain counted evidence gaps. These checks
keep map coordinates separate from the observed index-page allowance and do not
validate map membership, bitmap sharing or exclusive incoming ownership.

`CompactRawIndexValidatorTests` independently compares reopened private trees
with original stored keys and relocated bookmarks on Jet3/Jet4/ACE, including
stored observations from Microsoft-authored catalog fixtures and multilevel
duplicate opaque keys. Altered keys/bookmarks, child maxima, owner, root, tails,
height and uncovered pages refuse. Reused source buffers, missing relocations,
early EOF, partial-read accounting, budgets, changing lengths, cancellation and
real I/O errors are covered. The result establishes complete private allocation
coverage and preservation of supplied observations, not source completeness,
key/value semantics, global ownership or native acceptance of a compact database.

Raw LVAL staging and its independent reopened validator retain and compare observed Jet4/ACE page-header tokens, including nonzero native chained tokens. Differing source tokens force separate private pages; changing an output token refuses validation even when payload bytes and pointers agree. Native mixed Memo/OLE controls distinguish healthy originals from the previously unreadable staged output and verify the correction; Jet3 never treats its row count at offset 8 as a token.

`CompactLongValueAuditTests` checks preservation observations for complete column
slices, defined external references and complete external rows, including
zero-length descriptors, trailing bytes and extra chain rows. Synthetic dangling
references, trailing cycles, malformed directories, budgets, cancellation and
real I/O failures require explicit refusal without a terminal record. Observed
bytes remain pinned. Existing Access-authored Jet3 packed Memo and chained OLE
fields additionally check agreement with their original physical row bytes.
These observations do not establish value semantics, exclusive ownership or safe
relocation.

`CompactDiscoverySessionTests` checks cumulative limits across repeated tables,
index roots and long values, shared page-read accounting, nested row/value
storage admission, release after failure, cancellation and real I/O exception
identity. These are synthetic composition checks. Whole-database enumeration,
inventory/catalog lifetime and verified index callbacks additionally run on
generated Jet3/Jet4/ACE databases in `CompactInventoryTests`; a bookmark into
index storage refuses the whole-database scan. Session-level target capacity and
repeated bookmark scans share the existing read/directory/object limits.
Missing ownership and native compact-result validation remain separate requirements.

`CompactDatabaseFieldTests` and `CompactCalculatedColumnHydratorTests` check
bounded persisted ResultType hydration, full signed catalog Id matching,
cumulative payload/schema limits, unknown result types and property-copy bounds.
The archived Access calculated fixture now exposes its stored Memo results to
automatic raw LVAL discovery without expression evaluation. Accepted recovered
catalog bookmarks, including damaged original and moved-payload owners backed
by complete independent catalog-key evidence, also feed property hydration and raw field scans.
These regressions mutate archived native fixture copies; they preserve source
hashes and withhold ordinary same-owner bookmark validation.

`CompactRawDataBuilderTests` checks dense private DATA pages, exact opaque and
moved-row payloads, original-header relocation records, the 255-row output limit,
budget refusal, cancellation, flush failures and unchanged source bytes.
It composes the ledger with stored-key index staging and validates the resulting
bookmarks against packed rows on Jet3/Jet4/ACE layouts.
`CompactRowRelocationMapTests` checks monotone/unique mappings, binary lookup,
missing bookmarks, partial reads, cumulative I/O limits, changed lengths and real
I/O propagation. These are component tests, not native acceptance of a complete
managed compact result; complete catalog/TDEF/map assembly remains outstanding.

`LvalRowBoundsDecodePolicyTests` and `CalculatedLongValueDecodePolicyTests` check
that malformed external directories and assigned calculated payloads produce
contextual strict corruption, diagnosed lenient missing values and write refusal.
These synthetic corruption cases do not establish native calculation-error
semantics or the contents of lost values.

`CompactRawDataLongValueValidatorTests` independently compares reopened packed
rows and complete staged LVAL rows with source fields and the complete relocation
ledger. Synthetic tests cover trailing bytes beyond declared payloads, opaque
unassigned slots, calculated type projections, chains crossing pages, shared or
uncovered private slots, altered nonpointer bytes, short reads, exhausted budgets,
I/O errors and cancellation. The proof covers only those supplied private
allocations, not global incoming ownership or native compact acceptance.

`CompactUsageMapBuilderTests` checks generated INLINE and REFERENCE native map
membership through the existing reader on Jet3/Jet4/ACE layouts, including
shifted windows, sparse runs, promotion, malformed membership, native capacity,
budgets and write/flush failures. `CompactTDefReferencePatcherTests` checks exact
raw-image preservation outside the complete table/index/LVAL reference words,
including wide logical definitions, counters and relationship descriptors.
These synthetic checks establish composable map/reference staging; they do not
prove global allocation accounting, reclaimability or a complete native result.

`CompactUsageMapValidatorTests` independently compares reopened private INLINE
and REFERENCE layouts against ordered allocation runs on Jet3/Jet4/ACE. Changed
members, counts, row ownership/directory geometry, aliased or external bitmap
pointers, uncovered allocations and extra empty bitmaps refuse. Empty maps,
partial reads, aggregate work/storage/read limits, cancellation and real I/O
propagation are covered. This establishes exact supplied map membership and
private allocation coverage; the expected runs' ownership and whole-database
allocation accounting remain separate requirements.

`CompactStagedTableCompositionTests` combines raw packing, native table maps and
pinned TDEF patches on writer-created Jet3/Jet4/ACE tables. Reopened ordinary
reads select only the staged DATA pages and retain every expected row, while
source bytes and unrelated destination prefix bytes remain exact. Original
storage remains pinned; this is not a globally reallocated or compacted file.

`CompactRawTableBuilderTests` additionally composes mixed inline/spilled Memo
and OLE values with per-column native maps and pinned definition images across
Jet3/Jet4/ACE. Reopened raw allocation, independent original-column ledger and
exact map membership checks precede ordinary map-selected row reads. Altered or
swapped column records, missing/extra/truncated records and changed ranges refuse
despite unchanged raw values. Aggregate output/map growth and conservative
working-storage boundaries are checked separately. Guarded DAO36 Jet3 and DAO120 Jet4/ACE controls compare original, staged and native-compacted mixed Memo/OLE contents using ordinal text and exact binary comparisons. These are controlled
writer-created tables; original storage remains pinned and no global allocation
or reclaimability verdict is implied.

`CompactPinnedTablePipelineTests` compares zero/multiple physical stored-index
assemblies and exact installed chains against independent original observations
on generated Jet3/Jet4/ACE tables. Cross-index admission, physical root/map aliases,
tampered keys/ledgers/images and oversized per-column allocation lists refuse.
Reusable assembly sessions additionally retain source traversal, private I/O,
entry/key and output admission across tables and failed attempts. Construction
and independently reopened validation have separate phase sessions; releasing
working reservations does not reset consumed work.
`CompactPinnedPrefixValidatorTests` reconstructs only defined pointer changes and
requires every other original physical byte to survive, including opaque pages,
unlisted definitions, continuation headers and padding. Partial-read retries,
chain overlap, incomplete reference inputs and exhausted work/storage limits
have focused coverage.

`CompactGlobalMapTests` checks exact all-pinned INLINE/REFERENCE membership and
self-covered appended bitmap storage, including a bitmap-window boundary.
Independent reopened validation checks the entire page-one surround, complete
row directory, every map byte and exact append-point/EOF coverage. Its guarded
native tests compare read, allocation-growing Memo mutation, primary-key refusal,
native compact and managed/native exact readback through DAO3.6 Jet3 and DAO120
Jet4/ACE. `Jet3NativeExternalMemoTests` separately verifies fresh Jet3 global-map
rows against the frozen native 133-byte geometry and native external-Memo
mutation, table allocation, duplicate-key refusal and compact/readback. Imported
map rows keep their declared bounds; these controlled native checks do not prove
reclamation or complete managed Compact & Repair acceptance.

`CompactPinnedDatabaseCompositionTests` compares multiple tables, native global
maps, exact complete allocation ledgers and the entire pinned prefix together.
`CompactPinnedCatalogCompositionTests` extends the generated compositions to
every local catalog table, including `MSysObjects`, system/security tables,
relationship storage and hidden tables. It compares stored catalog identities,
all physical stored keys and relocated bookmarks, then ordinary schema, rows,
relationship partner references and linked-table metadata without opening links.
Changing appended catalog Id/ParentId/Name bytes passes prefix preservation and
fails independent raw catalog comparison. Independently compared active catalog
membership scopes reopened destination discovery, including accepted catalog
recoveries. Exact original and active bookmarks distinguish pinned copies;
unscoped damaged-source duplicate Ids still refuse.
`CompactPinnedGlobalMapCompositionTests` requires concrete independent map
validation before accepting any changed page-one byte and includes its reads in
prefix admission. These are append-only compositions retaining every original
page; they establish neither reclamation, complete database acceptance nor
crash-safe replacement.

`CompactPinnedDatabaseBuilderTests` exercises the bounded disk-backed complete
private assembly driver, validates the full local-table plan before creating
output, closes all construction handles before independent reopening, and
checks cumulative read/write/output admission, cancellation and factory failures.
`CompactPinnedDatabaseValidatorTests` additionally requires exact independently
discovered source/active-destination catalog identities and local definition
membership. Inventories and membership comparisons share cumulative requested
read admission. Matching discovered inventories do not prove damaged-source
completeness, unknown incoming ownership, reclamation or durable publication.

`CompactPinnedDatabaseCorpusTests` compares complete private assemblies of
Microsoft-authored fixed Text, calculated-field, complex-value and Northwind
relationship/application specimens with their immutable sources. Ordinary rows,
column metadata, relationships, attachment bytes and complex-item identities
must survive alongside the raw comparisons. Independently specified invalid row
directory offsets refuse unchanged. Native encrypted Jet3 RC4, Jet4 RC4, ACE
Agile and ACE Standard inputs exercise decrypted private staging only; preserved
header metadata does not turn that internal image into an encoded native file.
A DAO120-authored Byte calculation-error/Null specimen retains its exact
39-byte/23-byte envelopes through complete assembly and native compact, with the
native error/Null distinction independently rechecked. Guarded complete-driver
controls additionally verify native read, insert, update, delete, primary-key
refusal and compact through DAO3.6 Jet3 and DAO120 Jet4/ACE. This controlled
coverage does not establish every native feature variant or reclaimed storage.

`CompactActiveCatalogRowsTests` covers independently corroborated recovered
headers and moved payloads, retained-byte tampering, bookmark order and exact
cross-owner exclusions during source and active destination row/field scans.
`CompactIndexMapAuditTests` independently inspects native INLINE/REFERENCE map
membership, admits bitmap/pointer/bit work and retained entries, detects missing
observed members and DATA/index/bitmap role conflicts, and retains extra map
members and aliases as pinned evidence. Map evidence is bound to one immutable
page source and format. `CompactIndexSeparatorAuditTests` independently compares
complete stored separator key/bookmark pairs with observed child maxima through
all child/tail aliases. These scoped proofs establish neither semantic key/row
correspondence nor complete incoming ownership.

`CompactIndexRowAuditTests` independently derives supported scalar keys from
raw rows, preserving stored Date doubles and strictly decoded text. Complete
leaf and intermediate key/bookmark correspondence, independently resolved payloads, complete ordered leaf chains, routing, exact separators, duplicate and missing rows, IgnoreNulls
omission and bounded evidence have focused coverage. Positive four-byte complex parent references preserve their stored identity without following items. Unsupported calculated,
non-Memo LVAL and collation cases refuse. Ordinary Memo correspondence reads the exact descriptor-declared payload while auditing the complete bounded LVAL chain, including trailing references. Inline/single-row descriptor modes permit compressed markers; chained payloads preserve plain UCS-2, including literal U+FEFF. Per-value payload/storage and cumulative field/read/LVAL work admission cover repeated and failed active-row consumption. Scalar-only descriptors do not create Memo sessions or inherit unrelated Memo limits. Frozen DAO3.6 General97 key/bookmark
fixtures also pin embedded NUL `0x10` and two significant positions for simple
expansions before later accents. Physical row coverage is scoped to readable
same-owner headers; it does not establish active or complete source membership. A guarded DAO120 control on the pinned calcFieldTestV2010.accdb accepts an ordinary FirstName index and refuses indexes on LastFirstLen, LastFirst, MonthlySalary and WeeklySalary with native error 3251. This records four observed calculated-field refusals rather than inventing calculated-key semantics.

`PlainChainedMemoDecodePolicyTests` makes one independently specified descriptor-byte change in each pinned native Jet4/ACE ordinary and leading-U+FEFF chained-Memo control. Strict reads report contextual corruption, lenient reads return missing values with diagnostics, and unrelated updates preserve the complete database on refusal across public read surfaces. Raw payload and full trailing-chain observations remain exact; real I/O failures and cancellation propagate. Native DAO120 returns Null or shortened text for the damaged cases and native compact substitutes a marker or omits a row, so native success is not a completeness proof. The fixture README records source/damaged hashes, offsets and exact observations. Calculated, compressed-marker, Jet3 and OLE paths retain their separate semantics.

`CompactBooleanIndexRowAuditTests` pins six DAO-authored Jet3/Jet4/ACE source and native-compacted specimens. All four ascending/descending and IgnoreNulls descriptors agree with independently read present mask values and exact native key/bookmark bytes. Both Boolean values are non-null: native Null and omitted inserts reopen as False, and IgnoreNulls retains them. Corruption, missing/duplicate entries, opposite-value bookmarks, active all-index consumption and admission have focused controls. Twelve additional native schema-evolution source/compact specimens establish absent old Boolean slots as False under both False and True later defaults, preserve raw Absent observations before compact, and verify new-row default behavior, exact native keys and public decode surfaces. Calculated Boolean keys and general writer/index-creation admission remain separate. Provenance, producing engines and hashes are beside `Databases/NativeBooleanIndexes/`.

`CompactActiveTableRowsTests` independently corroborates native table-map rows,
strict covering-index proofs, complete scalar key/bookmark agreement and declared
counts across supported ordinary indexes in Jet3/Jet4/ACE fixtures. Source/image
identity, changed payloads, contradictory native evidence, lifetime bounds and
released reservations have focused checks. `CompactNestedDiscoveryAccountingTests`
verifies parent/child attempted-work charges and exactly released simultaneous
reservations. Complete-driver tests retain and report deliberately duplicated
same-owner pages across all three formats, require an observer before creating
output and forward separate independent validation limits. The legacy Umsätze
driver control rebuilds 830 active rows while reporting and preserving 2,049
unassigned rows in its independently compared pinned source prefix. None of
those rows is declared reclaimable, and incomplete source/incoming ownership
remains explicit. Controlled Jet3/Jet4/ACE tail-alias regressions independently require chain, routing, exact child maxima and bookmark proofs while the optional complete-edge envelope flag is false; active membership accepts those composed proofs. These synthetic controls do not add native sharing or reclamation authority.

`CompactPinnedApplicationObjectsTests` uses the controlled native Access
16.0.20430.20182 fixture whose provenance and SHA-256 are pinned beside it.
The saved query is never executed. Raw catalog/application rows and complete
pinned-prefix comparisons preserve stored content and identities, while DAO
checks query metadata and Forms/Reports names before and after native compact.
Independent SaveAsText controls retain exact form/report exports through the
private assembly and native compact. A separate hashed native specimen adds a named StopMacro-only macro and a constant-only standard module; neither is invoked. Complete raw catalog/application storage and pinned-prefix comparisons preserve both identities, DAO checks Scripts/Modules document names through compact, and same-engine SaveAsText exports of all four application definitions match source, assembly and native-compacted output. The specimen README records producing builds and exact export hashes. Arbitrary application behavior, damaged objects, replication and broader
security variants remain separate evidence requirements.

`NativeApplicationObjectPreservationTests` extends both pinned inert ACE specimens to public insert/update/delete, unrelated column add/rename/drop and another table's create/drop in every write mode. It compares all unedited catalog field values (including exact timestamp ticks), complete unaffected table values/binary payloads, and selected original owning TDEF/data/index pages. It also compares every original MSysACEs row value, stored long-value descriptors bound to stable object identities, and exact reachable external LVAL rows, including page tokens, directory slots, chain pointers, padding and terminal pointers. Physical shared ACL pages and broader security variants remain outside this evidence. Fresh DAO120 public-mutation/reopen/compact controls remain guarded and skipped on the current host; frozen Microsoft-authored inputs plus managed comparisons are not fresh native acceptance. No embedded content is invoked, and broader application/security/replication variants remain S9 work.

`CompactPinnedReplicationTests` pins native DAO3.6 Jet4 Design Master/full-replica fixtures and compares all 18 local/system tables, 22 catalog identities and 94/95 complete pinned-prefix pages through private assembly. Native read-only identity/replicated metadata/value comparisons and separate compacts of original controls and assembled subjects preserve both ReplicaID and DesignMasterID. No synchronization, transport or stored code is invoked. The default native principal refuses four system-definition inspections with DAO 3110; managed raw/catalog/prefix comparisons still retain their complete storage. Jet3 Replicable conversion refuses with DAO 3703 before replica creation. The fixture README records exact provenance, hashes, identities and limits; general replication semantics, genuine Jet3 variants, ownership and reclaiming compact acceptance remain separate.

`CompactPinnedSecurityTests` preserves both native Jet4 workgroup fixtures and
their isolated native-authored restricted profiles. Raw security/catalog rows,
property LVAL values, owner/SID/ACL bytes and original header survive the full
private assembly. DAO120 checks exact owner/grants through native compacts of
control and assembled copies, owner write/rollback, wrong account password 3029,
wrong database password 3031, and the restricted account's read permission 20
with update refusal 3108. Frozen database/MDW bytes remain unchanged. This
evidence does not establish managed workgroup authentication, replication or
every native security configuration.

`CompactEncodedDatabaseCopyTests` checks exact page-zero binding before output,
wrong passwords and unsupported providers, byte/page/working limits, partial
read requests, cumulative password hashing through independent reopening,
ciphertext tampering, cancellation and transferred-stream cleanup.
`CompactEncodedDatabaseNativeTests` encodes complete private assemblies of the
frozen native Jet3 RC4, Jet4 RC4, ACE Agile and ACE Standard specimens. Original
header/provider parameters and original/staging bytes remain exact; independent
reopened codec comparison checks every decrypted page. DAO3.6/DAO120 then read,
mutate, reject duplicate primary keys and password-compact the encoded copies,
with exact managed readback. Workgroup security controls use this same bounded
production codec boundary. These comparisons establish private codec equality
for the named providers, without complete database, reclaimability or durable
publication acceptance.

`NativeTextMarkerTests` retains hashed DAO120-generated cached Text rows and their native compacted counterpart, with provenance beside `Databases/NativeTextMarkers/`. Disabled-compression Text preserves literal U+FEFF across every read surface and write-back. Calculated Text retains compressed-marker semantics, including native escaped FEFF and terminal uncompressed low bytes with implicit zero high bytes. Native cached results are compared independently rather than inferred from expression inputs. `CalculatedTextLeadingMarkerDaoTests` checks exact writer payloads and cached results for the measured calculated conversion, including native loss or reinterpretation of a leading marker. Its guarded controls compare independently native-authored and writer-produced results through native recalculation and compact. Unverified ordinary compressed Text shapes refuse before allocation. Complete private assembly independently compares both frozen inputs without recalculation. These controls do not settle every Text/Memo encoding variant.

## Native row boundaries and recovery fault evidence

`CompactNativeRowBoundaryTests` retains native DAO3.6 Jet3 and DAO120 Jet4/ACE
specimens with fixed-only rows, dropped fixed-slot gaps, actual Jet3 variable
offset jumps across 256/512 and first-refused fixed-binary schema shapes.
Exact raw rows and field slices survive the complete private assembly; DAO
compact/reopen retains every controlled visible value. The fixture directory
records hashes, producer versions and shape-specific accepted/refused sizes.
Other native row shapes, unverified trailing-Null slot variants and remaining
F8 LVAL/text forms still need independent evidence.

`RecoveryReplayCrashTests` covers repeated actual process interruptions through
raw restoration, tail re-extension, final decisions and lock/journal cleanup,
including four native encrypted profiles. `ReplacementNamespaceModelTests`
selects surviving names from actual replacement snapshots and drives real
journal recovery with full, torn and conflicting content. Replacement intents bind
old/new generations and a process commit decision; exact selected bytes recover
through path writer open even without a destination name. Exclusive recovery
leases span publication and cleanup. Incomplete intent/ownership acquisition,
legacy candidates without an intent and conflicting evidence require refusal.
Preparation-only journal replay and generation-bound repeated writer recovery
have exact-byte controls. Bootstrap namespace persistence remains unqualified.
Crash children require the exact parent shared-runtime product version,
including preview/RC suffixes, with roll-forward disabled and a matching child
readiness report. Focused Linux/macOS CI includes these checks on .NET 10/11.
The [recovery contract](transaction-crash-recovery.md) records the concrete Windows
managed directory-flush probe and its limits. None of this evidence establishes
physical power-loss durability or authorizes compact publication.

NativeDisplayPropertyTests compares independently DAO3.6-edited controls with writer-edited Jet3/Jet4 subjects for related and unrelated column rename/drop and case-only rename. Stored Filter and OrderBy text and entry metadata survive reopen and native compact unchanged, including stale references; native insert/update/delete still succeeds. The local engine was DAO3.6 with dao360.dll 10.0.26100.5074. Managed regressions cover Jet3/Jet4/ACE and direct, automatic and explicit transactions, including opaque and repeated display entries. ACE DAO120 native checks remain guarded and unexecuted on the current host; Access UI Name AutoCorrect is a separate evidence gap.

`TableRenamedQualifiedExpressionTests` and `QualifiedExpressionReferenceTests` cover unchanged bare-qualifier binding, explicit-context/unknown-field refusal and byte-identical CreateTable/AddColumn rejection of new bracketed foreign declarations. `CalculatedStaleCacheTests` pins independently ACE-authored before/after and constraint specimens: stale bracketed plans retain existing caches, new rows have absent caches, and dependent fields retain status -5617. Writer insert/update matches those native stored results; public reads distinguish an absent cache from ordinary Null. Native Required, field-rule and table-rule controls establish the corresponding refusal behavior, and native compact refuses stale bracketed expressions with 3790. These controls retain their separate scope from the remaining R4 expression qualification.

NativeMemoCompressionTests compares the leading-U+FEFF escape byte-for-byte with pinned DAO120-authored Jet4/ACE payloads at 11 and 1025 characters. New writer insert/update/delete and schema-rewrite controls retain exact text through DAO3.6 Jet4 reads and native compact for Unicode-compressed values of 6 through 2048 characters with a nonzero Latin-1 tail, including all 255 permitted tail characters and inline/external boundaries. The local engine was DAO3.6 with dao360.dll 10.0.26100.5074. Refusal tests retain unchanged database bytes for unsupported short, NUL/non-Latin-1-tail and uncompressed-column cases. Existing longer plain chains remain covered. Fresh ACE native read/compact confirmation remains DAO120-guarded; frozen native byte parity is distinct evidence.

`NativeLookupPropertyTests` compares independently DAO3.6-edited controls and writer-edited Jet3/Jet4 subjects for column rename, case-only rename, column drop and referenced-table deletion. Direct table sources, aliased SQL, saved queries and a saved-query chain retain lookup property bytes and native QueryDef SQL through reopen and compact. Writer edits also retain every MSysQueries field and matching query catalog row field. Native surrounding read/insert/update/delete succeeds. The local engine was DAO3.6 with dao360.dll 10.0.26100.5074. The metadata can retain stale references: preservation does not establish query rebinding or executability after deletion. ACE DAO120 controls remain guarded and unexecuted on the current host; broader application/query forms and Access UI Name AutoCorrect remain separate evidence gaps.

`NativeCompressedTextMarkerTests` pins Jet4 source/compacted files whose schemas were writer-created and whose rows were native-authored by DAO3.6. Descriptor-bit and exact raw payload checks establish the leading-U+FEFF escape for ordinary compressed Text, distinct from an inert UnicodeCompression property. Writer insert/update and schema rewrite retain complete values of 6 through 255 UTF-16 units with nonzero Latin-1 tails and optional contiguous nonsurrogate wide prefixes whose low bytes are nonzero. A wide prefix requires at least four tail units; otherwise five are required. Fresh DAO3.6 native authoring produces exact payload parity before and after compact, including both extreme prefix/tail lengths at 255 units; native reads and updates accept writer values and all 255 permitted tail characters. Declared-size overflow and unsupported short/NUL/low-byte-zero/surrogate or later-wide forms refuse before allocation with unchanged database bytes. Native U+FEFF-plus-a loses its marker on insertion and is retained as negative evidence. Producing engine/module details and hashes are beside Databases/NativeTextMarkers. Fresh DAO120 Jet4/ACE confirmation of ordinary compressed Text remains open. Calculated Text has its separate native cached-conversion controls described above.

`CalculatedExpressionFloatingSubtypeTests` retains native x86 DAO3.6 Jet4 query
controls (`dao360.dll` 10.0.26100.5074) for floating arithmetic, unary signs,
Abs/Int/Fix and 15/7-digit notation. Public stored defaults remain separate from
the fresh ACE table controls described below. Shared Variant division, missing
results, additional function/subtype combinations and locale contexts remain R4
work; these query controls do not expand table-calculated function admission.

`CalculatedExpressionRoundTests` compares native DAO3.6 Jet4 Round subtypes,
precision coercion, Null and large values. Exact Double-field source/stored/result
bit controls isolate scaling boundaries from literal conversion;
native Decimal-field controls retain values above the .NET rounding precision
limit. The older native engine module is `dao360.dll` 10.0.26100.5074. Fresh ACE
table controls independently repeat the exact floating boundary fields and
admitted Decimal/precision cases through writer/native recalculation and compact;
Currency and Date retain their separate native identity controls. Table Round
clears floating zero sign, while Int/Fix retain it. Additional shared function
and regional contexts remain R4/E2 work; no Access desktop/UI execution is inferred.

`CalculatedExpressionDecimalCoercionTests` compares exact Single/Double source
bits with native Jet4 mixed-Decimal field results, including scaled midpoints,
underflow and range boundaries. The interpreter uses explicit binary scaling
and integer Decimal construction across runtimes. `CalculatedExpressionDecimalPromotionTests` separately verifies finite floating
operand-conversion overflow: mixed Decimal arithmetic uses Double for both
signs of Single/Double 2^96, including signed-zero products and quotients. Fresh
DAO3.6 Jet4 TypeName and exact-bit controls cover both operand orders with
Decimal 0/1; public ACE calculated insert/update/reopen retains those results.
Convertible operands still use Decimal arithmetic. Fresh ACE table controls also
compare both signed Single/Double boundaries and Decimal 0/1, including exact
source and result bits through changed-source updates and compact. Calculated
Decimal overflow retains its measured status; valid financial nonfinite values
remain distinct. Shared missing-result behavior, additional coefficient/scale
representations, caller-field/storage conversions and locale contexts retain
their separate R4/E2 qualification requirements.

`CalculatedExpressionNumericLiteralTests` pins native Long/Decimal/Double literal
selection, exact Decimal admission, scale normalization, signed-minimum grammar,
Long arithmetic promotion and Decimal text. Explicit OLE Decimal-to-Single/Double
conversion preserves native result bits across runtime changes. DAO3.6 Jet4
query and stored-default controls ran through writer insertion, native
read/update/compact and managed compacted readback; Decimal-field controls
separately retain source scale. Literal `CDbl(99.99999999999999)` returns `100`,
while equivalent field values can retain a different scale and result bits.
Fresh ACE table controls independently qualify admitted literals and stored
Decimal-field conversions, while shared constructors and broader function,
representation, caller/storage and locale contexts remain R4/E2 work. Ordinary
CI checks both shipped library assets and the .NET 10/.NET 11 runtime combinations.

`CalculatedNativeNumericQualificationTests` authors independent original table
caches with DAO120 16.0 / Access Runtime `16.0.20527.20112` on an en-US Windows
profile. Native source readback checks exact runtime types, floating bits and
Decimal coefficient/scale before comparing results. The admitted literal,
integral, floating/notation, Round and Decimal matrices use typed source fields
in place of table-forbidden conversion constructors; Byte fields project Long.
Binary, Double, concatenated Text and direct Text are independently compared with
writer inserts, recomputation and changed-source updates, native recalculation
and native compact of both images. `CalculatedNativeNumericValueTests` freezes
ordinary public regressions for the measured distinctions.

These controls establish Date/Decimal promotion to Decimal for both orders of
addition, subtraction, multiplication and division, including Null and values
outside Date range; exponentiation remains Double. Fresh ACE query TypeName
independently qualifies the shared promotion. They also distinguish Integer
literal/comparison/And/Or/Not/In results from Boolean Like/IsNull/IsEmpty results:
intrinsic Boolean text is `True`/`False`, final Text is `-1`/`0`, and stored
calculated Boolean references project Integer. Asc returns Integer, while Len
and InStr return Long. Str converts numeric text through Double.

Calculated Text preserves `-0`, uses `inf`/`-inf`, distinguishes signed quiet and
signaling Double NaNs, and retains the exact negative indefinite `-nan(ind)`
payload. Single widens for text without changing its stored or Binary bits.
Direct Text, concatenation and Str have independent financial and raw-payload
controls; positive Str results retain their leading space. The shared formatting
profile remains separate. `CalculatedChooseTests` adds pinned native
source/compacted fixtures and fresh controls for Single index rounding,
out-of-range Null, overflow/Null statuses and selected branch widths; provenance
is in `Databases/CalculatedChoose/README.md`. The shared-language and
caller/storage requirements in R4 retain their separate contexts.

`CalculatedRegionalProfileTests` compares all retained calculated caches in
independently native-authored en-US, fr-FR, de-DE, tr-TR, ja-JP and ar-SA
source/compacted controls through writer insert and recomputation. Actual Windows
user regional settings vary while the database LCID stays 1033. Native numeric
parsing, date/number/Boolean text, names and system week starts match the captured
profiles; `Str` retains its decimal-point output. `CalculatedRegionalAnsiTests`
covers every ANSI byte and native Asc/casing controls, while
`CalculatedCultureIsolationTests` checks per-writer ownership and calendar-window
snapshots. Fixture hashes and engine/workflow provenance are retained beside
`Databases/CalculatedRegionalProfiles/`.

`CalculatedOperatorStatusTests` independently compares native source/compacted
unary and membership matrices through writer inserts and updates, then repeats
fresh DAO authoring. Unary plus preserves Text subtype and unary minus converts
Text through Double; invalid numeric storage retains native error statuses.
`IN` keeps native early-match/Null/error order and its distinct mixed-type
coercions, including formatted Date and nonfinite numeric text.

`PublicTextCollationTests` and `TextSortOrderFixtureTests` cover public General97/General Legacy/General choices, raw stored ID/version metadata, null header-default selection, unknown-ID reporting, no-mutation invalid-choice refusal, Text/Memo/calculated Text and schema preservation. `PublicTextCollationDaoTests` independently checks writer-created columns through schema edits and row updates, native case-equivalent seek, duplicate refusal, insert/update and compact/readback. Current x86 DAO3.6 (`dao360.dll` 10.0.26100.5074) controls passed for Jet3 General97 and Jet4 General Legacy. Four fresh DAO120 Jet4/ACE cases remain guarded and skipped on this host. Existing encoders and text code pages are unchanged; broader locale, long-key and native mixed-column coverage remains F9/E2 work.

`CreateIndexTests` exercises indexes added to populated tables across Jet3,
Jet4 and ACE and all write modes, including composite descending keys, existing
indexes, primary keys, required/ignored null keys, unchanged bytes on refusal and
explicit rollback. `RelationshipSchemaRewriteTests` retains enforcement and
cascades when an index is added to either relationship endpoint.
`DaoCreateIndexTests` independently authors Jet3/Jet4 source tables with DAO3.6
and checks added composite unique/IgnoreNulls and nonunique Required indexes,
native key order and seek, refused duplicate/null inserts, native row mutations,
original primary-key/property preservation, late primary-key creation and
CompactDatabase/readback. An independent native index edit retains the exact
Access-authored nonempty `NameMap` in `testV2000.mdb`; the writer edit retains
the same bytes and passes native reopen/compact beside its new index.
Fresh x86 DAO3.6 (`dao360.dll` 10.0.26100.5074) controls passed on 2026-10-31.
DAO120 Jet4/ACE checks remain guarded; these controls do not establish complete
schema/application preservation or untested native index variants.

`TableExpressionFunctionPolicyTests` checks declaration-time and imported-property
refusal for application functions in stored defaults and field/table validation
rules, including unused branches, while retaining ordinary scalar expressions.
Fresh x86 DAO3.6 Jet3/Jet4 controls on 2026-10-31 reject `CurrentUser`, `Eval`,
the domain aggregates and a nested `CurrentUser` call in all three constraint
contexts with error 3388. Native `Abs(-3)` defaults/rules and insertion succeed.
DAO120 checks remain guarded. These table-constraint observations do not define
the functions' separate query/application or workgroup behavior.
