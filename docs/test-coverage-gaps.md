# Test Coverage Gaps — Cross-Project Survey

Tracks coverage gaps surfaced by reviewing test suites in upstream projects
that target the same on-disk format(s) we do:

- **Jackcess** — `src/test/java/com/healthmarketscience/jackcess/` and the
  `IndexCodes*`, `Encrypt`, `Complex*`, `Crypt*`, `LongValue*`, `BlobConverter`
  test families.
- **mdbtools** — `test/sql/`, `test/sargs/`, the `pkrep` round-trip suite, and
  `src/libmdb/*.c` regression cases referenced from `HACKING.md`.
- **OpenMcdf** — `OpenMcdf.Tests/` (CFB stream suite, mini-FAT corner cases).
- **Microsoft Office Compound File / Agile Encryption** specs (MS-OFFCRYPTO,
  MS-CFB) — used to cross-check the encryption fixtures we already have.

This file lists **open** gaps only. Each entry is tagged with the source
project (`[J]` Jackcess, `[M]` mdbtools, `[O]` OpenMcdf) and the rough
difficulty (`S`/`M`/`L`). When a gap is closed, remove it from this file —
the test source is the canonical record of what's covered.

All tests should be **self-contained** — exercisable with `dotnet test`
alone. No Java runtime or mdbtools install should be required. DAO
(COM-based Access engine) is acceptable for validation on Windows hosts
where Microsoft Access is installed; such tests auto-skip via
`AccessRoundTripEnvironment.IsAvailable` when the runtime is absent.

---

## 1. Index encoding & B-tree

### 1.1 Text / sort-order encoders

Byte-exact fixture validation across all five formats ships in
[GeneralLegacyEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Indexes/Collation/GeneralLegacyEncoderFixtureTests.cs)
(V2000 / V2003 / V2007),
[GeneralEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Indexes/Collation/GeneralEncoderFixtureTests.cs)
(V2010), and
[General97EncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Indexes/Collation/General97EncoderFixtureTests.cs)
(V1997), all driven through
[TextIndexEncoderFixtureHarness.cs](../../JetDatabaseWriter.Tests/Infrastructure/TextIndexEncoderFixtureHarness.cs).
Property/structural assertions for the General + General 97 encoders are
in [GeneralEncoderSharedTests.cs](../../JetDatabaseWriter.Tests/Indexes/Collation/GeneralEncoderSharedTests.cs).

- [ ] **V2010 long-row stress tables (`Table11`, `Table11_desc`)** are
  partially covered: V2000 / V2003 / V2007 long-row leaves now validate
  byte-exact in [GeneralLegacyEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Indexes/Collation/GeneralLegacyEncoderFixtureTests.cs)
  via the 2-chunk encoder
  ([GeneralLegacyTextIndexEncoder.EncodeTwoChunks](../../JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs)).
  V2010 (General sort order) `Table11` / `Table11_desc` are still skipped
  in [GeneralEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Indexes/Collation/GeneralEncoderFixtureTests.cs):
  the encoder matches the first 508 of 510 bytes but the final 2-byte
  suffix algorithm is unknown (exhaustive testing of ~3.4 M hash/CRC/input
  combinations found no match — see resolution doc for details). The
  partial result is now locked in by
  [GeneralEncoderLongRowPrefixTests.cs](../../JetDatabaseWriter.Tests/Indexes/Collation/GeneralEncoderLongRowPrefixTests.cs),
  which asserts byte-exact match on bytes `[0..507]` for every 510-byte
  long-row leaf in those two tables (and full byte-exact match for the
  short / null entries that share the same indexes); regressions in the
  encoder body or in the suffix algorithm (when discovered) will trip it.
  Binary single-column long keys in V2010 `binIdxTest` and Memo-keyed
  indexes via
  [IndexCodesAggregateTests.cs](../../JetDatabaseWriter.Tests/Indexes/IndexCodesAggregateTests.cs)
  also still run un-asserted for the same reason. See
  [long-row-index-encoding.md](long-row-index-encoding.md).

---

## 2. Column type & row decode

### 2.3 Calculated columns

- [ ] **`[J]`** Access-authored calculated columns whose expressions use the
  `Switch`/`IIf` builtins, with byte-level validation of the cached result
  payload. General calculated-column fixture coverage now exists in
  `CalculatedColumnFixtureTests` (metadata, expression text, result type,
  and row decode), but we still treat the cached value bytes as opaque and
  do not yet assert builtin-specific on-disk results.

---

## 3. DAO round-trip validation

Tests in this section use `DAO.DBEngine.120` via
`AccessRoundTripEnvironment` and auto-skip when Access is not installed.
Existing infrastructure in `AccessRoundTripTests` (`RoundTripSession`,
`CaptureSnapshotAsync`, `AssertTdefMagicStampsAsync`, etc.) and the
newer `DaoValidationTests` (single-table DAO scripts via
`AccessRoundTripEnvironment.RunDaoScript`) should be reused for new DAO
scenarios rather than reinventing the setup/teardown boilerplate.

- [ ] **`[S]`** **FK Compact & Repair acceptance** — the two existing tests
  (`SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`,
  `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`) remain known-gap
  opt-in tests. DAO Compact itself succeeds, but post-compact verification
  still fails for writer-created FK-bearing tables. The focused single-FK
  test currently reports row count 0 after compact; the standalone
  `fk-dao-baseline` FormatProbe mode (legacy `DIAG_FK_DAO_BASELINE`) can produce an even stronger symptom where
  CompactDatabase exits 0 but omits the writer-created FK tables and
  relationship rows entirely. See
  [round-trip-openrecordset-hypothesis.md](/docs/design/round-trip-openrecordset-hypothesis.md).
- [x] **`[S]`** **DAO OpenRecordset row-count** —
  `DaoValidationTests.DaoOpenRecordset_RowCount_MatchesWriterOutput` now
  passes on Access-equipped hosts. The original `OpenRecordset`
  TDEF-layout blocker is resolved.
- [x] **`[S]`** **DAO index traversal** —
  `DaoValidationTests.DaoIndexTraversal_Seek_LocatesRowByPrimaryKey` now
  passes on Access-equipped hosts.
- [x] **`[S]`** **DAO AutoNumber continuation** —
  `DaoValidationTests.DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert`
  now passes; DAO continues after the writer's persisted AutoNumber
  high-water value instead of reusing an existing ID.
- [x] **`[M]`** **DAO Memo/OLE fidelity** —
  `DaoValidationTests.DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly`
  now passes after the OpenRecordset and usage-map fixes.
- [ ] **`[S]`** **DAO FK enforcement unskip** —
  `DaoValidationTests.DaoRelationshipEnforcement_FkViolation_RaisesError`
  passes with `JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1` after
  DAO-shaped FK logical cross-references and real-index `used_pages` fixes.
  It is still guarded as a known-gap opt-in test until the adjacent FK
  Compact & Repair behavior is either fixed or intentionally split into a
  separate tracked gap.
- [ ] **`[L]`** **DAO CompactDatabase on encrypted output** — test
  implemented in
  `DaoValidationTests.DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds`;
  still a known-gap opt-in test. Do not start this until plaintext FK
  Compact & Repair has a clear root cause; the encrypted failure may be in
  encryption/container metadata rather than the plain TDEF/index path.

---

## Notes on prioritisation

- Items in **§3** (DAO) can only run on Windows + Access hosts but provide
  the highest-confidence signal that our output is correct — the canonical
  engine is the final arbiter.
