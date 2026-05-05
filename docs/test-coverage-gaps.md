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

- [ ] **`[S]`** **Compact & Repair acceptance** — the two existing tests
  (`SinglePk_AndSingleColumnFk_SurviveCompactAndRepair`,
  `CompositePk_AndMultiColumnFk_SurviveCompactAndRepair`) now run on
  Access-equipped hosts (via `SkipUnless`) and fail visibly. DAO Compact
  itself **succeeds** (catalog index fixes resolved `MSysDb 3011`), but DAO
  **drops rows** from writer-created user tables (row count = 0 post-compact)
  because it cannot open the writer's user-table TDEFs (“Unrecognized
  database format”). Blocked on a per-table TDEF structural incompatibility;
  the catalog layer is correct. See
  [round-trip-test-failures.md](round-trip-test-failures.md).
- [ ] **`[S]`** **DAO OpenRecordset row-count** — test implemented in
  `DaoValidationTests.DaoOpenRecordset_RowCount_MatchesWriterOutput`;
  skipped with a doc pointer. DAO can `OpenDatabase` the writer’s file
  (catalog is fine) but `OpenRecordset` rejects the writer-created
  user-table TDEF with “Unrecognized database format”. Disconfirmed
  hypothesis: catalog text-encoding (MSysObjects.Name compression) was
  fixed (`EncodeJet4Text` now respects `COMPRESSED_UNICODE_EXT_FLAG_MASK`)
  but did not resolve this. Blocked on TDEF page-layout compatibility.
- [ ] **`[S]`** **DAO index traversal** — test implemented in
  `DaoValidationTests.DaoIndexTraversal_Seek_LocatesRowByPrimaryKey`;
  same status as row-count (skipped, blocked on TDEF).
- [ ] **`[S]`** **DAO AutoNumber continuation** — test implemented in
  `DaoValidationTests.DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert`;
  same status as row-count (skipped, blocked on TDEF).
- [ ] **`[M]`** **DAO Memo/OLE fidelity** — write Memo values containing
  embedded NULs, non-Latin-1 (CJK), and OLE binary payloads; verify DAO
  `Recordset.Fields("col").Value` returns the identical bytes. Catches
  LVAL-chain encoding bugs that survive our own reader.
- [ ] **`[M]`** **DAO relationship enforcement** — write a parent/child pair
  with `EnforceReferentialIntegrity = true`, then use DAO to attempt an
  insert that violates the FK. Assert DAO raises error 3201 (cannot add
  record — referential integrity violated). Confirms the relationship
  metadata is fully understood by Access.
- [ ] **`[L]`** **DAO CompactDatabase on encrypted output** — write a
  password-protected ACCDB (AES), run Compact & Repair with the password
  supplied via DAO, and verify the compacted file reopens. Validates the
  encryption header is well-formed enough for Access's own engine.
- [ ] **`[L]`** **DAO multi-table stress** — create 10+ tables with indexes,
  relationships, and 1000+ rows each; Compact & Repair the result. Stress
  test for page allocation and usage-map consistency at scale.

---

## Notes on prioritisation

- Items in **§3** (DAO) can only run on Windows + Access hosts but provide
  the highest-confidence signal that our output is correct — the canonical
  engine is the final arbiter.
