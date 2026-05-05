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
[GeneralLegacyEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralLegacyEncoderFixtureTests.cs)
(V2000 / V2003 / V2007),
[GeneralEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralEncoderFixtureTests.cs)
(V2010), and
[General97EncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/General97EncoderFixtureTests.cs)
(V1997), all driven through
[TextIndexEncoderFixtureHarness.cs](../../JetDatabaseWriter.Tests/Internal/TextIndexEncoderFixtureHarness.cs).
Property/structural assertions for the General + General 97 encoders are
in [GeneralAndGeneral97EncoderUnitTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralAndGeneral97EncoderUnitTests.cs).

- [ ] **V2010 long-row stress tables (`Table11`, `Table11_desc`)** are
  partially covered: V2000 / V2003 / V2007 long-row leaves now validate
  byte-exact in [GeneralLegacyEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralLegacyEncoderFixtureTests.cs)
  via the 2-chunk encoder
  ([GeneralLegacyTextIndexEncoder.EncodeTwoChunks](../../JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs)).
  V2010 (General sort order) `Table11` / `Table11_desc` are still skipped
  in [GeneralEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralEncoderFixtureTests.cs):
  the encoder matches the first 508 of 510 bytes but the final 2-byte
  suffix algorithm is unknown (exhaustive testing of ~3.4 M hash/CRC/input
  combinations found no match — see resolution doc for details). Binary
  single-column long keys in V2010 `binIdxTest` and Memo-keyed indexes via
  [IndexCodesAggregateDiagnosticTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexCodesAggregateDiagnosticTests.cs)
  also still run un-asserted for the same reason. See
  [long-row-index-encoding.md](long-row-index-encoding.md).

### 1.2 B-tree structural

Structural-invariant sweep across the Jackcess fixture corpus is in
[IndexBTreeStructuralFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexBTreeStructuralFixtureTests.cs):
asserts row-count == leaf-entry-count for `compIndexTest*` (Jackcess
`testComplexIndex`), unsigned byte-order monotonicity of every leaf chain
(Jackcess `testByteOrder`), and that every leaf entry's row trailer points
at a data-page within the file. Doubly-linked list consistency
(`prev(next(page)) == page`) is verified by
[IndexLeafChainConsistencyTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexLeafChainConsistencyTests.cs).
`child_tail` pointer validity on intermediate pages is checked by
[IndexChildTailPointerTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexChildTailPointerTests.cs).
Leaf-walker bugs that would silently degrade the §1.1 / §1.2 fixture
comparisons are caught up-front.

- [ ] **`[J]` `[L]`** Round-trip an index after **multi-level intermediate
  splits** caused by deletes — `IndexSurgicalCascadingIntermediateCollapseTests`
  covers collapse, but not the rebalance/borrow path Jackcess exercises in
  `testCursors`/`testIndexCreation`.
- [ ] **`[J]` `[M]`** **`bigIndexTest*` fixtures surface zero scannable
  B-trees / zero leaf entries / zero "applicable" single-column non-text
  indexes across all four format variants (V2000, V2003, V2007, V2010)** in
  [IndexBTreeStructuralFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexBTreeStructuralFixtureTests.cs)
  and [NonTextSingleColumnIndexFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/NonTextSingleColumnIndexFixtureTests.cs).
  These fixtures are purpose-built to stress wide/deep index B-trees, so
  the consistent "nothing to scan" signal across three independent theory
  tests strongly suggests our `ListIndexesAsync` path is returning
  `FirstDp=0` (or filtering as FK) for every index in those fixtures
  rather than the fixtures legitimately being empty. Same symptom appears
  for `compIndexTestV2003` (zero leaf entries, no non-FK index with
  `first_dp` on Table1). Worth re-checking after the 2026-05-03 Jet3
  `ColMapOffset` fix — the V1997 reader bug that fix closed had the same
  symptom, but for Jet4/ACE fixtures so the root cause must differ.

---

## 2. Column type & row decode

### 2.1 Long-value (LVAL) chains

Per-type form coverage (inline 0x80, single-page 0x40, chained 0x00) for
both Memo and OLE round-trips through the writer is in
[LvalFormAssertionTests.cs](../../JetDatabaseWriter.Tests/Core/LvalFormAssertionTests.cs).
Compressed (Latin-1) Memo LVAL chains are covered by
[CompressedMemoLvalTests.cs](../../JetDatabaseWriter.Tests/Core/CompressedMemoLvalTests.cs).
Reader-side Memo from Access-authored overflow fixtures is in
[OverflowMemoReadTests.cs](../../JetDatabaseWriter.Tests/Core/OverflowMemoReadTests.cs).
Reader-side Memo with embedded `0x00` bytes from a DAO-authored database
is covered by
[DaoValidationTests.DaoAuthoredMemo_WithEmbeddedNuls_ReaderReturnsExactContent](../../JetDatabaseWriter.Tests/Core/DaoValidationTests.cs).

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
  Access-equipped hosts (via `SkipUnless`) and fail visibly. The N1
  (single-table) case passes DAO C&R after the entry-start bitmask
  sentinel fix, but N2 (two tables + relationship) still fails — the
  second `CreateTableAsync` splice overflows the `ParentIdName` leaf
  without `maxPrefixLength` cap. See
  [round-trip-test-failures.md](round-trip-test-failures.md).
- [ ] **`[S]`** **DAO OpenRecordset row-count** — test implemented in
  `DaoValidationTests.DaoOpenRecordset_RowCount_MatchesWriterOutput`;
  now runs (and fails with "Unrecognized database format") on
  Access-equipped hosts rather than being unconditionally skipped.
  Blocked on TDEF page-layout compatibility.
- [ ] **`[S]`** **DAO index traversal** — test implemented in
  `DaoValidationTests.DaoIndexTraversal_Seek_LocatesRowByPrimaryKey`;
  same status as row-count (runs, fails, blocked on TDEF).
- [ ] **`[S]`** **DAO AutoNumber continuation** — test implemented in
  `DaoValidationTests.DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert`;
  same status as row-count (runs, fails, blocked on TDEF).
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
