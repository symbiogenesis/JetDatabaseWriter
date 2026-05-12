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
difficulty (`S`/`M`/`L`). When a gap is closed, remove it from this file and
leave the detailed resolution in the relevant design note or test source.

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
  [format-probe-long-row-index-encoding.md](format-probe/format-probe-long-row-index-encoding.md).

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

No open DAO round-trip coverage gaps are currently tracked here. The former
OpenRecordset, FK enforcement, FK Compact & Repair, Memo/OLE fidelity,
AutoNumber continuation, and encrypted CompactDatabase gaps now run under the
normal Microsoft Access guard and pass on Access-equipped hosts. The resolution
details live in [round-trip-openrecordset-hypothesis.md](design/round-trip-openrecordset-hypothesis.md)
and [round-trip-test-failures.md](design/round-trip-test-failures.md).

---

## Notes on prioritisation

- Items in **§3** (DAO) can only run on Windows + Access hosts but provide
  the highest-confidence signal that our output is correct — the canonical
  engine is the final arbiter.
