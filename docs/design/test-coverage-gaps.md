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
  ([GeneralLegacyTextIndexEncoder.EncodeTwoChunks](../../JetDatabaseWriter/Internal/GeneralLegacyTextIndexEncoder.cs)).
  V2010 (General sort order) `Table11` / `Table11_desc` are still skipped
  in [GeneralEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralEncoderFixtureTests.cs):
  the encoder matches the first 508 of 510 bytes but the final 2-byte
  suffix algorithm is unknown (exhaustive testing of ~3.4 M hash/CRC/input
  combinations found no match — see resolution doc for details). Binary
  single-column long keys in V2010 `binIdxTest` and Memo-keyed indexes via
  [IndexCodesAggregateDiagnosticTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexCodesAggregateDiagnosticTests.cs)
  also still run un-asserted for the same reason. See
  [long-row-index-encoding.md](long-row-index-encoding.md).

### 1.2 Numeric / temporal / binary keys

Broad fixture-driven coverage for single-column non-text indexes
(`Byte` / `Short` / `Long` / `Single` / `Double` / `Money` / `DateTime` /
`Boolean` / `GUID` / `Binary`) is in
[NonTextSingleColumnIndexFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/NonTextSingleColumnIndexFixtureTests.cs),
running across the Jackcess `indexTest`, `bigIndexTest`, `binIdxTest`, and
`fixedNumericTest` corpora (V2000–V2010). Each row's value is round-tripped
through `IndexKeyEncoder.EncodeEntry` and compared positionally against the
on-disk leaf entries (sorted unsigned). The remaining sub-items below call
out cases that fixture sweep does **not** specifically isolate.

- [ ] **`[J]` `[S]`** OLE long-value index keys — not present in the current
  Jackcess fixture set; needs a synthetic fixture.
- [ ] **`[J]` `[S]`** Extended Date/Time (`SHORT_DATE_TIME` extended; 42-byte
  blocks separated by `0x09`, asc/desc trailer flip). Jackcess test:
  `testExtDateIndex`.

### 1.3 B-tree structural

Structural-invariant sweep across the Jackcess fixture corpus is in
[IndexBTreeStructuralFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexBTreeStructuralFixtureTests.cs):
asserts row-count == leaf-entry-count for `compIndexTest*` (Jackcess
`testComplexIndex`), unsigned byte-order monotonicity of every leaf chain
(Jackcess `testByteOrder`), and that every leaf entry's row trailer points
at a data-page within the file. Leaf-walker bugs that would silently
degrade the §1.1 / §1.2 fixture comparisons are caught up-front.

- [ ] **`[J]`** Index page splits where the **child_tail** pointer is
  populated (Jackcess `IndexPageCache.validate` walks this; we don't read it).
- [ ] **`[J]` `[L]`** Round-trip an index after **multi-level intermediate
  splits** caused by deletes — `IndexSurgicalCascadingIntermediateCollapseTests`
  covers collapse, but not the rebalance/borrow path Jackcess exercises in
  `testCursors`/`testIndexCreation`.
- [ ] **`[M]` `[S]`** Compare our `next_page`/`prev_page` chain against
  mdbtools' `mdb-index` dump for at least one fixture — provides an
  external sanity check. (The internal sweep validates ordering, but not
  the chain itself against an independent implementation.)
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

### 1.4 Index flags & metadata

- [ ] **`[J]` `[S]`** Writer round-trip of `IGNORE_NULLS_INDEX_FLAG` (`0x02`)
  and `REQUIRED_INDEX_FLAG` (`0x08`) combinations (reader exposure is
  covered by `IndexFlagCombinationsTests`). Requires adding `IgnoreNulls` /
  `IsRequired` properties to `IndexDefinition`.
- [ ] **`[J]` `[S]`** **Foreign-key surrogate** indexes (the auto-created
  ones backing relationships) — round-trip + listing semantics; we filter
  them out today.

---

## 2. Column type & row decode

### 2.1 Long-value (LVAL) chains

- [ ] **`[J]`** Type 1 (inline), Type 2 (single-page), Type 3 (multi-page
  chained) Memo/OLE values across V1997 → V2010 — Jackcess `LongValueTest`
  covers all three; we have round-trip but no per-type assertion.
- [ ] **`[J]` `[M]`** Compressed (UCS-2 → ASCII) Memo prefixes longer than 1
  inline page — mdbtools regressions historically hit truncation here.
- [ ] **`[J]` `[S]`** Reader-side coverage of an Access-authored fixture
  containing a Memo with embedded `0x00` bytes. The writer round-trip path
  is closed by `AccessWriterTests.InsertRow_MemoWithEmbeddedNulls_RoundTrips`,
  but no Access-authored fixture exercises the read path yet.
- [ ] **`[J]`** OLE long values whose header reports a **mismatched length**
  vs the actual chain — Jackcess has a "lvalLength" sanity test.

### 2.2 Complex columns (Multi-Value, Attachment, Versioned text)

- [ ] **`[J]`** Versioned-text column with > 100 historical versions to
  exercise the LVAL chain inside the per-row complex sub-table.

### 2.3 Calculated columns

- [ ] **`[J]`** Reading a calculated column whose expression uses the
  Access `Switch`/`IIf` builtins; verify cached result bytes match
  expectations on disk (we treat them as opaque today).

---

## 3. Encryption / password

- [ ] **`[J]`** **Office 2007 (ECMA-376) standard** AES-128 encryption — we
  have agile-encryption coverage; Jackcess tests both standard and agile.
- [ ] **`[J]` `[O]`** CFB streams whose **mini-FAT chain spans more than one
  mini-stream sector** (OpenMcdf `MiniStreamLargeChain` fixtures).
- [ ] **`[J]`** `lv_prop` / `MSysDb` properties block round-trip after a
  password change.

---

## 4. Linked tables & relationships

- [ ] **`[J]`** Linked table to a CSV / text file (the `MSysObjects.Type`
  variant we don't currently surface).

---

## 5. Conformance / cross-tool

- [ ] **`[M]`** Run `mdb-export` over each of our written outputs in CI and
  diff against the source CSV. We do an in-process round-trip; an external
  tool diff catches encoder bugs that round-trip silently.
- [ ] **`[J]`** Open every database under `JetDatabaseWriter.Tests/Databases/Jackcess/`
  with both Jackcess (via a small Java sidecar in CI) **and** our reader,
  diff the resulting `DataTable`s. Currently we trust Jackcess's
  fixtures by construction; an explicit diff would catch encoding drift on
  re-write.

---

## Notes on prioritisation

- Items in **§3** require new fixture authoring (Office tooling) and are
  larger.
