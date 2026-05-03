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

Items already covered are **not** listed; this is a pure gap list. Each
section tags the source project with `[J]` (Jackcess), `[M]` (mdbtools), or
`[O]` (OpenMcdf) and the rough difficulty (`S`/`M`/`L`).

---

## 1. Index encoding & B-tree

### 1.1 Text / sort-order encoders
- [ ] **`[J]` `[S]`** Per-fixture byte-exact validation for **General** (2010+) and
  **General 97** sort orders, mirroring the new `GeneralLegacyEncoderFixtureTests`.
  Current coverage validates only the `GeneralLegacy` encoder against
  `testIndexCodes*` fixtures (V2000–V2007 in [GeneralLegacyEncoderFixtureTests.cs](JetDatabaseWriter.Tests/Internal/GeneralLegacyEncoderFixtureTests.cs);
  V2010 is excluded there because its default sort order is **General**, not
  **General Legacy**). The V2010 long-row tables (`Table11`, `Table11_desc`)
  are explicitly skipped — Jackcess does the same
  ("TODO long rows not handled completely yet in V2010"). Closing the General /
  General 97 gap would also let us re-include those tables.
- [x] **`[J]` `[S]`** Composite (multi-column) text index encoding round-trip
  against `testCompIndex.mdb` / `.accdb`. Addressed by
  [CompositeTextIndexFixtureTests.cs](JetDatabaseWriter.Tests/Internal/CompositeTextIndexFixtureTests.cs)
  (V2000–V2010; V1997 excluded — Jet3 page layout). The pre-existing
  `IndexKeyEncoderTests` cover composite ordering at the unit level; the new
  fixture test re-encodes per-row key tuples and compares positionally
  against the on-disk leaf bytes.
- [ ] **`[J]` `[S]`** Aggregate-diagnostic variant of the encoder fixture test
  (does **not** bail on first mismatch; emits per-(table, index) match counts,
  length-mismatch counts, and up to N example mismatches with hex) is now in
  [IndexCodesAggregateDiagnosticTests.cs](JetDatabaseWriter.Tests/Internal/IndexCodesAggregateDiagnosticTests.cs).
  Useful when triaging encoder regressions; not a coverage gap by itself.
- [ ] **`[J]` `[M]`** Descending text columns where the first byte of the
  encoded key would otherwise be `0x00` (encoder must emit the descending
  flag and not collapse to "null"). The Jackcess `IndexCodesTest.testReadIndex`
  loop covers this implicitly.
- [ ] **`[J]` `[S]`** "Crazy code" surrogate pair coverage: extend the fixture
  test to assert at least one indexed value in the corpus that exercises the
  high+low surrogate handler (fixture rows containing characters in the
  Supplementary Multilingual Plane). If the Jackcess fixtures don't include
  any, add a synthetic round-trip test.
- [ ] **`[J]` `[M]`** Right-to-left scripts (Hebrew, Arabic) and combining-
  diacritic NFD/NFC equivalence in keys — Jackcess has scattered cases under
  `testIndexCodesV*`; we should confirm our encoder matches Access's
  pre-normalisation.

### 1.2 Numeric / temporal / binary keys

Broad fixture-driven coverage for single-column non-text indexes
(`Byte` / `Short` / `Long` / `Single` / `Double` / `Money` / `DateTime` /
`Boolean` / `GUID` / `Binary`) is now in
[NonTextSingleColumnIndexFixtureTests.cs](JetDatabaseWriter.Tests/Internal/NonTextSingleColumnIndexFixtureTests.cs),
running across the Jackcess `indexTest`, `bigIndexTest`, `binIdxTest`, and
`fixedNumericTest` corpora (V2000–V2010). Each row's value is round-tripped
through `IndexKeyEncoder.EncodeEntry` and compared positionally against the
on-disk leaf entries (sorted unsigned). The remaining sub-items below call
out cases that fixture sweep does **not** specifically isolate.

- [x] **`[J]` `[M]` `[M]`** Money / Decimal (NUMERIC) keys — covered by
  `NonTextSingleColumnIndexFixtureTests` against the `fixedNumericTest*`
  fixtures (V2000–V2010), exercising both legacy (Jet4) and ACE
  twiddling. The encoder defaults to `T_MONEY` (0x05); ACE-only `T_NUMERIC`
  (0x11) still routes through the same fixed-point path. **Per-format
  isolated assertion** of `LegacyFixedPointColumnDescriptor` vs
  `FixedPointColumnDescriptor` sign-byte handling is still open.
- [x] **`[J]` `[M]`** GUID index keys — covered by the fixture sweep where
  GUID-typed indexes appear in the corpus.
- [x] **`[J]` `[S]`** `BINARY` index keys — covered by the fixture sweep
  against `binIdxTestV2010` (closes the "no fixture comparison" complement
  to `IndexBinaryKeyTests`). **OLE long-value** index keys are not present
  in the current fixture set and remain open.
- [ ] **`[J]` `[S]`** Extended Date/Time (`SHORT_DATE_TIME` extended; 42-byte
  blocks separated by `0x09`, asc/desc trailer flip). Jackcess test:
  `testExtDateIndex`.
- [x] **`[J]` `[M]`** Negative Single/Double values across asc/desc —
  exercised wherever the `indexTest*` / `bigIndexTest*` fixtures contain
  negative float/double rows (the `(isAsc, isNeg)` matrix is hit
  positionally rather than enumerated explicitly). A focused unit-level
  matrix test would still be useful as a regression guard.

### 1.3 B-tree structural

Structural-invariant sweep across the Jackcess fixture corpus is now in
[IndexBTreeStructuralFixtureTests.cs](JetDatabaseWriter.Tests/Internal/IndexBTreeStructuralFixtureTests.cs):
asserts row-count == leaf-entry-count for `compIndexTest*` (Jackcess
`testComplexIndex`), unsigned byte-order monotonicity of every leaf chain
(Jackcess `testByteOrder`), and that every leaf entry's row trailer points
at a data-page within the file. Leaf-walker bugs that would silently
degrade the §1.1 / §1.2 fixture comparisons are now caught up-front.

- [ ] **`[J]`** Index page splits where the **child_tail** pointer is
  populated (Jackcess `IndexPageCache.validate` walks this; we don't read it).
- [ ] **`[J]` `[L]`** Round-trip an index after **multi-level intermediate
  splits** caused by deletes — `IndexSurgicalCascadingIntermediateCollapseTests`
  covers collapse, but not the rebalance/borrow path Jackcess exercises in
  `testCursors`/`testIndexCreation`.
- [x] **`[J]` `[S]`** Internal sanity check that every B-tree leaf chain is
  monotonically sorted in unsigned byte order (Jackcess
  `IndexTest.testByteOrder` analogue) — fixture sweep in
  `IndexBTreeStructuralFixtureTests.LeafChain_IsSortedByUnsignedByteOrder`,
  plus the unit-level invariants in
  [IndexByteOrderUnitTests.cs](JetDatabaseWriter.Tests/Internal/IndexByteOrderUnitTests.cs)
  (`testByteOrder`, `testByteCodeComparator`, asc/desc encoder ordering,
  null-flag bytes 0x00 / 0xFF, non-null start flags 0x7F / 0x80).
- [x] **`[J]` `[S]`** Row-count vs leaf-entry-count parity for the
  `compIndexTest*` fixtures (Jackcess `testComplexIndex` analogue) —
  `IndexBTreeStructuralFixtureTests.CompIndex_RowCount_EqualsLeafEntryCount`.
- [ ] **`[M]` `[S]`** Compare our `next_page`/`prev_page` chain against
  mdbtools' `mdb-index` dump for at least one fixture — provides an
  external sanity check. (The new internal sweep validates ordering, but
  not the chain itself against an independent implementation.)

### 1.4 Index flags & metadata
- [x] **`[J]` `[S]`** Indexes with `IGNORE_NULLS_INDEX_FLAG` (`0x02`) and
  `REQUIRED_INDEX_FLAG` (`0x08`) — reader exposure validated against the
  Jackcess `testIndexProperties*` fixtures (V2000/V2003/V2007) by
  [IndexFlagCombinationsTests.cs](JetDatabaseWriter.Tests/Core/IndexFlagCombinationsTests.cs),
  which asserts each fixture surfaces ≥ 2 distinct `(unique, ignoreNulls,
  required)` combinations and that at least one `IgnoreNulls=true` index is
  visible. **Writer round-trip** of these flag combinations is still open.
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
- [ ] **`[J]`** Memo column with embedded **`0x00` bytes** (Jackcess
  `testEmbeddedNulls`).
- [ ] **`[J]`** OLE long values whose header reports a **mismatched length**
  vs the actual chain — Jackcess has a "lvalLength" sanity test.

### 2.2 Complex columns (Multi-Value, Attachment, Versioned text)
- [ ] **`[J]`** `testComplex.accdb` round-trip per complex sub-type:
  - Multi-value text **with mixed value lengths** including null members.
  - Attachment with **0-byte payload** and **deflate-compressed** payload;
    verify our reader matches Jackcess `AttachmentImpl.getFileData`.
  - Versioned-text column with > 100 historical versions to exercise the
    LVAL chain inside the per-row complex sub-table.
- [ ] **`[J]`** Cascading delete across complex columns where the parent
  table has **two** complex columns referencing different sub-tables. We
  cover the single-complex case in `ComplexColumnsCascadeDeleteTests`.

### 2.3 Calculated columns
- [ ] **`[J]`** Calculated-column expressions that reference **another
  calculated column** (forward and backward dependency order) — Jackcess
  `CalcColTest` covers this.
- [ ] **`[J]`** Reading a calculated column whose expression uses the
  Access `Switch`/`IIf` builtins; verify cached result bytes match
  expectations on disk (we treat them as opaque today).

### 2.4 Numeric edge cases
- [ ] **`[J]` `[M]`** NUMERIC(28,28) min/max & scale boundaries — mdbtools
  historically rounded these; confirm we don't.
- [ ] **`[J]`** Currency rounding at `MIN/MAX_VALUE` and the
  `Decimal.MinValue + 1` boundary.
- [ ] **`[J]`** DateTime values straddling the 1899-12-30 epoch and the 1900
  Excel-leap-year quirk.

### 2.5 Hyperlink fields
- [ ] **`[J]`** Hyperlink with **all four parts** populated (display, address,
  subaddress, screentip) at maximum length — current `HyperlinkTests` cover
  basics only.
- [ ] **`[J]`** Hyperlink with embedded `#` literal in the URL (must be
  doubled in the on-disk encoding).

---

## 3. Encryption / password

- [ ] **`[J]`** **Office 2007 (ECMA-376) standard** AES-128 encryption — we
  have agile-encryption coverage; Jackcess tests both standard and agile.
- [ ] **`[J]` `[O]`** CFB streams whose **mini-FAT chain spans more than one
  mini-stream sector** (OpenMcdf `MiniStreamLargeChain` fixtures).
- [ ] **`[J]`** Round-trip writing of an **encrypted** database (we currently
  read AES/Agile but don't re-encrypt on write — confirm this is intentional
  and add a `Skip = "writer not supported"` Theory marker so the gap is
  visible in the test report).
- [ ] **`[J]`** `lv_prop` / `MSysDb` properties block round-trip after a
  password change.
- [ ] **`[J]`** RC4 (Jet 4 "obfuscation") password verification with the
  legacy `0x6b39dac7` / `0xa5316276` constants — we verify ACE; verify the
  Jet4 path too.
- [ ] **`[O]`** CFB **DIFAT chain** with > 109 FAT sectors (i.e., a database
  > ~7 MB after encryption) — exercises the secondary DIFAT walker that
  OpenMcdf fuzz-tests.

---

## 4. Catalog / system tables

- [ ] **`[J]`** `MSysAccessStorage` / `MSysNavPaneGroups` round-trip for
  ACE (these are referenced by Jackcess but not asserted).
- [ ] **`[J]`** `MSysObjects.Flags` bits: hidden, system, replicated.
  Verify our `ListTablesAsync` filtering matches Access's "show
  hidden/system" semantics.
- [ ] **`[J]`** Table with > 32 columns and > 16 indexes (boundary of the
  index-block layout) — mdbtools historically clipped at 32.
- [ ] **`[J]`** Querying `MSysQueries` row-set for a **parameterised** query
  definition; we have basic query metadata but no parameter assertions.

---

## 5. Page / row layout corner cases

- [ ] **`[M]`** Row whose **variable-length column count exceeds 127** — uses
  the 2-byte length prefix path; mdbtools `pkrep` exercises this.
- [ ] **`[M]`** Row with a **null mask** that crosses an 8-byte boundary
  exactly (off-by-one historic bug in mdbtools).
- [ ] **`[J]`** Row spanning a **page boundary via overflow pointer**
  (`0x80`-flagged row id) where the overflow target is itself a row whose
  variable section needs another overflow.
- [ ] **`[J]`** Empty data page whose `free_space` equals the full page-size
  minus header — verify reader doesn't decode any rows.

---

## 6. Linked tables & relationships

- [ ] **`[J]`** Linked table to a CSV / text file (the `MSysObjects.Type`
  variant we don't currently surface).
- [ ] **`[J]`** Self-referential foreign key with cascade-update.
- [ ] **`[J]`** Many-to-many through a junction table where both FKs have
  `enforce_referential_integrity=true` — assert relationship mutation
  ordering.

---

## 7. Fuzz / robustness

- [ ] **`[J]`** Truncated file fuzz (last N bytes lopped off) — Jackcess
  `testCorruptDb`. We have generic fuzz; add a deterministic truncation
  matrix.
- [ ] **`[O]`** CFB with a **circular FAT chain** — must throw, not loop.
  OpenMcdf has explicit tests.
- [ ] **`[O]`** CFB with **negative free-sector count** in header.
- [ ] **`[J]`** Index page whose entry-mask claims more entries than the
  page's free-space allows.

---

## 8. Conformance / cross-tool

- [ ] **`[M]`** Run `mdb-export` over each of our written outputs in CI and
  diff against the source CSV. We do an in-process round-trip; an external
  tool diff catches encoder bugs that round-trip silently.
- [ ] **`[J]`** Open every database under `JetDatabaseWriter.Tests/Databases/Jackcess/`
  with both Jackcess (via a small Java sidecar in CI) **and** our reader,
  diff the resulting `DataTable`s. Currently we trust Jackcess's
  fixtures by construction; an explicit diff would catch encoding drift on
  re-write.

---

## 9. API / model surface

- [ ] **`[J]`** Concurrent `AccessReader` open + iterate from multiple
  tasks against the same file (we have `JetByteRangeLock` tests but no
  end-to-end multi-reader scenario).
- [ ] **`[J]`** `AccessWriter` + transaction rollback after a constraint
  violation deep in a multi-row insert (`PendingChange.rollback`
  equivalent path).
- [x] **`[J]`** `IndexMetadata.IgnoreNulls` / `IsRequired` / `IsUnique`
  combinations — surface-area assertion that the reader parses multiple
  distinct combinations correctly from the on-disk flag byte. Covered by
  [IndexFlagCombinationsTests.cs](JetDatabaseWriter.Tests/Core/IndexFlagCombinationsTests.cs)
  against the `testIndexProperties*` fixtures. (Strict full-Cartesian
  coverage of all 8 combinations would require synthetic fixtures; the
  Jackcess corpus contains a representative subset.)

---

## Notes on prioritisation

- Items in **§1.1** are the most directly actionable now that the
  fixture-driven encoder harness exists; new sort orders (General,
  General 97) plug into the same `Fixtures` `TheoryData` shape used by
  `GeneralLegacyEncoderFixtureTests` and `CompositeTextIndexFixtureTests`.
  Closing the General sort-order gap would also let V2010 fixtures (and
  the `Table11` / `Table11_desc` long-row tables) re-enter scope.
- Items in **§3** require new fixture authoring (Office tooling) and are
  larger.
- Items in **§7** can be largely automated by extending the existing
  `Fuzz/` projects with deterministic seeds.

---

## Recently closed

- **§1.1** composite-text-index round-trip — `CompositeTextIndexFixtureTests`.
- **§1.2** non-text single-column index encoding (Byte/Short/Long/Single/
  Double/Money/DateTime/Boolean/GUID/Binary) across `indexTest*`,
  `bigIndexTest*`, `binIdxTestV2010`, and `fixedNumericTest*` fixtures —
  `NonTextSingleColumnIndexFixtureTests`.
- **§1.3** structural invariants (row-count vs leaf-entry-count for
  `compIndexTest*`, leaf chain unsigned-byte ordering, leaf trailers
  pointing at valid data pages) — `IndexBTreeStructuralFixtureTests`;
  unit-level `testByteOrder` / `testByteCodeComparator` / encoder
  asc/desc and null-flag invariants — `IndexByteOrderUnitTests`.
- **§1.4** / **§9** index flag-byte parse (`IGNORE_NULLS`, `REQUIRED`,
  `UNIQUE` combinations) — `IndexFlagCombinationsTests`.
- Supporting reader change: `IndexMetadata.FirstDp` (internal) plus
  ascending-flag mask fix in `IndexLayout.ReadColMap` /
  `ReadColMapAndCollect` — both required for the encoder fixture tests to
  walk B-tree leaves correctly across writer conventions.
