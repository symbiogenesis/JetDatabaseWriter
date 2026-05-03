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

Items already covered are **not** listed here — closed work is summarised
in the "Recently closed" section at the bottom. Each open gap is tagged
with the source project (`[J]` Jackcess, `[M]` mdbtools, `[O]` OpenMcdf)
and the rough difficulty (`S`/`M`/`L`).

---

## 1. Index encoding & B-tree

### 1.1 Text / sort-order encoders
- [ ] **`[J]` `[S]`** Per-fixture byte-exact validation for **General** (2010+) and
  **General 97** sort orders, mirroring the existing `GeneralLegacyEncoderFixtureTests`.
  Current coverage validates only the `GeneralLegacy` encoder against
  `testIndexCodes*` fixtures (V2000–V2007 in
  [GeneralLegacyEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralLegacyEncoderFixtureTests.cs));
  V2010 is excluded there because its default sort order is **General**, not
  **General Legacy**. The V2010 long-row tables (`Table11`, `Table11_desc`)
  are explicitly skipped — Jackcess does the same
  ("TODO long rows not handled completely yet in V2010"). Closing the General /
  General 97 gap would also let us re-include those tables.

  > **Canonical home for the upstream Jackcess long-row TODO.** The same
  > limitation is referenced (and these tables/keys are skipped) from:
  > [GeneralLegacyEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralLegacyEncoderFixtureTests.cs)
  > (text long-row stress tables `Table11` / `Table11_desc`),
  > [NonTextSingleColumnIndexFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/NonTextSingleColumnIndexFixtureTests.cs)
  > (binary single-column long keys in V2010 `binIdxTest`),
  > [IndexCodesAggregateDiagnosticTests.cs](../../JetDatabaseWriter.Tests/Internal/IndexCodesAggregateDiagnosticTests.cs)
  > (aggregate ignores Memo-keyed indexes), and the README "Limitations →
  > Index keys" bullet. Repo-memory note: `/memories/repo/long-row-index-todo.md`.
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
`Boolean` / `GUID` / `Binary`) is in
[NonTextSingleColumnIndexFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/NonTextSingleColumnIndexFixtureTests.cs),
running across the Jackcess `indexTest`, `bigIndexTest`, `binIdxTest`, and
`fixedNumericTest` corpora (V2000–V2010). Each row's value is round-tripped
through `IndexKeyEncoder.EncodeEntry` and compared positionally against the
on-disk leaf entries (sorted unsigned). The remaining sub-items below call
out cases that fixture sweep does **not** specifically isolate.

- [ ] **`[J]` `[S]`** Per-format isolated assertion of
  `LegacyFixedPointColumnDescriptor` vs `FixedPointColumnDescriptor`
  sign-byte handling for Money / NUMERIC keys (the fixture sweep exercises
  both paths positionally but does not isolate the sign-byte branch).
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
- [ ] **`[J]`** Many-to-many through a junction table where both FKs have
  `enforce_referential_integrity=true` — assert relationship mutation
  ordering.

---

## 7. Fuzz / robustness

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

- [ ] **`[J]`** `AccessWriter` + transaction rollback after a constraint
  violation deep in a multi-row insert (`PendingChange.rollback`
  equivalent path).
- [ ] **`[S]`** Writer round-trip of a parent-side `UpdateRowsAsync` that
  rewrites the PK column itself — surfaced while authoring the §6
  self-referential FK test. Existing cascade-update tests assert the
  child-side index repoint, but the parent row's new PK value is not
  currently observed on disk after the update. Needs a focused regression
  that opens the file post-update and asserts the parent row's PK column
  has the new value.

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
