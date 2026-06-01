# README Critical Review Assessment TODOs

**Status:** Proposed backlog, created 2026-05-31.

This document assesses the supplied README-level critical review against the current repository and extracts only the follow-up items that still appear useful. It intentionally excludes the review's popularity/reputation concern; per the review request, the accuracy and support for the claims matter more than repository popularity signals.

The main distinction to preserve is:

- **Confirmed or partially confirmed risk:** current docs or source support an actionable follow-up.
- **Already documented or already implemented:** the criticism names a real caveat, but the current repo already has the relevant caveat, abstraction, or test coverage.
- **Not accepted as a todo:** the finding is stale, too broad, or already tracked in a more specific design backlog.

Primary evidence used here: [README.md](../../README.md), [cve-vulnerability-analysis.md](../cve-vulnerability-analysis.md), [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md), [data-remanence-todos.md](data-remanence-todos.md), [THIRD-PARTY-NOTICES.md](../../THIRD-PARTY-NOTICES.md), public interfaces under `JetDatabaseWriter/Interfaces/`, encryption code under `JetDatabaseWriter/Encryption/`, transaction journaling in `JetDatabaseWriter/Pages/PageJournal.cs`, and index stress tests under `JetDatabaseWriter.Tests/Indexes/`.

## Validity Assessment

| Review area | Assessment | Disposition |
|---|---|---|
| CVE claim framing | **Valid.** The README and CVE doc still use completeness language such as "All 36 known relevant CVEs have been addressed" / "0 unmitigated gaps remain." The underlying work is better described as a surveyed-CVE triage and vulnerability-class hardening effort. | Add claim-framing todos. |
| Collation "byte-exact" wording | **Partially valid.** The README already narrows V2010 ACE byte parity to checked-in Access-authored `Table11` / `Table11_desc` long rows and exported matrices, so the critique overstates the overclaim. Still, the public phrase "byte-exact" can read as general correctness for arbitrary Unicode/index shapes. | Add a docs-scope todo and optional oracle-expansion todo. |
| Broad writer compatibility risk | **Partially valid.** The validation matrix has substantial DAO CompactDatabase/OpenRecordset coverage and explicitly treats writer-created files as subjects under test, not oracles. The combinatorial risk remains inherent to a writer for JET/ACE pages. | Link the validation matrix from public docs; keep feature-change coverage rules. |
| Calculated column semantic fidelity | **Valid.** The evaluator is a local implementation/adaptation, with DoS tests and metadata round trips, but there is no concise public grammar/divergence contract. | Add grammar/divergence docs and refusal tests. |
| Referential integrity guarantees | **Valid.** FK checks and cascades are enforced by `AccessWriter` code, not by a running database engine. The README states concurrent writers can corrupt files, but the relationship section should say RI is writer-instance/process-local. | Add docs todo. |
| B-tree depth, split, merge, and rebalance | **Mostly stale.** The repo now has multi-level B-tree, recursive intermediate split, collapse, borrow/rebalance, and DAO validation coverage. | No broad todo; only extend tests when index bytes or algorithms change. |
| Free-page/free-list management | **Already tracked.** Storage maintenance and remanence concerns are covered by the validation matrix and [data-remanence-todos.md](data-remanence-todos.md). | Do not duplicate here. |
| Crypto CSPRNG | **Mostly already implemented.** Office Standard/Agile salts, verifier material, HMAC keys, flat Agile encoding keys, and Jet4 RC4 db keys use `RandomNumberGenerator`. | No CSPRNG todo unless new random material is added. |
| Crypto comparisons | **Partially valid.** Office Standard/Agile password verifier and Agile HMAC comparisons use fixed-time comparison. Legacy Jet4/ACCDB header password checks in `EncryptionManager.ResolveReaderPageKeys` still use `SequenceEqual`. | Add fixed-time legacy password compare todo. |
| Key material lifetime | **Partially valid.** Password UTF-16 buffers and some PBKDF scratch buffers are zeroed, but derived keys/intermediate keys/HMAC material commonly remain in managed arrays until GC. | Add best-effort zeroization and documentation todo. |
| Password `string` exposure | **Partially valid.** Options store `ReadOnlyMemory<char>`, and encryption mutation APIs now take `ReadOnlyMemory<char>`. Convenience constructors and caller-created `string.AsMemory()` values can still leave immutable strings on the GC heap. | Mutation API completed; keep residual managed-memory documentation todo. |
| Weak encryption formats | **Valid as documentation/API risk.** The API can explicitly write legacy RC4/password-only/AES-ECB-compatible formats because Access compatibility requires them. Agile is available, but docs should make it the recommended new-write target and warn on legacy choices. | Add docs/API warning todo. |
| Wrong-password exception type | **Valid.** Wrong or missing database passwords map to `UnauthorizedAccessException`, which callers can confuse with filesystem ACL failures. | Add exception taxonomy todo. |
| Linked-table hardening and disclosure path | **Valid split.** Linked source hardening is strong and well documented. A standard `SECURITY.md` is absent. | Add `SECURITY.md` todo. |
| LINQ cost model | **Partially valid.** README says filtering/projection run client-side and `SeekRowsAsync` is exact-match only, but XML docs/examples still make `Rows<T>().Where(...)` look query-like without saying "full table scan" at the API surface. | Add cost-model docs and optional index-aware API todos. |
| Transaction memory scaling | **Partially stale.** The critique is right that transactions use an in-memory page journal, but the repo now has `MaxTransactionPageBudget` with a default cap. README's transaction section should surface that cap. | Add README/API docs todo. |
| DDL whole-table rewrite | **Valid but documented.** `AddColumnAsync`, `DropColumnAsync`, and `RenameColumnAsync` use copy-and-swap, and README/interface docs say so. Multiple changes still cost multiple rewrites. | Add batched schema migration todo. |
| Single I/O gate vs parallel reads | **Mostly already documented.** README Limitations explicitly says a single instance is single-flight. The feature bullet "optional parallel page reads" can still be read too broadly. | Add small wording todo. |
| Cache size and AutoNumber seeding | **Low, mostly documented.** Cache default is documented as 256 pages. AutoNumber first use scans existing rows through `ConstraintRegistry.GetNextAutoValueAsync`; README and `ColumnDefinition` say `max(existing)+1`. | Optional tuning note only; not a primary todo. |
| Materializing APIs | **Already documented.** README and `IAccessReader` docs warn that `ReadDataTableAsync` / `ReadAllTablesAsync` materialize. | No new todo. |
| God-object concern | **Partially valid but lower priority.** `AccessReader` / `AccessWriter` remain large facades, but the codebase already has domain modules and public reader/writer/schema interfaces. | No immediate todo from this review. |
| `object[]` / `DBNull.Value` row model | **Valid API tradeoff.** The current streaming object-array surface is ADO.NET-shaped by design. POCO mapping uses compiled expression delegates and caches write mapping per `TableDef`, so the reflection-per-row concern is stale. | Optional future null-native row API todo. |
| No shared abstraction | **Stale.** `IAccessReader`, `IAccessWriter`, `IAccessSchema`, `IAccessBase`, and `IAccessOptions` exist. | No todo. |
| `AsStrings` surface area | **Valid but accepted compatibility surface.** There are several string-typed twins, including streaming `RowsAsStrings`. | Add "do not expand without design" todo only. |
| Mutation predicates | **Valid.** `UpdateRowsAsync` and `DeleteRowsAsync` accept equality by one column only. | Add predicate/range API todo. |
| Client-side vs persisted constraints | **Partially valid.** README and `ColumnDefinition` clearly label `DefaultValue` / `ValidationRule` as client-side and `DefaultValueExpression` / `ValidationRuleExpression` as persisted. The API still invites mixing them. | Add API clarity todo. |
| Exception model | **Valid.** `UnauthorizedAccessException` and broad `NotSupportedException` uses make caller branching harder. `JetLimitationException` is a good precedent. | Add exception taxonomy todo. |
| Boolean options | **Low priority.** Booleans are ordinary on options objects and are named at call sites; less concerning than positional boolean method parameters. | No todo. |
| `DBNull` instead of `null` | **Valid API tradeoff.** Current docs explicitly say object-array rows surface `DBNull.Value`; typed POCOs are the modern path. | Optional future API todo. |
| Release/versioning discipline | **Valid.** The project has package version metadata and a publish workflow, but there is no standalone release-history file; package release notes now stay inline instead of pointing to an absent one. No local GitHub release metadata can be verified from the repo. | Add release-process todo. |
| Test-fixture and reference licensing | **Already documented.** [THIRD-PARTY-NOTICES.md](../../THIRD-PARTY-NOTICES.md) documents verbatim Jackcess tables, a Jackcess port/adaptation, OpenMcdf fixtures, Sep influence, ESE reference-only use, and links to local Jackcess and mdbtools fixture notices. The mdbtools fixture notice records mdbtools format-spec reference-only use. | Completed provenance cleanup. |
| Upstream/fork attribution | **Partially valid.** NuGet metadata names Diego Ripera and points to the original repository, but README does not explain project lineage or retained upstream material. | Add README attribution todo. |
| `SECURITY.md` | **Valid.** `.github/SECURITY.md` is absent. | Add disclosure-process todo. |

## Action Items

### 1. Reframe public security and correctness claims

- [ ] Replace README security wording like "All 36 known relevant CVEs have been addressed" with a precise statement such as "36 relevant historical CVEs were surveyed; applicable vulnerability classes are covered by hardening and regression tests."
- [ ] Make the same change in [cve-vulnerability-analysis.md](../cve-vulnerability-analysis.md), especially the header and final inventory line.
- [ ] Avoid implying that historical Microsoft CVE coverage is a complete security guarantee for this independent implementation.
- [ ] Keep the CVE table valuable by framing each row as class coverage, code guard, and regression signal.
- [ ] Add a short README link to [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md) near the correctness claims so readers can see the actual DAO/OpenRecordset/CompactDatabase evidence.
- [ ] Reword the collation/index-key paragraph so "byte-exact" is visibly scoped to specific fixtures, exported matrices, and tested formats, not arbitrary text indexes.

### 2. Tighten crypto hygiene and password ergonomics

- [x] Replace legacy Jet4 / ACCDB header password `SequenceEqual` checks with a fixed-time comparison over a normalized representation.
- [x] Add regression tests that cover wrong-password paths for Jet4 RC4, ACCDB legacy password, and ACCDB AES CFB-wrapped header-password verification.
- [x] Add best-effort `CryptographicOperations.ZeroMemory` cleanup for derived keys, intermediate Agile keys, verifier/HMAC material, and temporary plaintext password encodings where lifetimes are locally owned.
- [ ] Document the residual managed-memory limitation: caller-provided strings and copied arrays can still remain on the GC heap.
- [x] Replace encryption mutation API `string` password parameters with `ReadOnlyMemory<char>` parameters.
- [ ] Make Agile encryption the documented recommendation for new encrypted ACCDB output.
- [ ] Warn in docs, and possibly via an opt-in diagnostic callback, when callers explicitly select legacy weak formats such as Jet4 RC4, ACCDB legacy password-only, or AES-128 ECB-compatible page encryption.
- [ ] Consider a dedicated exception type for missing/incorrect database passwords so callers can distinguish password failure from filesystem access denial.

### 3. Make query and mutation costs harder to miss

- [x] Update `IAccessReader.Rows`, `Rows<T>`, and `RowsAsStrings` XML docs to say that LINQ `Where` / `Select` operators are client-side and require a table scan unless enumeration short-circuits.
- [x] Keep README examples, but add one nearby sentence pointing index users to `SeekRowsAsync` for exact-key lookups.
- [x] Document that `SeekRowsAsync` is exact-match only, not a range scan, anywhere query-like examples are shown.
- [x] Design an index-aware range/equality helper if range scans become a supported API goal.
- [ ] Add richer mutation APIs for `UpdateRowsAsync` / `DeleteRowsAsync`, such as composite equality, range predicates, or an explicit scan predicate with clear cost semantics.
- [ ] Do not add more `AsStrings` twins without a short API review; prefer options, extension methods, or clearly compatibility-scoped helpers.

### 4. Clarify scaling limits for writes and schema changes

- [ ] Surface `AccessWriterOptions.MaxTransactionPageBudget` in the README transaction section, including the default 16,384-page cap and approximate memory cost by page size.
- [ ] Add a short example of choosing a smaller transaction page budget for bounded-memory tools.
- [ ] Ensure transaction-budget overflow tests stay tied to the public option and exception type.
- [ ] Design a batched schema migration API so multiple add/drop/rename operations can be paid for with one copy-and-swap rewrite.
- [ ] Keep the existing DDL copy-and-swap caveat in README and XML docs until a batched or in-place path exists.
- [ ] Reword the top-level performance/feature bullet for "optional parallel page reads" so it coexists clearly with the single-instance I/O gate limitation.

### 5. Document semantic boundaries of Access-like behavior

- [ ] Add a calculated-column grammar/divergence document or README subsection that lists supported operators, functions, type coercions, null behavior, locale assumptions, and unsupported Access expression features.
- [ ] Add tests that assert unsupported calculated expressions fail clearly instead of silently approximating Access behavior.
- [ ] Update the relationship section to state that referential integrity and cascades are enforced by the current `AccessWriter` operation, not by a persistent engine lock against external writers or Access edits.
- [ ] Consider a short "not a database engine" cross-reference from relationships and mutation APIs to the Limitations section.
- [ ] Add an optional future API note for a null-native row shape if the project wants an alternative to `object[]` plus `DBNull.Value` without breaking existing ADO.NET-style consumers.

### 6. Keep validation work tied to release-relevant changes

- [ ] If the README continues to use strong collation wording, add a DAO/Access oracle corpus for broader text-index inputs, including arbitrary Unicode, composite text keys, long rows, descending keys, and non-curated values.
- [ ] Promote any new FormatProbe finding into a focused regression test rather than leaving it as probe-only knowledge.
- [ ] Keep B-tree, free-list, remanence, and complex-column validation extensions in their existing focused docs unless a specific algorithm or byte layout changes.
- [ ] Reference [data-remanence-todos.md](data-remanence-todos.md) for secure erase, free-page, LVAL reclamation, and Compact & Repair style rebuild work instead of duplicating that backlog here.

### 7. Fix release, disclosure, and provenance paperwork

- [ ] Add a standard `.github/SECURITY.md` with supported versions, private reporting instructions, expected response cadence, and what information reporters should include.
- [x] Keep package release notes inline instead of pointing to an absent release-history file.
- [ ] Define the intended SemVer/API stability policy for the current `4.0.0` package line.
- [ ] Add a README lineage/attribution note explaining the relationship to the original `diegoripera/JetDatabaseWriter` / `JetDatabaseReader` work and which code/material remains derived.
- [x] Extend [THIRD-PARTY-NOTICES.md](../../THIRD-PARTY-NOTICES.md) to link to the local Jackcess and mdbtools fixture notices, with mdbtools format-spec reference-only usage documented in `JetDatabaseWriter.Tests/Databases/mdbtools/THIRD-PARTY-NOTICES.txt`.
- [ ] Re-audit checked-in test fixtures and generated index-code resources when adding new copied corpus material; record source, license, and whether the material is verbatim, adapted, or reference-only.

## Non-Todos From This Review

- Do not add a broad "fix B-tree depth" task. Multi-level split, recursive intermediate split, collapse, borrow/rebalance, and DAO validation signals already exist. Add only targeted tests when a specific index algorithm changes.
- Do not add a generic "create interfaces" task. `IAccessReader`, `IAccessWriter`, `IAccessSchema`, and `IAccessBase` already exist.
- Do not duplicate data-remanence/free-list work here. The active backlog is [data-remanence-todos.md](data-remanence-todos.md).
- Do not treat `ReadDataTableAsync` / `ReadAllTablesAsync` materialization as an undocumented defect. The README and XML docs already warn about it.
- Do not turn popularity, star count, or fork status into repository TODOs. They are not evidence for or against the correctness of the implementation.