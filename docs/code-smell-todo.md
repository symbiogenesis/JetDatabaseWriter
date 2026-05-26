# Code Smell TODO

This backlog captures the maintainability hotspots from the May 25, 2026 code-smell pass. It is intentionally scoped to production library code, not tests, probes, or benchmarks.

## Priority 0: Stop Silent Data Loss In Value Parsing

Hotspot: [TypedValueParser.cs](../JetDatabaseWriter/ValueDecoding/TypedValueParser.cs#L13-L84)

- [x] Split parsing from coercion policy. A method named `ParseValue` should not also decide whether failed conversion becomes `DBNull.Value`, an empty byte array, or an exception.
- [x] Replace the broad `catch (Exception)` in `ParseValue` with type-specific parsing paths or specific exception handling for expected parse failures.
- [x] Rework `ParseByteArray` so invalid binary text does not silently become `[]`. Decide one explicit non-strict outcome, such as `DBNull.Value`, while preserving strict-mode exceptions.
- [x] Make OLE/memo diagnostic-string handling explicit instead of hiding it behind the same invalid-hex fallback used for bad user data.
- [x] Add focused tests for empty strings, invalid numeric/date values, invalid hex, invalid base64 data URIs, valid dash-separated hex, and OLE/memo diagnostic strings.

Done criteria:

- Strict mode surfaces actionable conversion errors.
- Non-strict mode has one documented coercion policy per target type.
- Invalid binary input cannot be mistaken for a legitimate zero-length payload.

## Priority 1: Finish Breaking Up The Writer Hub

Hotspot: [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs#L40-L132), especially the forwarder sections around [relationship APIs](../JetDatabaseWriter/AccessWriter.cs#L1739-L1750) and [index/page maintenance](../JetDatabaseWriter/AccessWriter.cs#L4119-L4145)

- [x] Inventory all thin forwarders and categorize them as public API boundary, internal compatibility shim, or removable call-site debt.
- [x] Move internal call sites to the owning subsystem where that does not leak too much writer state.
- [x] Replace broad friend-style access to the writer with narrow collaborator APIs where practical.
- [x] Remove or reduce file-level suppressions once the compatibility wrappers shrink.
- [x] Keep public API behavior unchanged while internal ownership moves.

Done criteria:

- [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs) remains the public orchestration surface, but fewer subsystems need to call back through it for unrelated operations.
- New subsystem methods expose narrow operations rather than the whole writer instance whenever possible.

## Priority 1: Split Reader Responsibilities And Fallback Policy

Hotspot: [AccessReader.cs](../JetDatabaseWriter/AccessReader.cs), including typed variable decoding around [ReadVarTypedSync](../JetDatabaseWriter/AccessReader.cs#L4306-L4345) and best-effort complex-column metadata fallback around [ReadComplexColumnSubtypesAsync](../JetDatabaseWriter/AccessReader.cs#L4574-L4581)

- [x] Extract complex column read APIs and metadata joins into a dedicated reader-side component.
- [x] Extract index seek row materialization from general table scanning.
- [x] Centralize typed-row decoding fallback rules so `DBNull.Value`, `[]`, skipped rows, and traced best-effort failures are applied consistently.
- [x] Audit strict parsing behavior across row decoding, complex column metadata, OLE extraction, and hyperlink wrapping.
- [x] Add tests that pin strict and non-strict behavior for malformed variable-area payloads and complex-column metadata corruption.

Done criteria:

- The main reader class no longer owns every stage of open, decrypt, cache, table discovery, row decode, seek, complex column handling, and diagnostics.
- Lossy fallback decisions are easy to find and deliberately tested.

## Priority 1: Decompose Relationship Management

Hotspot: [RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs) lifecycle/TDEF mutation and [RelationshipEnforcer.cs](../JetDatabaseWriter/Relationships/RelationshipEnforcer.cs) runtime cascade enforcement.

- [x] Split relationship catalog row operations from TDEF logical-index mutation.
- [x] Split runtime referential-integrity enforcement from schema creation/drop/rename workflows.
- [x] Isolate child-side index seek planning from snapshot fallback scanning.
- [x] Make the cascade-depth and cycle-handling policy easy to test without full catalog mutation setup.
- [x] Add regression tests for seek success, seek rejection with fallback, cascade delete, cascade update, self-reference, and malformed catalog rows.

Done criteria:

- Relationship lifecycle, TDEF mutation, and runtime enforcement can be understood and tested independently.
- Fast path and fallback path share key-building semantics through one small API.

## Priority 2: Refactor Calculated Expression Evaluation

Hotspot: [CalculatedExpressionEvaluator.cs](../JetDatabaseWriter/Schema/Expressions/CalculatedExpressionEvaluator.cs), [CalculatedExpressionFunctionRegistry.cs](../JetDatabaseWriter/Schema/Expressions/CalculatedExpressionFunctionRegistry.cs), and the domain-specific `CalculatedExpression*Functions.cs` helpers.

- [x] Split parser normalization, AST nodes, coercion helpers, and function implementations into separate files or nested components with clear ownership.
- [x] Replace the giant function switch with a small registry of function descriptors that carry name aliases, argument count, and evaluator delegate.
- [x] Group functions by domain: logical, text, date/time, numeric, formatting, financial, and metadata.
- [x] Keep Access-specific coercion semantics centralized so function implementations do not each invent null/date/number behavior.
- [x] Expand golden tests for Access/VBA-compatible edge cases before changing dispatch mechanics.

Done criteria:

- Adding a supported calculated-column function does not require editing a thousand-line switch.
- Unsupported spreadsheet-only operations remain rejected with clear errors.

## Priority 2: Track Size And Suppression Debt

Current largest production files after the extraction pass:

- [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs): about 4,330 lines.
- [AccessReader.cs](../JetDatabaseWriter/AccessReader.cs): about 4,082 lines.
- [IndexMaintainer.cs](../JetDatabaseWriter/Indexes/IndexMaintainer.cs): about 3,389 lines.
- [ComplexColumnManager.cs](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs): about 1,824 lines.
- [RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs): about 1,602 lines.
- [AccessBase.cs](../JetDatabaseWriter/AccessBase.cs): about 1,430 lines.
- [OfficeCryptoAgile.cs](../JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs): about 1,112 lines.
- [LinkedTableManager.cs](../JetDatabaseWriter/Relationships/LinkedTableManager.cs): about 1,042 lines.
- [EncryptionManager.cs](../JetDatabaseWriter/Encryption/EncryptionManager.cs): about 1,001 lines.

Current broad production suppressions to revisit first:

- [RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs): `SA1202`, `SA1204`, `SA1648`.
- [IndexMaintainer.cs](../JetDatabaseWriter/Indexes/IndexMaintainer.cs): `SA1202`, `SA1204`.
- [ComplexColumnManager.cs](../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs): `SA1202`, `SA1204`.
- [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs): `SA1202`, `SA1204`.
- [JetDatabaseWriter.csproj](../JetDatabaseWriter/JetDatabaseWriter.csproj): project-wide `NoWarn` list.

- [ ] Decide whether this project wants a soft file-size review threshold for production code.
- [ ] Track broad file-level suppressions such as `CA1822`, `SA1202`, `SA1204`, `SA1648`, and `CA1031` as cleanup markers rather than permanent background noise.
- [ ] Prefer small extraction PRs that preserve behavior and move tests with the code.
- [ ] Avoid pure churn: only extract when the new boundary has a real owner, stable vocabulary, or reusable policy.

## Suggested Order Of Attack

- [x] Fix [TypedValueParser.cs](../JetDatabaseWriter/ValueDecoding/TypedValueParser.cs#L13-L84) first because it has the highest risk of hiding bad data behind valid-looking results.
- [x] Then reduce [AccessWriter.cs](../JetDatabaseWriter/AccessWriter.cs) internal forwarder debt, because it affects subsystem boundaries across the library.
- [x] Then carve reader-side complex column and typed-row fallback policy out of [AccessReader.cs](../JetDatabaseWriter/AccessReader.cs).
- [x] Then split [RelationshipManager.cs](../JetDatabaseWriter/Relationships/RelationshipManager.cs) along lifecycle, TDEF mutation, and runtime enforcement boundaries.
- [x] Refactor [CalculatedExpressionEvaluator.cs](../JetDatabaseWriter/Schema/Expressions/CalculatedExpressionEvaluator.cs) after adding enough golden tests to protect Access-compatible semantics.
