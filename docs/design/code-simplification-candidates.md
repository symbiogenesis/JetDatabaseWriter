# Code simplification candidates

Status: todo candidates
Date: 2026-05-24
Last updated: 2026-05-24

This note captures candidate cleanup items identified during the linked-table
and read-path simplification review. The goal is to reduce code size and
duplication while retaining, or expanding, existing features and performance.

## High-confidence items

- [ ] Extract a linked-text source context in `JetDatabaseWriter/Relationships/LinkedTableManager.cs`.
  - Current repetition: `RowsLinkedTextAsStringsAsync`, `GetLinkedTextColumnMetadataAsync`, and `ReadLinkedTextDataTableAsync` each resolve the source file, check existence, parse the connect string, and read column names.
  - Candidate shape: an internal `LinkedTextSource` record/struct plus a single async helper that returns the resolved file path, parsed `TextLinkFormat`, and column names.
  - Expected benefit: less duplication, simpler future support for text-link features such as encoding or `schema.ini`, and one security/file-existence path to audit.
  - Keep coverage: `LinkedTextTableTests` and linked-table catalog writer tests.

- [ ] Count linked text rows without materializing normalized rows.
  - Current path: `CountLinkedTextRowsAsync` calls `RowsLinkedTextAsStringsAsync`, which reads column names and normalizes each record only to increment a counter.
  - Candidate shape: resolve the source once, parse `TextLinkFormat`, enumerate data records directly with `EnumerateTextDataRowsAsync`, and increment the count.
  - Expected benefit: same behavior with less allocation and less per-row work for `GetRealRowCountAsync` on linked text tables.
  - Keep coverage: linked CSV row-count tests with header and no-header formats.

- [ ] Replace path containment prefix checks with a `Path.GetRelativePath`-based helper.
  - Current path: `ResolveLinkedSourcePath`, `ResolveLinkedTextSourceFilePath`, `IsPathWithinDirectory`, and `EnsureTrailingDirectorySeparator` use full-path normalization plus `StartsWith`.
  - Candidate shape: use `Path.GetFullPath(path, baseDirectory)` where available and a shared containment helper that treats `.` as inside/equal and rejects rooted `..` escapes.
  - Expected benefit: clearer intent, better equality handling for the allowed root itself, and less hand-rolled path string work.
  - Risk to check: maintain `netstandard2.1` support and Windows path semantics around drive roots, UNC paths, and alternate separators.

- [ ] Cache linked-table metadata like user-table catalog metadata.
  - Current path: `FindLinkedTableAsync` calls `GetLinkedTablesAsync`, which scans `MSysObjects` each time a missing local table might be a linked table.
  - Candidate shape: add a linked-table cache or broader catalog snapshot that is invalidated with the existing catalog cache.
  - Expected benefit: fewer repeated catalog scans and simpler call sites in `AccessReader` fallback paths.
  - Risk to check: writer-side catalog mutations must invalidate the linked-table cache whenever user-table catalog cache is invalidated.

- [ ] Centralize linked-table dispatch in `AccessReader`.
  - Current repetition: row count, untyped rows, typed rows, string rows, metadata, `ReadDataTableAsync`, and `ReadTableAsStringsAsync` each perform the same local-table-missing, find-link, text-vs-Access branch.
  - Candidate shape: after linked-table lookup is cached, introduce small private helpers for linked-table fallback dispatch instead of open-coded branches.
  - Expected benefit: fewer behavioral branches to keep synchronized when adding linked-table features.
  - Risk to check: keep async iterator disposal semantics clear for opened source readers.

## Lower-confidence or research items

- [ ] Investigate `Microsoft.VisualBasic.FileIO.TextFieldParser` for linked text parsing.
  - Possible upside: replace the custom delimited reader and potentially expand support toward fixed-width text files.
  - Current blocker: `TextFieldParser` appears available in the .NETCore ref pack, but not in the `netstandard2.1` reference set used by the library.
  - Risks: added package/reference surface, synchronous file IO under async APIs, cancellation behavior, and subtle behavior changes for quoted CRLF, escaped quotes, custom delimiters, and unsupported formats.
  - Recommendation: do not replace the current parser unless fixed-width or broader Access text-driver compatibility becomes a priority.

- [ ] Consider conditional modern fast paths for binary/base64 helpers.
  - Current path: `JetDatabaseWriter/Infrastructure/BinaryStringParser.cs` supports span-based base64 and dash-separated hex parsing across target frameworks.
  - Possible upside: newer target frameworks expose more `Convert` helpers that could shorten some code paths.
  - Current blocker: `netstandard2.1` still needs custom logic for allocation control and dash-separated `BitConverter.ToString` formats.
  - Recommendation: leave the existing implementation alone unless benchmarks or analyzer findings point at it.

## Suggested order

1. Extract linked-text source context.
2. Add direct linked-text row counting.
3. Simplify path containment with focused path-policy tests.
4. Add linked-table metadata caching.
5. Centralize `AccessReader` linked-table dispatch.
6. Revisit `TextFieldParser` or binary helper fast paths only if a feature or benchmark justifies the tradeoff.
