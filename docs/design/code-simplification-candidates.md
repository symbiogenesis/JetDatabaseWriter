# Code simplification candidates

Status: todo candidates
Date: 2026-05-24
Last updated: 2026-05-25

This note captures candidate cleanup items identified during the linked-table
and read-path simplification review. The goal is to reduce code size and
duplication while retaining, or expanding, existing features and performance.

## High-confidence items

- [x] Extract a linked-text source context in `JetDatabaseWriter/Relationships/LinkedTableManager.cs`.
  - Completed 2026-05-25: `RowsLinkedTextAsStringsAsync`, `GetLinkedTextColumnMetadataAsync`, and `ReadLinkedTextDataTableAsync` now share one helper that resolves the source file, checks existence, parses the connect string, and reads column names.
  - Candidate shape: an internal `LinkedTextSource` record/struct plus a single async helper that returns the resolved file path, parsed `TextLinkFormat`, and column names.
  - Expected benefit: less duplication, simpler future support for text-link features such as encoding or `schema.ini`, and one security/file-existence path to audit.
  - Keep coverage: `LinkedTextTableTests` and linked-table catalog writer tests.

- [x] Count linked text rows without materializing normalized rows.
  - Completed 2026-05-25: `CountLinkedTextRowsAsync` now resolves the text source once, parses the format, and counts records directly via `EnumerateTextDataRowsAsync` without reading column names or normalizing rows.
  - Candidate shape: resolve the source once, parse `TextLinkFormat`, enumerate data records directly with `EnumerateTextDataRowsAsync`, and increment the count.
  - Expected benefit: same behavior with less allocation and less per-row work for `GetRealRowCountAsync` on linked text tables.
  - Keep coverage: linked CSV row-count tests with header and no-header formats.

- [x] Replace path containment prefix checks with a `Path.GetRelativePath`-based helper.
  - Completed 2026-05-25: `ResolveLinkedSourcePath` and `ResolveLinkedTextSourceFilePath` now share a relative-path containment helper that treats the root itself as allowed and rejects rooted or parent-directory escapes.
  - Candidate shape: use `Path.GetFullPath(path, baseDirectory)` where available and a shared containment helper that treats `.` as inside/equal and rejects rooted `..` escapes.
  - Expected benefit: clearer intent, better equality handling for the allowed root itself, and less hand-rolled path string work.
  - Risk to check: maintain `netstandard2.1` support and Windows path semantics around drive roots, UNC paths, and alternate separators.

- [x] Cache linked-table metadata like user-table catalog metadata.
  - Completed 2026-05-25: `FindLinkedTableAsync` and `ListLinkedTablesAsync` now use a cached linked-table catalog scan that is cleared by the existing catalog-cache invalidation path.
  - Candidate shape: add a linked-table cache or broader catalog snapshot that is invalidated with the existing catalog cache.
  - Expected benefit: fewer repeated catalog scans and simpler call sites in `AccessReader` fallback paths.
  - Risk to check: writer-side catalog mutations must invalidate the linked-table cache whenever user-table catalog cache is invalidated.

- [x] Centralize linked-table dispatch in `AccessReader`.
  - Completed 2026-05-25: row count, untyped rows, typed rows, string rows, metadata, `ReadDataTableAsync`, and `ReadTableAsStringsAsync` now delegate linked-table fallback through private dispatch helpers.
  - Candidate shape: after linked-table lookup is cached, introduce small private helpers for linked-table fallback dispatch instead of open-coded branches.
  - Expected benefit: fewer behavioral branches to keep synchronized when adding linked-table features.
  - Risk to check: keep async iterator disposal semantics clear for opened source readers.

## Lower-confidence or research items

- [ ] Investigate `Microsoft.VisualBasic.FileIO.TextFieldParser` for linked text parsing.
  - Possible upside: replace the custom delimited reader and potentially expand support toward fixed-width text files.
  - Current blocker: `TextFieldParser` appears available in the .NETCore ref pack, but not in the `netstandard2.1` reference set used by the library.
  - Risks: added package/reference surface, synchronous file IO under async APIs, cancellation behavior, and subtle behavior changes for quoted CRLF, escaped quotes, custom delimiters, and unsupported formats.
  - Recommendation: do not replace the current parser unless fixed-width or broader Access text-driver compatibility becomes a priority.

- [x] Consider conditional modern fast paths for binary/base64 helpers.
  - Completed 2026-05-25: base64 already used span-based `Convert.TryFromBase64Chars`; plain-hex parsing now lives in `BinaryStringParser` and uses `Convert.FromHexString` on modern targets while keeping a `netstandard2.1` nibble-loop fallback and the dash-separated parser for `BitConverter.ToString` formats.
  - Recommendation: keep the custom dash-separated logic unless benchmarks or analyzer findings point at a better replacement.

## Suggested order

1. Extract linked-text source context. (DONE)
2. Add direct linked-text row counting. (DONE)
3. Simplify path containment with focused path-policy tests. (DONE)
4. Add linked-table metadata caching. (DONE)
5. Centralize `AccessReader` linked-table dispatch. (DONE)
6. Revisit `TextFieldParser` only if a feature or benchmark justifies the tradeoff.
