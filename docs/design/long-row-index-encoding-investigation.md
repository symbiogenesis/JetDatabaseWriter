# Microsoft Access "long row" indexed-text key encoding — investigation

This document captures a reverse-engineering investigation into how Microsoft
Access (Jet 4 `.mdb` and ACE `.accdb`) physically encodes single-column
text-index leaf entries when the indexed value exceeds the canonical
127-character indexed-prefix cap.

It is intended as a hand-off for any project that maintains its own
implementation of the JET text-index sort key — Jackcess, mdbtools,
JetDatabaseWriter, or others. The findings are language-agnostic; all hex
in this document is taken straight from Access-authored fixtures.

## Background

The widely-mirrored upstream comment is:

> `TODO long rows not handled completely yet in V2010 — seems to truncate
> entry at 508 bytes` *(Jackcess `IndexCodesTest.findRow`)*

Most independent Access-format implementations replicate this behaviour by:

1. Slicing the input value to `MAX_TEXT_INDEX_CHAR_LENGTH = 127` characters
   (`TEXT_FIELD_MAX_LENGTH (255 bytes) / TEXT_FIELD_UNIT_SIZE (2 B/char)`).
2. Running the General-Legacy (or General, in 2010+) state machine over those
   127 chars to produce the per-column entry block.

This is observably wrong: Access itself emits significantly longer entries
when the source value exceeds 127 chars, and re-importing those entries
through the 127-char-truncating encoder produces stale leaves that
Compact-and-Repair will reject.

## Test corpus

All findings below were obtained by walking the leaf B-tree of the
`Table11` / `Table11_desc` indexes in the upstream Jackcess
`testIndexCodesV2000.mdb` / `…V2003.mdb` / `…V2007.accdb` /
`…V2010.accdb` fixtures and dumping the leaf entry bytes verbatim.

Each fixture's `Table11` has 13 rows; rows 2–4 carry ~900-character text
values that exercise the long-row encoding. Rows 0–1 and 5–12 are short
and are decoded correctly by every implementation in scope.

## Observed leaf-entry layout (V2000 / V2003 / V2007 — Jet 4, "General Legacy")

Single-column ascending leaf entry for a long value:

```
┌────────┬──────────────────┬─────────────┬──────────────────┬────────┬──────────────────────┬───────┐
│ flag   │ inline chunk #1  │ separator   │ inline chunk #2  │ END_TX │ extras + unprintable │ END_X │
│ (1 B)  │ (variable)       │ 08 07 08 04 │ (variable)       │ (0x01) │ codes (variable)     │ (0x00)│
└────────┴──────────────────┴─────────────┴──────────────────┴────────┴──────────────────────┴───────┘
```

The descending form one's-complements the entire payload (everything except
the leading `0x80` flag) and appends a fresh unflipped `0x00`, exactly as
in the short-row case.

### Empirical sample — `Table11.DataIndex` (ascending), V2000

Source value (901 chars; first few chars shown):

> `-a;sldjfl;akÁsj dfl;kasj ldfkaslhdfkjhasjk dhfkljas djfhaskljdhfkjshadfj hkj skj…`

Five identical 180-char lines separated by `\n`, ending with `AAAA-A`.

On-disk leaf entry (278 bytes):

```
7F                                                      ← flag
4A 22 6B 5E 4F 5B 53 5E 22 4A 5C 4A 6B 5B 07 4F 53 5E 22 5C 4A 6B 5B 07 5E
4F 53 5C 4A 6B 5E 57 4F 53 5C 5B 57 4A 6B 5B 5C 07 4F 57 53 5C 5E 5B 4A 6B
07 4F 5B 53 57 4A 6B 5C 5E 5B 4F 57 53 5C 5B 6B 57 4A 4F 53 5B 07 57 5C 5B
07 6B 5C 5B 07 4F 57 53 5C 5B 4A 6B 57 07 4F 53 57 07 5C 4A 6B 5B 07 4F 53
5B 5C 57 4A 6B 5C 5B 4F 57 53 5C 5B 4A 6B 57 07 5C 4F 07 53 5C 5B 4A 6B 4F
57 53 07 5C 5B 4A 07 6B 57 5C 07 4F 53 5C 5B 4A 6B 53 4F 57 5C 5B 4A 07 6B
4F 57 53 5C 5B 07 4A 6B 4F 57 53 5C 5B 6B 4A 57 5C 4F 07 53 5C 4A 5B 6B 07
4F 57 53                                                ← chunk #1 (178 bytes)
08 07 08 04                                             ← separator
4A 22 6B 5E 4F 5B 53 5E 22 4A 5C 6B 5B 07 4F 53 5E 22 5C 4A 6B 5B 07 5E 4F
53 5C 4A 6B 5E 57 4F 53 5C 5B 57 4A 6B 5B 5C 07 4F 57 53 5C 5E 5B 4A 6B 07
4F 5B 53 57 4A 6B 5C 5E 5B 4F 57 53 5C 5B 6B 57 4A 4F 53 5B 07 57 5C 5B
                                                        ← chunk #2 (74 bytes)
01                                                      ← END_TEXT
02 02 02 02 02 02 02 02 02 02 02                        ← extras (Á)
0E 01 01 01 80 07 06 82                                 ← unprintables
00                                                      ← END_EXTRA_TEXT
```

### Verified properties

The chunk bytes were confirmed by re-running the standard General-Legacy
state machine with a small modification (collect inline bytes only, suppress
END_TEXT and the trailing extras/unprintable streams). Specifically, for the
sample row above:

| chunk | source range | encoder output                                  |
|------:|--------------|-------------------------------------------------|
|  #1   | `val[0..179]` | byte-exact match against on-disk chunk #1 (178 B) |
|  #2   | `val[181..255]` | byte-exact match against on-disk chunk #2 (74 B) |

The 2 source characters between the chunks (`val[179]` = last char of line 1,
and `val[180]` = `\n`) are **not represented anywhere in the encoded entry**;
their presence is signalled solely by the 4-byte separator. This was
reproduced across all three rows (`row[2]`, `row[3]`, `row[4]`) and across
all three Jet 4 format variants (V2000, V2003, V2007), with byte-identical
results.

The trailing extras + unprintables block aggregates the auxiliary streams for
**both chunks** — i.e. an `Á` at position 12 of the source produces a single
international-extra placeholder in the trailing block, not one placeholder
per chunk.

## Format-version differences

| format | chunk count observed | max leaf entry length | notes |
|--------|---------------------:|----------------------:|-------|
| V1997 (Jet 3, "General 97") | not applicable here — this fixture's V1997 build does not contain Table11 in the published Jackcess corpus | n/a | Different state machine (no END_TEXT framing, nibble-packed extras). Long-row behaviour not investigated. |
| V2000 (Jet 4) | 2 chunks | 291 bytes | "General Legacy" sort order |
| V2003 (Jet 4) | 2 chunks | 291 bytes | Same as V2000 |
| V2007 (ACE)   | 2 chunks | 291 bytes | Same as V2000 |
| V2010 (ACE, "General") | unknown — different code tables, encoder not available for direct comparison | **510 bytes** (consistent across all three long rows in the corpus) | The 510-byte hard cap is the source of the upstream "508 bytes" comment (which appears to count from a slightly different baseline). |

The `08 07 08 04` separator is observed in V2000 / V2003 / V2007.
V2010 leaves should be inspected to confirm whether the same separator is
used or whether the General code tables substitute a different sentinel.

## Open questions / proposed implementation roadmap

The following items remain under-specified by this investigation. Closing
them is the work needed to produce a byte-exact long-row encoder.

1. **Chunk-boundary rule.** In our corpus chunk #1 always ends at exactly
   the character one position before the first `\n`. Whether this rule
   generalises to:
   - other "line break" characters (`\r`, `\r\n`, `\f`, paragraph
     separator),
   - non-line-break breaks (e.g. byte-budget driven splits when no `\n`
     is present in the source), or
   - any input whose first 127 characters do not contain a `\n`,

   is not yet determined. The fixtures in scope all have a `\n` near
   position 180; pure-prose long values without newlines should be probed
   next.

2. **Why the character at `val[179]` is dropped.** The chunk #1 source
   range ends at 179 chars (i.e. covers indices `0..178`), then chunk #2
   resumes at index 181. Index 180 is the `\n`. Index 179 (the last
   character of line 1) appears nowhere in the encoded entry. Possible
   explanations:
   - The General-Legacy state machine has a one-character look-ahead and
     the `\n` causes the in-flight character to be flushed without inline
     output.
   - A stray bug in Access that we should reproduce verbatim.

3. **Maximum chunk count.** All observed long-row entries have exactly
   2 chunks. We do not yet have a fixture with multiple `\n`-separated
   sections short enough to fit additional chunks under the per-format
   byte budget. A synthetic fixture with many short newline-terminated
   lines would establish whether 3+ chunks are produced.

4. **Per-format byte budget.** V2000–V2007 truncate at ≤ 291 bytes,
   V2010 truncates at exactly 510 bytes. The exact formulas and whether
   the budget is page-size dependent or sort-order dependent is not
   determined.

5. **V2010 / "General" sort order long-row format.** The General code
   tables produce different inline bytes than General-Legacy; the
   chunk-and-separator structure is *plausibly* identical but unverified.
   The byte-cap is observably different (510 vs ~291).

## Suggested implementation strategy

Once items 1–4 are nailed down for General Legacy (2-chunk path):

1. Generalise the existing per-codepoint state machine to expose an
   "inline-only" emission mode (suppress END_TEXT + auxiliary streams).
2. Add a top-level driver:
   - If `len(input) ≤ 127`, fall through to the existing single-chunk path.
   - Otherwise, split the input at the first chunk-break point (currently
     hypothesised: position of first `\n`, minus 1).
   - Run the inline-only encoder over chunk #1 (`input[0..K1]`).
   - Emit the 4-byte separator `08 07 08 04`.
   - Run the inline-only encoder over chunk #2 (`input[K2..K3]`), where
     `K2` is the post-break resume point (currently hypothesised: position
     of first `\n` + 1) and `K3` is the maximum source position whose
     encoded output stays within the per-format byte budget.
   - Run a single unified extras + unprintable codes pass over the union
     of chars in chunks #1 and #2, with chunk-relative offsets translated
     into a single global character-offset stream.
   - Append `END_TEXT (0x01)`, the extras/unprintable streams, and
     `END_EXTRA_TEXT (0x00)`.
3. Apply the standard descending pass (one's-complement everything after
   the flag, append fresh `0x00`).
4. For V2010 / General: same algorithm, but verify the byte cap (510)
   and confirm the `08 07 08 04` separator survives the General code
   tables.

Reading is symmetric — a long-row-aware decoder must:

- Detect the in-band `08 07 08 04` separator while walking the inline
  payload (it cannot otherwise occur — none of the per-codepoint inline
  outputs in either General or General-Legacy emit `0x08` followed by
  `0x07` followed by `0x08` followed by `0x04`).
- Treat each chunk as inline-only payload (no embedded END_TEXT).
- Treat the trailing block (after the final inline chunk's
  END_TEXT) as a single combined extras + unprintables section whose
  character offsets address into the *concatenated* chunk stream.

## Reproduction recipe (for any language)

1. Open one of the four `testIndexCodesV20{00,03,07,10}` fixtures.
2. Locate the `Table11` index whose first column is the Memo/Text column
   `data` (ascending). Record `firstDp` from the index descriptor.
3. Walk the leaf B-tree starting at `firstDp` (descend the leftmost
   intermediate child until you hit `page_type == 0x04`, then follow
   `next_page` pointers).
4. Extract every entry's key bytes (everything before the trailing
   `(page_no:3, row_no:1)` row pointer).
5. Search the bytes for the substring `08 07 08 04`. In V2000 / V2003 /
   V2007 you will find exactly one occurrence in any entry whose source
   value exceeds 127 chars.
6. Split the entry on that occurrence; you have chunk #1 and the
   remainder (chunk #2 + END_TEXT + auxiliary streams + END_EXTRA_TEXT).

The same recipe applied to V2010 should reveal whether the General
sort order uses the same or a substituted separator.

## Status in this repository

JetDatabaseWriter currently inherits the upstream "truncate at 127 chars"
behaviour. The Table11 / Table11_desc fixtures are explicitly skipped in
the General-Legacy / General fixture sweeps and the binary single-column
sweep. See:

- `JetDatabaseWriter.Tests/Internal/GeneralLegacyEncoderFixtureTests.cs`
- `JetDatabaseWriter.Tests/Internal/GeneralEncoderFixtureTests.cs`
- `JetDatabaseWriter.Tests/Internal/NonTextSingleColumnIndexFixtureTests.cs`
- `JetDatabaseWriter.Tests/Internal/IndexCodesAggregateDiagnosticTests.cs`

The skips can be removed once items 1–5 of the roadmap above are closed.
