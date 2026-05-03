# Microsoft Access "long row" indexed-text key encoding — resolution

This is a follow-on to
[long-row-index-encoding-investigation.md](long-row-index-encoding-investigation.md).
The investigation reverse-engineered the on-disk layout of two-chunk index
leaf entries in the Access-authored `Table11` / `Table11_desc` fixtures
(V2000 / V2003 / V2007 / V2010); this document describes the algorithm we
arrived at, the open questions that remain, and a step-by-step
implementation strategy any conforming implementation can use.

The strategy is intentionally language-agnostic. The notation `val[a..b)`
denotes a half-open source-character range; "byte-exact" means the resulting
on-disk leaf entry matches what Microsoft Access itself writes when given
the same source value.

The reference fixtures referenced throughout are the Jackcess
`testIndexCodesV{2000,2003,2007,2010}` corpus, distributed under Apache 2.0.

## TL;DR

When the indexed value exceeds the canonical 127-char single-chunk cap
**and** contains an embedded line break (CR or LF) within the input:

1. Find the first CR or LF position; call it `splitAt`.
2. Compute `resumeAt`:
   - If the line break is `\r\n` (CRLF), `resumeAt = splitAt + 2`.
   - If it is a single `\r` or `\n`, `resumeAt = splitAt + 1`.
3. Encode `chunk1 = val[0..splitAt)` using the standard per-codepoint
   state machine, but emit **inline bytes only** (no END_TEXT, no
   extras / unprintable / crazy streams, no terminator).
4. Emit a 4-byte chunk separator:
   - General Legacy (Jet 4 / V2000 – V2007): `08 07 08 04`.
   - General (V2010+): `07 09 07 06`.
5. Encode `chunk2 = val[resumeAt..min(255, len(val)))` (right-trim spaces)
   in the same inline-only mode, using char offsets that **continue** from
   chunk #1's offset stream (i.e. extras emitted for chunk #2 use char
   offsets `chunk1_chars + index_within_chunk2`).
6. Append the standard tail: `END_TEXT (0x01)`, the unified extras
   stream, optional `END_TEXT END_TEXT` then unprintable / crazy block,
   and `END_EXTRA_TEXT (0x00)`. The descending pass — one's-complement
   everything after the leading flag, then append a fresh unflipped
   `0x00` — is unchanged.

Inputs ≤ 127 chars (or > 127 with no CR/LF) keep the existing
single-chunk path.

## Why "split on CR/LF and drop both"?

Inspecting val[178..182] in every long-row fixture shows the same shape
across all four format versions:

| index | char     | role                              |
|------:|----------|-----------------------------------|
|  178  | `f`      | last char of chunk #1 inline      |
|  179  | `\r`     | dropped (chunk-boundary marker)   |
|  180  | `\n`     | dropped (CRLF second byte)        |
|  181  | `a`      | first char of chunk #2 inline     |

The chunk-1 inline contains exactly `val[0..179)`, the separator bytes
follow, and chunk-2 inline starts at `val[181]`. Both the CR and the LF
are absent from the encoded output (no inline bytes, no extras, no
unprintable codes). That's consistent across all three long rows in each
of the four fixture versions (12 leaves total).

Since CR (U+000D) and LF (U+000A) emit no inline bytes anyway under the
standard per-codepoint table (they're either ignored or extras-only),
"drop both" is the simplest rule that matches the observation. We have
not verified the lone-CR or lone-LF case (no Access-authored fixture in
the public corpus), but the conservative implementation above handles
either by skipping just the line-break char.

## Why `val[resumeAt..min(255, len(val)))` for chunk #2?

In V2000 / V2003 / V2007 the chunk #2 source range is **exactly**
`val[181..255)` for every long row, regardless of which chars chunk #2
contains. That's `255 − resumeAt = 255 − 181 = 74` chars. The
constant `255` matches the Jet text-index `TEXT_FIELD_MAX_LENGTH`
(255 bytes = 127.5 chars under 2-byte-per-char accounting), suggesting
Access measures the chunk-2 cap as a **source-character upper bound**
rather than as a chunk-relative length. Implementations that use the
length-based interpretation (`min(127, len(val) − resumeAt)`) will
produce a longer chunk #2 than Access does — sortable and lossless,
but not byte-exact.

The `TrimEnd(' ')` mirrors the trailing-space trim the single-chunk path
applies to its input; this is required by Jackcess's
`toIndexCharSequence` and observed in Access's writer.

## Unified extras stream

The trailing extras (and unprintable / crazy) block must address into the
**concatenated** chunk character stream, not into the original source.
That is, when chunk #1 contributes `n1` characters and chunk #2's char `i`
needs an extras placeholder, the placeholder's char offset is `n1 + i`,
not `resumeAt + i`. The 4-byte separator bytes do not advance the char
offset.

Concretely: maintain a single `charOffset` counter that runs over the
inline emission. Reset it only at the start of an entry, not between
chunks. When a chunk's inline byte is emitted, increment `charOffset`.
When a chunk's character produces an extras / unprintable / crazy
contribution, address it at the **current** `charOffset` value.

Implementations that already factor the per-codepoint state-machine into a
helper (Jackcess's `writeNonNullIndexTextValue`, mdbtools'
`mdb_iconv_text_to_g11n`, ours' `EncodeWithTables`) can usually plumb the
shared state through two consecutive invocations rather than refactoring
the inner loop.

## Descending pass

Unchanged from the single-chunk path:

1. Write `END_EXTRA_TEXT (0x00)` to the buffer.
2. One's-complement every byte after the leading flag (so the chunk
   separator `08 07 08 04` becomes `F7 F8 F7 FB` for general legacy and
   `07 09 07 06` becomes `F8 F6 F8 F9` for general; the `0x00` written
   in step 1 becomes `0xFF`).
3. Append a fresh unflipped `0x00` as the entry terminator.

The complemented separator bytes can be used as a sanity check while
diffing descending leaves against an Access-authored fixture.

## V2010 truncation cap (open)

For V2010 (General sort order, 4 KiB pages) every observed `Table11` /
`Table11_desc` long-row leaf entry is **exactly 510 bytes**. This matches
the upstream Jackcess `IndexCodesTest.findRow` comment "TODO long rows
not handled completely yet in V2010 — seems to truncate entry at 508
bytes" (the 508 vs 510 difference appears to come from counting the
3-byte data-page + 1-byte data-row trailer that follows the key in some
counters but not in others).

Whichever way the cap is counted, V2010 entries are aggressively
truncated: the END_TEXT, extras, and END_EXTRA_TEXT bytes are all dropped
from the on-disk image when the budget would otherwise be exceeded. For
ascending entries the last few bytes are an in-progress inline pair plus
~2 trailing bytes whose origin we have not pinned down. For descending
entries the truncation evidently happens after the descending pass
writes its terminator, because the last byte is always the unflipped
`0x00` sentinel.

The implementation we ship today emits the **un-truncated** form for
V2010: it's strictly more informative (preserves chunk 2 + extras +
terminator), it sorts correctly relative to other long entries, and it
round-trips through our own reader. It does **not** match Access
byte-for-byte in the truncated cases, and we have not validated whether
Microsoft Access's Compact-and-Repair will accept the longer entries.
Closing that gap requires:

1. Finding the exact cap formula (likely 510 bytes for 4 KiB ACE pages,
   page-size derived in general).
2. Determining whether the truncation point is byte-aligned mid-pair or
   pair-aligned. (The trailing `0E XX YY` byte triple at the end of two
   of the three V2010 ascending fixtures suggests mid-pair.)
3. Confirming whether Access tolerates the un-truncated form when the
   page has space for it.

Until those are answered, the safe choice is to emit the un-truncated
form and document the deviation.

## Branches still missing an Access-authored fixture

1. **Long input with no CR/LF.** All public fixtures with > 127 chars
   contain CRLF separators. Pure-prose long values (e.g. a 1 KiB Memo
   with no embedded line breaks) are unmodelled. The conservative
   implementation falls back to the 127-char single-chunk truncation
   for this case, matching pre-existing behaviour.

2. **Three-or-more chunk inputs.** The fixtures top out at one CRLF
   in the first 255 chars (so two chunks). Inputs with multiple CRLFs
   in that prefix could trigger 3+ chunks; we have no observation of
   that case.

3. **V2010 truncation rule.** See the section above.

4. **V1997 / "General 97" long-row format.** Jet 3 uses a different
   state machine (no END_TEXT framing; nibble-packed extras). Whether
   the same chunk-and-separator approach applies is unknown — the
   `testIndexCodesV1997` fixture as published does not contain a
   `Table11` analogue.

## Reproduction recipe (any language)

1. Open one of the four `testIndexCodesV20{00,03,07,10}` fixtures.
2. Locate the `Table11.DataIndex` index (single-column ascending Memo
   index on column `data`); record `firstDp`.
3. Walk the leaf B-tree starting at `firstDp`: descend the leftmost
   intermediate child until you hit `page_type == 0x04`, then follow
   `next_page` pointers and collect every leaf entry's key bytes
   (everything before the trailing 4-byte data-page + data-row trailer).
4. For any entry whose source value is > 127 chars, search the key bytes
   for the substring `08 07 08 04` (V2000 / V2003 / V2007) or
   `07 09 07 06` (V2010). It will appear exactly once.
5. Split the entry on that occurrence: chunk #1 + separator + chunk #2 +
   tail. Confirm that re-encoding `val[0..179)` and `val[181..255)`
   under your inline-only encoder reproduces chunks #1 and #2 byte-for-byte.

The same recipe applied to a synthetic fixture you author yourself (for
example, a Memo with three CRLF-separated 80-char lines) is the cleanest
path to closing the "three chunks" branch.

## Implementation strategy summary

```
encode(val, ascending):
  if val is null:
    return [null_flag(ascending)]

  if len(val) > 127:
    splitAt = first index of CR or LF in val
    if splitAt >= 0:
      resumeAt = splitAt + 1
      if (val[splitAt], val[resumeAt]) is (CR, LF) or (LF, CR):
        resumeAt += 1
      return encode_two_chunks(val, splitAt, resumeAt, ascending)

  # Fall through: existing single-chunk path (truncate at 127, trim
  # trailing spaces, run state machine, emit terminator + tail).
  return encode_single_chunk(val, ascending)


encode_two_chunks(val, splitAt, resumeAt, ascending):
  chunk1 = val[0..splitAt)
  chunk2 = trim_trailing_spaces(val[resumeAt..min(255, len(val))))

  state = new shared state (extras stream, unprintable list, crazy list,
                            char offset counter starting at 0)
  buf = [non_null_flag(ascending)]

  emit_chunk_inline_only(chunk1, codes, ext_codes, buf, state)
  buf += chunk_separator(format)
  emit_chunk_inline_only(chunk2, codes, ext_codes, buf, state)

  # Tail is identical to the single-chunk path:
  buf += END_TEXT
  buf += trim_trailing_low(state.extras)
  if state.unprintable or state.crazy:
    buf += [END_TEXT, END_TEXT]
    if state.crazy: write_crazy_codes(state.crazy, buf)
    if state.crazy and state.unprintable: buf += CRAZY_CODES_UNPRINT_SUFFIX
    if state.unprintable: buf += [END_TEXT] + state.unprintable
  if not ascending:
    buf += END_EXTRA_TEXT
    one_complement(buf[1..])
  buf += END_EXTRA_TEXT
  return buf
```

## Status in this repository

Closed:
- General Legacy (V2000 / V2003 / V2007) `Table11` / `Table11_desc` long
  rows: byte-exact match against Access-authored fixtures, validated by
  [GeneralLegacyEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralLegacyEncoderFixtureTests.cs).

Partial:
- General (V2010) `Table11` / `Table11_desc`: emits a sortable / lossless
  un-truncated entry but does not match Access's ~510-byte
  total-entry truncation cap byte-for-byte. The fixture sweep in
  [GeneralEncoderFixtureTests.cs](../../JetDatabaseWriter.Tests/Internal/GeneralEncoderFixtureTests.cs)
  still skips these tables.

Open:
- V2010 truncation cap exact rule.
- No-line-break long-input branch (no Access-authored fixture).
- V1997 / "General 97" long-row format (different state machine).
- 3-or-more-chunk inputs (no Access-authored fixture).
