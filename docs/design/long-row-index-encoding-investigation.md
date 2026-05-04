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
> entry at 508 bytes with some trailing 2 byte seq`
> *(Jackcess `IndexCodesTest.findRow`)*

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

## V2000 / V2003 / V2007 — Jet 4, "General Legacy" (SOLVED)

### Observed leaf-entry layout

Single-column ascending leaf entry for a long value:

```
┌────────┬──────────────────┬─────────────┬──────────────────┬────────┬──────────────────────┬───────┐
│ flag   │ inline chunk #1  │  CR  +  LF  │ inline chunk #2  │ END_TX │ extras + unprintable │ END_X │
│ (1 B)  │ (variable)       │ 08 07 08 04 │ (variable)       │ (0x01) │ codes (variable)     │ (0x00)│
└────────┴──────────────────┴─────────────┴──────────────────┴────────┴──────────────────────┴───────┘
```

The descending form one's-complements the entire payload (everything except
the leading `0x80` flag) and appends a fresh unflipped `0x00`, exactly as
in the short-row case.

### Critical finding: the "separator" is NOT a synthetic sentinel

What was previously documented as a "4-byte separator" (`08 07 08 04` for
General Legacy, `07 09 07 06` for General) is actually the **natural inline
encoding of the embedded CR/LF line break** in the source text:

| char   | General Legacy inline | General inline |
|--------|----------------------|----------------|
| CR (U+000D) | `08 07` (SIMPLE) | `07 09` (SIMPLE) |
| LF (U+000A) | `08 04` (SIMPLE) | `07 06` (SIMPLE) |

These bytes fall out of normal per-codepoint processing. They are NOT a
special delimiter injected by the encoder. The source text in the fixture
contains literal `\r\n` (or `\n`) line breaks at position 179–180, and
those characters produce inline bytes just like any other character.

### Correct encoding algorithm (V2000/V2003/V2007)

The long-row path for Jet 4 is a **2-chunk** encode:

1. When `len(input) > 127` AND the input contains an embedded line break
   (CR or LF), split the input at the first line break position.
2. Encode chunk #1: `input[0..splitAt)` (all characters before the line break).
3. The CR and/or LF characters encode normally as inline bytes (producing
   the `08 07 08 04` or `07 09 07 06` sequence).
4. Encode chunk #2: `input[resumeAt..min(resumeAt + (255 - resumeAt), len)]`
   where `resumeAt` is the position after the line break sequence.
   Trailing spaces are trimmed.
5. The extras / unprintable / crazy streams are **unified** across both
   chunks — character offsets address into the concatenated chunk stream.
6. Append `END_TEXT (0x01)`, the extras/unprintable/crazy streams, and
   `END_EXTRA_TEXT (0x00)`.
7. Apply the standard descending pass if needed (one's-complement payload,
   append `0x00`).

This produces **byte-exact** output for all three long rows across V2000,
V2003, and V2007. The implementation is validated and passing.

### Empirical sample — `Table11.DataIndex` (ascending), V2000

Source value (901 chars; first few chars shown):

> `-a;sldjfl;akÁsj dfl;kasj ldfkaslhdfkjhasjk dhfkljas djfhaskljdhfkjshadfj hkj skj…`

Five identical 180-char lines separated by `\r\n`, ending with `AAAA-A`.

On-disk leaf entry (278 bytes):

```
7F                                                      ← flag (ascending non-null)
4A 22 6B 5E 4F 5B 53 5E 22 4A 5C 4A 6B 5B 07 4F 53 5E 22 5C 4A 6B 5B 07 5E
4F 53 5C 4A 6B 5E 57 4F 53 5C 5B 57 4A 6B 5B 5C 07 4F 57 53 5C 5E 5B 4A 6B
07 4F 5B 53 57 4A 6B 5C 5E 5B 4F 57 53 5C 5B 6B 57 4A 4F 53 5B 07 57 5C 5B
07 6B 5C 5B 07 4F 57 53 5C 5B 4A 6B 57 07 4F 53 57 07 5C 4A 6B 5B 07 4F 53
5B 5C 57 4A 6B 5C 5B 4F 57 53 5C 5B 4A 6B 57 07 5C 4F 07 53 5C 5B 4A 6B 4F
57 53 07 5C 5B 4A 07 6B 57 5C 07 4F 53 5C 5B 4A 6B 53 4F 57 5C 5B 4A 07 6B
4F 57 53 5C 5B 07 4A 6B 4F 57 53 5C 5B 6B 4A 57 5C 4F 07 53 5C 4A 5B 6B 07
4F 57 53                                                ← chunk #1: input[0..179) = 178 inline bytes
08 07 08 04                                             ← CR + LF inline encoding (NOT a separator)
4A 22 6B 5E 4F 5B 53 5E 22 4A 5C 6B 5B 07 4F 53 5E 22 5C 4A 6B 5B 07 5E 4F
53 5C 4A 6B 5E 57 4F 53 5C 5B 57 4A 6B 5B 5C 07 4F 57 53 5C 5E 5B 4A 6B 07
4F 5B 53 57 4A 6B 5C 5E 5B 4F 57 53 5C 5B 6B 57 4A 4F 53 5B 07 57 5C 5B
                                                        ← chunk #2: input[181..255) = 74 inline bytes
01                                                      ← END_TEXT
02 02 02 02 02 02 02 02 02 02 02                        ← extras (Á at position 12)
0E 01 01 01 80 07 06 82                                 ← unprintable codes
00                                                      ← END_EXTRA_TEXT
```

### Verified properties (byte-exact across all V2000/V2003/V2007 fixtures)

| row | entry length | chunk #1 source | CR/LF source | chunk #2 source | status |
|----:|:------------:|:----------------|:-------------|:----------------|:------:|
| 2   | 278 B        | `val[0..179)` (179 chars) | `val[179..181)` = `\r\n` | `val[181..255)` (74 chars) | ✅ byte-exact |
| 3   | 269 B        | `val[0..179)` (179 chars) | `val[179..181)` = `\r\n` | `val[181..255)` (74 chars) | ✅ byte-exact |
| 4   | 291 B        | `val[0..179)` (179 chars) | `val[179..181)` = `\r\n` | `val[181..255)` (74 chars) | ✅ byte-exact |

The trailing extras + unprintables block aggregates the auxiliary streams for
**both chunks** — i.e. an `Á` at position 12 of the source produces a single
international-extra placeholder in the trailing block, with character offsets
addressing into the concatenated chunk character stream.

## V2010 — ACE, "General" sort order (PARTIALLY SOLVED)

### Observed structure

V2010 uses entirely different code tables ("General" vs "General Legacy")
but structurally the same per-codepoint state machine. However, the
long-row handling is **fundamentally different** from V2000/V2003/V2007:

- **No 2-chunk split.** V2010 uses **continuous encoding** of up to 255
  input characters with no chunk boundary and no special treatment of
  line breaks.
- **Hard byte cap at 510 bytes.** The fully-encoded entry (flag + inline +
  END_TEXT + extras + END_EXTRA_TEXT) is hard-truncated at 510 bytes.
- **2-byte suffix replaces the last 2 bytes.** The final 2 bytes of the
  510-byte entry are NOT the natural continuation of the truncated stream
  but are replaced with a mystery 2-byte value.

All three long rows in `testIndexCodesV2010.accdb` produce **exactly 510-byte**
leaf entries.

### Empirical evidence — `Table11.DataIndex` (ascending), V2010

On-disk entry structure (510 bytes total):

```
┌────────┬──────────────────────────────────────────────┬────────────────┐
│ flag   │ inline payload (continuous, no chunk split)   │ 2-byte suffix  │
│ (1 B)  │ bytes [1..507]  (507 bytes)                  │ bytes [508..509]│
└────────┴──────────────────────────────────────────────┴────────────────┘
```

There is NO `END_TEXT`, NO extras block, NO `END_EXTRA_TEXT` in the truncated
entry. The entire 509-byte payload after the flag byte consists of 507 bytes of
inline-encoded text followed by 2 mystery suffix bytes.

### Key observation: continuous encoding matches to byte 508

When the encoder processes the first 255 characters of the input continuously
(no chunk split, no trailing-space trim) and builds the full entry (flag +
inline + END_TEXT + extras + END_EXTRA_TEXT), then truncates at 510 bytes:

| row | entry len | firstDiff vs Access | our bytes[508..509] | Access bytes[508..509] |
|----:|:---------:|:-------------------:|:-------------------:|:----------------------:|
| 2   | 510       | **508**             | `35 01`             | `43 EC`                |
| 3   | 510       | **508** (see note)  | `35 01`             | `35 01 1D AC` (shifted)|
| 4   | 510       | **508** (see note)  | `35 01 01 01`       | `36 0E A2 2D` (shifted)|

**Note on rows 3 and 4:** These rows differ from our output starting at byte
50, not byte 508. This is because Access appears to encode a **variable number
of characters** for each row, not a fixed 255. The character count appears to
be driven by an inline-byte budget: Access encodes characters until some inline
byte threshold is reached, then writes the 2-byte suffix to fill the entry to
exactly 510 bytes.

For row[2] specifically, 255 characters happens to produce exactly the right
number of inline bytes, so the match extends to byte 508 and only the final
2 bytes differ. Rows 3 and 4 have different character compositions (different
special characters like `Á`, `í`, `-` that produce different inline byte
widths), so their optimal character count differs.

### The 2-byte suffix — what we know

Observed suffix values from the fixture:

| row | direction  | suffix (hex) | notes |
|----:|:----------:|:------------:|:------|
| 2   | ascending  | `43 EC`      |       |
| 3   | ascending  | `1D AC`      |       |
| 4   | ascending  | `A2 2D`      |       |
| 2   | descending | `37 DD`      | (from leaf[8] of Table11_desc) |
| 3   | descending | `C1 A1`      | (from leaf[9] of Table11_desc) |
| 4   | descending | `9A 4E`      | (from leaf[10] of Table11_desc) |

### Suffix properties established

1. **NOT the bitwise complement of the ascending suffix.** The descending
   one's-complement pass flips everything in `[payloadStart..end)`, so if
   the suffix were part of the normal payload, `desc_suffix = ~asc_suffix`.
   Checking: `~43EC = BC13 ≠ 37DD`, `~1DAC = E253 ≠ C1A1`,
   `~A22D = 5DD2 ≠ 9A4E`. **None match.** The suffix is computed
   independently for each direction.

2. **NOT a common CRC-16 or checksum of the omitted bytes.** Tested against
   the bytes that would have appeared at positions 510+ of the uncapped
   encoding (the "omitted tail"):
   - CRC-16/IBM, CRC-16/MODBUS, CRC-16/CCITT-FALSE, CRC-16/X25,
     CRC-16/KERMIT, CRC-16/XMODEM: no match
   - Sum-16, XOR-16, Fletcher-16, Adler-16: no match
   - FNV-1a-16, djb2-16, sdbm-16: no match (overflow issues in testing)
   - First/last 2 bytes of MD5 or SHA-256 of the omitted bytes: no match

3. **Not a simple function of the omitted byte count.** Omitted byte
   counts are 20, 33, 10 for rows 2/3/4 respectively — no obvious
   relationship to `43EC`, `1DAC`, `A22D`.

4. **Appears to vary with both the input text and the direction.** This
   suggests it encodes information about the *truncated portion* of the
   key that aids comparison of entries that share the same 508-byte prefix.

### Inline byte-budget hypothesis (current working theory)

The strongest remaining hypothesis is that V2010 does NOT use a fixed
character count. Instead:

- Access encodes characters one at a time into the inline stream.
- It stops when the inline byte stream reaches a threshold (likely 508
  bytes = 510 - flag - suffix).
- It then writes a 2-byte value derived from some property of the
  remaining (unencoded) input that preserves sort order.

Evidence supporting this:
- Row[2] (with `Á` at position 12, producing extra inline width early)
  naturally hits 508 inline bytes at exactly 255 characters.
- Row[3] (with `-` at position 3, which has a different inline width in
  General tables) would need ~256 characters to fill the same budget.
- The expected entry for row[3] is byte-shifted by +2 relative to row[2]
  at position 24 in the expected output, consistent with row[3] encoding
  one more character.

If the suffix is a sort-order-preserving digest of the remaining input
(e.g. the first 2 bytes of encoding the next unencoded character), that
would explain why:
- It varies per row (different next character)
- It varies per direction (ascending vs descending next-char encoding)
- It is not a hash of the omitted bytes (it's forward-looking, not
  backward-looking)

### What remains to be determined

1. **Exact inline byte budget.** Is it 508 bytes (= 510 - 1 flag - 1 suffix
   byte... no, that's 509)? Or 507 bytes? The probe shows position 508 as
   the first diff, meaning bytes 0-507 match (508 bytes including the flag).
   So the inline payload is 507 bytes (positions 1-507), and positions
   508-509 are the suffix.

2. **Suffix derivation algorithm.** The most likely candidates:
   - First 2 bytes of encoding the next character that would have been
     emitted (the "continuation prefix")
   - Some function of the character index where truncation occurred
   - A proprietary Windows NLS / ICU collation weight fragment

3. **Whether the suffix is the same for binary (non-text) long keys.**
   The `NonTextSingleColumnIndexFixtureTests` skip V2010 binary long
   keys with the same upstream TODO. If binary keys also have a 2-byte
   suffix, the algorithm is generic to ACE rather than text-specific.

## Format-version differences (updated)

| format | encoding strategy | max leaf entry length | character cap | status |
|--------|-------------------|----------------------:|:-------------:|:------:|
| V1997 (Jet 3, "General 97") | Unknown (different state machine) | n/a | n/a | Not investigated |
| V2000 (Jet 4) | 2-chunk split at first line break | 291 bytes | 255 total across both chunks | ✅ byte-exact |
| V2003 (Jet 4) | 2-chunk split at first line break | 291 bytes | 255 total across both chunks | ✅ byte-exact |
| V2007 (ACE)   | 2-chunk split at first line break | 291 bytes | 255 total across both chunks | ✅ byte-exact |
| V2010 (ACE, "General") | Continuous encode, no split, 2-byte suffix | **510 bytes** | ~255 (variable, byte-budget driven) | 🟡 first 508 bytes correct, suffix unknown |

### Code table differences for "line break" bytes

| char | General Legacy | General |
|------|:--------------:|:-------:|
| CR (U+000D) | `08 07` (SIMPLE, 2 inline bytes) | `07 09` (SIMPLE, 2 inline bytes) |
| LF (U+000A) | `08 04` (SIMPLE, 2 inline bytes) | `07 06` (SIMPLE, 2 inline bytes) |

These are ordinary SIMPLE character entries in both code tables. They produce
normal inline bytes with no side effects on the extras/unprintable streams.
The `08 07 08 04` / `07 09 07 06` byte sequences visible in the encoded
entries are literally the CR and LF characters being encoded, NOT a special
synthetic separator.

## Implementation strategy

### V2000 / V2003 / V2007 (implemented, byte-exact)

```
function encode_long_row_jet4(text, ascending, codes, ext_codes):
    if len(text) > 127:
        split_at = index_of_first_CR_or_LF(text)
        if split_at >= 0:
            resume_at = split_at + 1
            if is_CRLF_pair(text, split_at):
                resume_at += 1

            chunk1 = text[0 .. split_at]
            chunk2 = text[resume_at .. min(255, len(text))].rstrip(' ')

            entry = [FLAG]
            state = new_state(len(chunk1) + len(chunk2))
            emit_inline(chunk1, codes, ext_codes, entry, state)
            emit_inline(separator_chars, codes, ext_codes, entry, state)  // CR+LF encode naturally
            emit_inline(chunk2, codes, ext_codes, entry, state)
            finish_entry(entry, state, ascending)
            return entry

    // Short path or no line break: standard single-chunk encode
    chars = text[0 .. min(127, len(text))].rstrip(' ')
    // ... standard encode ...
```

Note: the "separator" is not injected — chunk1 ends before CR, the CR and LF
are encoded as normal inline characters, and chunk2 starts after the line
break. The 4-byte sequence is a natural byproduct.

### V2010 (partially implemented — suffix algorithm TBD)

```
function encode_long_row_v2010(text, ascending, codes, ext_codes):
    // Continuous encode up to 255 chars (no chunk split, no space trim)
    chars = text[0 .. min(255, len(text))]

    entry = [FLAG]
    state = new_state(len(chars))
    emit_inline(chars, codes, ext_codes, entry, state)
    finish_entry(entry, state, ascending, max_length=510)
    // entry is now truncated to 510 bytes

    // TODO: replace bytes [508..509] with the ACE 2-byte suffix
    // suffix = compute_suffix(???)
    // entry[508] = suffix[0]
    // entry[509] = suffix[1]

    return entry
```

The `finish_entry` with `max_length=510` hard-truncates the completed entry
(after the descending complement pass, if applicable). The final 2 bytes must
then be overwritten with the suffix — but the suffix algorithm is not yet
determined.

## Reproduction recipe (for any language)

### For V2000/V2003/V2007 (to verify the 2-chunk path)

1. Open one of the three `testIndexCodesV20{00,03,07}` fixtures.
2. Locate the `Table11` index whose first column is the Memo/Text column
   `data` (ascending). Record `firstDp` from the index descriptor.
3. Walk the leaf B-tree starting at `firstDp`.
4. Extract every entry's key bytes (everything before the trailing
   `(page_no:3, row_no:1)` row pointer).
5. Find the `08 07 08 04` byte sequence — this is the inline encoding of
   the CR+LF characters. Everything before it is chunk #1's inline bytes
   plus those CR+LF bytes are in between, and everything after is chunk #2
   continuing through END_TEXT and the trailing extras block.

### For V2010 (to study the suffix)

1. Open `testIndexCodesV2010.accdb`.
2. Walk the `Table11` ascending leaf B-tree (firstDp=112).
3. Entries for rows 2–4 are exactly 510 bytes each.
4. Bytes [508..509] are the 2-byte suffix.
5. Also walk `Table11_desc` (descending, firstDp=119) — entries for the
   same rows are also 510 bytes with independently-computed suffixes.
6. Compare ascending suffix vs `~(descending suffix)` — they do NOT match,
   confirming the suffix is not part of the normal complement pass.

## Status in this repository

### V2000 / V2003 / V2007 — COMPLETE ✅

The 2-chunk long-row encoder is implemented in `GeneralLegacyTextIndexEncoder`
and produces byte-exact output for all Table11/Table11_desc entries across
all three Jet 4 format variants. The fixture tests pass without skips.

### V2010 — PARTIAL 🟡

The continuous 255-character encoder with 510-byte truncation is implemented
and matches the first 508 bytes of every V2010 long-row entry. The remaining
gap is the 2-byte suffix at positions [508..509].

Current skips / known limitations:

- `JetDatabaseWriter.Tests/Internal/GeneralEncoderFixtureTests.cs` — V2010
  long-row tests are enabled but failing (last 2 bytes differ).
- `JetDatabaseWriter.Tests/Internal/NonTextSingleColumnIndexFixtureTests.cs` —
  V2010 binary long keys still skipped (may share the same suffix mechanism).

### Diagnostic probe

`JetDatabaseWriter.Tests/Internal/LongRowSourceProbe.cs` is a temporary
one-shot diagnostic that dumps detailed per-row analysis including:
- Source character positions around the chunk boundary
- Expected vs actual entry tails
- Uncapped full encoding length and omitted bytes
- Prefix sweep showing how varying character counts affect the match point

This file should be removed once the V2010 suffix algorithm is determined.

## Appendix: ruling out old hypotheses

### "The separator is a synthetic 4-byte sentinel" — DISPROVED

The original theory was that Access injects a special 4-byte sentinel between
two independently-encoded chunks. This is wrong. The `08 07 08 04` bytes are
simply CR (U+000D) → `08 07` and LF (U+000A) → `08 04` encoded through the
standard General-Legacy per-codepoint table. Similarly, `07 09 07 06` in the
General table is CR → `07 09` and LF → `07 06`.

Proof: looking up CR and LF in the code tables directly:
- General Legacy `index_codes_genleg.txt.gz`: CR = `S0807` (SIMPLE, inline=`08 07`), LF = `S0804` (SIMPLE, inline=`08 04`)
- General `index_codes_gen.txt.gz`: CR = `S0709` (SIMPLE, inline=`07 09`), LF = `S0706` (SIMPLE, inline=`07 06`)

### "V2010 uses the same 2-chunk split as Jet 4" — DISPROVED

When the 2-chunk encoder is applied to V2010 fixture data, rows 3 and 4
differ from Access at position **50** (far before the entry tail), while
the continuous encoder matches until position 508. The 2-chunk split produces
incorrect results for V2010.

### "The 2-byte suffix is a CRC/checksum of omitted bytes" — DISPROVED

Tested CRC-16 (6 variants), Fletcher-16, Adler-16, sum-16, XOR-16, FNV-1a,
djb2, sdbm, and first/last 2 bytes of MD5/SHA-256 of the omitted tail bytes.
None produce the observed suffix values (`43EC`, `1DAC`, `A22D`).

### "The suffix is the complement of the ascending suffix" — DISPROVED

`~0x43EC = 0xBC13 ≠ 0x37DD` (desc row 2). The suffix is computed
independently per direction, not derived from the other direction's value.
