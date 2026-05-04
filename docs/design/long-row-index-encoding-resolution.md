# Microsoft Access "long row" indexed-text key encoding — resolution

This is a follow-on to
[long-row-index-encoding-investigation.md](long-row-index-encoding-investigation.md).
The investigation reverse-engineered the on-disk layout of long-row index
leaf entries in the Access-authored `Table11` / `Table11_desc` fixtures
(V2000 / V2003 / V2007 / V2010); this document describes the algorithms
we arrived at, the remaining open question (V2010 2-byte suffix), and a
step-by-step implementation strategy any conforming implementation can use.

The strategy is intentionally language-agnostic. The notation `val[a..b)`
denotes a half-open source-character range; "byte-exact" means the resulting
on-disk leaf entry matches what Microsoft Access itself writes when given
the same source value.

The reference fixtures referenced throughout are the Jackcess
`testIndexCodesV{2000,2003,2007,2010}` corpus, distributed under Apache 2.0.

---

## TL;DR

There are **two distinct algorithms** depending on the Access format version:

### V2000 / V2003 / V2007 (Jet 4, "General Legacy") — SOLVED ✅

When the indexed value exceeds 127 chars **and** contains an embedded line
break (CR or LF):

1. Split the input at the first CR or LF.
2. Encode chunk #1 (before the break) continuously.
3. The CR and LF characters encode normally as inline bytes (`08 07 08 04`
   in General Legacy) — they are NOT a synthetic separator.
4. Encode chunk #2 (after the break, up to source position 255, trimmed).
5. Append the unified extras/unprintable/crazy tail + terminators.

This produces **byte-exact** output for all fixture rows.

### V2010 (ACE, "General") — PARTIALLY SOLVED 🟡

V2010 uses a completely different strategy:

1. Encode up to **255 characters continuously** (no chunk split, no special
   line-break handling). The CR and LF characters encode as normal inline
   bytes (`07 09 07 06` in General), producing what *looks like* a separator
   but is actually just ordinary per-codepoint output.
2. Build the full entry (flag + inline + END_TEXT + extras + END_EXTRA_TEXT).
3. Hard-truncate at **510 bytes**.
4. Replace the final 2 bytes (positions [508..509]) with a **mystery 2-byte
   suffix** whose derivation algorithm is not yet determined.

The first 508 bytes match Access byte-for-byte. The suffix algorithm is the
sole remaining gap.

---

## V2000 / V2003 / V2007: The 2-chunk algorithm (byte-exact)

### Critical finding: "separator" bytes are just CR+LF

What was previously documented as a "4-byte chunk separator" is actually the
**natural inline encoding** of the embedded CR/LF in the source text:

| char | General Legacy inline | General inline |
|------|----------------------|----------------|
| CR (U+000D) | `08 07` (SIMPLE) | `07 09` (SIMPLE) |
| LF (U+000A) | `08 04` (SIMPLE) | `07 06` (SIMPLE) |

These fall out of normal per-codepoint processing. They are NOT special
delimiters injected by the encoder. The source text in the fixtures contains
literal `\r\n` at positions 179–180, and those characters encode just like
any other SIMPLE character.

### Algorithm

```
encode_long_row_jet4(val, ascending, codes, ext_codes):
  if len(val) > 127:
    splitAt = first index of CR or LF in val
    if splitAt >= 0:
      resumeAt = splitAt + 1
      if is_CRLF_pair(val, splitAt):
        resumeAt += 1

      chunk1 = val[0 .. splitAt)
      chunk2 = val[resumeAt .. min(255, len(val))].rstrip(' ')

      state = new shared_state(char_offset=0)
      buf = [non_null_flag(ascending)]

      # chunk1 encodes normally (inline + extras/unprintable/crazy)
      emit_chunk(chunk1, codes, ext_codes, buf, state)

      # CR+LF encode as normal inline bytes (produces 08 07 08 04)
      emit_chunk(val[splitAt..resumeAt), codes, ext_codes, buf, state)

      # chunk2 continues with shared state (char offsets are continuous)
      emit_chunk(chunk2, codes, ext_codes, buf, state)

      finish_entry(buf, state, ascending, max_length=0)
      return buf

  # ≤ 127 chars or no line break: standard single-chunk path
  return encode_single_chunk(val, ascending)
```

The key insight is that there is no special "separator injection" step.
The encoder simply processes all characters from chunk1, then the line-break
characters, then chunk2 — all through the same per-codepoint state machine
with continuous char-offset tracking.

### Why the split matters at all

If there's no synthetic separator, why does the split matter? Because:

1. The **character cap** for chunk2 is `min(255, len(val)) - resumeAt`, not
   `127 - resumeAt`. The total character budget across both chunks is 255
   source positions (not 127).
2. Chunk2's trailing spaces are **trimmed** (matching Jackcess's
   `toIndexCharSequence` behaviour).
3. The extras/unprintable/crazy streams use **continuous char offsets**
   across both chunks — a single shared state drives both.

Without the 2-chunk awareness, an encoder would cap at 127 chars total and
produce entries far too short.

### Unified extras stream

The trailing extras (and unprintable / crazy) block addresses into the
**concatenated** chunk character stream. Maintain a single `charOffset`
counter that runs over the inline emission. When a character produces an
extras / unprintable / crazy contribution, it uses the current `charOffset`
value. The counter increments for each character that produces inline bytes,
regardless of which chunk it belongs to. The 4 bytes from CR+LF advance the
counter by 2 (one for CR, one for LF) just like any other SIMPLE characters.

### Descending pass

Unchanged from the single-chunk path:

1. Write `END_EXTRA_TEXT (0x00)` to the buffer.
2. One's-complement every byte after the leading flag.
3. Append a fresh unflipped `0x00` as the entry terminator.

### Validation

Byte-exact for all 3 long rows × 4 directions (asc/desc) × 3 format versions
= 12 leaf entries validated. Implementation is in
`GeneralLegacyTextIndexEncoder.EncodeTwoChunks()`.

---

## V2010: Continuous encoding with 510-byte cap (partially solved)

### Why V2010 is different

V2010 uses the "General" code tables (different per-codepoint values) and a
fundamentally different long-row strategy:

- **No 2-chunk split.** The encoder processes characters continuously,
  regardless of line breaks.
- **255-character cap** (not 127). The encoder processes up to 255 chars
  with no trailing-space trim.
- **510-byte hard truncation.** After building the full entry (flag + inline
  + END_TEXT + extras + END_EXTRA_TEXT), it is hard-truncated at 510 bytes.
- **2-byte suffix.** The final 2 bytes of the 510-byte entry are overwritten
  with a value whose derivation is unknown.

### Evidence: V2010 does NOT use the 2-chunk split

When the V2000-style 2-chunk encoder is applied to V2010 data:
- Row[2]: `firstDiff = 50` (diverges early)
- Row[3]: `firstDiff = 50`
- Row[4]: `firstDiff = 50`

When the continuous 255-char encoder is applied to V2010 data:
- Row[2]: `firstDiff = 508` (matches all but last 2 bytes)
- Row[3]: `firstDiff = 508` (with correct char count — see below)
- Row[4]: `firstDiff = 508` (with correct char count — see below)

The continuous path is unambiguously correct for the first 508 bytes.

### The character-count question

Row[2] matches with exactly 255 characters. But rows 3 and 4 appear to
require a **different character count** to match at position 508. The
expected entries for rows 3 and 4 are byte-shifted relative to row 2 at
early positions, consistent with Access encoding one more (or fewer)
character.

The working theory is that V2010 uses an **inline byte budget** rather than
a fixed character count:
- Access encodes characters one at a time.
- It stops when the inline byte count reaches a threshold (~507 bytes of
  inline payload, leaving room for the flag byte and 2-byte suffix to fill
  510 total).
- Different characters have different inline widths (SIMPLE = 2 bytes,
  International = 2 bytes, Significant = 2 bytes, etc.), so the character
  count varies per input.

For row[2], 255 characters happen to produce exactly the right inline byte
count. For rows 3 and 4 (which have different special characters at
different positions), the optimal character count differs.

### The 2-byte suffix

Observed suffix values:

| row | ascending | descending |
|----:|:---------:|:----------:|
| 2   | `43 EC`   | `37 DD`    |
| 3   | `1D AC`   | `C1 A1`    |
| 4   | `A2 2D`   | `9A 4E`    |

Properties established:

1. **NOT the bitwise complement** of the ascending suffix in descending
   entries: `~43EC = BC13 ≠ 37DD`. The suffix is computed independently
   per direction, meaning it is NOT part of the normal descending
   one's-complement pass.

2. **NOT a CRC-16 or checksum** of the "omitted" tail bytes (the bytes
   that would appear at positions 510+ of the uncapped encoding). Tested:
   CRC-16 (IBM, MODBUS, CCITT-FALSE, X25, KERMIT, XMODEM), Fletcher-16,
   Adler-16, sum-16, XOR-16, FNV-1a, djb2, sdbm, MD5 first/last 2 bytes,
   SHA-256 first/last 2 bytes. None match.

3. **NOT a function of omitted byte count.** Omitted counts are 20, 33, 10
   for rows 2/3/4 — no relationship to `43EC`, `1DAC`, `A22D`.

4. **Varies with both input text and direction.** Suggests it encodes
   something about the truncated portion that aids sort-order comparison.

### Most likely suffix candidates (untested)

1. **First 2 bytes of encoding the next unencoded character** — the
   "continuation prefix". If Access emits characters until the budget is
   full, the next character's first 2 inline bytes would be a natural
   sort-preserving tiebreaker. This would also explain why it varies per
   direction (ascending vs descending encoding of the same char differ
   after the complement pass).

2. **A Windows NLS / ICU sort-key fragment** — Access/ACE may delegate to
   the OS collation engine for the suffix rather than using its own tables.

3. **A proprietary hash of the remaining source text** — designed to
   preserve sort order (not a standard hash family).

### Algorithm (current implementation)

```
encode_long_row_v2010(val, ascending, codes, ext_codes):
  chars = val[0 .. min(255, len(val))]   # no trailing-space trim

  state = new shared_state(char_offset=0)
  buf = [non_null_flag(ascending)]
  emit_chunk(chars, codes, ext_codes, buf, state)
  finish_entry(buf, state, ascending, max_length=510)

  # TODO: replace buf[508..509] with the correct 2-byte suffix
  # suffix = compute_suffix(???)
  # buf[508] = suffix[0]
  # buf[509] = suffix[1]

  return buf
```

The `finish_entry` with `max_length=510` builds the complete entry (inline +
END_TEXT + extras + END_EXTRA_TEXT), applies the descending complement if
needed, then hard-truncates to 510 bytes.

---

## Branches without Access-authored fixtures

1. **Long input with no CR/LF (V2000/V2003/V2007).** All public fixtures
   with > 127 chars contain CRLF. The implementation falls back to the
   127-char single-chunk path for this case (matching Jackcess behaviour).

2. **V1997 / "General 97" long-row format.** Jet 3 uses a different state
   machine (no END_TEXT framing; nibble-packed extras). Not investigated.

3. **V2010 binary (non-text) long keys.** The `NonTextSingleColumnIndex`
   fixture tests skip V2010 binary long keys with the same upstream TODO.
   These may share the 2-byte suffix mechanism — if so, it's a generic ACE
   property rather than text-specific.

---

## Reproduction recipe (any language)

### V2000 / V2003 / V2007

1. Open one of the three `testIndexCodesV20{00,03,07}` fixtures.
2. Locate `Table11.DataIndex` (ascending, firstDp from index descriptor).
3. Walk the leaf B-tree and extract key bytes for rows 2–4.
4. Find the `08 07 08 04` sequence — this is just CR+LF encoded normally.
5. Re-encode `val[0..splitAt)` + CR+LF + `val[resumeAt..255)` through
   your per-codepoint state machine with continuous char offsets.
6. Confirm byte-exact match.

### V2010

1. Open `testIndexCodesV2010.accdb`.
2. Walk `Table11.DataIndex` ascending (firstDp=112) and descending
   (firstDp=119).
3. Long-row entries are exactly 510 bytes each.
4. Encode the first 255 characters continuously with no split; build
   full entry and truncate to 510.
5. Bytes [0..507] should match. Bytes [508..509] are the unknown suffix.
6. Compare ascending vs descending suffixes — they are NOT complements.

---

## Status in this repository

### Closed ✅

- **General Legacy (V2000 / V2003 / V2007)** `Table11` / `Table11_desc`:
  byte-exact match against Access-authored fixtures, validated by
  `GeneralLegacyEncoderFixtureTests.cs`. No skips.

### Partially closed 🟡

- **General (V2010)** `Table11` / `Table11_desc`: first 508 bytes match.
  Final 2 bytes (the suffix) are incorrect. Tests in
  `GeneralEncoderFixtureTests.cs` are enabled but failing on long rows.

### Open

- V2010 2-byte suffix derivation algorithm.
- V2010 inline byte-budget character count (currently using fixed 255;
  only row[2] matches at byte 508 with this count — rows 3/4 need a
  variable count driven by inline byte budget).
- V2010 binary long-key suffix (may be the same mechanism).
- V1997 "General 97" long-row format.

---

## Appendix: superseded documents

- **`long-row-bisect.md`** — Raw output from an early chunk-boundary
  bisection probe. Its V2000/V2003/V2007 data remains accurate but its
  V2010 interpretation (treating `07 09 07 06` as a chunk separator) is
  **incorrect** — those bytes are just CR+LF encoding, and V2010 does not
  use a 2-chunk split. The file is retained as historical evidence but
  should not be used as a reference for V2010 behaviour.
