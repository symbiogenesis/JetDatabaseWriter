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

When the V2000-style 2-chunk encoder is applied to V2010 data, it diverges
early because the 2-chunk path caps at 127 characters before the split.
The continuous 255-char encoder matches all three rows to byte 508.

### Leaf-to-row mapping (ascending index)

The ascending leaf entries are sorted by key value, NOT by row order.
Correct mapping (established by best-match search across all leaf keys):

| Data row | Leaf index | Match length | Special chars in encoded range (0–254) |
|---------:|-----------:|:-------------|----------------------------------------|
| row[2]   | leaf[2]    | 508 bytes    | `-` (Unprintable) at pos 0, `Á` (International) at pos 12 |
| row[3]   | leaf[4]    | 508 bytes    | `-` (Unprintable) at pos 3, `í` (International) at pos 25 |
| row[4]   | leaf[3]    | 508 bytes    | `-` (Unprintable) at pos 86, `-` (Unprintable) at pos 102 |

**Important**: an earlier analysis used `ascKeys[rowIdx]` directly (i.e.
row[3]→leaf[3], row[4]→leaf[4]), which gave `firstDiff = 50` for rows 3
and 4. This was a **mapping error**, not an encoding error. With the
correct mapping, all three rows match to byte 508.

### Character count is fixed at 255

The earlier "inline byte budget" / variable character count theory is
**disproven**. All three rows use a fixed 255-character cap:

| Row | Unprintable chars (0 inline) | International chars (2 inline) | Total inline bytes | flag + inline |
|----:|:----------------------------:|:------------------------------:|-------------------:|--------------:|
| 2   | 1                            | 1                              | 508                | 509           |
| 3   | 1                            | 1                              | 508                | 509           |
| 4   | 2                            | 0                              | 506                | 507           |

All three rows encode exactly 255 source characters (indices 0–254).
Unprintable characters produce 0 inline bytes, so the inline byte count
varies, but the character count is constant. The encoder does NOT use a
byte budget — it always encodes 255 chars, builds the full entry, and
hard-truncates to 510 bytes.

### Full (untruncated) entry structure

Encoding 255 chars with `maxEntryLength = 0` (no truncation) reveals the
full entry that would be produced before the 510-byte cap is applied:

| Row | Full entry length | Truncated bytes | Tail contents |
|----:|------------------:|----------------:|:--------------|
| 2   | 530               | 20              | END_TEXT + 11× extras(02) + extras(0E) + 01 01 + 80 07 06 82 + 00 |
| 3   | 543               | 33              | END_TEXT + 24× extras(02) + extras(0E) + 01 01 + 80 13 06 82 + 00 |
| 4   | 520               | 10              | END_TEXT + 01 01 01 + 81 5F 06 82 + 81 9B 06 82 + 00 |

Row 2 has 1 International char (Á at pos 12 → extra byte `0E`) and
1 Unprintable char (- at pos 0 → unprint offset `8007`, midfix `06`,
value `82`). Row 3 has 1 International (í at pos 25 → extra `0E`) and
1 Unprintable (- at pos 3 → offset `8013`). Row 4 has 2 Unprintables
(- at pos 86 → offset `815F`, - at pos 102 → offset `819B`), no
International chars.

### The 2-byte suffix

Observed suffix values (with corrected leaf-to-row mapping):

| row | ascending | descending |
|----:|:---------:|:----------:|
| 2   | `43 EC`   | (TBD)      |
| 3   | `A2 2D`   | (TBD)      |
| 4   | `1D AC`   | (TBD)      |

**Note:** the ascending suffix values above differ from an earlier version
of this document which had the wrong leaf mapping (rows 3 and 4 were
swapped). The descending values need re-verification with the corrected
mapping.

Properties established:

1. **NOT the bitwise complement** of the ascending suffix in descending
   entries. The suffix is computed independently per direction, meaning
   it is NOT part of the normal descending one's-complement pass.

2. **Varies per row.** The suffix differs between rows with different
   special-character compositions, even when the inline bytes are nearly
   identical. This means it encodes something about the extras/unprintable
   section that got truncated.

3. **NOT a function of omitted byte count.** Omitted counts are 20, 33, 10
   for rows 2/3/4 — no relationship to the suffix values.

4. **NOT inline bytes of the next unencoded character** ("continuation
   prefix" hypothesis). Tested: char[255] for each row produces inline
   bytes that do not match the suffix. E.g., row[2] char[255]=' ' → inline
   `07 02`, but suffix is `43 EC`.

### Exhaustive suffix hypothesis testing

The following algorithms and input formulations were tested systematically.
**None produced a match** for all three rows:

#### Algorithms tested

| Family | Algorithms |
|--------|------------|
| CRC-16 | CCITT/Kermit (poly 0x1021, reflected, init 0), CCITT-FALSE (poly 0x1021, unreflected, init 0xFFFF), XMODEM (poly 0x1021, unreflected, init 0), ARC (poly 0x8005, reflected, init 0), MODBUS (poly 0x8005, reflected, init 0xFFFF) |
| Non-CRC hash | Fletcher-16, FNV-1a (32→16 XOR-fold), DJB2 (32→16 XOR-fold), XOR-rotate-fold, additive sum mod 65536, Pearson-16 |
| Brute-force CRC | All 65536 polynomials × 4 modes (reflected/unreflected × init 0/0xFFFF) = 262144 CRC variants |
| Other | Adler-16 (mod 251), MurmurHash3-32 (seeds 0–255, 16-bit fold) |

#### Input formulations tested

All of the above algorithms were tested against every combination of these
input ranges:

| Input | Description |
|-------|-------------|
| `full[508..]` | Truncated tail (bytes 508 to end of untruncated entry) |
| `full[509..]` | From END_TEXT byte onwards |
| `full[510..]` | Only the bytes past the 510-byte boundary |
| `full[508..^1]` | Tail minus final END_EXTRA_TEXT |
| `full` | Complete untruncated entry |
| `full[1..]` | Untruncated entry minus flag byte |
| `full[..508]` | Just the retained portion (flag + inline) |
| `full[1..508]` | Just the inline bytes (no flag) |
| `text[255..] UTF-16LE` | Remaining source text as UTF-16LE bytes |
| `text full UTF-16LE` | Full source text as UTF-16LE bytes |
| `text[255..] ASCII` | Remaining source text as ASCII bytes |

Total combinations tested: ~3.4 million per row. Zero matches.

#### Other hypotheses disproven

- **Windows NLS sort key fragment.** `CompareInfo.GetSortKey` (InvariantCulture
  and en-US, with and without IgnoreCase) produces sort keys of ~276–957 bytes
  with completely different byte values from the Access encoding. No bytes at
  any offset match the expected suffix.

- **Pure inline continuation.** Encoding more than 255 chars (up to the full
  901-char source text) through the V2010 path produces the same inline bytes
  at positions [508..509] as encoding 255 chars. The truncation always cuts at
  the same point regardless of how many chars are encoded beyond the budget.

- **Google AI "CRC-16 of truncated data" claim.** An AI-generated answer
  confidently stated the suffix is a "CRC-16 variant" of the truncated tail.
  This was empirically disproven (see above). The AI subsequently admitted
  there is no public documentation confirming this.

### Remaining suffix candidates

1. **A proprietary hash/checksum with a non-standard polynomial or mixing
   function** — something that doesn't match any known CRC, Fletcher, or
   general-purpose hash family.

2. **A function of the raw on-disk compressed text bytes** — Memo fields may
   use LZSS compression; the suffix could derive from the compressed page
   data rather than the decoded string.

3. **A function of internal ACE state** — page number, row number, or other
   metadata mixed into the suffix computation.

4. **Recoverable only via disassembly of `acecore.dll`** — the ACE engine's
   binary may contain the suffix algorithm in its index-building code path.
   No public reverse-engineering of this specific code path is known.

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

- V2010 2-byte suffix derivation algorithm (see exhaustive testing above).
- V2010 binary long-key suffix (may be the same mechanism).
- V1997 "General 97" long-row format.

---

## Appendix A: fixture row details

The three long rows in the V2010 `Table11` fixture are 901, 901, and 903
characters respectively. All contain `\r\n` at source positions 179–180.
The "General" code tables map `-` (U+002D) as Unprintable (0 inline bytes,
unprint value `82`), `Á` (U+00C1) as International (inline `0E02`, extra
`0E`), and `í` (U+00ED) as International (inline `0E32`, extra `0E`).

Character `í` and character `i` share the same inline bytes (`0E32`).
Similarly, `Á` and `A`/`a` share inline bytes (`0E02`). International
characters are distinguished from their Simple counterparts only by their
extras stream contributions.

The ascending leaf page starts at page 112 (13 leaf entries total,
including short rows and the null entry). Descending leaf page starts at
page 119.

---

## Appendix B: superseded documents

- **`long-row-bisect.md`** — Raw output from an early chunk-boundary
  bisection probe. Its V2000/V2003/V2007 data remains accurate but its
  V2010 interpretation (treating `07 09 07 06` as a chunk separator) is
  **incorrect** — those bytes are just CR+LF encoding, and V2010 does not
  use a 2-chunk split. The file is retained as historical evidence but
  should not be used as a reference for V2010 behaviour.

- **`long-row-probe-dump.md`** — Full hex leaf entries for all fixture rows.
  Active reference material. Contains the expected V2010 entry bytes used
  for debugging.
